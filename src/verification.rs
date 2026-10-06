//! Bounded end-to-end challenges. Runtime receipts cannot acknowledge for the agent.
use crate::{
    diagnostics::{Operation, Phase, error_text},
    identity::Binding,
    states::{DeliveryReadiness as State, NativeRuntime},
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sqlx::{Sqlite, Transaction};
use std::{os::unix::fs::MetadataExt, path::Path};
use uuid::Uuid;

/// Current capability plus historical, explicitly scoped evidence.
#[derive(Debug, Serialize)]
pub struct DeliveryStatus {
    /// Fleet containing the recipient.
    pub group: String,
    /// Recipient registration.
    pub agent: String,
    /// Current delivery readiness.
    pub state: State,
    /// True only with agent acknowledgment and current transport health.
    pub ready: bool,
    /// Agent executed the challenge command; not proof of understanding or progress.
    pub agent_acknowledged: bool,
    /// Explicit acknowledgment timestamp.
    pub acknowledged_at: Option<i64>,
    /// Runtime hook observed this exact notification.
    pub runtime_received_at: Option<i64>,
    /// Runtime accepted the dispatch; not agent acknowledgment.
    pub transport_accepted_at: Option<i64>,
    /// Last successful route health check.
    pub checked_at: Option<i64>,
    /// Persisted attempts for this connection.
    pub attempts: i64,
    /// Next bounded dispatch attempt, if another attempt is available.
    pub next_attempt_at: Option<i64>,
    /// Deadline for the current challenge.
    pub deadline: Option<i64>,
    /// Concrete repair instruction when not ready.
    pub next_action: Option<String>,
}
impl DeliveryStatus {
    fn new(actor: &Mailbox, state: State) -> Self {
        Self {
            group: actor.group_name.clone(),
            agent: actor.name.clone(),
            state,
            ready: false,
            agent_acknowledged: false,
            acknowledged_at: None,
            runtime_received_at: None,
            transport_accepted_at: None,
            checked_at: None,
            attempts: 0,
            next_attempt_at: None,
            deadline: None,
            next_action: None,
        }
    }
    fn finish(mut self) -> Self {
        self.ready = self.state == State::Verified;
        self.next_action=match self.state {
            State::Verified=>None,
            State::Unknown=>Some("Delivery diagnostics failed; inspect database health".into()),
            State::WorkerStopped=>Some("Start the delivery service or use a managed agent-mail run launch".into()),
            State::MissingEndpoint=>Some(format!("Launch {} with agent-mail run, or attach its existing native session",self.agent)),
            State::Paused=>Some("Delivery is explicitly paused; resume only when authorized".into()),
            State::NotifyOnly=>Some("Herdr has no idle delivery under notify-only policy; use a native managed launch or explicitly opt into unguarded prompts".into()),
            State::Retired=>Some("Restore the agent registration explicitly before delivery".into()),
            State::RemoteUnsupported=>Some("Verify delivery on the recipient's home machine; local readiness cannot confirm a remote agent".into()),
            State::Unverified|State::Verifying=>Some("Waiting for the idle client to receive and acknowledge its delivery challenge; do not acknowledge on its behalf".into()),
            State::Expired=>Some(format!("Inspect the endpoint and client permissions, then run agent-mail --group {} agent retry {}",self.group,self.agent)),
            State::Unavailable=>Some(format!("Run agent-mail --group {} status --check {} and repair the current connection",self.group,self.agent)),
        };
        self
    }
}
#[derive(Serialize)]
pub(crate) enum Route {
    Native {
        socket: String,
        thread: String,
        runtime: NativeRuntime,
        launch: Option<String>,
        inbox_token: Option<String>,
        inbox_identity: Option<String>,
        device: u64,
        inode: u64,
    },
    Herdr {
        socket: String,
        device: u64,
        inode: u64,
    },
}
enum RouteCheck {
    Available(Route),
    Blocked(State),
}
impl Route {
    fn key(&self, actor: &Mailbox) -> Result<String> {
        Ok(serde_json::to_string(&(
            actor.binding_version,
            &actor.binding,
            self,
        ))?)
    }
}
async fn route(tx: &mut Transaction<'_, Sqlite>, actor: &Mailbox) -> Result<RouteCheck> {
    if actor.state == crate::states::AgentState::Retired {
        return Ok(RouteCheck::Blocked(State::Retired));
    }
    let r=sqlx::query!("SELECT g.paused,g.auto_prompt,g.socket AS herdr_socket,p.enabled,w.socket,w.thread,w.runtime,h.launch,h.client_session,i.token,i.socket_identity FROM mailboxes b JOIN groups g ON g.name=b.group_name LEFT JOIN runtime_policy p ON p.recipient=b.id AND p.binding_version=b.binding_version LEFT JOIN runtime_wakes w ON w.recipient=b.id AND w.binding_version=b.binding_version LEFT JOIN runtime_readiness h ON h.recipient=b.id AND h.binding_version=b.binding_version LEFT JOIN claude_inboxes i ON i.recipient=w.recipient WHERE b.id=? AND b.binding_version=?",actor.id,actor.binding_version).fetch_optional(&mut **tx).await?.context("agent binding changed")?;
    if r.paused != 0 || r.enabled == Some(0) {
        return Ok(RouteCheck::Blocked(State::Paused));
    }
    match actor.binding {
        Binding::Remote { .. } => Ok(RouteCheck::Blocked(State::RemoteUnsupported)),
        Binding::Herdr(_) => {
            if r.auto_prompt == 0 {
                return Ok(RouteCheck::Blocked(State::NotifyOnly));
            }
            let Ok(meta) = std::fs::metadata(&r.herdr_socket) else {
                return Ok(RouteCheck::Blocked(State::Unavailable));
            };
            Ok(RouteCheck::Available(Route::Herdr {
                socket: r.herdr_socket,
                device: meta.dev(),
                inode: meta.ino(),
            }))
        }
        Binding::Standalone { .. } => {
            let (Some(socket), Some(thread), Some(runtime)) = (r.socket, r.thread, r.runtime)
            else {
                return Ok(RouteCheck::Blocked(State::MissingEndpoint));
            };
            if r.launch.is_some() && r.client_session.as_deref() != Some(thread.as_str()) {
                return Ok(RouteCheck::Blocked(State::MissingEndpoint));
            }
            let Ok(meta) = std::fs::metadata(&socket) else {
                return Ok(RouteCheck::Blocked(State::Unavailable));
            };
            Ok(RouteCheck::Available(Route::Native {
                socket,
                thread,
                runtime: runtime.parse()?,
                launch: r.launch,
                inbox_token: r.token,
                inbox_identity: r.socket_identity,
                device: meta.dev(),
                inode: meta.ino(),
            }))
        }
    }
}
/// A trusted, bounded instruction delivered only through the selected runtime route.
pub(crate) fn challenge(actor: &Mailbox, nonce: &str) -> String {
    // Agent shells may rebuild PATH (for example, login zsh runs path_helper),
    // so the authenticated acknowledgment must use the same binary that sent it.
    let binary = std::env::current_exe()
        .ok()
        .and_then(|path| path.into_os_string().into_string().ok())
        .filter(|path| !path.contains('\n') && !path.contains('\r'))
        .map(|path| format!("'{}'", path.replace('\'', "'\\''")))
        .unwrap_or_else(|| "agent-mail".into());
    format!(
        "Agent Mail delivery check: run `{binary} --group {} agent ack {nonce}` once as this agent. Ack is delivery only; fetch records and act.",
        actor.group_name,
    )
}
impl Store {
    /// Inspect current delivery readiness without exposing challenge nonces.
    pub async fn delivery_status(&self, actor: &Mailbox, now: i64) -> Result<DeliveryStatus> {
        let current = self.mailbox(&actor.group_name, &actor.name).await?;
        ensure!(
            current.binding_version == actor.binding_version,
            "agent binding changed"
        );
        let mut tx = self.pool().begin().await?;
        let mut status = DeliveryStatus::new(&current, State::Unverified);
        let route = match route(&mut tx, &current).await? {
            RouteCheck::Available(route) => route,
            RouteCheck::Blocked(state) => {
                status.state = state;
                return Ok(status.finish());
            }
        };
        let key = route.key(&current)?;
        if let Some(p)=sqlx::query!("SELECT deadline,next_attempt,attempts,transport_accepted_at,runtime_received_at,acknowledged_at,healthy_at,failed FROM delivery_probes WHERE recipient=? AND binding_version=? AND route_key=?",current.id,current.binding_version,key).fetch_optional(&mut *tx).await? {
            status.deadline=Some(p.deadline);
            status.next_attempt_at=(p.acknowledged_at.is_none() && p.attempts<3 && now<p.deadline).then_some(p.next_attempt.max(now));
            status.attempts=p.attempts;status.acknowledged_at=p.acknowledged_at;status.agent_acknowledged=p.acknowledged_at.is_some();status.runtime_received_at=p.runtime_received_at;status.transport_accepted_at=p.transport_accepted_at;status.checked_at=p.healthy_at;
            status.state=if p.failed!=0 {State::Unavailable} else if p.acknowledged_at.is_some() {
                if p.healthy_at.is_some_and(|t|t<=now && now-t<=30){State::Verified}else{State::Unavailable}
            } else if now>=p.deadline{State::Expired}else{State::Verifying};
        }
        if !crate::service::running(self.root()) {
            status.state = State::WorkerStopped;
        }
        Ok(status.finish())
    }
    /// Read readiness for a group's recipients; no challenge values are returned.
    pub async fn delivery_statuses(
        &self,
        group: Option<&str>,
        now: i64,
    ) -> Result<Vec<DeliveryStatus>> {
        let mut result = Vec::new();
        for g in self
            .groups()
            .await?
            .into_iter()
            .filter(|g| group.is_none_or(|s| s == g.name))
        {
            for agent in self.participants(&g.name).await? {
                let actor = self.mailbox(&g.name, &agent.name).await?;
                result.push(self.delivery_status(&actor, now).await?);
            }
        }
        Ok(result)
    }
    async fn ensure_probe(&self, actor: &Mailbox, now: i64) -> Result<Option<(Route, String)>> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let RouteCheck::Available(route) = route(&mut tx, actor).await? else {
            return Ok(None);
        };
        let key = route.key(actor)?;
        let nonce = Uuid::new_v4().to_string();
        let deadline = now + 180;
        sqlx::query!("INSERT INTO delivery_probes(recipient,binding_version,route_key,nonce,created,deadline) VALUES(?,?,?,?,?,?) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,route_key=excluded.route_key,nonce=excluded.nonce,created=excluded.created,deadline=excluded.deadline,attempts=0,next_attempt=0,transport_accepted_at=NULL,runtime_received_at=NULL,acknowledged_at=NULL,healthy_at=NULL,failed=0 WHERE delivery_probes.route_key<>excluded.route_key",actor.id,actor.binding_version,key,nonce,now,deadline).execute(&mut *tx).await?;
        let row = sqlx::query!(
            "SELECT nonce FROM delivery_probes WHERE recipient=?",
            actor.id
        )
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some((route, row.nonce)))
    }
    pub(crate) async fn probe_current(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        nonce: &str,
        now: i64,
    ) -> Result<bool> {
        Self::check_actor(tx, actor).await?;
        let RouteCheck::Available(route) = route(tx, actor).await? else {
            return Ok(false);
        };
        let key = route.key(actor)?;
        Ok(sqlx::query!("SELECT recipient FROM delivery_probes WHERE recipient=? AND binding_version=? AND route_key=? AND nonce=? AND deadline>?",actor.id,actor.binding_version,key,nonce,now).fetch_optional(&mut **tx).await?.is_some())
    }
    pub(crate) async fn reserve_probe(
        &self,
        actor: &Mailbox,
        nonce: &str,
        now: i64,
    ) -> Result<bool> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if !Self::probe_current(&mut tx, actor, nonce, now).await? {
            return Ok(false);
        }
        // Work and verification share the wake channel. If a real notification
        // is waiting, let its delivery carry the challenge instead of opening
        // a second turn for the same recipient.
        let pending = sqlx::query!(
            "SELECT recipient FROM wake_events WHERE recipient=? LIMIT 1",
            actor.id
        )
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        if pending {
            tx.commit().await?;
            return Ok(false);
        }
        let next = now + 60;
        let count=sqlx::query!("UPDATE delivery_probes SET attempts=attempts+1,next_attempt=? WHERE recipient=? AND nonce=? AND attempts<3 AND acknowledged_at IS NULL AND next_attempt<=?",next,actor.id,nonce,now).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(count == 1)
    }
    /// Reserve the first challenge for an actionable notification.
    pub(crate) async fn reserve_delivery_challenge(
        &self,
        actor: &Mailbox,
        now: i64,
    ) -> Result<Option<String>> {
        let Some((_route, nonce)) = self.ensure_probe(actor, now).await? else {
            return Ok(None);
        };
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if !Self::probe_current(&mut tx, actor, &nonce, now).await? {
            tx.commit().await?;
            return Ok(None);
        }
        let next = now + 60;
        let reserved = sqlx::query!(
            "UPDATE delivery_probes SET attempts=attempts+1,next_attempt=? WHERE recipient=? AND binding_version=? AND nonce=? AND attempts<3 AND transport_accepted_at IS NULL AND acknowledged_at IS NULL AND deadline>?",
            next,
            actor.id,
            actor.binding_version,
            nonce,
            now
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok((reserved == 1).then_some(nonce))
    }
    /// Return a challenge for the exact Claude notification receipt, if one was
    /// accepted by the current runtime but has not yet been observed by its hook.
    pub(crate) async fn unobserved_challenge(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        now: i64,
    ) -> Result<Option<String>> {
        Ok(sqlx::query!(
            "SELECT nonce FROM delivery_probes WHERE recipient=? AND binding_version=? AND attempts>0 AND transport_accepted_at IS NOT NULL AND runtime_received_at IS NULL AND acknowledged_at IS NULL AND deadline>?",
            actor.id,
            actor.binding_version,
            now
        )
        .fetch_optional(&mut **tx)
        .await?
        .map(|row| row.nonce))
    }
    /// Record a challenge carried by a normal notification after its transport
    /// accepted the message. Failure here must not change business delivery.
    pub(crate) async fn record_delivery_challenge(
        &self,
        actor: &Mailbox,
        nonce: &str,
        now: i64,
    ) -> Result<()> {
        sqlx::query!(
            "UPDATE delivery_probes SET transport_accepted_at=COALESCE(transport_accepted_at,?),healthy_at=?,failed=0 WHERE recipient=? AND binding_version=? AND nonce=?",
            now,
            now,
            actor.id,
            actor.binding_version,
            nonce
        )
        .execute(self.pool())
        .await?;
        Ok(())
    }
    /// Record an agent's explicit response; never called automatically from hooks.
    pub async fn acknowledge_delivery(&self, actor: &Mailbox, nonce: Uuid, now: i64) -> Result<()> {
        let nonce = nonce.to_string();
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let RouteCheck::Available(route) = route(&mut tx, actor).await? else {
            anyhow::bail!("delivery route is unavailable");
        };
        let key = route.key(actor)?;
        let row=sqlx::query!("SELECT deadline,attempts,acknowledged_at FROM delivery_probes WHERE recipient=? AND binding_version=? AND route_key=? AND nonce=?",actor.id,actor.binding_version,key,nonce).fetch_optional(&mut *tx).await?.context("challenge is not for this agent's current connection")?;
        if row.acknowledged_at.is_some() {
            return Ok(());
        }
        ensure!(
            row.deadline > now && row.attempts > 0,
            "challenge was not dispatched or has expired"
        );
        sqlx::query!(
            "UPDATE delivery_probes SET acknowledged_at=? WHERE recipient=? AND nonce=?",
            now,
            actor.id,
            nonce
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(())
    }
    /// Match a Claude notification hook, recording receipt but not agent acknowledgment.
    pub(crate) async fn probe_hook(
        &self,
        actor: &Mailbox,
        prompt: &str,
        now: i64,
    ) -> Result<Option<String>> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let p = sqlx::query!(
            "SELECT nonce,attempts FROM delivery_probes WHERE recipient=? AND binding_version=?",
            actor.id,
            actor.binding_version
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(p) = p else {
            return Ok(None);
        };
        if p.attempts == 0
            || prompt != crate::claude_inbox::notification(&p.nonce)
            || !Self::probe_current(&mut tx, actor, &p.nonce, now).await?
        {
            return Ok(None);
        }
        sqlx::query!("UPDATE delivery_probes SET runtime_received_at=COALESCE(runtime_received_at,?) WHERE recipient=? AND nonce=?",now,actor.id,p.nonce).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(challenge(actor, &p.nonce)))
    }
}
async fn healthy(store: &Store, actor: &Mailbox, route: &Route) -> Result<bool> {
    match route {
        Route::Native {
            socket,
            thread,
            runtime,
            ..
        } => {
            if let Some(inbox) = store.claude_inbox(actor).await? {
                crate::claude_inbox::verify(store, Path::new(socket), &inbox)?;
                // A crashed runtime can leave the socket inode behind. Opening a
                // connection proves a listener still exists without sending input.
                let _connection = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    tokio::net::UnixStream::connect(socket),
                )
                .await
                .context("Claude health connection timed out")??;
                return Ok(true);
            }
            let info = crate::native::probe(*runtime, Path::new(socket), thread.parse()?).await?;
            Ok(info["ready"] != false
                && info["state"] != "not_loaded"
                && info["state"] != "system_error")
        }
        Route::Herdr { socket, .. } => {
            if !crate::herdr::plugin_enabled(Path::new(socket)).await? {
                return Ok(false);
            }
            let binding = actor.binding.herdr().context("Herdr binding missing")?;
            Ok(crate::herdr::agent(Path::new(socket), &binding.pane)
                .await?
                .matches(actor))
        }
    }
}
async fn reconcile_one(store: &Store, actor: &Mailbox, now: i64) -> Result<()> {
    let Some((route, nonce)) = store.ensure_probe(actor, now).await? else {
        return Ok(());
    };
    let health = healthy(store, actor, &route).await;
    let available = matches!(&health, Ok(true));
    let checked = available.then_some(now);
    let failed = !available;
    sqlx::query!(
        "UPDATE delivery_probes SET healthy_at=?,failed=? WHERE recipient=? AND nonce=?",
        checked,
        failed,
        actor.id,
        nonce
    )
    .execute(store.pool())
    .await
    .with_context(|| match &health {
        Err(error) => format!("could not record route health failure: {error:#}"),
        Ok(_) => "could not record route health check".into(),
    })?;
    if !health.context("delivery route health check failed")? {
        return Ok(());
    }
    if sqlx::query!(
        "SELECT id FROM wake_events WHERE recipient=? LIMIT 1",
        actor.id
    )
    .fetch_optional(store.pool())
    .await?
    .is_some()
    {
        return Ok(());
    }
    let pending=sqlx::query!("SELECT recipient FROM delivery_probes WHERE recipient=? AND nonce=? AND acknowledged_at IS NULL AND deadline>? AND attempts<3 AND next_attempt<=?",actor.id,nonce,now,now).fetch_optional(store.pool()).await?.is_some();
    if !pending {
        return Ok(());
    }
    let result = match route {
        Route::Native { .. } => crate::native::send_verification(store, actor, &nonce, now).await,
        Route::Herdr { socket, .. } => {
            send_herdr(store, actor, &nonce, Path::new(&socket), now).await
        }
    };
    if let Err(error) = result {
        sqlx::query!(
            "UPDATE delivery_probes SET healthy_at=NULL,failed=1 WHERE recipient=? AND nonce=?",
            actor.id,
            nonce
        )
        .execute(store.pool())
        .await
        .with_context(|| format!("could not record delivery check failure: {error:#}"))?;
        return Err(error).context("delivery challenge dispatch failed");
    }
    Ok(())
}
async fn send_herdr(
    store: &Store,
    actor: &Mailbox,
    nonce: &str,
    socket: &Path,
    now: i64,
) -> Result<()> {
    let Some(_wake_lock) = crate::service::wake_lock(store.root(), actor.id)? else {
        return Ok(());
    };
    let binding = actor.binding.herdr().context("missing binding")?;
    let live = crate::herdr::agent(socket, &binding.pane).await?;
    if !live.matches(actor) || !live.ready() {
        return Ok(());
    }
    if !store.reserve_probe(actor, nonce, now).await? {
        return Ok(());
    }
    let operation = Operation::HerdrVerification;
    let mut tx = store.delivery_transaction(actor, operation).await?;
    ensure!(
        Store::probe_current(&mut tx, actor, nonce, now).await?,
        "connection changed"
    );
    store
        .diagnostics()
        .measure(operation, Phase::Transport, async {
            ensure!(
                crate::herdr::plugin_enabled(socket).await?,
                "plugin disabled"
            );
            crate::herdr::call(
                socket,
                "agent.prompt",
                serde_json::json!({"target":binding.pane,"text":challenge(actor,nonce)}),
            )
            .await?;
            Ok(())
        })
        .await?;
    sqlx::query!("UPDATE delivery_probes SET transport_accepted_at=COALESCE(transport_accepted_at,?) WHERE recipient=? AND nonce=?",now,actor.id,nonce).execute(&mut **tx).await?;
    tx.commit().await?;
    Ok(())
}
/// Bounded results of a verification scan; individual failures do not stop other checks.
#[derive(Debug, Serialize)]
pub struct ReconcileReport {
    /// Unix seconds when this scan was started.
    pub checked_at: i64,
    /// Number of registrations checked, including failures.
    pub checked: usize,
    /// Number of failed checks, including errors omitted from the detail list.
    pub failed: usize,
    /// At most eight contextual errors, each limited to 1 KiB of UTF-8 text.
    pub errors: Vec<String>,
}

/// Reconcile checks in a separate worker task. At most four endpoint probes run together.
///
/// # Errors
/// The registration list cannot be read. Individual failures are retained in the report.
pub async fn reconcile(store: &Store, now: i64) -> Result<ReconcileReport> {
    use futures_util::{StreamExt, stream};
    let rows=sqlx::query!("SELECT group_name,name FROM mailboxes WHERE agent_state='registered' AND remote_machine IS NULL").fetch_all(store.pool()).await?;
    let checks = stream::iter(rows)
        .map(|r| async move {
            async {
                let actor = store.mailbox(&r.group_name, &r.name).await?;
                reconcile_one(store, &actor, now).await
            }
            .await
            .with_context(|| format!("delivery check for {}/{} failed", r.group_name, r.name))
        })
        .buffer_unordered(4);
    tokio::pin!(checks);
    let mut report = ReconcileReport {
        checked_at: now,
        checked: 0,
        failed: 0,
        errors: Vec::new(),
    };
    while let Some(result) = checks.next().await {
        report.checked += 1;
        if let Err(error) = result {
            report.failed += 1;
            if report.errors.len() < 8 {
                let error = error_text(format!("{error:#}"));
                eprintln!("agent-mail: {error}");
                report.errors.push(error);
            }
        }
    }
    if report.failed > report.errors.len() {
        eprintln!(
            "agent-mail: {} additional delivery check failures omitted",
            report.failed - report.errors.len()
        );
    }
    Ok(report)
}

impl Store {
    /// Read one recipient status without allowing diagnostic failure to mask a committed write.
    pub async fn recipient_delivery_outcome(&self, group: &str, name: &str) -> serde_json::Value {
        let result = async {
            let actor = self.mailbox(group, name).await?;
            self.delivery_status(&actor, crate::now()?).await
        }
        .await;
        match result {
            Ok(status) => serde_json::json!(status),
            Err(_) => {
                serde_json::json!({"group":group,"agent":name,"ready":false,"state":State::Unknown,"next_action":"The write persisted but delivery diagnostics failed; inspect status"})
            }
        }
    }
    /// Project delivery evidence after a successful message write. Failure never masks persistence.
    pub async fn message_delivery_outcome(&self, group: &str, message: i64) -> serde_json::Value {
        match sqlx::query!("SELECT b.name FROM deliveries d JOIN mailboxes b ON b.id=d.recipient WHERE d.message=? AND b.group_name=? ORDER BY b.name",message,group).fetch_all(self.pool()).await {
            Ok(rows) => {
                let mut outcomes=Vec::new();
                for row in rows { outcomes.push(self.recipient_delivery_outcome(group,&row.name).await); }
                serde_json::json!(outcomes)
            }
            Err(_) => serde_json::json!({"ready":false,"state":State::Unknown,"next_action":"The write persisted but delivery diagnostics failed; inspect status"}),
        }
    }
    /// Report all recipients notified by this task revision, including a previous owner.
    pub async fn task_delivery_outcome(
        &self,
        group: &str,
        task: &str,
        version: i64,
    ) -> serde_json::Value {
        match sqlx::query!("SELECT DISTINCT b.name FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient WHERE b.group_name=? AND e.kind='work_changed' AND e.subject=? AND e.version=? ORDER BY b.name",group,task,version).fetch_all(self.pool()).await {
            Ok(rows) => {
                let mut outcomes=Vec::new();
                for row in rows { outcomes.push(self.recipient_delivery_outcome(group,&row.name).await); }
                serde_json::json!(outcomes)
            }
            Err(_) => serde_json::json!({"ready":false,"state":State::Unknown,"next_action":"The write persisted but delivery diagnostics failed; inspect status"}),
        }
    }
}
