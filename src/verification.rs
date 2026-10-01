//! Bounded end-to-end challenges. Runtime receipts cannot acknowledge for the agent.
use crate::{
    identity::Binding,
    states::{DeliveryReadiness as State, NativeRuntime},
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sqlx::{Sqlite, Transaction};
use std::{os::unix::fs::MetadataExt, path::Path};
use uuid::Uuid;

/// Maximum wait for an eligible first exposure in one unchanged verification episode.
pub const DISCOVERY_SECONDS: i64 = 900;
/// Acknowledgment window begins once, at the first durable exposure reservation.
pub const ACK_SECONDS: i64 = 180;
const RETRY_SECONDS: i64 = 60;

/// Verification timing phase; transport health and business completion remain separate.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationPhase {
    /// The original episode is waiting for an eligible first send.
    WaitingForExposure,
    /// At least one exposure occurred and the original ACK window is open.
    AwaitingAcknowledgment,
    /// No eligible send occurred within the finite discovery horizon.
    DiscoveryExhausted,
    /// The first-exposure ACK horizon expired.
    AcknowledgmentExpired,
    /// The agent explicitly acknowledged this connection.
    Acknowledged,
}

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
    /// Finite verification phase, when a current episode exists.
    pub verification_phase: Option<VerificationPhase>,
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
            verification_phase: None,
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
        if matches!(
            self.state,
            State::Unverified | State::Verifying | State::Expired
        ) {
            self.next_action = match self.verification_phase {
                Some(VerificationPhase::WaitingForExposure) => Some("Waiting for an eligible idle delivery; the acknowledgment window has not started".into()),
                Some(VerificationPhase::DiscoveryExhausted) => Some("No eligible delivery occurred within the discovery horizon; inspect the endpoint and permissions before explicitly retrying verification".into()),
                _ => self.next_action,
            };
        }
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
        if let Some(p)=sqlx::query!("SELECT created,deadline,next_attempt,attempts,transport_accepted_at,runtime_received_at,acknowledged_at,healthy_at,failed FROM delivery_probes WHERE recipient=? AND binding_version=? AND route_key=?",current.id,current.binding_version,key).fetch_optional(&mut *tx).await? {
            let deadline = if p.attempts == 0 { p.created.checked_add(DISCOVERY_SECONDS).context("verification clock overflow")? } else { p.deadline };
            status.deadline=Some(deadline);
            status.next_attempt_at=(p.acknowledged_at.is_none() && p.attempts<3 && now<deadline).then_some(p.next_attempt.max(now));
            status.verification_phase=Some(if p.acknowledged_at.is_some() { VerificationPhase::Acknowledged }
                else if p.attempts == 0 { if now >= deadline { VerificationPhase::DiscoveryExhausted } else { VerificationPhase::WaitingForExposure } }
                else if now >= deadline { VerificationPhase::AcknowledgmentExpired } else { VerificationPhase::AwaitingAcknowledgment });
            status.attempts=p.attempts;status.acknowledged_at=p.acknowledged_at;status.agent_acknowledged=p.acknowledged_at.is_some();status.runtime_received_at=p.runtime_received_at;status.transport_accepted_at=p.transport_accepted_at;status.checked_at=p.healthy_at;
            status.state=if p.failed!=0 {State::Unavailable} else if p.acknowledged_at.is_some() {
                if p.healthy_at.is_some_and(|t|t<=now && now-t<=30){State::Verified}else{State::Unavailable}
            } else if now>=deadline{State::Expired}else{State::Verifying};
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
        let probe = Self::ensure_probe_tx(&mut tx, actor, now).await?;
        tx.commit().await?;
        Ok(probe)
    }
    async fn ensure_probe_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        now: i64,
    ) -> Result<Option<(Route, String)>> {
        let RouteCheck::Available(route) = route(tx, actor).await? else {
            return Ok(None);
        };
        let key = route.key(actor)?;
        let nonce = Uuid::new_v4().to_string();
        let deadline = now
            .checked_add(DISCOVERY_SECONDS)
            .context("verification clock overflow")?;
        sqlx::query!("INSERT INTO delivery_probes(recipient,binding_version,route_key,nonce,created,deadline) VALUES(?,?,?,?,?,?) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,route_key=excluded.route_key,nonce=excluded.nonce,created=excluded.created,deadline=excluded.deadline,attempts=0,next_attempt=0,transport_accepted_at=NULL,runtime_received_at=NULL,acknowledged_at=NULL,healthy_at=NULL,failed=0 WHERE delivery_probes.route_key<>excluded.route_key",actor.id,actor.binding_version,key,nonce,now,deadline).execute(&mut **tx).await?;
        let row = sqlx::query!(
            "SELECT nonce FROM delivery_probes WHERE recipient=?",
            actor.id
        )
        .fetch_one(&mut **tx)
        .await?;
        Ok(Some((route, row.nonce)))
    }
    pub(crate) async fn delivery_route_key_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
    ) -> Result<Option<String>> {
        Self::check_actor(tx, actor).await?;
        match route(tx, actor).await? {
            RouteCheck::Available(route) => Ok(Some(route.key(actor)?)),
            RouteCheck::Blocked(_) => Ok(None),
        }
    }
    pub(crate) async fn probe_current(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        nonce: &str,
        now: i64,
    ) -> Result<bool> {
        let Some(key) = Self::delivery_route_key_tx(tx, actor).await? else {
            return Ok(false);
        };
        Ok(sqlx::query!("SELECT recipient FROM delivery_probes WHERE recipient=? AND binding_version=? AND route_key=? AND nonce=? AND ((attempts=0 AND created+?>?) OR (attempts>0 AND deadline>?))",actor.id,actor.binding_version,key,nonce,DISCOVERY_SECONDS,now,now).fetch_optional(&mut **tx).await?.is_some())
    }
    /// A currently eligible normal wake may carry the challenge. Retrieved business
    /// work, exhausted normal delivery and its cooldown do not reserve the endpoint.
    async fn normal_delivery_due_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        now: i64,
    ) -> Result<bool> {
        let due = match actor.binding {
            Binding::Herdr(_) => sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM mailboxes b WHERE b.id=? AND b.binding_version=? AND b.next_wake<=? AND EXISTS(SELECT 1 FROM herdr_wake_events e WHERE e.recipient=b.id AND (b.attempts<3 OR e.id>b.wake_attempted)))")
                .bind(actor.id).bind(actor.binding_version).bind(now).fetch_one(&mut **tx).await?,
            Binding::Standalone { .. } => sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM runtime_wakes w WHERE w.recipient=? AND w.binding_version=? AND EXISTS(SELECT 1 FROM wake_events e WHERE e.recipient=w.recipient AND e.id>w.scanned) AND (w.attempted<>(SELECT max(id) FROM coordination_events WHERE recipient=w.recipient) OR (w.attempts<3 AND w.next_attempt<=?)))")
                .bind(actor.id).bind(actor.binding_version).bind(now).fetch_one(&mut **tx).await?,
            Binding::Remote { .. } => false,
        };
        Ok(due)
    }
    async fn reserve_challenge_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        nonce: &str,
        now: i64,
    ) -> Result<bool> {
        if !Self::probe_current(tx, actor, nonce, now).await? {
            return Ok(false);
        }
        let next = now
            .checked_add(RETRY_SECONDS)
            .context("verification clock overflow")?;
        let deadline = now
            .checked_add(ACK_SECONDS)
            .context("verification clock overflow")?;
        let changed = sqlx::query!("UPDATE delivery_probes SET deadline=CASE WHEN attempts=0 THEN ? ELSE deadline END,attempts=attempts+1,next_attempt=? WHERE recipient=? AND binding_version=? AND nonce=? AND attempts<3 AND acknowledged_at IS NULL AND next_attempt<=?",deadline,next,actor.id,actor.binding_version,nonce,now).execute(&mut **tx).await?.rows_affected();
        Ok(changed == 1)
    }
    pub(crate) async fn reserve_probe(
        &self,
        actor: &Mailbox,
        nonce: &str,
        now: i64,
    ) -> Result<bool> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if Self::normal_delivery_due_tx(&mut tx, actor, now).await? {
            tx.commit().await?;
            return Ok(false);
        }
        let reserved = Self::reserve_challenge_tx(&mut tx, actor, nonce, now).await?;
        tx.commit().await?;
        Ok(reserved)
    }
    /// Compose normal-wake reservation, challenge exposure and immutable payload in one tx.
    pub(crate) async fn reserve_delivery_challenge_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        now: i64,
    ) -> Result<Option<String>> {
        let Some((_, nonce)) = Self::ensure_probe_tx(tx, actor, now).await? else {
            return Ok(None);
        };
        Ok(Self::reserve_challenge_tx(tx, actor, &nonce, now)
            .await?
            .then_some(nonce))
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
    /// A late send result may update only its still-current route and challenge.
    pub(crate) async fn record_delivery_challenge_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        nonce: &str,
        now: i64,
    ) -> Result<()> {
        ensure!(
            Self::probe_current(tx, actor, nonce, now).await?,
            "delivery connection changed after send"
        );
        sqlx::query!("UPDATE delivery_probes SET transport_accepted_at=COALESCE(transport_accepted_at,?),healthy_at=?,failed=0 WHERE recipient=? AND binding_version=? AND nonce=?",now,now,actor.id,actor.binding_version,nonce).execute(&mut **tx).await?;
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
    /// Match a Claude notification hook under the caller's fenced writer reservation.
    /// Receipt and its input boundary commit together; agent acknowledgment remains separate.
    pub(crate) async fn probe_hook_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        prompt: &str,
        now: i64,
    ) -> Result<Option<String>> {
        let p = sqlx::query!(
            "SELECT nonce,attempts FROM delivery_probes WHERE recipient=? AND binding_version=?",
            actor.id,
            actor.binding_version
        )
        .fetch_optional(&mut **tx)
        .await?;
        let Some(p) = p else {
            return Ok(None);
        };
        if p.attempts == 0
            || prompt != crate::claude_inbox::notification(&p.nonce)
            || !Self::probe_current(tx, actor, &p.nonce, now).await?
        {
            return Ok(None);
        }
        sqlx::query!("UPDATE delivery_probes SET runtime_received_at=COALESCE(runtime_received_at,?) WHERE recipient=? AND nonce=?",now,actor.id,p.nonce).execute(&mut **tx).await?;
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
    let health = healthy(store, actor, &route).await.unwrap_or(false);
    let checked = health.then_some(now);
    let failed = !health;
    sqlx::query!(
        "UPDATE delivery_probes SET healthy_at=?,failed=? WHERE recipient=? AND nonce=?",
        checked,
        failed,
        actor.id,
        nonce
    )
    .execute(store.pool())
    .await?;
    if !health {
        return Ok(());
    }
    let pending=sqlx::query!("SELECT recipient FROM delivery_probes WHERE recipient=? AND nonce=? AND acknowledged_at IS NULL AND ((attempts=0 AND created+?>?) OR (attempts>0 AND deadline>?)) AND attempts<3 AND next_attempt<=?",actor.id,nonce,DISCOVERY_SECONDS,now,now,now).fetch_optional(store.pool()).await?.is_some();
    if !pending {
        return Ok(());
    }
    let result = match route {
        Route::Native { .. } => crate::native::send_verification(store, actor, &nonce, now).await,
        Route::Herdr { socket, .. } => {
            send_herdr(store, actor, &nonce, Path::new(&socket), now).await
        }
    };
    if result.is_err() {
        sqlx::query!(
            "UPDATE delivery_probes SET healthy_at=NULL,failed=1 WHERE recipient=? AND nonce=?",
            actor.id,
            nonce
        )
        .execute(store.pool())
        .await?;
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
    let binding = actor.binding.herdr().context("missing binding")?;
    let live = crate::herdr::agent(socket, &binding.pane).await?;
    if !live.matches(actor) || !live.ready() {
        return Ok(());
    }
    if !store.reserve_probe(actor, nonce, now).await? {
        return Ok(());
    }
    let mut tx = store.pool().begin().await?;
    Store::lock_actor(&mut tx, actor).await?;
    ensure!(
        Store::probe_current(&mut tx, actor, nonce, now).await?,
        "connection changed"
    );
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
    Store::record_delivery_challenge_tx(&mut tx, actor, nonce, now).await?;
    tx.commit().await?;
    Ok(())
}
/// Reconcile checks without blocking domain delivery. At most four endpoint probes run together.
pub async fn reconcile(store: &Store, now: i64) -> Result<()> {
    use futures_util::{StreamExt, stream};
    let rows=sqlx::query!("SELECT group_name,name FROM mailboxes WHERE agent_state='registered' AND remote_machine IS NULL").fetch_all(store.pool()).await?;
    stream::iter(rows)
        .for_each_concurrent(4, |r| async move {
            if let Ok(actor) = store.mailbox(&r.group_name, &r.name).await {
                if let Err(error) = reconcile_one(store, &actor, now).await {
                    eprintln!(
                        "agent-mail: delivery check for {}/{} failed: {error}",
                        r.group_name, r.name
                    );
                }
            }
        })
        .await;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        followup::{Mode, PolicyPatch},
        herdr::{Agent, AgentStatus, Session},
        states::{SessionKind, TaskState},
        work::WorkDraft,
    };

    // Explicit transport fixture. It supplies no native execution or agent ACK evidence.
    async fn fixture() -> Result<(tempfile::TempDir, tokio::net::UnixListener, Store, Mailbox)> {
        let temp = tempfile::Builder::new()
            .prefix("verify-tx-")
            .tempdir_in("/tmp")?;
        let socket = temp.path().join("h.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let store = Store::open(&temp.path().join("state"), true).await?;
        store.enroll("g", Some(&socket)).await?;
        store.set_auto_prompt("g", true).await?;
        store
            .patch_followups(
                "g",
                &PolicyPatch {
                    mode: Some(Mode::Observe),
                    ..Default::default()
                },
                1000,
            )
            .await?;
        let agent = Agent {
            pane_id: "w1:p1".into(),
            terminal_id: "fixture".into(),
            agent: Some("codex".into()),
            agent_session: Some(Session {
                agent: "codex".into(),
                kind: SessionKind::Id,
                value: "fixture-session".into(),
            }),
            agent_status: AgentStatus::Idle,
            interactive_ready: Some(true),
            launch_pending: false,
            cwd: None,
        };
        store.bind("g", "worker", &agent, false).await?;
        let actor = store.mailbox("g", "worker").await?;
        Ok((temp, listener, store, actor))
    }

    async fn assign(store: &Store, actor: &Mailbox) -> Result<()> {
        store
            .work_create(
                actor,
                WorkDraft {
                    id: "work".into(),
                    scope: "Inspect fixture".into(),
                    owner: actor.name.clone(),
                    state: TaskState::Active,
                    next_action: "Inspect".into(),
                    deadline: None,
                    evidence: vec![],
                },
                1000,
            )
            .await?;
        Ok(())
    }

    async fn reserve_pair(store: &Store, actor: &Mailbox) -> Result<bool> {
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, actor).await?;
        let reserved = Store::reserve_tx(&mut tx, actor, 1000).await?;
        if reserved {
            ensure!(
                Store::reserve_delivery_challenge_tx(&mut tx, actor, 1000)
                    .await?
                    .is_some(),
                "missing fixture challenge"
            );
        }
        tx.commit().await?;
        Ok(reserved)
    }

    #[tokio::test]
    async fn wake_and_challenge_reservations_roll_back_together() -> Result<()> {
        let (_temp, _listener, store, actor) = fixture().await?;
        assign(&store, &actor).await?;
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        assert!(Store::reserve_tx(&mut tx, &actor, 1000).await?);
        assert!(
            Store::reserve_delivery_challenge_tx(&mut tx, &actor, 1000)
                .await?
                .is_some()
        );
        tx.rollback().await?;
        assert_eq!(store.mailbox("g", "worker").await?.attempts, 0);
        let probes: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_probes")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(probes, 0);
        assert!(reserve_pair(&store, &actor).await?);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_normal_reservations_expose_one_challenge() -> Result<()> {
        let (_temp, _listener, store, actor) = fixture().await?;
        assign(&store, &actor).await?;
        let (a, b) = tokio::join!(reserve_pair(&store, &actor), reserve_pair(&store, &actor));
        assert_ne!(a?, b?);
        assert_eq!(store.mailbox("g", "worker").await?.attempts, 1);
        let (attempts, deadline): (i64, i64) =
            sqlx::query_as("SELECT attempts,deadline FROM delivery_probes WHERE recipient=?")
                .bind(actor.id)
                .fetch_one(store.pool())
                .await?;
        assert_eq!((attempts, deadline), (1, 1180));
        Ok(())
    }

    #[tokio::test]
    async fn discovery_and_exposure_horizons_do_not_refresh_on_reads_or_retries() -> Result<()> {
        let (_temp, _listener, store, actor) = fixture().await?;
        let (_, nonce) = store.ensure_probe(&actor, 1000).await?.unwrap();
        assert_eq!(store.ensure_probe(&actor, 1700).await?.unwrap().1, nonce);
        assert!(store.reserve_probe(&actor, &nonce, 1800).await?);
        assert!(!store.reserve_probe(&actor, &nonce, 1859).await?);
        assert!(store.reserve_probe(&actor, &nonce, 1860).await?);
        assert!(store.reserve_probe(&actor, &nonce, 1920).await?);
        assert!(!store.reserve_probe(&actor, &nonce, 1980).await?);
        let (created, deadline, attempts): (i64, i64, i64) = sqlx::query_as(
            "SELECT created,deadline,attempts FROM delivery_probes WHERE recipient=?",
        )
        .bind(actor.id)
        .fetch_one(store.pool())
        .await?;
        assert_eq!((created, deadline, attempts), (1000, 1980, 3));
        Ok(())
    }

    #[tokio::test]
    async fn undispatched_discovery_exhaustion_stays_visible_without_a_reset() -> Result<()> {
        let (_temp, _listener, store, actor) = fixture().await?;
        let (_, nonce) = store.ensure_probe(&actor, 1000).await?.unwrap();
        assert!(!store.reserve_probe(&actor, &nonce, 1900).await?);
        assert_eq!(store.ensure_probe(&actor, 2000).await?.unwrap().1, nonce);
        let status = store.delivery_status(&actor, 2000).await?;
        assert!(matches!(
            status.verification_phase,
            Some(VerificationPhase::DiscoveryExhausted)
        ));
        assert_eq!(status.attempts, 0);
        assert!(!status.ready);
        Ok(())
    }

    #[tokio::test]
    async fn late_transport_result_cannot_cross_socket_replacement() -> Result<()> {
        let (temp, _listener, store, actor) = fixture().await?;
        let (_, nonce) = store.ensure_probe(&actor, 1000).await?.unwrap();
        assert!(store.reserve_probe(&actor, &nonce, 1000).await?);
        // Keep the original inode alive so immediate inode reuse cannot hide the change.
        let replacement_path = temp.path().join("replacement.sock");
        let _replacement = tokio::net::UnixListener::bind(&replacement_path)?;
        std::fs::rename(replacement_path, temp.path().join("h.sock"))?;
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        assert!(
            Store::record_delivery_challenge_tx(&mut tx, &actor, &nonce, 1001)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let accepted: Option<i64> = sqlx::query_scalar(
            "SELECT transport_accepted_at FROM delivery_probes WHERE recipient=?",
        )
        .bind(actor.id)
        .fetch_one(store.pool())
        .await?;
        assert_eq!(accepted, None);
        Ok(())
    }

    #[tokio::test]
    async fn late_transport_result_cannot_cross_session_rebinding() -> Result<()> {
        let (_temp, _listener, store, actor) = fixture().await?;
        let (_, nonce) = store.ensure_probe(&actor, 1000).await?.unwrap();
        assert!(store.reserve_probe(&actor, &nonce, 1000).await?);
        let replacement = Agent {
            pane_id: "w1:p1".into(),
            terminal_id: "fixture".into(),
            agent: Some("codex".into()),
            agent_session: Some(Session {
                agent: "codex".into(),
                kind: SessionKind::Id,
                value: "replacement-session".into(),
            }),
            agent_status: AgentStatus::Idle,
            interactive_ready: Some(true),
            launch_pending: false,
            cwd: None,
        };
        store.bind("g", "worker", &replacement, true).await?;
        let current = store.mailbox("g", "worker").await?;
        assert_ne!(current.binding_version, actor.binding_version);
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &current).await?;
        assert!(
            Store::record_delivery_challenge_tx(&mut tx, &actor, &nonce, 1001)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let accepted: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM delivery_probes WHERE transport_accepted_at IS NOT NULL",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(accepted, 0);
        Ok(())
    }
}
