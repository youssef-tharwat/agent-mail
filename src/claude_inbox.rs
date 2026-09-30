//! Native Claude inbox integration, owned by the existing Claude session.
//!
//! Startup hooks register the session's exported socket. Socket writes are only
//! attempts: a matching UserPromptSubmit hook confirms receipt and supplies fresh
//! bounded context. No model acknowledgment, terminal client or permission relay.
use crate::states::InboxActivity;
use crate::{
    identity::Binding,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::Path,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::UnixStream};
use uuid::Uuid;

const PREFIX: &str = "Agent Mail delivery ";

pub use crate::states::HookEvent as Event;

/// Native lifecycle input. Unknown client fields are intentionally ignored.
#[derive(Deserialize)]
pub struct Input {
    /// Native Claude session UUID, not a Mail credential.
    pub session_id: Uuid,
    /// The runtime lifecycle boundary.
    pub hook_event_name: Event,
    /// Submitted user text, present at UserPromptSubmit.
    #[serde(default)]
    pub prompt: String,
}

pub(crate) struct Endpoint {
    pub token: String,
    pub socket_identity: String,
    pub activity: InboxActivity,
}

fn socket_identity(path: &Path, owner: u32) -> Result<String> {
    ensure!(path.is_absolute(), "Claude inbox path must be absolute");
    let m = std::fs::symlink_metadata(path).context("inspect Claude inbox")?;
    ensure!(
        m.file_type().is_socket() && m.uid() == owner && m.mode() & 0o077 == 0,
        "Claude inbox must be a private socket owned by the Mail state owner"
    );
    Ok(format!("{}:{}", m.dev(), m.ino()))
}

impl Store {
    pub(crate) async fn claude_inbox(&self, actor: &Mailbox) -> Result<Option<Endpoint>> {
        Ok(sqlx::query!("SELECT token,socket_identity,activity AS 'activity: InboxActivity' FROM claude_inboxes i JOIN runtime_wakes w ON w.recipient=i.recipient WHERE i.recipient=? AND w.binding_version=? AND w.runtime='claude'", actor.id, actor.binding_version)
            .fetch_optional(self.pool()).await?.map(|r| Endpoint {token:r.token,socket_identity:r.socket_identity,activity:r.activity}))
    }

    /// Register or observe a native Claude session through its trusted hook.
    /// Returns fresh context only for a correlated delivery, otherwise normal
    /// recovery hooks remain responsible for context injection.
    ///
    /// # Errors
    /// Identity, socket ownership, lifecycle input or persistence is invalid.
    pub async fn claude_inbox_hook(
        &self,
        actor: &Mailbox,
        input: &Input,
        socket: &Path,
        token: &str,
    ) -> Result<Option<Value>> {
        ensure!(
            matches!(actor.binding, Binding::Standalone { .. }),
            "Claude inbox requires a standalone Mail identity"
        );
        if !self.runtime_enabled(actor).await? {
            return Ok(None);
        }
        ensure!(
            !token.is_empty() && token.len() <= 4096,
            "missing or invalid Claude messaging token"
        );
        // Claude can unlink its inbox before running SessionEnd. Authenticate
        // against the stored endpoint without requiring a surviving socket.
        if input.hook_event_name == Event::SessionEnd {
            let socket = socket.to_str().context("Claude inbox path must be UTF-8")?;
            let session = input.session_id.to_string();
            let mut tx = self.pool().begin().await?;
            Self::lock_actor(&mut tx, actor).await?;
            let updated = sqlx::query!("UPDATE claude_inboxes SET activity='ended' WHERE recipient=? AND token=? AND EXISTS(SELECT 1 FROM runtime_wakes w WHERE w.recipient=claude_inboxes.recipient AND w.binding_version=? AND w.socket=? AND w.thread=? AND w.runtime='claude')", actor.id,token,actor.binding_version,socket,session).execute(&mut *tx).await?;
            ensure!(
                updated.rows_affected() == 1,
                "Claude inbox is not registered for this session"
            );
            tx.commit().await?;
            return Ok(None);
        }
        let identity = socket_identity(socket, std::fs::metadata(self.root())?.uid())?;
        let socket = socket.to_str().context("Claude inbox path must be UTF-8")?;
        let session = input.session_id.to_string();
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if sqlx::query!(
            "SELECT enabled FROM runtime_policy WHERE recipient=? AND binding_version=?",
            actor.id,
            actor.binding_version
        )
        .fetch_optional(&mut *tx)
        .await?
        .is_some_and(|r| r.enabled == 0)
        {
            return Ok(None);
        }
        if input.hook_event_name == Event::SessionStart {
            // Never silently steal a participant from another live session.
            let mut same_session = false;
            if let Some(old) = sqlx::query!(
                "SELECT socket,thread FROM runtime_wakes WHERE recipient=? AND binding_version=?",
                actor.id,
                actor.binding_version
            )
            .fetch_optional(&mut *tx)
            .await?
            {
                same_session = old.thread == session;
                ensure!(
                    old.thread == session || !Path::new(&old.socket).exists(),
                    "Mail participant already has a runtime session; detach it explicitly first"
                );
            }
            sqlx::query!("INSERT INTO runtime_wakes(recipient,binding_version,socket,thread,runtime) VALUES(?,?,?,?,'claude') ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,socket=excluded.socket,thread=excluded.thread,runtime='claude',scanned=0,delivered=0,attempted=0,attempts=0,next_attempt=0 WHERE runtime_wakes.binding_version<>excluded.binding_version OR runtime_wakes.thread<>excluded.thread OR runtime_wakes.runtime<>'claude'",actor.id,actor.binding_version,socket,session).execute(&mut *tx).await?;
            if !same_session {
                sqlx::query!("DELETE FROM claude_inboxes WHERE recipient=?", actor.id)
                    .execute(&mut *tx)
                    .await?;
            }
            // Same-session resume can change the process/socket, retaining retry state.
            sqlx::query!(
                "UPDATE runtime_wakes SET socket=? WHERE recipient=?",
                socket,
                actor.id
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query!("INSERT INTO claude_inboxes(recipient,token,socket_identity,activity) VALUES(?,?,?,'idle') ON CONFLICT(recipient) DO UPDATE SET token=excluded.token,socket_identity=excluded.socket_identity,activity='idle'",actor.id,token,identity).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(None);
        }
        let endpoint = sqlx::query!("SELECT i.pending_id,i.pending_event FROM claude_inboxes i JOIN runtime_wakes w ON w.recipient=i.recipient WHERE i.recipient=? AND w.binding_version=? AND w.socket=? AND w.thread=? AND i.token=? AND i.socket_identity=?",actor.id,actor.binding_version,socket,session,token,identity).fetch_optional(&mut *tx).await?.context("Claude inbox is not registered for this session; restart with its startup hook")?;
        let activity = match input.hook_event_name {
            Event::Stop | Event::StopFailure => InboxActivity::Idle,
            Event::SessionEnd => InboxActivity::Ended,
            _ => InboxActivity::Active,
        };
        let activity = activity.as_str();
        sqlx::query!(
            "UPDATE claude_inboxes SET activity=? WHERE recipient=?",
            activity,
            actor.id
        )
        .execute(&mut *tx)
        .await?;
        let receipt = input.hook_event_name == Event::UserPromptSubmit
            && endpoint
                .pending_id
                .as_deref()
                .is_some_and(|id| input.prompt == notification(id));
        let observed_at = crate::now()?;
        let challenge = if receipt {
            self.unobserved_challenge(&mut tx, actor, observed_at)
                .await?
        } else {
            None
        };
        let context = if receipt {
            Some(self.delivery_text(actor, challenge.as_deref(), 0).await?)
        } else {
            None
        };
        if receipt {
            let event = endpoint.pending_event;
            sqlx::query!("UPDATE runtime_wakes SET delivered=MAX(delivered,?),scanned=MAX(scanned,?),attempts=0,next_attempt=0 WHERE recipient=?",event,event,actor.id).execute(&mut *tx).await?;
            sqlx::query!("INSERT OR IGNORE INTO event_receipts(recipient,binding_version,event) SELECT recipient,?,id FROM coordination_events WHERE recipient=? AND id<=?",actor.binding_version,actor.id,event).execute(&mut *tx).await?;
            sqlx::query!(
                "UPDATE claude_inboxes SET pending_id=NULL,pending_event=0 WHERE recipient=?",
                actor.id
            )
            .execute(&mut *tx)
            .await?;
            if let Some(nonce) = challenge.as_deref() {
                sqlx::query!(
                    "UPDATE delivery_probes SET runtime_received_at=COALESCE(runtime_received_at,?) WHERE recipient=? AND binding_version=? AND nonce=?",
                    observed_at,
                    actor.id,
                    actor.binding_version,
                    nonce
                )
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        if receipt {
            Ok(Some(
                json!({"hookSpecificOutput":{"hookEventName":"UserPromptSubmit","additionalContext":context.context("missing receipt context")?}}),
            ))
        } else if input.hook_event_name == Event::UserPromptSubmit {
            Ok(self.probe_hook(actor, &input.prompt, crate::now()?).await?.map(|text| json!({"hookSpecificOutput":{"hookEventName":"UserPromptSubmit","additionalContext":text}})))
        } else {
            Ok(None)
        }
    }
}

pub(crate) fn notification(id: &str) -> String {
    format!("{PREFIX}{id}\nUse the current state supplied by the Agent Mail lifecycle hook.")
}

/// Check the registered socket identity without injecting a message.
pub(crate) fn verify(store: &Store, socket: &Path, endpoint: &Endpoint) -> Result<()> {
    ensure!(
        endpoint.activity != InboxActivity::Ended,
        "Claude session ended; resume it normally"
    );
    ensure!(
        socket_identity(socket, std::fs::metadata(store.root())?.uid())?
            == endpoint.socket_identity,
        "Claude inbox was replaced; wait for the new session's startup hook"
    );
    Ok(())
}

pub(crate) async fn send(
    socket: &Path,
    session: Uuid,
    token: &str,
    id: &str,
    active: bool,
) -> Result<()> {
    let auth = json!({"type":"auth","token":token});
    let message = json!({"type":"user","session_id":session,"priority":if active {"now"} else {"next"},"message":{"role":"user","content":notification(id)}});
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut io = UnixStream::connect(socket).await?;
        io.write_all(format!("{auth}\n{message}\n").as_bytes())
            .await?;
        io.shutdown().await
    })
    .await
    .context("Claude inbox write timed out")??;
    Ok(())
}
