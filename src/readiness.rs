//! Evidence of hook execution scoped to a launch and binding generation.
use crate::states::{HookEvent, NativeRuntime, RecoveryState};
use crate::store::{Mailbox, Store};
use anyhow::Result;
use serde::Serialize;

/// Hook evidence for the current binding and launch.
#[derive(Debug, Serialize)]
pub struct Readiness {
    /// Current evidence state.
    pub state: RecoveryState,
    /// Managed client, when a launch exists.
    pub runtime: Option<NativeRuntime>,
    /// Pinned native session.
    pub session: Option<String>,
    /// Last authenticated lifecycle boundary.
    pub last_hook: Option<HookEvent>,
    /// Evidence timestamp in Unix seconds.
    pub observed_at: Option<i64>,
    /// Hook execution never proves model consumption.
    pub model_consumption_confirmed: bool,
}

impl Store {
    /// Begin a fresh readiness observation; earlier launches cannot prove readiness.
    pub async fn begin_launch(
        &self,
        actor: &Mailbox,
        launch: &str,
        runtime: NativeRuntime,
    ) -> Result<()> {
        let runtime = runtime.as_str();
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("INSERT INTO runtime_readiness(recipient,binding_version,launch,runtime) VALUES(?,?,?,?) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,launch=excluded.launch,runtime=excluded.runtime,client_session=NULL,last_hook=NULL,observed_at=NULL", actor.id,actor.binding_version,launch,runtime).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Pin the sole loaded session of a launcher-owned backend without claiming hook execution.
    pub async fn bind_launch_session(
        &self,
        actor: &Mailbox,
        launch: &str,
        session: &str,
    ) -> Result<bool> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let changed = sqlx::query!("UPDATE runtime_readiness SET client_session=? WHERE recipient=? AND binding_version=? AND launch=? AND (client_session IS NULL OR client_session=?)",session,actor.id,actor.binding_version,launch,session).execute(&mut *tx).await?.rows_affected() == 1;
        tx.commit().await?;
        Ok(changed)
    }

    /// Record authenticated hook execution, without claiming model consumption.
    pub async fn observe_hook(
        &self,
        actor: &Mailbox,
        launch: &str,
        session: &str,
        event: HookEvent,
        now: i64,
    ) -> Result<bool> {
        let event = event.as_str();
        crate::bounded(session, 160, "client session")?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let updated = sqlx::query!("UPDATE runtime_readiness SET client_session=?,last_hook=?,observed_at=? WHERE recipient=? AND binding_version=? AND launch=? AND (client_session IS NULL OR client_session=?)",session,event,now,actor.id,actor.binding_version,launch,session).execute(&mut *tx).await?.rows_affected() == 1;
        tx.commit().await?;
        Ok(updated)
    }

    /// Read observed readiness for the current identity, including its evidence time.
    pub async fn launch_readiness(&self, actor: &Mailbox) -> Result<Readiness> {
        let row = sqlx::query!("SELECT runtime AS 'runtime: NativeRuntime',client_session,last_hook AS 'last_hook: HookEvent',observed_at FROM runtime_readiness WHERE recipient=? AND binding_version=?",actor.id,actor.binding_version).fetch_optional(self.pool()).await?;
        Ok(match row {
            Some(r) => Readiness {
                state: if r.observed_at.is_some() {
                    RecoveryState::HookObserved
                } else {
                    RecoveryState::AwaitingHook
                },
                runtime: Some(r.runtime),
                session: r.client_session,
                last_hook: r.last_hook,
                observed_at: r.observed_at,
                model_consumption_confirmed: false,
            },
            None => Readiness {
                state: RecoveryState::NotObserved,
                runtime: None,
                session: None,
                last_hook: None,
                observed_at: None,
                model_consumption_confirmed: false,
            },
        })
    }
}
