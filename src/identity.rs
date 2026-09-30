//! Durable participant identities and explicit runtime bindings.
//!
//! Standalone credentials identify registrations, not running processes. Replacing
//! a binding preserves its mailbox and invalidates previous actor snapshots.
//! [`Store::authenticate`] verifies the explicit credential or current Herdr identity;
//! invalid standalone credentials never fall back to environment-based identity.

use crate::states::{Availability, BindingKind};
use crate::{
    herdr::HerdrBinding,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The runtime association for one durable Mail address.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "runtime", rename_all = "snake_case")]
pub enum Binding {
    /// A participant bound to a verified Herdr identity.
    Herdr(HerdrBinding),
    /// A locally registered participant authenticated by a session credential.
    Standalone {
        /// Registration credential rotated by explicit replacement.
        session: Uuid,
    },
    /// A participant routed to another machine.
    Remote {
        /// UUID of the machine responsible for this participant.
        machine: Uuid,
    },
}

impl Binding {
    /// Return the stable serialization name of this binding’s runtime.
    pub fn runtime(&self) -> BindingKind {
        match self {
            Self::Herdr(_) => BindingKind::Herdr,
            Self::Standalone { .. } => BindingKind::Standalone,
            Self::Remote { .. } => BindingKind::Remote,
        }
    }
    /// Borrow the Herdr identity when this binding targets Herdr.
    pub fn herdr(&self) -> Option<&HerdrBinding> {
        match self {
            Self::Herdr(binding) => Some(binding),
            _ => None,
        }
    }
    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Herdr(a), Self::Herdr(b)) => {
                a.pane == b.pane
                    && a.terminal == b.terminal
                    && a.agent == b.agent
                    && a.session_kind == b.session_kind
                    && a.session_value == b.session_value
            }
            _ => self == other,
        }
    }
}

/// Public registry entry. Session credentials are deliberately excluded.
#[derive(Debug, Serialize)]
pub struct Participant {
    /// Persistent identifier for this record.
    pub id: i64,
    /// Name within the enclosing registration or group.
    pub name: String,
    /// Stable name of the participant’s runtime association.
    pub runtime: BindingKind,
    /// Herdr pane identifier associated with the registration, when applicable.
    pub pane: Option<String>,
    /// Observed availability; standalone registrations do not prove liveness.
    pub availability: Availability,
    /// Durable registration lifecycle.
    pub state: crate::states::AgentState,
    /// Registration version.
    pub version: i64,
    /// Last registration change.
    pub updated: i64,
}

impl Store {
    /// Register a standalone session and optionally rotate its credential.
    ///
    /// # Errors
    /// The group or name is invalid, replacement is disallowed, or persistence fails.
    pub async fn register(&self, group: &str, name: &str, replace: bool) -> Result<Uuid> {
        if replace {
            self.mailbox(group, name)
                .await
                .context("agent does not exist; use agent add NAME first")?;
        }
        let session = Uuid::new_v4();
        self.set_binding(group, name, &Binding::Standalone { session }, replace)
            .await?;
        Ok(session)
    }

    /// Authenticate a standalone credential or the current Herdr pane.
    /// An invalid standalone credential never falls back to Herdr identity.
    ///
    /// # Errors
    /// The credential is stale, required environment is absent, live identity differs, or I/O fails.
    pub async fn authenticate(&self, group: &str, session: Option<&Uuid>) -> Result<Mailbox> {
        if let Some(session) = session {
            return self.standalone_caller(group, session).await;
        }
        self.authenticate_herdr(group)
            .await
            .context("launch with agent-mail run NAME -- COMMAND, or use a bound Herdr pane")
    }
}

impl Store {
    /// Select an explicit group, an authenticated identity's group, or the sole group.
    ///
    /// # Errors
    /// Selection is ambiguous, a supplied identity is invalid, or storage fails.
    pub async fn select_group(
        &self,
        requested: Option<&str>,
        session: Option<&Uuid>,
        agent: bool,
    ) -> Result<String> {
        let groups = self.groups().await?;
        if let Some(group) = requested {
            self.group(group).await?;
            if session.is_some() {
                self.authenticate(group, session).await?;
            }
            return Ok(group.into());
        }
        if let Some(session) = session {
            let credential = session.to_string();
            let rows = sqlx::query!(
                "SELECT group_name FROM mailboxes WHERE standalone_session=?",
                credential
            )
            .fetch_all(self.pool())
            .await?;
            anyhow::ensure!(
                !rows.is_empty(),
                "standalone credential is unknown or replaced; obtain the current credential from the operator"
            );
            anyhow::ensure!(
                rows.len() == 1,
                "credential matches multiple groups; select --group explicitly"
            );
            return Ok(rows[0].group_name.clone());
        }
        if agent && std::env::var("HERDR_ENV").as_deref() == Ok("1") {
            let mut matches = Vec::new();
            for group in &groups {
                if self.authenticate_herdr(&group.name).await.is_ok() {
                    matches.push(group.name.clone());
                }
            }
            if matches.is_empty() && groups.len() == 1 {
                return Ok(groups[0].name.clone());
            }
            anyhow::ensure!(
                matches.len() == 1,
                "Herdr identity has no unique verified group; select --group and verify its participant binding"
            );
            return Ok(matches.remove(0));
        }
        anyhow::ensure!(
            !groups.is_empty(),
            "no groups configured; run agent-mail init GROUP"
        );
        anyhow::ensure!(
            groups.len() == 1,
            "multiple groups configured ({}); select --group GROUP or AGENT_MAIL_GROUP",
            groups
                .iter()
                .map(|g| g.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(groups[0].name.clone())
    }
}
