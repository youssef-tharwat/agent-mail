//! Durable participant identities and explicit runtime bindings.
//!
//! Standalone credentials identify registrations, not running processes. Replacing
//! a binding preserves its mailbox and invalidates previous actor snapshots.
//! [`Store::authenticate`] verifies the explicit credential or current Herdr identity;
//! invalid standalone credentials never fall back to environment-based identity.

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
    pub fn runtime(&self) -> &'static str {
        match self {
            Self::Herdr(_) => "herdr",
            Self::Standalone { .. } => "standalone",
            Self::Remote { .. } => "remote",
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
    pub runtime: &'static str,
    /// Herdr pane identifier associated with the registration, when applicable.
    pub pane: Option<String>,
    /// Observed availability; standalone registrations do not prove liveness.
    pub availability: &'static str,
}

impl Store {
    /// Register a standalone session and optionally rotate its credential.
    ///
    /// # Errors
    /// The group or name is invalid, replacement is disallowed, or persistence fails.
    pub async fn register(&self, group: &str, name: &str, replace: bool) -> Result<Uuid> {
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
        self.authenticate_herdr(group).await.context("supply AGENT_MAIL_SESSION for a registered standalone participant, or use a bound Herdr pane")
    }
}
