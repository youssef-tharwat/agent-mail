//! Durable participant identities and explicit runtime bindings.
//!
//! Standalone session credentials identify a registration, not a running process.
//! Replacing a binding preserves the mailbox and invalidates previous actor snapshots.
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
    Herdr(HerdrBinding),
    Standalone { session: Uuid },
    Remote { machine: Uuid },
}

impl Binding {
    pub fn runtime(&self) -> &'static str {
        match self {
            Self::Herdr(_) => "herdr",
            Self::Standalone { .. } => "standalone",
            Self::Remote { .. } => "remote",
        }
    }
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
    pub id: i64,
    pub name: String,
    pub runtime: &'static str,
    pub pane: Option<String>,
    pub availability: &'static str,
}

impl Store {
    /// Register a standalone session; replacement rotates its credential explicitly.
    pub async fn register(&self, group: &str, name: &str, replace: bool) -> Result<Uuid> {
        let session = Uuid::new_v4();
        self.set_binding(group, name, &Binding::Standalone { session }, replace)
            .await?;
        Ok(session)
    }

    /// Authenticate an explicit standalone session or the current Herdr pane.
    /// An invalid standalone credential never falls back to Herdr identity.
    pub async fn authenticate(&self, group: &str, session: Option<&Uuid>) -> Result<Mailbox> {
        if let Some(session) = session {
            return self.standalone_caller(group, session).await;
        }
        self.authenticate_herdr(group).await.context("supply AGENT_MAIL_SESSION for a registered standalone participant, or use a bound Herdr pane")
    }
}
