//! Durable mail, work records, and explicit runtime bindings for coding agents.
//!
//! Start with [`store::Store::open`] and register or bind participants through
//! [`identity`] or [`herdr`]. Mutations authenticate a current mailbox snapshot;
//! transport receipts never imply business completion. [`service`] drives retries,
//! [`stream`] replays committed changes, and [`relay`] exchanges them over SSH.
//!
//! This is the application support library: fallible operations use `anyhow`.
//! State and sockets require Unix. Automatic supervisor installation requires macOS.
//! Timestamps supplied to business operations are Unix seconds, enabling deterministic tests.
//!
//! # Examples
//!
//! ```no_run
//! use agent_mail::store::Store;
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let root = std::path::Path::new("/tmp/agent-mail-example");
//! let store = Store::open(root, true).await?;
//! store.enroll("review", None).await?;
//! let credential = store.register("review", "reviewer", false).await?;
//! let actor = store.authenticate("review", Some(&credential)).await?;
//! assert_eq!(actor.name, "reviewer");
//! store.close().await;
//! # Ok(())
//! # }
//! ```

/// Versioned agent registration lifecycle.
pub mod agents;
/// Typed resource references and managed content-addressed evidence.
pub mod artifacts;
pub mod herdr;
pub mod identity;
/// Shared immutable document revisions.
pub mod records;
/// Explicit task relationships and dependency facts.
pub mod relationships;
pub mod relay;
pub mod service;
/// Group-scoped diagnostics and explicit installation overview.
pub mod status;
pub mod store;
pub mod supervision;
/// End-to-end delivery verification, separate from business decisions.
pub mod verification;
pub mod work;

/// Stable Herdr plugin identifier used for session-scoped enablement checks.
pub const PLUGIN_ID: &str = "youssef-tharwat.agent-mail";
/// Maximum message body size in UTF-8 bytes, keeping relay batches bounded.
pub const BODY_LIMIT: usize = 8 * 1024;
/// Maximum summary size in UTF-8 bytes for compact recovery views.
pub const SUMMARY_LIMIT: usize = 240;

/// Read the system clock as Unix seconds.
///
/// # Errors
/// The clock precedes the Unix epoch or exceeds the signed timestamp range.
pub fn now() -> anyhow::Result<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?)
}

/// Validate a bounded ASCII identifier.
///
/// # Errors
/// The name is empty, exceeds 48 bytes, or contains unsupported characters.
pub fn name(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 48
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
        "names must be 1–48 ASCII letters, digits, dots, dashes or underscores"
    );
    Ok(())
}

/// Validate a UTF-8 byte budget and reject embedded NUL characters.
///
/// # Errors
/// The value exceeds `limit` bytes or contains a NUL character.
pub fn bounded(value: &str, limit: usize, label: &str) -> anyhow::Result<()> {
    anyhow::ensure!(value.len() <= limit, "{label} exceeds {limit} UTF-8 bytes");
    anyhow::ensure!(!value.contains('\0'), "{label} contains a NUL byte");
    Ok(())
}

pub mod events;

pub mod hooks;
pub mod recovery;

pub mod codex;

pub mod attention;

pub mod stream;

/// Resumable grouped watches and event-driven request waits.
pub mod watch;

pub mod doctor;

pub mod claude;

pub mod native;

/// Native Claude inbox registration and automatic hook receipts.
pub mod claude_inbox;

/// Version-matched operating instructions bundled with the binary.
pub const SKILL: &str = include_str!("../docs/agent-guide.md");

/// Observed launcher and lifecycle-hook readiness.
pub mod readiness;

/// Closed coordination state vocabularies.
pub mod states;

/// Automatic store migration and coordinated local worker replacement.
pub mod upgrade;

/// Durable checkpoints and bounded follow-through reconciliation.
pub mod followup;

mod lifecycle;
mod turns;
