//! Transactional mail storage with schema locks shared by every store handle.
//!
//! [`Store::open`] explicitly distinguishes setup/migration from normal access.
//! Mutations use SQLite transactions and validate the actor's binding generation.
//! The database pool is private so callers cannot detach access from its schema lock.
//! All clones must be dropped before migration; [`Store::close`] first drains the pool.
//! Socket paths use OS path types, with UTF-8 encoding required by the SQLite schema.

use crate::states::{AgentState, Availability, MessageState};
use crate::{
    BODY_LIMIT, SUMMARY_LIMIT, bounded,
    identity::{Binding, Participant},
    mail_context::{ContextSource, MessageContext, ParentMessage},
    name,
    relay::{self, Event},
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sqlx::{
    Connection, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{
    fs::OpenOptions,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

/// Current participant identity, binding generation, and reminder budget.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Mailbox {
    /// Persistent identifier for this record.
    pub id: i64,
    /// Enrolled group containing this record.
    pub group_name: String,
    /// Name within the enclosing registration or group.
    pub name: String,
    /// Runtime identity authenticated for this mailbox generation.
    #[serde(skip)]
    pub binding: Binding,
    /// Generation invalidated by explicit identity replacement.
    pub binding_version: i64,
    /// Durable registration state.
    pub state: AgentState,
    /// Registration version.
    pub version: i64,
    /// Last registration change.
    pub updated: i64,
    /// Persisted delivery attempt count for the current retry budget.
    pub attempts: i64,
    /// Earliest next reminder attempt, in Unix seconds.
    pub next_wake: i64,
    /// Whether an operator alert has been reserved, encoded as zero or one.
    pub alerted: i64,
}

// Database representation; decode the binding into a closed enum at the boundary.
struct MailboxRow {
    id: i64,
    group_name: String,
    name: String,
    binding: String,
    binding_version: i64,
    state: AgentState,
    version: i64,
    updated: i64,
    attempts: i64,
    next_wake: i64,
    alerted: i64,
}
impl TryFrom<MailboxRow> for Mailbox {
    type Error = anyhow::Error;
    fn try_from(row: MailboxRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            group_name: row.group_name,
            name: row.name,
            binding: serde_json::from_str(&row.binding).context("decode participant binding")?,
            binding_version: row.binding_version,
            state: row.state,
            version: row.version,
            updated: row.updated,
            attempts: row.attempts,
            next_wake: row.next_wake,
            alerted: row.alerted,
        })
    }
}

/// An enrolled group’s routing and automatic delivery configuration.
#[derive(Debug, Clone, Serialize)]
pub struct Group {
    /// Name within the enclosing registration or group.
    pub name: String,
    /// Absolute Herdr socket path; absent for standalone-only groups.
    pub socket: Option<PathBuf>,
    /// Whether automatic delivery is paused, encoded as zero or one.
    pub paused: i64,
    /// Whether automatic Herdr prompts are enabled, encoded as zero or one.
    pub auto_prompt: i64,
    /// UUID of the authoritative machine for this group.
    pub home_machine: String,
}

// SQLite representation; paths become OS types at the store boundary.
struct GroupRow {
    name: String,
    socket: Option<String>,
    paused: i64,
    auto_prompt: i64,
    home_machine: String,
}
impl From<GroupRow> for Group {
    fn from(row: GroupRow) -> Self {
        Self {
            name: row.name,
            socket: row.socket.map(PathBuf::from),
            paused: row.paused,
            auto_prompt: row.auto_prompt,
            home_machine: row.home_machine,
        }
    }
}

/// Shared database access that retains its schema lock across clones.
#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

#[derive(Debug)]
struct StoreInner {
    pool: SqlitePool,
    root: PathBuf,
    _guard: DatabaseGuard,
    diagnostics: crate::diagnostics::Diagnostics,
}

/// Excludes migration for the lifetime of all store handles.
#[derive(Debug)]
pub(crate) struct DatabaseGuard(std::fs::File);

impl DatabaseGuard {
    pub(crate) fn acquire(root: &Path, exclusive: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(root.join("schema.lock"))?;
        if exclusive {
            file.try_lock_exclusive()
        } else {
            FileExt::try_lock_shared(&file)
        }
        .context("database is in use; stop the service and other commands before setup")?;
        Ok(Self(file))
    }
}

impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

/// Message content, recipients, and associations validated when publishing.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Publish {
    /// Explicit effect; omission retains legacy request behavior.
    #[serde(
        default,
        skip_serializing_if = "crate::states::MessageIntent::is_request"
    )]
    pub intent: crate::states::MessageIntent,
    /// Participant names receiving this message.
    pub recipients: Vec<String>,
    /// Caller-supplied idempotency key; retries must preserve their original content.
    pub key: String,
    /// Short UTF-8 summary used in inbox and recovery views.
    pub summary: String,
    /// Full UTF-8 message body, subject to the message byte limit.
    pub body: String,
    /// Number of seconds after publication until the message becomes overdue.
    pub due_after: Option<i64>,
    /// Required subject, or a parent from which to inherit that subject.
    pub context: ContextSource,
}

impl Publish {
    /// Validate a message request and canonicalize its recipients.
    ///
    /// # Errors
    /// Recipients, payload lengths, reply timing, or identifiers are invalid.
    pub fn normalize(&mut self) -> Result<()> {
        ensure!(
            self.intent.is_request() || self.due_after.is_none(),
            "only requests may have a business deadline"
        );
        ensure!(
            self.intent != crate::states::MessageIntent::Response
                || self.context.reply_to().is_some(),
            "a response must reference its request"
        );
        self.recipients.sort();
        self.recipients.dedup();
        ensure!(
            !self.recipients.is_empty() && self.recipients.len() <= 32,
            "supply 1–32 recipients"
        );
        for recipient in &self.recipients {
            name(recipient)?;
        }
        bounded(&self.key, 128, "send key")?;
        ensure!(!self.key.is_empty(), "send key is required");
        bounded(&self.summary, SUMMARY_LIMIT, "summary")?;
        ensure!(!self.summary.trim().is_empty(), "summary is required");
        bounded(&self.body, BODY_LIMIT, "body")?;
        ensure!(
            self.due_after
                .is_none_or(|seconds| (1..=31_536_000).contains(&seconds)),
            "due-after must be 1–31536000 seconds"
        );
        Ok(())
    }
}

/// A message summary for an unresolved inbox delivery.
#[derive(Debug, Serialize)]
pub struct InboxItem {
    /// Whether this communication creates a response obligation.
    pub intent: crate::states::MessageIntent,
    /// Persistent identifier for this record.
    pub id: i64,
    /// Name of the participant that published this message.
    pub sender: String,
    /// Short UTF-8 summary used in inbox and recovery views.
    pub summary: String,
    /// Creation timestamp in Unix seconds.
    pub created: i64,
    /// Deadline timestamp in Unix seconds.
    pub due: Option<i64>,
    /// Optional work identifier associated with this message.
    pub work_id: Option<String>,
    /// Durable task revision or conversation this message concerns.
    pub context: MessageContext,
    /// Parent message whose context this message inherited.
    pub reply_to: Option<i64>,
    /// Portable parent reference, also retained when the parent is not stored locally.
    pub parent: Option<ParentMessage>,
}

/// Message content and the recipient’s current delivery disposition.
#[derive(Debug, Serialize)]
pub struct Message {
    /// Whether this communication creates a response obligation.
    pub intent: crate::states::MessageIntent,
    /// Persistent identifier for this record.
    pub id: i64,
    /// Name of the participant that published this message.
    pub sender: String,
    /// Short UTF-8 summary used in inbox and recovery views.
    pub summary: String,
    /// Full UTF-8 message body, subject to the message byte limit.
    pub body: String,
    /// Creation timestamp in Unix seconds.
    pub created: i64,
    /// Deadline timestamp in Unix seconds.
    pub due: Option<i64>,
    /// Stored business state; it does not imply transport delivery.
    pub state: MessageState,
    /// Identifier of the reply created while resolving this delivery, if any.
    pub reply_id: Option<i64>,
    /// Optional work identifier associated with this message.
    pub work_id: Option<String>,
    /// Durable task revision or conversation this message concerns.
    pub context: MessageContext,
    /// Parent message whose context this message inherited.
    pub reply_to: Option<i64>,
    /// Portable parent reference, also retained when the parent is not stored locally.
    pub parent: Option<ParentMessage>,
}

/// Aggregate pending obligations and reminder state for one mailbox.
#[derive(Debug, Serialize)]
pub struct Pending {
    /// Persistent identifier for this record.
    pub id: i64,
    /// Enrolled group containing this record.
    pub group_name: String,
    /// Name within the enclosing registration or group.
    pub name: String,
    /// Number of pending obligations included in this aggregate.
    pub pending: i64,
    /// Earliest queued creation timestamp in Unix seconds, when available.
    pub oldest: i64,
    /// Deadline timestamp in Unix seconds.
    pub due: Option<i64>,
    /// Persisted delivery attempt count for the current retry budget.
    pub attempts: i64,
    /// Earliest next reminder attempt, in Unix seconds.
    pub next_wake: i64,
    /// Whether an operator alert has been reserved, encoded as zero or one.
    pub alerted: i64,
}

/// Schema understood by this binary.
pub(crate) const SCHEMA_VERSION: i64 = 27;

impl Store {
    /// Open a database, optionally creating and migrating its schema.
    ///
    /// Setup takes an exclusive schema lock; normal access takes a shared lock.
    /// Every clone retains that lock. Close and drop all handles before setup.
    /// Setup creates private state files and applies embedded migrations.
    ///
    /// # Errors
    /// Returns an error for lock contention, filesystem or database failures,
    /// incompatible schema versions, or missing state when `setup` is false.
    pub async fn open(root: &Path, setup: bool) -> Result<Self> {
        if setup {
            std::fs::create_dir_all(root)?;
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
        }
        ensure!(
            root.is_dir(),
            "state directory missing; run agent-mail init GROUP first"
        );
        let guard = DatabaseGuard::acquire(root, setup)?;
        Self::open_guarded(root, setup, guard).await
    }

    pub(crate) async fn open_guarded(
        root: &Path,
        setup: bool,
        guard: DatabaseGuard,
    ) -> Result<Self> {
        let path = root.join("mail.db");
        if setup && !path.exists() {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
        }
        ensure!(
            path.is_file(),
            "database missing; run agent-mail init GROUP explicitly to create it"
        );
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(false)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(3));
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await?;
        let version = sqlx::query!("PRAGMA user_version")
            .fetch_one(&pool)
            .await?
            .user_version
            .context("SQLite did not report its schema version")?;
        ensure!(
            version <= SCHEMA_VERSION,
            "database schema is newer than this binary"
        );
        if setup {
            // Rebuilding a referenced table requires FK enforcement off outside
            // the migration transaction. The migration checks every FK before commit.
            // The schema guard excludes every other Mail process during setup.
            let mut connection = pool.acquire().await?;
            if version > 0 && version < SCHEMA_VERSION {
                let backup = crate::upgrade::backup(&mut connection, root, version).await?;
                eprintln!(
                    "agent-mail: migrating schema {version} to {SCHEMA_VERSION}; backup {}",
                    backup.display()
                );
            }
            sqlx::query!("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await?;
            let mut transaction = connection.begin().await?;
            let migrated = sqlx::migrate!("./migrations").run(&mut *transaction).await;
            match migrated {
                Ok(()) => transaction.commit().await?,
                Err(error) => {
                    transaction
                        .rollback()
                        .await
                        .context("schema rollback failed")?;
                    drop(connection);
                    pool.close().await;
                    return Err(error).context("schema migration failed; original schema and records preserved; see docs/usage.md for unsupported legacy states");
                }
            }
            sqlx::query!("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await?;
            drop(connection);
            if sqlx::query!("SELECT id FROM node LIMIT 1")
                .fetch_optional(&pool)
                .await?
                .is_none()
            {
                let machine = uuid::Uuid::new_v4().to_string();
                sqlx::query!("INSERT INTO node(id) VALUES (?)", machine)
                    .execute(&pool)
                    .await?;
            }
            let machine = sqlx::query!("SELECT id FROM node LIMIT 1")
                .fetch_one(&pool)
                .await?
                .id;
            sqlx::query!(
                "UPDATE groups SET home_machine=? WHERE home_machine=''",
                machine
            )
            .execute(&pool)
            .await?;
        } else {
            ensure!(
                version == SCHEMA_VERSION,
                "database schema needs initialization or migration; run agent-mail init GROUP"
            );
        }
        Ok(Self {
            inner: Arc::new(StoreInner {
                pool,
                root: root.to_path_buf(),
                _guard: guard,
                diagnostics: crate::diagnostics::Diagnostics::default(),
            }),
        })
    }

    /// Return the directory containing this store's database and private sockets.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.inner.pool
    }

    pub(crate) fn diagnostics(&self) -> &crate::diagnostics::Diagnostics {
        &self.inner.diagnostics
    }

    pub(crate) async fn delivery_transaction(
        &self,
        actor: &Mailbox,
        operation: crate::diagnostics::Operation,
    ) -> Result<crate::diagnostics::DeliveryTransaction<'_>> {
        use crate::diagnostics::{DeliveryTransaction, Phase};
        let transaction = self
            .diagnostics()
            .measure(operation, Phase::WriterAcquire, async {
                let mut tx = self.pool().begin().await?;
                Self::lock_actor(&mut tx, actor).await?;
                Ok(tx)
            })
            .await?;
        Ok(DeliveryTransaction {
            transaction,
            timer: self.diagnostics().start(operation, Phase::WriterHold),
        })
    }

    /// Close all pooled connections and consume this handle.
    ///
    /// All clones share the pool, so further database operations on them fail.
    /// The schema lock remains held until the last clone is dropped. Await this
    /// before releasing the final handle when preparing to run migrations.
    pub async fn close(self) {
        self.inner.pool.close().await;
    }

    /// Read one enrolled group.
    ///
    /// # Errors
    /// The group is missing or its database query fails.
    pub async fn group(&self, group: &str) -> Result<Group> {
        sqlx::query_as!(
            GroupRow,
            "SELECT name, NULLIF(socket, '') AS \"socket?: String\", paused, auto_prompt, home_machine FROM groups WHERE name = ?",
            group
        )
        .fetch_optional(self.pool())
        .await?
        .map(Group::from)
        .context("group not enrolled; run agent-mail init NAME")
    }

    /// List enrolled groups in name order.
    ///
    /// # Errors
    /// The database query fails.
    pub async fn groups(&self) -> Result<Vec<Group>> {
        Ok(sqlx::query_as!(
            GroupRow,
            "SELECT name, NULLIF(socket, '') AS \"socket?: String\", paused, auto_prompt, home_machine FROM groups ORDER BY name"
        )
        .fetch_all(self.pool())
        .await?.into_iter().map(Group::from).collect())
    }

    /// Enroll a group with an optional absolute Herdr socket path.
    ///
    /// # Errors
    /// The group name or path is invalid, the socket conflicts, or persistence fails.
    pub async fn enroll(&self, group: &str, socket: Option<&Path>) -> Result<()> {
        name(group)?;
        ensure!(
            socket.is_none_or(Path::is_absolute),
            "Herdr socket path must be absolute"
        );
        // The existing SQLite schema stores UTF-8 text; retain that wire format.
        let socket = socket
            .map(|path| path.to_str().context("socket path is not UTF-8"))
            .transpose()?
            .unwrap_or("");
        let mut tx = self.pool().begin().await?;
        let machine = self.machine_id().await?;
        sqlx::query!(
            "INSERT INTO groups(name, socket, home_machine) VALUES (?, ?, ?) ON CONFLICT(name) DO NOTHING",
            group,
            socket,
            machine
        )
        .execute(&mut *tx)
        .await?;
        let old = sqlx::query!("SELECT socket FROM groups WHERE name = ?", group)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            old.socket == socket || old.socket.is_empty(),
            "group belongs to another socket; choose a different group"
        );
        if old.socket.is_empty() && !socket.is_empty() {
            sqlx::query!("UPDATE groups SET socket=? WHERE name=?", socket, group)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Read this installation’s persistent machine UUID.
    ///
    /// # Errors
    /// The database query fails or the node record is missing.
    pub async fn machine_id(&self) -> Result<String> {
        Ok(sqlx::query!("SELECT id FROM node LIMIT 1")
            .fetch_one(self.pool())
            .await?
            .id)
    }

    /// Set whether a group permits automatic delivery.
    ///
    /// # Errors
    /// The database update fails.
    pub async fn pause(&self, group: &str, paused: bool) -> Result<()> {
        self.group(group).await?;
        sqlx::query!("UPDATE groups SET paused = ? WHERE name = ?", paused, group)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Opt a group into automatic Herdr prompts, or disable them.
    ///
    /// # Errors
    /// The group is missing, enabling lacks a Herdr socket, or persistence fails.
    pub async fn set_auto_prompt(&self, group: &str, enabled: bool) -> Result<()> {
        let config = self.group(group).await?;
        ensure!(
            !enabled || config.socket.is_some(),
            "automatic prompts require a Herdr socket"
        );
        sqlx::query!(
            "UPDATE groups SET auto_prompt = ? WHERE name = ?",
            enabled,
            group
        )
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Read the current identity and retry state for a participant.
    ///
    /// # Errors
    /// The participant is missing, its binding cannot be decoded, or the query fails.
    pub async fn mailbox(&self, group: &str, participant: &str) -> Result<Mailbox> {
        self.find_mailbox(group, participant)
            .await?
            .context("participant is not registered in this group")
    }

    pub(crate) async fn find_mailbox(
        &self,
        group: &str,
        participant: &str,
    ) -> Result<Option<Mailbox>> {
        sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,agent_state AS 'state: AgentState',agent_version AS version,agent_updated AS updated,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND name=?",
            group, participant).fetch_optional(self.pool()).await?
            .map(Mailbox::try_from).transpose()
    }

    /// Look up a participant by its bound Herdr pane.
    ///
    /// # Errors
    /// The pane is unbound, the binding cannot be decoded, or the query fails.
    pub async fn caller(&self, group: &str, pane: &str) -> Result<Mailbox> {
        sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,agent_state AS 'state: AgentState',agent_version AS version,agent_updated AS updated,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND pane=? AND agent_state='registered'",
            group, pane).fetch_optional(self.pool()).await?
            .context("caller pane is not bound in this group")?.try_into()
    }

    pub(crate) async fn standalone_caller(
        &self,
        group: &str,
        session: &uuid::Uuid,
    ) -> Result<Mailbox> {
        let session = session.to_string();
        sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,agent_state AS 'state: AgentState',agent_version AS version,agent_updated AS updated,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND standalone_session=? AND agent_state='registered'",
            group, session).fetch_optional(self.pool()).await?
            .context("standalone session is unknown or replaced in this group")?.try_into()
    }

    /// List registrations without exposing standalone session credentials.
    ///
    /// # Errors
    /// The database query or binding decoding fails.
    pub async fn participants(&self, group: &str) -> Result<Vec<Participant>> {
        self.group(group).await?;
        let rows = sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,agent_state AS 'state: AgentState',agent_version AS version,agent_updated AS updated,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? ORDER BY name",
            group).fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                let mailbox = Mailbox::try_from(row)?;
                Ok(Participant {
                    id: mailbox.id,
                    name: mailbox.name,
                    runtime: mailbox.binding.runtime(),
                    pane: mailbox.binding.herdr().map(|binding| binding.pane.clone()),
                    availability: Availability::Unknown,
                    state: mailbox.state,
                    version: mailbox.version,
                    updated: mailbox.updated,
                })
            })
            .collect()
    }

    pub(crate) async fn set_binding(
        &self,
        group: &str,
        participant: &str,
        binding: &Binding,
        replace: bool,
    ) -> Result<()> {
        name(participant)?;
        self.group(group).await?;
        let mut tx = self.pool().begin().await?;
        sqlx::query!("UPDATE groups SET paused=paused WHERE name=?", group)
            .execute(&mut *tx)
            .await?;
        let old = sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,agent_state AS 'state: AgentState',agent_version AS version,agent_updated AS updated,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND name=?",
            group, participant).fetch_optional(&mut *tx).await?;
        let encoded = serde_json::to_string(binding)?;
        if let Some(old) = old {
            let old = Mailbox::try_from(old)?;
            let state = sqlx::query!("SELECT agent_state FROM mailboxes WHERE id=?", old.id)
                .fetch_one(&mut *tx)
                .await?;
            ensure!(
                state.agent_state == "registered",
                "agent is retired; restore it explicitly before rebinding"
            );
            ensure!(
                !matches!(old.binding, Binding::Remote { .. }),
                "remote route cannot be rebound as a local agent"
            );
            ensure!(
                old.binding.same_identity(binding) || replace,
                "participant already exists with a different binding; use agent replace NAME for standalone identity or agent bind NAME --replace for Herdr"
            );
            if old.binding != *binding {
                sqlx::query!(
                    "UPDATE mailboxes SET binding=?,binding_version=binding_version+1 WHERE id=?",
                    encoded,
                    old.id
                )
                .execute(&mut *tx)
                .await?;
            }
        } else {
            sqlx::query!(
                "INSERT INTO mailboxes(group_name,name,binding) VALUES (?,?,?)",
                group,
                participant,
                encoded
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(())
    }

    pub(crate) async fn lock_actor(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
    ) -> Result<()> {
        let result = sqlx::query!("UPDATE mailboxes SET attempts=attempts WHERE id=? AND binding_version=? AND remote_machine IS NULL AND agent_state='registered'",
            actor.id, actor.binding_version).execute(&mut **tx).await?;
        ensure!(
            result.rows_affected() == 1,
            "binding changed during operation; retry from the current participant"
        );
        Ok(())
    }

    pub(crate) async fn check_actor(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
    ) -> Result<()> {
        let current = sqlx::query!(
            "SELECT id FROM mailboxes WHERE id=? AND binding_version=? AND remote_machine IS NULL AND agent_state='registered'",
            actor.id,
            actor.binding_version
        )
        .fetch_optional(&mut **tx)
        .await?;
        ensure!(
            current.is_some(),
            "binding changed during operation; retry from the current participant"
        );
        Ok(())
    }

    async fn publish_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        publish: &mut Publish,
        now: i64,
    ) -> Result<i64> {
        publish.normalize()?;
        let canonical = serde_json::to_string(&publish)?;
        if let Some(existing) = sqlx::query!(
            "SELECT id, canonical FROM messages WHERE sender=? AND dedup_key=?",
            actor.id,
            publish.key
        )
        .fetch_optional(&mut **tx)
        .await?
        {
            ensure!(
                existing.canonical == canonical,
                "send key already exists with different content"
            );
            return Ok(existing.id);
        }
        if publish.intent == crate::states::MessageIntent::Response {
            let reply_to = publish.context.reply_to();
            let parent = sqlx::query!("SELECT m.sender,b.name FROM messages m JOIN mailboxes b ON b.id=m.sender JOIN deliveries d ON d.message=m.id WHERE m.id=? AND d.recipient=? AND m.intent='request'",reply_to,actor.id)
                .fetch_optional(&mut **tx).await?.context("response requires a request addressed to this agent")?;
            ensure!(
                publish.recipients == [parent.name],
                "a response must address the original requester"
            );
        }
        let mut recipient_ids = Vec::new();
        let subject = crate::mail_context::resolve_tx(tx, actor, &publish.context).await?;
        let context = serde_json::to_string(&subject.context)?;
        for recipient in &publish.recipients {
            let record = sqlx::query!(
                "SELECT id FROM mailboxes WHERE group_name=? AND name=?",
                actor.group_name,
                recipient
            )
            .fetch_optional(&mut **tx)
            .await?
            .with_context(|| format!("unknown recipient: {recipient}"))?;
            recipient_ids.push(record.id);
        }
        let due = publish
            .due_after
            .map(|seconds| now.checked_add(seconds).context("deadline overflow"))
            .transpose()?;
        let legacy_due = due.unwrap_or(now);
        let global_id = uuid::Uuid::new_v4().to_string();
        let intent = publish.intent.as_str();
        let result = sqlx::query!("INSERT INTO messages(sender,dedup_key,canonical,summary,body,created,due,reply_to,work_id,global_id,deadline,intent,context,parent_global_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            actor.id, publish.key, canonical, publish.summary, publish.body, now, legacy_due, subject.reply_to, subject.work_id, global_id, due,intent,context,subject.parent_global_id).execute(&mut **tx).await?;
        let id = result.last_insert_rowid();
        for recipient in recipient_ids {
            sqlx::query!(
                "INSERT INTO deliveries(message,recipient) VALUES (?,?)",
                id,
                recipient
            )
            .execute(&mut **tx)
            .await?;
            relay::enqueue_message(tx, id, recipient, now).await?;
        }
        Ok(id)
    }

    /// Publish an idempotent message and its delivery records atomically.
    ///
    /// # Errors
    /// The actor is stale, the payload or recipients are invalid, the key conflicts, or persistence fails.
    pub async fn publish(&self, actor: &Mailbox, mut publish: Publish, now: i64) -> Result<i64> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let id = Self::publish_tx(&mut tx, actor, &mut publish, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(id)
    }

    /// Read a bounded page of unresolved messages after a message cursor.
    ///
    /// # Errors
    /// The actor is stale or the database query fails.
    pub async fn inbox(&self, actor: &Mailbox, after: i64) -> Result<Vec<InboxItem>> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let items = sqlx::query_as!(InboxItem,
            "SELECT m.intent AS 'intent: crate::states::MessageIntent',m.id, b.name AS sender, m.summary, m.created, m.deadline AS due, m.work_id,m.context AS 'context!: MessageContext',m.reply_to,CASE WHEN m.parent_global_id IS NOT NULL THEN json_object('global_id',m.parent_global_id,'local_id',m.reply_to) END AS 'parent?: ParentMessage' FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=m.sender WHERE d.recipient=? AND d.state='pending' AND m.id>? AND (m.intent='request' OR NOT EXISTS(SELECT 1 FROM message_observations o WHERE o.message=m.id AND o.recipient=d.recipient AND o.binding_version=?)) ORDER BY m.id LIMIT 6",
            actor.id, after,actor.binding_version).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(items)
    }

    /// Read a message addressed to the authenticated participant.
    ///
    /// # Errors
    /// The actor is stale, the message is outside its inbox, or the query fails.
    pub async fn message(&self, actor: &Mailbox, id: i64) -> Result<Message> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let item = sqlx::query_as!(Message,
            "SELECT m.intent AS 'intent: crate::states::MessageIntent',m.id,b.name AS sender,m.summary,m.body,m.created,m.deadline AS due,d.state AS 'state: MessageState',d.reply_id,m.work_id,m.context AS 'context!: MessageContext',m.reply_to,CASE WHEN m.parent_global_id IS NOT NULL THEN json_object('global_id',m.parent_global_id,'local_id',m.reply_to) END AS 'parent?: ParentMessage' FROM messages m JOIN deliveries d ON d.message=m.id JOIN mailboxes b ON b.id=m.sender WHERE m.id=? AND d.recipient=?",
            id, actor.id).fetch_optional(&mut *tx).await?.context("message is not in this inbox")?;
        Self::retrieve_tx(
            &mut tx,
            actor,
            crate::states::EventKind::MailPending,
            &id.to_string(),
            0,
        )
        .await?;
        sqlx::query!("INSERT OR IGNORE INTO message_observations(message,recipient,binding_version) VALUES(?,?,?)",id,actor.id,actor.binding_version).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(item)
    }

    /// Resolve an inbox delivery and optionally publish an atomic reply.
    ///
    /// # Errors
    /// The actor is stale, disposition conflicts, the delivery is withdrawn or absent, or persistence fails.
    pub async fn resolve(
        &self,
        actor: &Mailbox,
        id: i64,
        note: &str,
        reply: Option<(String, String)>,
        now: i64,
    ) -> Result<Option<i64>> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let result = Self::resolve_tx(&mut tx, actor, id, note, reply, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }

    pub(crate) async fn resolve_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        id: i64,
        note: &str,
        reply: Option<(String, String)>,
        now: i64,
    ) -> Result<Option<i64>> {
        bounded(note, SUMMARY_LIMIT, "resolution")?;
        let resolution = serde_json::to_string(&(note, &reply))?;
        let d = sqlx::query!(
            "SELECT d.state AS 'state: MessageState',d.resolution,d.reply_id,m.intent AS 'intent: crate::states::MessageIntent' FROM deliveries d JOIN messages m ON m.id=d.message WHERE d.message=? AND d.recipient=?",
            id,
            actor.id
        )
        .fetch_optional(&mut **tx)
        .await?
        .context("message is not in this inbox")?;
        ensure!(
            d.intent.is_request(),
            "notices and responses have no business disposition; use mail show to record observation"
        );
        if d.state == MessageState::Resolved {
            ensure!(
                d.resolution.as_deref() == Some(resolution.as_str()),
                "message already resolved with a different disposition"
            );
            Self::retrieve_tx(
                tx,
                actor,
                crate::states::EventKind::MailPending,
                &id.to_string(),
                0,
            )
            .await?;
            return Ok(d.reply_id);
        }
        ensure!(
            d.state == MessageState::Pending,
            "message was withdrawn; it cannot be resolved"
        );
        let reply_id = if let Some((key, body)) = reply {
            let sender = sqlx::query!(
                "SELECT b.name FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE m.id=?",
                id
            )
            .fetch_one(&mut **tx)
            .await?;
            let mut summary = body.lines().next().unwrap_or("Reply").to_string();
            while summary.len() > SUMMARY_LIMIT {
                summary.pop();
            }
            let mut p = Publish {
                intent: crate::states::MessageIntent::Response,
                recipients: vec![sender.name],
                key,
                summary,
                body,
                due_after: None,
                context: ContextSource::Reply {
                    message: id.try_into()?,
                },
            };
            Some(Self::publish_tx(tx, actor, &mut p, now).await?)
        } else {
            d.reply_id
        };
        sqlx::query!("UPDATE deliveries SET state='resolved',resolution=?,reply_id=? WHERE message=? AND recipient=?",
            resolution, reply_id, id, actor.id).execute(&mut **tx).await?;
        let sender = sqlx::query!("SELECT m.global_id,b.remote_machine FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE m.id=?", id)
            .fetch_one(&mut **tx).await?;
        if let Some(machine) = sender.remote_machine {
            relay::enqueue(
                tx,
                relay::machine(&machine)?,
                Event::Resolution {
                    group: actor.group_name.clone(),
                    message: relay::machine(
                        sender
                            .global_id
                            .as_deref()
                            .context("message has no global ID")?,
                    )?,
                    recipient: actor.name.clone(),
                    resolution,
                },
                now,
            )
            .await?;
        }
        Self::retrieve_tx(
            tx,
            actor,
            crate::states::EventKind::MailPending,
            &id.to_string(),
            0,
        )
        .await?;
        Self::reset_empty(tx, &actor.group_name).await?;
        Ok(reply_id)
    }

    /// Withdraw a sender’s pending deliveries at the supplied Unix timestamp.
    ///
    /// # Errors
    /// The actor is stale, the message is absent or owned by another sender, or persistence fails.
    pub async fn withdraw(&self, actor: &Mailbox, id: i64, time: i64) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let m = sqlx::query!("SELECT sender FROM messages WHERE id=?", id)
            .fetch_optional(&mut *tx)
            .await?
            .context("message not found")?;
        ensure!(
            m.sender == actor.id,
            "only the sender can withdraw a message"
        );
        let remote = sqlx::query!("SELECT b.name,b.remote_machine,m.global_id FROM deliveries d JOIN mailboxes b ON b.id=d.recipient JOIN messages m ON m.id=d.message WHERE d.message=? AND d.state='pending' AND b.remote_machine IS NOT NULL", id)
            .fetch_all(&mut *tx).await?;
        sqlx::query!(
            "UPDATE deliveries SET state='withdrawn' WHERE message=? AND state='pending'",
            id
        )
        .execute(&mut *tx)
        .await?;
        for recipient in remote {
            let machine = recipient.remote_machine.context("remote route vanished")?;
            relay::enqueue(
                &mut tx,
                relay::machine(&machine)?,
                Event::Withdrawal {
                    group: actor.group_name.clone(),
                    message: relay::machine(
                        recipient
                            .global_id
                            .as_deref()
                            .context("message has no global ID")?,
                    )?,
                    recipient: recipient.name,
                },
                time,
            )
            .await?;
        }
        Self::reset_empty(&mut tx, &actor.group_name).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(())
    }

    pub(crate) async fn reset_empty(tx: &mut Transaction<'_, Sqlite>, group: &str) -> Result<()> {
        sqlx::query!("UPDATE mailboxes SET attempts=0,next_wake=0,alerted=0 WHERE group_name=? AND NOT EXISTS(SELECT 1 FROM deliveries d JOIN messages m ON m.id=d.message WHERE d.recipient=mailboxes.id AND m.intent='request' AND d.state='pending') AND NOT EXISTS(SELECT 1 FROM pending_work_events WHERE recipient=mailboxes.id)", group)
            .execute(&mut **tx).await?;
        Ok(())
    }

    /// List mailboxes with pending mail or work notifications.
    ///
    /// # Errors
    /// The database query fails.
    pub async fn pending(&self) -> Result<Vec<Pending>> {
        Ok(sqlx::query_as!(Pending,
            "SELECT b.id AS 'id!: i64',b.group_name AS 'group_name!: String',b.name AS 'name!: String',COUNT(*) AS 'pending!: i64',MIN(m.created) AS 'oldest!: i64',MIN(m.due) AS 'due?: i64',b.attempts AS 'attempts!: i64',b.next_wake AS 'next_wake!: i64',b.alerted AS 'alerted!: i64' FROM mailboxes b JOIN (SELECT d.recipient,m.created,m.deadline AS due FROM deliveries d JOIN messages m ON m.id=d.message WHERE d.state='pending' AND (m.intent='request' OR (m.intent='response' AND NOT EXISTS(SELECT 1 FROM event_receipts r JOIN coordination_events e ON e.id=r.event JOIN mailboxes recipient ON recipient.id=d.recipient WHERE r.recipient=d.recipient AND r.binding_version=recipient.binding_version AND e.kind='mail_pending' AND e.subject=CAST(m.id AS TEXT)))) UNION ALL SELECT recipient,created,created+900 AS due FROM pending_work_events UNION ALL SELECT recipient,created,NULL AS due FROM attention_events WHERE reason='stop_work') m ON m.recipient=b.id WHERE b.remote_machine IS NULL AND b.agent_state='registered' GROUP BY b.id ORDER BY b.group_name,b.id")
            .fetch_all(self.pool()).await?)
    }

    /// Reserve an operator alert for overdue or exhausted delivery attempts.
    ///
    /// # Errors
    /// The database update fails; an already reserved or ineligible alert returns false.
    pub async fn reserve_alert(&self, id: i64, now: i64) -> Result<bool> {
        let mut tx = self.pool().begin().await?;
        let result = sqlx::query!("UPDATE mailboxes SET alerted=1 WHERE id=? AND ((alerted=0 AND (EXISTS(SELECT 1 FROM deliveries d JOIN messages m ON m.id=d.message WHERE d.recipient=mailboxes.id AND d.state='pending' AND m.intent='request' AND m.deadline<=?) OR EXISTS(SELECT 1 FROM pending_work_events WHERE recipient=mailboxes.id AND created+900<=?))) OR EXISTS(SELECT 1 FROM attention_events e JOIN attention_attempts a ON a.recipient=e.recipient AND a.binding_version=mailboxes.binding_version AND a.event=e.id WHERE e.recipient=mailboxes.id AND a.attempts>=3 AND a.next_attempt<=? AND a.alerted=0))", id, now, now,now).execute(&mut *tx).await?;
        if result.rows_affected() != 0 {
            sqlx::query!("UPDATE attention_attempts SET alerted=1 WHERE recipient=? AND binding_version=(SELECT binding_version FROM mailboxes WHERE id=?) AND attempts>=3 AND next_attempt<=? AND EXISTS(SELECT 1 FROM attention_events e WHERE e.id=attention_attempts.event AND e.recipient=attention_attempts.recipient)",id,id,now).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(result.rows_affected() != 0)
    }

    /// Reset notification and verification budgets without changing identity, policy or work.
    ///
    /// # Errors
    /// The participant is missing or the transaction fails.
    pub async fn rearm(&self, group: &str, participant: &str) -> Result<()> {
        let actor = self.mailbox(group, participant).await?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, &actor).await?;
        sqlx::query!("DELETE FROM attention_dispatch WHERE recipient=?", actor.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("DELETE FROM attention_attempts WHERE recipient=?", actor.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query!(
            "UPDATE mailboxes SET attempts=0,next_wake=0,alerted=0 WHERE id=?",
            actor.id
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!("UPDATE runtime_wakes SET attempts=0,next_attempt=0 WHERE recipient=? AND binding_version=?",actor.id,actor.binding_version).execute(&mut *tx).await?;
        sqlx::query!("DELETE FROM delivery_probes WHERE recipient=?", actor.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(())
    }
}
