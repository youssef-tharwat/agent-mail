use crate::{
    BODY_LIMIT, SUMMARY_LIMIT, bounded,
    identity::{Binding, Participant},
    name,
    relay::{self, Event},
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sqlx::{
    Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{
    fs::OpenOptions,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Mailbox {
    pub id: i64,
    pub group_name: String,
    pub name: String,
    #[serde(skip)]
    pub binding: Binding,
    pub binding_version: i64,
    pub attempts: i64,
    pub next_wake: i64,
    pub alerted: i64,
}

// Database representation; decode the binding into a closed enum at the boundary.
struct MailboxRow {
    id: i64,
    group_name: String,
    name: String,
    binding: String,
    binding_version: i64,
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
            attempts: row.attempts,
            next_wake: row.next_wake,
            alerted: row.alerted,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Group {
    pub name: String,
    pub socket: Option<String>,
    pub paused: i64,
    pub auto_prompt: i64,
    pub home_machine: String,
}

#[derive(Clone)]
pub struct Store {
    pub pool: SqlitePool,
    pub root: PathBuf,
}

/// Held for every process using the database. Setup needs an exclusive lock.
pub struct DatabaseGuard(std::fs::File);

impl DatabaseGuard {
    fn acquire(root: &Path, exclusive: bool) -> Result<Self> {
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Publish {
    pub recipients: Vec<String>,
    pub key: String,
    pub summary: String,
    pub body: String,
    pub due_after: i64,
    pub reply_to: Option<i64>,
    pub work_id: Option<String>,
}

impl Publish {
    pub fn normalize(&mut self) -> Result<()> {
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
            (1..=31_536_000).contains(&self.due_after),
            "due-after must be 1–31536000 seconds"
        );
        if let Some(work_id) = &self.work_id {
            name(work_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct InboxItem {
    pub id: i64,
    pub sender: String,
    pub summary: String,
    pub created: i64,
    pub due: i64,
    pub work_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Message {
    pub id: i64,
    pub sender: String,
    pub summary: String,
    pub body: String,
    pub created: i64,
    pub due: i64,
    pub state: String,
    pub reply_id: Option<i64>,
    pub work_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Pending {
    pub id: i64,
    pub group_name: String,
    pub name: String,
    pub pending: i64,
    pub oldest: i64,
    pub due: i64,
    pub attempts: i64,
    pub next_wake: i64,
    pub alerted: i64,
}

impl Store {
    pub async fn open(root: &Path, setup: bool) -> Result<(Self, DatabaseGuard)> {
        if setup {
            std::fs::create_dir_all(root)?;
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
        }
        ensure!(root.is_dir(), "state directory missing; run setup first");
        let guard = DatabaseGuard::acquire(root, setup)?;
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
            "database missing; run setup explicitly to create it"
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
        ensure!(version <= 7, "database schema is newer than this binary");
        if setup {
            // Rebuilding a referenced table requires FK enforcement off outside
            // the migration transaction. The migration checks every FK before commit.
            // The schema guard excludes every other Mail process during setup.
            let mut connection = pool.acquire().await?;
            sqlx::query!("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await?;
            let migrated = sqlx::migrate!("./migrations").run(&mut *connection).await;
            sqlx::query!("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await?;
            migrated?;
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
                version == 7,
                "database schema needs initialization or migration; run setup"
            );
        }
        Ok((
            Self {
                pool,
                root: root.to_path_buf(),
            },
            guard,
        ))
    }

    pub async fn group(&self, group: &str) -> Result<Group> {
        sqlx::query_as!(
            Group,
            "SELECT name, NULLIF(socket, '') AS \"socket?: String\", paused, auto_prompt, home_machine FROM groups WHERE name = ?",
            group
        )
        .fetch_optional(&self.pool)
        .await?
        .context("group not enrolled; run setup --group NAME")
    }

    pub async fn groups(&self) -> Result<Vec<Group>> {
        Ok(sqlx::query_as!(
            Group,
            "SELECT name, NULLIF(socket, '') AS \"socket?: String\", paused, auto_prompt, home_machine FROM groups ORDER BY name"
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn enroll(&self, group: &str, socket: &str) -> Result<()> {
        name(group)?;
        ensure!(
            socket.is_empty() || Path::new(socket).is_absolute(),
            "Herdr socket path must be absolute"
        );
        let mut tx = self.pool.begin().await?;
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

    pub async fn machine_id(&self) -> Result<String> {
        Ok(sqlx::query!("SELECT id FROM node LIMIT 1")
            .fetch_one(&self.pool)
            .await?
            .id)
    }

    pub async fn pause(&self, group: &str, paused: bool) -> Result<()> {
        self.group(group).await?;
        sqlx::query!("UPDATE groups SET paused = ? WHERE name = ?", paused, group)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

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
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mailbox(&self, group: &str, participant: &str) -> Result<Mailbox> {
        sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND name=?",
            group, participant).fetch_optional(&self.pool).await?
            .context("participant is not registered in this group")?.try_into()
    }

    pub async fn caller(&self, group: &str, pane: &str) -> Result<Mailbox> {
        sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND pane=?",
            group, pane).fetch_optional(&self.pool).await?
            .context("caller pane is not bound in this group")?.try_into()
    }

    pub(crate) async fn standalone_caller(
        &self,
        group: &str,
        session: &uuid::Uuid,
    ) -> Result<Mailbox> {
        let session = session.to_string();
        sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND standalone_session=?",
            group, session).fetch_optional(&self.pool).await?
            .context("standalone session is unknown or replaced in this group")?.try_into()
    }

    pub async fn participants(&self, group: &str) -> Result<Vec<Participant>> {
        self.group(group).await?;
        let rows = sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? ORDER BY name",
            group).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let mailbox = Mailbox::try_from(row)?;
                Ok(Participant {
                    id: mailbox.id,
                    name: mailbox.name,
                    runtime: mailbox.binding.runtime(),
                    pane: mailbox.binding.herdr().map(|binding| binding.pane.clone()),
                    availability: "unknown",
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
        let mut tx = self.pool.begin().await?;
        sqlx::query!("UPDATE groups SET paused=paused WHERE name=?", group)
            .execute(&mut *tx)
            .await?;
        let old = sqlx::query_as!(MailboxRow,
            "SELECT id,group_name,name,binding,binding_version,attempts,next_wake,alerted FROM mailboxes WHERE group_name=? AND name=?",
            group, participant).fetch_optional(&mut *tx).await?;
        let encoded = serde_json::to_string(binding)?;
        if let Some(old) = old {
            let old = Mailbox::try_from(old)?;
            ensure!(
                !matches!(old.binding, Binding::Remote { .. }),
                "remote route cannot be rebound as a local agent"
            );
            ensure!(
                old.binding.same_identity(binding) || replace,
                "binding changed; use --replace after confirming the intended participant"
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
        Ok(())
    }

    pub(crate) async fn lock_actor(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
    ) -> Result<()> {
        let result = sqlx::query!("UPDATE mailboxes SET attempts=attempts WHERE id=? AND binding_version=? AND remote_machine IS NULL",
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
            "SELECT id FROM mailboxes WHERE id=? AND binding_version=? AND remote_machine IS NULL",
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
        let mut recipient_ids = Vec::new();
        if let Some(work_id) = &publish.work_id {
            ensure!(
                sqlx::query!(
                    "SELECT id FROM work_items WHERE group_name=? AND id=?",
                    actor.group_name,
                    work_id
                )
                .fetch_optional(&mut **tx)
                .await?
                .is_some()
                    || sqlx::query!(
                        "SELECT work_id FROM work_snapshots WHERE group_name=? AND work_id=?",
                        actor.group_name,
                        work_id
                    )
                    .fetch_optional(&mut **tx)
                    .await?
                    .is_some(),
                "work item is not in this group"
            );
        }
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
        let due = now
            .checked_add(publish.due_after)
            .context("deadline overflow")?;
        let global_id = uuid::Uuid::new_v4().to_string();
        let result = sqlx::query!("INSERT INTO messages(sender,dedup_key,canonical,summary,body,created,due,reply_to,work_id,global_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            actor.id, publish.key, canonical, publish.summary, publish.body, now, due, publish.reply_to, publish.work_id, global_id).execute(&mut **tx).await?;
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

    pub async fn publish(&self, actor: &Mailbox, mut publish: Publish, now: i64) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let id = Self::publish_tx(&mut tx, actor, &mut publish, now).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn inbox(&self, actor: &Mailbox, after: i64) -> Result<Vec<InboxItem>> {
        let mut tx = self.pool.begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let items = sqlx::query_as!(InboxItem,
            "SELECT m.id, b.name AS sender, m.summary, m.created, m.due, m.work_id FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=m.sender WHERE d.recipient=? AND d.state='pending' AND m.id>? ORDER BY m.id LIMIT 6",
            actor.id, after).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(items)
    }

    pub async fn message(&self, actor: &Mailbox, id: i64) -> Result<Message> {
        let mut tx = self.pool.begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let item = sqlx::query_as!(Message,
            "SELECT m.id,b.name AS sender,m.summary,m.body,m.created,m.due,d.state,d.reply_id,m.work_id FROM messages m JOIN deliveries d ON d.message=m.id JOIN mailboxes b ON b.id=m.sender WHERE m.id=? AND d.recipient=?",
            id, actor.id).fetch_optional(&mut *tx).await?.context("message is not in this inbox")?;
        tx.commit().await?;
        Ok(item)
    }

    pub async fn resolve(
        &self,
        actor: &Mailbox,
        id: i64,
        note: &str,
        reply: Option<(String, String)>,
        now: i64,
    ) -> Result<Option<i64>> {
        let mut tx = self.pool.begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let result = Self::resolve_tx(&mut tx, actor, id, note, reply, now).await?;
        tx.commit().await?;
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
            "SELECT state,resolution,reply_id FROM deliveries WHERE message=? AND recipient=?",
            id,
            actor.id
        )
        .fetch_optional(&mut **tx)
        .await?
        .context("message is not in this inbox")?;
        if d.state == "resolved" {
            ensure!(
                d.resolution.as_deref() == Some(resolution.as_str()),
                "message already resolved with a different disposition"
            );
            return Ok(d.reply_id);
        }
        ensure!(
            d.state == "pending",
            "message was withdrawn; it cannot be resolved"
        );
        let reply_id = if let Some((key, body)) = reply {
            let sender = sqlx::query!(
                "SELECT b.name, m.work_id FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE m.id=?",
                id
            )
            .fetch_one(&mut **tx)
            .await?;
            let mut summary = body.lines().next().unwrap_or("Reply").to_string();
            while summary.len() > SUMMARY_LIMIT {
                summary.pop();
            }
            let mut p = Publish {
                recipients: vec![sender.name],
                key,
                summary,
                body,
                due_after: 900,
                reply_to: Some(id),
                work_id: sender.work_id,
            };
            Some(Self::publish_tx(tx, actor, &mut p, now).await?)
        } else {
            None
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
        Self::reset_empty(tx, &actor.group_name).await?;
        Ok(reply_id)
    }

    pub async fn withdraw(&self, actor: &Mailbox, id: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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
                crate::now()?,
            )
            .await?;
        }
        Self::reset_empty(&mut tx, &actor.group_name).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn reset_empty(tx: &mut Transaction<'_, Sqlite>, group: &str) -> Result<()> {
        sqlx::query!("UPDATE mailboxes SET attempts=0,next_wake=0,alerted=0 WHERE group_name=? AND NOT EXISTS(SELECT 1 FROM deliveries WHERE recipient=mailboxes.id AND state='pending') AND NOT EXISTS(SELECT 1 FROM pending_work_events WHERE recipient=mailboxes.id)", group)
            .execute(&mut **tx).await?;
        Ok(())
    }

    pub async fn pending(&self) -> Result<Vec<Pending>> {
        Ok(sqlx::query_as!(Pending,
            "SELECT b.id AS 'id!: i64',b.group_name AS 'group_name!: String',b.name AS 'name!: String',COUNT(*) AS 'pending!: i64',MIN(m.created) AS 'oldest!: i64',MIN(m.due) AS 'due!: i64',b.attempts AS 'attempts!: i64',b.next_wake AS 'next_wake!: i64',b.alerted AS 'alerted!: i64' FROM mailboxes b JOIN (SELECT d.recipient,m.created,m.due FROM deliveries d JOIN messages m ON m.id=d.message WHERE d.state='pending' UNION ALL SELECT recipient,created,created+900 AS due FROM pending_work_events) m ON m.recipient=b.id WHERE b.remote_machine IS NULL GROUP BY b.id ORDER BY b.group_name,b.id")
            .fetch_all(&self.pool).await?)
    }

    pub async fn reserve(&self, actor: &Mailbox, now: i64) -> Result<bool> {
        let next = now.checked_add(300).context("clock overflow")?;
        let result = sqlx::query!("UPDATE mailboxes SET attempts=attempts+1,next_wake=? WHERE id=? AND binding_version=? AND pane IS NOT NULL AND attempts<3 AND next_wake<=? AND EXISTS(SELECT 1 FROM groups WHERE name=mailboxes.group_name AND paused=0 AND auto_prompt=1) AND (EXISTS(SELECT 1 FROM deliveries WHERE recipient=mailboxes.id AND state='pending') OR EXISTS(SELECT 1 FROM pending_work_events WHERE recipient=mailboxes.id))",
            next, actor.id, actor.binding_version, now).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn reserve_alert(&self, id: i64, now: i64) -> Result<bool> {
        let result = sqlx::query!("UPDATE mailboxes SET alerted=1 WHERE id=? AND alerted=0 AND (EXISTS(SELECT 1 FROM deliveries d JOIN messages m ON m.id=d.message WHERE d.recipient=mailboxes.id AND d.state='pending' AND (m.due<=? OR mailboxes.attempts>=3)) OR EXISTS(SELECT 1 FROM pending_work_events WHERE recipient=mailboxes.id AND (created+900<=? OR mailboxes.attempts>=3)))", id, now, now).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn rearm(&self, group: &str, participant: &str) -> Result<()> {
        let actor = self.mailbox(group, participant).await?;
        sqlx::query!(
            "UPDATE mailboxes SET attempts=0,next_wake=0,alerted=0 WHERE id=?",
            actor.id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
