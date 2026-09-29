//! Bounded store-and-forward exchange over an operator-configured SSH connection.

use crate::{BODY_LIMIT, SUMMARY_LIMIT, bounded, name, now, store::Store, work::WorkItem};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, Transaction};
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};
use uuid::Uuid;

const BATCH_LIMIT: usize = 16;
const WIRE_LIMIT: usize = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub event_id: Uuid,
    pub origin: Uuid,
    pub destination: Uuid,
    pub event: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Event {
    Message(WireMessage),
    Resolution {
        group: String,
        message: Uuid,
        recipient: String,
        resolution: String,
    },
    Withdrawal {
        group: String,
        message: Uuid,
        recipient: String,
    },
    WorkSnapshot(WorkItem),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage {
    pub id: Uuid,
    pub group: String,
    pub sender: String,
    pub recipient: String,
    pub key: String,
    pub summary: String,
    pub body: String,
    pub created: i64,
    pub due: i64,
    pub work_id: Option<String>,
    pub reply_to: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Exchange {
    pub incoming: Vec<Envelope>,
    pub ack: Vec<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub ack: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct PeerStatus {
    pub machine_id: String,
    pub ssh_target: String,
    pub auto_sync: bool,
    pub last_sync: Option<i64>,
    pub last_error: Option<String>,
    pub queued: i64,
    pub oldest: Option<i64>,
}

fn parse_id(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).context("invalid machine or event UUID")
}

async fn local_id(tx: &mut Transaction<'_, Sqlite>) -> Result<Uuid> {
    let row = sqlx::query!("SELECT id FROM node LIMIT 1")
        .fetch_one(&mut **tx)
        .await?;
    parse_id(&row.id)
}

pub(crate) async fn enqueue(
    tx: &mut Transaction<'_, Sqlite>,
    destination: Uuid,
    event: Event,
    created: i64,
) -> Result<()> {
    let envelope = Envelope {
        event_id: Uuid::new_v4(),
        origin: local_id(tx).await?,
        destination,
        event,
    };
    persist_outbox(tx, &envelope, created).await
}

async fn persist_outbox(
    tx: &mut Transaction<'_, Sqlite>,
    envelope: &Envelope,
    created: i64,
) -> Result<()> {
    let payload = serde_json::to_string(envelope)?;
    ensure!(
        payload.len() <= WIRE_LIMIT,
        "relay event exceeds wire limit"
    );
    let event_id = envelope.event_id.to_string();
    let destination = envelope.destination.to_string();
    sqlx::query!(
        "INSERT OR IGNORE INTO outbox(event_id,dest_machine,payload,created) VALUES (?,?,?,?)",
        event_id,
        destination,
        payload,
        created
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn enqueue_message(
    tx: &mut Transaction<'_, Sqlite>,
    message_id: i64,
    recipient_id: i64,
    created: i64,
) -> Result<()> {
    let row = sqlx::query!(
        "SELECT m.global_id,m.dedup_key,m.summary,m.body,m.created,m.due,m.work_id,m.reply_to,s.name AS sender,s.group_name,r.name AS recipient,r.remote_machine FROM messages m JOIN mailboxes s ON s.id=m.sender JOIN mailboxes r ON r.id=? WHERE m.id=?",
        recipient_id,
        message_id
    )
    .fetch_one(&mut **tx)
    .await?;
    let Some(machine) = row.remote_machine else {
        return Ok(());
    };
    let reply_to = if let Some(id) = row.reply_to {
        sqlx::query!("SELECT global_id FROM messages WHERE id=?", id)
            .fetch_optional(&mut **tx)
            .await?
            .and_then(|r| r.global_id)
            .map(|s| parse_id(&s))
            .transpose()?
    } else {
        None
    };
    enqueue(
        tx,
        parse_id(&machine)?,
        Event::Message(WireMessage {
            id: parse_id(
                row.global_id
                    .as_deref()
                    .context("message has no global ID")?,
            )?,
            group: row.group_name,
            sender: row.sender,
            recipient: row.recipient,
            key: row.dedup_key,
            summary: row.summary,
            body: row.body,
            created: row.created,
            due: row.due,
            work_id: row.work_id,
            reply_to,
        }),
        created,
    )
    .await
}

pub(crate) async fn enqueue_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    item: &WorkItem,
    previous_owner: Option<&str>,
    now: i64,
) -> Result<()> {
    let previous_owner = previous_owner.unwrap_or(&item.owner);
    let rows = sqlx::query!(
        "SELECT DISTINCT remote_machine FROM mailboxes WHERE group_name=? AND (name=? OR name=?) AND remote_machine IS NOT NULL",
        item.group_name, item.owner, previous_owner
    )
    .fetch_all(&mut **tx)
    .await?;
    for row in rows {
        if let Some(machine) = row.remote_machine {
            enqueue(
                tx,
                parse_id(&machine)?,
                Event::WorkSnapshot(item.clone()),
                now,
            )
            .await?;
        }
    }
    Ok(())
}

impl Store {
    pub async fn set_home(&self, group: &str, home: Uuid) -> Result<()> {
        name(group)?;
        let local = parse_id(&self.machine_id().await?)?;
        ensure!(home != local, "this machine already owns the group");
        let mut tx = self.pool.begin().await?;
        let record = sqlx::query!("SELECT home_machine FROM groups WHERE name=?", group)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            record.home_machine == local.to_string() || record.home_machine == home.to_string(),
            "group already joined to another home"
        );
        ensure!(
            sqlx::query!(
                "SELECT id FROM work_items WHERE group_name=? LIMIT 1",
                group
            )
            .fetch_optional(&mut *tx)
            .await?
            .is_none(),
            "cannot move a group with local work records"
        );
        ensure!(
            sqlx::query!("SELECT m.id FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE b.group_name=? LIMIT 1", group)
                .fetch_optional(&mut *tx).await?.is_none(),
            "cannot move a group after messages were published"
        );
        let home = home.to_string();
        sqlx::query!("UPDATE groups SET home_machine=? WHERE name=?", home, group)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn add_peer(&self, machine: Uuid, ssh_target: &str) -> Result<()> {
        ensure!(
            machine != parse_id(&self.machine_id().await?)?,
            "peer cannot be this machine"
        );
        name(ssh_target)?;
        ensure!(
            ssh_target.as_bytes()[0].is_ascii_alphanumeric(),
            "SSH alias must start with a letter or digit"
        );
        let machine = machine.to_string();
        sqlx::query!("INSERT INTO peers(machine_id,ssh_target) VALUES (?,?) ON CONFLICT(machine_id) DO UPDATE SET auto_sync=CASE WHEN peers.ssh_target=excluded.ssh_target THEN peers.auto_sync ELSE 0 END,last_sync=CASE WHEN peers.ssh_target=excluded.ssh_target THEN peers.last_sync ELSE NULL END,last_error=NULL,ssh_target=excluded.ssh_target",
            machine, ssh_target).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn set_auto_sync(&self, machine: Uuid, enabled: bool) -> Result<()> {
        if enabled {
            let local = self.machine_id().await?;
            let groups = self.groups().await?;
            ensure!(
                !groups.is_empty() && groups.iter().all(|group| group.home_machine == local),
                "automatic sync can be enabled only on a home machine"
            );
        }
        let machine = machine.to_string();
        let result = sqlx::query!(
            "UPDATE peers SET auto_sync=? WHERE machine_id=?",
            enabled,
            machine
        )
        .execute(&self.pool)
        .await?;
        ensure!(result.rows_affected() == 1, "peer is not configured");
        Ok(())
    }

    pub async fn route(&self, group: &str, participant: &str, machine: Uuid) -> Result<()> {
        name(participant)?;
        self.group(group).await?;
        ensure!(
            machine != parse_id(&self.machine_id().await?)?,
            "use bind for a local mailbox"
        );
        let machine = machine.to_string();
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query!(
            "SELECT remote_machine FROM mailboxes WHERE group_name=? AND name=?",
            group,
            participant
        )
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(existing) = existing {
            ensure!(
                existing.remote_machine.as_deref() == Some(machine.as_str()),
                "route already belongs to another machine; reconcile explicitly"
            );
        } else {
            sqlx::query!("INSERT INTO mailboxes(group_name,name,binding) VALUES (?,?,json_object('runtime','remote','machine',?))",
                group, participant, machine).execute(&mut *tx).await?;
            let latest = sqlx::query!("SELECT c.snapshot FROM work_changes c WHERE c.group_name=? AND c.version=(SELECT MAX(version) FROM work_changes WHERE group_name=c.group_name AND work_id=c.work_id)", group)
                .fetch_all(&mut *tx).await?;
            for row in latest {
                let item: WorkItem = serde_json::from_str(&row.snapshot)?;
                if item.owner == participant {
                    enqueue(
                        &mut tx,
                        parse_id(&machine)?,
                        Event::WorkSnapshot(item),
                        now()?,
                    )
                    .await?;
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn peers_status(&self) -> Result<Vec<PeerStatus>> {
        Ok(sqlx::query_as!(PeerStatus,
            "SELECT p.machine_id AS 'machine_id!',p.ssh_target AS 'ssh_target!',p.auto_sync AS 'auto_sync!: bool',p.last_sync,p.last_error,COUNT(o.event_id) AS 'queued!: i64',MIN(o.created) AS oldest FROM peers p LEFT JOIN outbox o ON o.dest_machine=p.machine_id GROUP BY p.machine_id ORDER BY p.machine_id")
            .fetch_all(&self.pool).await?)
    }

    pub async fn outbox_status(&self) -> Result<(i64, Option<i64>)> {
        let row =
            sqlx::query!("SELECT COUNT(*) AS 'queued!: i64', MIN(created) AS oldest FROM outbox")
                .fetch_one(&self.pool)
                .await?;
        Ok((row.queued, row.oldest))
    }

    pub async fn export(&self) -> Result<Vec<Envelope>> {
        let rows = sqlx::query!(
            "SELECT payload FROM outbox ORDER BY rowid LIMIT ?",
            BATCH_LIMIT as i64
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(&row.payload).map_err(Into::into))
            .collect()
    }

    pub async fn export_for(&self, machine: Uuid) -> Result<Vec<Envelope>> {
        let machine = machine.to_string();
        let rows = sqlx::query!(
            "SELECT payload FROM outbox WHERE dest_machine=? ORDER BY rowid LIMIT ?",
            machine,
            BATCH_LIMIT as i64
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(&row.payload).map_err(Into::into))
            .collect()
    }

    pub async fn exchange(&self, source: Uuid, exchange: Exchange, time: i64) -> Result<Receipt> {
        ensure!(
            exchange.incoming.len() <= BATCH_LIMIT && exchange.ack.len() <= BATCH_LIMIT,
            "relay batch exceeds limit"
        );
        let local = parse_id(&self.machine_id().await?)?;
        let mut tx = self.pool.begin().await?;
        let mut accepted = Vec::new();
        for envelope in &exchange.incoming {
            ensure!(
                envelope.origin != local && envelope.destination != envelope.origin,
                "invalid relay origin or destination"
            );
            let group = event_group(&envelope.event);
            name(group)?;
            let home = sqlx::query!("SELECT home_machine FROM groups WHERE name=?", group)
                .fetch_one(&mut *tx)
                .await?
                .home_machine;
            let home = parse_id(&home)?;
            ensure!(
                source == home || (local == home && source == envelope.origin),
                "relay source is not this group's home or origin"
            );
            if local != home {
                ensure!(
                    envelope.destination == local,
                    "remote node received event for another machine"
                );
            }
            let event_id = envelope.event_id.to_string();
            if sqlx::query!(
                "SELECT event_id FROM seen_events WHERE event_id=?",
                event_id
            )
            .fetch_optional(&mut *tx)
            .await?
            .is_none()
            {
                if envelope.destination == local {
                    apply_event(&mut tx, envelope, home, time).await?;
                } else {
                    ensure!(local == home, "only home may forward events");
                    validate_forward(&mut tx, envelope).await?;
                    persist_outbox(&mut tx, envelope, time).await?;
                }
                sqlx::query!(
                    "INSERT INTO seen_events(event_id,received) VALUES (?,?)",
                    event_id,
                    time
                )
                .execute(&mut *tx)
                .await?;
            }
            accepted.push(envelope.event_id);
        }
        for id in exchange.ack {
            let id = id.to_string();
            if let Some(row) = sqlx::query!(
                "SELECT payload,dest_machine FROM outbox WHERE event_id=?",
                id
            )
            .fetch_optional(&mut *tx)
            .await?
            {
                let outbound: Envelope = serde_json::from_str(&row.payload)?;
                let group = event_group(&outbound.event);
                let home = sqlx::query!("SELECT home_machine FROM groups WHERE name=?", group)
                    .fetch_one(&mut *tx)
                    .await?
                    .home_machine;
                ensure!(
                    home == source.to_string()
                        || (home == local.to_string() && row.dest_machine == source.to_string()),
                    "acknowledgement came from the wrong machine"
                );
            }
            sqlx::query!("DELETE FROM outbox WHERE event_id=?", id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(Receipt { ack: accepted })
    }

    pub async fn sync_peer(&self, peer: Uuid) -> Result<usize> {
        let local = parse_id(&self.machine_id().await?)?;
        ensure!(
            self.groups()
                .await?
                .iter()
                .all(|group| group.home_machine == local.to_string()),
            "only a group home machine may initiate sync"
        );
        let peer_id = peer.to_string();
        let target = sqlx::query!("SELECT ssh_target FROM peers WHERE machine_id=?", peer_id)
            .fetch_one(&self.pool)
            .await?
            .ssh_target;
        let result = self.sync_peer_inner(local, peer, &target).await;
        let (last_sync, last_error) = match &result {
            Ok(_) => (Some(now()?), None),
            Err(e) => (
                None,
                Some(format!("{e:#}").chars().take(512).collect::<String>()),
            ),
        };
        sqlx::query!(
            "UPDATE peers SET last_sync=COALESCE(?,last_sync),last_error=? WHERE machine_id=?",
            last_sync,
            last_error,
            peer_id
        )
        .execute(&self.pool)
        .await?;
        result
    }

    async fn sync_peer_inner(&self, local: Uuid, peer: Uuid, target: &str) -> Result<usize> {
        // The remote command is fixed; SSH aliases are restricted to plain names.
        let bytes = ssh(target, &["bridge", "export"], None).await?;
        let remote: Vec<Envelope> = serde_json::from_slice(&bytes)?;
        ensure!(
            remote.len() <= BATCH_LIMIT,
            "remote exported too many events"
        );
        let received = self
            .exchange(
                peer,
                Exchange {
                    incoming: remote,
                    ack: vec![],
                },
                now()?,
            )
            .await?;
        let outgoing = self.export_for(peer).await?;
        let sent = outgoing.len();
        let sent_ids: std::collections::HashSet<_> =
            outgoing.iter().map(|event| event.event_id).collect();
        let request = Exchange {
            incoming: outgoing,
            ack: received.ack,
        };
        let bytes = serde_json::to_vec(&request)?;
        ensure!(
            bytes.len() <= WIRE_LIMIT,
            "relay request exceeds wire limit"
        );
        let response = ssh(
            target,
            &["bridge", "exchange", "--source", &local.to_string()],
            Some(&bytes),
        )
        .await?;
        let receipt: Receipt = serde_json::from_slice(&response)?;
        ensure!(
            receipt.ack.len() <= BATCH_LIMIT,
            "remote acknowledged too many events"
        );
        ensure!(
            receipt.ack.iter().all(|id| sent_ids.contains(id)),
            "remote acknowledged an unsent event"
        );
        self.exchange(
            peer,
            Exchange {
                incoming: vec![],
                ack: receipt.ack,
            },
            now()?,
        )
        .await?;
        Ok(sent)
    }
}

fn event_group(event: &Event) -> &str {
    match event {
        Event::Message(m) => &m.group,
        Event::Resolution { group, .. } | Event::Withdrawal { group, .. } => group,
        Event::WorkSnapshot(item) => &item.group_name,
    }
}

async fn validate_forward(tx: &mut Transaction<'_, Sqlite>, envelope: &Envelope) -> Result<()> {
    match &envelope.event {
        Event::Message(m) => {
            name(&m.group)?;
            name(&m.sender)?;
            name(&m.recipient)?;
            let sender = sqlx::query!(
                "SELECT remote_machine FROM mailboxes WHERE group_name=? AND name=?",
                m.group,
                m.sender
            )
            .fetch_one(&mut **tx)
            .await?;
            let recipient = sqlx::query!(
                "SELECT remote_machine FROM mailboxes WHERE group_name=? AND name=?",
                m.group,
                m.recipient
            )
            .fetch_one(&mut **tx)
            .await?;
            ensure!(
                sender.remote_machine.as_deref() == Some(envelope.origin.to_string().as_str()),
                "sender route does not match origin"
            );
            ensure!(
                recipient.remote_machine.as_deref()
                    == Some(envelope.destination.to_string().as_str()),
                "recipient route does not match destination"
            );
        }
        Event::Resolution {
            group, recipient, ..
        } => {
            name(recipient)?;
            let route = sqlx::query!(
                "SELECT remote_machine FROM mailboxes WHERE group_name=? AND name=?",
                group,
                recipient
            )
            .fetch_one(&mut **tx)
            .await?;
            ensure!(
                route.remote_machine.as_deref() == Some(envelope.origin.to_string().as_str()),
                "resolver route does not match origin"
            );
        }
        Event::Withdrawal {
            group, recipient, ..
        } => {
            name(recipient)?;
            let route = sqlx::query!(
                "SELECT remote_machine FROM mailboxes WHERE group_name=? AND name=?",
                group,
                recipient
            )
            .fetch_one(&mut **tx)
            .await?;
            ensure!(
                route.remote_machine.as_deref() == Some(envelope.destination.to_string().as_str()),
                "withdrawal recipient route does not match destination"
            );
        }
        Event::WorkSnapshot(_) => anyhow::bail!("remote node cannot publish work snapshots"),
    }
    Ok(())
}

async fn apply_event(
    tx: &mut Transaction<'_, Sqlite>,
    envelope: &Envelope,
    home: Uuid,
    time: i64,
) -> Result<()> {
    match &envelope.event {
        Event::Message(m) => {
            name(&m.sender)?;
            name(&m.recipient)?;
            bounded(&m.key, 128, "send key")?;
            bounded(&m.summary, SUMMARY_LIMIT, "summary")?;
            bounded(&m.body, BODY_LIMIT, "body")?;
            ensure!(m.created > 0 && m.due >= m.created, "invalid message time");
            let destination = sqlx::query!(
                "SELECT id,remote_machine FROM mailboxes WHERE group_name=? AND name=?",
                m.group,
                m.recipient
            )
            .fetch_one(&mut **tx)
            .await?;
            ensure!(
                destination.remote_machine.is_none(),
                "recipient is not local"
            );
            let sender = sqlx::query!(
                "SELECT id,remote_machine FROM mailboxes WHERE group_name=? AND name=?",
                m.group,
                m.sender
            )
            .fetch_optional(&mut **tx)
            .await?;
            let sender_id = if let Some(sender) = sender {
                ensure!(
                    sender.remote_machine.as_deref() == Some(envelope.origin.to_string().as_str()),
                    "sender route does not match origin"
                );
                sender.id
            } else {
                ensure!(envelope.destination != home, "home sender route is missing");
                let machine = envelope.origin.to_string();
                sqlx::query!("INSERT INTO mailboxes(group_name,name,binding) VALUES (?,?,json_object('runtime','remote','machine',?))",
                    m.group, m.sender, machine).execute(&mut **tx).await?.last_insert_rowid()
            };
            let id = m.id.to_string();
            let canonical = serde_json::to_string(&(
                m.id, &m.group, &m.sender, &m.key, &m.summary, &m.body, m.created, m.due,
                &m.work_id, m.reply_to,
            ))?;
            let existing = sqlx::query!("SELECT id,canonical FROM messages WHERE global_id=?", id)
                .fetch_optional(&mut **tx)
                .await?;
            let message_id = if let Some(existing) = existing {
                ensure!(
                    existing.canonical == canonical,
                    "global message ID has different content"
                );
                existing.id
            } else {
                let reply_to = if let Some(parent) = m.reply_to {
                    let parent_id = parent.to_string();
                    sqlx::query!("SELECT id FROM messages WHERE global_id=?", parent_id)
                        .fetch_optional(&mut **tx)
                        .await?
                        .map(|r| r.id)
                } else {
                    None
                };
                let key = format!("relay:{id}");
                sqlx::query!("INSERT INTO messages(sender,dedup_key,canonical,summary,body,created,due,reply_to,work_id,global_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    sender_id, key, canonical, m.summary, m.body, m.created, m.due, reply_to, m.work_id, id)
                    .execute(&mut **tx).await?.last_insert_rowid()
            };
            sqlx::query!(
                "INSERT OR IGNORE INTO deliveries(message,recipient) VALUES (?,?)",
                message_id,
                destination.id
            )
            .execute(&mut **tx)
            .await?;
        }
        Event::Resolution {
            group,
            message,
            recipient,
            resolution,
        } => {
            name(recipient)?;
            bounded(resolution, 64 * 1024, "resolution")?;
            let id = message.to_string();
            let result = sqlx::query!("UPDATE deliveries SET state='resolved',resolution=? WHERE message=(SELECT id FROM messages WHERE global_id=?) AND recipient=(SELECT id FROM mailboxes WHERE group_name=? AND name=?) AND state='pending'",
                resolution, id, group, recipient).execute(&mut **tx).await?;
            ensure!(result.rows_affected() == 1 || sqlx::query!("SELECT d.state FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient WHERE m.global_id=? AND b.group_name=? AND b.name=?", id, group, recipient)
                .fetch_optional(&mut **tx).await?.is_some_and(|r| r.state == "resolved"), "resolution has no pending delivery");
        }
        Event::Withdrawal {
            group,
            message,
            recipient,
        } => {
            name(recipient)?;
            let id = message.to_string();
            sqlx::query!("UPDATE deliveries SET state='withdrawn' WHERE message=(SELECT id FROM messages WHERE global_id=?) AND recipient=(SELECT id FROM mailboxes WHERE group_name=? AND name=?) AND state='pending'",
                id, group, recipient).execute(&mut **tx).await?;
        }
        Event::WorkSnapshot(item) => {
            ensure!(envelope.origin == home, "work snapshot must come from home");
            name(&item.group_name)?;
            name(&item.id)?;
            let snapshot = serde_json::to_string(item)?;
            ensure!(snapshot.len() <= 16 * 1024, "work snapshot too large");
            sqlx::query!("INSERT INTO work_snapshots(group_name,work_id,owner,snapshot,home_version,synced_at) VALUES (?,?,?,?,?,?) ON CONFLICT(group_name,work_id) DO UPDATE SET owner=excluded.owner,snapshot=excluded.snapshot,home_version=excluded.home_version,synced_at=excluded.synced_at WHERE excluded.home_version>=work_snapshots.home_version",
                item.group_name, item.id, item.owner, snapshot, item.version, time).execute(&mut **tx).await?;
        }
    }
    Ok(())
}

async fn ssh(target: &str, args: &[&str], input: Option<&[u8]>) -> Result<Vec<u8>> {
    name(target)?;
    ensure!(
        target.as_bytes()[0].is_ascii_alphanumeric(),
        "SSH alias must start with a letter or digit"
    );
    let mut child = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            target,
            "agent-mail",
        ])
        .args(args)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    if let Some(input) = input {
        timeout(
            Duration::from_secs(20),
            child
                .stdin
                .take()
                .context("missing SSH stdin")?
                .write_all(input),
        )
        .await??;
    }
    let output = timeout(Duration::from_secs(20), child.wait_with_output()).await??;
    ensure!(
        output.status.success(),
        "SSH relay failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(
        output.stdout.len() <= WIRE_LIMIT,
        "SSH relay response exceeds limit"
    );
    Ok(output.stdout)
}

pub fn decode_exchange(input: &[u8]) -> Result<Exchange> {
    ensure!(
        input.len() <= WIRE_LIMIT,
        "relay request exceeds wire limit"
    );
    Ok(serde_json::from_slice(input)?)
}

pub fn machine(value: &str) -> Result<Uuid> {
    parse_id(value)
}
