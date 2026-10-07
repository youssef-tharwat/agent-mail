//! Bounded store-and-forward exchange over operator-configured SSH connections.
//!
//! Only a group's home machine initiates synchronization. Events carry stable UUIDs
//! for idempotent receipt, and acknowledgements are checked against their source.
//! Both subprocess output streams are bounded while reading, and exchange failures
//! kill and reap the child. Business operations accept Unix seconds from their caller.

use crate::mail_context::MessageContext;
use crate::states::{MessageIntent, MessageState};
use crate::{BODY_LIMIT, SUMMARY_LIMIT, bounded, name, store::Store, work::WorkItem};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, Transaction};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    time::timeout,
};
use uuid::Uuid;

// Keep each exchange small enough to fit the wire budget and avoid starving peers.
const BATCH_LIMIT: usize = 16;
const INTENT_CAPABILITY: &str = "communication_intent_v1";
const CONTEXT_CAPABILITY: &str = "mail_context_v1";
const TASK_GRAPH_CAPABILITY: &str = "task_graph_v1";

/// Capabilities advertised by this relay implementation.
pub fn capabilities() -> Vec<String> {
    vec![
        INTENT_CAPABILITY.into(),
        CONTEXT_CAPABILITY.into(),
        TASK_GRAPH_CAPABILITY.into(),
    ]
}
// Shared by encoding and streaming reads; changing this changes the relay wire contract.
const WIRE_LIMIT: usize = 256 * 1024;
// Diagnostics need only a short tail-sized budget, independent of message payloads.
const STDERR_LIMIT: usize = 16 * 1024;
// Fail unreachable hosts promptly; leave the rest of the exchange budget for SQL and transfer.
const SSH_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
// Bounds the entire write/read/wait exchange, allowing four connection-timeout intervals.
const SSH_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(20);

/// An idempotent relay event with explicit origin and destination machines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Stable UUID used to deduplicate this envelope.
    pub event_id: Uuid,
    /// UUID of the machine that originated the event.
    pub origin: Uuid,
    /// UUID of the machine that should receive the event.
    pub destination: Uuid,
    /// Business event carried by this envelope.
    pub event: Event,
}

/// A mail disposition or work snapshot exchanged between machines.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Event {
    /// Contextual communication; older relays reject it instead of discarding its subject.
    ContextMessage {
        /// Explicit communication meaning.
        intent: MessageIntent,
        /// Durable task revision or conversation this communication concerns.
        context: MessageContext,
        /// Original message content and request reference.
        message: WireMessage,
    },
    /// An explicit resolution of a received message.
    Resolution {
        /// Group owning the referenced message.
        group: String,
        /// Message UUID or protocol failure description.
        message: Uuid,
        /// Participant whose delivery disposition changed.
        recipient: String,
        /// Serialized explicit resolution recorded by the recipient.
        resolution: String,
    },
    /// A sender’s withdrawal of outstanding delivery.
    Withdrawal {
        /// Group owning the referenced message.
        group: String,
        /// Message UUID or protocol failure description.
        message: Uuid,
        /// Participant whose delivery disposition changed.
        recipient: String,
    },
    /// An authoritative work revision from the group home.
    WorkSnapshot(WorkItem),
    /// Group-visible immutable document revision and authorized references.
    RecordSnapshot(crate::records::RecordSnapshot),
    /// Typed resource metadata; payload transfer remains explicit.
    ArtifactSnapshot(crate::artifacts::ArtifactSnapshot),
    /// Explicit task relationship facts from the authoritative group home.
    TaskGraphSnapshot(crate::relationships::TaskGraphSnapshot),
}

/// Portable message content identified by UUIDs across installations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage {
    /// Persistent identifier for this record.
    pub id: Uuid,
    /// Enrolled group containing the referenced participant or record.
    pub group: String,
    /// Name of the participant that published this message.
    pub sender: String,
    /// Participant name receiving this event or delivery.
    pub recipient: String,
    /// Caller-supplied idempotency key; retries must preserve their original content.
    pub key: String,
    /// Short UTF-8 summary used in inbox and recovery views.
    pub summary: String,
    /// Full UTF-8 message body, subject to the message byte limit.
    pub body: String,
    /// Creation timestamp in Unix seconds.
    pub created: i64,
    /// Deadline timestamp in Unix seconds.
    pub due: Option<i64>,
    /// Optional work identifier associated with this message.
    pub work_id: Option<String>,
    /// Optional identifier of the message being answered.
    pub reply_to: Option<Uuid>,
}

/// Incoming envelopes and acknowledgements submitted atomically by a peer.
#[derive(Debug, Serialize, Deserialize)]
pub struct Exchange {
    /// Sender capabilities; legacy exchanges advertise none.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Bounded batch of envelopes to validate and apply.
    pub incoming: Vec<Envelope>,
    /// UUIDs of envelopes confirmed as received.
    pub ack: Vec<Uuid>,
}

/// The UUIDs accepted by a relay exchange.
#[derive(Debug, Serialize, Deserialize)]
pub struct Receipt {
    /// Receiver capabilities for subsequent typed traffic.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// UUIDs of envelopes confirmed as received.
    pub ack: Vec<Uuid>,
}

/// A configured peer’s queue size and latest synchronization result.
#[derive(Debug, Serialize)]
pub struct PeerStatus {
    /// Persistent UUID of the remote machine.
    pub machine_id: String,
    /// Restricted operator-configured SSH alias.
    pub ssh_target: String,
    /// Whether the worker may synchronize this peer automatically.
    pub auto_sync: bool,
    /// Most recent successful synchronization time in Unix seconds.
    pub last_sync: Option<i64>,
    /// Bounded description of the latest synchronization failure.
    pub last_error: Option<String>,
    /// Number of envelopes waiting for this peer.
    pub queued: i64,
    /// Earliest queued creation timestamp in Unix seconds, when available.
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
        "SELECT m.intent AS 'intent: MessageIntent',m.context AS 'context!: MessageContext',m.global_id,m.dedup_key,m.summary,m.body,m.created,m.deadline AS due,m.work_id,m.parent_global_id,s.name AS sender,s.group_name,r.name AS recipient,r.remote_machine FROM messages m JOIN mailboxes s ON s.id=m.sender JOIN mailboxes r ON r.id=? WHERE m.id=?",
        recipient_id,
        message_id
    )
    .fetch_one(&mut **tx)
    .await?;
    let Some(machine) = row.remote_machine else {
        return Ok(());
    };
    {
        let home = sqlx::query_scalar!(
            "SELECT home_machine FROM groups WHERE name=?",
            row.group_name
        )
        .fetch_one(&mut **tx)
        .await?;
        let next_hop = if home == local_id(tx).await?.to_string() {
            machine.clone()
        } else {
            home
        };
        ensure!(
            sqlx::query_scalar!(
                "SELECT EXISTS(SELECT 1 FROM relay_capabilities WHERE machine=? AND capability=?)",
                next_hop,
                CONTEXT_CAPABILITY
            )
            .fetch_one(&mut **tx)
            .await?
                != 0,
            "next relay hop has not advertised mail context support; sync the upgraded peer first"
        );
    }
    let reply_to = row.parent_global_id.as_deref().map(parse_id).transpose()?;
    let message = WireMessage {
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
    };
    let event = Event::ContextMessage {
        intent: row.intent,
        context: row.context,
        message,
    };
    enqueue(tx, parse_id(&machine)?, event, created).await
}

pub(crate) async fn enqueue_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    item: &WorkItem,
    _previous_owner: Option<&str>,
    now: i64,
) -> Result<()> {
    let rows = sqlx::query!(
        "SELECT DISTINCT remote_machine FROM mailboxes WHERE group_name=? AND remote_machine IS NOT NULL",
        item.group_name
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
    /// Join an empty group to a remote home machine.
    ///
    /// # Errors
    /// The home conflicts, the group contains local work or mail, or persistence fails.
    pub async fn set_home(&self, group: &str, home: Uuid) -> Result<()> {
        name(group)?;
        let local = parse_id(&self.machine_id().await?)?;
        ensure!(home != local, "this machine already owns the group");
        let mut tx = self.pool().begin().await?;
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
        let coordination: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM shared_records WHERE group_name=?) OR EXISTS(SELECT 1 FROM artifacts WHERE group_name=?)")
            .bind(group).bind(group).fetch_one(&mut *tx).await?;
        ensure!(
            !coordination,
            "cannot move a group with shared records or artifacts"
        );
        let home = home.to_string();
        sqlx::query!("UPDATE groups SET home_machine=? WHERE name=?", home, group)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Configure a remote machine’s restricted SSH alias.
    ///
    /// # Errors
    /// The machine is local, the alias is invalid, or persistence fails.
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
            machine, ssh_target).execute(self.pool()).await?;
        Ok(())
    }

    /// Set whether the worker automatically synchronizes a configured peer.
    ///
    /// # Errors
    /// The peer is absent, enabling is not on a group home machine, or persistence fails.
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
        .execute(self.pool())
        .await?;
        ensure!(result.rows_affected() == 1, "peer is not configured");
        Ok(())
    }

    /// Register a remote participant and queue applicable work snapshots.
    ///
    /// # Errors
    /// The route is local or conflicts, the group is missing, or persistence fails.
    pub async fn route(
        &self,
        group: &str,
        participant: &str,
        machine: Uuid,
        time: i64,
    ) -> Result<()> {
        name(participant)?;
        self.group(group).await?;
        ensure!(
            machine != parse_id(&self.machine_id().await?)?,
            "use bind for a local mailbox"
        );
        let machine = machine.to_string();
        let mut tx = self.pool().begin().await?;
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
            crate::records::enqueue_records_for_route(&mut tx, group, parse_id(&machine)?, time)
                .await?;
            crate::relationships::enqueue_graphs_for_route(
                &mut tx,
                group,
                parse_id(&machine)?,
                time,
            )
            .await?;
            crate::artifacts::enqueue_artifacts_for_route(
                &mut tx,
                group,
                parse_id(&machine)?,
                time,
            )
            .await?;
            let latest = sqlx::query!("SELECT c.snapshot FROM work_changes c WHERE c.group_name=? AND c.version=(SELECT MAX(version) FROM work_changes WHERE group_name=c.group_name AND work_id=c.work_id)", group)
                .fetch_all(&mut *tx).await?;
            for row in latest {
                let item: WorkItem = serde_json::from_str(&row.snapshot)?;
                {
                    enqueue(
                        &mut tx,
                        parse_id(&machine)?,
                        Event::WorkSnapshot(item),
                        time,
                    )
                    .await?;
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// List configured peers, queued events, and last synchronization results.
    ///
    /// # Errors
    /// The database query fails.
    pub async fn peers_status(&self) -> Result<Vec<PeerStatus>> {
        Ok(sqlx::query_as!(PeerStatus,
            "SELECT p.machine_id AS 'machine_id!',p.ssh_target AS 'ssh_target!',p.auto_sync AS 'auto_sync!: bool',p.last_sync,p.last_error,COUNT(o.event_id) AS 'queued!: i64',MIN(o.created) AS oldest FROM peers p LEFT JOIN outbox o ON o.dest_machine=p.machine_id GROUP BY p.machine_id ORDER BY p.machine_id")
            .fetch_all(self.pool()).await?)
    }

    /// Return the queued event count and oldest creation timestamp.
    ///
    /// # Errors
    /// The database query fails.
    pub async fn outbox_status(&self) -> Result<(i64, Option<i64>)> {
        let row =
            sqlx::query!("SELECT COUNT(*) AS 'queued!: i64', MIN(created) AS oldest FROM outbox")
                .fetch_one(self.pool())
                .await?;
        Ok((row.queued, row.oldest))
    }

    /// Export a bounded batch of queued envelopes.
    ///
    /// # Errors
    /// The query fails or a stored envelope cannot be decoded.
    pub async fn export(&self) -> Result<Vec<Envelope>> {
        let rows = sqlx::query!(
            "SELECT payload FROM outbox ORDER BY rowid LIMIT ?",
            BATCH_LIMIT as i64
        )
        .fetch_all(self.pool())
        .await?;
        let mut events = Vec::new();
        for row in rows {
            let event: Envelope = serde_json::from_str(&row.payload)?;
            events.push(event);
            // Reserve space for acknowledgements and the Exchange JSON envelope.
            if serde_json::to_vec(&events)?.len() > WIRE_LIMIT - 4096 {
                events.pop();
                break;
            }
        }
        Ok(events)
    }

    /// Export a bounded batch of envelopes destined for one machine.
    ///
    /// # Errors
    /// The query fails or a stored envelope cannot be decoded.
    pub async fn export_for(&self, machine: Uuid) -> Result<Vec<Envelope>> {
        let machine = machine.to_string();
        let rows = sqlx::query!(
            "SELECT payload FROM outbox WHERE dest_machine=? ORDER BY rowid LIMIT ?",
            machine,
            BATCH_LIMIT as i64
        )
        .fetch_all(self.pool())
        .await?;
        let mut events = Vec::new();
        for row in rows {
            let event: Envelope = serde_json::from_str(&row.payload)?;
            events.push(event);
            // Reserve space for acknowledgements and the Exchange JSON envelope.
            if serde_json::to_vec(&events)?.len() > WIRE_LIMIT - 4096 {
                events.pop();
                break;
            }
        }
        Ok(events)
    }

    /// Apply validated incoming events and acknowledgements in one transaction.
    ///
    /// # Errors
    /// Source authority, event content, or acknowledgement validation fails, or persistence fails.
    pub async fn exchange(&self, source: Uuid, exchange: Exchange, time: i64) -> Result<Receipt> {
        ensure!(
            exchange.incoming.len() <= BATCH_LIMIT && exchange.ack.len() <= BATCH_LIMIT,
            "relay batch exceeds limit"
        );
        let local = parse_id(&self.machine_id().await?)?;
        let mut tx = self.pool().begin().await?;
        let mut accepted = Vec::new();
        ensure!(
            exchange.capabilities.len() <= 16
                && exchange.capabilities.iter().all(|c| c.len() <= 128),
            "invalid relay capabilities"
        );
        let source_id = source.to_string();
        sqlx::query!("DELETE FROM relay_capabilities WHERE machine=?", source_id)
            .execute(&mut *tx)
            .await?;
        for capability in [INTENT_CAPABILITY, CONTEXT_CAPABILITY, TASK_GRAPH_CAPABILITY] {
            if !exchange.capabilities.iter().any(|c| c == capability) {
                continue;
            }
            sqlx::query!(
                "INSERT INTO relay_capabilities(machine,capability) VALUES(?,?)",
                source_id,
                capability
            )
            .execute(&mut *tx)
            .await?;
        }
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
        crate::stream::hint(self.root()).await;
        Ok(Receipt {
            ack: accepted,
            capabilities: capabilities(),
        })
    }

    /// Synchronize one configured peer using the supplied Unix timestamp.
    ///
    /// # Errors
    /// This is not a home machine, peer I/O or validation fails, or persistence fails.
    pub async fn sync_peer(&self, peer: Uuid, time: i64) -> Result<usize> {
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
            .fetch_one(self.pool())
            .await?
            .ssh_target;
        let result = self.sync_peer_inner(local, peer, &target, time).await;
        let (last_sync, last_error) = match &result {
            Ok(_) => (Some(time), None),
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
        .execute(self.pool())
        .await?;
        result
    }

    async fn sync_peer_inner(
        &self,
        local: Uuid,
        peer: Uuid,
        target: &str,
        time: i64,
    ) -> Result<usize> {
        // Negotiate against the actual peer before importing or exporting typed traffic.
        let handshake = serde_json::to_vec(&Exchange {
            capabilities: capabilities(),
            incoming: vec![],
            ack: vec![],
        })?;
        let bytes = ssh(
            target,
            &[
                "adapter",
                "bridge",
                "exchange",
                "--source",
                &local.to_string(),
            ],
            Some(&handshake),
        )
        .await?;
        let support: Receipt = serde_json::from_slice(&bytes)?;
        ensure!(
            support.ack.is_empty(),
            "peer acknowledged an unsent event during negotiation"
        );
        // The remote command is fixed; SSH aliases are restricted to plain names.
        let bytes = ssh(target, &["adapter", "bridge", "export"], None).await?;
        let remote: Vec<Envelope> = serde_json::from_slice(&bytes)?;
        ensure!(
            remote.len() <= BATCH_LIMIT,
            "remote exported too many events"
        );
        let received = self
            .exchange(
                peer,
                Exchange {
                    capabilities: support.capabilities,
                    incoming: remote,
                    ack: vec![],
                },
                time,
            )
            .await?;
        let outgoing = self.export_for(peer).await?;
        if outgoing
            .iter()
            .any(|e| matches!(e.event, Event::TaskGraphSnapshot(_)))
        {
            ensure!(sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM relay_capabilities WHERE machine=? AND capability=?)")
                .bind(peer.to_string()).bind(TASK_GRAPH_CAPABILITY).fetch_one(self.pool()).await?,
                "peer does not support task graphs; upgrade it before synchronizing dependency plans");
        }
        let sent = outgoing.len();
        let sent_ids: std::collections::HashSet<_> =
            outgoing.iter().map(|event| event.event_id).collect();
        let request = Exchange {
            capabilities: capabilities(),
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
            &[
                "adapter",
                "bridge",
                "exchange",
                "--source",
                &local.to_string(),
            ],
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
                capabilities: receipt.capabilities,
                incoming: vec![],
                ack: receipt.ack,
            },
            time,
        )
        .await?;
        Ok(sent)
    }
}

fn event_group(event: &Event) -> &str {
    match event {
        Event::ContextMessage { message: m, .. } => &m.group,
        Event::Resolution { group, .. } | Event::Withdrawal { group, .. } => group,
        Event::WorkSnapshot(item) => &item.group_name,
        Event::RecordSnapshot(item) => &item.record.group_name,
        Event::ArtifactSnapshot(item) => &item.artifact.group_name,
        Event::TaskGraphSnapshot(item) => &item.group_name,
    }
}

async fn validate_context_tx(
    tx: &mut Transaction<'_, Sqlite>,
    message: &WireMessage,
    context: &MessageContext,
) -> Result<()> {
    if let MessageContext::Task { id, .. } = context {
        ensure!(
            message.work_id.as_deref() == Some(id.as_str()),
            "task context does not match the message association"
        );
        crate::mail_context::validate_task_tx(tx, &message.group, context).await?;
    }
    Ok(())
}

async fn validate_forward(tx: &mut Transaction<'_, Sqlite>, envelope: &Envelope) -> Result<()> {
    match &envelope.event {
        Event::ContextMessage {
            message: m,
            intent,
            context,
        } => {
            name(&m.group)?;
            name(&m.sender)?;
            name(&m.recipient)?;
            {
                ensure!(
                    intent.is_request() || m.due.is_none(),
                    "only requests may carry a deadline"
                );
                ensure!(
                    *intent != MessageIntent::Response || m.reply_to.is_some(),
                    "response has no referenced request"
                );
                let destination = envelope.destination.to_string();
                ensure!(sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM relay_capabilities WHERE machine=? AND capability=?)",destination,CONTEXT_CAPABILITY).fetch_one(&mut **tx).await? != 0,"destination has not negotiated mail context; synchronize upgraded peers first");
            }
            validate_context_tx(tx, m, context).await?;
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
        Event::RecordSnapshot(_)
        | Event::ArtifactSnapshot(_)
        | Event::TaskGraphSnapshot(_)
        | Event::WorkSnapshot(_) => anyhow::bail!("remote node cannot publish work snapshots"),
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
        Event::ContextMessage {
            message: m,
            intent,
            context,
        } => {
            let intent = *intent;
            ensure!(
                intent.is_request() || m.due.is_none(),
                "only requests may carry a deadline"
            );
            ensure!(
                intent != MessageIntent::Response || m.reply_to.is_some(),
                "response has no referenced request"
            );
            name(&m.sender)?;
            name(&m.recipient)?;
            validate_context_tx(tx, m, context).await?;
            bounded(&m.key, 128, "send key")?;
            bounded(&m.summary, SUMMARY_LIMIT, "summary")?;
            bounded(&m.body, BODY_LIMIT, "body")?;
            ensure!(
                m.created > 0 && m.due.is_none_or(|due| due >= m.created),
                "invalid message time"
            );
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
            let original = (
                m.id, &m.group, &m.sender, &m.key, &m.summary, &m.body, m.created, m.due,
                &m.work_id, m.reply_to,
            );
            let canonical = if intent.is_request() {
                serde_json::to_string(&original)?
            } else {
                serde_json::to_string(&(intent, original))?
            };
            let context_text = serde_json::to_string(context)?;
            let existing = sqlx::query!("SELECT id,canonical,context AS 'context!: MessageContext' FROM messages WHERE global_id=?", id)
                .fetch_optional(&mut **tx)
                .await?;
            let message_id = if let Some(existing) = existing {
                ensure!(
                    existing.canonical == canonical && existing.context == *context,
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
                let storage_due = m.due.unwrap_or(m.created);
                if intent == MessageIntent::Response {
                    ensure!(sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM messages m JOIN deliveries d ON d.message=m.id WHERE m.id=? AND m.sender=? AND d.recipient=? AND m.intent='request')",reply_to,destination.id,sender_id).fetch_one(&mut **tx).await? != 0,"response does not match the requester and responder");
                }
                if let Some(parent) = reply_to {
                    let stored: String =
                        sqlx::query_scalar("SELECT context FROM messages WHERE id=?")
                            .bind(parent)
                            .fetch_one(&mut **tx)
                            .await?;
                    ensure!(
                        serde_json::from_str::<MessageContext>(&stored)? == *context,
                        "reply context does not match its parent"
                    );
                }
                let intent = intent.as_str();
                let parent_global_id = m.reply_to.map(|id| id.to_string());
                sqlx::query!("INSERT INTO messages(sender,dedup_key,canonical,summary,body,created,due,reply_to,work_id,global_id,deadline,intent,context,parent_global_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    sender_id, key, canonical, m.summary, m.body, m.created, storage_due, reply_to, m.work_id, id, m.due,intent,context_text,parent_global_id)
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
            ensure!(result.rows_affected() == 1 || sqlx::query!("SELECT d.state AS 'state: MessageState' FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient WHERE m.global_id=? AND b.group_name=? AND b.name=?", id, group, recipient)
                .fetch_optional(&mut **tx).await?.is_some_and(|r| r.state == MessageState::Resolved), "resolution has no pending delivery");
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
        Event::RecordSnapshot(item) => {
            ensure!(
                envelope.origin == home,
                "record snapshot must come from home"
            );
            crate::records::apply_record_snapshot(tx, item).await?;
        }
        Event::TaskGraphSnapshot(item) => {
            ensure!(
                envelope.origin == home,
                "relationship snapshot must come from home"
            );
            ensure!(sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM relay_capabilities WHERE machine=? AND capability=?)")
                .bind(envelope.origin.to_string()).bind(TASK_GRAPH_CAPABILITY).fetch_one(&mut **tx).await?,
                "home has not negotiated task graph support; upgrade and synchronize it first");
            crate::relationships::apply_graph_snapshot_tx(tx, item, time).await?;
        }
        Event::ArtifactSnapshot(item) => {
            ensure!(
                envelope.origin == home,
                "artifact snapshot must come from home"
            );
            crate::artifacts::apply_snapshot(tx, item).await?;
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
    let child = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            &format!("ConnectTimeout={}", SSH_CONNECT_TIMEOUT.as_secs()),
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
    collect_output(child, input).await
}

async fn read_bounded(
    reader: impl AsyncRead + Unpin,
    limit: usize,
    label: &str,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= limit, "SSH {label} exceeds limit");
    Ok(bytes)
}

async fn collect_output(mut child: Child, input: Option<&[u8]>) -> Result<Vec<u8>> {
    let result = timeout(SSH_EXCHANGE_TIMEOUT, async {
        let stdout = child.stdout.take().context("missing SSH stdout")?;
        let stderr = child.stderr.take().context("missing SSH stderr")?;
        let stdin = child.stdin.take();
        // Drain both pipes while writing input: a peer may respond before consuming stdin.
        let write = async {
            if let Some(input) = input {
                let mut stdin = stdin.context("missing SSH stdin")?;
                stdin.write_all(input).await?;
                stdin.shutdown().await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let (_, stdout, stderr, status) = tokio::try_join!(
            write,
            read_bounded(stdout, WIRE_LIMIT, "relay response"),
            read_bounded(stderr, STDERR_LIMIT, "diagnostics"),
            async { Ok::<_, anyhow::Error>(child.wait().await?) },
        )?;
        ensure!(
            status.success(),
            "SSH relay failed: {}",
            String::from_utf8_lossy(&stderr)
        );
        Ok(stdout)
    })
    .await
    .context("SSH relay exchange timed out")
    .and_then(|result| result);
    if result.is_err() {
        // Reap the process on size, I/O, and timeout failures; kill_on_drop also
        // covers cancellation of this future by the worker.
        let _ = child.kill().await;
    }
    result
}

/// Decode a relay exchange within the protocol byte budget.
///
/// # Errors
/// Input exceeds the wire limit or is not a valid exchange.
pub fn decode_exchange(input: &[u8]) -> Result<Exchange> {
    ensure!(
        input.len() <= WIRE_LIMIT,
        "relay request exceeds wire limit"
    );
    Ok(serde_json::from_slice(input)?)
}

/// Parse a machine identifier as a UUID.
///
/// # Errors
/// The string is not a valid UUID.
pub fn machine(value: &str) -> Result<Uuid> {
    parse_id(value)
}

/// Queue group-visible artifact metadata to each configured group route.
pub(crate) async fn enqueue_artifact_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    snapshot: &crate::artifacts::ArtifactSnapshot,
    now: i64,
) -> Result<()> {
    let routes: Vec<String> = sqlx::query_scalar("SELECT DISTINCT remote_machine FROM mailboxes WHERE group_name=? AND remote_machine IS NOT NULL")
        .bind(&snapshot.artifact.group_name).fetch_all(&mut **tx).await?;
    for machine in routes {
        enqueue(
            tx,
            parse_id(&machine)?,
            Event::ArtifactSnapshot(snapshot.clone()),
            now,
        )
        .await?;
    }
    Ok(())
}

/// Queue relationship metadata to every configured group route.
pub(crate) async fn enqueue_graph_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    snapshot: &crate::relationships::TaskGraphSnapshot,
    now: i64,
) -> Result<()> {
    let routes: Vec<String> = sqlx::query_scalar("SELECT DISTINCT remote_machine FROM mailboxes WHERE group_name=? AND remote_machine IS NOT NULL")
        .bind(&snapshot.group_name).fetch_all(&mut **tx).await?;
    for machine in routes {
        enqueue(
            tx,
            parse_id(&machine)?,
            Event::TaskGraphSnapshot(snapshot.clone()),
            now,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child(script: &str) -> Result<Child> {
        Ok(Command::new("sh")
            .args(["-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?)
    }

    #[tokio::test]
    async fn rejects_unending_stdout_and_stderr_before_deadline() -> Result<()> {
        for script in ["exec yes oversized", "exec yes oversized >&2"] {
            let error = timeout(Duration::from_secs(3), collect_output(child(script)?, None))
                .await?
                .unwrap_err();
            assert!(error.to_string().contains("exceeds limit"), "{error:#}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn drains_output_while_writing_input() -> Result<()> {
        let input = vec![b'x'; 100_000];
        let output = timeout(
            Duration::from_secs(3),
            collect_output(child("head -c 100000 /dev/zero; cat")?, Some(&input)),
        )
        .await??;
        assert_eq!(output.len(), 200_000);
        assert!(output[..100_000].iter().all(|byte| *byte == 0));
        assert_eq!(&output[100_000..], &input);
        Ok(())
    }

    #[tokio::test]
    async fn retains_bounded_remote_failure_diagnostics() -> Result<()> {
        let error = collect_output(child("printf 'permission denied' >&2; exit 7")?, None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("permission denied"));
        Ok(())
    }

    #[tokio::test]
    async fn accepts_exact_wire_limit_and_rejects_one_more_byte() -> Result<()> {
        let bytes = vec![0; WIRE_LIMIT + 1];
        assert_eq!(
            read_bounded(&bytes[..WIRE_LIMIT], WIRE_LIMIT, "test")
                .await?
                .len(),
            WIRE_LIMIT
        );
        assert!(
            read_bounded(bytes.as_slice(), WIRE_LIMIT, "test")
                .await
                .is_err()
        );
        Ok(())
    }
}
