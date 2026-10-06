//! Explicit subjects for mail, with inherited replies and private conversation history.
//!
//! A context identifies a task revision or a durable conversation; it never grants
//! task authority or changes a message's communication intent. Conversation access
//! is derived from authored and addressed messages, without making bodies public.

use crate::{
    names::TaskId,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};
use std::{fmt, str::FromStr};
use uuid::Uuid;

/// Positive task version explicitly observed by the publisher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct TaskVersion(i64);
impl TaskVersion {
    /// Validate an observed task version.
    /// # Errors
    /// The version is not positive.
    pub fn new(value: i64) -> Result<Self> {
        ensure!(value > 0, "task version must be positive");
        Ok(Self(value))
    }
    /// The observed version number.
    pub fn get(self) -> i64 {
        self.0
    }
}
impl TryFrom<i64> for TaskVersion {
    type Error = anyhow::Error;
    fn try_from(value: i64) -> Result<Self> {
        Self::new(value)
    }
}
impl From<TaskVersion> for i64 {
    fn from(value: TaskVersion) -> Self {
        value.0
    }
}
impl FromStr for TaskVersion {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        Self::new(value.parse()?)
    }
}

/// Positive local message identifier used to inherit context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct MessageId(i64);
impl MessageId {
    /// Validate a local message identifier.
    /// # Errors
    /// The identifier is not positive.
    pub fn new(value: i64) -> Result<Self> {
        ensure!(value > 0, "message ID must be positive");
        Ok(Self(value))
    }
    /// The local identifier number.
    pub fn get(self) -> i64 {
        self.0
    }
}
impl TryFrom<i64> for MessageId {
    type Error = anyhow::Error;
    fn try_from(value: i64) -> Result<Self> {
        Self::new(value)
    }
}
impl From<MessageId> for i64 {
    fn from(value: MessageId) -> Self {
        value.0
    }
}
impl FromStr for MessageId {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        Self::new(value.parse()?)
    }
}

/// Stable conversation UUID shared across machines in the same group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct ConversationId(Uuid);
impl ConversationId {
    /// Validate a conversation UUID.
    /// # Errors
    /// The UUID is nil.
    pub fn new(value: Uuid) -> Result<Self> {
        ensure!(!value.is_nil(), "conversation ID must not be nil");
        Ok(Self(value))
    }
}
impl TryFrom<Uuid> for ConversationId {
    type Error = anyhow::Error;
    fn try_from(value: Uuid) -> Result<Self> {
        Self::new(value)
    }
}
impl From<ConversationId> for Uuid {
    fn from(value: ConversationId) -> Self {
        value.0
    }
}
impl FromStr for ConversationId {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        Self::new(value.parse()?)
    }
}
impl fmt::Display for ConversationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Durable subject of a message; independent of whether an answer is owed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageContext {
    /// Work and the specific version the publisher observed.
    Task {
        /// Group-scoped task identifier.
        id: TaskId,
        /// Version observed when composing the message.
        version: TaskVersion,
    },
    /// Discussion with no invented task or task revision.
    Conversation {
        /// Stable identity of this conversation.
        id: ConversationId,
    },
}
impl MessageContext {
    /// Task association for storage indexes and task scheduling.
    pub fn task_id(&self) -> Option<&TaskId> {
        match self {
            Self::Task { id, .. } => Some(id),
            Self::Conversation { .. } => None,
        }
    }
}
impl sqlx::Type<Sqlite> for MessageContext {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <String as sqlx::Type<Sqlite>>::type_info()
    }
    fn compatible(info: &sqlx::sqlite::SqliteTypeInfo) -> bool {
        <String as sqlx::Type<Sqlite>>::compatible(info)
    }
}
impl<'r> sqlx::Decode<'r, Sqlite> for MessageContext {
    fn decode(
        value: sqlx::sqlite::SqliteValueRef<'r>,
    ) -> std::result::Result<Self, sqlx::error::BoxDynError> {
        let text = <&str as sqlx::Decode<Sqlite>>::decode(value)?;
        Ok(serde_json::from_str(text)?)
    }
}

pub(crate) async fn validate_task_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    context: &MessageContext,
) -> Result<()> {
    if let MessageContext::Task { id, version } = context {
        let known: Option<i64> = sqlx::query_scalar("SELECT version FROM work_items WHERE group_name=? AND id=? UNION ALL SELECT home_version FROM work_snapshots WHERE group_name=? AND work_id=? ORDER BY 1 DESC LIMIT 1")
            .bind(group).bind(id.as_str()).bind(group).bind(id.as_str()).fetch_optional(&mut **tx).await?;
        ensure!(
            known.is_some_and(|known| version.get() <= known),
            "task is absent from this group or the observed version is not known"
        );
    }
    Ok(())
}

/// Required publication context, resolved atomically before a message is persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextSource {
    /// Explicitly observed task version.
    Task {
        /// Group-scoped task identifier.
        id: TaskId,
        /// Version observed by the sender.
        version: TaskVersion,
    },
    /// An existing conversation in which the sender participates.
    Conversation {
        /// Stable conversation identity.
        id: ConversationId,
    },
    /// Explicitly begin a discussion; identical retries reuse its original message.
    NewConversation,
    /// Inherit context from a message authored by or addressed to the sender.
    Reply {
        /// Local identifier of the parent message.
        message: MessageId,
    },
}
impl ContextSource {
    /// Referenced parent, when this publication inherits a context.
    pub fn reply_to(&self) -> Option<i64> {
        match self {
            Self::Reply { message } => Some(message.get()),
            _ => None,
        }
    }
}

pub(crate) struct ResolvedContext {
    pub context: MessageContext,
    pub reply_to: Option<i64>,
    pub parent_global_id: Option<String>,
    // Historical associations remain intact without claiming an observed revision.
    pub work_id: Option<String>,
}

pub(crate) async fn resolve_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    source: &ContextSource,
) -> Result<ResolvedContext> {
    let context = match source {
        ContextSource::Task { id, version } => {
            let context = MessageContext::Task {
                id: id.clone(),
                version: *version,
            };
            validate_task_tx(tx, &actor.group_name, &context).await?;
            context
        }
        ContextSource::Conversation { id } => {
            ensure!(
                conversation_visible_tx(tx, actor, *id).await?,
                "conversation is not available to this agent in this group"
            );
            MessageContext::Conversation { id: *id }
        }
        ContextSource::NewConversation => MessageContext::Conversation {
            id: ConversationId(Uuid::new_v4()),
        },
        ContextSource::Reply { message } => {
            let row = sqlx::query("SELECT m.context,m.work_id,m.global_id FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE m.id=? AND b.group_name=? AND (m.sender=? OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message=m.id AND d.recipient=?))")
                .bind(message.get()).bind(&actor.group_name).bind(actor.id).bind(actor.id)
                .fetch_optional(&mut **tx).await?.context("reply parent is not available to this agent in this group")?;
            return Ok(ResolvedContext {
                context: serde_json::from_str(row.get("context"))?,
                reply_to: Some(message.get()),
                parent_global_id: Some(
                    row.get::<Option<String>, _>("global_id")
                        .context("reply parent has no global identifier")?,
                ),
                work_id: row.get("work_id"),
            });
        }
    };
    let work_id = context.task_id().map(|id| id.as_str().to_owned());
    Ok(ResolvedContext {
        context,
        reply_to: None,
        parent_global_id: None,
        work_id,
    })
}

async fn conversation_visible_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: ConversationId,
) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE b.group_name=? AND json_extract(m.context,'$.kind')='conversation' AND json_extract(m.context,'$.id')=? AND (m.sender=? OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message=m.id AND d.recipient=?)))")
        .bind(&actor.group_name).bind(id.to_string()).bind(actor.id).bind(actor.id).fetch_one(&mut **tx).await?)
}

/// Message metadata visible to one conversation participant.
#[derive(Debug, Serialize)]
pub struct ConversationMessage {
    /// Local message identifier for a subsequent read or reply.
    pub id: i64,
    /// Explicit communication intent.
    pub intent: crate::states::MessageIntent,
    /// Author of the message.
    pub sender: String,
    /// Bounded message summary.
    pub summary: String,
    /// Unix creation timestamp.
    pub created: i64,
    /// Parent message identifier, when available locally.
    pub reply_to: Option<i64>,
    /// Portable parent reference, including when the parent is private to another recipient.
    pub parent: Option<ParentMessage>,
}

/// Portable reply ancestry with an optional local identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentMessage {
    /// Globally stable parent message identity.
    pub global_id: Uuid,
    /// Local identifier when the parent exists in this store.
    pub local_id: Option<MessageId>,
}
impl sqlx::Type<Sqlite> for ParentMessage {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <String as sqlx::Type<Sqlite>>::type_info()
    }
    fn compatible(info: &sqlx::sqlite::SqliteTypeInfo) -> bool {
        <String as sqlx::Type<Sqlite>>::compatible(info)
    }
}
impl<'r> sqlx::Decode<'r, Sqlite> for ParentMessage {
    fn decode(
        value: sqlx::sqlite::SqliteValueRef<'r>,
    ) -> std::result::Result<Self, sqlx::error::BoxDynError> {
        let text = <&str as sqlx::Decode<Sqlite>>::decode(value)?;
        Ok(serde_json::from_str(text)?)
    }
}

/// Bounded conversation history that preserves recipient privacy.
#[derive(Debug, Serialize)]
pub struct ConversationPage {
    /// The requested conversation.
    pub context: MessageContext,
    /// Authored and addressed message summaries only.
    pub messages: Vec<ConversationMessage>,
    /// Whether another page remains.
    pub more: bool,
    /// Cursor for the next page.
    pub next_after: i64,
}

impl Store {
    /// Read a visible message's subject without acknowledging or resolving its delivery.
    /// # Errors
    /// The actor is stale, the message is unavailable, or its stored context cannot be decoded.
    pub async fn message_context(&self, actor: &Mailbox, id: i64) -> Result<MessageContext> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let context: String = sqlx::query_scalar("SELECT m.context FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE m.id=? AND b.group_name=? AND (m.sender=? OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message=m.id AND d.recipient=?))")
            .bind(id).bind(&actor.group_name).bind(actor.id).bind(actor.id)
            .fetch_optional(&mut *tx).await?.context("message context is not available to this agent in this group")?;
        tx.commit().await?;
        Ok(serde_json::from_str(&context)?)
    }

    /// Read authored and addressed conversation summaries, without receipting their bodies.
    /// # Errors
    /// The actor is stale, the cursor is negative, the conversation is unavailable, or SQL fails.
    pub async fn conversation(
        &self,
        actor: &Mailbox,
        id: ConversationId,
        after: i64,
    ) -> Result<ConversationPage> {
        ensure!(after >= 0, "conversation cursor must not be negative");
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        ensure!(
            conversation_visible_tx(&mut tx, actor, id).await?,
            "conversation is not available to this agent in this group"
        );
        let conversation = id.to_string();
        let mut messages = sqlx::query_as!(ConversationMessage,
            "SELECT m.id,m.intent AS 'intent: crate::states::MessageIntent',b.name AS sender,m.summary,m.created,m.reply_to,CASE WHEN m.parent_global_id IS NOT NULL THEN json_object('global_id',m.parent_global_id,'local_id',m.reply_to) END AS 'parent?: ParentMessage' FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE b.group_name=? AND json_extract(m.context,'$.kind')='conversation' AND json_extract(m.context,'$.id')=? AND m.id>? AND (m.sender=? OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message=m.id AND d.recipient=?)) ORDER BY m.id LIMIT 7",
            actor.group_name, conversation, after, actor.id, actor.id).fetch_all(&mut *tx).await?;
        let more = messages.len() > 6;
        messages.truncate(6);
        let next_after = messages.last().map_or(after, |message| message.id);
        tx.commit().await?;
        Ok(ConversationPage {
            context: MessageContext::Conversation { id },
            messages,
            more,
            next_after,
        })
    }
}
