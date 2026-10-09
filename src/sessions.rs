//! Session-scoped delivery consent with separately authorized group bindings.
//!
//! Sharing an exact Herdr endpoint does not merge tasks, mailboxes, receipts or
//! decision authority. Session views expose only addresses already bound to that
//! endpoint. Standalone credentials remain confined to their own registration.

use crate::{
    identity::Binding,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{Sqlite, Transaction};
use std::path::Path;

pub(crate) fn endpoint_key(socket: &Path, binding: &Binding) -> Result<String> {
    let h = binding
        .herdr()
        .context("a Herdr session binding is required")?;
    Ok(serde_json::to_string(&(
        socket,
        &h.pane,
        &h.terminal,
        &h.agent,
        h.session_kind,
        &h.session_value,
    ))?)
}

impl Store {
    /// Select an anchor for a read-only session view, without choosing a write identity.
    /// # Errors
    /// No current authorized registration matches the session, or storage fails.
    pub async fn select_session_group(
        &self,
        requested: Option<&str>,
        session: Option<&uuid::Uuid>,
    ) -> Result<String> {
        if requested.is_some()
            || session.is_some()
            || std::env::var("HERDR_ENV").as_deref() != Ok("1")
        {
            return self.select_group(requested, session, true).await;
        }
        for group in self.groups().await? {
            if self.authenticate_herdr(&group.name).await.is_ok() {
                return Ok(group.name);
            }
        }
        anyhow::bail!(
            "no group has a verified binding to the current Herdr session; inspect its registration and endpoint"
        )
    }

    pub(crate) async fn herdr_policy_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
    ) -> Result<bool> {
        let group = sqlx::query!("SELECT socket FROM groups WHERE name=?", actor.group_name)
            .fetch_one(&mut **tx)
            .await?;
        let key = endpoint_key(Path::new(&group.socket), &actor.binding)?;
        Ok(sqlx::query_scalar!(
            "SELECT auto_prompt FROM herdr_session_policy WHERE endpoint=?",
            key
        )
        .fetch_optional(&mut **tx)
        .await?
        .unwrap_or(0)
            != 0)
    }

    /// Read the exact session's effective Herdr prompting policy.
    /// # Errors
    /// The actor is stale, is not bound to Herdr, or storage fails.
    pub async fn herdr_prompt_enabled(&self, actor: &Mailbox) -> Result<bool> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let enabled = Self::herdr_policy_tx(&mut tx, actor).await?;
        tx.commit().await?;
        Ok(enabled)
    }

    /// Set operator consent for every binding of this exact Herdr session.
    ///
    /// Callers must verify the live target before changing consent. Group pauses
    /// and participant opt-outs remain independent and are never cleared here.
    /// # Errors
    /// Identity is stale, the binding lacks Herdr, or persistence fails.
    pub async fn set_herdr_prompt_policy(&self, actor: &Mailbox, enabled: bool) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let socket =
            sqlx::query_scalar!("SELECT socket FROM groups WHERE name=?", actor.group_name)
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            !socket.is_empty(),
            "Herdr prompting requires a configured socket"
        );
        let key = endpoint_key(Path::new(&socket), &actor.binding)?;
        sqlx::query!("INSERT INTO herdr_session_policy(endpoint,auto_prompt) VALUES(?,?) ON CONFLICT(endpoint) DO UPDATE SET auto_prompt=excluded.auto_prompt",key,enabled).execute(&mut *tx).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(())
    }

    pub(crate) async fn initialize_herdr_policy_tx(
        tx: &mut Transaction<'_, Sqlite>,
        group: &str,
        binding: &Binding,
    ) -> Result<()> {
        if binding.herdr().is_none() {
            return Ok(());
        }
        let config = sqlx::query!("SELECT socket FROM groups WHERE name=?", group)
            .fetch_one(&mut **tx)
            .await?;
        let key = endpoint_key(Path::new(&config.socket), binding)?;
        sqlx::query!(
            "INSERT OR IGNORE INTO herdr_session_policy(endpoint,auto_prompt) VALUES(?,?)",
            key,
            false
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Discover only registered group addresses bound to the authenticated session.
    ///
    /// Standalone and remote identities never gain access to another registration
    /// merely because names, runtime socket paths or thread identifiers overlap.
    /// # Errors
    /// The anchor actor is stale or a stored binding is invalid.
    pub async fn session_mailboxes(&self, actor: &Mailbox) -> Result<Vec<Mailbox>> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let Some(h) = actor.binding.herdr() else {
            tx.commit().await?;
            return Ok(vec![actor.clone()]);
        };
        let rows = sqlx::query!("SELECT b.group_name,b.name FROM mailboxes b JOIN groups g ON g.name=b.group_name WHERE b.pane=? AND b.agent_state='registered' AND g.socket=(SELECT socket FROM groups WHERE name=?) ORDER BY b.group_name",h.pane,actor.group_name).fetch_all(&mut *tx).await?;
        let mut bindings = Vec::new();
        for row in rows {
            // Both reads share this transaction's snapshot; use the existing
            // mailbox decoder rather than inventing another binding format.
            let binding = sqlx::query_scalar!(
                "SELECT binding FROM mailboxes WHERE group_name=? AND name=?",
                row.group_name,
                row.name
            )
            .fetch_one(&mut *tx)
            .await?;
            let binding: Binding = serde_json::from_str(&binding)?;
            if binding.same_identity(&actor.binding) {
                bindings.push((row.group_name, row.name));
            }
        }
        tx.commit().await?;
        let mut result = Vec::new();
        for (group, name) in bindings {
            let current = self.mailbox(&group, &name).await?;
            // Replacements after the snapshot must not acquire the old session's access.
            if current.binding.same_identity(&actor.binding)
                && current.state == crate::states::AgentState::Registered
            {
                result.push(current);
            }
        }
        Ok(result)
    }

    /// Read bounded attention across this session's verified group bindings.
    ///
    /// The view records no receipt. Fetch each listed record with its own group;
    /// omitted groups remain available through the returned continuation cursor.
    /// # Errors
    /// An identity changed or storage fails.
    pub async fn session_attention(&self, actor: &Mailbox, after_group: &str) -> Result<Value> {
        if !after_group.is_empty() {
            crate::name(after_group)?;
        }
        let bindings = self.session_mailboxes(actor).await?;
        let total = bindings.len();
        let eligible: Vec<_> = bindings
            .into_iter()
            .filter(|b| b.group_name.as_str() > after_group)
            .collect();
        let mut groups = Vec::new();
        for binding in eligible.iter().take(8) {
            let config = self.group(&binding.group_name).await?;
            let enabled = if binding.binding.herdr().is_some() {
                Some(self.herdr_prompt_enabled(binding).await?)
            } else {
                None
            };
            let item = json!({"group":binding.group_name,"agent":binding.name,"binding_version":binding.binding_version,"group_paused":config.paused!=0,"runtime_enabled":self.runtime_enabled(binding).await?,"herdr_prompt_enabled":enabled,"attention":self.attention_snapshot(binding).await?});
            groups.push(item);
            if serde_json::to_vec(&groups)?.len() > 3600 {
                groups.pop();
                break;
            }
        }
        let next = groups
            .last()
            .and_then(|g| g["group"].as_str())
            .unwrap_or(after_group);
        Ok(
            json!({"groups":groups,"total_bindings":total,"more":eligible.len()>groups.len(),"next_after_group":next,"retrieved":false,"fetch":"Use agent-mail --group GROUP mail/task/attention show ID for each listed source"}),
        )
    }
}
