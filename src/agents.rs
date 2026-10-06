//! Durable registration state. Runtime observations never imply retirement.
use crate::{
    identity::Binding,
    states::{AgentState, BindingKind},
    store::Store,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

/// Credential-free, versioned agent registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    /// Stable local identifier.
    pub id: i64,
    /// Coordination group.
    pub group_name: String,
    /// Address within the group.
    pub name: String,
    /// Durable registration lifecycle.
    pub state: AgentState,
    /// Version required for the next decision.
    pub version: i64,
    /// Last registration change, Unix seconds.
    pub updated: i64,
    /// Runtime association, not a liveness claim.
    pub runtime: BindingKind,
}
/// One committed registration change.
#[derive(Debug, Serialize)]
pub struct AgentChange {
    /// Committed version.
    pub version: i64,
    /// Resulting registration state.
    pub state: AgentState,
    /// Operator explanation or binding change description.
    pub reason: String,
    /// Unix timestamp.
    pub changed: i64,
}
impl Store {
    /// Read a registration without exposing its credential.
    /// # Errors
    /// Missing registration or database failure.
    pub async fn agent_record(&self, group: &str, name: &str) -> Result<AgentRecord> {
        sqlx::query_as!(AgentRecord,"SELECT id,group_name,name,agent_state AS 'state: AgentState',agent_version AS version,agent_updated AS updated,json_extract(binding,'$.runtime') AS 'runtime!: BindingKind' FROM mailboxes WHERE group_name=? AND name=?",group,name)
            .fetch_optional(self.pool()).await?.context("agent is not registered")
    }
    /// List registrations, including retired agents.
    /// # Errors
    /// Missing group or database failure.
    pub async fn agent_records(&self, group: &str) -> Result<Vec<AgentRecord>> {
        self.group(group).await?;
        Ok(sqlx::query_as!(AgentRecord,"SELECT id,group_name,name,agent_state AS 'state: AgentState',agent_version AS version,agent_updated AS updated,json_extract(binding,'$.runtime') AS 'runtime!: BindingKind' FROM mailboxes WHERE group_name=? ORDER BY name",group).fetch_all(self.pool()).await?)
    }
    /// Read the latest twenty registration changes, newest first.
    /// # Errors
    /// Missing registration or database failure.
    pub async fn agent_history(&self, group: &str, name: &str) -> Result<Vec<AgentChange>> {
        let record = self.agent_record(group, name).await?;
        Ok(sqlx::query_as!(AgentChange,"SELECT version,state AS 'state: AgentState',reason,changed FROM agent_changes WHERE recipient=? ORDER BY version DESC LIMIT 20",record.id).fetch_all(self.pool()).await?)
    }
    /// Apply one explicit lifecycle decision with optimistic concurrency and retry safety.
    /// Retirement requires no open obligations. Restoration rotates standalone identity.
    /// # Errors
    /// Stale version, conflicting retry, unchanged state, remote route, open obligations or I/O.
    pub async fn update_agent(
        &self,
        group: &str,
        name: &str,
        version: i64,
        state: AgentState,
        reason: &str,
    ) -> Result<AgentRecord> {
        ensure!(version > 0, "version must be positive");
        ensure!(
            !reason.trim().is_empty() && reason.len() <= crate::BODY_LIMIT,
            "reason must be nonempty and at most 8192 bytes"
        );
        let canonical = serde_json::to_string(&(state, reason))?;
        let mut tx = self.pool().begin().await?;
        sqlx::query!("UPDATE groups SET paused=paused WHERE name=?", group)
            .execute(&mut *tx)
            .await?;
        let row=sqlx::query!("SELECT id,binding,binding_version,agent_state AS 'state: AgentState',agent_version FROM mailboxes WHERE group_name=? AND name=?",group,name).fetch_optional(&mut *tx).await?.context("agent is not registered")?;
        if let Some(prior) = sqlx::query!(
            "SELECT canonical,result FROM agent_decisions WHERE recipient=? AND expected_version=?",
            row.id,
            version
        )
        .fetch_optional(&mut *tx)
        .await?
        {
            ensure!(
                prior.canonical == canonical,
                "conflicting retry for this agent version"
            );
            return Ok(serde_json::from_str(&prior.result)?);
        }
        ensure!(
            row.agent_version == version,
            "agent version changed; read agent show before deciding again"
        );
        ensure!(row.state != state, "agent is already {state}");
        let mut binding: Binding = serde_json::from_str(&row.binding)?;
        ensure!(
            !matches!(binding, Binding::Remote { .. }),
            "manage remote registration on its home machine"
        );
        if state == AgentState::Retired {
            let work=sqlx::query!("SELECT COUNT(*) AS count FROM work_items WHERE group_name=? AND open=1 AND (owner=? OR writer=?)",group,name,name).fetch_one(&mut *tx).await?;
            let snapshots=sqlx::query!("SELECT COUNT(*) AS count FROM work_snapshots WHERE group_name=? AND owner=? AND json_extract(snapshot,'$.state') NOT IN ('done','accepted','cancelled')",group,name).fetch_one(&mut *tx).await?;
            ensure!(
                work.count == 0 && snapshots.count == 0,
                "agent has open tasks; close or transfer obligations first"
            );
            let mail=sqlx::query!("SELECT COUNT(*) AS count FROM deliveries d JOIN messages m ON m.id=d.message WHERE m.intent='request' AND d.state='pending' AND (d.recipient=? OR m.sender=?)",row.id,row.id).fetch_one(&mut *tx).await?;
            ensure!(
                mail.count == 0,
                "agent has pending mail; resolve or withdraw it first"
            );
        } else if let Binding::Standalone { session } = &mut binding {
            *session = uuid::Uuid::new_v4();
        }
        let encoded = serde_json::to_string(&binding)?;
        let state_wire = state.as_str();
        let now = crate::now()?;
        sqlx::query!("UPDATE mailboxes SET agent_state=?,agent_version=agent_version+1,agent_updated=?,binding=?,binding_version=binding_version+1 WHERE id=? AND agent_version=?",state_wire,now,encoded,row.id,version).execute(&mut *tx).await?;
        // Preserve explicit delivery pause while invalidating every old runtime attachment.
        let binding_version = row.binding_version + 1;
        sqlx::query!(
            "UPDATE runtime_policy SET binding_version=? WHERE recipient=? AND binding_version=?",
            binding_version,
            row.id,
            row.binding_version
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!("DELETE FROM runtime_wakes WHERE recipient=?", row.id)
            .execute(&mut *tx)
            .await?;
        let next = version + 1;
        sqlx::query!(
            "INSERT INTO agent_changes(recipient,version,state,reason,changed) VALUES(?,?,?,?,?)",
            row.id,
            next,
            state_wire,
            reason,
            now
        )
        .execute(&mut *tx)
        .await?;
        let result = AgentRecord {
            id: row.id,
            group_name: group.into(),
            name: name.into(),
            state,
            version: next,
            updated: now,
            runtime: binding.runtime(),
        };
        let result_json = serde_json::to_string(&result)?;
        sqlx::query!(
            "INSERT INTO agent_decisions VALUES(?,?,?,?)",
            row.id,
            version,
            canonical,
            result_json
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }
}
