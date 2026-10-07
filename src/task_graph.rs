//! Persistent task structure, separate from lifecycle decisions and checkpoints.
//!
//! A dependency plan is one atomic writer decision. Its edges and readiness are
//! database projections, so scheduling and inspection always use the same facts.
//! Parent links organize normal tasks; they never imply acceptance or waiting.
use crate::{
    bounded,
    followup::PrerequisiteMode,
    mail_context::TaskVersion,
    names::TaskId,
    relationships::{GraphChange, TaskGraphSnapshot, record_graph_change_tx},
    states::TaskState,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};

/// A task revision observed before a graph mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskReference {
    /// Same-group task.
    pub task: TaskId,
    /// Observed revision, not a pin on future progress.
    pub version: TaskVersion,
}

/// A persistent condition on a prerequisite's current business facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRequirement {
    /// Same-group prerequisite.
    pub task: TaskId,
    /// Qualifying states. Cancellation counts only when explicitly selected.
    pub states: Vec<TaskState>,
    /// Require this exact accepted revision in addition to a qualifying state.
    #[serde(default)]
    pub accepted_revision: Option<String>,
}
impl TaskRequirement {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.states.is_empty() && self.states.len() <= 8,
            "dependency needs 1..8 qualifying states"
        );
        for (index, state) in self.states.iter().enumerate() {
            ensure!(
                !self.states[..index].contains(state),
                "duplicate qualifying state"
            );
        }
        if let Some(revision) = &self.accepted_revision {
            bounded(revision, 128, "accepted revision")?;
            ensure!(
                !revision.trim().is_empty(),
                "accepted revision cannot be empty"
            );
        }
        Ok(())
    }
}

/// The complete persistent contract for one task. An empty plan is ready.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyPlan {
    /// How prerequisite conditions combine.
    pub mode: PrerequisiteMode,
    /// At most 32 distinct prerequisites.
    pub requirements: Vec<TaskRequirement>,
}
impl Default for DependencyPlan {
    fn default() -> Self {
        Self {
            mode: PrerequisiteMode::All,
            requirements: Vec::new(),
        }
    }
}
impl DependencyPlan {
    pub(crate) fn validate(&self, source: &str) -> Result<()> {
        ensure!(
            self.requirements.len() <= 32,
            "dependency plan supports at most 32 prerequisites"
        );
        for (index, requirement) in self.requirements.iter().enumerate() {
            requirement.validate()?;
            ensure!(
                requirement.task.as_str() != source,
                "task cannot depend on itself"
            );
            ensure!(
                !self.requirements[..index]
                    .iter()
                    .any(|r| r.task == requirement.task),
                "duplicate prerequisite task"
            );
        }
        Ok(())
    }
}

/// A prerequisite with its observed endpoint version, used only for mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedRequirement {
    /// Desired persistent condition.
    pub condition: TaskRequirement,
    /// The prerequisite version inspected by the writer.
    pub version: TaskVersion,
}

/// Atomically replace the entire dependency contract at an observed task version.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyUpdate {
    /// Observed dependent task version.
    pub version: TaskVersion,
    /// Combination rule.
    pub mode: PrerequisiteMode,
    /// Complete desired set; empty explicitly clears the plan.
    pub requirements: Vec<ObservedRequirement>,
    /// Audit explanation.
    pub reason: String,
}

/// Derived readiness; it never changes task lifecycle or grants approval.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Readiness {
    /// Combination rule.
    pub mode: PrerequisiteMode,
    /// Number of conditions.
    pub total: i64,
    /// Number currently satisfied.
    pub satisfied: i64,
    /// Current aggregate truth.
    pub ready: bool,
}
pub(crate) async fn readiness_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    task: Option<&str>,
) -> Result<Option<Readiness>> {
    Ok(sqlx::query_as(
        "SELECT mode,total,satisfied,ready FROM task_readiness WHERE group_name=? AND task=?",
    )
    .bind(group)
    .bind(task)
    .fetch_optional(&mut **tx)
    .await?)
}

impl Store {
    /// Replace dependencies as the task's current writer, with idempotent retries.
    /// # Errors
    /// Authority, endpoint versions, bounds, or cycle validation fail.
    pub async fn task_dependencies_set(
        &self,
        actor: &Mailbox,
        source: &str,
        update: DependencyUpdate,
        now: i64,
    ) -> Result<Value> {
        crate::name(source)?;
        bounded(&update.reason, 512, "dependency reason")?;
        ensure!(
            !update.reason.trim().is_empty(),
            "dependency reason is required"
        );
        let plan = DependencyPlan {
            mode: update.mode,
            requirements: update
                .requirements
                .iter()
                .map(|r| r.condition.clone())
                .collect(),
        };
        plan.validate(source)?;
        let canonical = serde_json::to_string(&update)?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let home: bool = sqlx::query_scalar(
            "SELECT home_machine=(SELECT id FROM node LIMIT 1) FROM groups WHERE name=?",
        )
        .bind(&actor.group_name)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(home, "dependency plans require the home machine");
        let row = sqlx::query("SELECT writer,version FROM work_items WHERE group_name=? AND id=?")
            .bind(&actor.group_name)
            .bind(source)
            .fetch_optional(&mut *tx)
            .await?
            .context("task missing in this group")?;
        ensure!(
            row.get::<String, _>("writer") == actor.name,
            "only task writer may replace dependencies"
        );
        if let Some(old) = sqlx::query("SELECT actor,binding_version,canonical,result FROM task_relation_retries WHERE group_name=? AND source=? AND expected=?")
            .bind(&actor.group_name).bind(source).bind(update.version.get()).fetch_optional(&mut *tx).await? {
            ensure!(old.get::<i64,_>("actor") == actor.id && old.get::<i64,_>("binding_version") == actor.binding_version && old.get::<String,_>("canonical") == canonical, "graph retry differs from committed mutation");
            return Ok(serde_json::from_str(&old.get::<String,_>("result"))?);
        }
        ensure!(
            row.get::<i64, _>("version") == update.version.get(),
            "source task version conflict"
        );
        for requirement in &update.requirements {
            let version: Option<i64> =
                sqlx::query_scalar("SELECT version FROM work_items WHERE group_name=? AND id=?")
                    .bind(&actor.group_name)
                    .bind(requirement.condition.task.as_str())
                    .fetch_optional(&mut *tx)
                    .await?;
            ensure!(
                version == Some(requirement.version.get()),
                "prerequisite missing or version conflict: {}",
                requirement.condition.task
            );
        }
        let targets: Vec<String> = plan
            .requirements
            .iter()
            .map(|r| r.task.to_string())
            .collect();
        crate::followup::validate_dependency_graph(
            &mut tx,
            &actor.group_name,
            Some(source),
            &targets,
        )
        .await?;
        sqlx::query("INSERT INTO task_dependency_plans(group_name,source,plan) VALUES(?,?,?) ON CONFLICT(group_name,source) DO UPDATE SET plan=excluded.plan")
            .bind(&actor.group_name).bind(source).bind(serde_json::to_string(&plan)?).execute(&mut *tx).await?;
        let version = update
            .version
            .get()
            .checked_add(1)
            .context("task version overflow")?;
        let fact = json!({"source":source,"version":version,"dependencies":plan});
        record_graph_change_tx(
            &mut tx,
            actor,
            GraphChange {
                source,
                expected: update.version.get(),
                reason: &update.reason,
                canonical: &canonical,
                fact: &fact,
            },
            now,
        )
        .await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(fact)
    }

    /// Read a plan and its current prerequisite facts without marking retrieval.
    /// # Errors
    /// The actor or task is unavailable on the authoritative home machine.
    pub async fn task_dependencies(&self, actor: &Mailbox, task: &str) -> Result<Value> {
        crate::name(task)?;
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let readiness = readiness_tx(&mut tx, &actor.group_name, Some(task)).await?;
        if readiness.is_none() {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM work_snapshots WHERE group_name=? AND work_id=?)",
            )
            .bind(&actor.group_name)
            .bind(task)
            .fetch_one(&mut *tx)
            .await?;
            ensure!(exists, "task missing in this group");
            let snapshot: Option<String> = sqlx::query_scalar(
                "SELECT snapshot FROM task_relation_snapshots WHERE group_name=? AND source=?",
            )
            .bind(&actor.group_name)
            .bind(task)
            .fetch_optional(&mut *tx)
            .await?;
            let snapshot: Option<TaskGraphSnapshot> =
                snapshot.map(|s| serde_json::from_str(&s)).transpose()?;
            tx.commit().await?;
            return Ok(
                json!({"task":task,"plan":snapshot.as_ref().map(|s| &s.dependencies),"version":snapshot.as_ref().map(|s| s.version),"readiness":null,"authority":"home_snapshot","consistency":"live_cache","scheduling":"home_only"}),
            );
        }
        let plan: Option<String> = sqlx::query_scalar(
            "SELECT plan FROM task_dependency_plans WHERE group_name=? AND source=?",
        )
        .bind(&actor.group_name)
        .bind(task)
        .fetch_optional(&mut *tx)
        .await?;
        let plan: DependencyPlan = plan
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or_default();
        let version: i64 =
            sqlx::query_scalar("SELECT version FROM work_items WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(task)
                .fetch_one(&mut *tx)
                .await?;
        let facts = sqlx::query("SELECT target,state,accepted_revision,target_version,COALESCE(satisfied,0) AS satisfied FROM task_dependency_evaluations WHERE group_name=? AND source=? ORDER BY target")
            .bind(&actor.group_name).bind(task).fetch_all(&mut *tx).await?;
        let facts: Vec<Value> = facts.iter().map(|r| json!({"task":r.get::<String,_>("target"),"state":r.get::<Option<String>,_>("state"),"accepted_revision":r.get::<Option<String>,_>("accepted_revision"),"version":r.get::<Option<i64>,_>("target_version"),"satisfied":r.get::<bool,_>("satisfied")})).collect();
        tx.commit().await?;
        Ok(
            json!({"task":task,"version":version,"plan":plan,"readiness":readiness,"prerequisites":facts,"authority":"home"}),
        )
    }

    /// Inspect a bounded subtask graph. Shared descendants appear only once.
    /// # Errors
    /// The actor, task, or limit is invalid; remote caches are not authoritative.
    pub async fn task_tree(&self, actor: &Mailbox, root: &str, limit: usize) -> Result<Value> {
        crate::name(root)?;
        ensure!((1..=100).contains(&limit), "tree limit must be 1..100");
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        ensure!(
            readiness_tx(&mut tx, &actor.group_name, Some(root))
                .await?
                .is_some(),
            "task tree requires the home machine"
        );
        let ids: Vec<String> = sqlx::query_scalar("WITH RECURSIVE tree(id) AS (SELECT ? UNION SELECT r.source FROM task_relations r JOIN tree t ON r.target=t.id WHERE r.group_name=? AND r.kind='parent' AND r.active=1 LIMIT ?) SELECT id FROM tree")
            .bind(root).bind(&actor.group_name).bind((limit+1) as i64).fetch_all(&mut *tx).await?;
        let more = ids.len() > limit;
        let ids = serde_json::to_string(&ids[..ids.len().min(limit)])?;
        let rows = sqlx::query("SELECT w.id,w.owner,w.writer,w.state,w.version,w.accepted_revision,r.ready FROM work_items w JOIN task_readiness r ON r.group_name=w.group_name AND r.task=w.id WHERE w.group_name=? AND w.id IN (SELECT value FROM json_each(?)) ORDER BY w.id")
            .bind(&actor.group_name).bind(&ids).fetch_all(&mut *tx).await?;
        let nodes: Vec<Value> = rows.iter().map(|r| json!({"task":r.get::<String,_>("id"),"owner":r.get::<String,_>("owner"),"writer":r.get::<String,_>("writer"),"state":r.get::<String,_>("state"),"version":r.get::<i64,_>("version"),"accepted_revision":r.get::<Option<String>,_>("accepted_revision"),"dependencies_ready":r.get::<bool,_>("ready")})).collect();
        let rows = sqlx::query("SELECT DISTINCT source,target FROM task_relations WHERE group_name=? AND kind='parent' AND active=1 AND source IN (SELECT value FROM json_each(?)) AND target IN (SELECT value FROM json_each(?)) ORDER BY source,target LIMIT 1001")
            .bind(&actor.group_name).bind(&ids).bind(&ids).fetch_all(&mut *tx).await?;
        let edges_more = rows.len() > 1000;
        let edges: Vec<Value> = rows.iter().take(1000).map(|r| json!({"child":r.get::<String,_>("source"),"parent":r.get::<String,_>("target")})).collect();
        tx.commit().await?;
        Ok(
            json!({"root":root,"tasks":nodes,"parents":edges,"more":more || edges_more,"edges_more":edges_more,"limit":limit,"authority":"home"}),
        )
    }
}

pub(crate) async fn plan_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    source: &str,
) -> Result<DependencyPlan> {
    let plan: Option<String> = sqlx::query_scalar(
        "SELECT plan FROM task_dependency_plans WHERE group_name=? AND source=?",
    )
    .bind(group)
    .bind(source)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(plan
        .map(|s| serde_json::from_str(&s))
        .transpose()?
        .unwrap_or_default())
}

pub(crate) fn validate_snapshot(snapshot: &TaskGraphSnapshot) -> Result<()> {
    snapshot.dependencies.validate(&snapshot.source)
}
