//! Explicit group task ownership links and blocking dependency facts.
use crate::{
    bounded, name,
    store::{Mailbox, Store},
    work::WorkItem,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

/// Link direction: source has parent/related/dependency target.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    /// Ownership hierarchy; never gates acceptance.
    Parent,
    /// Informational association.
    Related,
    /// Blocking prerequisite; scheduler only wakes reassessment.
    Dependency,
}
impl RelationKind {
    fn text(self) -> &'static str {
        match self {
            Self::Parent => "parent",
            Self::Related => "related",
            Self::Dependency => "dependency",
        }
    }
}
/// Source-writer authorized link mutation with observed endpoint versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationUpdate {
    /// Observed source task version.
    pub version: i64,
    /// Same-group target task.
    pub target: String,
    /// Observed target task version.
    pub target_version: i64,
    /// Meaning of this edge.
    pub kind: RelationKind,
    /// Explicit review round identifier.
    #[serde(default)]
    pub review_round: String,
    /// Exact source revision being reviewed.
    #[serde(default)]
    pub source_revision: String,
    /// False supersedes this exact edge, preserving its history.
    #[serde(default = "yes")]
    pub active: bool,
    /// Audited reason.
    pub reason: String,
}
fn yes() -> bool {
    true
}
/// Bounded incident-edge query, including terminal target tasks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationQuery {
    /// Optional relationship kind.
    pub kind: Option<RelationKind>,
    /// Exact review round filter.
    pub review_round: Option<String>,
    /// Exact reviewed source revision filter.
    pub source_revision: Option<String>,
    /// Include superseded edges.
    #[serde(default)]
    pub include_inactive: bool,
    /// Scoped opaque continuation.
    pub cursor: Option<String>,
    /// One to one hundred records, twenty by default.
    pub limit: Option<usize>,
}
impl Store {
    /// Mutate an edge as its source writer; endpoint versions serialize races.
    pub async fn work_relation(
        &self,
        actor: &Mailbox,
        source: &str,
        update: RelationUpdate,
        now: i64,
    ) -> Result<Value> {
        name(source)?;
        name(&update.target)?;
        ensure!(source != update.target, "task cannot link to itself");
        bounded(&update.reason, 512, "relation reason")?;
        ensure!(
            !update.reason.trim().is_empty(),
            "relation reason is required"
        );
        bounded(&update.review_round, 128, "review round")?;
        bounded(&update.source_revision, 128, "source revision")?;
        let canonical = serde_json::to_string(&update)?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let home: bool = sqlx::query_scalar(
            "SELECT home_machine=(SELECT id FROM node LIMIT 1) FROM groups WHERE name=?",
        )
        .bind(&actor.group_name)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(home, "relationships require the home machine");
        let source_row =
            sqlx::query("SELECT writer,version FROM work_items WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(source)
                .fetch_optional(&mut *tx)
                .await?
                .context("source task missing in this group")?;
        ensure!(
            source_row.get::<String, _>("writer") == actor.name,
            "only source task writer may change its relationships"
        );
        if let Some(old)=sqlx::query("SELECT actor,binding_version,canonical,result FROM task_relation_retries WHERE group_name=? AND source=? AND expected=?").bind(&actor.group_name).bind(source).bind(update.version).fetch_optional(&mut *tx).await? {
            ensure!(old.get::<i64,_>("actor")==actor.id && old.get::<i64,_>("binding_version")==actor.binding_version && old.get::<String,_>("canonical")==canonical,"relationship retry differs from committed mutation");
            return Ok(serde_json::from_str(&old.get::<String,_>("result"))?);
        }
        ensure!(
            source_row.get::<i64, _>("version") == update.version,
            "source task version conflict"
        );
        let target_version: Option<i64> =
            sqlx::query_scalar("SELECT version FROM work_items WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(&update.target)
                .fetch_optional(&mut *tx)
                .await?;
        ensure!(
            target_version == Some(update.target_version),
            "target task missing or version conflict"
        );
        if update.active && update.kind != RelationKind::Related {
            // Follow every active blocking/ownership edge; cycles cannot be hidden across kinds.
            let cycle:bool=sqlx::query_scalar("WITH RECURSIVE reach(id) AS (SELECT ? UNION SELECT r.target FROM task_relations r JOIN reach n ON r.source=n.id WHERE r.group_name=? AND r.active=1 AND r.kind=?) SELECT EXISTS(SELECT 1 FROM reach WHERE id=?)").bind(&update.target).bind(&actor.group_name).bind(update.kind.text()).bind(source).fetch_one(&mut *tx).await?;
            ensure!(!cycle, "relationship cycle");
            if update.kind == RelationKind::Dependency {
                crate::followup::validate_dependency_graph(
                    &mut tx,
                    &actor.group_name,
                    Some(source),
                    std::slice::from_ref(&update.target),
                )
                .await?;
            }
        }
        let version = update
            .version
            .checked_add(1)
            .context("task version overflow")?;
        sqlx::query("INSERT INTO task_relations(group_name,source,target,kind,review_round,source_revision,active,version,actor,reason,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(group_name,source,target,kind,review_round,source_revision) DO UPDATE SET active=excluded.active,version=excluded.version,actor=excluded.actor,reason=excluded.reason,updated=excluded.updated")
            .bind(&actor.group_name).bind(source).bind(&update.target).bind(update.kind.text()).bind(&update.review_round).bind(&update.source_revision).bind(update.active).bind(version).bind(&actor.name).bind(&update.reason).bind(now).execute(&mut *tx).await?;
        let fact = json!({"source":source,"target":update.target,"kind":update.kind,"review_round":update.review_round,"source_revision":update.source_revision,"active":update.active,"version":version});
        sqlx::query("INSERT INTO task_relation_history(group_name,source,snapshot,actor,reason,changed) VALUES(?,?,?,?,?,?)").bind(&actor.group_name).bind(source).bind(fact.to_string()).bind(&actor.name).bind(&update.reason).bind(now).execute(&mut *tx).await?;
        sqlx::query(
            "UPDATE work_items SET version=?,updated=? WHERE group_name=? AND id=? AND version=?",
        )
        .bind(version)
        .bind(now)
        .bind(&actor.group_name)
        .bind(source)
        .bind(update.version)
        .execute(&mut *tx)
        .await?;
        let snapshot:String=sqlx::query_scalar("SELECT snapshot FROM work_changes WHERE group_name=? AND work_id=? ORDER BY version DESC LIMIT 1").bind(&actor.group_name).bind(source).fetch_one(&mut *tx).await?;
        let mut item: WorkItem = serde_json::from_str(&snapshot)?;
        item.version = version;
        item.updated = now;
        sqlx::query("INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed) VALUES(?,?,?,?,?,?,?)").bind(&actor.group_name).bind(source).bind(version).bind(&actor.name).bind(format!("relationship: {}",update.reason)).bind(serde_json::to_string(&item)?).bind(now).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_relation_retries(group_name,source,expected,actor,binding_version,canonical,result) VALUES(?,?,?,?,?,?,?)").bind(&actor.group_name).bind(source).bind(update.version).bind(actor.id).bind(actor.binding_version).bind(canonical).bind(fact.to_string()).execute(&mut *tx).await?;
        crate::relay::enqueue_snapshot(&mut tx, &item, None, now).await?;
        let relations = relation_snapshot_tx(&mut tx, &actor.group_name, source).await?;
        crate::relay::enqueue_relation_snapshot(&mut tx, &relations, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(fact)
    }
    /// Inspect incident links and current endpoint facts without settling tasks or messages.
    pub async fn work_relations(
        &self,
        actor: &Mailbox,
        id: &str,
        query: RelationQuery,
    ) -> Result<Value> {
        name(id)?;
        let limit = query.limit.unwrap_or(20);
        ensure!((1..=100).contains(&limit), "relation limit must be 1..100");
        let scope = serde_json::to_string(&(
            &actor.group_name,
            actor.id,
            actor.binding_version,
            id,
            query.kind,
            &query.review_round,
            &query.source_revision,
            query.include_inactive,
        ))?;
        let after = if let Some(cursor) = query.cursor {
            let (saved, after): (String, i64) = serde_json::from_str(&cursor)?;
            ensure!(
                saved == scope,
                "relationship cursor belongs to another query"
            );
            after
        } else {
            0
        };
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=?)",
        )
        .bind(&actor.group_name)
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        if !exists {
            let remote: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM work_snapshots WHERE group_name=? AND work_id=?)",
            )
            .bind(&actor.group_name)
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
            ensure!(remote, "task not found in this group");
            let rows=sqlx::query("SELECT snapshot FROM task_relation_snapshots WHERE group_name=? ORDER BY source LIMIT 1001").bind(&actor.group_name).fetch_all(&mut *tx).await?;
            ensure!(
                rows.len() <= 1000,
                "remote relationship cache exceeds supported query bound"
            );
            let mut facts = Vec::new();
            for row in rows {
                let snapshot: RelationSnapshot =
                    serde_json::from_str(&row.get::<String, _>("snapshot"))?;
                for fact in snapshot.relations {
                    if fact["source"].as_str() != Some(id) && fact["target"].as_str() != Some(id) {
                        continue;
                    }
                    if !query.include_inactive && fact["active"] != true {
                        continue;
                    }
                    if query
                        .kind
                        .is_some_and(|k| fact["kind"].as_str() != Some(k.text()))
                    {
                        continue;
                    }
                    if query
                        .review_round
                        .as_ref()
                        .is_some_and(|r| fact["review_round"].as_str() != Some(r.as_str()))
                    {
                        continue;
                    }
                    if query
                        .source_revision
                        .as_ref()
                        .is_some_and(|r| fact["source_revision"].as_str() != Some(r.as_str()))
                    {
                        continue;
                    }
                    facts.push(fact);
                }
            }
            facts.sort_by_key(|f| {
                format!(
                    "{}:{}:{}:{}:{}",
                    f["source"], f["target"], f["kind"], f["review_round"], f["source_revision"]
                )
            });
            ensure!(after >= 0, "invalid relationship cursor");
            let more = facts.len() > after as usize + limit;
            let items = facts
                .into_iter()
                .skip(after as usize)
                .take(limit)
                .collect::<Vec<_>>();
            let next = if more {
                Some(serde_json::to_string(&(
                    &scope,
                    after + items.len() as i64,
                ))?)
            } else {
                None
            };
            tx.commit().await?;
            return Ok(
                json!({"items":items,"more":more,"next_cursor":next,"authority":"home_snapshot","ordering":"source_target_kind_round_revision","consistency":"live_cache"}),
            );
        }
        let rows=sqlx::query("SELECT r.rowid AS edge_id,r.*,s.state AS source_state,s.version AS current_source_version,t.state AS target_state,t.version AS current_target_version FROM task_relations r JOIN work_items s ON s.group_name=r.group_name AND s.id=r.source JOIN work_items t ON t.group_name=r.group_name AND t.id=r.target WHERE r.group_name=? AND (r.source=? OR r.target=?) AND r.rowid>? AND (? IS NULL OR r.kind=?) AND (? IS NULL OR r.review_round=?) AND (? IS NULL OR r.source_revision=?) AND (? OR r.active=1) ORDER BY r.rowid LIMIT ?")
            .bind(&actor.group_name).bind(id).bind(id).bind(after).bind(query.kind.map(RelationKind::text)).bind(query.kind.map(RelationKind::text)).bind(&query.review_round).bind(&query.review_round).bind(&query.source_revision).bind(&query.source_revision).bind(query.include_inactive).bind((limit+1) as i64).fetch_all(&mut *tx).await?;
        let more = rows.len() > limit;
        let items=rows.iter().take(limit).map(|r|json!({"edge_id":r.get::<i64,_>("edge_id"),"source":r.get::<String,_>("source"),"target":r.get::<String,_>("target"),"kind":r.get::<String,_>("kind"),"review_round":r.get::<String,_>("review_round"),"source_revision":r.get::<String,_>("source_revision"),"active":r.get::<bool,_>("active"),"version":r.get::<i64,_>("version"),"source_state":r.get::<String,_>("source_state"),"target_state":r.get::<String,_>("target_state"),"current_source_version":r.get::<i64,_>("current_source_version"),"current_target_version":r.get::<i64,_>("current_target_version")})).collect::<Vec<_>>();
        let next = if more {
            Some(serde_json::to_string(&(
                &scope,
                items.last().unwrap()["edge_id"].as_i64().unwrap(),
            ))?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(json!({"items":items,"more":more,"next_cursor":next}))
    }
}

impl Store {
    /// Bounded recovery facts, including handoffs and links; private reports stay private.
    pub async fn task_coordination_context(&self, actor: &Mailbox, id: &str) -> Result<Value> {
        name(id)?;
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows=sqlx::query("SELECT old_writer,new_writer,operator,changed,expected FROM task_transfers WHERE group_name=? AND work_id=? ORDER BY expected DESC LIMIT 5").bind(&actor.group_name).bind(id).fetch_all(&mut *tx).await?;
        let transfers=rows.iter().map(|r|json!({"old_writer":r.get::<String,_>("old_writer"),"new_writer":r.get::<String,_>("new_writer"),"operator":r.get::<bool,_>("operator"),"changed":r.get::<i64,_>("changed"),"version":r.get::<i64,_>("expected")+1})).collect::<Vec<_>>();
        tx.commit().await?;
        let relations = self
            .work_relations(
                actor,
                id,
                RelationQuery {
                    limit: Some(5),
                    ..Default::default()
                },
            )
            .await?;
        Ok(
            json!({"transfers":transfers,"relations":relations,"private_reports":"remain addressed to their original recipients; explicit sharing required"}),
        )
    }
}

/// Home-authoritative relationship snapshot transported independently of private messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationSnapshot {
    /// Authoritative group.
    pub group_name: String,
    /// Source task.
    pub source: String,
    /// Observed source version.
    pub version: i64,
    /// Public relationship facts.
    pub relations: Vec<Value>,
}
/// Capture complete current incident source facts under the mutation transaction.
pub(crate) async fn relation_snapshot_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group: &str,
    source: &str,
) -> Result<RelationSnapshot> {
    let version: i64 =
        sqlx::query_scalar("SELECT version FROM work_items WHERE group_name=? AND id=?")
            .bind(group)
            .bind(source)
            .fetch_one(&mut **tx)
            .await?;
    let rows=sqlx::query("SELECT target,kind,review_round,source_revision,active,version FROM task_relations WHERE group_name=? AND source=? ORDER BY rowid LIMIT 1001").bind(group).bind(source).fetch_all(&mut **tx).await?;
    ensure!(
        rows.len() <= 1000,
        "source task relationship limit is 1000 retained edges"
    );
    let snapshot=RelationSnapshot{group_name:group.to_owned(),source:source.to_owned(),version,relations:rows.iter().map(|r|json!({"source":source,"target":r.get::<String,_>("target"),"kind":r.get::<String,_>("kind"),"review_round":r.get::<String,_>("review_round"),"source_revision":r.get::<String,_>("source_revision"),"active":r.get::<bool,_>("active"),"version":r.get::<i64,_>("version")})).collect()};
    bounded(
        &serde_json::to_string(&snapshot)?,
        192 * 1024,
        "relationship snapshot",
    )?;
    Ok(snapshot)
}
/// Apply a previously origin-authorized home snapshot without granting private mail access.
pub(crate) async fn apply_relation_snapshot_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    snapshot: &RelationSnapshot,
    now: i64,
) -> Result<()> {
    bounded(
        &serde_json::to_string(snapshot)?,
        192 * 1024,
        "relationship snapshot",
    )?;
    name(&snapshot.group_name)?;
    name(&snapshot.source)?;
    ensure!(snapshot.version > 0, "invalid relation snapshot version");
    ensure!(
        snapshot.relations.len() <= 1000,
        "relation snapshot exceeds supported bound"
    );
    for relation in &snapshot.relations {
        ensure!(
            relation["source"].as_str() == Some(snapshot.source.as_str()),
            "relation snapshot source mismatch"
        );
        name(
            relation["target"]
                .as_str()
                .context("missing relationship target")?,
        )?;
    }
    sqlx::query("INSERT INTO task_relation_snapshots(group_name,source,version,snapshot,synced_at) VALUES(?,?,?,?,?) ON CONFLICT(group_name,source) DO UPDATE SET version=excluded.version,snapshot=excluded.snapshot,synced_at=excluded.synced_at WHERE excluded.version>task_relation_snapshots.version")
        .bind(&snapshot.group_name).bind(&snapshot.source).bind(snapshot.version).bind(serde_json::to_string(snapshot)?).bind(now).execute(&mut **tx).await?;
    Ok(())
}

/// Seed all retained source relationship facts when a remote group route is installed.
pub(crate) async fn enqueue_relations_for_route(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group: &str,
    machine: uuid::Uuid,
    time: i64,
) -> Result<()> {
    let sources: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT source FROM task_relations WHERE group_name=? ORDER BY source",
    )
    .bind(group)
    .fetch_all(&mut **tx)
    .await?;
    for source in sources {
        let snapshot = relation_snapshot_tx(tx, group, &source).await?;
        crate::relay::enqueue(
            tx,
            machine,
            crate::relay::Event::RelationSnapshot(snapshot),
            time,
        )
        .await?;
    }
    Ok(())
}
