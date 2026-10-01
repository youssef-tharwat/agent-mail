//! One transport ledger for canonical source/cause accounts.
//!
//! These transactions never perform I/O, claim business completion, or commit.
//! The central dispatcher must commit exposure before invoking its bounded
//! transport, then record the actual result in a fresh transaction. A lost
//! process is uncertain; lease expiry does not establish quiescence.
use crate::{decision_recovery as recovery, decision_supervisor, store::Store};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};
use std::collections::BTreeSet;

type Tx<'a> = Transaction<'a, Sqlite>;
const MAX_ITEMS: usize = 10;
const MAX_BYTES: usize = 8192;
const MAX_EXPOSURES: i64 = 3;
const COOLDOWN: i64 = 300;

/// A provenance selector, never the identity of a fresh transmission budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum NoticeSource {
    /// An original stage-three attention occurrence, including inactive aliases.
    AttentionOccurrence(i64),
    /// Recovery's original operator business responsibility.
    OperatorObligation(i64),
    /// Original protected supervisor failure; never an invented business task.
    SupervisorFailure(i64),
}
impl NoticeSource {
    fn class(self) -> NoticeClass {
        match self {
            Self::SupervisorFailure(_) => NoticeClass::Infrastructure,
            _ => NoticeClass::Ordinary,
        }
    }
    fn parts(self) -> (&'static str, i64) {
        match self {
            Self::AttentionOccurrence(id) => ("attention_occurrence", id),
            Self::OperatorObligation(id) => ("operator_obligation", id),
            Self::SupervisorFailure(id) => ("supervisor_failure", id),
        }
    }
    fn from_parts(kind: &str, id: i64) -> Result<Self> {
        match kind {
            "attention_occurrence" => Ok(Self::AttentionOccurrence(id)),
            "operator_obligation" => Ok(Self::OperatorObligation(id)),
            "supervisor_failure" => Ok(Self::SupervisorFailure(id)),
            _ => anyhow::bail!("unknown notice source"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NoticeClass {
    #[default]
    Ordinary,
    Infrastructure,
}
impl NoticeClass {
    fn text(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Infrastructure => "infrastructure",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "ordinary" => Ok(Self::Ordinary),
            "infrastructure" => Ok(Self::Infrastructure),
            _ => anyhow::bail!("unknown notice class"),
        }
    }
}

/// Audit identity of the original operator, never mailbox authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NoticeResponsibility {
    /// Operator of the original home service, reached through its configured route.
    HomeOperator {
        /// Original group of the failed supervisor.
        group: String,
        /// Original home service identity, independent of mailbox registration.
        home_machine: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceSnapshot {
    source: NoticeSource,
    source_key: String,
    episode: String,
    responsible: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    responsibility: Option<NoticeResponsibility>,
    unresolved: bool,
    due_at: i64,
    facts: Value,
}

/// Readback distinguishes projection, transport and business responsibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeView {
    /// Stable ledger identity.
    pub id: i64,
    /// Actual source selector.
    pub source: NoticeSource,
    /// Canonical source/cause spending account.
    pub account: i64,
    /// Current exact projection revision.
    pub revision: i64,
    /// Largest revision accepted by a transport; never a retrieval receipt.
    pub accepted_revision: i64,
    /// Route generation of that acceptance; legacy evidence can be unknown.
    pub accepted_generation: Option<i64>,
    /// Transport state; accepted does not mean business handled.
    pub state: String,
    /// Original mailbox name or infrastructure operator display label.
    pub responsible: String,
    /// Typed original infrastructure principal; absent for ordinary mailbox sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responsibility: Option<NoticeResponsibility>,
    /// Current business responsibility, independent of transport state.
    pub unresolved: bool,
    /// Original business source key, independent of occurrence/task versions.
    pub source_key: String,
    /// Stable cause episode assigned by its source owner.
    pub episode: String,
    /// Exact captured case/obligation or attention/source facts.
    pub source_snapshot: Value,
    /// Current route generation; historical generations retain their spending.
    pub route_generation: i64,
    /// Whether a route is actually configured (not proof it can deliver).
    pub route_configured: bool,
    /// Saved generation still describes the actual configured route.
    pub route_current: bool,
    /// Canonical cause exposures on the current route generation.
    pub exposures: i64,
    /// Earliest retry time from the canonical spending ledger.
    pub next_attempt: i64,
    /// Live or uncertain sender which prevents overlap, including old routes.
    pub outstanding_batch: Option<String>,
    /// A pre-migration invocation still lacks a supported closure fact.
    pub legacy_sender_uncertain: bool,
    /// Last result detail on this exact projection's batch, when available.
    pub detail: Option<String>,
}

/// Immutable reservation. Its ID is also the transport idempotency key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NoticeBatch {
    pub id: String,
    pub group: String,
    pub generation: i64,
    pub route: String,
    pub payload: String,
    pub owner: String,
    pub lease_until: i64,
    #[serde(default)]
    pub class: NoticeClass,
}

/// Only the dispatcher which observed I/O supplies these results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportResult {
    /// A supported response confirms acceptance and completes the invocation.
    Accepted,
    /// Transport accepted, but a spawned sender's descendants/effects may live.
    AcceptedUncertain,
    /// No sender started, or supported closure evidence establishes failure.
    Failed,
    /// Missing/ambiguous completion, including an unjoined timed-out process.
    Uncertain,
}
impl TransportResult {
    fn text(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Failed => "failed",
            Self::Uncertain | Self::AcceptedUncertain => "uncertain",
        }
    }

    fn accepted(self) -> bool {
        matches!(self, Self::Accepted | Self::AcceptedUncertain)
    }

    fn uncertain(self) -> bool {
        matches!(self, Self::Uncertain | Self::AcceptedUncertain)
    }
}

/// Snapshot the persisted route before the trusted configuration writer edits it.
pub(crate) async fn route_generation_tx(tx: &mut Tx<'_>, group: &str) -> Result<i64> {
    Ok(route_tx(tx, group).await?.0)
}

/// Stable internal receipt key, not an authorization token. Exact canonical
/// bytes are also checked by repair, so a digest collision fails closed.
pub(crate) fn route_repair_key(group: &str, generation: i64, notifier: &Option<String>) -> String {
    let canonical = json!([group, generation, notifier]).to_string();
    // FNV-1a-128 has a specified algorithm; no randomized/platform hash state.
    let digest = canonical
        .bytes()
        .fold(0x6c62272e07bb014262b821756295c58du128, |hash, byte| {
            (hash ^ u128::from(byte)).wrapping_mul(0x0000000001000000000000000000013bu128)
        });
    format!("route-{digest:032x}")
}

async fn event_tx(
    tx: &mut Tx<'_>,
    group: &str,
    batch: Option<&str>,
    kind: &str,
    payload: Value,
    now: i64,
) -> Result<()> {
    sqlx::query("INSERT INTO operator_notice_events(group_name,batch,kind,payload,created) VALUES(?,?,?,?,?)")
        .bind(group).bind(batch).bind(kind).bind(serde_json::to_string(&payload)?).bind(now).execute(&mut **tx).await?;
    Ok(())
}

async fn route_tx(tx: &mut Tx<'_>, group: &str) -> Result<(i64, String, bool)> {
    recovery::reserve_home_tx(tx, group).await?;
    let row = sqlx::query("SELECT p.notifier,g.socket,p.mode,g.paused,p.updated FROM followup_policy p JOIN groups g ON g.name=p.group_name WHERE g.name=?")
        .bind(group).fetch_one(&mut **tx).await?;
    let notifier: Option<Value> = row
        .get::<Option<String>, _>("notifier")
        .map(|s| serde_json::from_str(&s))
        .transpose()?;
    let route = serde_json::to_string(
        &json!({"notifier":notifier,"socket":row.get::<Option<String>,_>("socket")}),
    )?;
    sqlx::query(
        "INSERT OR IGNORE INTO operator_notice_routes(group_name,route,changed) VALUES(?,?,?)",
    )
    .bind(group)
    .bind(&route)
    .bind(row.get::<i64, _>("updated"))
    .execute(&mut **tx)
    .await?;
    let saved =
        sqlx::query("SELECT generation,route FROM operator_notice_routes WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
    // Compare JSON values because SQLite migration object key order is not an API.
    let same = serde_json::from_str::<Value>(&saved.get::<String, _>("route"))?
        == serde_json::from_str::<Value>(&route)?;
    let enabled =
        row.get::<String, _>("mode") == "enabled" && !row.get::<bool, _>("paused") && same;
    Ok((saved.get("generation"), route, enabled))
}

fn configured(route: &str) -> Result<bool> {
    let value: Value = serde_json::from_str(route)?;
    Ok(value["notifier"].as_array().is_some_and(|a| !a.is_empty())
        || value["socket"].as_str().is_some_and(|s| !s.is_empty()))
}

async fn source_tx(
    tx: &mut Tx<'_>,
    group: &str,
    source: NoticeSource,
    now: i64,
) -> Result<SourceSnapshot> {
    recovery::reserve_home_tx(tx, group).await?;
    ensure!(source.parts().1 > 0, "positive notice source required");
    match source {
        NoticeSource::SupervisorFailure(id) => {
            let validated = crate::supervisor_failures::validate_supervisor_failure_source_tx(
                tx, group, id, now,
            )
            .await?;
            let item = validated.source();
            let crate::supervisor_failures::HomeOperator::HomeOperator {
                group: responsible_group,
                home_machine,
            } = &item.responsible;
            ensure!(
                item.group == group && item.id == id && responsible_group == group,
                "supervisor notice source mismatch"
            );
            Ok(SourceSnapshot {
                source,
                source_key: item.source_key.clone(),
                episode: item.episode.clone(),
                responsible: format!("Home operator ({responsible_group}, {home_machine})"),
                responsibility: Some(NoticeResponsibility::HomeOperator {
                    group: responsible_group.clone(),
                    home_machine: home_machine.clone(),
                }),
                unresolved: item.unresolved,
                due_at: item.due_at,
                facts: serde_json::to_value(item)?,
            })
        }
        NoticeSource::OperatorObligation(id) => {
            // Actual owner API; no caller-supplied source permission or revision.
            let validated =
                decision_supervisor::validate_operator_notice_source_tx(tx, group, id, now).await?;
            let item = &validated.source;
            Ok(SourceSnapshot {
                source,
                source_key: item.source_key.clone(),
                episode: item.episode.clone(),
                responsible: item.responsible.clone(),
                responsibility: None,
                unresolved: matches!(item.state.as_str(), "pending" | "escalated"),
                due_at: item.due_at,
                facts: serde_json::to_value(&validated)?,
            })
        }
        NoticeSource::AttentionOccurrence(id) => {
            // Source owner validation is independent of active_attention and
            // mailbox availability. A later plan is provenance, not a new cause.
            let row = sqlx::query("SELECT o.id,o.plan_version,o.created,o.operator_after,f.task,f.task_version,f.message,b.name AS recipient FROM attention_occurrences o JOIN followups f ON f.id=o.followup JOIN mailboxes b ON b.id=f.recipient WHERE o.id=? AND f.group_name=? AND o.stage=3")
                .bind(id).bind(group).fetch_optional(&mut **tx).await?.context("stage-three attention source missing")?;
            let selector = match row.get::<Option<String>, _>("task") {
                Some(id) => recovery::Obligation::Task {
                    id,
                    version: row.get("task_version"),
                },
                None => recovery::Obligation::Delivery {
                    message: row.get("message"),
                    recipient: row.get("recipient"),
                },
            };
            let current = recovery::inspect_source_tx(tx, group, &selector).await?;
            // The occurrence retains its original operator grace for audit.
            // A source-authorized plan correction can defer this alias, while
            // its canonical account and already spent allowance stay intact.
            let plan = current
                .plan
                .as_ref()
                .context("attention source plan missing")?;
            let due_at = row
                .get::<Option<i64>, _>("operator_after")
                .context("operator boundary missing")?;
            let due_at = if plan.version == row.get::<i64, _>("plan_version") {
                due_at
            } else {
                due_at.max(plan.escalate_at)
            };
            Ok(SourceSnapshot {
                source,
                source_key: current.source_key.clone(),
                episode: "obligation".into(),
                responsible: current.authority.clone(),
                responsibility: None,
                unresolved: current.unresolved,
                due_at,
                facts: json!({"occurrence":id,"plan_version":row.get::<i64,_>("plan_version"),"created":row.get::<i64,_>("created"),"source":current}),
            })
        }
    }
}

/// Refresh exactly one source from its owner within the caller's writer tx.
pub(crate) async fn project_notice_tx(
    tx: &mut Tx<'_>,
    group: &str,
    source: NoticeSource,
    now: i64,
) -> Result<i64> {
    let current = source_tx(tx, group, source, now).await?;
    let snapshot = serde_json::to_string(&current)?;
    sqlx::query("INSERT OR IGNORE INTO operator_notice_accounts(group_name,source_key,episode) VALUES(?,?,?)")
        .bind(group).bind(&current.source_key).bind(&current.episode).execute(&mut **tx).await?;
    let account: i64 = sqlx::query_scalar(
        "SELECT id FROM operator_notice_accounts WHERE group_name=? AND source_key=? AND episode=?",
    )
    .bind(group)
    .bind(&current.source_key)
    .bind(&current.episode)
    .fetch_one(&mut **tx)
    .await?;
    let (kind, source_id) = source.parts();
    let prior = sqlx::query("SELECT id,account,source_snapshot FROM operator_notices WHERE group_name=? AND source_kind=? AND source_id=?")
        .bind(group).bind(kind).bind(source_id).fetch_optional(&mut **tx).await?;
    if let Some(prior) = prior {
        ensure!(
            prior.get::<i64, _>("account") == account,
            "notice canonical account changed"
        );
        let id: i64 = prior.get("id");
        if prior.get::<String, _>("source_snapshot") != snapshot {
            sqlx::query("UPDATE operator_notices SET revision=revision+1,source_snapshot=?,responsible=?,unresolved=?,due_at=?,first_dirty=CASE WHEN revision<=accepted_revision THEN ? ELSE first_dirty END,state=?,batch=NULL WHERE id=?")
                .bind(&snapshot).bind(&current.responsible).bind(current.unresolved).bind(current.due_at).bind(now)
                .bind(if current.unresolved { "pending" } else { "retired" }).bind(id).execute(&mut **tx).await?;
            event_tx(
                tx,
                group,
                None,
                "source_changed",
                json!({"notice":id,"source":source}),
                now,
            )
            .await?;
        }
        return Ok(id);
    }
    let legacy = if let NoticeSource::AttentionOccurrence(id) = source {
        sqlx::query("SELECT state,provenance FROM operator_notice_legacy WHERE occurrence=?")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
    } else {
        None
    };
    // A legacy acceptance has exact occurrence provenance, not the later source
    // revision. Keep it in legacy evidence; don't manufacture a new watermark.
    let state = if !current.unresolved {
        "retired"
    } else if let Some(old) = &legacy {
        match old.get::<String, _>("state").as_str() {
            "accepted" => "accepted",
            "attempting" | "uncertain" => "uncertain",
            "failed" => "failed",
            "unconfigured" => "unconfigured",
            _ => "pending",
        }
    } else {
        "pending"
    };
    let id = sqlx::query("INSERT INTO operator_notices(group_name,source_kind,source_id,account,revision,source_snapshot,responsible,unresolved,due_at,first_dirty,state) VALUES(?,?,?,?,1,?,?,?,?,?,?)")
        .bind(group).bind(kind).bind(source_id).bind(account).bind(snapshot).bind(current.responsible).bind(current.unresolved).bind(current.due_at).bind(now).bind(state)
        .execute(&mut **tx).await?.last_insert_rowid();
    event_tx(
        tx,
        group,
        None,
        "source_projected",
        json!({"notice":id,"source":source,"legacy":legacy.is_some()}),
        now,
    )
    .await?;
    Ok(id)
}

/// Bounded independent reconciliation; both source cursors commit with effects.
pub(crate) async fn project_notice_page_tx(
    tx: &mut Tx<'_>,
    group: &str,
    now: i64,
    limit: usize,
) -> Result<(i64, i64)> {
    ensure!((1..=100).contains(&limit), "notice page must be 1..100");
    recovery::reserve_home_tx(tx, group).await?;
    sqlx::query("INSERT OR IGNORE INTO operator_notice_projection(group_name) VALUES(?)")
        .bind(group)
        .execute(&mut **tx)
        .await?;
    let cursor = sqlx::query("SELECT attention_after,obligation_after FROM operator_notice_projection WHERE group_name=?").bind(group).fetch_one(&mut **tx).await?;
    let rows: Vec<i64> = sqlx::query_scalar("SELECT o.id FROM attention_occurrences o JOIN followups f ON f.id=o.followup WHERE f.group_name=? AND o.stage=3 AND o.id>? ORDER BY o.id LIMIT ?")
        .bind(group).bind(cursor.get::<i64,_>("attention_after")).bind((limit+1) as i64).fetch_all(&mut **tx).await?;
    for id in rows.iter().take(limit) {
        project_notice_tx(tx, group, NoticeSource::AttentionOccurrence(*id), now).await?;
    }
    let attention_after = if rows.len() > limit {
        rows[limit - 1]
    } else {
        0
    };
    let page = decision_supervisor::operator_notice_sources_tx(
        tx,
        group,
        cursor.get("obligation_after"),
        limit,
    )
    .await?;
    for item in &page.items {
        project_notice_tx(
            tx,
            group,
            NoticeSource::OperatorObligation(item.obligation_id),
            now,
        )
        .await?;
    }
    let obligation_after = if page.more { page.next_after } else { 0 };
    sqlx::query("UPDATE operator_notice_projection SET attention_after=?,obligation_after=? WHERE group_name=?")
        .bind(attention_after).bind(obligation_after).bind(group).execute(&mut **tx).await?;
    Ok((attention_after, obligation_after))
}

/// Infrastructure projection never invokes ordinary graph/source readers.
async fn project_infrastructure_notice_page_tx(
    tx: &mut Tx<'_>,
    group: &str,
    now: i64,
    limit: usize,
) -> Result<i64> {
    ensure!((1..=100).contains(&limit), "notice page must be 1..100");
    crate::supervisor_failures::reconcile_supervisor_visits_tx(tx, group, now, limit).await?;
    sqlx::query("INSERT OR IGNORE INTO operator_notice_projection(group_name) VALUES(?)")
        .bind(group)
        .execute(&mut **tx)
        .await?;
    let after: i64 = sqlx::query_scalar(
        "SELECT infrastructure_after FROM operator_notice_projection WHERE group_name=?",
    )
    .bind(group)
    .fetch_one(&mut **tx)
    .await?;
    let page =
        crate::supervisor_failures::supervisor_failure_sources_tx(tx, group, after, limit).await?;
    for id in page.ids {
        project_notice_tx(tx, group, NoticeSource::SupervisorFailure(id), now).await?;
    }
    sqlx::query("UPDATE operator_notice_projection SET infrastructure_after=? WHERE group_name=?")
        .bind(page.next_after)
        .bind(group)
        .execute(&mut **tx)
        .await?;
    Ok(page.next_after)
}

/// Reservation freezes membership and bytes but spends no exposure allowance.
#[cfg(test)]
pub(crate) async fn reserve_operator_notice_batch_tx(
    tx: &mut Tx<'_>,
    group: &str,
    owner: &str,
    now: i64,
) -> Result<Option<NoticeBatch>> {
    reserve_notice_batch_for_class_tx(tx, group, owner, now, NoticeClass::Ordinary).await
}

async fn reserve_notice_batch_for_class_tx(
    tx: &mut Tx<'_>,
    group: &str,
    owner: &str,
    now: i64,
    class: NoticeClass,
) -> Result<Option<NoticeBatch>> {
    crate::name(owner)?;
    let (generation, route, enabled) = route_tx(tx, group).await?;
    // A missing result after the transport bound becomes visible uncertainty.
    // This retains the busy account; it does not authorize another process.
    sqlx::query("UPDATE operator_notice_batches SET state='uncertain',detail='No result within the bounded transport interval; sender closure is unknown' WHERE group_name=? AND state='exposed' AND exposed_at<=?")
        .bind(group).bind(now.saturating_sub(5)).execute(&mut **tx).await?;
    sqlx::query("UPDATE operator_notices SET state='uncertain' WHERE group_name=? AND state='exposed' AND batch IN (SELECT id FROM operator_notice_batches WHERE state='uncertain')")
        .bind(group).execute(&mut **tx).await?;
    if !enabled {
        return Ok(None);
    }
    // Expired unexposed reservations are safe to cancel. Exposed senders are not.
    sqlx::query("UPDATE operator_notice_batches SET state='cancelled',finished_at=?,detail='unexposed reservation expired' WHERE group_name=? AND state='reserved' AND lease_until<=?")
        .bind(now).bind(group).bind(now).execute(&mut **tx).await?;
    sqlx::query("UPDATE operator_notices SET state='pending',batch=NULL WHERE group_name=? AND state='reserved' AND batch IN (SELECT id FROM operator_notice_batches WHERE state='cancelled')")
        .bind(group).execute(&mut **tx).await?;
    let rows = sqlx::query("SELECT n.id,n.source_kind,n.source_id FROM operator_notices n WHERE n.group_name=? AND ((?='infrastructure' AND n.source_kind='supervisor_failure') OR (?='ordinary' AND n.source_kind IN ('attention_occurrence','operator_obligation'))) AND n.unresolved=1 AND n.due_at<=? AND n.state IN ('pending','failed','unconfigured') AND NOT EXISTS(SELECT 1 FROM operator_notice_spending s WHERE s.account=n.account AND s.generation=? AND (s.exposures>=3 OR s.next_at>?)) AND NOT EXISTS(SELECT 1 FROM operator_notice_batch_items i JOIN operator_notice_batches b ON b.id=i.batch WHERE i.account=n.account AND b.state IN ('reserved','exposed','uncertain')) AND NOT EXISTS(SELECT 1 FROM operator_notice_legacy l WHERE l.account=n.account AND l.state IN ('attempting','uncertain')) AND NOT EXISTS(SELECT 1 FROM operator_notices u WHERE u.account=n.account AND u.state='uncertain' AND u.batch IS NULL) ORDER BY n.due_at,n.id LIMIT 100")
        .bind(group).bind(class.text()).bind(class.text()).bind(now).bind(generation).bind(now).fetch_all(&mut **tx).await?;
    let id = uuid::Uuid::new_v4().to_string();
    let mut items = Vec::new();
    let mut captured = Vec::new();
    let mut accounts = BTreeSet::new();
    for row in rows {
        let source =
            NoticeSource::from_parts(&row.get::<String, _>("source_kind"), row.get("source_id"))?;
        ensure!(source.class() == class, "notice batch class mismatch");
        let notice = project_notice_tx(tx, group, source, now).await?;
        let row = sqlx::query("SELECT * FROM operator_notices WHERE id=?")
            .bind(notice)
            .fetch_one(&mut **tx)
            .await?;
        if !row.get::<bool, _>("unresolved") || row.get::<i64, _>("due_at") > now {
            continue;
        }
        let account: i64 = row.get("account");
        // One alias per cause per batch, and no overlapping live/uncertain sender.
        if accounts.contains(&account) {
            continue;
        }
        let busy: i64 = sqlx::query_scalar("SELECT count(*) FROM operator_notice_batch_items i JOIN operator_notice_batches b ON b.id=i.batch WHERE i.account=? AND b.state IN ('reserved','exposed','uncertain')")
            .bind(account).fetch_one(&mut **tx).await?;
        let legacy_unknown: i64 = sqlx::query_scalar("SELECT count(*) FROM operator_notices WHERE account=? AND state='uncertain' AND batch IS NULL")
            .bind(account).fetch_one(&mut **tx).await?;
        if busy > 0 || legacy_unknown > 0 {
            continue;
        }
        sqlx::query(
            "INSERT OR IGNORE INTO operator_notice_spending(account,generation) VALUES(?,?)",
        )
        .bind(account)
        .bind(generation)
        .execute(&mut **tx)
        .await?;
        let spending = sqlx::query("SELECT exposures,next_at FROM operator_notice_spending WHERE account=? AND generation=?").bind(account).bind(generation).fetch_one(&mut **tx).await?;
        if spending.get::<i64, _>("exposures") >= MAX_EXPOSURES
            || spending.get::<i64, _>("next_at") > now
        {
            continue;
        }
        if !configured(&route)? {
            sqlx::query("UPDATE operator_notices SET state='unconfigured' WHERE id=?")
                .bind(notice)
                .execute(&mut **tx)
                .await?;
            continue;
        }
        let snapshot: String = row.get("source_snapshot");
        let revision: i64 = row.get("revision");
        // Transport carries bounded references. Full source evidence stays local.
        let source =
            NoticeSource::from_parts(&row.get::<String, _>("source_kind"), row.get("source_id"))?;
        let mut item = json!({"notice":notice,"revision":revision,"source":source,"responsible":row.get::<String,_>("responsible")});
        if let Some(principal) = serde_json::from_str::<Value>(&snapshot)?.get("responsibility") {
            item["responsibility"] = principal.clone();
        }
        items.push(item);
        let payload = serde_json::to_string(
            &json!({"group":group,"batch":id,"items":items,"instruction":"Inspect Agent Mail status and exact source records. Transport acceptance does not settle responsibility."}),
        )?;
        if payload.len() > MAX_BYTES {
            items.pop();
            break;
        }
        accounts.insert(account);
        captured.push((notice, revision, account, snapshot));
        if items.len() == MAX_ITEMS {
            break;
        }
    }
    if items.is_empty() {
        return Ok(None);
    }
    let payload = serde_json::to_string(
        &json!({"group":group,"batch":id,"items":items,"instruction":"Inspect Agent Mail status and exact source records. Transport acceptance does not settle responsibility."}),
    )?;
    let lease_until = now.checked_add(30).context("notice lease overflow")?;
    sqlx::query("INSERT INTO operator_notice_batches(id,group_name,generation,route,payload,owner,lease_until,notice_class,state) VALUES(?,?,?,?,?,?,?,?,'reserved')")
        .bind(&id).bind(group).bind(generation).bind(&route).bind(&payload).bind(owner).bind(lease_until).bind(class.text()).execute(&mut **tx).await?;
    for (notice, revision, account, snapshot) in captured {
        sqlx::query("INSERT INTO operator_notice_batch_items(batch,notice,revision,account,source_snapshot) VALUES(?,?,?,?,?)")
            .bind(&id).bind(notice).bind(revision).bind(account).bind(snapshot).execute(&mut **tx).await?;
        sqlx::query("UPDATE operator_notices SET state='reserved',batch=? WHERE id=?")
            .bind(&id)
            .bind(notice)
            .execute(&mut **tx)
            .await?;
    }
    event_tx(
        tx,
        group,
        Some(&id),
        "reserved",
        json!({"generation":generation}),
        now,
    )
    .await?;
    Ok(Some(NoticeBatch {
        id,
        group: group.into(),
        generation,
        route,
        payload,
        owner: owner.into(),
        lease_until,
        class,
    }))
}

async fn load_batch_tx(
    tx: &mut Tx<'_>,
    group: &str,
    id: &str,
    owner: &str,
) -> Result<(NoticeBatch, String)> {
    recovery::reserve_home_tx(tx, group).await?;
    let r = sqlx::query(
        "SELECT * FROM operator_notice_batches WHERE id=? AND group_name=? AND owner=?",
    )
    .bind(id)
    .bind(group)
    .bind(owner)
    .fetch_optional(&mut **tx)
    .await?
    .context("notice batch or owner mismatch")?;
    Ok((
        NoticeBatch {
            id: r.get("id"),
            group: r.get("group_name"),
            generation: r.get("generation"),
            route: r.get("route"),
            payload: r.get("payload"),
            owner: r.get("owner"),
            lease_until: r.get("lease_until"),
            class: NoticeClass::parse(&r.get::<String, _>("notice_class"))?,
        },
        r.get("state"),
    ))
}

/// The only spending transition. The caller commits this before any I/O.
/// A replay after exposure returns None, never a second permission to send.
pub(crate) async fn expose_operator_notice_batch_tx(
    tx: &mut Tx<'_>,
    group: &str,
    id: &str,
    owner: &str,
    now: i64,
) -> Result<Option<NoticeBatch>> {
    let (batch, state) = load_batch_tx(tx, group, id, owner).await?;
    if state != "reserved" {
        return Ok(None);
    }
    let (generation, route, enabled) = route_tx(tx, group).await?;
    let mut valid = enabled
        && generation == batch.generation
        && route == batch.route
        && now < batch.lease_until;
    let items = sqlx::query("SELECT i.*,n.source_kind,n.source_id FROM operator_notice_batch_items i JOIN operator_notices n ON n.id=i.notice WHERE i.batch=? ORDER BY i.notice LIMIT 11")
        .bind(id).fetch_all(&mut **tx).await?;
    ensure!(
        !items.is_empty() && items.len() <= MAX_ITEMS,
        "invalid frozen notice membership"
    );
    for item in &items {
        let source =
            NoticeSource::from_parts(&item.get::<String, _>("source_kind"), item.get("source_id"))?;
        ensure!(source.class() == batch.class, "notice batch class mismatch");
        let notice = project_notice_tx(tx, group, source, now).await?;
        let n = sqlx::query("SELECT revision,source_snapshot,batch,unresolved,due_at FROM operator_notices WHERE id=?").bind(notice).fetch_one(&mut **tx).await?;
        valid &= n.get::<i64, _>("revision") == item.get::<i64, _>("revision")
            && n.get::<String, _>("source_snapshot") == item.get::<String, _>("source_snapshot")
            && n.get::<Option<String>, _>("batch").as_deref() == Some(id)
            && n.get::<bool, _>("unresolved")
            && n.get::<i64, _>("due_at") <= now;
        let exposures: i64 = sqlx::query_scalar(
            "SELECT exposures FROM operator_notice_spending WHERE account=? AND generation=?",
        )
        .bind(item.get::<i64, _>("account"))
        .bind(generation)
        .fetch_optional(&mut **tx)
        .await?
        .unwrap_or(MAX_EXPOSURES);
        valid &= exposures < MAX_EXPOSURES;
    }
    if !valid {
        sqlx::query("UPDATE operator_notice_batches SET state='cancelled',finished_at=?,detail='source, mode, route or lease changed' WHERE id=?").bind(now).bind(id).execute(&mut **tx).await?;
        sqlx::query("UPDATE operator_notices SET state=CASE WHEN unresolved=1 THEN 'pending' ELSE 'retired' END,batch=NULL WHERE batch=?").bind(id).execute(&mut **tx).await?;
        event_tx(
            tx,
            group,
            Some(id),
            "cancelled_before_exposure",
            json!({}),
            now,
        )
        .await?;
        return Ok(None);
    }
    for item in items {
        let result = sqlx::query("UPDATE operator_notice_spending SET exposures=exposures+1,next_at=? WHERE account=? AND generation=? AND exposures<? AND next_at<=?")
            .bind(now.checked_add(COOLDOWN).context("notice cooldown overflow")?).bind(item.get::<i64,_>("account")).bind(generation).bind(MAX_EXPOSURES).bind(now).execute(&mut **tx).await?;
        ensure!(
            result.rows_affected() == 1,
            "notice exposure allowance conflict"
        );
    }
    sqlx::query("UPDATE operator_notice_batches SET state='exposed',exposed_at=? WHERE id=?")
        .bind(now)
        .bind(id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE operator_notices SET state='exposed' WHERE batch=?")
        .bind(id)
        .execute(&mut **tx)
        .await?;
    event_tx(
        tx,
        group,
        Some(id),
        "exposed",
        json!({"generation":generation}),
        now,
    )
    .await?;
    Ok(Some(batch))
}

/// Fence results by exact batch, route generation, source revision and membership.
pub(crate) async fn finish_operator_notice_batch_tx(
    tx: &mut Tx<'_>,
    group: &str,
    id: &str,
    owner: &str,
    result: TransportResult,
    detail: &str,
    now: i64,
) -> Result<()> {
    crate::bounded(detail, 4096, "notice result detail")?;
    let (batch, state) = load_batch_tx(tx, group, id, owner).await?;
    if state == result.text() && state != "uncertain" {
        return Ok(());
    }
    ensure!(
        state == "exposed" || state == "uncertain",
        "notice result requires an exposed batch"
    );
    let (generation, route, _) = route_tx(tx, group).await?;
    let items = sqlx::query("SELECT i.*,n.source_kind,n.source_id FROM operator_notice_batch_items i JOIN operator_notices n ON n.id=i.notice WHERE i.batch=? ORDER BY i.notice LIMIT 11")
        .bind(id).fetch_all(&mut **tx).await?;
    ensure!(
        !items.is_empty() && items.len() <= MAX_ITEMS,
        "invalid frozen notice membership"
    );
    for item in items {
        let source =
            NoticeSource::from_parts(&item.get::<String, _>("source_kind"), item.get("source_id"))?;
        ensure!(source.class() == batch.class, "notice batch class mismatch");
        project_notice_tx(tx, group, source, now).await?;
        if generation == batch.generation && route == batch.route {
            sqlx::query("UPDATE operator_notices SET state=?,accepted_revision=CASE WHEN ? THEN max(accepted_revision,?) ELSE accepted_revision END,accepted_generation=CASE WHEN ? THEN ? ELSE accepted_generation END WHERE id=? AND revision=? AND source_snapshot=? AND batch=? AND unresolved=1")
                .bind(result.text()).bind(result.accepted()).bind(item.get::<i64,_>("revision")).bind(result.accepted()).bind(batch.generation).bind(item.get::<i64,_>("notice")).bind(item.get::<i64,_>("revision")).bind(item.get::<String,_>("source_snapshot")).bind(id).execute(&mut **tx).await?;
        } else {
            // Record the old result without accepting the repaired route. A
            // supported completed invocation releases it; uncertainty still blocks.
            sqlx::query("UPDATE operator_notices SET state=?,batch=CASE WHEN ? THEN NULL ELSE batch END WHERE id=? AND batch=? AND unresolved=1")
                .bind(if result.uncertain() {"uncertain"} else {"pending"})
                .bind(!result.uncertain()).bind(item.get::<i64,_>("notice")).bind(id).execute(&mut **tx).await?;
        }
    }
    sqlx::query("UPDATE operator_notice_batches SET state=?,finished_at=?,detail=? WHERE id=?")
        .bind(result.text())
        .bind(now)
        .bind(detail)
        .bind(id)
        .execute(&mut **tx)
        .await?;
    event_tx(tx, group, Some(id), "transport_result", json!({"result":result.text(),"transport_accepted":result.accepted(),"sender_uncertain":result.uncertain(),"detail":detail,"captured_generation":batch.generation,"current_generation":generation}), now).await?;
    Ok(())
}

/// Called only by the central explicit operator route configuration operation,
/// after it has changed the saved route in this same transaction. No actor is
/// fabricated. An identical retry keeps its generation and all prior spending.
pub(crate) async fn repair_operator_route_tx(
    tx: &mut Tx<'_>,
    group: &str,
    expected_generation: i64,
    key: &str,
    reason: &str,
    now: i64,
) -> Result<i64> {
    crate::name(key)?;
    crate::bounded(reason, 4096, "route repair reason")?;
    ensure!(!reason.trim().is_empty(), "route repair reason required");
    let (generation, route, _) = route_tx(tx, group).await?;
    let canonical = serde_json::to_string(
        &json!({"expected_generation":expected_generation,"route":serde_json::from_str::<Value>(&route)?,"reason":reason}),
    )?;
    if let Some(old) = sqlx::query(
        "SELECT canonical,generation FROM operator_notice_repairs WHERE group_name=? AND key=?",
    )
    .bind(group)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?
    {
        ensure!(
            old.get::<String, _>("canonical") == canonical,
            "route repair key conflict"
        );
        return Ok(old.get("generation"));
    }
    ensure!(
        generation == expected_generation,
        "route generation conflict"
    );
    let old_route: String =
        sqlx::query_scalar("SELECT route FROM operator_notice_routes WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
    let changed =
        serde_json::from_str::<Value>(&old_route)? != serde_json::from_str::<Value>(&route)?;
    let next = if changed {
        generation
            .checked_add(1)
            .context("route generation overflow")?
    } else {
        generation
    };
    if changed {
        sqlx::query(
            "UPDATE operator_notice_routes SET generation=?,route=?,changed=? WHERE group_name=?",
        )
        .bind(next)
        .bind(route)
        .bind(now)
        .bind(group)
        .execute(&mut **tx)
        .await?;
        sqlx::query("UPDATE operator_notice_batches SET state='cancelled',finished_at=?,detail='operator route repair before exposure' WHERE group_name=? AND state='reserved'")
            .bind(now).bind(group).execute(&mut **tx).await?;
        // Keep exposed/uncertain batches until an actual process result. A new
        // route must not overlap a potentially live old sender on this account.
        sqlx::query("UPDATE operator_notices SET state='pending',batch=NULL WHERE group_name=? AND unresolved=1 AND state IN ('accepted','failed','unconfigured','reserved')")
            .bind(group).execute(&mut **tx).await?;
        event_tx(
            tx,
            group,
            None,
            "route_repaired",
            json!({"from":generation,"to":next,"reason":reason}),
            now,
        )
        .await?;
    }
    sqlx::query(
        "INSERT INTO operator_notice_repairs(group_name,key,canonical,generation) VALUES(?,?,?,?)",
    )
    .bind(group)
    .bind(key)
    .bind(canonical)
    .bind(next)
    .execute(&mut **tx)
    .await?;
    Ok(next)
}

/// Read a bounded ledger page without recording source retrieval.
pub(crate) async fn notice_readback_tx(
    tx: &mut Tx<'_>,
    group: &str,
    after: i64,
    limit: usize,
) -> Result<Vec<NoticeView>> {
    ensure!(
        after >= 0 && (1..=100).contains(&limit),
        "invalid notice readback page"
    );
    let (generation, route, _) = route_tx(tx, group).await?;
    let route_configured = configured(&route)?;
    let saved: String =
        sqlx::query_scalar("SELECT route FROM operator_notice_routes WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
    let route_current =
        serde_json::from_str::<Value>(&saved)? == serde_json::from_str::<Value>(&route)?;
    let rows = sqlx::query(
        "SELECT n.*,a.source_key,a.episode,(SELECT detail FROM operator_notice_batches b WHERE b.id=n.batch) AS detail,coalesce(s.exposures,0) AS exposures,coalesce(s.next_at,0) AS next_at,(SELECT b.id FROM operator_notice_batch_items i JOIN operator_notice_batches b ON b.id=i.batch WHERE i.account=n.account AND b.state IN ('reserved','exposed','uncertain') ORDER BY b.id LIMIT 1) AS outstanding_batch,EXISTS(SELECT 1 FROM operator_notice_legacy l WHERE l.account=n.account AND l.state IN ('attempting','uncertain')) AS legacy_sender_uncertain FROM operator_notices n JOIN operator_notice_accounts a ON a.id=n.account LEFT JOIN operator_notice_spending s ON s.account=n.account AND s.generation=? WHERE n.group_name=? AND n.id>? ORDER BY n.id LIMIT ?",
    )
    .bind(generation)
    .bind(group)
    .bind(after)
    .bind(limit as i64)
    .fetch_all(&mut **tx)
    .await?;
    rows.into_iter()
        .map(|r| {
            let source_snapshot: Value =
                serde_json::from_str(&r.get::<String, _>("source_snapshot"))?;
            let responsibility = source_snapshot
                .get("responsibility")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?;
            Ok(NoticeView {
                id: r.get("id"),
                source: NoticeSource::from_parts(
                    &r.get::<String, _>("source_kind"),
                    r.get("source_id"),
                )?,
                account: r.get("account"),
                revision: r.get("revision"),
                accepted_revision: r.get("accepted_revision"),
                accepted_generation: r.get("accepted_generation"),
                state: r.get("state"),
                responsible: r.get("responsible"),
                responsibility,
                unresolved: r.get("unresolved"),
                source_key: r.get("source_key"),
                episode: r.get("episode"),
                source_snapshot,
                route_generation: generation,
                route_configured,
                route_current,
                exposures: r.get("exposures"),
                next_attempt: r.get("next_at"),
                outstanding_batch: r.get("outstanding_batch"),
                legacy_sender_uncertain: r.get("legacy_sender_uncertain"),
                detail: r.get("detail"),
            })
        })
        .collect()
}

/// Bounded status projection. Actor scoping preserves the original attention
/// recipient/authority view and adds only that actor's operator obligations.
pub(crate) async fn status_notices_tx(
    tx: &mut Tx<'_>,
    group: Option<&str>,
    actor: Option<i64>,
    now: i64,
) -> Result<(Vec<Value>, bool)> {
    let groups: Vec<String> = sqlx::query_scalar("SELECT g.name FROM groups g WHERE g.home_machine IN (SELECT id FROM node) AND (? IS NULL OR g.name=?) AND (? IS NULL OR EXISTS(SELECT 1 FROM mailboxes b WHERE b.id=? AND b.group_name=g.name)) ORDER BY g.name LIMIT 11")
        .bind(group).bind(group).bind(actor).bind(actor).fetch_all(&mut **tx).await?;
    let mut more = groups.len() > 10;
    for group in groups.iter().take(10) {
        let (attention, obligation) = project_notice_page_tx(tx, group, now, 20).await?;
        more |= attention != 0 || obligation != 0;
    }
    let rows = sqlx::query("SELECT n.id,n.group_name,n.source_kind,n.source_id FROM operator_notices n WHERE (? IS NULL OR n.group_name=?) AND (? IS NULL OR (n.source_kind IN ('attention_occurrence','operator_obligation') AND EXISTS(SELECT 1 FROM mailboxes b WHERE b.id=? AND b.group_name=n.group_name AND b.name=n.responsible)) OR (n.source_kind='attention_occurrence' AND EXISTS(SELECT 1 FROM attention_occurrences o JOIN followups f ON f.id=o.followup WHERE o.id=n.source_id AND (f.recipient=? OR f.authority=?)))) ORDER BY n.id DESC LIMIT 101")
        .bind(group).bind(group).bind(actor).bind(actor).bind(actor).bind(actor).fetch_all(&mut **tx).await?;
    more |= rows.len() > 100;
    let mut notices = Vec::new();
    for row in rows.iter().take(100) {
        let group: String = row.get("group_name");
        let source =
            NoticeSource::from_parts(&row.get::<String, _>("source_kind"), row.get("source_id"))?;
        let id = project_notice_tx(tx, &group, source, now).await?;
        let view = notice_readback_tx(tx, &group, id - 1, 1)
            .await?
            .pop()
            .context("projected notice missing")?;
        let mut value = serde_json::to_value(&view)?;
        value["notice_id"] = json!(id);
        // Preserve legacy occurrence identifiers for existing status consumers.
        if let NoticeSource::AttentionOccurrence(occurrence) = source {
            value["id"] = json!(occurrence);
        }
        value["group"] = json!(group);
        value["attempts"] = json!(view.exposures);
        value["projection"] = json!("shared_operator_notice");
        value["transport_accepted_current"] = json!(
            view.route_current
                && view.accepted_generation == Some(view.route_generation)
                && view.accepted_revision == view.revision
        );
        notices.push(value);
    }
    Ok((notices, more))
}

/// Reserve one group/class opportunity before either projection can fail.
async fn reserve_dispatch_turn_tx(
    tx: &mut Tx<'_>,
    now: i64,
) -> Result<Option<(String, NoticeClass)>> {
    sqlx::query(
        "UPDATE operator_notice_dispatch_cursor SET last_group=last_group WHERE singleton=1",
    )
    .execute(&mut **tx)
    .await?;
    let after: String = sqlx::query_scalar(
        "SELECT last_group FROM operator_notice_dispatch_cursor WHERE singleton=1",
    )
    .fetch_one(&mut **tx)
    .await?;
    let group: Option<String> = sqlx::query_scalar("SELECT name FROM groups WHERE home_machine IN (SELECT id FROM node) ORDER BY CASE WHEN name>? THEN 0 ELSE 1 END,name LIMIT 1")
        .bind(after).fetch_optional(&mut **tx).await?;
    if let Some(group) = &group {
        sqlx::query("UPDATE operator_notice_dispatch_cursor SET last_group=? WHERE singleton=1")
            .bind(group)
            .execute(&mut **tx)
            .await?;
    }
    let Some(group) = group else {
        return Ok(None);
    };
    sqlx::query("INSERT OR IGNORE INTO operator_notice_projection(group_name) VALUES(?)")
        .bind(&group)
        .execute(&mut **tx)
        .await?;
    let class = if crate::supervisor_failures::supervisor_failure_work_available_tx(tx, &group, now)
        .await?
    {
        let saved: String = sqlx::query_scalar(
            "SELECT next_class FROM operator_notice_projection WHERE group_name=?",
        )
        .bind(&group)
        .fetch_one(&mut **tx)
        .await?;
        let class = NoticeClass::parse(&saved)?;
        let next = match class {
            NoticeClass::Infrastructure => NoticeClass::Ordinary,
            NoticeClass::Ordinary => NoticeClass::Infrastructure,
        };
        sqlx::query("UPDATE operator_notice_projection SET next_class=? WHERE group_name=?")
            .bind(next.text())
            .bind(&group)
            .execute(&mut **tx)
            .await?;
        class
    } else {
        NoticeClass::Ordinary
    };
    Ok(Some((group, class)))
}

/// Sole dispatcher behind followup::notify_operators. One fair group and at
/// most ten frozen notice references per call; no I/O holds a database writer.
pub(crate) async fn dispatch_notices(store: &Store, now: i64) -> Result<()> {
    let started = tokio::time::Instant::now();
    let mut select = store.pool().begin().await?;
    let turn = reserve_dispatch_turn_tx(&mut select, now).await?;
    // Commit the group and its class before either source projection can fail.
    select.commit().await?;
    let Some((group, class)) = turn else {
        return Ok(());
    };
    let owner = format!("notice-{}", uuid::Uuid::new_v4());
    let mut tx = store.pool().begin().await?;
    match class {
        NoticeClass::Ordinary => {
            project_notice_page_tx(&mut tx, &group, now, 100).await?;
        }
        NoticeClass::Infrastructure => {
            project_infrastructure_notice_page_tx(&mut tx, &group, now, 100).await?;
        }
    }
    let batch = reserve_notice_batch_for_class_tx(&mut tx, &group, &owner, now, class).await?;
    tx.commit().await?;
    let Some(batch) = batch else {
        return Ok(());
    };
    // Revalidate in a new transaction after reservation, immediately before I/O.
    let observed = || now.saturating_add(started.elapsed().as_secs().min(i64::MAX as u64) as i64);
    let mut tx = store.pool().begin().await?;
    let exposed =
        expose_operator_notice_batch_tx(&mut tx, &group, &batch.id, &owner, observed()).await?;
    tx.commit().await?;
    let Some(batch) = exposed else {
        return Ok(());
    };
    let (result, detail) = send_batch(&batch).await;
    let mut tx = store.pool().begin().await?;
    let finishing = finish_operator_notice_batch_tx(
        &mut tx,
        &group,
        &batch.id,
        &owner,
        result,
        &detail,
        observed(),
    )
    .await;
    if let Err(error) = finishing {
        tx.rollback().await?;
        // Keep the observed transport fact even when its source owner cannot
        // currently validate a projection. Never turn that failure into a retry.
        let mut failed = store.pool().begin().await?;
        recovery::reserve_home_tx(&mut failed, &group).await?;
        let bounded_error: String = format!("{error:#}").chars().take(512).collect();
        event_tx(&mut failed, &group, Some(&batch.id), "transport_result_unprojected",
            json!({"transport_accepted":result.accepted(),"observed_result":result.text(),"detail":detail,"projection_error":bounded_error}), observed()).await?;
        sqlx::query("UPDATE operator_notice_batches SET state='uncertain',detail='Observed result could not be projected; inspect immutable transport_result_unprojected event' WHERE id=? AND owner=? AND state IN ('exposed','uncertain')")
            .bind(&batch.id).bind(&owner).execute(&mut *failed).await?;
        sqlx::query("UPDATE operator_notices SET state='uncertain' WHERE batch=? AND unresolved=1")
            .bind(&batch.id)
            .execute(&mut *failed)
            .await?;
        failed.commit().await?;
        return Err(error);
    }
    tx.commit().await?;
    Ok(())
}

async fn send_batch(batch: &NoticeBatch) -> (TransportResult, String) {
    let route: Value = match serde_json::from_str(&batch.route) {
        Ok(route) => route,
        Err(error) => {
            return (
                TransportResult::Failed,
                format!("Invalid frozen route: {error}"),
            );
        }
    };
    if !route["notifier"].is_null() {
        let args: Vec<String> = match serde_json::from_value(route["notifier"].clone()) {
            Ok(args) => args,
            Err(error) => {
                return (
                    TransportResult::Failed,
                    format!("Invalid notifier arguments: {error}"),
                );
            }
        };
        return run_notifier(&args, &batch.payload).await;
    }
    if let Some(socket) = route["socket"].as_str().filter(|s| !s.is_empty()) {
        let count = serde_json::from_str::<Value>(&batch.payload)
            .ok()
            .and_then(|v| v["items"].as_array().map(Vec::len))
            .unwrap_or(0);
        return match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::herdr::notify(std::path::Path::new(socket), &batch.group, count),
        )
        .await
        {
            Ok(Ok(())) => (
                TransportResult::Accepted,
                "Herdr acknowledged notification.show; no business receipt".into(),
            ),
            Ok(Err(error)) => (
                TransportResult::Uncertain,
                format!("Herdr acceptance unknown: {error:#}"),
            ),
            Err(_) => (
                TransportResult::Uncertain,
                "Herdr response timed out; external acceptance unknown".into(),
            ),
        };
    }
    (
        TransportResult::Failed,
        "Frozen route has no transport; no I/O performed".into(),
    )
}

async fn run_notifier(args: &[String], payload: &str) -> (TransportResult, String) {
    use tokio::io::AsyncWriteExt;
    let Some(executable) = args.first() else {
        return (
            TransportResult::Failed,
            "Empty frozen notifier; no I/O performed".into(),
        );
    };
    let started = tokio::time::Instant::now();
    let mut child = match tokio::process::Command::new(executable)
        .args(&args[1..])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            return (
                TransportResult::Failed,
                format!("Notifier did not spawn: {error}"),
            );
        }
    };
    let io = tokio::time::timeout_at(started + std::time::Duration::from_secs(4), async {
        let mut input = child.stdin.take().context("notifier stdin missing")?;
        input.write_all(payload.as_bytes()).await?;
        input.shutdown().await?;
        drop(input);
        Ok::<_, anyhow::Error>(child.wait().await?)
    })
    .await;
    match io {
        Ok(Ok(status)) => {
            let result = if status.success() {
                TransportResult::AcceptedUncertain
            } else {
                TransportResult::Uncertain
            };
            (
                result,
                format!(
                    "Notifier parent exited {status}; descendant and external-effect closure unverified"
                ),
            )
        }
        outcome => {
            let cause = match outcome {
                Ok(Err(error)) => format!("notifier I/O failed: {error:#}"),
                Err(_) => "notifier I/O timed out".into(),
                Ok(Ok(_)) => unreachable!(),
            };
            let kill = child.start_kill();
            let observation =
                tokio::time::timeout_at(started + std::time::Duration::from_secs(5), child.wait())
                    .await;
            // Parent reaping never proves arbitrary descendants/effects closed.
            (
                TransportResult::Uncertain,
                format!(
                    "{cause}; parent kill={kill:?}, observation={observation:?}; sender closure unknown"
                ),
            )
        }
    }
}

impl Store {
    /// Inspect current notice projections; reading never acknowledges business work.
    pub async fn operator_notices(
        &self,
        group: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<NoticeView>> {
        let mut tx = self.pool().begin().await?;
        let result = notice_readback_tx(&mut tx, group, after, limit).await?;
        tx.commit().await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        followup::{self, Mode, Policy},
        store::Publish,
    };

    async fn fixture(count: usize) -> Result<(tempfile::TempDir, Store, i64)> {
        let root = tempfile::tempdir()?;
        let store = Store::open(root.path(), true).await?;
        store.enroll("g", None).await?;
        let token = store.register("g", "writer", false).await?;
        store.register("g", "worker", false).await?;
        let actor = store.authenticate("g", Some(&token)).await?;
        let now = crate::now()?;
        store
            .configure_followups(
                "g",
                &Policy {
                    mode: Mode::Enabled,
                    interval_seconds: 60,
                    max_seconds: 240,
                    notifier: Some(vec!["/usr/bin/false".into()]),
                },
                now,
            )
            .await?;
        for i in 0..count {
            store
                .publish(
                    &actor,
                    Publish {
                        recipients: vec!["worker".into()],
                        key: format!("notice-{i}"),
                        summary: "Decision needed".into(),
                        body: "Unresolved original source".into(),
                        due_after: None,
                        reply_to: None,
                        work_id: None,
                    },
                    now,
                )
                .await?;
        }
        followup::reconcile(&store, now + 241).await?;
        let mut tx = store.pool().begin().await?;
        project_notice_page_tx(&mut tx, "g", now + 542, 100).await?;
        tx.commit().await?;
        Ok((root, store, now + 542))
    }

    #[tokio::test]
    async fn reservation_spends_nothing_and_exposure_replay_never_sends_twice() -> Result<()> {
        let (_root, store, now) = fixture(1).await?;
        let mut tx = store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
            .await?
            .context("batch")?;
        let spent: i64 = sqlx::query_scalar("SELECT sum(exposures) FROM operator_notice_spending")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(spent, 0);
        assert!(
            expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now)
                .await?
                .is_some()
        );
        assert!(
            expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now)
                .await?
                .is_none()
        );
        tx.commit().await?;
        // A deterministic transport-result fixture; no native delivery claim.
        let mut tx = store.pool().begin().await?;
        finish_operator_notice_batch_tx(
            &mut tx,
            "g",
            &batch.id,
            "dispatcher",
            TransportResult::Accepted,
            "transport fixture accepted",
            now + 1,
        )
        .await?;
        let rows = notice_readback_tx(&mut tx, "g", 0, 100).await?;
        assert_eq!(rows[0].accepted_revision, rows[0].revision);
        assert!(
            rows[0].unresolved,
            "transport acceptance cannot handle the source"
        );
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn route_repair_fences_old_result_and_blocks_overlapping_sender() -> Result<()> {
        let (_root, store, now) = fixture(1).await?;
        let mut tx = store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
            .await?
            .context("batch")?;
        expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now)
            .await?
            .context("exposed")?;
        tx.commit().await?;
        let mut tx = store.pool().begin().await?;
        sqlx::query("UPDATE followup_policy SET notifier=? WHERE group_name='g'")
            .bind("[\"/usr/bin/true\"]")
            .execute(&mut *tx)
            .await?;
        let generation = repair_operator_route_tx(
            &mut tx,
            "g",
            batch.generation,
            "repair",
            "explicit operator repair",
            now + 1,
        )
        .await?;
        assert_eq!(generation, batch.generation + 1);
        assert!(
            reserve_operator_notice_batch_tx(&mut tx, "g", "other", now + 400)
                .await?
                .is_none(),
            "lease expiry cannot stop an exposed process"
        );
        finish_operator_notice_batch_tx(
            &mut tx,
            "g",
            &batch.id,
            "dispatcher",
            TransportResult::Accepted,
            "old sender exited",
            now + 401,
        )
        .await?;
        let view = notice_readback_tx(&mut tx, "g", 0, 100).await?;
        assert_eq!(
            view[0].accepted_revision, 0,
            "old route cannot accept repaired route"
        );
        assert_eq!(view[0].state, "pending");
        assert!(
            reserve_operator_notice_batch_tx(&mut tx, "g", "other", now + 402)
                .await?
                .is_some()
        );
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_sender_stays_blocked_and_later_sources_are_not_covered() -> Result<()> {
        let (_root, store, now) = fixture(11).await?;
        let mut tx = store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
            .await?
            .context("batch")?;
        let payload: Value = serde_json::from_str(&batch.payload)?;
        assert_eq!(payload["items"].as_array().context("items")?.len(), 10);
        assert!(batch.payload.len() <= MAX_BYTES);
        expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now)
            .await?
            .context("exposed")?;
        finish_operator_notice_batch_tx(
            &mut tx,
            "g",
            &batch.id,
            "dispatcher",
            TransportResult::Uncertain,
            "no authenticated completion",
            now + 5,
        )
        .await?;
        let next = reserve_operator_notice_batch_tx(&mut tx, "g", "other", now + 1000)
            .await?
            .context("eleventh source")?;
        let next_payload: Value = serde_json::from_str(&next.payload)?;
        assert_eq!(next_payload["items"].as_array().context("items")?.len(), 1);
        let rows = notice_readback_tx(&mut tx, "g", 0, 100).await?;
        assert_eq!(rows.iter().filter(|n| n.state == "uncertain").count(), 10);
        assert!(rows.iter().all(|n| n.accepted_revision == 0));
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn explicit_pause_after_reservation_cancels_without_spending() -> Result<()> {
        let (_root, store, now) = fixture(1).await?;
        let mut tx = store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
            .await?
            .context("batch")?;
        sqlx::query("UPDATE groups SET paused=1 WHERE name='g'")
            .execute(&mut *tx)
            .await?;
        assert!(
            expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now)
                .await?
                .is_none()
        );
        let spent: i64 = sqlx::query_scalar("SELECT sum(exposures) FROM operator_notice_spending")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(spent, 0);
        tx.commit().await?;
        Ok(())
    }

    async fn obligation_fixture() -> Result<(tempfile::TempDir, Store, i64, i64)> {
        let (root, store, now) = fixture(0).await?;
        let writer = store.mailbox("g", "writer").await?;
        let message = store
            .publish(
                &writer,
                Publish {
                    recipients: vec!["worker".into()],
                    key: "operator-source".into(),
                    summary: "Decision needed".into(),
                    body: "Original source".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                now - 542,
            )
            .await?;
        let case = store
            .recover_expired_obligation(
                &writer,
                "create-case",
                recovery::Obligation::Delivery {
                    message,
                    recipient: "worker".into(),
                },
                now,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        project_notice_tx(
            &mut tx,
            "g",
            NoticeSource::OperatorObligation(case.operator_obligation),
            now,
        )
        .await?;
        tx.commit().await?;
        Ok((root, store, now, message))
    }

    #[tokio::test]
    async fn real_source_settlement_before_exposure_cancels_frozen_obligation() -> Result<()> {
        let (_root, store, now, message) = obligation_fixture().await?;
        let mut tx = store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
            .await?
            .context("batch")?;
        tx.commit().await?;
        let worker = store.mailbox("g", "worker").await?;
        store
            .resolve(&worker, message, "Handled original source", None, now + 1)
            .await?;
        // No supervisor page runs between disposition and exposure.
        let mut tx = store.pool().begin().await?;
        assert!(
            expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now + 2)
                .await?
                .is_none()
        );
        let view = notice_readback_tx(&mut tx, "g", 0, 100).await?;
        assert_eq!(view[0].state, "retired");
        assert!(!view[0].unresolved);
        assert_eq!(view[0].exposures, 0);
        assert_eq!(view[0].accepted_revision, 0);
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn real_source_settlement_during_io_fences_late_accepted_revision() -> Result<()> {
        let (_root, store, now, message) = obligation_fixture().await?;
        let mut tx = store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
            .await?
            .context("batch")?;
        expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now)
            .await?
            .context("exposed")?;
        tx.commit().await?;
        let worker = store.mailbox("g", "worker").await?;
        store
            .resolve(&worker, message, "Handled original source", None, now + 1)
            .await?;
        let mut tx = store.pool().begin().await?;
        finish_operator_notice_batch_tx(
            &mut tx,
            "g",
            &batch.id,
            "dispatcher",
            TransportResult::Accepted,
            "late result fixture",
            now + 2,
        )
        .await?;
        let view = notice_readback_tx(&mut tx, "g", 0, 100).await?;
        assert_eq!(view[0].state, "retired");
        assert_eq!(
            view[0].accepted_revision, 0,
            "old membership cannot accept a newer source revision"
        );
        assert_eq!(
            view[0].exposures, 1,
            "spent attempt remains after source settlement"
        );
        let state: String =
            sqlx::query_scalar("SELECT state FROM operator_notice_batches WHERE id=?")
                .bind(&batch.id)
                .fetch_one(&mut *tx)
                .await?;
        assert_eq!(
            state, "accepted",
            "retain actual old transport evidence separately"
        );
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn legacy_process_uncertainty_is_not_erased_by_pending_projection_or_new_route()
    -> Result<()> {
        let (_root, store, now) = fixture(1).await?;
        let mut tx = store.pool().begin().await?;
        let view = notice_readback_tx(&mut tx, "g", 0, 100).await?;
        let occurrence = match view[0].source {
            NoticeSource::AttentionOccurrence(id) => id,
            _ => anyhow::bail!("legacy fixture"),
        };
        // Explicit imported-history fixture, not proof of a live runtime. The
        // negative fact is that no supported process-closure evidence exists.
        sqlx::query("INSERT INTO operator_notice_legacy(occurrence,account,attempts,state,next_at,provenance) VALUES(?,?,1,'uncertain',0,'{}')")
            .bind(occurrence).bind(view[0].account).execute(&mut *tx).await?;
        assert_eq!(view[0].state, "pending");
        assert!(
            reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
                .await?
                .is_none()
        );
        sqlx::query("UPDATE followup_policy SET notifier=? WHERE group_name='g'")
            .bind("[\"/usr/bin/true\"]")
            .execute(&mut *tx)
            .await?;
        repair_operator_route_tx(
            &mut tx,
            "g",
            view[0].route_generation,
            "repair-unknown",
            "Explicit route repair cannot stop an old process",
            now + 1,
        )
        .await?;
        assert!(
            reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now + 1000)
                .await?
                .is_none()
        );
        assert!(notice_readback_tx(&mut tx, "g", 0, 100).await?[0].legacy_sender_uncertain);
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn acknowledged_old_generation_remains_history_after_actual_route_change() -> Result<()> {
        let (_root, store, now) = fixture(1).await?;
        let mut tx = store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "dispatcher", now)
            .await?
            .context("batch")?;
        expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "dispatcher", now)
            .await?
            .context("exposure")?;
        finish_operator_notice_batch_tx(
            &mut tx,
            "g",
            &batch.id,
            "dispatcher",
            TransportResult::Accepted,
            "acknowledged transport fixture",
            now + 1,
        )
        .await?;
        tx.commit().await?;
        let before = store.operator_notices("g", 0, 100).await?.remove(0);
        store
            .patch_followups(
                "g",
                &crate::followup::PolicyPatch {
                    notifier: Some(Some(vec!["/usr/bin/true".into()])),
                    ..Default::default()
                },
                now + 2,
            )
            .await?;
        let status = store.followup_status(Some("g"), now + 2).await?;
        let after = store.operator_notices("g", 0, 100).await?.remove(0);
        assert_eq!(after.accepted_revision, before.accepted_revision);
        assert_eq!(after.accepted_generation, Some(before.route_generation));
        assert_eq!(after.route_generation, before.route_generation + 1);
        assert_eq!(after.state, "pending");
        assert_eq!(
            status["operator_notifications"][0]["transport_accepted_current"],
            false
        );
        Ok(())
    }

    #[tokio::test]
    async fn shared_status_does_not_expose_another_mailbox_source() -> Result<()> {
        let (_root, store, now) = fixture(1).await?;
        store.register("g", "unrelated", false).await?;
        let unrelated = store.mailbox("g", "unrelated").await?;
        let writer = store.mailbox("g", "writer").await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            status_notices_tx(&mut tx, Some("g"), Some(unrelated.id), now)
                .await?
                .0
                .is_empty()
        );
        let visible = status_notices_tx(&mut tx, Some("g"), Some(writer.id), now)
            .await?
            .0;
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0]["unresolved"], true);
        tx.rollback().await?;
        Ok(())
    }
    // These controls also compile with the frozen predecessor notifier plus
    // actual owner bridge dependencies; use unchanged assertions for red/green.
    mod bridge_predecessor_controls {
        include!("operator_notices_bridge_predecessor_tests.rs");
    }
    // Genuine bridge controls reuse the established ordinary-source fixtures.
    mod bridge_tests {
        include!("operator_notices_bridge_tests.rs");
    }
    mod corrected_boundary_tests {
        include!("operator_notices_corrected_boundary_tests.rs");
    }
}
