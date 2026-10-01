//! Independent recovery scans. The service owner supplies the separate process.
//!
//! This module performs bounded SQLite work only. It never wakes an agent,
//! consumes execution capacity, treats a lease as closure, or transmits notices.
//! Shared notifier and scheduler-source coverage remain explicit capabilities.
use crate::{
    decision_recovery::{self as recovery, DecisionCase, Obligation},
    store::Store,
    supervisor_failures::{self, ActiveSupervisorVisit, SupervisorVisit, SupervisorVisitIdentity},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};

/// A committed bounded scan; source effects and cursors commit together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisionPage {
    /// Number of followup sources inspected, including inactive plans.
    pub sources_scanned: usize,
    /// Number of existing cases reconciled.
    pub cases_scanned: usize,
    /// Eligible unlinked cases checked against actual model-owned consent.
    pub materialization_attempts: usize,
    /// New ordinary decision tasks created by the model in this page.
    pub decisions_materialized: usize,
    /// Historical materializations observed without granting fresh authority.
    pub materializations_replayed: usize,
    /// Typed model refusals which retain original operator responsibility.
    pub materializations_refused: usize,
    /// Original source rows with absent followup metadata inspected this page.
    pub missing_plans_scanned: usize,
    /// Actual scheduler causes revalidated, including terminal business tasks.
    pub execution_causes_scanned: usize,
    /// Scheduler-owned stable cause cursor for this pass.
    pub execution_cursor: String,
    /// Durable source cursor, zero after finishing the source pass.
    pub source_cursor: i64,
    /// Durable case cursor, zero after finishing the case pass.
    pub case_cursor: i64,
    /// Whether both finite passes reached their ends in this call.
    pub completed_scan: bool,
    /// Total unresolved operator business responsibilities.
    pub unresolved: i64,
    /// Remaining shared transport integration limits.
    pub capability_hold: String,
}

/// Durable process evidence, separate from host or notifier availability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisionStatus {
    /// Last successful committed page, absent until a real scan ran.
    pub heartbeat: Option<i64>,
    /// Last completed pass over this module's currently supported sources.
    pub last_full_scan: Option<i64>,
    /// Number of completed supported-source scans.
    pub completed_scans: i64,
    /// Outstanding business responsibility, regardless of transport state.
    pub unresolved: i64,
    /// True when heartbeat is absent, future-dated, or older than the limit.
    pub stale: bool,
    /// Required owner capabilities which have not been composed.
    pub capability_hold: String,
}

const CAPABILITY_HOLD: &str = "shared_notifier_unavailable";

// Schema 1 covers the five categories this owner actually implements. Schema 2
// is reserved in migration29 for a genuine sixth category, not empty padding.
const SUPERVISOR_RECEIPT_SCHEMA: i64 = 1;
const SUPERVISOR_RECEIPT_BYTES: usize = 32_768;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "key", rename_all = "snake_case")]
enum SupervisorCursor {
    Execution(String),
    Followup(i64),
    MissingTask(String),
    MissingDelivery { message: i64, recipient: i64 },
    Case(i64),
}

impl SupervisorCursor {
    fn exhausted(&self) -> bool {
        match self {
            Self::Execution(key) | Self::MissingTask(key) => key.is_empty(),
            Self::Followup(key) | Self::Case(key) => *key == 0,
            Self::MissingDelivery { message, recipient } => *message == 0 && *recipient == 0,
        }
    }

    fn advances_from(&self, before: &Self) -> bool {
        match (before, self) {
            (Self::Execution(_), Self::Execution(_)) => true, // Scheduler owns this opaque key.
            (Self::Followup(old), Self::Followup(new)) | (Self::Case(old), Self::Case(new)) => {
                new > old
            }
            (Self::MissingTask(old), Self::MissingTask(new)) => new > old,
            (
                Self::MissingDelivery {
                    message: old_message,
                    recipient: old_recipient,
                },
                Self::MissingDelivery { message, recipient },
            ) => (message, recipient) > (old_message, old_recipient),
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorSnapshot {
    cursors: [SupervisorCursor; 5],
    passes: [i64; 5],
    completed_scans: i64,
    heartbeat: Option<i64>,
    last_full_scan: Option<i64>,
    unresolved: i64,
    capability_hold: String,
}

impl SupervisorSnapshot {
    fn from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Self> {
        Ok(Self {
            cursors: [
                SupervisorCursor::Execution(row.try_get("execution_cursor")?),
                SupervisorCursor::Followup(row.try_get("source_cursor")?),
                SupervisorCursor::MissingTask(row.try_get("missing_task_cursor")?),
                SupervisorCursor::MissingDelivery {
                    message: row.try_get("missing_mail_cursor")?,
                    recipient: row.try_get("missing_recipient_cursor")?,
                },
                SupervisorCursor::Case(row.try_get("case_cursor")?),
            ],
            passes: [
                row.try_get("execution_passes")?,
                row.try_get("source_passes")?,
                row.try_get("missing_task_passes")?,
                row.try_get("missing_mail_passes")?,
                row.try_get("case_passes")?,
            ],
            completed_scans: row.try_get("completed_scans")?,
            heartbeat: row.try_get("heartbeat")?,
            last_full_scan: row.try_get("last_full_scan")?,
            unresolved: row.try_get("unresolved")?,
            capability_hold: row.try_get("capability_hold")?,
        })
    }

    async fn load_tx(tx: &mut Transaction<'_, Sqlite>, group: &str) -> Result<Self> {
        let row = sqlx::query("SELECT * FROM decision_supervision WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
        Self::from_row(&row)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            matches!(&self.cursors[0], SupervisorCursor::Execution(_))
                && matches!(&self.cursors[1], SupervisorCursor::Followup(id) if *id >= 0)
                && matches!(&self.cursors[2], SupervisorCursor::MissingTask(_))
                && matches!(&self.cursors[3], SupervisorCursor::MissingDelivery { message, recipient }
                if *message >= 0 && *recipient >= 0 && (*message == 0) == (*recipient == 0))
                && matches!(&self.cursors[4], SupervisorCursor::Case(id) if *id >= 0),
            "corrupt_supervisor_category_cursor"
        );
        ensure!(
            self.passes.iter().all(|pass| *pass >= 0)
                && self.completed_scans
                    == *self.passes.iter().min().context("missing categories")?
                && self.unresolved >= 0
                && self.heartbeat.is_none_or(|at| at >= 0)
                && self.last_full_scan.is_none_or(|at| at >= 0),
            "corrupt_supervisor_page_metadata"
        );
        Ok(())
    }
}

/// Counts describe inspected rows, not business completion or native execution.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanCounts {
    attempted: usize,
    completed: usize,
    refused: usize,
    skipped: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaterializationCounts {
    attempted: usize,
    created: usize,
    replayed: usize,
    refused: usize,
}

// Deserializing this immutable audit body never constructs completed-page proof.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorReceiptBody {
    schema: i64,
    visit: serde_json::Value,
    start_nonce: String,
    observed_at: i64,
    finalized_at: i64,
    limit: usize,
    before: SupervisorSnapshot,
    after: SupervisorSnapshot,
    scans: [ScanCounts; 5],
    materializations: MaterializationCounts,
}

struct CompletedVisit {
    identity: SupervisorVisitIdentity,
    start_nonce: String,
}

// No Clone/Deserialize or DTO constructor. Only the genuine page core below
// constructs this after its actual effects and final owner metadata write.
struct ActualCompletedSupervisorPage {
    original_visit: Option<CompletedVisit>,
    before: SupervisorSnapshot,
    after: SupervisorSnapshot,
    scans: [ScanCounts; 5],
    page: SupervisionPage,
    observed_at: i64,
    limit: usize,
}

struct SupervisorCommit {
    page: SupervisionPage,
}

/// Historical evidence read from the real protected owner receipt.
#[derive(Debug)]
pub(crate) struct ValidatedSupervisorCommit {
    visit: SupervisorVisitIdentity,
}

impl ValidatedSupervisorCommit {
    pub(crate) fn visit(&self) -> &SupervisorVisitIdentity {
        &self.visit
    }
}

fn validate_receipt_body(
    body: &SupervisorReceiptBody,
    visit: &SupervisorVisitIdentity,
) -> Result<()> {
    ensure!(
        body.schema == SUPERVISOR_RECEIPT_SCHEMA,
        "unsupported_supervisor_receipt_schema"
    );
    ensure!(
        body.visit == serde_json::to_value(visit)?,
        "supervisor_receipt_original_mismatch"
    );
    uuid::Uuid::parse_str(&body.start_nonce).context("invalid_supervisor_receipt_start")?;
    ensure!(
        (1..=100).contains(&body.limit)
            && body.observed_at >= visit.opened
            && body.finalized_at >= body.observed_at
            && body.finalized_at < visit.deadline
            && body.after.heartbeat == Some(body.observed_at),
        "corrupt_supervisor_receipt_time"
    );
    body.before.validate()?;
    body.after.validate()?;
    for (index, counts) in body.scans.iter().enumerate() {
        let accounted = counts
            .completed
            .checked_add(counts.refused)
            .and_then(|count| count.checked_add(counts.skipped));
        ensure!(
            counts.attempted <= body.limit && accounted == Some(counts.attempted),
            "corrupt_supervisor_receipt_counts"
        );
        let exhausted = body.after.cursors[index].exhausted();
        ensure!(
            body.before.passes[index].checked_add(i64::from(exhausted))
                == Some(body.after.passes[index]),
            "corrupt_supervisor_receipt_pass"
        );
        ensure!(
            exhausted || counts.attempted == body.limit,
            "incomplete_supervisor_page_cursor"
        );
        ensure!(
            exhausted || body.after.cursors[index].advances_from(&body.before.cursors[index]),
            "supervisor_receipt_cursor_did_not_advance"
        );
        if index == 0 {
            // The scheduler cause reader returns no lookahead; an exactly full
            // page retains its cursor and exhaustion is learned next visit.
            ensure!(
                exhausted == (counts.attempted < body.limit),
                "corrupt_execution_exhaustion"
            );
        }
        ensure!(
            counts.refused == 0 || index == 4,
            "invalid_supervisor_refusal_category"
        );
        ensure!(
            counts.skipped == 0 || index == 1,
            "invalid_supervisor_skip_category"
        );
    }
    let expected_full_scan = if body.after.completed_scans > body.before.completed_scans {
        Some(body.observed_at)
    } else {
        body.before.last_full_scan
    };
    ensure!(
        body.after.last_full_scan == expected_full_scan,
        "corrupt_supervisor_full_scan"
    );
    let materializations = &body.materializations;
    ensure!(
        materializations.attempted <= body.scans[4].attempted
            && materializations
                .created
                .checked_add(materializations.replayed)
                .and_then(|count| count.checked_add(materializations.refused))
                == Some(materializations.attempted)
            && materializations.refused == body.scans[4].refused,
        "corrupt_supervisor_materialization_counts"
    );
    Ok(())
}

async fn record_supervisor_commit_tx(
    tx: &mut Transaction<'_, Sqlite>,
    active: ActiveSupervisorVisit,
    completed: ActualCompletedSupervisorPage,
    finalized_at: i64,
) -> Result<SupervisorCommit> {
    let original = completed
        .original_visit
        .context("supervisor_page_has_no_visit")?;
    ensure!(
        original.identity == *active.identity() && original.start_nonce == active.start_nonce(),
        "supervisor_completed_page_start_mismatch"
    );
    let identity = supervisor_failures::validate_active_visit_tx(tx, &active, finalized_at).await?;
    ensure!(
        SupervisorSnapshot::load_tx(tx, &identity.group).await? == completed.after,
        "supervisor_completed_metadata_changed"
    );
    let body = SupervisorReceiptBody {
        schema: SUPERVISOR_RECEIPT_SCHEMA,
        visit: serde_json::to_value(&identity)?,
        start_nonce: original.start_nonce,
        observed_at: completed.observed_at,
        finalized_at,
        limit: completed.limit,
        before: completed.before,
        after: completed.after,
        scans: completed.scans,
        materializations: MaterializationCounts {
            attempted: completed.page.materialization_attempts,
            created: completed.page.decisions_materialized,
            replayed: completed.page.materializations_replayed,
            refused: completed.page.materializations_refused,
        },
    };
    validate_receipt_body(&body, &identity)?;
    let canonical = serde_json::to_string(&body)?;
    ensure!(
        canonical.len() <= SUPERVISOR_RECEIPT_BYTES,
        "supervisor_receipt_too_large"
    );
    let digest = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    sqlx::query("INSERT INTO execution_supervisor_commits(visit,nonce,group_name,schema,canonical,digest) VALUES(?,?,?,?,?,?)")
        .bind(identity.id).bind(&identity.nonce).bind(&identity.group)
        .bind(body.schema).bind(&canonical).bind(digest).execute(&mut **tx).await?;
    Ok(SupervisorCommit {
        page: completed.page,
    })
}

/// Read original commit evidence without requiring today's graph, page metadata,
/// controller, open gate or deadline. None means a real visit has no receipt.
pub(crate) async fn validate_supervisor_commit_tx(
    tx: &mut Transaction<'_, Sqlite>,
    visit_id: i64,
) -> Result<Option<ValidatedSupervisorCommit>> {
    let visit = supervisor_failures::validate_original_visit_tx(tx, visit_id).await?;
    let row = sqlx::query("SELECT nonce,group_name,schema,canonical,digest FROM execution_supervisor_commits WHERE visit=?")
        .bind(visit_id).fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let canonical: String = row.try_get("canonical")?;
    ensure!(
        canonical.len() <= SUPERVISOR_RECEIPT_BYTES,
        "supervisor_receipt_too_large"
    );
    ensure!(
        row.try_get::<String, _>("nonce")? == visit.nonce
            && row.try_get::<String, _>("group_name")? == visit.group
            && row.try_get::<i64, _>("schema")? == SUPERVISOR_RECEIPT_SCHEMA,
        "supervisor_receipt_identity_mismatch"
    );
    let expected = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    ensure!(
        row.try_get::<String, _>("digest")? == expected,
        "supervisor_receipt_digest_mismatch"
    );
    let body: SupervisorReceiptBody =
        serde_json::from_str(&canonical).context("corrupt_supervisor_receipt_body")?;
    ensure!(
        serde_json::to_string(&body)? == canonical,
        "noncanonical_supervisor_receipt"
    );
    validate_receipt_body(&body, &visit)?;
    supervisor_failures::validate_visit_start_tx(tx, visit_id, &body.start_nonce).await?;
    Ok(Some(ValidatedSupervisorCommit { visit }))
}

/// Observe actual model consent in the same transaction as the bounded page.
/// Identical consecutive refusals share evidence; every visit still revalidates
/// current policy. A storage error must abort the page, including its cursors.
async fn materialize_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case: &DecisionCase,
    now: i64,
) -> Result<Option<crate::task_graph::DecisionMaterialization>> {
    use crate::task_graph::DecisionMaterialization;
    if case.decision_task.is_some()
        || case.requires_reassessment
        || !case.current_source.unresolved
        || !matches!(case.state.as_str(), "held" | "decision_pending")
        || now >= case.hard_due
    {
        return Ok(None);
    }
    let materialization = match execution_materialization_refusal_tx(tx, group, case).await? {
        Some(reason) => DecisionMaterialization::Refused(reason),
        None => {
            crate::task_graph::materialize_recovery_case_tx(tx, group, case.id, case.version, now)
                .await?
        }
    };
    let result = match &materialization {
        DecisionMaterialization::Materialized(receipt) => {
            json!({"state":"materialized","receipt":receipt})
        }
        DecisionMaterialization::Replayed(receipt) => {
            json!({"state":"replayed","receipt":receipt})
        }
        DecisionMaterialization::Refused(reason) => {
            json!({"state":"refused","reason":reason})
        }
    };
    let canonical = json!({"case_id":case.id,"case_version":case.version,
        "source":case.current_source,"execution_guard":case.execution_guard});
    let prior: Option<(String, String)> = sqlx::query_as("SELECT canonical,result FROM decision_audit WHERE group_name=? AND case_id=? AND operation='supervisor_materialization' ORDER BY id DESC LIMIT 1")
        .bind(group).bind(case.id).fetch_optional(&mut **tx).await?;
    let unchanged = match prior {
        Some((before, observed)) => {
            serde_json::from_str::<serde_json::Value>(&before)? == canonical
                && serde_json::from_str::<serde_json::Value>(&observed)? == result
        }
        None => false,
    };
    if !unchanged {
        recovery::record_audit_tx(
            tx,
            recovery::AuditRecord {
                group,
                case: Some(case.id),
                actor: None,
                key: "supervision",
                operation: "supervisor_materialization",
                canonical: &canonical,
                result: &result,
                now,
            },
        )
        .await?;
    }
    Ok(Some(materialization))
}

/// A cleared scheduler predicate removes fresh materialization authority, not
/// the historical case or independently unresolved physical cleanup. Authenticate
/// that disposition and original link through the scheduler; malformed or missing
/// history still aborts the whole page. The normal refusal audit is deduplicated.
async fn execution_materialization_refusal_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case: &DecisionCase,
) -> Result<Option<crate::task_graph::MaterializationRefusal>> {
    use crate::execution::{ExecutionCauseRef, ExecutionCauseState, ExecutionSourceGuard};
    let Some(reference) = &case.execution_source else {
        return Ok(None);
    };
    let reference: ExecutionCauseRef = serde_json::from_value(reference.clone())?;
    ensure!(
        reference.group == group && case.group == group,
        "execution materialization group mismatch"
    );
    match crate::execution::inspect_execution_cause_tx(tx, &reference).await? {
        ExecutionCauseState::Current(_) => Ok(None),
        ExecutionCauseState::Missing => {
            anyhow::bail!("execution cause missing; not supersession evidence")
        }
        ExecutionCauseState::Superseded(_) => {
            let original: ExecutionSourceGuard = serde_json::from_value(
                case.execution_original_guard
                    .clone()
                    .context("original execution guard missing")?,
            )?;
            let ack = crate::execution::execution_case_ack_tx(tx, &reference)
                .await?
                .context("superseded execution case acknowledgement missing")?;
            ensure!(
                original.cause_ref == reference
                    && ack.source_guard == original
                    && ack.case_id == case.id
                    && case.episode == format!("execution:{}", reference.cause_generation)
                    && matches!(&case.original_source.source, Obligation::Task { id, .. }
                    if id == &reference.source_task),
                "superseded execution case identity mismatch"
            );
            Ok(Some(
                crate::task_graph::MaterializationRefusal::CaseUnavailable,
            ))
        }
    }
}

async fn reconcile_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case: DecisionCase,
    now: i64,
) -> Result<()> {
    // Execution cases require scheduler guards, including independent cleanup
    // responsibility. Until that real handoff exists, a source state alone
    // cannot settle them.
    if case.episode != "obligation"
        || case.execution_source.is_some()
        || case.decision_task.is_some()
    {
        return Ok(());
    }
    let source = recovery::inspect_source_tx(tx, group, &case.current_source.source).await?;
    if source.input_epoch.is_some() {
        return Ok(());
    }
    // A source disposition supersedes this case; a later reopen retains its
    // identity, audit and allocation. No new ordinary task is created here.
    let desired = if !source.unresolved {
        "superseded"
    } else if now >= case.hard_due {
        "operator_required"
    } else if case.state == "superseded" || case.state == "handled" {
        "held"
    } else {
        &case.state
    };
    let changed = desired != case.state || source != case.current_source;
    if changed {
        sqlx::query("UPDATE decision_cases SET state=?,current_source=?,version=version+1,last_scan=? WHERE group_name=? AND id=? AND version=?")
            .bind(desired).bind(serde_json::to_string(&source)?).bind(now).bind(group).bind(case.id).bind(case.version)
            .execute(&mut **tx).await?;
        let state = if desired == "superseded" {
            "superseded"
        } else if desired == "operator_required" {
            "escalated"
        } else {
            "pending"
        };
        sqlx::query("UPDATE operator_obligations SET state=?,version=version+1,last_scan=?,reason=? WHERE group_name=? AND case_id=?")
            .bind(state).bind(now).bind(if source.unresolved { "source remains unresolved; original authority still responsible" } else { "source disposition superseded this episode; transport did not settle it" })
            .bind(group).bind(case.id).execute(&mut **tx).await?;
        let current = recovery::load_case_tx(tx, group, case.id).await?;
        recovery::record_audit_tx(
            tx,
            recovery::AuditRecord {
                group,
                case: Some(case.id),
                actor: None,
                key: "supervision",
                operation: "source_reconciled",
                canonical: &json!({"before":case,"source":source}),
                result: &serde_json::to_value(current)?,
                now,
            },
        )
        .await?;
    } else {
        sqlx::query("UPDATE decision_cases SET last_scan=? WHERE group_name=? AND id=?")
            .bind(now)
            .bind(group)
            .bind(case.id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("UPDATE operator_obligations SET last_scan=? WHERE group_name=? AND case_id=?")
            .bind(now)
            .bind(group)
            .bind(case.id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Scan one bounded page independently of the scheduler and coordinator runtime.
///
/// The service owner must call this from the separate supervisor lifecycle.
/// A heartbeat proves this database page ran, not that the service is installed,
/// the host will stay alive, or a notification reached an operator. Current
/// supported sources are real scheduler causes, local legacy obligations and
/// existing cases. Missing composition capabilities stay visible in every result.
pub async fn supervise_recovery_page(
    store: &Store,
    group: &str,
    now: i64,
    limit: usize,
) -> Result<SupervisionPage> {
    let mut tx = store.pool().begin().await?;
    recovery::reserve_home_tx(&mut tx, group).await?;
    let completed = supervise_page_tx(&mut tx, group, now, limit, None).await?;
    tx.commit().await?;
    crate::stream::hint(store.root()).await;
    Ok(completed.page)
}

/// Run the genuine page and record its original visit in the SAME owner commit.
/// The driver retains the original outer timeout; no page grants more time.
pub(crate) async fn supervise_recovery_visit(
    store: &Store,
    visit: SupervisorVisit,
    limit: usize,
) -> Result<SupervisionPage> {
    let mut tx = store.pool().begin().await?;
    let group = visit.identity().group.clone();
    recovery::reserve_home_tx(&mut tx, &group).await?;
    let observed_at = crate::now()?;
    let active =
        supervisor_failures::begin_supervisor_visit_tx(&mut tx, &visit, observed_at).await?;
    let completed = supervise_page_tx(&mut tx, &group, observed_at, limit, Some(&active)).await?;
    let receipt = record_supervisor_commit_tx(&mut tx, active, completed, crate::now()?).await?;
    tx.commit().await?;
    crate::stream::hint(store.root()).await;
    Ok(receipt.page)
}

// This is the same bounded page used by the existing public entrypoint. Only a
// genuine guarded caller can attach a visit; the public page cannot mint one.
async fn supervise_page_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    now: i64,
    limit: usize,
    active: Option<&ActiveSupervisorVisit>,
) -> Result<ActualCompletedSupervisorPage> {
    ensure!(
        (1..=100).contains(&limit),
        "supervision page limit must be 1..100"
    );
    if let Some(active) = active {
        ensure!(
            active.identity().group == group,
            "supervisor_page_group_mismatch"
        );
    }
    let state = sqlx::query("SELECT * FROM decision_supervision WHERE group_name=?")
        .bind(group)
        .fetch_one(&mut **tx)
        .await?;
    let before = SupervisorSnapshot::from_row(&state)?;
    let original_visit = active.map(|visit| CompletedVisit {
        identity: visit.identity().clone(),
        start_nonce: visit.start_nonce().to_owned(),
    });
    let old_source: i64 = state.get("source_cursor");
    let old_case: i64 = state.get("case_cursor");
    let old_execution: String = state.get("execution_cursor");
    let execution_causes =
        crate::execution::list_execution_causes_tx(tx, group, &old_execution, limit as u32).await?;
    for reference in &execution_causes {
        let case = recovery::ensure_execution_case_tx(tx, reference, now).await?;
        // The scheduler authenticates immutable history before fresh guards.
        // Later materialization/phase versions must not remint an old ACK.
        // A historical link is evidence only, never current action authority.
        if let Some(original) = crate::execution::execution_case_ack_tx(tx, reference).await? {
            ensure!(
                original.case_id == case.id,
                "execution ACK belongs to another recovery case"
            );
        } else {
            let crate::execution::ExecutionCauseState::Current(current) =
                crate::execution::inspect_execution_cause_tx(tx, reference).await?
            else {
                anyhow::bail!("execution cause changed before current case ACK");
            };
            let proof = recovery::validate_execution_case_tx(
                tx,
                group,
                case.id,
                case.version,
                &current.guard,
            )
            .await?;
            crate::execution::ack_execution_decision_tx(tx, &proof, now).await?;
        }
    }
    let execution_cursor = if execution_causes.len() == limit {
        execution_causes
            .last()
            .map_or_else(String::new, |source| source.cause_generation.clone())
    } else {
        String::new()
    };
    // Read source rows, not active_followups: retired owners remain visible.
    let plans = sqlx::query("SELECT f.id,f.task,f.message,b.name AS recipient,w.version AS task_version,m.input_epoch FROM followups f JOIN mailboxes b ON b.id=f.recipient JOIN mailboxes a ON a.id=f.authority LEFT JOIN work_items w ON w.group_name=f.group_name AND w.id=f.task LEFT JOIN task_models m ON m.group_name=f.group_name AND m.task=f.task WHERE f.group_name=? AND f.id>? AND b.remote_machine IS NULL AND a.remote_machine IS NULL ORDER BY f.id LIMIT ?")
        .bind(group).bind(old_source).bind((limit + 1) as i64).fetch_all(&mut **tx).await?;
    let mut sources_skipped = 0;
    for row in plans.iter().take(limit) {
        // Contracted sources need scheduler's genuine stable cause and guards.
        // Keeping their durable source plan plus the explicit capability hold
        // does not claim a successful handoff or a completed global scan.
        if row.get::<Option<i64>, _>("input_epoch").is_some() {
            sources_skipped += 1;
            continue;
        }
        let source = match row.get::<Option<String>, _>("task") {
            Some(id) => Obligation::Task {
                id,
                version: row.get::<i64, _>("task_version"),
            },
            None => Obligation::Delivery {
                message: row.get("message"),
                recipient: row.get("recipient"),
            },
        };
        let current = recovery::inspect_source_tx(tx, group, &source).await?;
        if current.unresolved
            && (!current.authority_registered
                || !current.recipient_registered
                || current.plan.as_ref().is_some_and(|p| p.escalate_at <= now))
        {
            recovery::ensure_obligation_case_tx(tx, group, &current, now).await?;
        }
    }
    let source_cursor = if plans.len() > limit {
        plans[limit - 1].get("id")
    } else {
        0
    };
    // Separate keyset passes find source rows which a plan-only scan cannot see.
    // No plan is fabricated and no owner-registration predicate drops the row.
    let old_task: String = state.get("missing_task_cursor");
    let missing_tasks = sqlx::query("SELECT w.id,w.version FROM work_items w JOIN mailboxes b ON b.group_name=w.group_name AND b.name=w.owner JOIN mailboxes a ON a.group_name=w.group_name AND a.name=w.writer WHERE w.group_name=? AND w.id>? AND w.open=1 AND b.remote_machine IS NULL AND a.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM task_models m WHERE m.group_name=w.group_name AND m.task=w.id) AND NOT EXISTS(SELECT 1 FROM followups f WHERE f.group_name=w.group_name AND f.task=w.id) ORDER BY w.id LIMIT ?")
        .bind(group).bind(old_task).bind((limit + 1) as i64).fetch_all(&mut **tx).await?;
    for row in missing_tasks.iter().take(limit) {
        let source = Obligation::Task {
            id: row.get("id"),
            version: row.get("version"),
        };
        let current = recovery::inspect_source_tx(tx, group, &source).await?;
        recovery::ensure_obligation_case_tx(tx, group, &current, now).await?;
    }
    let missing_task_cursor: String = if missing_tasks.len() > limit {
        missing_tasks[limit - 1].get("id")
    } else {
        String::new()
    };
    let old_message: i64 = state.get("missing_mail_cursor");
    let old_recipient: i64 = state.get("missing_recipient_cursor");
    let missing_mail = sqlx::query("SELECT d.message,d.recipient,b.name FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient JOIN mailboxes a ON a.id=m.sender WHERE b.group_name=? AND a.group_name=b.group_name AND b.remote_machine IS NULL AND a.remote_machine IS NULL AND d.state='pending' AND (d.message>? OR (d.message=? AND d.recipient>?)) AND NOT EXISTS(SELECT 1 FROM followups f WHERE f.message=d.message AND f.recipient=d.recipient) ORDER BY d.message,d.recipient LIMIT ?")
        .bind(group).bind(old_message).bind(old_message).bind(old_recipient).bind((limit + 1) as i64).fetch_all(&mut **tx).await?;
    for row in missing_mail.iter().take(limit) {
        let source = Obligation::Delivery {
            message: row.get("message"),
            recipient: row.get("name"),
        };
        let current = recovery::inspect_source_tx(tx, group, &source).await?;
        recovery::ensure_obligation_case_tx(tx, group, &current, now).await?;
    }
    let (missing_mail_cursor, missing_recipient_cursor) = if missing_mail.len() > limit {
        (
            missing_mail[limit - 1].get::<i64, _>("message"),
            missing_mail[limit - 1].get::<i64, _>("recipient"),
        )
    } else {
        (0, 0)
    };
    // Include terminal cases to discover reopen without allocating another case.
    let case_ids = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM decision_cases WHERE group_name=? AND id>? ORDER BY id LIMIT ?",
    )
    .bind(group)
    .bind(old_case)
    .bind((limit + 1) as i64)
    .fetch_all(&mut **tx)
    .await?;
    let mut materialization_attempts = 0;
    let mut decisions_materialized = 0;
    let mut materializations_replayed = 0;
    let mut materializations_refused = 0;
    for id in case_ids.iter().take(limit) {
        let case = recovery::load_case_tx(tx, group, *id).await?;
        reconcile_case_tx(tx, group, case, now).await?;
        let case = recovery::load_case_tx(tx, group, *id).await?;
        if let Some(materialization) = materialize_case_tx(tx, group, &case, now).await? {
            materialization_attempts += 1;
            match materialization {
                crate::task_graph::DecisionMaterialization::Materialized(_) => {
                    decisions_materialized += 1
                }
                crate::task_graph::DecisionMaterialization::Replayed(_) => {
                    materializations_replayed += 1
                }
                crate::task_graph::DecisionMaterialization::Refused(_) => {
                    materializations_refused += 1
                }
            }
        }
    }
    let case_cursor = if case_ids.len() > limit {
        case_ids[limit - 1]
    } else {
        0
    };
    let source_passes = state
        .get::<i64, _>("source_passes")
        .checked_add(i64::from(source_cursor == 0))
        .ok_or_else(|| anyhow::anyhow!("source pass overflow"))?;
    let case_passes = state
        .get::<i64, _>("case_passes")
        .checked_add(i64::from(case_cursor == 0))
        .ok_or_else(|| anyhow::anyhow!("case pass overflow"))?;
    let missing_task_passes = state
        .get::<i64, _>("missing_task_passes")
        .checked_add(i64::from(missing_task_cursor.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("task pass overflow"))?;
    let missing_mail_passes = state
        .get::<i64, _>("missing_mail_passes")
        .checked_add(i64::from(missing_mail_cursor == 0))
        .ok_or_else(|| anyhow::anyhow!("delivery pass overflow"))?;
    let execution_passes = state
        .get::<i64, _>("execution_passes")
        .checked_add(i64::from(execution_cursor.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("execution pass overflow"))?;
    let completed_scans = source_passes
        .min(case_passes)
        .min(missing_task_passes)
        .min(missing_mail_passes)
        .min(execution_passes);
    let completed_scan = completed_scans > state.get::<i64, _>("completed_scans");
    let unresolved = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operator_obligations WHERE group_name=? AND state IN ('pending','escalated')")
        .bind(group).fetch_one(&mut **tx).await?;
    sqlx::query("UPDATE decision_supervision SET source_cursor=?,case_cursor=?,execution_cursor=?,missing_task_cursor=?,missing_mail_cursor=?,missing_recipient_cursor=?,heartbeat=?,source_passes=?,case_passes=?,execution_passes=?,missing_task_passes=?,missing_mail_passes=?,completed_scans=?,last_full_scan=CASE WHEN ? THEN ? ELSE last_full_scan END,unresolved=?,capability_hold=? WHERE group_name=?")
        .bind(source_cursor).bind(case_cursor).bind(&execution_cursor).bind(missing_task_cursor).bind(missing_mail_cursor).bind(missing_recipient_cursor).bind(now).bind(source_passes).bind(case_passes).bind(execution_passes).bind(missing_task_passes).bind(missing_mail_passes).bind(completed_scans).bind(completed_scan).bind(now).bind(unresolved).bind(CAPABILITY_HOLD).bind(group)
        .execute(&mut **tx).await?;
    let after = SupervisorSnapshot::load_tx(tx, group).await?;
    let counts = [
        (execution_causes.len(), 0, 0),
        (plans.len().min(limit), 0, sources_skipped),
        (missing_tasks.len().min(limit), 0, 0),
        (missing_mail.len().min(limit), 0, 0),
        (case_ids.len().min(limit), materializations_refused, 0),
    ];
    let scans = counts.map(|(attempted, refused, skipped)| ScanCounts {
        attempted,
        completed: attempted - refused - skipped,
        refused,
        skipped,
    });
    let page = SupervisionPage {
        sources_scanned: plans.len().min(limit),
        cases_scanned: case_ids.len().min(limit),
        materialization_attempts,
        decisions_materialized,
        materializations_replayed,
        materializations_refused,
        missing_plans_scanned: missing_tasks.len().min(limit) + missing_mail.len().min(limit),
        execution_causes_scanned: execution_causes.len(),
        execution_cursor,
        source_cursor,
        case_cursor,
        completed_scan,
        unresolved,
        capability_hold: CAPABILITY_HOLD.to_owned(),
    };
    Ok(ActualCompletedSupervisorPage {
        original_visit,
        before,
        after,
        scans,
        page,
        observed_at: now,
        limit,
    })
}

/// Read durable supervisor evidence. Clock reversal is unhealthy, not freshness.
pub async fn supervision_status(
    store: &Store,
    group: &str,
    now: i64,
    stale_after: i64,
) -> Result<SupervisionStatus> {
    ensure!(
        (5..=3600).contains(&stale_after),
        "heartbeat freshness bound must be 5..3600 seconds"
    );
    let row = sqlx::query("SELECT heartbeat,last_full_scan,completed_scans,unresolved,capability_hold FROM decision_supervision WHERE group_name=?")
        .bind(group).fetch_one(store.pool()).await?;
    let heartbeat: Option<i64> = row.get("heartbeat");
    let fresh = heartbeat
        .and_then(|at| now.checked_sub(at))
        .is_some_and(|age| (0..=stale_after).contains(&age));
    Ok(SupervisionStatus {
        heartbeat,
        last_full_scan: row.get("last_full_scan"),
        completed_scans: row.get("completed_scans"),
        unresolved: row.get("unresolved"),
        stale: !fresh,
        capability_hold: row.get("capability_hold"),
    })
}

/// Business facts for the sole shared notifier. This contains no route, lease,
/// retry count or transport receipt; projection does not mean delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorNoticeSource {
    /// Home group for the obligation and source.
    pub group: String,
    /// Stable operator business obligation, used in the notifier's typed source.
    pub obligation_id: i64,
    /// Captured business revision; late transport must retain this revision.
    pub obligation_version: i64,
    /// Canonical case identity.
    pub case_id: i64,
    /// Captured case revision.
    pub case_version: i64,
    /// Canonical source identity, independent of followup versions.
    pub source_key: String,
    /// Causal episode; route changes do not create another episode.
    pub episode: String,
    /// Original writer/sender responsibility, even when retired or unavailable.
    pub responsible: String,
    /// Current business state, including handled/superseded for notice retirement.
    pub state: String,
    /// Finite effective hard due time.
    pub due_at: i64,
    /// Causal explanation to project.
    pub reason: String,
    /// Bounded evidence references, not asserted execution proof.
    pub evidence: Vec<String>,
}

/// Bounded source page for the shared notifier owner's durable projection scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorNoticeSources {
    /// Captured source revisions; include terminal rows to retire old notices.
    pub items: Vec<OperatorNoticeSource>,
    /// Whether another source row follows this page.
    pub more: bool,
    /// Resume cursor for this pass; restart at zero for a new reconciliation pass.
    pub next_after: i64,
}

/// Current owner facts to capture and compare at every notifier boundary.
/// These facts confer no execution or business-disposition authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ValidatedOperatorNoticeSource {
    pub(crate) source: OperatorNoticeSource,
    pub(crate) observed_source: recovery::ObligationView,
    pub(crate) observed_execution: Option<serde_json::Value>,
    pub(crate) observed_decision: Option<recovery::ObligationView>,
}

/// Refresh one notice source in the notifier's existing writer transaction.
/// Legacy settlement is reconciled without waiting for a supervisor page.
/// Execution cases retain responsibility until the actual disposition/closure
/// transaction handles them; terminal source state or a missing cause cannot
/// retire their notice. The notifier must capture the entire returned value,
/// then compare it again before exposure and on completion. A changed value
/// requires reprojection; it must not authorize sending previously frozen bytes.
pub(crate) async fn validate_operator_notice_source_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    obligation_id: i64,
    now: i64,
) -> Result<ValidatedOperatorNoticeSource> {
    ensure!(obligation_id > 0, "positive operator obligation required");
    recovery::reserve_home_tx(tx, group).await?;
    let case_id = sqlx::query_scalar::<_, i64>(
        "SELECT case_id FROM operator_obligations WHERE group_name=? AND id=?",
    )
    .bind(group)
    .bind(obligation_id)
    .fetch_optional(&mut **tx)
    .await?
    .context("operator obligation missing")?;
    let case = recovery::load_case_tx(tx, group, case_id).await?;
    // This existing reconciliation only changes nonexecution legacy cases. It
    // neither authenticates a caller for source mutation nor settles cleanup.
    reconcile_case_tx(tx, group, case, now).await?;
    let current = recovery::load_case_tx(tx, group, case_id).await?;
    let observed_source =
        recovery::inspect_source_tx(tx, group, &current.current_source.source).await?;
    let observed_execution = if let Some(reference) = &current.execution_source {
        let reference: crate::execution::ExecutionCauseRef =
            serde_json::from_value(reference.clone())?;
        ensure!(
            reference.group == group,
            "operator execution source group mismatch"
        );
        Some(
            match crate::execution::inspect_execution_cause_tx(tx, &reference).await? {
                crate::execution::ExecutionCauseState::Current(cause) => json!({
                    "state":"current", "guard":cause.guard, "code":cause.cause.code,
                    "responsible":cause.cause.responsible, "hard_due":cause.cause.hard_due,
                }),
                crate::execution::ExecutionCauseState::Superseded(evidence) => {
                    json!({"state":"superseded","evidence":evidence})
                }
                crate::execution::ExecutionCauseState::Missing => json!({"state":"missing"}),
            },
        )
    } else {
        None
    };
    let observed_decision = if let Some(task) = &current.decision_task {
        let version = sqlx::query_scalar::<_, i64>(
            "SELECT version FROM work_items WHERE group_name=? AND id=?",
        )
        .bind(group)
        .bind(task)
        .fetch_optional(&mut **tx)
        .await?
        .context("decision task source missing")?;
        Some(
            recovery::inspect_source_tx(
                tx,
                group,
                &Obligation::Task {
                    id: task.clone(),
                    version,
                },
            )
            .await?,
        )
    } else {
        None
    };
    let source = operator_notice_sources_tx(tx, group, obligation_id - 1, 1)
        .await?
        .items
        .into_iter()
        .next()
        .filter(|item| item.obligation_id == obligation_id)
        .context("operator obligation disappeared")?;
    Ok(ValidatedOperatorNoticeSource {
        source,
        observed_source,
        observed_execution,
        observed_decision,
    })
}

/// Read authoritative operator sources inside the shared notifier transaction.
/// The notifier owner commits projection and its own cursor together, compares
/// captured revisions on completion and owns all reservation/transmission state.
/// Current responsibility never depends on a mailbox availability filter.
/// This page is an inventory; validate_operator_notice_source_tx supplies the
/// live source check required for projection, exposure and completion.
pub(crate) async fn operator_notice_sources_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    after: i64,
    limit: usize,
) -> Result<OperatorNoticeSources> {
    ensure!(
        after >= 0 && (1..=100).contains(&limit),
        "invalid operator projection page"
    );
    recovery::reserve_home_tx(tx, group).await?;
    let rows = sqlx::query("SELECT o.id,o.version,o.case_id,o.authority,o.state,o.hard_due,o.reason,o.evidence,c.version AS case_version,c.source_key,c.episode FROM operator_obligations o JOIN decision_cases c ON c.group_name=o.group_name AND c.id=o.case_id WHERE o.group_name=? AND o.id>? ORDER BY o.id LIMIT ?")
        .bind(group).bind(after).bind((limit + 1) as i64).fetch_all(&mut **tx).await?;
    let mut items = Vec::with_capacity(rows.len().min(limit));
    for row in rows.iter().take(limit) {
        let evidence: Vec<String> = serde_json::from_str(&row.get::<String, _>("evidence"))?;
        recovery::evidence_valid(&evidence)?;
        items.push(OperatorNoticeSource {
            group: group.to_owned(),
            obligation_id: row.get("id"),
            obligation_version: row.get("version"),
            case_id: row.get("case_id"),
            case_version: row.get("case_version"),
            source_key: row.get("source_key"),
            episode: row.get("episode"),
            responsible: row.get("authority"),
            state: row.get("state"),
            due_at: row.get("hard_due"),
            reason: row.get("reason"),
            evidence,
        });
    }
    Ok(OperatorNoticeSources {
        more: rows.len() > limit,
        next_after: items.last().map_or(after, |item| item.obligation_id),
        items,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Publish;

    async fn receipt_fixture() -> Result<(
        tempfile::TempDir,
        Store,
        crate::execution_driver::Controller,
    )> {
        const {
            assert!(cfg!(debug_assertions));
        }
        let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
        assert_eq!(root, "/tmp/agent-mail-durable-execution/decision-recovery");
        let dir = tempfile::Builder::new()
            .prefix("supervisor-receipt-")
            .tempdir_in(root)?;
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "sender", false).await?;
        store.register("g", "recipient", false).await?;
        let sender = store.mailbox("g", "sender").await?;
        store
            .publish(
                &sender,
                Publish {
                    recipients: vec!["recipient".into()],
                    key: "original-obligation".into(),
                    summary: "Original finite review".into(),
                    body: "Real owner source".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                100,
            )
            .await?;
        let controller = crate::execution_driver::Controller::acquire(&store, 4000).await?;
        Ok((dir, store, controller))
    }

    // Only actual persisted owner effects are compared. Controller reservation
    // occurs before this snapshot; the owner transaction may change none on error.
    async fn receipt_state(store: &Store) -> Result<std::collections::BTreeMap<String, String>> {
        let mut snapshot = std::collections::BTreeMap::new();
        for table in [
            "decision_supervision",
            "followups",
            "followup_history",
            "attention_occurrences",
            "coordination_events",
            "event_receipts",
            "outbox",
            "work_changes",
            "task_materializations",
            "task_decision_policies",
            "task_decision_policy_history",
            "work_items",
            "task_models",
            "task_results",
            "task_model_events",
            "task_decisions",
            "task_blocking_edges",
            "decision_cases",
            "decision_blockers",
            "decision_audit",
            "operator_obligations",
            "execution_tasks",
            "execution_causes",
            "execution_events",
            "execution_receipts",
            "execution_budgets",
            "execution_clock",
            "execution_attempts",
            "execution_slots",
            "execution_charges",
            "execution_supervisor_visits",
            "execution_supervisor_commits",
            "execution_supervisor_failures",
            "execution_supervisor_failure_events",
        ] {
            let columns: Vec<String> =
                sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
                    .bind(table)
                    .fetch_all(store.pool())
                    .await?;
            ensure!(
                !columns.is_empty(),
                "missing receipt control table: {table}"
            );
            let fields = columns
                .iter()
                .map(|name| format!("'{name}',\"{name}\""))
                .collect::<Vec<_>>()
                .join(",");
            let order = columns
                .iter()
                .map(|name| format!("\"{name}\""))
                .collect::<Vec<_>>()
                .join(",");
            let query = format!(
                "SELECT json_group_array(json(row_json)) FROM (SELECT json_object({fields}) AS row_json FROM {table} ORDER BY {order})"
            );
            let rows: String = sqlx::query_scalar(&query).fetch_one(store.pool()).await?;
            snapshot.insert(table.to_owned(), rows);
        }
        Ok(snapshot)
    }

    #[tokio::test]
    async fn supervisor_receipt_rejects_saved_completed_page_and_active_after_rollback()
    -> Result<()> {
        let (_dir, store, controller) = receipt_fixture().await?;
        let visit = controller
            .reserve_supervisor_visit_for_test(&store, 4000, 4010)
            .await?
            .context("genuine original reservation")?;
        let before = receipt_state(&store).await?;
        let mut tx = store.pool().begin().await?;
        let old_active =
            supervisor_failures::begin_supervisor_visit_tx(&mut tx, &visit, 4001).await?;
        let old_completed = supervise_page_tx(&mut tx, "g", 4001, 100, Some(&old_active)).await?;
        assert!(
            old_completed.page.cases_scanned > 0,
            "real source generated recovery work"
        );
        tx.rollback().await?;
        assert_eq!(receipt_state(&store).await?, before);

        let mut tx = store.pool().begin().await?;
        assert!(
            supervisor_failures::validate_active_visit_tx(&mut tx, &old_active, 4002)
                .await
                .is_err()
        );
        let new_active =
            supervisor_failures::begin_supervisor_visit_tx(&mut tx, &visit, 4002).await?;
        assert_ne!(new_active.start_nonce(), old_active.start_nonce());
        let new_completed = supervise_page_tx(&mut tx, "g", 4002, 100, Some(&new_active)).await?;
        let old_active_error =
            record_supervisor_commit_tx(&mut tx, old_active, new_completed, 4002)
                .await
                .err()
                .context("old active must not consume new completion")?;
        assert!(
            old_active_error
                .to_string()
                .contains("completed_page_start_mismatch")
        );
        let old_completed_error =
            record_supervisor_commit_tx(&mut tx, new_active, old_completed, 4002)
                .await
                .err()
                .context("old completion must not be relabeled")?;
        assert!(
            old_completed_error
                .to_string()
                .contains("completed_page_start_mismatch")
        );
        tx.rollback().await?;
        assert_eq!(receipt_state(&store).await?, before);

        let mut tx = store.pool().begin().await?;
        let active = supervisor_failures::begin_supervisor_visit_tx(&mut tx, &visit, 4003).await?;
        let completed = supervise_page_tx(&mut tx, "g", 4003, 100, Some(&active)).await?;
        record_supervisor_commit_tx(&mut tx, active, completed, 4003).await?;
        tx.commit().await?;
        let mut tx = store.pool().begin().await?;
        let receipt = validate_supervisor_commit_tx(&mut tx, visit.identity().id)
            .await?
            .context("actual fresh page committed")?;
        assert_eq!(receipt.visit(), visit.identity());
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_receipt_failures_and_expiry_roll_back_all_real_page_effects() -> Result<()>
    {
        let (_dir, store, controller) = receipt_fixture().await?;
        let visit = controller
            .reserve_supervisor_visit_for_test(&store, 4000, 4010)
            .await?
            .context("genuine reservation")?;
        let before = receipt_state(&store).await?;
        for fault in [
            "BEFORE UPDATE ON decision_supervision",
            "BEFORE INSERT ON execution_supervisor_commits",
            "AFTER INSERT ON execution_supervisor_commits",
        ] {
            sqlx::query(&format!("CREATE TRIGGER reject_owner_receipt {fault} BEGIN SELECT RAISE(ABORT,'forced_owner_receipt_failure'); END"))
                .execute(store.pool()).await?;
            let mut tx = store.pool().begin().await?;
            let failed: Result<SupervisorCommit> = async {
                let active =
                    supervisor_failures::begin_supervisor_visit_tx(&mut tx, &visit, 4001).await?;
                let completed = supervise_page_tx(&mut tx, "g", 4001, 100, Some(&active)).await?;
                record_supervisor_commit_tx(&mut tx, active, completed, 4001).await
            }
            .await;
            let error = failed.err().context("actual owner write fault must fail")?;
            assert!(format!("{error:#}").contains("forced_owner_receipt_failure"));
            tx.rollback().await?;
            sqlx::query("DROP TRIGGER reject_owner_receipt")
                .execute(store.pool())
                .await?;
            assert_eq!(
                receipt_state(&store).await?,
                before,
                "whole state after {fault}"
            );
        }
        let mut tx = store.pool().begin().await?;
        let active = supervisor_failures::begin_supervisor_visit_tx(&mut tx, &visit, 4009).await?;
        let completed = supervise_page_tx(&mut tx, "g", 4009, 100, Some(&active)).await?;
        let expired = record_supervisor_commit_tx(&mut tx, active, completed, 4010)
            .await
            .err()
            .context("original deadline applies at receipt")?;
        assert!(expired.to_string().contains("supervisor_visit_expired"));
        tx.rollback().await?;
        assert_eq!(receipt_state(&store).await?, before);
        let mut tx = store.pool().begin().await?;
        let classified =
            supervisor_failures::reconcile_supervisor_visits_tx(&mut tx, "g", 4011, 100).await?;
        assert_eq!(classified.failed, 1);
        assert!(
            validate_supervisor_commit_tx(&mut tx, visit.identity().id)
                .await?
                .is_none()
        );
        assert!(
            supervisor_failures::begin_supervisor_visit_tx(&mut tx, &visit, 4001)
                .await
                .is_err(),
            "queued stale clock cannot pass an already-closed gate"
        );
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_receipt_serializes_classifier_and_survives_restart_and_poison() -> Result<()>
    {
        let (dir, store, controller) = receipt_fixture().await?;
        let visit = controller
            .reserve_supervisor_visit_for_test(&store, 4000, 4010)
            .await?
            .context("genuine reservation")?;
        let visit_id = visit.identity().id;
        let mut owner_tx = store.pool().begin().await?;
        let active =
            supervisor_failures::begin_supervisor_visit_tx(&mut owner_tx, &visit, 4001).await?;
        let completed = supervise_page_tx(&mut owner_tx, "g", 4001, 100, Some(&active)).await?;
        record_supervisor_commit_tx(&mut owner_tx, active, completed, 4002).await?;
        let pool = store.pool().clone();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let mut classifier = tokio::spawn(async move {
            let mut tx = pool.begin().await?;
            let _ = entered.send(());
            let page = supervisor_failures::reconcile_supervisor_visits_tx(&mut tx, "g", 4011, 100)
                .await?;
            tx.commit().await?;
            Ok::<_, anyhow::Error>(page)
        });
        waiting.await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut classifier)
                .await
                .is_err(),
            "classifier cannot inspect absence while owner holds the writer"
        );
        owner_tx.commit().await?;
        // Discard the owner's response: only the retained receipt may determine
        // success once the original deadline/diagnostic path has been lost.
        let classified =
            tokio::time::timeout(std::time::Duration::from_secs(5), classifier).await???;
        assert_eq!((classified.completed, classified.failed), (1, 0));
        let original: String =
            sqlx::query_scalar("SELECT canonical FROM execution_supervisor_commits WHERE visit=?")
                .bind(visit_id)
                .fetch_one(store.pool())
                .await?;
        controller
            .finish(&store, 4020, "original owner done")
            .await?;
        drop(controller);
        store.close().await;
        let store = Store::open(dir.path(), false).await?;
        let successor = crate::execution_driver::Controller::acquire(&store, 4100).await?;
        supervise_recovery_page(&store, "g", 4101, 100).await?;
        // Negative source corruption remains visible to ordinary recovery. It
        // cannot make the historical validator depend on today's graph.
        sqlx::query("UPDATE decision_cases SET current_source='{}'")
            .execute(store.pool())
            .await?;
        let mut tx = store.pool().begin().await?;
        let receipt = validate_supervisor_commit_tx(&mut tx, visit_id)
            .await?
            .context("historical receipt")?;
        assert_eq!(receipt.visit(), visit.identity());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonical FROM execution_supervisor_commits WHERE visit=?"
            )
            .bind(visit_id)
            .fetch_one(&mut *tx)
            .await?,
            original
        );
        tx.rollback().await?;
        assert!(
            supervise_recovery_page(&store, "g", 4102, 100)
                .await
                .is_err()
        );

        // Only negative corruption fixtures bypass the immutable row trigger.
        // Every starting receipt above came from actual owner work and commit.
        for corrupt in ["digest", "start", "counts", "schema"] {
            let mut tx = store.pool().begin().await?;
            sqlx::query("DROP TRIGGER execution_supervisor_commit_no_update")
                .execute(&mut *tx)
                .await?;
            let mut body: serde_json::Value = serde_json::from_str(&original)?;
            let mut schema = SUPERVISOR_RECEIPT_SCHEMA;
            match corrupt {
                "start" => body["start_nonce"] = json!(uuid::Uuid::new_v4().to_string()),
                "counts" => body["scans"][0]["attempted"] = json!(101),
                "schema" => {
                    schema = 2;
                    body["schema"] = json!(2);
                }
                _ => {}
            }
            // Preserve the production canonical field order for semantic faults.
            let canonical =
                serde_json::to_string(&serde_json::from_value::<SupervisorReceiptBody>(body)?)?;
            let digest = if corrupt == "digest" {
                "0".repeat(64)
            } else {
                format!("{:x}", Sha256::digest(canonical.as_bytes()))
            };
            sqlx::query("UPDATE execution_supervisor_commits SET canonical=?,digest=?,schema=? WHERE visit=?")
                .bind(canonical).bind(digest).bind(schema).bind(visit_id).execute(&mut *tx).await?;
            assert!(
                validate_supervisor_commit_tx(&mut tx, visit_id)
                    .await
                    .is_err(),
                "corrupt {corrupt} must not become None"
            );
            tx.rollback().await?;
        }
        let late = successor
            .reserve_supervisor_visit_for_test(&store, 4120, 4130)
            .await?
            .context("later original visit")?;
        let mut tx = store.pool().begin().await?;
        let classified =
            supervisor_failures::reconcile_supervisor_visits_tx(&mut tx, "g", 4131, 100).await?;
        assert_eq!(classified.failed, 1);
        tx.commit().await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            supervisor_failures::begin_supervisor_visit_tx(&mut tx, &late, 4121)
                .await
                .is_err()
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_real_wrapper_counts_compound_cursor_without_claiming_sixth_category()
    -> Result<()> {
        let (_dir, store, controller) = receipt_fixture().await?;
        let sender = store.mailbox("g", "sender").await?;
        store.register("g", "another", false).await?;
        let now = crate::now()?;
        let compound_message = store
            .publish(
                &sender,
                Publish {
                    recipients: vec!["recipient".into(), "another".into()],
                    key: "compound-source".into(),
                    summary: "Two original delivery responsibilities".into(),
                    body: "Same real message".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                now - 4000,
            )
            .await?;
        // Real missing-metadata source control; no positive receipt is injected.
        sqlx::query("DELETE FROM followups WHERE message IS NOT NULL")
            .execute(store.pool())
            .await?;
        let visit = controller
            .reserve_supervisor_visit_for_test(&store, now, now + 10)
            .await?
            .context("real wrapper reservation")?;
        let id = visit.identity().id;
        let page = supervise_recovery_visit(&store, visit, 1).await?;
        assert_eq!(page.missing_plans_scanned, 1);
        let mut tx = store.pool().begin().await?;
        assert!(validate_supervisor_commit_tx(&mut tx, id).await?.is_some());
        let canonical: String =
            sqlx::query_scalar("SELECT canonical FROM execution_supervisor_commits WHERE visit=?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        let receipt: SupervisorReceiptBody = serde_json::from_str(&canonical)?;
        assert_eq!(receipt.schema, 1);
        assert_eq!(receipt.scans.len(), 5);
        assert_eq!(receipt.scans[3].attempted, 1);
        assert!(
            matches!(&receipt.after.cursors[3], SupervisorCursor::MissingDelivery { message, recipient }
            if *message > 0 && *recipient > 0)
        );
        assert!(!receipt.after.cursors[3].exhausted());
        assert!(!page.completed_scan);
        tx.rollback().await?;

        let now = crate::now()?;
        let second = controller
            .reserve_supervisor_visit_for_test(&store, now, now + 10)
            .await?
            .context("second real page")?;
        let second_id = second.identity().id;
        supervise_recovery_visit(&store, second, 1).await?;
        let second: String =
            sqlx::query_scalar("SELECT canonical FROM execution_supervisor_commits WHERE visit=?")
                .bind(second_id)
                .fetch_one(store.pool())
                .await?;
        let second: SupervisorReceiptBody = serde_json::from_str(&second)?;
        assert!(
            matches!(&second.after.cursors[3], SupervisorCursor::MissingDelivery { message, recipient }
            if *message == compound_message && *recipient > 0)
        );
        let now = crate::now()?;
        let third = controller
            .reserve_supervisor_visit_for_test(&store, now, now + 10)
            .await?
            .context("third real page")?;
        let third_id = third.identity().id;
        supervise_recovery_visit(&store, third, 1).await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            validate_supervisor_commit_tx(&mut tx, third_id)
                .await?
                .is_some()
        );
        let third: String =
            sqlx::query_scalar("SELECT canonical FROM execution_supervisor_commits WHERE visit=?")
                .bind(third_id)
                .fetch_one(&mut *tx)
                .await?;
        let third: SupervisorReceiptBody = serde_json::from_str(&third)?;
        assert_eq!(third.before.cursors[3], second.after.cursors[3]);
        assert!(third.after.cursors[3].exhausted());
        assert_eq!(
            third.scans[3].attempted, 1,
            "the second recipient of the same message was visited"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_cases")
                .fetch_one(&mut *tx)
                .await?,
            3
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn notice_validation_observes_settlement_before_supervisor_and_rolls_back() -> Result<()>
    {
        const {
            assert!(cfg!(debug_assertions));
        }
        let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
        assert_eq!(root, "/tmp/agent-mail-durable-execution/decision-recovery");
        let dir = tempfile::Builder::new()
            .prefix("notice-race-")
            .tempdir_in(root)?;
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "sender", false).await?;
        store.register("g", "recipient", false).await?;
        let sender = store.mailbox("g", "sender").await?;
        let opened = 1_700_000_000;
        let message = store
            .publish(
                &sender,
                Publish {
                    recipients: vec!["recipient".into()],
                    key: "notice-race".into(),
                    summary: "Review".into(),
                    body: "Source evidence".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                opened,
            )
            .await?;
        let case = store
            .recover_expired_obligation(
                &sender,
                "recover",
                Obligation::Delivery {
                    message,
                    recipient: "recipient".into(),
                },
                opened + 4000,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        let before = validate_operator_notice_source_tx(
            &mut tx,
            "g",
            case.operator_obligation,
            opened + 4001,
        )
        .await?;
        tx.commit().await?;
        assert!(before.observed_source.unresolved);
        assert_eq!(before.source.state, "escalated");
        // Simulate a committed source settlement while the supervisor is idle.
        // This fault fixture deliberately leaves its cached case/obligation old.
        sqlx::query("UPDATE deliveries SET state='resolved' WHERE message=?")
            .bind(message)
            .execute(store.pool())
            .await?;
        let mut tx = store.pool().begin().await?;
        let after = validate_operator_notice_source_tx(
            &mut tx,
            "g",
            case.operator_obligation,
            opened + 4002,
        )
        .await?;
        assert!(!after.observed_source.unresolved);
        assert_eq!(after.source.state, "superseded");
        assert_eq!(after.source.obligation_id, before.source.obligation_id);
        assert_eq!(after.source.source_key, before.source.source_key);
        assert_eq!(after.source.episode, before.source.episode);
        assert!(after.source.case_version > before.source.case_version);
        assert_ne!(
            serde_json::to_value(&before)?,
            serde_json::to_value(&after)?
        );
        tx.rollback().await?;
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM operator_obligations WHERE id=?")
                .bind(case.operator_obligation)
                .fetch_one(store.pool())
                .await?,
            "escalated"
        );
        let mut tx = store.pool().begin().await?;
        let committed = validate_operator_notice_source_tx(
            &mut tx,
            "g",
            case.operator_obligation,
            opened + 4002,
        )
        .await?;
        assert_eq!(
            serde_json::to_value(&after)?,
            serde_json::to_value(&committed)?
        );
        tx.commit().await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operator_obligations")
                .fetch_one(store.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT retrieved_at FROM followups WHERE message=?"
            )
            .bind(message)
            .fetch_one(store.pool())
            .await?,
            None
        );
        Ok(())
    }
}
