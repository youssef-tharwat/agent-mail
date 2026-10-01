//! One installation controller repairs contracted work independently of Mail.
//! The lock and epoch fence controller jobs, never external execution lifetimes.
use crate::{execution, runtime_adapter::ManagedRuntimeGate, store::Store};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::Serialize;
use serde_json::json;
use sqlx::{Sqlite, Transaction};
#[cfg(target_os = "linux")]
use std::collections::BTreeMap;
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    sync::Arc,
    time::Duration,
};
use tokio::time::{Instant, timeout_at};

const PAGE_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(target_os = "linux")]
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(target_os = "linux")]
const CLAIM_PAGES_PER_ROUND: usize = 11;

/// Service-owned independent jobs. Each retains the controller lock until joined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobKind {
    Repair,
    Claim,
    Reconcile,
    Supervise,
}
impl JobKind {
    pub(crate) const ALL: [Self; 4] = [Self::Repair, Self::Claim, Self::Reconcile, Self::Supervise];
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Repair => "repair",
            Self::Claim => "claim",
            Self::Reconcile => "reconcile",
            Self::Supervise => "supervise",
        }
    }
}

/// Scheduling opportunities only; neither class grants physical closure.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconciliationClass {
    Held,
    Reclamation,
}
#[cfg(any(target_os = "linux", test))]
impl ReconciliationClass {
    fn name(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Reclamation => "reclamation",
        }
    }
    fn other(self) -> Self {
        match self {
            Self::Held => Self::Reclamation,
            Self::Reclamation => Self::Held,
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "held" => Ok(Self::Held),
            "reclamation" => Ok(Self::Reclamation),
            _ => anyhow::bail!("invalid_reconciliation_class"),
        }
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug)]
struct ReconciliationVisit {
    group: String,
    preferred: ReconciliationClass,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct ReconciliationSelection {
    correlation: execution::Correlation,
    stop: bool,
    class: ReconciliationClass,
}

/// Merge one bounded job report without replacing the other independent jobs.
pub(crate) fn publish_status(
    status: &tokio::sync::watch::Sender<serde_json::Value>,
    kind: JobKind,
    report: serde_json::Value,
) {
    status.send_modify(|value| {
        if !value.is_object() {
            *value = json!({});
        }
        if let Some(jobs) = value.as_object_mut() {
            jobs.insert(kind.name().into(), report);
        }
    });
}

/// Only a current controller can mint this receipt after the actual dispatch
/// exposure reservation. Worker transport carries its ID, not a boolean proof.
#[derive(Debug)]
#[cfg(any(target_os = "linux", test))]
pub(crate) struct DispatchAuthority {
    id: String,
}

#[cfg(any(target_os = "linux", test))]
impl DispatchAuthority {
    pub(crate) fn receipt(&self) -> &str {
        &self.id
    }
}

/// Recheck immediately before physical worker exposure and inside the worker's
/// scheduler-admission transaction. The protected persisted row authenticates
/// the original correlation, epoch, exact offer and short admission interval.
/// A timeout/revocation denies new work; it never releases the old attempt slot.
#[cfg(any(target_os = "linux", test))]
pub(crate) async fn validate_dispatch_controller_tx(
    tx: &mut Transaction<'_, Sqlite>,
    correlation: &execution::Correlation,
    receipt: &str,
    now: i64,
) -> Result<()> {
    ensure!(
        !receipt.is_empty() && receipt.len() <= 128,
        "invalid_controller_receipt"
    );
    // Reserve the SQLite writer before reading the epoch; this also rejects a
    // stale WAL read transaction rather than validating an obsolete snapshot.
    sqlx::query("UPDATE execution_controller SET observed=observed WHERE id=1")
        .execute(&mut **tx)
        .await?;
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_controller_dispatches r JOIN execution_controller c ON c.id=1 AND c.generation=r.generation AND c.owner=r.owner AND c.state='running' JOIN execution_attempts a ON a.id=r.attempt JOIN execution_dispatches d ON d.attempt=a.id WHERE r.id=? AND a.id=? AND a.group_name=? AND a.task=? AND a.fence=? AND a.dispatch_key=? AND a.holds_slot=1 AND r.dispatch_revision=d.revision AND r.request=d.request AND d.phase='exposed' AND r.created<=? AND r.valid_until>?)")
        .bind(receipt).bind(&correlation.attempt).bind(&correlation.group).bind(&correlation.task)
        .bind(correlation.fence).bind(&correlation.dispatch_key).bind(now).bind(now)
        .fetch_one(&mut **tx).await?;
    ensure!(current, "execution_controller_dispatch_fenced");
    Ok(())
}

#[derive(Clone)]
pub(crate) struct Controller {
    // Every running job retains this lock. Dropping the service's handle alone
    // cannot permit a successor while an old job still holds it.
    _lock: Arc<File>,
    owner: String,
    generation: i64,
    #[cfg(target_os = "linux")]
    dispatch_operation: Arc<tokio::sync::Mutex<()>>,
    #[cfg(target_os = "linux")]
    reconcile_operation: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct PageStatus {
    pub job: Option<JobKind>,
    pub groups_attempted: usize,
    pub visits_completed: usize,
    pub pages: usize,
    pub candidates: Vec<(String, String)>,
    pub next_cursor: Option<String>,
    pub more: bool,
    pub oldest_due_at: Option<i64>,
    pub existing: usize,
    pub supervision: Option<serde_json::Value>,
    pub group: Option<String>,
    pub generation: i64,
    pub tasks: usize,
    pub runtime_reconciliation_due: usize,
    pub reclamation_checks: usize,
    /// A Ready result replays original closure; it is not another slot release
    /// or evidence of a particular physical removal observation.
    pub reclamation_ready: usize,
    pub model_event: Option<i64>,
    pub error: Option<String>,
    pub dispatches: usize,
    pub closed: usize,
    pub populated: usize,
    pub held: usize,
    pub runtime_errors: Vec<String>,
}

fn bounded_error(error: &str) -> String {
    error.chars().take(2048).collect()
}

// This boundary substitutes transport only in explicit controller controls.
// Production always supplies ManagedIo and the actual ManagedRuntimeGate.
#[cfg(target_os = "linux")]
type IoFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<execution::Checked<T>>> + Send + 'a>>;
#[cfg(target_os = "linux")]
pub(crate) trait RuntimeIo {
    fn dispatch<'a>(
        &'a self,
        store: &'a Store,
        offer: &'a execution::DispatchOffer,
        authority: &'a DispatchAuthority,
    ) -> IoFuture<'a, crate::managed_runtime::ManagedDispatch>;
    fn reconcile<'a>(
        &'a self,
        store: &'a Store,
        correlation: &'a execution::Correlation,
        stop: bool,
        now: i64,
    ) -> IoFuture<'a, crate::managed_runtime::ManagedReconciliation>;
}
#[cfg(target_os = "linux")]
struct ManagedIo;
#[cfg(target_os = "linux")]
impl RuntimeIo for ManagedIo {
    fn dispatch<'a>(
        &'a self,
        store: &'a Store,
        offer: &'a execution::DispatchOffer,
        authority: &'a DispatchAuthority,
    ) -> IoFuture<'a, crate::managed_runtime::ManagedDispatch> {
        Box::pin(crate::managed_runtime::dispatch_managed(
            store, offer, authority,
        ))
    }
    fn reconcile<'a>(
        &'a self,
        store: &'a Store,
        correlation: &'a execution::Correlation,
        stop: bool,
        now: i64,
    ) -> IoFuture<'a, crate::managed_runtime::ManagedReconciliation> {
        Box::pin(crate::managed_runtime::reconcile_managed(
            store,
            correlation,
            stop,
            now,
        ))
    }
}

impl Controller {
    pub(crate) async fn acquire(store: &Store, now: i64) -> Result<Self> {
        timeout_at(
            Instant::now() + PAGE_TIMEOUT,
            Self::acquire_inner(store, now),
        )
        .await
        .context("controller acquisition timed out")?
    }

    async fn acquire_inner(store: &Store, now: i64) -> Result<Self> {
        ensure!(now >= 0, "invalid_controller_time");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(store.root().join("execution-controller.lock"))?;
        file.try_lock_exclusive()
            .context("execution controller already running")?;
        let owner = uuid::Uuid::new_v4().to_string();
        let mut tx = store.pool().begin().await?;
        // A successor can reach this writer only after the previous OS lock is
        // released. Record interrupted controller work without touching attempts.
        sqlx::query("UPDATE execution_controller SET generation=generation+1,owner=?,state='running',observed=?,last_error=NULL WHERE id=1 AND generation<9223372036854775807")
            .bind(&owner).bind(now).execute(&mut *tx).await?;
        let (generation, current): (i64, String) =
            sqlx::query_as("SELECT generation,owner FROM execution_controller WHERE id=1")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(current == owner, "controller_generation_exhausted");
        sqlx::query(
            "UPDATE execution_controller_runs SET finished=?,outcome=? WHERE finished IS NULL",
        )
        .bind(now)
        .bind(
            json!({"state":"interrupted","successor":generation,"external_closure":false})
                .to_string(),
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO execution_controller_runs(generation,owner,started) VALUES(?,?,?)",
        )
        .bind(generation)
        .bind(&owner)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Self {
            _lock: Arc::new(file),
            owner,
            generation,
            #[cfg(target_os = "linux")]
            dispatch_operation: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(target_os = "linux")]
            reconcile_operation: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Reserve the writer and reject obsolete controller jobs in that same tx.
    async fn fence_tx(&self, tx: &mut Transaction<'_, Sqlite>, now: i64) -> Result<()> {
        let changed = sqlx::query("UPDATE execution_controller SET observed=max(observed,?) WHERE id=1 AND owner=? AND generation=? AND state='running'")
            .bind(now).bind(&self.owner).bind(self.generation).execute(&mut **tx).await?;
        ensure!(
            changed.rows_affected() == 1,
            "execution_controller_superseded"
        );
        Ok(())
    }

    #[cfg(any(target_os = "linux", test))]
    pub(crate) async fn authorize_dispatch_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        offer: &execution::DispatchOffer,
        now: i64,
    ) -> Result<DispatchAuthority> {
        self.fence_tx(tx, now).await?;
        let actual: Option<(String,i64)> = sqlx::query_as("SELECT d.request,d.revision FROM execution_dispatches d JOIN execution_attempts a ON a.id=d.attempt WHERE a.id=? AND a.group_name=? AND a.task=? AND a.fence=? AND a.dispatch_key=? AND a.holds_slot=1 AND d.phase='exposed'")
            .bind(&offer.correlation.attempt).bind(&offer.correlation.group).bind(&offer.correlation.task)
            .bind(offer.correlation.fence).bind(&offer.correlation.dispatch_key).fetch_optional(&mut **tx).await?;
        ensure!(
            actual == Some((offer.request.clone(), offer.revision)),
            "controller_offer_not_current"
        );
        let id = uuid::Uuid::new_v4().to_string();
        let valid_until = now
            .checked_add(10)
            .context("controller_receipt_clock_overflow")?;
        sqlx::query("INSERT INTO execution_controller_dispatches(id,generation,owner,attempt,dispatch_revision,request,created,valid_until) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(attempt,dispatch_revision) DO NOTHING")
            .bind(&id).bind(self.generation).bind(&self.owner).bind(&offer.correlation.attempt)
            .bind(offer.revision).bind(&offer.request).bind(now).bind(valid_until).execute(&mut **tx).await?;
        let id: String = sqlx::query_scalar("SELECT id FROM execution_controller_dispatches WHERE attempt=? AND dispatch_revision=?")
            .bind(&offer.correlation.attempt).bind(offer.revision).fetch_one(&mut **tx).await?;
        validate_dispatch_controller_tx(tx, &offer.correlation, &id, now).await?;
        Ok(DispatchAuthority { id })
    }

    /// Commit an attempted visit before owner work. Failed pages do not pin the
    /// installation to one group, and this cursor never claims page completion.
    async fn reserve_group_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        kind: JobKind,
        excluded: &BTreeSet<String>,
        now: i64,
    ) -> Result<Option<String>> {
        self.fence_tx(tx, now).await?;
        let after: String =
            sqlx::query_scalar("SELECT last_group FROM execution_driver_cursors WHERE job=?")
                .bind(kind.name())
                .fetch_one(&mut **tx)
                .await?;
        let group: Option<String> = sqlx::query_scalar("SELECT g.name FROM groups g JOIN node n ON n.id=g.home_machine WHERE (? IN ('supervise','reconcile') OR EXISTS(SELECT 1 FROM task_models m WHERE m.group_name=g.name)) AND g.name NOT IN (SELECT value FROM json_each(?)) ORDER BY CASE WHEN g.name>? THEN 0 ELSE 1 END,g.name LIMIT 1")
            .bind(kind.name()).bind(serde_json::to_string(excluded)?).bind(after).fetch_optional(&mut **tx).await?;
        if let Some(group) = &group {
            let updated=sqlx::query("UPDATE execution_driver_cursors SET last_group=?,attempted_at=?,attempts=attempts+1 WHERE job=? AND attempts<9223372036854775807")
                .bind(group).bind(now).bind(kind.name()).execute(&mut **tx).await?;
            ensure!(
                updated.rows_affected() == 1,
                "controller_attempt_counter_exhausted"
            );
        }
        Ok(group)
    }

    async fn reserve_group(
        &self,
        store: &Store,
        kind: JobKind,
        excluded: &BTreeSet<String>,
        now: i64,
    ) -> Result<Option<String>> {
        let mut tx = store.pool().begin().await?;
        let group = self.reserve_group_tx(&mut tx, kind, excluded, now).await?;
        tx.commit().await?;
        Ok(group)
    }

    #[cfg(any(target_os = "linux", test))]
    async fn reserve_reconciliation_visit_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        excluded: &BTreeSet<String>,
        now: i64,
    ) -> Result<Option<ReconciliationVisit>> {
        let Some(group) = self
            .reserve_group_tx(tx, JobKind::Reconcile, excluded, now)
            .await?
        else {
            return Ok(None);
        };
        sqlx::query("INSERT INTO execution_reconcile_groups(group_name) VALUES(?) ON CONFLICT(group_name) DO NOTHING")
            .bind(&group).execute(&mut **tx).await?;
        let saved: String = sqlx::query_scalar(
            "SELECT next_class FROM execution_reconcile_groups WHERE group_name=?",
        )
        .bind(&group)
        .fetch_one(&mut **tx)
        .await?;
        let preferred = ReconciliationClass::parse(&saved)?;
        let changed=sqlx::query("UPDATE execution_reconcile_groups SET next_class=? WHERE group_name=? AND next_class=?")
            .bind(preferred.other().name()).bind(&group).bind(&saved).execute(&mut **tx).await?;
        ensure!(
            changed.rows_affected() == 1,
            "reconciliation_class_conflict"
        );
        Ok(Some(ReconciliationVisit { group, preferred }))
    }

    /// Persist preference with the real group visit, independently of any owner
    /// selector rollback, failed diagnostic, cancelled I/O or controller restart.
    #[cfg(any(target_os = "linux", test))]
    async fn reserve_reconciliation_visit(
        &self,
        store: &Store,
        excluded: &BTreeSet<String>,
        now: i64,
    ) -> Result<Option<ReconciliationVisit>> {
        let mut tx = store.pool().begin().await?;
        let visit = self
            .reserve_reconciliation_visit_tx(&mut tx, excluded, now)
            .await?;
        tx.commit().await?;
        Ok(visit)
    }

    #[cfg(target_os = "linux")]
    async fn select_reconciliation_class_tx(
        tx: &mut Transaction<'_, Sqlite>,
        group: &str,
        class: ReconciliationClass,
        now: i64,
    ) -> Result<Option<ReconciliationSelection>> {
        match class {
            ReconciliationClass::Held => {
                let mut candidates =
                    execution::reserve_driver_reconciliation_tx(tx, group, now, 1).await?;
                ensure!(candidates.len() <= 1, "reconciliation_selection_limit");
                Ok(candidates
                    .pop()
                    .map(|(correlation, stop)| ReconciliationSelection {
                        correlation,
                        stop,
                        class,
                    }))
            }
            ReconciliationClass::Reclamation => {
                let after: String = sqlx::query_scalar(
                    "SELECT reclamation_after FROM execution_reconcile_groups WHERE group_name=?",
                )
                .bind(group)
                .fetch_one(&mut **tx)
                .await?;
                ensure!(after.len() <= 128, "invalid_reclamation_cursor");
                let cursor = if after.is_empty() {
                    None
                } else {
                    Some(after.as_str())
                };
                let Some(reserved) =
                    crate::runtime_capture::reserve_reclamation_after_tx(tx, group, cursor, now)
                        .await?
                else {
                    return Ok(None);
                };
                let id = reserved.original_attempt_id();
                ensure!(
                    !id.is_empty() && id.len() <= 128 && reserved.correlation().group == group,
                    "invalid_reclamation_selection"
                );
                let changed=sqlx::query("UPDATE execution_reconcile_groups SET reclamation_after=? WHERE group_name=? AND reclamation_after=?")
                    .bind(id).bind(group).bind(&after).execute(&mut **tx).await?;
                ensure!(changed.rows_affected() == 1, "reclamation_cursor_conflict");
                Ok(Some(ReconciliationSelection {
                    correlation: reserved.into_correlation(),
                    stop: false,
                    class,
                }))
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn reconciliation_page(
        &self,
        store: &Store,
        visit: &ReconciliationVisit,
    ) -> Result<(Option<ReconciliationSelection>, Option<i64>)> {
        let mut tx = store.pool().begin().await?;
        self.fence_tx(&mut tx, crate::now()?).await?;
        crate::decision_recovery::reserve_home_tx(&mut tx, &visit.group).await?;
        let now = crate::now()?;
        let selected =
            match Self::select_reconciliation_class_tx(&mut tx, &visit.group, visit.preferred, now)
                .await?
            {
                Some(selected) => Some(selected),
                // Only authentic emptiness permits one bounded alternative. Error
                // propagation and the caller's original timeout prevent fallback.
                None => {
                    Self::select_reconciliation_class_tx(
                        &mut tx,
                        &visit.group,
                        visit.preferred.other(),
                        now,
                    )
                    .await?
                }
            };
        let oldest = Self::oldest_due_tx(&mut tx, &visit.group, JobKind::Reconcile).await?;
        self.completed_tx(&mut tx, JobKind::Reconcile, now).await?;
        tx.commit().await?;
        Ok((selected, oldest))
    }

    async fn reserve_supervisor_visit(
        &self,
        store: &Store,
        opened: i64,
        deadline: i64,
    ) -> Result<Option<crate::supervisor_failures::SupervisorVisit>> {
        let mut tx = store.pool().begin().await?;
        let group = self
            .reserve_group_tx(&mut tx, JobKind::Supervise, &BTreeSet::new(), opened)
            .await?;
        let visit = match group {
            Some(group) => Some(
                crate::supervisor_failures::reserve_supervisor_visit_tx(
                    &mut tx,
                    &group,
                    self.generation,
                    &self.owner,
                    opened,
                    deadline,
                )
                .await?,
            ),
            None => None,
        };
        tx.commit().await?;
        Ok(visit)
    }

    /// Owner controls use the genuine committed reservation, never a token fixture.
    #[cfg(test)]
    pub(crate) async fn reserve_supervisor_visit_for_test(
        &self,
        store: &Store,
        opened: i64,
        deadline: i64,
    ) -> Result<Option<crate::supervisor_failures::SupervisorVisit>> {
        self.reserve_supervisor_visit(store, opened, deadline).await
    }

    async fn completed_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        kind: JobKind,
        now: i64,
    ) -> Result<()> {
        self.fence_tx(tx, now).await?;
        let updated = sqlx::query("UPDATE execution_driver_cursors SET completed_at=?,completed=completed+1,last_error=NULL WHERE job=? AND completed<9223372036854775807")
            .bind(now).bind(kind.name()).execute(&mut **tx).await?;
        ensure!(
            updated.rows_affected() == 1,
            "controller_completion_counter_exhausted"
        );
        Ok(())
    }

    async fn failed(&self, store: &Store, kind: JobKind, error: &str, now: i64) -> Result<()> {
        let mut tx = store.pool().begin().await?;
        self.fence_tx(&mut tx, now).await?;
        sqlx::query("UPDATE execution_driver_cursors SET last_error=? WHERE job=?")
            .bind(bounded_error(error))
            .bind(kind.name())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    fn status(&self, kind: JobKind) -> PageStatus {
        PageStatus {
            job: Some(kind),
            generation: self.generation,
            ..Default::default()
        }
    }

    /// One deadline covers selection, owner work, and best-effort error writing.
    async fn database_failure(
        &self,
        store: &Store,
        kind: JobKind,
        error: String,
        deadline: Instant,
        status: &mut PageStatus,
    ) {
        status.error = Some(bounded_error(&error));
        status.more = true;
        if Instant::now() < deadline {
            match crate::now() {
                Ok(now) => {
                    let _ = timeout_at(deadline, self.failed(store, kind, &error, now)).await;
                }
                Err(clock) => status
                    .runtime_errors
                    .push(bounded_error(&format!("failure clock: {clock:#}"))),
            }
        }
        // No unbounded timeout-reporting transaction after the deadline.
    }

    async fn oldest_due_tx(
        tx: &mut Transaction<'_, Sqlite>,
        group: &str,
        kind: JobKind,
    ) -> Result<Option<i64>> {
        if kind == JobKind::Reconcile {
            Ok(sqlx::query_scalar("SELECT min(reconcile_at) FROM execution_attempts WHERE group_name=? AND holds_slot=1")
                .bind(group).fetch_one(&mut **tx).await?)
        } else {
            Ok(sqlx::query_scalar("SELECT min(e.due_at) FROM execution_tasks e JOIN work_items w ON w.group_name=e.group_name AND w.id=e.task WHERE e.group_name=? AND w.state IN ('open','ready','active')")
                .bind(group).fetch_one(&mut **tx).await?)
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) async fn oldest_claim_due_for_test(
        tx: &mut Transaction<'_, Sqlite>,
        group: &str,
    ) -> Result<Option<i64>> {
        Self::oldest_due_tx(tx, group, JobKind::Claim).await
    }

    /// Repair remains callable as one bounded page; runtime I/O lives elsewhere.
    #[cfg(test)]
    pub(crate) async fn tick(&self, store: &Store, now: i64) -> Result<PageStatus> {
        self.repair(store, Some(now)).await
    }

    async fn repair(&self, store: &Store, fixed_time: Option<i64>) -> Result<PageStatus> {
        let deadline = Instant::now() + PAGE_TIMEOUT;
        let mut status = self.status(JobKind::Repair);
        let result: Result<()> = match timeout_at(deadline, async {
            let now = match fixed_time {
                Some(now) => now,
                None => crate::now()?,
            };
            let Some(group) = self
                .reserve_group(store, JobKind::Repair, &BTreeSet::new(), now)
                .await?
            else {
                return Ok(());
            };
            status.group = Some(group.clone());
            status.groups_attempted += 1;
            let mut tx = store.pool().begin().await?;
            self.fence_tx(&mut tx, now).await?;
            // Production samples after obtaining the writer; waiting for a
            // sibling transaction must not manufacture a backwards clock.
            let now = match fixed_time {
                Some(now) => now,
                None => crate::now()?,
            };
            let page =
                execution::reconcile_page_tx(&mut tx, &ManagedRuntimeGate, &group, now).await?;
            #[cfg(not(target_os = "linux"))]
            execution::hold_missing_driver_tx(&mut tx, &group, &page.tasks, now).await?;
            #[cfg(target_os = "linux")]
            execution::clear_missing_driver_tx(&mut tx, &group, &page.tasks, now).await?;
            status.oldest_due_at = Self::oldest_due_tx(&mut tx, &group, JobKind::Repair).await?;
            self.completed_tx(&mut tx, JobKind::Repair, now).await?;
            tx.commit().await?;
            status.tasks = page.tasks.len();
            status.runtime_reconciliation_due = page.reconcile.len();
            status.model_event = Some(page.model_event);
            status.pages = 1;
            status.visits_completed = 1;
            Ok(())
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "repair database turn timed out; persisted owner cursors determine committed work"
            )),
        };
        if let Err(error) = result {
            self.database_failure(
                store,
                JobKind::Repair,
                format!("{error:#}"),
                deadline,
                &mut status,
            )
            .await;
        }
        Ok(status)
    }

    async fn supervise(&self, store: &Store) -> Result<PageStatus> {
        let opened = crate::now()?;
        let deadline = Instant::now() + PAGE_TIMEOUT;
        let absolute_deadline = opened
            .checked_add(10)
            .context("supervisor_visit_clock_overflow")?;
        let mut status = self.status(JobKind::Supervise);
        let result: Result<()> = match timeout_at(deadline, async {
            // Anchor the persisted interval to this original round. Selection and
            // writer waiting consume it; Recovery rechecks before start/receipt.
            let Some(visit) = self
                .reserve_supervisor_visit(store, opened, absolute_deadline)
                .await?
            else {
                return Ok(());
            };
            status.group = Some(visit.identity().group.clone());
            status.groups_attempted = 1;
            // Recovery owns actual categories, effects and the unique visit receipt
            // in its existing commit. Missing migrated state remains corruption.
            let page =
                crate::decision_supervisor::supervise_recovery_visit(store, visit, 100).await?;
            // Owner completion is real even if this diagnostic write later fails.
            status.supervision = Some(serde_json::to_value(page)?);
            status.pages = 1;
            status.visits_completed = 1;
            let mut tx = store.pool().begin().await?;
            self.completed_tx(&mut tx, JobKind::Supervise, crate::now()?)
                .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "supervision database turn timed out; inspect actual owner heartbeat for any committed page"
            )),
        };
        if let Err(error) = result {
            self.database_failure(
                store,
                JobKind::Supervise,
                format!("{error:#}"),
                deadline,
                &mut status,
            )
            .await;
        }
        Ok(status)
    }

    #[cfg(target_os = "linux")]
    async fn runtime_failure(
        &self,
        store: &Store,
        kind: JobKind,
        c: &execution::Correlation,
        error: &str,
        status: &mut PageStatus,
    ) {
        let error = bounded_error(error);
        status.runtime_errors.push(error.clone());
        // A separately bounded outcome turn; a failed write never hides durable
        // original exposure/slot responsibility from subsequent reconciliation.
        let result = timeout_at(Instant::now() + PAGE_TIMEOUT, async {
            let mut tx = store.pool().begin().await?;
            let now = crate::now()?;
            self.fence_tx(&mut tx, now).await?;
            execution::driver_failure_tx(&mut tx, c, &error, now).await?;
            sqlx::query("UPDATE execution_driver_cursors SET last_error=? WHERE job=?")
                .bind(&error)
                .bind(kind.name())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            status.runtime_errors.push(
                "runtime failure persistence unavailable; original ledger remains authoritative"
                    .into(),
            );
        }
    }

    #[cfg(target_os = "linux")]
    async fn defer_existing(&self, store: &Store, c: &execution::Correlation) -> Result<()> {
        let mut tx = store.pool().begin().await?;
        let now = crate::now()?;
        self.fence_tx(&mut tx, now).await?;
        execution::defer_existing_dispatch_tx(&mut tx, c, now).await?;
        tx.commit().await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn claim_page<R: execution::RuntimeGate>(
        &self,
        store: &Store,
        group: &str,
        runtime: &R,
        seen: &BTreeSet<String>,
    ) -> Result<(execution::ClaimPage, Option<DispatchAuthority>, Option<i64>)> {
        let mut tx = store.pool().begin().await?;
        let now = crate::now()?;
        self.fence_tx(&mut tx, now).await?;
        let now = crate::now()?;
        let page = execution::next_driver_claim_tx(&mut tx, runtime, group, &self.owner, now, seen)
            .await?;
        let authority = match &page.offer {
            Some(offer) => Some(self.authorize_dispatch_tx(&mut tx, offer, now).await?),
            None => None,
        };
        let oldest = Self::oldest_due_tx(&mut tx, group, JobKind::Claim).await?;
        self.completed_tx(&mut tx, JobKind::Claim, now).await?;
        // Held causes and page cursor commit without any fabricated admission.
        tx.commit().await?;
        Ok((page, authority, oldest))
    }

    /// Eleven pages are shared across the whole round, rotating groups between
    /// pages. No offer is minted until this job owns dispatch capacity.
    #[cfg(target_os = "linux")]
    pub(crate) async fn claim_round_with<R: execution::RuntimeGate, I: RuntimeIo>(
        &self,
        store: &Store,
        runtime: &R,
        io: &I,
    ) -> Result<PageStatus> {
        use crate::{execution::Checked, managed_runtime::ManagedDispatch};
        let mut status = self.status(JobKind::Claim);
        let Ok(_operation) = self.dispatch_operation.try_lock() else {
            status.more = true;
            return Ok(status);
        };
        let deadline = Instant::now() + PAGE_TIMEOUT;
        let mut seen: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut completed = BTreeSet::new();
        let mut offers = 0;
        for _ in 0..CLAIM_PAGES_PER_ROUND {
            if Instant::now() >= deadline || offers >= 2 {
                status.more = true;
                break;
            }
            let selected = timeout_at(
                deadline,
                self.reserve_group(store, JobKind::Claim, &completed, crate::now()?),
            )
            .await;
            let group = match selected {
                Ok(Ok(Some(group))) => group,
                Ok(Ok(None)) => break,
                Ok(Err(error)) => {
                    self.database_failure(
                        store,
                        JobKind::Claim,
                        format!("{error:#}"),
                        deadline,
                        &mut status,
                    )
                    .await;
                    break;
                }
                Err(_) => {
                    self.database_failure(
                        store,
                        JobKind::Claim,
                        "claim group reservation timed out".into(),
                        deadline,
                        &mut status,
                    )
                    .await;
                    break;
                }
            };
            status.group = Some(group.clone());
            status.groups_attempted += 1;
            let selected = timeout_at(
                deadline,
                self.claim_page(
                    store,
                    &group,
                    runtime,
                    seen.entry(group.clone()).or_default(),
                ),
            )
            .await;
            let (page, authority, oldest) = match selected {
                Ok(Ok(page)) => page,
                failure => {
                    let error=match failure {Ok(Err(error))=>format!("claim page: {error:#}"),Err(_)=>"claim page timed out; original exposure, if committed, remains discoverable".into(),Ok(Ok(_))=>unreachable!()};
                    self.database_failure(store, JobKind::Claim, error, deadline, &mut status)
                        .await;
                    completed.insert(group);
                    continue;
                }
            };
            status.pages += 1;
            status.visits_completed += 1;
            status.oldest_due_at = match (status.oldest_due_at, oldest) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            status.next_cursor = Some(page.next_cursor);
            for task in page.examined {
                seen.entry(group.clone()).or_default().insert(task.clone());
                status.candidates.push((group.clone(), task));
            }
            if !page.has_more {
                completed.insert(group.clone());
            }
            let Some(offer) = page.offer else {
                continue;
            };
            let authority = authority.context("controller_offer_authority_missing")?;
            offers += 1;
            let result = timeout_at(
                Instant::now() + RUNTIME_TIMEOUT,
                io.dispatch(store, &offer, &authority),
            )
            .await;
            match result {
                Ok(Ok(Checked::Ready(ManagedDispatch::WorkerExposed))) => status.dispatches += 1,
                Ok(Ok(Checked::Ready(ManagedDispatch::Existing))) => {
                    status.existing += 1;
                    // Existing proves neither process identity nor absence. A
                    // later independent job rechecks stop and original closure.
                    let marked = timeout_at(
                        Instant::now() + PAGE_TIMEOUT,
                        self.defer_existing(store, &offer.correlation),
                    )
                    .await;
                    if !matches!(marked, Ok(Ok(()))) {
                        status.runtime_errors.push(
                            "Existing due marker unavailable; original attempt remains durable"
                                .into(),
                        );
                    }
                }
                Ok(Ok(Checked::Held(_))) => status.held += 1,
                Ok(Err(error)) => {
                    self.runtime_failure(
                        store,
                        JobKind::Claim,
                        &offer.correlation,
                        &format!("managed dispatch: {error:#}"),
                        &mut status,
                    )
                    .await
                }
                Err(_) => {
                    self.runtime_failure(
                        store,
                        JobKind::Claim,
                        &offer.correlation,
                        "managed dispatch timed out; original exposure/slot retained",
                        &mut status,
                    )
                    .await
                }
            }
        }
        if status.pages == CLAIM_PAGES_PER_ROUND {
            status.more = true;
        }
        Ok(status)
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn reconcile_round_with<I: RuntimeIo>(
        &self,
        store: &Store,
        io: &I,
    ) -> Result<PageStatus> {
        use crate::{execution::Checked, managed_runtime::ManagedReconciliation};
        let mut status = self.status(JobKind::Reconcile);
        let Ok(_operation) = self.reconcile_operation.try_lock() else {
            status.more = true;
            return Ok(status);
        };
        let deadline = Instant::now() + PAGE_TIMEOUT;
        let mut empty_groups = BTreeSet::new();
        let mut operations = 0;
        for _ in 0..CLAIM_PAGES_PER_ROUND {
            if Instant::now() >= deadline || operations >= 2 {
                status.more = true;
                break;
            }
            let selected = timeout_at(deadline, async {
                let Some(visit) = self
                    .reserve_reconciliation_visit(store, &empty_groups, crate::now()?)
                    .await?
                else {
                    return Ok(None);
                };
                status.group = Some(visit.group.clone());
                status.groups_attempted += 1;
                let (selected, oldest) = self.reconciliation_page(store, &visit).await?;
                status.oldest_due_at = match (status.oldest_due_at, oldest) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
                status.pages += 1;
                status.visits_completed += 1;
                Ok::<_, anyhow::Error>(Some((visit.group, selected)))
            })
            .await;
            let (group, attempt) = match selected {
                Ok(Ok(Some(value))) => value,
                Ok(Ok(None)) => break,
                failure => {
                    let error = match failure {
                        Ok(Err(error)) => format!("reconciliation selection: {error:#}"),
                        Err(_) => "reconciliation selection timed out".into(),
                        Ok(Ok(_)) => unreachable!(),
                    };
                    self.database_failure(store, JobKind::Reconcile, error, deadline, &mut status)
                        .await;
                    break;
                }
            };
            let Some(ReconciliationSelection {
                correlation,
                stop,
                class,
            }) = attempt
            else {
                empty_groups.insert(group);
                continue;
            };
            operations += 1;
            match class {
                ReconciliationClass::Held => status.runtime_reconciliation_due += 1,
                ReconciliationClass::Reclamation => status.reclamation_checks += 1,
            }
            let result = timeout_at(
                Instant::now() + RUNTIME_TIMEOUT,
                io.reconcile(store, &correlation, stop, crate::now()?),
            )
            .await;
            match result {
                Ok(Ok(Checked::Ready(ManagedReconciliation::Closed { event }))) => {
                    ensure!(event > 0, "invalid_managed_closure_event");
                    match class {
                        ReconciliationClass::Held => status.closed += 1,
                        ReconciliationClass::Reclamation => status.reclamation_ready += 1,
                    }
                }
                Ok(Ok(Checked::Ready(ManagedReconciliation::Populated))) => status.populated += 1,
                Ok(Ok(Checked::Held(_))) => status.held += 1,
                Ok(Err(error)) => {
                    self.runtime_failure(
                        store,
                        JobKind::Reconcile,
                        &correlation,
                        &format!("managed reconciliation: {error:#}"),
                        &mut status,
                    )
                    .await
                }
                Err(_) => {
                    self.runtime_failure(
                        store,
                        JobKind::Reconcile,
                        &correlation,
                        "managed reconciliation timed out; original slot retained",
                        &mut status,
                    )
                    .await
                }
            }
        }
        if status.pages == CLAIM_PAGES_PER_ROUND {
            status.more = true;
        }
        Ok(status)
    }

    pub(crate) async fn tick_job(&self, store: &Store, kind: JobKind) -> Result<PageStatus> {
        match kind {
            JobKind::Repair => self.repair(store, None).await,
            JobKind::Supervise => self.supervise(store).await,
            #[cfg(target_os = "linux")]
            JobKind::Claim => {
                self.claim_round_with(store, &ManagedRuntimeGate, &ManagedIo)
                    .await
            }
            #[cfg(target_os = "linux")]
            JobKind::Reconcile => self.reconcile_round_with(store, &ManagedIo).await,
            #[cfg(not(target_os = "linux"))]
            JobKind::Claim | JobKind::Reconcile => Ok(PageStatus {
                error: Some("managed runtime driver unavailable on this host".into()),
                ..self.status(kind)
            }),
        }
    }

    /// Independent timer per job. A job error is visible and retried next tick;
    /// it cannot terminate its siblings or silently become successful accounting.
    pub(crate) async fn run(
        &self,
        store: &Store,
        kind: JobKind,
        status: tokio::sync::watch::Sender<serde_json::Value>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let value = match self.tick_job(store, kind).await {
                Ok(page) => serde_json::to_value(page)?,
                Err(error) => {
                    json!({"job":kind,"error":bounded_error(&format!("{error:#}")),"external_closure":false})
                }
            };
            publish_status(&status, kind, value);
        }
    }

    pub(crate) async fn finish(&self, store: &Store, now: i64, outcome: &str) -> Result<()> {
        timeout_at(
            Instant::now() + PAGE_TIMEOUT,
            self.finish_inner(store, now, outcome),
        )
        .await
        .context("controller finish timed out; original attempts retained")?
    }

    async fn finish_inner(&self, store: &Store, now: i64, outcome: &str) -> Result<()> {
        let mut tx = store.pool().begin().await?;
        self.fence_tx(&mut tx, now).await?;
        sqlx::query("UPDATE execution_controller_runs SET finished=?,outcome=? WHERE generation=? AND owner=? AND finished IS NULL")
            .bind(now).bind(json!({"state":bounded_error(outcome),"external_closure":false}).to_string())
            .bind(self.generation).bind(&self.owner).execute(&mut *tx).await?;
        sqlx::query("UPDATE execution_controller SET owner=NULL,state='stopped' WHERE id=1 AND owner=? AND generation=?")
            .bind(&self.owner).bind(self.generation).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // task, attempts_spent, attempts_reserved, anchor, deadline
    #[cfg(target_os = "linux")]
    type TaskBudgetRow = (String, i64, i64, Option<i64>, Option<i64>);
    // group_name followed by the same task budget columns.
    #[cfg(target_os = "linux")]
    type GroupTaskBudgetRow = (String, String, i64, i64, Option<i64>, Option<i64>);

    #[tokio::test]
    async fn reclamation_group_preference_avoids_two_and_three_group_parity_traps() -> Result<()> {
        for groups in [&["a", "b"][..], &["a", "b", "c"][..]] {
            let temp = tempfile::tempdir()?;
            let store = Store::open(temp.path(), true).await?;
            for group in groups {
                store.enroll(group, None).await?;
            }
            let model_count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_models")
                .fetch_one(store.pool())
                .await?;
            assert_eq!(
                model_count, 0,
                "historical-cleanup-only home groups need no model row"
            );
            let controller = Controller::acquire(&store, 100).await?;
            // An uncommitted group/class reservation changes neither cursor.
            let mut tx = store.pool().begin().await?;
            let abandoned = controller
                .reserve_reconciliation_visit_tx(&mut tx, &BTreeSet::new(), 100)
                .await?
                .context("abandoned visit")?;
            assert_eq!(abandoned.preferred, ReconciliationClass::Held);
            tx.rollback().await?;
            let attempts: i64 = sqlx::query_scalar(
                "SELECT attempts FROM execution_driver_cursors WHERE job='reconcile'",
            )
            .fetch_one(store.pool())
            .await?;
            assert_eq!(attempts, 0);
            let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_reconcile_groups")
                .fetch_one(store.pool())
                .await?;
            assert_eq!(rows, 0);
            for preferred in [ReconciliationClass::Held, ReconciliationClass::Reclamation] {
                for group in groups {
                    let visit = controller
                        .reserve_reconciliation_visit(&store, &BTreeSet::new(), 100)
                        .await?
                        .context("committed visit")?;
                    assert_eq!(visit.group, *group);
                    assert_eq!(visit.preferred, preferred);
                    // Rolling back the later owner diagnostic cannot undo this
                    // group's preference or its already committed group visit.
                    let mut tx = store.pool().begin().await?;
                    controller
                        .completed_tx(&mut tx, JobKind::Reconcile, 100)
                        .await?;
                    tx.rollback().await?;
                }
            }
            let counters: (i64, i64) = sqlx::query_as(
                "SELECT attempts,completed FROM execution_driver_cursors WHERE job='reconcile'",
            )
            .fetch_one(store.pool())
            .await?;
            assert_eq!(counters, ((groups.len() * 2) as i64, 0));
            controller
                .finish(&store, 100, "reclamation restart control")
                .await?;
            drop(controller);
            store.close().await;
            let store = Store::open(temp.path(), false).await?;
            let successor = Controller::acquire(&store, 99).await?;
            // Same-second reservations above and a backwards wall clock after
            // real reopen do not alter preference or manufacture future time.
            for group in groups {
                let visit = successor
                    .reserve_reconciliation_visit(&store, &BTreeSet::new(), 99)
                    .await?
                    .context("restarted visit")?;
                assert_eq!(visit.group, *group);
                assert_eq!(visit.preferred, ReconciliationClass::Held);
            }
            let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_attempts")
                .fetch_one(store.pool())
                .await?;
            assert_eq!(
                attempts, 0,
                "scheduling metadata grants no execution attempt"
            );
            successor.finish(&store, 100, "joined").await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn reclamation_migration_preserves_identity_and_requires_exact_predecessor() -> Result<()>
    {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("history-only", None).await?;
        let controller = Controller::acquire(&store, 100).await?;
        controller
            .reserve_reconciliation_visit(&store, &BTreeSet::new(), 100)
            .await?
            .context("real group visit")?;
        let original:(String,String)=sqlx::query_as("SELECT next_class,reclamation_after FROM execution_reconcile_groups WHERE group_name='history-only'").fetch_one(store.pool()).await?;
        assert_eq!(original, ("reclamation".into(), String::new()));
        for invalid in [
            "DELETE FROM execution_reconcile_groups",
            "UPDATE execution_reconcile_groups SET group_name='other'",
            "UPDATE execution_reconcile_groups SET next_class='unknown'",
            "UPDATE execution_reconcile_groups SET reclamation_after=NULL",
        ] {
            assert!(sqlx::query(invalid).execute(store.pool()).await.is_err());
        }
        assert!(
            sqlx::query("UPDATE execution_reconcile_groups SET reclamation_after=?")
                .bind("é".repeat(65))
                .execute(store.pool())
                .await
                .is_err(),
            "cursor limit is UTF-8 bytes"
        );
        let mut tx = store.pool().begin().await?;
        assert!(
            sqlx::raw_sql(include_str!("../migrations/0032_reclamation_fairness.sql"))
                .execute(&mut *tx)
                .await
                .is_err(),
            "actual current32 cannot be passed off as predecessor31"
        );
        tx.rollback().await?;
        let retained:(String,String)=sqlx::query_as("SELECT next_class,reclamation_after FROM execution_reconcile_groups WHERE group_name='history-only'").fetch_one(store.pool()).await?;
        assert_eq!(retained, original);
        controller.finish(&store, 101, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    struct RefuseIo;
    #[cfg(target_os = "linux")]
    impl RuntimeIo for RefuseIo {
        fn dispatch<'a>(
            &'a self,
            _: &'a Store,
            _: &'a execution::DispatchOffer,
            _: &'a DispatchAuthority,
        ) -> IoFuture<'a, crate::managed_runtime::ManagedDispatch> {
            panic!("empty reconciliation cannot dispatch")
        }
        fn reconcile<'a>(
            &'a self,
            _: &'a Store,
            _: &'a execution::Correlation,
            _: bool,
            _: i64,
        ) -> IoFuture<'a, crate::managed_runtime::ManagedReconciliation> {
            panic!("empty or failed selection cannot expose runtime I/O")
        }
    }

    // Capability/transport controls exercise real scheduler claims and due
    // reservations. This gate never supplies physical observation or closure,
    // writes Runtime receipts, starts a worker or qualifies a native target.
    #[cfg(target_os = "linux")]
    struct SchedulingGate;
    #[cfg(target_os = "linux")]
    impl execution::RuntimeGate for SchedulingGate {
        async fn target(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            group: &str,
            task: &str,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<Option<execution::RuntimeTarget>> {
            Ok(Some(execution::RuntimeTarget {
                identity: "scheduler-control-only".into(),
                concurrency_key: format!("scheduler-control:{group}:{task}"),
                generation: 1,
                profile: "control-only".into(),
                durable_dedupe: true,
                cost_caps: BTreeMap::new(),
            }))
        }
        async fn current(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            _: &execution::Correlation,
            _: &execution::RuntimeTarget,
            _: execution::CurrentUse,
            _: i64,
        ) -> Result<bool> {
            Ok(true)
        }
        async fn closed(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            _: &execution::Correlation,
            _: &execution::RuntimeTarget,
            _: &str,
        ) -> Result<Option<execution::ClosedRuntime>> {
            Ok(None)
        }
        async fn observation(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            _: &execution::Correlation,
            _: &execution::RuntimeTarget,
            _: &str,
        ) -> Result<Option<execution::RuntimeObservation>> {
            Ok(None)
        }
    }

    #[cfg(target_os = "linux")]
    async fn held_control_originals(
        store: &Store,
        group: &str,
        count: usize,
        now: i64,
    ) -> Result<Vec<execution::Correlation>> {
        use crate::{
            states::TaskState,
            task_graph::{
                AuthoritySource, AuthorityState, Authorization, Budget, Completion, Contract,
                Criterion, TaskCreate, TaskDraft,
            },
            work::WorkDraft,
        };
        let credential = store.register(group, "driver-writer", false).await?;
        let writer = store.authenticate(group, Some(&credential)).await?;
        store.register(group, "driver-worker", false).await?;
        let mut originals = Vec::new();
        for index in 0..count {
            let task = format!("held-{index:02}");
            store
                .task_create(
                    &writer,
                    TaskCreate {
                        key: format!("driver-create:{task}"),
                        reason: "scheduler transport control; no native proof".into(),
                        expected_parent_versions: BTreeMap::new(),
                        draft: TaskDraft {
                            work: WorkDraft {
                                id: task.clone(),
                                scope: "control artifact".into(),
                                owner: "driver-worker".into(),
                                state: TaskState::Ready,
                                next_action: "produce control artifact".into(),
                                deadline: None,
                                evidence: vec![],
                            },
                            contract: Contract {
                                deliverable: "control artifact".into(),
                                criteria: vec![Criterion {
                                    id: "artifact".into(),
                                    description: "control artifact exists".into(),
                                }],
                                allowed_scope: vec!["control artifact".into()],
                                completion: Completion::WriterAcceptance,
                                allow_delegation: true,
                                allow_input_invalidation: true,
                                budget: Budget {
                                    max_attempts: 4,
                                    max_elapsed_seconds: 600,
                                    max_cost: None,
                                },
                            },
                            authorization: Authorization {
                                state: AuthorityState::Authorized,
                                source: AuthoritySource::Direct {
                                    authority_ref: "scheduler-control-only".into(),
                                },
                                approved_scope: vec!["control artifact".into()],
                                reason: "test-only scope".into(),
                            },
                            requirements: vec![],
                            parent: None,
                        },
                    },
                    now,
                )
                .await?;
            let mut tx = store.pool().begin().await?;
            let revision: i64 = sqlx::query_scalar(
                "SELECT revision FROM execution_tasks WHERE group_name=? AND task=?",
            )
            .bind(group)
            .bind(&task)
            .fetch_one(&mut *tx)
            .await?;
            let execution::Checked::Ready(correlation) = execution::claim_attempt_tx(
                &mut tx,
                &SchedulingGate,
                &execution::ClaimRequest {
                    group: group.into(),
                    task,
                    revision,
                    key: format!("driver-claim:{index}"),
                },
                now,
            )
            .await?
            else {
                anyhow::bail!("real control claim held");
            };
            tx.commit().await?;
            originals.push(correlation);
        }
        Ok(originals)
    }

    #[cfg(target_os = "linux")]
    #[derive(Clone, Copy)]
    enum ProbeMode {
        Held,
        Error,
        Pending,
    }
    #[cfg(target_os = "linux")]
    struct ProbeIo {
        mode: ProbeMode,
        calls: std::sync::Mutex<Vec<execution::Correlation>>,
    }
    #[cfg(target_os = "linux")]
    impl ProbeIo {
        fn new(mode: ProbeMode) -> Self {
            Self {
                mode,
                calls: Default::default(),
            }
        }
    }
    #[cfg(target_os = "linux")]
    impl RuntimeIo for ProbeIo {
        fn dispatch<'a>(
            &'a self,
            _: &'a Store,
            _: &'a execution::DispatchOffer,
            _: &'a DispatchAuthority,
        ) -> IoFuture<'a, crate::managed_runtime::ManagedDispatch> {
            panic!("reconciliation cannot dispatch")
        }
        fn reconcile<'a>(
            &'a self,
            _: &'a Store,
            correlation: &'a execution::Correlation,
            _: bool,
            _: i64,
        ) -> IoFuture<'a, crate::managed_runtime::ManagedReconciliation> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("control calls")
                    .push(correlation.clone());
                match self.mode {
                    ProbeMode::Held => Ok(execution::Checked::Held(vec![
                        "transport control has no physical proof".into(),
                    ])),
                    ProbeMode::Error => anyhow::bail!("negative reconciliation transport control"),
                    ProbeMode::Pending => std::future::pending().await,
                }
            })
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reclamation_empty_fallback_shares_two_io_calls_and_preserves_held_due_charge()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let now = crate::now()?;
        let originals = held_control_originals(&store, "g", 3, now - 60).await?;
        let before:Vec<TaskBudgetRow>=sqlx::query_as("SELECT task,attempts_spent,attempts_reserved,anchor,deadline FROM execution_budgets ORDER BY task").fetch_all(store.pool()).await?;
        let controller = Controller::acquire(&store, now).await?;
        let io = ProbeIo::new(ProbeMode::Held);
        let started = crate::now()?;
        let report = controller.reconcile_round_with(&store, &io).await?;
        let finished = crate::now()?;
        assert!(report.error.is_none(), "{report:?}");
        assert_eq!(
            (
                report.pages,
                report.runtime_reconciliation_due,
                report.reclamation_checks,
                report.closed
            ),
            (2, 2, 0, 0)
        );
        let calls = io.calls.lock().expect("control calls").clone();
        assert_eq!(
            calls.len(),
            2,
            "both preferred classes share one two-I/O allowance"
        );
        assert_ne!(calls[0].attempt, calls[1].attempt);
        for call in &calls {
            assert!(originals.contains(call));
            let (held, due): (bool, i64) =
                sqlx::query_as("SELECT holds_slot,reconcile_at FROM execution_attempts WHERE id=?")
                    .bind(&call.attempt)
                    .fetch_one(store.pool())
                    .await?;
            assert!(held);
            assert!(
                (started + 30..=finished + 30).contains(&due),
                "original now+30 Held reservation changed: {due}"
            );
        }
        let after:Vec<TaskBudgetRow>=sqlx::query_as("SELECT task,attempts_spent,attempts_reserved,anchor,deadline FROM execution_budgets ORDER BY task").fetch_all(store.pool()).await?;
        assert_eq!(after, before);
        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(slots, 3);
        controller.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reclamation_failed_io_reservations_survive_real_restart() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let originals = held_control_originals(&store, "g", 3, crate::now()? - 60).await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        let failed = ProbeIo::new(ProbeMode::Error);
        let report = controller.reconcile_round_with(&store, &failed).await?;
        assert_eq!(report.runtime_errors.len(), 2, "{report:?}");
        let called = failed.calls.lock().expect("control calls").clone();
        assert_eq!(called.len(), 2);
        controller
            .finish(&store, crate::now()?, "restart after I/O errors")
            .await?;
        drop(controller);
        store.close().await;
        let store = Store::open(temp.path(), false).await?;
        let successor = Controller::acquire(&store, crate::now()?).await?;
        let retained: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_attempts WHERE holds_slot=1 AND reconcile_at>?",
        )
        .bind(crate::now()?)
        .fetch_one(store.pool())
        .await?;
        assert_eq!(
            retained, 2,
            "failed I/O cannot refund the original due reservation"
        );
        let io = ProbeIo::new(ProbeMode::Held);
        let next = successor.reconcile_round_with(&store, &io).await?;
        assert!(next.error.is_none(), "{next:?}");
        let calls = io.calls.lock().expect("control calls").clone();
        assert_eq!(calls.len(), 1);
        assert!(originals.contains(&calls[0]) && !called.contains(&calls[0]));
        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(slots, 3);
        successor.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reclamation_real_io_timeout_does_not_start_a_second_operation_after_selection_budget()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        held_control_originals(&store, "g", 3, crate::now()? - 60).await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        let io = ProbeIo::new(ProbeMode::Pending);
        let started = std::time::Instant::now();
        let report = controller.reconcile_round_with(&store, &io).await?;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= RUNTIME_TIMEOUT,
            "uses the real15-second I/O timer"
        );
        assert_eq!(
            io.calls.lock().expect("control calls").len(),
            1,
            "elapsed10-second selection budget forbids the second call"
        );
        assert_eq!((report.pages, report.runtime_reconciliation_due), (1, 1));
        assert!(
            report
                .runtime_errors
                .iter()
                .any(|error| error.contains("timed out")),
            "{report:?}"
        );
        let preference: String = sqlx::query_scalar(
            "SELECT next_class FROM execution_reconcile_groups WHERE group_name='g'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(preference, "reclamation");
        let healthy = ProbeIo::new(ProbeMode::Held);
        let next = controller.reconcile_round_with(&store, &healthy).await?;
        assert_eq!(healthy.calls.lock().expect("control calls").len(), 2);
        assert_eq!(next.closed, 0);
        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(slots, 3);
        println!(
            "{}",
            json!({"control":"reclamation_selection_budget_after_real_timeout","elapsed_ms":elapsed.as_millis(),"first":report,"next":next,"native_qualified":false})
        );
        controller.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn pending_control_original(
        store: &Store,
        group: &str,
        task: &str,
    ) -> Result<execution::Correlation> {
        let parent = std::env::var_os("AGENT_MAIL_TEST_CGROUP_ROOT")
            .context("root must provide a private original cgroup-v2 parent")?;
        // Runtime calls genuine never-exposed reconciliation and holds only the
        // final reclamation write with a negative trigger. No positive receipt
        // fixture or physical proof constructor exists in this driver control.
        crate::runtime_capture::reclamation_tests::pending_original(
            store,
            group,
            task,
            std::path::Path::new(&parent),
        )
        .await
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent; genuine Runtime closure"]
    async fn reclamation_mixed_two_and_three_groups_share_two_calls_despite_io_errors() -> Result<()>
    {
        for groups in [&["a", "b"][..], &["a", "b", "c"][..]] {
            let temp = tempfile::tempdir()?;
            let store = Store::open(&temp.path().canonicalize()?, true).await?;
            let mut reclaim = BTreeMap::new();
            let mut held = BTreeMap::new();
            for group in groups {
                let original = pending_control_original(&store, group, "closed-original").await?;
                reclaim.insert(group.to_string(), original);
                held.insert(
                    group.to_string(),
                    held_control_originals(&store, group, 3, crate::now()? - 60).await?,
                );
            }
            let before:Vec<GroupTaskBudgetRow>=sqlx::query_as("SELECT group_name,task,attempts_spent,attempts_reserved,anchor,deadline FROM execution_budgets ORDER BY group_name,task").fetch_all(store.pool()).await?;
            let controller = Controller::acquire(&store, crate::now()?).await?;
            let io = ProbeIo::new(ProbeMode::Error);
            let started = std::time::Instant::now();
            let mut traces = Vec::new();
            // Exactly2G calls visit each group twice. Persisted per-group
            // preference must reach each inventory even with every I/O failing.
            for _ in 0..groups.len() {
                let previous = io.calls.lock().expect("control calls").len();
                let report = controller.reconcile_round_with(&store, &io).await?;
                assert!(report.error.is_none(), "{report:?}");
                assert_eq!(io.calls.lock().expect("control calls").len() - previous, 2);
                assert_eq!(
                    report.runtime_reconciliation_due + report.reclamation_checks,
                    2
                );
                assert_eq!((report.closed, report.reclamation_ready), (0, 0));
                assert!(report.pages <= 11);
                traces.push(serde_json::to_value(report)?);
            }
            let calls = io.calls.lock().expect("control calls").clone();
            for group in groups {
                let visited: Vec<_> = calls.iter().filter(|c| c.group == *group).collect();
                assert_eq!(visited.len(), 2);
                assert_eq!(
                    calls
                        .iter()
                        .filter(|c| c.group == *group && c.attempt == reclaim[*group].attempt)
                        .count(),
                    1
                );
                assert_eq!(
                    calls
                        .iter()
                        .filter(|c| c.group == *group
                            && held[*group]
                                .iter()
                                .any(|original| original.attempt == c.attempt))
                        .count(),
                    1
                );
            }
            let after:Vec<GroupTaskBudgetRow>=sqlx::query_as("SELECT group_name,task,attempts_spent,attempts_reserved,anchor,deadline FROM execution_budgets ORDER BY group_name,task").fetch_all(store.pool()).await?;
            assert_eq!(after, before);
            let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots")
                .fetch_one(store.pool())
                .await?;
            assert_eq!(slots, (groups.len() * 3) as i64);
            println!(
                "{}",
                json!({"control":"genuine_mixed_reclamation_groups","groups":groups.len(),"elapsed_ms":started.elapsed().as_millis(),"traces":traces,"native_qualified":false})
            );
            controller.finish(&store, crate::now()?, "joined").await?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent; genuine Runtime closure"]
    async fn reclamation_genuine_pending_class_gets_next_round_after_real_held_io_timeout()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(&temp.path().canonicalize()?, true).await?;
        let pending = pending_control_original(&store, "g", "closed-original").await?;
        let held = held_control_originals(&store, "g", 3, crate::now()? - 60).await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        let slow = ProbeIo::new(ProbeMode::Pending);
        let started = std::time::Instant::now();
        let first = controller.reconcile_round_with(&store, &slow).await?;
        let elapsed = started.elapsed();
        assert!(elapsed >= RUNTIME_TIMEOUT);
        let calls = slow.calls.lock().expect("control calls").clone();
        assert_eq!(calls.len(), 1);
        assert!(held.contains(&calls[0]));
        assert_eq!(
            (first.runtime_reconciliation_due, first.reclamation_checks),
            (1, 0)
        );
        assert!(
            first
                .runtime_errors
                .iter()
                .any(|error| error.contains("timed out")),
            "{first:?}"
        );
        let next = ProbeIo::new(ProbeMode::Held);
        let second = controller.reconcile_round_with(&store, &next).await?;
        assert!(second.error.is_none(), "{second:?}");
        let calls = next.calls.lock().expect("control calls").clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0], pending,
            "genuine pending cleanup has the next preferred opportunity"
        );
        assert!(held.contains(&calls[1]));
        assert_eq!(
            (second.runtime_reconciliation_due, second.reclamation_checks),
            (1, 1)
        );
        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(slots, 3);
        println!(
            "{}",
            json!({"control":"genuine_mixed_reclamation_after_timeout","elapsed_ms":elapsed.as_millis(),"first":first,"next":second,"native_qualified":false})
        );
        controller.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent; genuine Runtime closure"]
    async fn reclamation_genuine_cursor_rolls_back_atomically_and_survives_restart_and_backward_time()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(&temp.path().canonicalize()?, true).await?;
        let mut originals = Vec::new();
        for task in ["one", "two", "three"] {
            originals.push(pending_control_original(&store, "g", task).await?);
        }
        originals.sort_by(|a, b| a.attempt.cmp(&b.attempt));
        let controller = Controller::acquire(&store, 500).await?;
        controller
            .reserve_reconciliation_visit(&store, &BTreeSet::new(), 500)
            .await?
            .context("real class reservation")?;
        let mut tx = store.pool().begin().await?;
        controller.fence_tx(&mut tx, 500).await?;
        crate::decision_recovery::reserve_home_tx(&mut tx, "g").await?;
        let abandoned = Controller::select_reconciliation_class_tx(
            &mut tx,
            "g",
            ReconciliationClass::Reclamation,
            500,
        )
        .await?
        .context("actual original selection")?;
        assert_eq!(abandoned.correlation, originals[0]);
        controller
            .completed_tx(&mut tx, JobKind::Reconcile, 500)
            .await?;
        tx.rollback().await?;
        let cursor: String = sqlx::query_scalar(
            "SELECT reclamation_after FROM execution_reconcile_groups WHERE group_name='g'",
        )
        .fetch_one(store.pool())
        .await?;
        assert!(cursor.is_empty());
        for expected in &originals[..2] {
            let mut tx = store.pool().begin().await?;
            controller.fence_tx(&mut tx, 500).await?;
            crate::decision_recovery::reserve_home_tx(&mut tx, "g").await?;
            let selected = Controller::select_reconciliation_class_tx(
                &mut tx,
                "g",
                ReconciliationClass::Reclamation,
                500,
            )
            .await?
            .context("genuine pending selection")?;
            assert_eq!(&selected.correlation, expected);
            assert!(!selected.stop);
            controller
                .completed_tx(&mut tx, JobKind::Reconcile, 500)
                .await?;
            tx.commit().await?;
        }
        // Simulate crash after committed reservation and before any I/O. A real
        // reopen retains both the Runtime marker and Scheduler's original ID.
        controller.finish(&store, 500, "restart before I/O").await?;
        drop(controller);
        store.close().await;
        let store = Store::open(&temp.path().canonicalize()?, false).await?;
        let controller = Controller::acquire(&store, 499).await?;
        for expected in [&originals[2], &originals[0]] {
            let mut tx = store.pool().begin().await?;
            controller.fence_tx(&mut tx, 400).await?;
            crate::decision_recovery::reserve_home_tx(&mut tx, "g").await?;
            let selected = Controller::select_reconciliation_class_tx(
                &mut tx,
                "g",
                ReconciliationClass::Reclamation,
                400,
            )
            .await?
            .context("genuine seek/wrap after restart")?;
            assert_eq!(&selected.correlation, expected);
            controller
                .completed_tx(&mut tx, JobKind::Reconcile, 400)
                .await?;
            tx.commit().await?;
        }
        let cursor: String = sqlx::query_scalar(
            "SELECT reclamation_after FROM execution_reconcile_groups WHERE group_name='g'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(cursor, originals[0].attempt);
        controller.finish(&store, 500, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent; genuine Runtime closure"]
    async fn reclamation_genuine_selector_error_cannot_fall_back_to_due_held_inventory()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(&temp.path().canonicalize()?, true).await?;
        let original = pending_control_original(&store, "g", "closed-original").await?;
        let held = held_control_originals(&store, "g", 3, crate::now()? - 60).await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        controller
            .reserve_reconciliation_visit(&store, &BTreeSet::new(), crate::now()?)
            .await?
            .context("reserve initial Held opportunity")?;
        sqlx::query("ALTER TABLE runtime_capture_intents RENAME TO unavailable_capture_intents")
            .execute(store.pool())
            .await?;
        let failed = controller.reconcile_round_with(&store, &RefuseIo).await?;
        assert!(
            failed
                .error
                .as_deref()
                .is_some_and(|error| error.contains("runtime_capture_intents")),
            "{failed:?}"
        );
        assert_eq!(
            (
                failed.pages,
                failed.runtime_reconciliation_due,
                failed.reclamation_checks
            ),
            (0, 0, 0)
        );
        sqlx::query("ALTER TABLE unavailable_capture_intents RENAME TO runtime_capture_intents")
            .execute(store.pool())
            .await?;
        let next = ProbeIo::new(ProbeMode::Held);
        let healthy = controller.reconcile_round_with(&store, &next).await?;
        assert!(healthy.error.is_none(), "{healthy:?}");
        assert_eq!(
            (
                healthy.runtime_reconciliation_due,
                healthy.reclamation_checks
            ),
            (1, 1)
        );
        let calls = next.calls.lock().expect("control calls").clone();
        assert_eq!(calls.len(), 2);
        assert!(held.contains(&calls[0]));
        assert_eq!(calls[1], original);
        controller.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent; genuine Runtime closure"]
    async fn reclamation_genuine_closed_replay_records_cleanup_without_second_closure_or_charge()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(&temp.path().canonicalize()?, true).await?;
        pending_control_original(&store, "g", "closed-original").await?;
        crate::runtime_capture::reclamation_tests::release_reclamation_fault(&store).await?;
        let events: Vec<(i64, String, String, i64)> =
            sqlx::query_as("SELECT id,kind,payload,created FROM execution_events ORDER BY id")
                .fetch_all(store.pool())
                .await?;
        let budgets:Vec<TaskBudgetRow>=sqlx::query_as("SELECT task,attempts_spent,attempts_reserved,anchor,deadline FROM execution_budgets ORDER BY task").fetch_all(store.pool()).await?;
        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots")
            .fetch_one(store.pool())
            .await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        let report = controller.reconcile_round_with(&store, &ManagedIo).await?;
        assert!(
            report.error.is_none() && report.runtime_errors.is_empty(),
            "{report:?}"
        );
        assert_eq!(
            (
                report.runtime_reconciliation_due,
                report.reclamation_checks,
                report.reclamation_ready,
                report.closed
            ),
            (0, 1, 1, 0)
        );
        let replay = controller.reconcile_round_with(&store, &RefuseIo).await?;
        assert!(replay.error.is_none(), "{replay:?}");
        assert_eq!((replay.reclamation_checks, replay.closed), (0, 0));
        let after_events: Vec<(i64, String, String, i64)> =
            sqlx::query_as("SELECT id,kind,payload,created FROM execution_events ORDER BY id")
                .fetch_all(store.pool())
                .await?;
        let after_budgets:Vec<TaskBudgetRow>=sqlx::query_as("SELECT task,attempts_spent,attempts_reserved,anchor,deadline FROM execution_budgets ORDER BY task").fetch_all(store.pool()).await?;
        let after_slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            (after_events, after_budgets, after_slots),
            (events, budgets, slots)
        );
        controller.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reclamation_empty_round_keeps_eleven_page_budget_and_visits_home_groups() -> Result<()>
    {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        for i in 0..15 {
            store.enroll(&format!("g{i:02}"), None).await?;
        }
        let controller = Controller::acquire(&store, crate::now()?).await?;
        let report = controller.reconcile_round_with(&store, &RefuseIo).await?;
        assert!(report.error.is_none(), "{report:?}");
        assert_eq!(
            (
                report.pages,
                report.groups_attempted,
                report.visits_completed
            ),
            (11, 11, 11)
        );
        assert_eq!(
            (
                report.runtime_reconciliation_due,
                report.reclamation_checks,
                report.closed,
                report.reclamation_ready
            ),
            (0, 0, 0, 0)
        );
        assert!(report.more);
        let groups:Vec<(String,String,String)>=sqlx::query_as("SELECT group_name,next_class,reclamation_after FROM execution_reconcile_groups ORDER BY group_name").fetch_all(store.pool()).await?;
        assert_eq!(groups.len(), 11);
        assert!(
            groups
                .iter()
                .all(|(_, class, cursor)| class == "reclamation" && cursor.is_empty())
        );
        let next = controller
            .reserve_reconciliation_visit(&store, &BTreeSet::new(), crate::now()?)
            .await?
            .context("next historical-only home group")?;
        assert_eq!(next.group, "g11");
        controller.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reclamation_selector_error_retains_committed_preference_without_fallback() -> Result<()>
    {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        let first = controller
            .reserve_reconciliation_visit(&store, &BTreeSet::new(), crate::now()?)
            .await?
            .context("first visit")?;
        assert_eq!(first.preferred, ReconciliationClass::Held);
        // Negative missing-owner-state fixture. No positive receipt or closure
        // is inserted. Genuine empty Held inventory cannot hide Runtime failure.
        sqlx::query("ALTER TABLE runtime_capture_intents RENAME TO unavailable_capture_intents")
            .execute(store.pool())
            .await?;
        let failed = controller.reconcile_round_with(&store, &RefuseIo).await?;
        assert!(
            failed
                .error
                .as_deref()
                .is_some_and(|error| error.contains("runtime_capture_intents")),
            "{failed:?}"
        );
        assert_eq!(
            (
                failed.pages,
                failed.visits_completed,
                failed.reclamation_checks
            ),
            (0, 0, 0)
        );
        let state:(String,String)=sqlx::query_as("SELECT next_class,reclamation_after FROM execution_reconcile_groups WHERE group_name='g'").fetch_one(store.pool()).await?;
        assert_eq!(
            state,
            ("held".into(), String::new()),
            "preferred Reclamation error committed Held next"
        );
        sqlx::query("ALTER TABLE unavailable_capture_intents RENAME TO runtime_capture_intents")
            .execute(store.pool())
            .await?;
        let healthy = controller.reconcile_round_with(&store, &RefuseIo).await?;
        assert!(healthy.error.is_none(), "{healthy:?}");
        assert_eq!(healthy.visits_completed, 1);
        controller.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reclamation_owner_commit_failure_and_restart_preserve_opposite_preference()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        sqlx::query("CREATE TRIGGER reclamation_completion_fault BEFORE UPDATE ON execution_driver_cursors WHEN NEW.job='reconcile' AND NEW.completed>OLD.completed BEGIN SELECT RAISE(ABORT,'reclamation completion fault'); END")
            .execute(store.pool()).await?;
        let failed = controller.reconcile_round_with(&store, &RefuseIo).await?;
        assert!(
            failed
                .error
                .as_deref()
                .is_some_and(|error| error.contains("reclamation completion fault")),
            "{failed:?}"
        );
        assert_eq!(failed.visits_completed, 0);
        let saved:(String,String)=sqlx::query_as("SELECT next_class,reclamation_after FROM execution_reconcile_groups WHERE group_name='g'").fetch_one(store.pool()).await?;
        assert_eq!(saved, ("reclamation".into(), String::new()));
        let counters: (i64, i64) = sqlx::query_as(
            "SELECT attempts,completed FROM execution_driver_cursors WHERE job='reconcile'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(counters, (1, 0));
        sqlx::query("DROP TRIGGER reclamation_completion_fault")
            .execute(store.pool())
            .await?;
        controller
            .finish(&store, crate::now()?, "restart after failed owner commit")
            .await?;
        drop(controller);
        store.close().await;
        let store = Store::open(temp.path(), false).await?;
        let successor = Controller::acquire(&store, crate::now()?).await?;
        let visit = successor
            .reserve_reconciliation_visit(&store, &BTreeSet::new(), crate::now()?)
            .await?
            .context("next committed opportunity")?;
        assert_eq!(visit.preferred, ReconciliationClass::Reclamation);
        let (selected, _) = successor.reconciliation_page(&store, &visit).await?;
        assert!(selected.is_none());
        let counters: (i64, i64) = sqlx::query_as(
            "SELECT attempts,completed FROM execution_driver_cursors WHERE job='reconcile'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(counters, (2, 1));
        successor.finish(&store, crate::now()?, "joined").await?;
        Ok(())
    }

    #[tokio::test]
    async fn controller_lock_epoch_and_interrupted_history_survive_restart() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        let first = Controller::acquire(&store, 100).await?;
        assert!(Controller::acquire(&store, 101).await.is_err());
        let held_job = first.clone();
        drop(first);
        assert!(Controller::acquire(&store, 102).await.is_err());
        drop(held_job);
        let second = Controller::acquire(&store, 103).await?;
        assert_eq!(second.generation, 2);
        let outcome: String =
            sqlx::query_scalar("SELECT outcome FROM execution_controller_runs WHERE generation=1")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&outcome)?["state"],
            "interrupted"
        );
        assert_eq!(second.tick(&store, 104).await?.tasks, 0);
        second.finish(&store, 105, "joined").await?;
        assert_eq!(
            second.tick(&store, 106).await?.error.as_deref(),
            Some("execution_controller_superseded")
        );
        let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_attempts")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(attempts, 0);
        Ok(())
    }
    #[tokio::test]
    async fn failed_supervision_rotates_home_groups_without_repairing_owner_metadata() -> Result<()>
    {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        for group in ["a", "b", "c"] {
            store.enroll(group, None).await?;
        }
        // Deliberately corrupt only a negative fixture. Missing recovery state
        // must remain visible; scheduler must not manufacture a fresh epoch.
        sqlx::query("DELETE FROM decision_supervision WHERE group_name='a'")
            .execute(store.pool())
            .await?;
        let controller = Controller::acquire(&store, crate::now()?).await?;
        let failed = controller.supervise(&store).await?;
        assert_eq!(failed.group.as_deref(), Some("a"));
        assert!(failed.error.is_some());
        assert_eq!(failed.visits_completed, 0);
        let next = controller.supervise(&store).await?;
        assert_eq!(next.group.as_deref(), Some("b"));
        assert_eq!(next.visits_completed, 1, "{next:?}");
        let mut tx = store.pool().begin().await?;
        // A cancelled owner page rolls back its completion marker, while the
        // earlier attempted group visit remains durable for restart fairness.
        controller
            .completed_tx(&mut tx, JobKind::Supervise, crate::now()?)
            .await?;
        tx.rollback().await?;
        let counters: (i64, i64) = sqlx::query_as(
            "SELECT attempts,completed FROM execution_driver_cursors WHERE job='supervise'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(counters, (2, 1));
        controller
            .finish(&store, crate::now()?, "restart-control")
            .await?;
        drop(controller);
        let successor = Controller::acquire(&store, crate::now()?).await?;
        assert_eq!(
            successor.supervise(&store).await?.group.as_deref(),
            Some("c")
        );
        let missing: i64 =
            sqlx::query_scalar("SELECT count(*) FROM decision_supervision WHERE group_name='a'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(missing, 0);
        successor
            .finish(&store, crate::now()?, "control-joined")
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_original_gate_and_rollback_nonce_preserve_one_failure_account() -> Result<()>
    {
        use crate::supervisor_failures as failures;
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("bridge", None).await?;
        let controller = Controller::acquire(&store, 100).await?;
        let visit = controller
            .reserve_supervisor_visit(&store, 100, 110)
            .await?
            .context("original visit")?;
        let mut tx = store.pool().begin().await?;
        let rolled_back = failures::begin_supervisor_visit_tx(&mut tx, &visit, 101).await?;
        tx.rollback().await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            failures::validate_active_visit_tx(&mut tx, &rolled_back, 102)
                .await
                .is_err()
        );
        let fresh = failures::begin_supervisor_visit_tx(&mut tx, &visit, 102).await?;
        assert_ne!(fresh.start_nonce(), rolled_back.start_nonce());
        tx.rollback().await?;
        let mut tx = store.pool().begin().await?;
        let page = failures::reconcile_supervisor_visits_tx(&mut tx, "bridge", 111, 100).await?;
        assert_eq!((page.classified, page.failed, page.completed), (1, 1, 0));
        tx.commit().await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            failures::begin_supervisor_visit_tx(&mut tx, &visit, 103)
                .await
                .is_err(),
            "closed gate rejects a queued old clock/start"
        );
        let sources = failures::supervisor_failure_sources_tx(&mut tx, "bridge", 0, 100).await?;
        assert_eq!(sources.ids.len(), 1);
        let id = sources.ids[0];
        let original =
            failures::validate_supervisor_failure_source_tx(&mut tx, "bridge", id, 111).await?;
        assert_eq!(original.source().episode, visit.identity().nonce);
        assert_eq!(original.source().due_at, 110);
        let key = original.source().source_key.clone();
        tx.commit().await?;
        controller
            .reserve_supervisor_visit(&store, 112, 122)
            .await?
            .context("later original visit")?;
        let mut tx = store.pool().begin().await?;
        let later = failures::reconcile_supervisor_visits_tx(&mut tx, "bridge", 123, 100).await?;
        assert_eq!(later.failed, 1);
        let sources = failures::supervisor_failure_sources_tx(&mut tx, "bridge", 0, 100).await?;
        assert_eq!(sources.ids, vec![id]);
        let same =
            failures::validate_supervisor_failure_source_tx(&mut tx, "bridge", id, 123).await?;
        assert_eq!(same.source().source_key, key);
        assert_eq!(same.source().due_at, 110);
        assert_eq!(same.source().episode, visit.identity().nonce);
        tx.commit().await?;
        let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_attempts")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            attempts, 0,
            "infrastructure visits grant no execution attempts"
        );
        controller.finish(&store, 124, "control joined").await?;
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_actual_owner_commit_survives_lost_controller_diagnostic() -> Result<()> {
        use crate::supervisor_failures as failures;
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("bridge", None).await?;
        let now = crate::now()?;
        let controller = Controller::acquire(&store, now).await?;
        sqlx::query("CREATE TRIGGER bridge_diagnostic_failure BEFORE UPDATE ON execution_driver_cursors WHEN NEW.completed>OLD.completed BEGIN SELECT RAISE(ABORT,'lost diagnostic control'); END")
            .execute(store.pool()).await?;
        let report = controller.supervise(&store).await?;
        assert_eq!(report.visits_completed, 1, "{report:?}");
        assert!(
            report.error.is_some(),
            "actual diagnostic failure must be reported"
        );
        let (visit, deadline): (i64, i64) = sqlx::query_as(
            "SELECT id,deadline FROM execution_supervisor_visits WHERE group_name='bridge'",
        )
        .fetch_one(store.pool())
        .await?;
        let mut tx = store.pool().begin().await?;
        let receipt = crate::decision_supervisor::validate_supervisor_commit_tx(&mut tx, visit)
            .await?
            .context("actual atomic owner receipt")?;
        assert_eq!(receipt.visit().id, visit);
        let page =
            failures::reconcile_supervisor_visits_tx(&mut tx, "bridge", deadline + 1, 100).await?;
        assert_eq!((page.classified, page.completed, page.failed), (1, 1, 0));
        assert!(
            failures::supervisor_failure_sources_tx(&mut tx, "bridge", 0, 100)
                .await?
                .ids
                .is_empty()
        );
        tx.commit().await?;
        controller
            .finish(&store, deadline + 2, "control joined")
            .await?;
        Ok(())
    }
}
