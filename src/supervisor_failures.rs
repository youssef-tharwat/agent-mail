//! Original Supervise visits and graph-independent finite operator responsibility.
//! Recovery alone writes/authenticates owner commit receipts; Progress owns transport.
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};

type Tx<'a> = Transaction<'a, Sqlite>;

/// Audit identity is not a page-start capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SupervisorVisitIdentity {
    pub id: i64,
    pub nonce: String,
    pub group: String,
    pub home_machine: String,
    pub job: String,
    pub generation: i64,
    pub owner: String,
    pub attempt_sequence: i64,
    pub opened: i64,
    pub deadline: i64,
}

#[derive(Debug)]
pub(crate) struct SupervisorVisit(SupervisorVisitIdentity);
impl SupervisorVisit {
    pub(crate) fn identity(&self) -> &SupervisorVisitIdentity {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) struct ActiveSupervisorVisit {
    identity: SupervisorVisitIdentity,
    start_nonce: String,
}
impl ActiveSupervisorVisit {
    pub(crate) fn identity(&self) -> &SupervisorVisitIdentity {
        &self.identity
    }
    pub(crate) fn start_nonce(&self) -> &str {
        &self.start_nonce
    }
}

/// Authenticate immutable original identity without requiring a current controller.
pub(crate) async fn validate_original_visit_tx(
    tx: &mut Tx<'_>,
    id: i64,
) -> Result<SupervisorVisitIdentity> {
    ensure!(id > 0, "invalid_supervisor_visit");
    let row=sqlx::query("SELECT v.*,r.owner AS run_owner FROM execution_supervisor_visits v JOIN execution_controller_runs r ON r.generation=v.generation WHERE v.id=?")
        .bind(id).fetch_optional(&mut **tx).await?.context("supervisor_visit_missing")?;
    let identity = SupervisorVisitIdentity {
        id: row.get("id"),
        nonce: row.get("nonce"),
        group: row.get("group_name"),
        home_machine: row.get("home_machine"),
        job: row.get("job"),
        generation: row.get("generation"),
        owner: row.get("owner"),
        attempt_sequence: row.get("attempt_sequence"),
        opened: row.get("opened"),
        deadline: row.get("deadline"),
    };
    ensure!(
        identity.job == "supervise"
            && identity.generation > 0
            && identity.attempt_sequence > 0
            && identity.opened >= 0
            && identity.deadline > identity.opened
            && identity.deadline - identity.opened <= 10
            && identity.owner == row.get::<String, _>("run_owner"),
        "corrupt_supervisor_visit_identity"
    );
    uuid::Uuid::parse_str(&identity.nonce).context("corrupt_supervisor_visit_nonce")?;
    crate::name(&identity.group)?;
    ensure!(!identity.home_machine.is_empty(), "supervisor_home_missing");
    Ok(identity)
}

/// Called only in the controller's genuine attempted-group reservation transaction.
pub(crate) async fn reserve_supervisor_visit_tx(
    tx: &mut Tx<'_>,
    group: &str,
    generation: i64,
    owner: &str,
    opened: i64,
    deadline: i64,
) -> Result<SupervisorVisit> {
    crate::decision_recovery::reserve_home_tx(tx, group).await?;
    ensure!(
        opened >= 0 && deadline > opened && deadline - opened <= 10,
        "invalid_supervisor_visit_interval"
    );
    let home: String = sqlx::query_scalar("SELECT home_machine FROM groups WHERE name=?")
        .bind(group)
        .fetch_one(&mut **tx)
        .await?;
    let sequence:Option<i64>=sqlx::query_scalar("SELECT d.attempts FROM execution_driver_cursors d JOIN execution_controller c ON c.id=1 WHERE d.job='supervise' AND d.last_group=? AND c.generation=? AND c.owner=? AND c.state='running'")
        .bind(group).bind(generation).bind(owner).fetch_optional(&mut **tx).await?;
    let sequence = sequence.context("supervisor_reservation_fenced")?;
    let nonce = uuid::Uuid::new_v4().to_string();
    let id=sqlx::query("INSERT INTO execution_supervisor_visits(nonce,group_name,home_machine,job,generation,owner,attempt_sequence,opened,deadline) VALUES(?,?,?,'supervise',?,?,?,?,?)")
        .bind(&nonce).bind(group).bind(home).bind(generation).bind(owner).bind(sequence).bind(opened).bind(deadline)
        .execute(&mut **tx).await?.last_insert_rowid();
    Ok(SupervisorVisit(validate_original_visit_tx(tx, id).await?))
}

async fn validate_current_visit_tx(
    tx: &mut Tx<'_>,
    identity: &SupervisorVisitIdentity,
    now: i64,
) -> Result<()> {
    crate::decision_recovery::reserve_home_tx(tx, &identity.group).await?;
    ensure!(
        validate_original_visit_tx(tx, identity.id).await? == *identity,
        "supervisor_visit_identity_changed"
    );
    ensure!(
        now >= identity.opened && now < identity.deadline,
        "supervisor_visit_expired"
    );
    let current:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_supervisor_visits v JOIN groups g ON g.name=v.group_name JOIN node n ON n.id=g.home_machine JOIN execution_controller c ON c.id=1 WHERE v.id=? AND v.gate='open' AND v.home_machine=g.home_machine AND c.generation=v.generation AND c.owner=v.owner AND c.state='running')")
        .bind(identity.id).fetch_one(&mut **tx).await?;
    ensure!(current, "supervisor_visit_start_fenced");
    Ok(())
}

pub(crate) async fn begin_supervisor_visit_tx(
    tx: &mut Tx<'_>,
    visit: &SupervisorVisit,
    now: i64,
) -> Result<ActiveSupervisorVisit> {
    validate_current_visit_tx(tx, visit.identity(), now).await?;
    let committed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_supervisor_commits WHERE visit=?)",
    )
    .bind(visit.identity().id)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(!committed, "supervisor_visit_already_committed");
    // This nonce is written in the OWNER transaction. Rollback removes it; a
    // retained active proof cannot bind a later transaction's genuine page.
    let start_nonce = uuid::Uuid::new_v4().to_string();
    let changed=sqlx::query("UPDATE execution_supervisor_visits SET page_nonce=? WHERE id=? AND gate='open' AND page_nonce IS NULL")
        .bind(&start_nonce).bind(visit.identity().id).execute(&mut **tx).await?;
    ensure!(
        changed.rows_affected() == 1,
        "supervisor_visit_already_started"
    );
    Ok(ActiveSupervisorVisit {
        identity: visit.identity().clone(),
        start_nonce,
    })
}

/// Historical page-start provenance; never requires today's controller fence.
pub(crate) async fn validate_visit_start_tx(
    tx: &mut Tx<'_>,
    id: i64,
    start_nonce: &str,
) -> Result<()> {
    uuid::Uuid::parse_str(start_nonce).context("invalid_supervisor_start_nonce")?;
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_supervisor_visits WHERE id=? AND page_nonce=?)",
    )
    .bind(id)
    .bind(start_nonce)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(matches, "supervisor_page_start_missing");
    Ok(())
}

/// Recovery consumes its private completed page and calls this before its receipt.
pub(crate) async fn validate_active_visit_tx(
    tx: &mut Tx<'_>,
    active: &ActiveSupervisorVisit,
    now: i64,
) -> Result<SupervisorVisitIdentity> {
    validate_current_visit_tx(tx, active.identity(), now).await?;
    validate_visit_start_tx(tx, active.identity().id, active.start_nonce()).await?;
    Ok(active.identity().clone())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum HomeOperator {
    HomeOperator { group: String, home_machine: String },
}

#[derive(Debug, Serialize)]
pub(crate) struct SupervisorFailureSource {
    pub group: String,
    pub id: i64,
    pub first_visit: SupervisorVisitIdentity,
    pub source_key: String,
    pub episode: String,
    pub responsible: HomeOperator,
    pub unresolved: bool,
    pub due_at: i64,
    pub revision: i64,
    pub facts: Value,
}

#[derive(Debug)]
pub(crate) struct ValidatedSupervisorFailureSource(SupervisorFailureSource);
impl ValidatedSupervisorFailureSource {
    pub(crate) fn source(&self) -> &SupervisorFailureSource {
        &self.0
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SupervisorFailurePage {
    pub ids: Vec<i64>,
    pub next_after: i64,
    pub more: bool,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct VisitReconciliationPage {
    pub classified: usize,
    pub completed: usize,
    pub failed: usize,
    pub more: bool,
}

fn source_key(visit: &SupervisorVisitIdentity) -> String {
    json!([
        "supervisor_failure",
        1,
        visit.home_machine,
        visit.group,
        "supervise"
    ])
    .to_string()
}

/// Two indexed infrastructure existence checks; no source or transport authority.
pub(crate) async fn supervisor_failure_work_available_tx(
    tx: &mut Tx<'_>,
    group: &str,
    now: i64,
) -> Result<bool> {
    ensure!(now >= 0, "invalid_supervisor_time");
    crate::decision_recovery::reserve_home_tx(tx, group).await?;
    let due:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_supervisor_visits WHERE group_name=? AND job='supervise' AND gate='open' AND deadline<=?)")
        .bind(group).bind(now).fetch_one(&mut **tx).await?;
    let unresolved:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_supervisor_failures WHERE group_name=? AND job='supervise' AND state='unresolved')")
        .bind(group).fetch_one(&mut **tx).await?;
    Ok(due || unresolved)
}

pub(crate) async fn supervisor_failure_sources_tx(
    tx: &mut Tx<'_>,
    group: &str,
    after: i64,
    limit: usize,
) -> Result<SupervisorFailurePage> {
    ensure!(
        after >= 0 && (1..=100).contains(&limit),
        "invalid_supervisor_failure_page"
    );
    crate::decision_recovery::reserve_home_tx(tx, group).await?;
    let mut ids:Vec<i64>=sqlx::query_scalar("SELECT id FROM execution_supervisor_failures WHERE group_name=? AND id>? ORDER BY id LIMIT ?")
        .bind(group).bind(after).bind((limit+1) as i64).fetch_all(&mut **tx).await?;
    let more = ids.len() > limit;
    ids.truncate(limit);
    let next_after = if more {
        *ids.last().context("missing_supervisor_failure_cursor")?
    } else {
        0
    };
    Ok(SupervisorFailurePage {
        ids,
        next_after,
        more,
    })
}

pub(crate) async fn validate_supervisor_failure_source_tx(
    tx: &mut Tx<'_>,
    group: &str,
    id: i64,
    now: i64,
) -> Result<ValidatedSupervisorFailureSource> {
    ensure!(id > 0 && now >= 0, "invalid_supervisor_failure_source");
    crate::decision_recovery::reserve_home_tx(tx, group).await?;
    let row=sqlx::query("SELECT f.*,g.home_machine AS current_home FROM execution_supervisor_failures f JOIN groups g ON g.name=f.group_name WHERE f.id=? AND f.group_name=?")
        .bind(id).bind(group).fetch_optional(&mut **tx).await?.context("supervisor_failure_missing")?;
    let first = validate_original_visit_tx(tx, row.get("first_visit")).await?;
    ensure!(
        first.group == group
            && first.home_machine == row.get::<String, _>("home_machine")
            && first.home_machine == row.get::<String, _>("current_home")
            && row.get::<String, _>("job") == "supervise"
            && row.get::<String, _>("episode") == first.nonce
            && row.get::<String, _>("source_key") == source_key(&first)
            && row.get::<i64, _>("opened") == first.opened
            && row.get::<i64, _>("due_at") == first.deadline
            && row.get::<i64, _>("revision") > 0
            && now >= first.deadline,
        "corrupt_supervisor_failure_identity"
    );
    // A future authorized full-scan settlement requires its own genuine proof.
    ensure!(
        row.get::<String, _>("state") == "unresolved"
            && row.get::<Option<i64>, _>("settlement_visit").is_none(),
        "supervisor_failure_settlement_not_allocated"
    );
    let closed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_supervisor_visits WHERE id=? AND gate='closed' AND closed_at>=deadline)")
        .bind(first.id).fetch_one(&mut **tx).await?;
    ensure!(closed, "supervisor_failure_visit_not_closed");
    ensure!(
        crate::decision_supervisor::validate_supervisor_commit_tx(tx, first.id)
            .await?
            .is_none(),
        "supervisor_failure_has_owner_commit"
    );
    let event:Option<String>=sqlx::query_scalar("SELECT payload FROM execution_supervisor_failure_events WHERE failure=? AND visit=? AND kind='first_failure'")
        .bind(id).bind(first.id).fetch_optional(&mut **tx).await?;
    let expected = json!({"visit":first,"classification":"owner_page_not_committed"});
    ensure!(
        event.as_deref() == Some(expected.to_string().as_str()),
        "supervisor_failure_original_event_missing"
    );
    Ok(ValidatedSupervisorFailureSource(SupervisorFailureSource {
        group: group.into(),
        id,
        source_key: source_key(&first),
        episode: first.nonce.clone(),
        responsible: HomeOperator::HomeOperator {
            group: group.into(),
            home_machine: first.home_machine.clone(),
        },
        unresolved: true,
        due_at: first.deadline,
        revision: row.get("revision"),
        facts: expected,
        first_visit: first,
    }))
}

/// Classify at most100 original visits under the same writer as gate closure.
pub(crate) async fn reconcile_supervisor_visits_tx(
    tx: &mut Tx<'_>,
    group: &str,
    now: i64,
    limit: usize,
) -> Result<VisitReconciliationPage> {
    ensure!(
        now >= 0 && (1..=100).contains(&limit),
        "invalid_supervisor_visit_page"
    );
    crate::decision_recovery::reserve_home_tx(tx, group).await?;
    let ids:Vec<i64>=sqlx::query_scalar("SELECT id FROM execution_supervisor_visits WHERE group_name=? AND job='supervise' AND gate='open' AND deadline<=? ORDER BY deadline,id LIMIT ?")
        .bind(group).bind(now).bind((limit+1) as i64).fetch_all(&mut **tx).await?;
    let mut page = VisitReconciliationPage {
        more: ids.len() > limit,
        ..Default::default()
    };
    for id in ids.iter().take(limit) {
        let visit = validate_original_visit_tx(tx, *id).await?;
        let home: String = sqlx::query_scalar("SELECT home_machine FROM groups WHERE name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
        ensure!(
            visit.group == group && visit.home_machine == home,
            "supervisor_visit_home_changed"
        );
        let changed=sqlx::query("UPDATE execution_supervisor_visits SET gate='closed',closed_at=? WHERE id=? AND gate='open' AND deadline<=?")
            .bind(now).bind(id).bind(now).execute(&mut **tx).await?;
        ensure!(
            changed.rows_affected() == 1,
            "supervisor_visit_classification_conflict"
        );
        // Receipt authentication remains entirely Recovery-owned, including corruption.
        if let Some(receipt) =
            crate::decision_supervisor::validate_supervisor_commit_tx(tx, *id).await?
        {
            ensure!(
                receipt.visit() == &visit,
                "supervisor_commit_visit_mismatch"
            );
            page.completed += 1;
        } else {
            let existing:Option<i64>=sqlx::query_scalar("SELECT id FROM execution_supervisor_failures WHERE group_name=? AND home_machine=? AND job='supervise' AND state='unresolved'")
                .bind(group).bind(&visit.home_machine).fetch_optional(&mut **tx).await?;
            let (failure, kind) = if let Some(existing) = existing {
                validate_supervisor_failure_source_tx(tx, group, existing, now).await?;
                (existing, "visit_failed")
            } else {
                let result=sqlx::query("INSERT INTO execution_supervisor_failures(group_name,home_machine,job,first_visit,episode,source_key,opened,due_at) VALUES(?,?,'supervise',?,?,?,?,?)")
                    .bind(group).bind(&visit.home_machine).bind(visit.id).bind(&visit.nonce).bind(source_key(&visit)).bind(visit.opened).bind(visit.deadline)
                    .execute(&mut **tx).await?;
                (result.last_insert_rowid(), "first_failure")
            };
            let payload =
                json!({"visit":visit,"classification":"owner_page_not_committed"}).to_string();
            sqlx::query("INSERT INTO execution_supervisor_failure_events(failure,visit,kind,created,payload) VALUES(?,?,?,?,?)")
                .bind(failure).bind(id).bind(kind).bind(now).bind(payload).execute(&mut **tx).await?;
            page.failed += 1;
        }
        page.classified += 1;
    }
    Ok(page)
}
