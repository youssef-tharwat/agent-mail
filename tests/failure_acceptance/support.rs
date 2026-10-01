//! Independent acceptance observations. No runtime gate or physical receipt is fabricated.
use agent_mail::{
    store::{Mailbox, Store},
    task_graph::{
        CandidateDraft, CandidateRequest, CriterionEvidence, Phase, TaskCreate, TaskDecision,
    },
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{
    Column, Row, TypeInfo, ValueRef,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub const GROUP: &str = "acceptance";
pub const SCOPE: &str = "publish acceptance artifact";
pub const NOW: i64 = 1_700_000_000;
pub const STATE_ROOT: &str = "/tmp/agent-mail-durable-execution/failure-acceptance";

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    std::fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

pub struct Fixture {
    pub root: PathBuf,
    pub store: Store,
    pub writer: Mailbox,
    pub worker: Mailbox,
}

/// Audit provenance from the source-verifying wrapper; this grants no runtime authority.
pub fn run_provenance(root: &Path) -> Result<Value> {
    let path = PathBuf::from(
        std::env::var("AGENT_MAIL_ACCEPTANCE_PROVENANCE")
            .context("verified run provenance required")?,
    );
    ensure!(
        path == root.join("run-provenance.json"),
        "provenance must be in the isolated run root"
    );
    ensure!(
        std::fs::metadata(&path)?.len() <= 262_144,
        "provenance exceeds bounded envelope"
    );
    let value: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    ensure!(value["schema_version"] == 1, "unsupported run provenance");
    let grant = std::env::var("AGENT_MAIL_ACCEPTANCE_GRANT")?;
    ensure!(
        !grant.trim().is_empty() && string(&value, "grant_id")? == grant.as_str(),
        "named grant provenance mismatch"
    );
    ensure!(
        matches!(
            string(&value, "mode")?,
            "public-controls" | "full-acceptance"
        ),
        "unsupported run mode"
    );
    ensure!(
        Path::new(string(&value, "state_dir")?) == root,
        "provenance state mismatch"
    );
    let cwd = std::env::current_dir()?.canonicalize()?;
    ensure!(
        Path::new(string(&value, "source_workdir")?) == cwd.as_path(),
        "provenance cwd mismatch"
    );
    for field in ["runner_id", "source_profile", "verified_at"] {
        ensure!(
            !string(&value, field)?.trim().is_empty(),
            "missing provenance {field}"
        );
    }
    for field in [
        "source_manifest_sha256",
        "source_archive_sha256",
        "harness_manifest_sha256",
        "wrapper_sha256",
    ] {
        let digest = string(&value, field)?;
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid provenance digest {field}"
        );
    }
    ensure!(
        value["debug_assertions"] == true
            && value["build_jobs"] == 2
            && value["telemetry"] == "disabled",
        "run policy provenance mismatch"
    );
    ensure!(
        value["source_files"]
            .as_object()
            .is_some_and(|files| !files.is_empty()),
        "verified source file map required"
    );
    ensure!(
        integer(&value, "effective_stop_epoch")? > agent_mail::now()?,
        "run stop boundary expired"
    );
    Ok(value)
}

impl Fixture {
    pub async fn new(run: &Path, name: &str) -> Result<Self> {
        let root = run.join(name);
        // Preserve every DB and log, including failed originals, for the remote artifact collector.
        std::fs::create_dir(&root)?;
        let store = Store::open(&root, true).await?;
        store.enroll(GROUP, None).await?;
        let writer = store.register(GROUP, "writer", false).await?;
        let worker = store.register(GROUP, "worker", false).await?;
        Ok(Self {
            root,
            writer: store.authenticate(GROUP, Some(&writer)).await?,
            worker: store.authenticate(GROUP, Some(&worker)).await?,
            store,
        })
    }

    pub async fn record(&self, label: &str, assignments: &[&str]) -> Result<Snapshot> {
        let snapshot = Snapshot::read(&self.root).await?;
        let inventory = snapshot.inventory(assignments)?;
        write_json(&self.root.join(format!("{label}.snapshot.json")), &snapshot)?;
        write_json(
            &self.root.join(format!("{label}.inventory.json")),
            &inventory,
        )?;
        Ok(snapshot)
    }
}

pub fn draft(id: &str) -> Result<TaskCreate> {
    Ok(serde_json::from_value(json!({
        "key":format!("create:{id}"), "reason":"Isolated acceptance model control",
        "expected_parent_versions":{},
        "draft":{
            "work":{"id":id,"owner":"worker","state":"ready","scope":SCOPE,
                    "next_action":"Publish the artifact","deadline":null,"evidence":[]},
            "contract":{"deliverable":"One artifact","criteria":[{"id":"artifact","description":"Artifact cites its immutable inputs"}],
                        "allowed_scope":[SCOPE],"completion":"writer_acceptance",
                        "allow_delegation":true,"allow_input_invalidation":true,
                        "budget":{"max_attempts":3,"max_elapsed_seconds":240,"max_cost":null}},
            "authorization":{"state":"authorized","source":{"kind":"direct","authority_ref":"isolated fixture authority"},
                             "approved_scope":[SCOPE],"reason":"Explicit bounded fixture"},
            "requirements":[],"parent":null
        }
    }))?)
}

pub fn decision(version: i64, key: &str) -> TaskDecision {
    use agent_mail::{
        task_graph::{Change, OutcomeChange},
        work::WorkPatch,
    };
    TaskDecision {
        key: key.into(),
        version,
        reason: "Explicit acceptance fixture decision".into(),
        work_patch: WorkPatch::default(),
        scope: Change::Keep,
        contract: Change::Keep,
        authorization: Change::Keep,
        requirements: Change::Keep,
        parent: Change::Keep,
        expected_parent_versions: BTreeMap::new(),
        clear_invalidation: false,
        outcome: OutcomeChange::Keep,
        resolve_message: None,
    }
}

pub async fn candidate(
    f: &Fixture,
    task: &str,
    revision: &str,
    key: &str,
    now: i64,
) -> Result<String> {
    let view = f.store.task_inspect(&f.writer, task).await?;
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, task, view.work.version, Phase::Accept)
        .await?;
    Ok(f.store
        .task_candidate(
            &f.writer,
            task,
            CandidateRequest {
                key: key.into(),
                version: view.work.version,
                candidate: CandidateDraft {
                    revision: revision.into(),
                    summary: "Model-only immutable candidate".into(),
                    criterion_evidence: vec![CriterionEvidence {
                        criterion_id: "artifact".into(),
                        references: vec!["fixture:model-only".into()],
                    }],
                    inputs,
                },
            },
            now,
        )
        .await?
        .id)
}

// Fixed, audited table allowlist. Include terminal runtime cleanup and all decision responsibility.
// Authentication secrets/mailboxes and arbitrary user tables are intentionally excluded.
const TABLES: &[&str] = &[
    "work_items",
    "work_changes",
    "work_decisions",
    "work_creations",
    "task_models",
    "task_requirements",
    "task_results",
    "task_model_events",
    "task_decisions",
    "task_blocking_edges",
    "task_decision_policies",
    "task_decision_policy_history",
    "task_grants",
    "task_grant_history",
    "task_materializations",
    "execution_tasks",
    "execution_budgets",
    "execution_attempts",
    "execution_slots",
    "execution_dispatches",
    "execution_charges",
    "execution_events",
    "execution_receipts",
    "execution_causes",
    "execution_cursors",
    "execution_clock",
    "execution_observations",
    "execution_controller",
    "execution_controller_runs",
    "execution_controller_dispatches",
    "runtime_targets",
    "runtime_target_versions",
    "runtime_capability_witnesses",
    "runtime_segments",
    "runtime_destinations",
    "runtime_effects",
    "runtime_effect_sets",
    "runtime_receipts",
    "runtime_closures",
    "runtime_observations",
    "decision_cases",
    "decision_blockers",
    "operator_obligations",
    "decision_supervision",
    "decision_audit",
    "followups",
    "followup_history",
    "attention_occurrences",
    "deliveries",
    "coordination_events",
    "event_receipts",
    "hook_emissions",
    "turn_offers",
    "turn_offer_items",
    "progress_records",
    "task_progress",
    "progress_milestones",
    "progress_current",
    "operator_notice_routes",
    "operator_notice_accounts",
    "operator_notice_spending",
    "operator_notices",
    "operator_notice_batches",
    "operator_notice_batch_items",
    "operator_notice_events",
    "operator_notice_projection",
    "operator_notice_dispatch_cursor",
    "operator_notice_repairs",
    "operator_notice_legacy",
];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub tables: BTreeMap<String, Vec<Value>>,
}

impl Snapshot {
    pub async fn read(root: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(root.join("mail.db"))
            .read_only(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let mut tx = pool.begin().await?;
        let mut tables = BTreeMap::new();
        for table in TABLES {
            let mut values = Vec::new();
            // Only allowlist constants are interpolated; the same transaction covers every table.
            for row in sqlx::query(&format!("SELECT * FROM {table}"))
                .fetch_all(&mut *tx)
                .await?
            {
                let mut value = serde_json::Map::new();
                for column in row.columns() {
                    let name = column.name();
                    let raw = row.try_get_raw(name)?;
                    let cell = if raw.is_null() {
                        Value::Null
                    } else {
                        match raw.type_info().name() {
                            "INTEGER" | "BOOLEAN" => json!(row.try_get::<i64, _>(name)?),
                            "REAL" => json!(row.try_get::<f64, _>(name)?),
                            "TEXT" => json!(row.try_get::<String, _>(name)?),
                            other => bail!("unsupported snapshot type {table}.{name}: {other}"),
                        }
                    };
                    value.insert(name.into(), cell);
                }
                values.push(Value::Object(value));
            }
            values.sort_by_cached_key(Value::to_string);
            tables.insert((*table).into(), values);
        }
        tx.rollback().await?;
        pool.close().await;
        Ok(Self { tables })
    }

    pub fn rows(&self, table: &str) -> Result<&[Value]> {
        Ok(self
            .tables
            .get(table)
            .with_context(|| format!("missing required table {table}"))?)
    }

    pub fn inventory(&self, assignments: &[&str]) -> Result<BTreeSet<String>> {
        // An immutable fixture roster is independent of the product's inbox/open-task projection.
        for table in TABLES {
            self.rows(table)?;
        }
        let mut tasks = BTreeSet::new();
        let mut obligations = BTreeSet::new();
        for row in self.rows("work_items")? {
            ensure!(string(row, "group_name")? == GROUP, "foreign fixture group");
            let id = string(row, "id")?;
            ensure!(tasks.insert(id), "duplicate task {id}");
            if integer(row, "open")? == 1 {
                obligations.insert(format!("task:{id}"));
            }
        }
        for id in assignments {
            ensure!(tasks.contains(id), "lost fixture assignment {id}");
        }
        let mut attempts = BTreeMap::new();
        let mut held_tasks = BTreeSet::new();
        let mut held = BTreeSet::new();
        for row in self.rows("execution_attempts")? {
            let id = string(row, "id")?;
            let task = string(row, "task")?;
            ensure!(tasks.contains(task), "orphan attempt {id}");
            ensure!(attempts.insert(id, row).is_none(), "duplicate attempt {id}");
            if integer(row, "holds_slot")? == 1 {
                ensure!(
                    held_tasks.insert(task),
                    "overlapping held attempts for {task}"
                );
                held.insert(id);
                obligations.insert(format!("attempt:{id}:{task}"));
            }
        }
        let mut slots = BTreeSet::new();
        for row in self.rows("execution_slots")? {
            let id = string(row, "attempt")?;
            ensure!(slots.insert(id), "duplicate runtime slot {id}");
            ensure!(held.contains(id), "runtime slot without held attempt {id}");
        }
        ensure!(slots == held, "held attempt missing runtime slot");
        for row in self.rows("runtime_effects")? {
            let attempt = string(row, "attempt")?;
            ensure!(
                attempts.contains_key(attempt),
                "orphan effect for {attempt}"
            );
            ensure!(
                integer(row, "retention_pin")? == 1,
                "effect retention pin lost"
            );
            if !matches!(string(row, "state")?, "published" | "abandoned") {
                obligations.insert(format!("effect:{attempt}:{}", string(row, "effect")?));
            }
        }
        for row in self.rows("execution_causes")? {
            if integer(row, "settled")? == 0 {
                obligations.insert(format!("cause:{}", string(row, "id")?));
            }
        }
        for (table, prefix) in [
            ("decision_cases", "decision"),
            ("operator_obligations", "operator"),
        ] {
            for row in self.rows(table)? {
                if !matches!(string(row, "state")?, "handled" | "superseded") {
                    obligations.insert(format!("{prefix}:{}", integer(row, "id")?));
                }
            }
        }
        let cases: BTreeSet<i64> = self
            .rows("decision_cases")?
            .iter()
            .map(|row| integer(row, "id"))
            .collect::<Result<_>>()?;
        let mut operator_cases = BTreeSet::new();
        for row in self.rows("operator_obligations")? {
            let case = integer(row, "case_id")?;
            ensure!(
                cases.contains(&case),
                "operator lost its decision source {case}"
            );
            ensure!(
                operator_cases.insert(case),
                "duplicate operator for case {case}"
            );
        }
        ensure!(
            cases == operator_cases,
            "decision case lost terminal operator responsibility"
        );
        for row in self.rows("deliveries")? {
            if string(row, "state")? == "pending" {
                obligations.insert(format!(
                    "delivery:{}:{}",
                    integer(row, "message")?,
                    integer(row, "recipient")?
                ));
            }
        }
        Ok(obligations)
    }
}

pub fn budget_continuity(before: &Snapshot, after: &Snapshot) -> Result<()> {
    let newer: BTreeMap<&str, &Value> = after
        .rows("execution_budgets")?
        .iter()
        .map(|row| Ok((string(row, "task")?, row)))
        .collect::<Result<_>>()?;
    for old in before.rows("execution_budgets")? {
        let task = string(old, "task")?;
        let new = newer.get(task).context("budget disappeared")?;
        if !old["anchor"].is_null() {
            ensure!(
                old["anchor"] == new["anchor"],
                "lifetime anchor reset for {task}"
            );
        }
        if let Some(deadline) = old["deadline"].as_i64() {
            ensure!(
                new["deadline"]
                    .as_i64()
                    .is_some_and(|current| current <= deadline),
                "lifetime deadline extended for {task}"
            );
        }
        for field in ["attempts_spent", "cost_spent"] {
            ensure!(
                integer(new, field)? >= integer(old, field)?,
                "settled spending reset for {task}.{field}"
            );
        }
    }
    Ok(())
}

pub fn string<'a>(row: &'a Value, key: &str) -> Result<&'a str> {
    row.get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing text {key}"))
}
pub fn integer(row: &Value, key: &str) -> Result<i64> {
    row.get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("missing integer {key}"))
}

#[derive(Serialize)]
pub struct CheckResult {
    pub id: String,
    pub evidence_class: String,
    pub verdict: String,
    pub elapsed_ms: u128,
    pub detail: Value,
}

pub async fn capture(
    run: &Path,
    id: &str,
    evidence_class: &str,
    check: impl std::future::Future<Output = Result<Value>>,
) -> Result<CheckResult> {
    let started = Instant::now();
    let outcome = check.await;
    let (verdict, detail) = match outcome {
        Ok(detail) => ("pass", detail),
        Err(error) => ("fail", json!({"error":format!("{error:#}")})),
    };
    let result = CheckResult {
        id: id.into(),
        evidence_class: evidence_class.into(),
        verdict: verdict.into(),
        elapsed_ms: started.elapsed().as_millis(),
        detail,
    };
    write_json(&run.join(format!("{id}.result.json")), &result)?;
    Ok(result)
}

pub async fn cli(
    root: &Path,
    label: &str,
    credential: Option<&str>,
    args: &[&str],
) -> Result<std::process::Output> {
    let home = root.join("fixture-home");
    std::fs::create_dir_all(&home)?;
    let out = std::fs::File::create(root.join(format!("{label}.stdout")))?;
    let err = std::fs::File::create(root.join(format!("{label}.stderr")))?;
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"));
    command
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/bin:/bin")
        .env("AGENT_MAIL_STATE_DIR", root)
        .env("AGENT_MAIL_GROUP", GROUP)
        .env("DO_NOT_TRACK", "1")
        .env("OTEL_SDK_DISABLED", "true")
        .arg("--state-dir")
        .arg(root)
        .args(args)
        .current_dir(root)
        .stdin(std::process::Stdio::null())
        .stdout(out)
        .stderr(err)
        .kill_on_drop(true);
    if let Some(credential) = credential {
        command.env("AGENT_MAIL_SESSION", credential);
    }
    let mut child = command.spawn()?;
    let waited = tokio::time::timeout(Duration::from_secs(30), child.wait()).await;
    let status = match waited {
        Ok(result) => result?,
        Err(_) => {
            child.kill().await?;
            let status = child.wait().await?;
            write_json(
                &root.join(format!("{label}.exit.json")),
                &json!({"timeout":true,"code":status.code()}),
            )?;
            bail!("CLI {label} exceeded30s; original output retained");
        }
    };
    write_json(
        &root.join(format!("{label}.exit.json")),
        &json!({"timeout":false,"code":status.code(),"success":status.success(),"args":args}),
    )?;
    Ok(std::process::Output {
        status,
        stdout: std::fs::read(root.join(format!("{label}.stdout")))?,
        stderr: std::fs::read(root.join(format!("{label}.stderr")))?,
    })
}
