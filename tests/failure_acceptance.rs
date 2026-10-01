//! Bounded preliminary controls with exact root-selected run provenance.
//! Full acceptance stays incomplete while any required native/cut witness is missing.
#[path = "failure_acceptance/support.rs"]
mod support;

use agent_mail::{
    decision_recovery::{ExpectedPlan, Obligation, SourceCorrection},
    decision_supervisor::supervise_recovery_page,
    states::TaskState,
    store::{Publish, Store},
    task_graph::{
        Change, InputValidity, OutcomeChange, OutcomeKind, ParentLink, Phase, Requirement,
        validate_publication_inputs_tx,
    },
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};
use support::{
    Fixture, GROUP, NOW, SCOPE, Snapshot, candidate, decision, draft, string, write_json,
};

async fn select_candidate(
    f: &Fixture,
    task: &str,
    artifact: &str,
    key: &str,
    now: i64,
) -> Result<String> {
    let id = candidate(f, task, artifact, &format!("candidate:{key}"), now).await?;
    let view = f.store.task_inspect(&f.writer, task).await?;
    let mut accept = decision(view.work.version, &format!("accept:{key}"));
    accept.outcome = OutcomeChange::Success {
        kind: OutcomeKind::Accepted,
        candidate: id,
    };
    f.store
        .task_decide(&f.writer, task, accept, now + 1)
        .await?
        .model
        .context("contracted model")?
        .current_outcome
        .context("selected outcome")
}

fn requirement(task: &str, outcome: OutcomeKind) -> Requirement {
    Requirement {
        task: task.into(),
        outcome,
        revision: None,
    }
}

async fn dependency_and_reopen(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "dependency").await?;
    f.store
        .task_create(&f.writer, draft("producer")?, NOW)
        .await?;
    let mut consumer = draft("consumer")?;
    consumer
        .draft
        .requirements
        .push(requirement("producer", OutcomeKind::Accepted));
    f.store.task_create(&f.writer, consumer, NOW).await?;
    let before = f.store.execution_inspect(&f.writer, "consumer").await?;
    ensure!(
        before.budgets.iter().all(|b| b.anchor.is_none()),
        "unsatisfied consumer already anchored"
    );
    ensure!(
        !f.store
            .task_model_readiness(&f.writer, "consumer", Phase::Execute)
            .await?
            .causes
            .is_empty()
    );
    let first = select_candidate(&f, "producer", "same-artifact", "first", NOW + 1).await?;
    ensure!(
        f.store
            .task_model_readiness(&f.writer, "consumer", Phase::Execute)
            .await?
            .causes
            .is_empty()
    );
    let consumer = f.store.task_inspect(&f.writer, "consumer").await?;
    let captured = f
        .store
        .task_capture_inputs(&f.writer, "consumer", consumer.work.version, Phase::Execute)
        .await?;
    ensure!(captured.prerequisite_outcome_ids.get("producer") == Some(&first));
    let budget = f.store.execution_inspect(&f.writer, "consumer").await?;
    ensure!(
        budget
            .budgets
            .iter()
            .any(|b| b.task == "consumer" && b.anchor == Some(NOW + 2))
    );
    let lifetime_before = f.record("before-reopen", &["producer", "consumer"]).await?;
    let producer = f.store.task_inspect(&f.writer, "producer").await?;
    let mut reopen = decision(producer.work.version, "reopen-producer");
    reopen.outcome = OutcomeChange::Withdraw;
    reopen.work_patch.state = Some(TaskState::Ready);
    f.store
        .task_decide(&f.writer, "producer", reopen, NOW + 4)
        .await?;
    ensure!(f.store.task_input_validity(&f.writer, &captured).await? == InputValidity::Stale);
    let second = select_candidate(&f, "producer", "same-artifact", "second", NOW + 5).await?;
    ensure!(
        first != second,
        "same revision replaced immutable outcome identity"
    );
    ensure!(f.store.task_input_validity(&f.writer, &captured).await? == InputValidity::Stale);
    let final_budget = f.store.execution_inspect(&f.writer, "consumer").await?;
    ensure!(
        serde_json::to_value(&budget.budgets)? == serde_json::to_value(&final_budget.budgets)?,
        "reopen reset consumer budget"
    );
    let lifetime_after = f.record("final", &["producer", "consumer"]).await?;
    support::budget_continuity(&lifetime_before, &lifetime_after)?;
    Ok(json!({"first_outcome":first,"replacement_outcome":second,
        "assertions":["unpinned first satisfaction","immutable input invalidation","same revision distinct outcome","lifetime anchor retained"],
        "limit":"Public model transactions; no native continuation/admission race witnessed"}))
}

async fn outcome_predicates(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "predicates").await?;
    f.store
        .task_create(&f.writer, draft("producer")?, NOW)
        .await?;
    for (id, outcome) in [
        ("accept-only", OutcomeKind::Accepted),
        ("cancel-only", OutcomeKind::Cancelled),
        ("fail-only", OutcomeKind::Failed),
    ] {
        let mut child = draft(id)?;
        child
            .draft
            .requirements
            .push(requirement("producer", outcome));
        f.store.task_create(&f.writer, child, NOW).await?;
    }
    let mut cancel = decision(1, "cancel");
    cancel.outcome = OutcomeChange::Negative {
        kind: OutcomeKind::Cancelled,
        revision: "cancel-one".into(),
    };
    f.store
        .task_decide(&f.writer, "producer", cancel, NOW + 1)
        .await?;
    for (id, ready) in [
        ("accept-only", false),
        ("cancel-only", true),
        ("fail-only", false),
    ] {
        ensure!(
            f.store
                .task_model_readiness(&f.writer, id, Phase::Execute)
                .await?
                .causes
                .is_empty()
                == ready,
            "wrong outcome predicate for {id}"
        );
    }
    f.store
        .task_create(&f.writer, draft("parent")?, NOW + 2)
        .await?;
    for (id, required) in [("required-child", true), ("optional-child", false)] {
        let parent = f.store.task_inspect(&f.writer, "parent").await?;
        let mut child = draft(id)?;
        child.draft.parent = Some(ParentLink {
            task: "parent".into(),
            required,
            outcome: OutcomeKind::Accepted,
            revision: None,
        });
        child
            .expected_parent_versions
            .insert("parent".into(), parent.work.version);
        f.store.task_create(&f.writer, child, NOW + 3).await?;
    }
    let parent = f.store.task_inspect(&f.writer, "parent").await?;
    ensure!(
        f.store
            .task_capture_inputs(&f.writer, "parent", parent.work.version, Phase::Accept)
            .await
            .is_err()
    );
    let selected =
        select_candidate(&f, "required-child", "required-result", "required", NOW + 4).await?;
    let parent = f.store.task_inspect(&f.writer, "parent").await?;
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, "parent", parent.work.version, Phase::Accept)
        .await?;
    ensure!(
        inputs.required_child_outcome_ids == BTreeMap::from([("required-child".into(), selected)])
    );
    f.record(
        "final",
        &[
            "producer",
            "accept-only",
            "cancel-only",
            "fail-only",
            "parent",
            "required-child",
            "optional-child",
        ],
    )
    .await?;
    Ok(
        json!({"assertions":["negative predicates remain distinct","required child gates Accept","optional child omitted from Accept inputs"],"limit":"No held runtime cancellation or parent aggregate claim"}),
    )
}

async fn publication_model_guards(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "publication-model").await?;
    f.store
        .task_create(&f.writer, draft("report")?, NOW)
        .await?;
    let mut child = draft("child")?;
    child.draft.parent = Some(ParentLink {
        task: "report".into(),
        required: true,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child.expected_parent_versions.insert("report".into(), 1);
    f.store.task_create(&f.writer, child, NOW + 1).await?;
    let view = f.store.task_inspect(&f.writer, "report").await?;
    let original = f
        .store
        .task_capture_inputs(&f.writer, "report", view.work.version, Phase::Execute)
        .await?;
    let mut note = decision(view.work.version, "note");
    note.work_patch.evidence = Some(vec!["fixture:note-only".into()]);
    let noted = f
        .store
        .task_decide(&f.writer, "report", note, NOW + 2)
        .await?;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(f.root.join("mail.db"))
                .foreign_keys(true),
        )
        .await?;
    let before = f.record("before-validation", &["report", "child"]).await?;
    let mut tx = pool.begin().await?;
    let observation = validate_publication_inputs_tx(&mut tx, &original, SCOPE).await?;
    ensure!(observation.phase == Phase::Execute && observation.input_epoch == original.input_epoch);
    ensure!(observation.task_version == noted.work.version && observation.scope_unit == SCOPE);
    ensure!(
        validate_publication_inputs_tx(&mut tx, &original, "publish any artifact")
            .await
            .expect_err("exact scope must be required")
            .to_string()
            .contains("publication_scope_not_authorized")
    );
    let mut relabeled = original.clone();
    relabeled.phase = Phase::Accept;
    ensure!(
        validate_publication_inputs_tx(&mut tx, &relabeled, SCOPE)
            .await
            .is_err()
    );
    tx.rollback().await?;
    ensure!(before == f.record("after-validation", &["report", "child"]).await?);
    let mut review = decision(noted.work.version, "review");
    review.work_patch.state = Some(TaskState::Review);
    f.store
        .task_decide(&f.writer, "report", review, NOW + 3)
        .await?;
    ensure!(f.store.task_input_validity(&f.writer, &original).await? == InputValidity::Current);
    let mut tx = pool.begin().await?;
    ensure!(
        validate_publication_inputs_tx(&mut tx, &original, SCOPE)
            .await
            .expect_err("Current cannot override a phase hold")
            .to_string()
            .contains("publication_model_hold")
    );
    tx.rollback().await?;
    f.store.register(GROUP, "worker", true).await?;
    ensure!(f.store.task_input_validity(&f.writer, &original).await? == InputValidity::Stale);
    pool.close().await;
    f.record("final", &["report", "child"]).await?;
    Ok(
        json!({"assertions":["exact scope","original phase","note-only epoch","Current plus held lifecycle","rebind invalidation","read rollback"],
        "publication_protocol":"Mail1158 SQL selection and receipt commit atomically",
        "limit":"Only actual public model guard; no selection transaction or filesystem publication proof"}),
    )
}

async fn authority_and_budget(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "authority-budget").await?;
    let view = f.store.task_create(&f.writer, draft("task")?, NOW).await?;
    let initial = f.store.execution_inspect(&f.writer, "task").await?;
    let mut contract = view.model.context("model")?.contract;
    contract.budget.max_attempts = 6;
    contract.budget.max_elapsed_seconds = 480;
    let mut edit = decision(1, "budget-and-missing-mail");
    edit.contract = Change::Set(contract.clone());
    edit.resolve_message = Some(i64::MAX);
    let before = f.record("before-rollback", &["task"]).await?;
    ensure!(
        f.store
            .task_decide(&f.writer, "task", edit, NOW + 1)
            .await
            .is_err()
    );
    ensure!(
        before == f.record("after-rollback", &["task"]).await?,
        "partial budget/model write survived failure"
    );
    let mut edit = decision(1, "budget-authorized");
    edit.contract = Change::Set(contract);
    let updated = f
        .store
        .task_decide(&f.writer, "task", edit, NOW + 2)
        .await?;
    let changed = f.store.execution_inspect(&f.writer, "task").await?;
    ensure!(initial.budgets.len() == changed.budgets.len());
    for (old, new) in initial.budgets.iter().zip(&changed.budgets) {
        ensure!(
            (
                old.anchor,
                old.deadline,
                old.attempts_spent,
                old.attempts_reserved
            ) == (
                new.anchor,
                new.deadline,
                new.attempts_spent,
                new.attempts_reserved
            )
        );
    }
    let mut authorization = updated.model.context("model")?.authorization;
    authorization.state = agent_mail::task_graph::AuthorityState::Revoked;
    let mut revoke = decision(updated.work.version, "revoke");
    revoke.authorization = Change::Set(authorization);
    let revoked = f
        .store
        .task_decide(&f.writer, "task", revoke, NOW + 3)
        .await?;
    ensure!(
        f.store
            .task_capture_inputs(&f.writer, "task", revoked.work.version, Phase::Execute)
            .await
            .is_err()
    );
    let before = f.record("before-unavailable-repair", &["task"]).await?;
    f.store.execution_reconcile(GROUP, NOW + 4).await?;
    let after = f.record("after-unavailable-repair", &["task"]).await?;
    ensure!(
        before.rows("task_models")? == after.rows("task_models")?,
        "timer changed authorization"
    );
    ensure!(
        after.rows("execution_attempts")?.is_empty(),
        "UnavailableRuntime admitted work"
    );
    Ok(
        json!({"assertions":["business failure rolls back budget","anchor and spending retained","revocation blocks inputs","repair grants no authority"],"limit":"No aggregate sibling/runtime cost proof"}),
    )
}

async fn opposite_graph_edges(run: &Path) -> Result<Value> {
    let mut repetitions = Vec::new();
    for round in 0..3 {
        let f = Fixture::new(run, &format!("graph-{round}")).await?;
        for task in ["left", "right"] {
            f.store.task_create(&f.writer, draft(task)?, NOW).await?;
        }
        let mut left = decision(1, "left-waits-right");
        left.requirements = Change::Set(vec![requirement("right", OutcomeKind::Accepted)]);
        let mut right = decision(1, "right-waits-left");
        right.requirements = Change::Set(vec![requirement("left", OutcomeKind::Accepted)]);
        let (a, b) = tokio::join!(
            f.store.task_decide(&f.writer, "left", left, NOW + 1),
            f.store.task_decide(&f.writer, "right", right, NOW + 1),
        );
        ensure!(
            a.is_ok() != b.is_ok(),
            "opposite graph writes must serialize with one rejection"
        );
        let error = match (a, b) {
            (Err(e), _) | (_, Err(e)) => format!("{e:#}"),
            _ => unreachable!(),
        };
        ensure!(
            error.to_lowercase().contains("cycle"),
            "unexpected rejection: {error}"
        );
        let snapshot = f.record("final", &["left", "right"]).await?;
        ensure!(snapshot.rows("task_models")?.len() == 2);
        repetitions.push(json!({"round":round,"rejected":error}));
    }
    Ok(
        json!({"repetitions":repetitions,"limit":"Real dependency writer race; mixed action/continuation projection and admission races remain gaps"}),
    )
}

async fn tracking_and_fair_repair(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "tracking-201").await?;
    let ids: Vec<String> = (0..201).map(|i| format!("task-{i:03}")).collect();
    for id in &ids {
        f.store.task_create(&f.writer, draft(id)?, NOW).await?;
    }
    let mut cancel = decision(1, "terminal-visible");
    cancel.outcome = OutcomeChange::Negative {
        kind: OutcomeKind::Cancelled,
        revision: "cancelled".into(),
    };
    f.store
        .task_decide(&f.writer, &ids[0], cancel, NOW + 1)
        .await?;
    let assignment_ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let before = f.record("before-tracking", &assignment_ids).await?;
    let mut seen = BTreeSet::new();
    let mut cursor = String::new();
    let mut pages = 0;
    loop {
        let page = f.store.task_tracking_page(&f.writer, &cursor, 50).await?;
        pages += 1;
        ensure!(pages <= 5 && page.items.len() <= 50);
        for item in page.items {
            ensure!(
                seen.insert(item.work.id),
                "duplicate task across keyset pages"
            );
        }
        if !page.has_more {
            ensure!(page.next_cursor.is_none());
            break;
        }
        let next = page.next_cursor.context("continuation cursor")?;
        ensure!(next > cursor);
        cursor = next;
    }
    ensure!(seen == ids.iter().cloned().collect());
    ensure!(f.store.task_tracking_page(&f.writer, "", 0).await.is_err());
    ensure!(f.store.task_tracking_page(&f.writer, "", 51).await.is_err());
    ensure!(
        before == f.record("after-tracking", &assignment_ids).await?,
        "tracking mutated persisted state"
    );
    let mut repaired = BTreeSet::new();
    let mut page_sizes = Vec::new();
    for tick in 0..6 {
        let page = f.store.execution_reconcile(GROUP, NOW + 2 + tick).await?;
        ensure!(page.tasks.len() <= 200);
        page_sizes.push(page.tasks.len());
        repaired.extend(page.tasks);
    }
    ensure!(
        repaired == ids.iter().cloned().collect(),
        "fair repair missed an assignment"
    );
    let final_snapshot = f.record("final", &assignment_ids).await?;
    ensure!(final_snapshot.rows("execution_attempts")?.is_empty());
    f.store.execution_reconcile(GROUP, NOW + 6).await?;
    let backward = f.record("backward-clock", &assignment_ids).await?;
    ensure!(
        backward
            .rows("execution_clock")?
            .iter()
            .any(|row| row["discontinuity"] == 1 && row["observed"] == NOW + 7)
    );
    f.store.execution_reconcile(GROUP, NOW + 500).await?;
    let forward = f.record("forward-clock", &assignment_ids).await?;
    ensure!(
        forward
            .rows("execution_clock")?
            .iter()
            .any(|row| row["discontinuity"] == 1 && row["observed"] == NOW + 500)
    );
    support::budget_continuity(&final_snapshot, &forward)?;
    ensure!(forward.rows("execution_attempts")?.is_empty());
    Ok(
        json!({"tasks":201,"tracking_pages":pages,"repair_page_sizes":page_sizes,
        "assertions":["terminal included","authenticated bounded pages","tracking creates no receipts/events/repair","persisted fair scan visits all201","backward-clock hold persists through forward jump","clock jumps preserve budget lifetime"],
        "limit":"Logical pages with UnavailableRuntime; no25s wall-time claim, failing endpoint, or native admission proof"}),
    )
}

async fn publish(f: &Fixture, key: &str) -> Result<i64> {
    f.store
        .publish(
            &f.writer,
            Publish {
                recipients: vec!["worker".into()],
                key: key.into(),
                summary: "Finite review".into(),
                body: "Review isolated acceptance evidence".into(),
                due_after: None,
                reply_to: None,
                work_id: None,
            },
            NOW,
        )
        .await
}

async fn recovery_and_restart(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "recovery-restart").await?;
    let mut sources = Vec::new();
    for index in 0..5 {
        sources.push(Obligation::Delivery {
            message: publish(&f, &format!("review-{index}")).await?,
            recipient: "worker".into(),
        });
    }
    let before = f.record("before-inspection", &[]).await?;
    let view = f
        .store
        .inspect_obligation(&f.writer, sources[0].clone())
        .await?;
    ensure!(view.unresolved && view.authority == "writer");
    ensure!(before == f.record("after-inspection", &[]).await?);
    ensure!(
        f.store
            .recover_expired_obligation(
                &f.worker,
                "wrong-authority",
                sources[0].clone(),
                NOW + 4000
            )
            .await
            .is_err()
    );
    ensure!(before == f.record("after-refusal", &[]).await?);
    let (a, b) = tokio::join!(
        f.store
            .recover_expired_obligation(&f.writer, "recover-a", sources[0].clone(), NOW + 4000),
        f.store
            .recover_expired_obligation(&f.writer, "recover-b", sources[0].clone(), NOW + 4000),
    );
    let a = a?;
    let b = b?;
    ensure!(a.id == b.id && a.operator_obligation == b.operator_obligation);
    ensure!(
        a.state == "operator_required" && a.decision_task.is_none() && a.capability_hold.is_some()
    );
    for tick in 0..8 {
        supervise_recovery_page(&f.store, GROUP, NOW + 4001 + tick, 2).await?;
    }
    let previous = f.record("before-restart", &[]).await?;
    ensure!(
        previous.rows("decision_cases")?.len() == 5
            && previous.rows("operator_obligations")?.len() == 5
    );
    ensure!(
        previous.rows("work_items")?.is_empty(),
        "missing policy funded a recursive task"
    );
    ensure!(
        previous
            .rows("followups")?
            .iter()
            .all(|r| r["retrieved_at"].is_null())
    );
    let Fixture {
        root,
        store,
        writer,
        worker: _,
    } = f;
    store.close().await;
    let reopened = Store::open(&root, false).await?;
    ensure!(
        previous == Snapshot::read(&root).await?,
        "reopen changed durable state"
    );
    let same = reopened
        .recover_expired_obligation(&writer, "after-restart", sources[0].clone(), NOW + 4010)
        .await?;
    ensure!(
        same.id == a.id
            && same.operator_obligation == a.operator_obligation
            && same.hard_due == a.hard_due
    );
    supervise_recovery_page(&reopened, GROUP, NOW + 4011, 2).await?;
    let after = Snapshot::read(&root).await?;
    write_json(&root.join("after-restart.snapshot.json"), &after)?;
    ensure!(
        after.rows("decision_cases")?.len() == 5 && after.rows("operator_obligations")?.len() == 5
    );
    after.inventory(&[])?;
    reopened.close().await;
    Ok(
        json!({"cases":5,"operators":5,"replayed_case":a.id,"hard_due":a.hard_due,
        "assertions":["concurrent coalescing","wrong authority rollback","read has no receipt","bounded pages","restart retains one finite episode"],
        "limit":"Actual recovery API/store restart; no killed native service or independently running supervisor witness"}),
    )
}

async fn correction_without_occurrence(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "source-correction").await?;
    f.store.register(GROUP, "other", false).await?;
    let message = f
        .store
        .publish(
            &f.writer,
            Publish {
                recipients: vec!["worker".into(), "other".into()],
                key: "expired-source".into(),
                summary: "Review two deliveries".into(),
                body: "Correct only the explicitly selected delivery".into(),
                due_after: None,
                reply_to: None,
                work_id: None,
            },
            NOW,
        )
        .await?;
    let source = Obligation::Delivery {
        message,
        recipient: "worker".into(),
    };
    let other_source = Obligation::Delivery {
        message,
        recipient: "other".into(),
    };
    let other_before = f
        .store
        .inspect_obligation(&f.writer, other_source.clone())
        .await?;
    let view = f
        .store
        .inspect_obligation(&f.writer, source.clone())
        .await?;
    let plan = view.plan.context("default finite plan")?;
    let before = f.record("before-correction", &[]).await?;
    ensure!(
        before.rows("decision_cases")?.is_empty()
            && before.rows("operator_obligations")?.is_empty()
    );
    ensure!(before.rows("attention_occurrences")?.is_empty());
    let request = SourceCorrection {
        key: "sender-correction".into(),
        source: source.clone(),
        expected_plan: ExpectedPlan::Present {
            id: plan.id,
            version: plan.version,
        },
        case_versions: BTreeMap::new(),
        reason: "Explicit original sender correction".into(),
        evidence: vec!["fixture:expired-boundary".into()],
        next_step: "Review evidence".into(),
        next_check_at: NOW + 4010,
        escalation_at: NOW + 4100,
    };
    ensure!(
        f.store
            .correct_obligation(&f.worker, request.clone(), NOW + 4000)
            .await
            .is_err()
    );
    ensure!(before == f.record("after-wrong-recipient", &[]).await?);
    let result = f
        .store
        .correct_obligation(&f.writer, request.clone(), NOW + 4000)
        .await?;
    let corrected = f.store.inspect_obligation(&f.writer, source).await?;
    ensure!(corrected.unresolved && corrected.business_deadline == view.business_deadline);
    let corrected_plan = corrected.plan.context("corrected plan")?;
    ensure!(corrected_plan.next_check == NOW + 4010 && corrected_plan.escalate_at == NOW + 4100);
    ensure!(
        other_before == f.store.inspect_obligation(&f.writer, other_source).await?,
        "correction changed the unselected recipient"
    );
    let after = f.record("after-correction", &[]).await?;
    ensure!(after.rows("decision_cases")?.is_empty());
    ensure!(after.rows("deliveries")? == before.rows("deliveries")?);
    ensure!(
        after
            .rows("followups")?
            .iter()
            .all(|r| r["retrieved_at"].is_null())
    );
    ensure!(
        f.store
            .correct_obligation(&f.writer, request.clone(), NOW + 4001)
            .await?
            == result
    );
    let mut stale = request;
    stale.key = "stale-plan".into();
    ensure!(
        f.store
            .correct_obligation(&f.writer, stale, NOW + 4001)
            .await
            .is_err()
    );
    ensure!(after == f.record("after-replay-and-stale", &[]).await?);
    Ok(
        json!({"message":message,"assertions":["original sender correction without occurrence","explicit recipient leaves other delivery unchanged","recipient refused","exact retry","stale CAS rollback","no resolve or retrieval"],"limit":"Public source API; multi-recipient CLI and absent-plan variants remain gaps"}),
    )
}

async fn public_cli(run: &Path) -> Result<Value> {
    let root = run.join("public-cli");
    std::fs::create_dir(&root)?;
    let help = support::cli(&root, "help", None, &["--help"]).await?;
    ensure!(
        help.status.success() && !root.join("mail.db").exists(),
        "help mutated state"
    );
    ensure!(!String::from_utf8_lossy(&help.stdout).contains("__managed-native-worker"));
    ensure!(
        support::cli(&root, "init", None, &["init", GROUP])
            .await?
            .status
            .success()
    );
    let registration = support::cli(
        &root,
        "writer",
        None,
        &["agent", "add", "writer", "--show-session"],
    )
    .await?;
    ensure!(registration.status.success());
    let registration: Value = serde_json::from_slice(&registration.stdout)?;
    let credential = registration["session"]
        .as_str()
        .context("fixture credential")?;
    let session = uuid::Uuid::parse_str(credential).context("parse returned fixture credential")?;
    ensure!(
        support::cli(&root, "worker", None, &["agent", "add", "worker"])
            .await?
            .status
            .success()
    );
    let before = Snapshot::read(&root).await?;
    let invalid = support::cli(
        &root,
        "incomplete",
        Some(credential),
        &[
            "task",
            "create",
            "review",
            "Review artifact",
            "--owner",
            "worker",
            "--key",
            "create-one",
            "--reason",
            "Explicit assignment",
        ],
    )
    .await?;
    ensure!(invalid.status.code() == Some(1) && invalid.stdout.is_empty());
    ensure!(String::from_utf8_lossy(&invalid.stderr).contains("incomplete_contract"));
    ensure!(
        before == Snapshot::read(&root).await?,
        "invalid create persisted a partial record"
    );
    let mut args = vec![
        "task",
        "create",
        "review",
        "Review artifact",
        "--owner",
        "worker",
        "--key",
        "create-one",
        "--reason",
        "Explicit assignment",
        "--criterion",
        "readable=Artifact is readable",
        "--allow",
        SCOPE,
        "--authorize",
        "fixture/approval",
        "--max-attempts",
        "3",
        "--max-elapsed",
        "4m",
    ];
    let no_consent = support::cli(&root, "missing-consent", Some(credential), &args).await?;
    ensure!(no_consent.status.code() == Some(1) && no_consent.stdout.is_empty());
    ensure!(
        String::from_utf8_lossy(&no_consent.stderr).contains("input_invalidation_consent_required")
    );
    ensure!(
        before == Snapshot::read(&root).await?,
        "missing consent persisted a partial record"
    );
    args.push("--allow-input-invalidation");
    let created = support::cli(&root, "create", Some(credential), &args).await?;
    ensure!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let created_json: Value = serde_json::from_slice(&created.stdout)?;
    ensure!(created_json["work"]["version"] == 1 && created_json["model"].is_object());
    let replay = support::cli(&root, "replay", Some(credential), &args).await?;
    ensure!(
        replay.status.success() && serde_json::from_slice::<Value>(&replay.stdout)? == created_json
    );
    let inspect = support::cli(
        &root,
        "inspect",
        Some(credential),
        &["task", "inspect", "review"],
    )
    .await?;
    ensure!(inspect.status.success());
    ensure!(serde_json::from_slice::<Value>(&inspect.stdout)?["execution"]["revision"].is_number());
    let legacy = support::cli(
        &root,
        "legacy",
        Some(credential),
        &[
            "task",
            "create",
            "legacy",
            "Legacy record",
            "--owner",
            "worker",
            "--untracked",
        ],
    )
    .await?;
    ensure!(legacy.status.success());
    let store = Store::open(&root, false).await?;
    let writer = store.authenticate(GROUP, Some(&session)).await?;
    let page = store.task_tracking_page(&writer, "", 50).await?;
    ensure!(page.items.len() == 2);
    let legacy = page
        .items
        .iter()
        .find(|i| i.work.id == "legacy")
        .context("legacy in tracking")?;
    ensure!(legacy.model.is_none() && legacy.execution_hold == "legacy_untracked");
    store.close().await;
    let snapshot = Snapshot::read(&root).await?;
    snapshot.inventory(&["review", "legacy"])?;
    write_json(&root.join("final.snapshot.json"), &snapshot)?;
    Ok(
        json!({"assertions":["help state-free","incomplete contract rollback","explicit input-invalidation consent refusal and rollback","actual create/replay/inspect with explicit consent","explicit legacy visible"],
        "limit":"Real isolated CLI process; no managed registration/configuration or native session claim"}),
    )
}

async fn oracle_sensitivity(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "oracle-sensitivity").await?;
    f.store
        .task_create(&f.writer, draft("terminal")?, NOW)
        .await?;
    let mut cancel = decision(1, "cancel");
    cancel.outcome = OutcomeChange::Negative {
        kind: OutcomeKind::Cancelled,
        revision: "cancelled".into(),
    };
    f.store
        .task_decide(&f.writer, "terminal", cancel, NOW + 1)
        .await?;
    let genuine = f.record("genuine", &["terminal"]).await?;
    let mut synthetic = genuine.clone();
    synthetic
        .tables
        .get_mut("execution_attempts")
        .context("attempts")?
        .push(json!({"id":"synthetic-old","task":"terminal","holds_slot":1}));
    synthetic
        .tables
        .get_mut("execution_slots")
        .context("slots")?
        .push(json!({"attempt":"synthetic-old"}));
    synthetic.tables.get_mut("runtime_effects").context("effects")?.push(json!({"attempt":"synthetic-old","effect":"synthetic-unknown","retention_pin":1,"state":"pinned"}));
    synthetic
        .tables
        .get_mut("execution_causes")
        .context("causes")?
        .push(json!({"id":"synthetic-cause","settled":0}));
    synthetic
        .tables
        .get_mut("decision_cases")
        .context("cases")?
        .push(json!({"id":999,"state":"operator_required"}));
    synthetic
        .tables
        .get_mut("operator_obligations")
        .context("operators")?
        .push(json!({"id":999,"case_id":999,"state":"escalated"}));
    let inventory = synthetic.inventory(&["terminal"])?;
    for required in [
        "attempt:synthetic-old:terminal",
        "effect:synthetic-old:synthetic-unknown",
        "cause:synthetic-cause",
        "decision:999",
        "operator:999",
    ] {
        ensure!(
            inventory.contains(required),
            "terminal obligation omitted: {required}"
        );
    }
    let mut omitted = synthetic.clone();
    omitted
        .tables
        .get_mut("work_items")
        .context("tasks")?
        .clear();
    ensure!(omitted.inventory(&["terminal"]).is_err());
    let mut overlapping = synthetic.clone();
    overlapping
        .tables
        .get_mut("execution_attempts")
        .context("attempts")?
        .push(json!({"id":"synthetic-new","task":"terminal","holds_slot":1}));
    ensure!(overlapping.inventory(&["terminal"]).is_err());
    let mut lost_slot = synthetic.clone();
    lost_slot
        .tables
        .get_mut("execution_slots")
        .context("slots")?
        .clear();
    ensure!(lost_slot.inventory(&["terminal"]).is_err());
    let mut lost_pin = synthetic.clone();
    lost_pin
        .tables
        .get_mut("runtime_effects")
        .context("effects")?[0]["retention_pin"] = json!(0);
    ensure!(lost_pin.inventory(&["terminal"]).is_err());
    let mut lost_case = synthetic.clone();
    lost_case
        .tables
        .get_mut("decision_cases")
        .context("cases")?
        .clear();
    ensure!(lost_case.inventory(&["terminal"]).is_err());
    let mut reset_budget = synthetic.clone();
    reset_budget
        .tables
        .get_mut("execution_budgets")
        .context("budgets")?[0]["anchor"] = Value::Null;
    ensure!(support::budget_continuity(&synthetic, &reset_budget).is_err());
    ensure!(
        genuine == Snapshot::read(&f.root).await?,
        "synthetic controls touched live SQL"
    );
    write_json(&f.root.join("synthetic.snapshot.json"), &synthetic)?;
    Ok(
        json!({"evidence":"synthetic in-memory oracle sensitivity only","detected":["omitted assignment","overlapping admission inventory","lost slot","lost retention pin","lost decision source","reset lifetime anchor"],"terminal_obligations":inventory}),
    )
}

async fn absent_managed_capability(run: &Path) -> Result<Value> {
    let f = Fixture::new(run, "managed-capability").await?;
    let before = f.record("before-query", &[]).await?;
    // Selector is the registration identity supplied by the caller. No display-name fallback.
    ensure!(
        f.store
            .managed_target_capabilities(&f.writer, "missing-registration", NOW)
            .await?
            .is_none()
    );
    ensure!(
        before == f.record("after-query", &[]).await?,
        "capability read registered or probed a target"
    );
    let old_writer = f.writer.clone();
    f.store.register(GROUP, "writer", true).await?;
    let before_stale = Snapshot::read(&f.root).await?;
    ensure!(
        f.store
            .managed_target_capabilities(&old_writer, "missing-registration", NOW + 1)
            .await
            .is_err()
    );
    ensure!(before_stale == Snapshot::read(&f.root).await?);
    Ok(
        json!({"assertions":["absent registration is None","query writes no capability or probe","stale real binding rejected"],
        "limit":"Actual R7 public query; no configured target, native capability, or qualification proof"}),
    )
}

#[tokio::test]
async fn finite_failure_acceptance_controls() -> Result<()> {
    ensure!(
        cfg!(debug_assertions),
        "acceptance requires debug assertions"
    );
    ensure!(
        cfg!(target_os = "linux"),
        "run only on the root-granted isolated Linux runner"
    );
    ensure!(
        std::env::var("AGENT_MAIL_ACCEPTANCE_GRANT").is_ok(),
        "named grant required"
    );
    let root = PathBuf::from(std::env::var("AGENT_MAIL_STATE_DIR")?);
    ensure!(
        root.starts_with(support::STATE_ROOT) && root != Path::new(support::STATE_ROOT),
        "isolated run state required"
    );
    ensure!(
        root.is_dir() && root.canonicalize()?.starts_with(support::STATE_ROOT),
        "state path escapes isolated root"
    );
    for key in [
        "CODEX_SESSION_ID",
        "CODEX_THREAD_ID",
        "HERDR_PANE_ID",
        "HERDR_TAB_ID",
        "HERDR_WORKSPACE_ID",
        "AGENT_MAIL_SESSION",
    ] {
        ensure!(
            std::env::var_os(key).is_none(),
            "inherited identity {key} must be cleared by remote wrapper"
        );
    }
    let provenance = support::run_provenance(&root)?;
    let mut results = Vec::new();
    macro_rules! check {
        ($id:literal, $class:literal, $future:expr) => {
            results.push(support::capture(&root, $id, $class, $future).await?);
        };
    }
    check!(
        "model_dependency",
        "public_model",
        dependency_and_reopen(&root)
    );
    check!(
        "model_predicates",
        "public_model",
        outcome_predicates(&root)
    );
    check!(
        "publication_model_guards",
        "public_model",
        publication_model_guards(&root)
    );
    check!(
        "authority_budget",
        "public_model",
        authority_and_budget(&root)
    );
    check!(
        "graph_opposite_edges",
        "public_model_concurrency",
        opposite_graph_edges(&root)
    );
    check!(
        "tracking_fair_repair",
        "public_model_and_unavailable_runtime",
        tracking_and_fair_repair(&root)
    );
    check!(
        "recovery_restart",
        "public_recovery",
        recovery_and_restart(&root)
    );
    check!(
        "source_correction",
        "public_recovery",
        correction_without_occurrence(&root)
    );
    check!("public_cli", "isolated_product_process", public_cli(&root));
    check!(
        "oracle_sensitivity",
        "synthetic_oracle_only",
        oracle_sensitivity(&root)
    );
    check!(
        "managed_capability_read",
        "public_runtime_read",
        absent_managed_capability(&root)
    );
    let manifest: Value = serde_json::from_str(include_str!("failure_acceptance/scenarios.json"))?;
    let mut scenarios = manifest["scenarios"]
        .as_array()
        .context("scenario inventory")?
        .clone();
    ensure!(
        scenarios.len() == 20
            && manifest["crash_cuts"]
                .as_array()
                .context("crash inventory")?
                .len()
                == 12
    );
    ensure!(
        manifest["native_rows"]
            .as_array()
            .context("native inventory")?
            .len()
            == 5
    );
    let mut seen = BTreeSet::new();
    for row in &mut scenarios {
        let id = string(row, "id")?.to_owned();
        ensure!(seen.insert(id), "duplicate scenario");
        let checks = row["checks"].as_array().context("scenario checks")?;
        let mut failed = false;
        for check in checks {
            let id = check.as_str().context("check ID")?;
            let outcome = results
                .iter()
                .find(|r| r.id == id)
                .context("unexecuted named check")?;
            failed |= outcome.verdict != "pass";
        }
        ensure!(
            !row["remaining"]
                .as_array()
                .context("remaining gates")?
                .is_empty(),
            "preliminary suite must retain unresolved gates"
        );
        row["verdict"] = json!(if failed { "fail" } else { "capability_gap" });
    }
    let failures: Vec<&str> = results
        .iter()
        .filter(|r| r.verdict != "pass")
        .map(|r| r.id.as_str())
        .collect();
    let report = json!({
        "schema_version":2,"source_profile":provenance["source_profile"],"suite":"failure-acceptance-public-controls-v1",
        "run_provenance":provenance,"design_basis":manifest["design_basis"],
        "public_controls":{"verdict":if failures.is_empty() {"pass"} else {"fail"},"executed_checks":results.len()},
        "full_acceptance":"incomplete","checks":results,"scenarios":scenarios,
        "crash_cuts":manifest["crash_cuts"],"native_rows":manifest["native_rows"],
        "timing":manifest["timing"],"failed_checks":failures,
        "interpretation":"Green public controls cannot discharge missing real native, fault-cut, baseline or final-source gates"
    });
    write_json(&root.join("acceptance-results.json"), &report)?;
    ensure!(
        failures.is_empty(),
        "public controls failed: {failures:?}; original artifacts retained at {}",
        root.display()
    );
    Ok(())
}
