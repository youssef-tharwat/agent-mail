use super::*;
use crate::execution_driver::Controller;

use super::bridge_predecessor_controls::fail_actual_visit;

async fn ordinary_projection_refuses(store: &Store, now: i64) -> Result<()> {
    let mut tx = store.pool().begin().await?;
    assert!(
        project_notice_page_tx(&mut tx, "g", now, 100)
            .await
            .is_err(),
        "the ordinary poison must actually be exercised"
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn genuine_failure_survives_ordinary_poison_in_all_three_notice_phases() -> Result<()> {
    let (_root, store, now, _message) = obligation_fixture().await?;
    let controller = Controller::acquire(&store, crate::now()?).await?;
    let now = now.max(fail_actual_visit(&store, &controller, "g").await?);
    // Valid JSON with the wrong owner evidence shape: a negative graph fixture.
    sqlx::query("UPDATE operator_obligations SET evidence='{}' WHERE group_name='g'")
        .execute(store.pool())
        .await?;
    ordinary_projection_refuses(&store, now).await?;
    let mut select = store.pool().begin().await?;
    assert_eq!(
        reserve_dispatch_turn_tx(&mut select, now).await?,
        Some(("g".into(), NoticeClass::Infrastructure))
    );
    select.commit().await?;
    let mut tx = store.pool().begin().await?;
    project_infrastructure_notice_page_tx(&mut tx, "g", now, 100).await?;
    let batch =
        reserve_notice_batch_for_class_tx(&mut tx, "g", "bridge", now, NoticeClass::Infrastructure)
            .await?
            .context("genuine infrastructure reservation")?;
    assert_eq!(batch.class, NoticeClass::Infrastructure);
    let payload: Value = serde_json::from_str(&batch.payload)?;
    assert_eq!(payload["items"].as_array().context("items")?.len(), 1);
    assert_eq!(payload["items"][0]["source"]["kind"], "supervisor_failure");
    assert_eq!(
        payload["items"][0]["responsibility"]["kind"],
        "home_operator"
    );
    assert!(batch.payload.len() <= MAX_BYTES);
    let initial = notice_readback_tx(&mut tx, "g", 0, 100)
        .await?
        .into_iter()
        .find(|n| matches!(n.source, NoticeSource::SupervisorFailure(_)))
        .context("original infrastructure projection")?;
    assert_eq!(initial.exposures, 0);
    tx.commit().await?;

    ordinary_projection_refuses(&store, now).await?;
    let mut tx = store.pool().begin().await?;
    assert!(
        expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "bridge", now)
            .await?
            .is_some()
    );
    assert!(
        expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "bridge", now)
            .await?
            .is_none()
    );
    tx.commit().await?;
    ordinary_projection_refuses(&store, now + 1).await?;
    let mut tx = store.pool().begin().await?;
    finish_operator_notice_batch_tx(
        &mut tx,
        "g",
        &batch.id,
        "bridge",
        TransportResult::Failed,
        "control: supported failure before sender creation",
        now + 1,
    )
    .await?;
    let after = notice_readback_tx(&mut tx, "g", initial.id - 1, 1)
        .await?
        .remove(0);
    assert_eq!(after.account, initial.account);
    assert_eq!(after.episode, initial.episode);
    assert_eq!(
        after.source_snapshot["due_at"],
        initial.source_snapshot["due_at"]
    );
    assert_eq!(after.exposures, 1);
    assert!(after.unresolved);
    assert_eq!(after.accepted_revision, 0);
    assert!(
        reserve_notice_batch_for_class_tx(
            &mut tx,
            "g",
            "bridge",
            now + 299,
            NoticeClass::Infrastructure
        )
        .await?
        .is_none(),
        "original cooldown retained"
    );
    tx.commit().await?;

    for attempt in 1..3 {
        let at = now + attempt * 301;
        ordinary_projection_refuses(&store, at).await?;
        let mut tx = store.pool().begin().await?;
        let retry = reserve_notice_batch_for_class_tx(
            &mut tx,
            "g",
            "bridge",
            at,
            NoticeClass::Infrastructure,
        )
        .await?
        .context("remaining finite exposure")?;
        expose_operator_notice_batch_tx(&mut tx, "g", &retry.id, "bridge", at)
            .await?
            .context("exposure")?;
        finish_operator_notice_batch_tx(
            &mut tx,
            "g",
            &retry.id,
            "bridge",
            TransportResult::Failed,
            "control: supported failure before sender creation",
            at + 1,
        )
        .await?;
        tx.commit().await?;
    }
    let mut tx = store.pool().begin().await?;
    assert!(
        reserve_notice_batch_for_class_tx(
            &mut tx,
            "g",
            "bridge",
            now + 2000,
            NoticeClass::Infrastructure
        )
        .await?
        .is_none(),
        "another page never renews the three-exposure account"
    );
    let after = notice_readback_tx(&mut tx, "g", initial.id - 1, 1)
        .await?
        .remove(0);
    assert_eq!((after.account, after.exposures), (initial.account, 3));
    tx.rollback().await?;
    controller
        .finish(&store, crate::now()?, "bridge control joined")
        .await?;
    Ok(())
}

#[tokio::test]
async fn class_identity_and_mixed_members_refuse_before_spending_or_completion() -> Result<()> {
    let (_root, store, now) = fixture(1).await?;
    let controller = Controller::acquire(&store, crate::now()?).await?;
    let now = now.max(fail_actual_visit(&store, &controller, "g").await?);
    let mut tx = store.pool().begin().await?;
    project_infrastructure_notice_page_tx(&mut tx, "g", now, 100).await?;
    let batch =
        reserve_notice_batch_for_class_tx(&mut tx, "g", "bridge", now, NoticeClass::Infrastructure)
            .await?
            .context("batch")?;
    tx.commit().await?;
    let mut tx = store.pool().begin().await?;
    assert!(
        sqlx::query("UPDATE operator_notice_batches SET notice_class='ordinary' WHERE id=?")
            .bind(&batch.id)
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await?;
    let mut tx = store.pool().begin().await?;
    // Negative corruption of a real reservation, not an invented authority proof.
    sqlx::query("INSERT INTO operator_notice_batch_items(batch,notice,revision,account,source_snapshot) SELECT ?,id,revision,account,source_snapshot FROM operator_notices WHERE source_kind='attention_occurrence' LIMIT 1")
        .bind(&batch.id).execute(&mut *tx).await?;
    let error = expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "bridge", now)
        .await
        .expect_err("mixed class refuses");
    assert!(error.to_string().contains("notice batch class mismatch"));
    tx.rollback().await?;
    let mut tx = store.pool().begin().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(exposures) FROM operator_notice_spending")
            .fetch_one(&mut *tx)
            .await?,
        0
    );
    expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "bridge", now)
        .await?
        .context("real exposure")?;
    tx.commit().await?;
    let mut tx = store.pool().begin().await?;
    sqlx::query("INSERT INTO operator_notice_batch_items(batch,notice,revision,account,source_snapshot) SELECT ?,id,revision,account,source_snapshot FROM operator_notices WHERE source_kind='attention_occurrence' LIMIT 1")
        .bind(&batch.id).execute(&mut *tx).await?;
    assert!(
        finish_operator_notice_batch_tx(
            &mut tx,
            "g",
            &batch.id,
            "bridge",
            TransportResult::Accepted,
            "negative mixed control",
            now + 1
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    let state: String = sqlx::query_scalar("SELECT state FROM operator_notice_batches WHERE id=?")
        .bind(&batch.id)
        .fetch_one(store.pool())
        .await?;
    assert_eq!(
        state, "exposed",
        "invalid completion cannot become acceptance"
    );
    controller
        .finish(&store, crate::now()?, "bridge control joined")
        .await?;
    Ok(())
}

#[tokio::test]
async fn per_group_class_rotation_and_empty_bridge_cadence_survive_real_restart() -> Result<()> {
    let (root, store, now) = fixture(0).await?;
    store.enroll("h", None).await?;
    for expected in ["g", "h", "g", "h"] {
        let mut tx = store.pool().begin().await?;
        assert_eq!(
            reserve_dispatch_turn_tx(&mut tx, now).await?,
            Some((expected.into(), NoticeClass::Ordinary))
        );
        tx.commit().await?;
    }
    let controller = Controller::acquire(&store, crate::now()?).await?;
    let deadline_g = fail_actual_visit(&store, &controller, "g").await?;
    let deadline_h = fail_actual_visit(&store, &controller, "h").await?;
    let now = now.max(deadline_g).max(deadline_h);
    for expected in ["g", "h"] {
        let mut tx = store.pool().begin().await?;
        assert_eq!(
            reserve_dispatch_turn_tx(&mut tx, now).await?,
            Some((expected.into(), NoticeClass::Infrastructure))
        );
        tx.commit().await?;
    }
    controller
        .finish(&store, crate::now()?, "restart control")
        .await?;
    drop(controller);
    store.close().await;
    let store = Store::open(root.path(), true).await?;
    for class in [NoticeClass::Ordinary, NoticeClass::Infrastructure] {
        for expected in ["g", "h"] {
            let mut tx = store.pool().begin().await?;
            assert_eq!(
                reserve_dispatch_turn_tx(&mut tx, now).await?,
                Some((expected.into(), class))
            );
            tx.commit().await?;
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_attempts")
            .fetch_one(store.pool())
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn original_unknown_sender_blocks_repaired_route_after_real_restart() -> Result<()> {
    let (root, store, now) = fixture(0).await?;
    let controller = Controller::acquire(&store, crate::now()?).await?;
    let now = now.max(fail_actual_visit(&store, &controller, "g").await?);
    let mut tx = store.pool().begin().await?;
    project_infrastructure_notice_page_tx(&mut tx, "g", now, 100).await?;
    let batch =
        reserve_notice_batch_for_class_tx(&mut tx, "g", "bridge", now, NoticeClass::Infrastructure)
            .await?
            .context("batch")?;
    expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "bridge", now)
        .await?
        .context("exposure")?;
    finish_operator_notice_batch_tx(
        &mut tx,
        "g",
        &batch.id,
        "bridge",
        TransportResult::Uncertain,
        "control: sender has no supported closure",
        now + 1,
    )
    .await?;
    let original = notice_readback_tx(&mut tx, "g", 0, 1).await?.remove(0);
    assert_eq!(
        repair_operator_route_tx(
            &mut tx,
            "g",
            batch.generation,
            "same",
            "same route",
            now + 2
        )
        .await?,
        batch.generation
    );
    sqlx::query("UPDATE followup_policy SET notifier='[\"/usr/bin/true\"]' WHERE group_name='g'")
        .execute(&mut *tx)
        .await?;
    assert_eq!(
        repair_operator_route_tx(
            &mut tx,
            "g",
            batch.generation,
            "changed",
            "changed route",
            now + 3
        )
        .await?,
        batch.generation + 1
    );
    tx.commit().await?;
    controller
        .finish(&store, crate::now()?, "restart control")
        .await?;
    drop(controller);
    store.close().await;
    let store = Store::open(root.path(), true).await?;
    let mut tx = store.pool().begin().await?;
    project_infrastructure_notice_page_tx(&mut tx, "g", now + 2000, 100).await?;
    assert!(
        reserve_notice_batch_for_class_tx(
            &mut tx,
            "g",
            "other",
            now + 2000,
            NoticeClass::Infrastructure
        )
        .await?
        .is_none(),
        "restart, route generation and elapsed lease do not prove closure"
    );
    let after = notice_readback_tx(&mut tx, "g", 0, 1).await?.remove(0);
    assert_eq!(after.account, original.account);
    assert_eq!(after.episode, original.episode);
    assert_eq!(
        after.source_snapshot["due_at"],
        original.source_snapshot["due_at"]
    );
    assert_eq!(after.outstanding_batch.as_deref(), Some(batch.id.as_str()));
    assert!(after.unresolved);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT exposures FROM operator_notice_spending WHERE account=? AND generation=?"
        )
        .bind(original.account)
        .bind(batch.generation)
        .fetch_one(&mut *tx)
        .await?,
        1
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn home_operator_display_never_grants_mailbox_visibility() -> Result<()> {
    let (_root, store, now) = fixture(0).await?;
    let controller = Controller::acquire(&store, crate::now()?).await?;
    let now = now.max(fail_actual_visit(&store, &controller, "g").await?);
    let mut tx = store.pool().begin().await?;
    project_infrastructure_notice_page_tx(&mut tx, "g", now, 100).await?;
    // A hostile display collision must not become mailbox authority.
    sqlx::query(
        "UPDATE operator_notices SET responsible='worker' WHERE source_kind='supervisor_failure'",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    let worker = store.mailbox("g", "worker").await?;
    let mut tx = store.pool().begin().await?;
    assert!(
        status_notices_tx(&mut tx, Some("g"), Some(worker.id), now)
            .await?
            .0
            .is_empty()
    );
    tx.rollback().await?;
    let saved = store.operator_notices("g", 0, 100).await?;
    assert_eq!(saved.len(), 1);
    let home: String = sqlx::query_scalar("SELECT home_machine FROM groups WHERE name='g'")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(
        saved[0].responsibility,
        Some(NoticeResponsibility::HomeOperator {
            group: "g".into(),
            home_machine: home
        })
    );
    controller
        .finish(&store, crate::now()?, "bridge control joined")
        .await?;
    Ok(())
}

#[tokio::test]
async fn bounded_original_visit_page_keeps_one_episode_and_existing_spending() -> Result<()> {
    let (_root, store, _now) = fixture(0).await?;
    let controller = Controller::acquire(&store, 100).await?;
    let mut first_nonce = None;
    for offset in 0..101 {
        // Scheduler2146's adapter commits its actual protected reservation.
        // No successful owner page/receipt is claimed for these missed visits.
        let visit = controller
            .reserve_supervisor_visit_for_test(&store, 100 + offset, 110 + offset)
            .await?
            .context("genuine original visit")?;
        if first_nonce.is_none() {
            first_nonce = Some(visit.identity().nonce.clone());
        }
    }
    let mut tx = store.pool().begin().await?;
    project_infrastructure_notice_page_tx(&mut tx, "g", 1000, 100).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM execution_supervisor_visits WHERE gate='closed'"
        )
        .fetch_one(&mut *tx)
        .await?,
        100
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM execution_supervisor_visits WHERE gate='open'"
        )
        .fetch_one(&mut *tx)
        .await?,
        1,
        "lookahead never classifies a 101st visit"
    );
    let batch = reserve_notice_batch_for_class_tx(
        &mut tx,
        "g",
        "bridge",
        1000,
        NoticeClass::Infrastructure,
    )
    .await?
    .context("one original episode")?;
    expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "bridge", 1000)
        .await?
        .context("exposed")?;
    finish_operator_notice_batch_tx(
        &mut tx,
        "g",
        &batch.id,
        "bridge",
        TransportResult::Failed,
        "control: no sender started",
        1000,
    )
    .await?;
    let original = notice_readback_tx(&mut tx, "g", 0, 100).await?;
    assert_eq!(original.len(), 1);
    assert_eq!(Some(original[0].episode.clone()), first_nonce);
    assert_eq!(original[0].source_snapshot["due_at"], 110);
    tx.commit().await?;
    let mut tx = store.pool().begin().await?;
    project_infrastructure_notice_page_tx(&mut tx, "g", 1001, 100).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM execution_supervisor_visits WHERE gate='closed'"
        )
        .fetch_one(&mut *tx)
        .await?,
        101
    );
    assert!(
        reserve_notice_batch_for_class_tx(
            &mut tx,
            "g",
            "bridge",
            1001,
            NoticeClass::Infrastructure
        )
        .await?
        .is_none(),
        "later original visits never reset the original cooldown"
    );
    let after = notice_readback_tx(&mut tx, "g", 0, 100).await?;
    assert_eq!(after.len(), 1);
    assert_eq!(
        (after[0].account, after[0].revision, after[0].exposures),
        (original[0].account, original[0].revision, 1)
    );
    assert_eq!(after[0].source_snapshot, original[0].source_snapshot);
    tx.rollback().await?;
    controller
        .finish(&store, 1002, "bounded control joined")
        .await?;
    Ok(())
}
