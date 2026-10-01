//! Shared text revision authority, retry, privacy and recovery contracts.
mod support;
use agent_mail::{
    records::{RecordDraft, RecordTarget, RecordUpdate},
    states::TaskState,
    store::{Publish, Store},
    work::WorkDraft,
};
use anyhow::Result;
fn draft() -> RecordDraft {
    RecordDraft {
        id: "contract".into(),
        title: "Contract".into(),
        body: "Original contract".into(),
        summary: "Frozen interface".into(),
    }
}
fn correction() -> RecordUpdate {
    RecordUpdate {
        revision: 1,
        title: "Contract".into(),
        body: "Corrected contract".into(),
        summary: "Corrected interface".into(),
        reason: "Fix an incorrect constraint".into(),
    }
}
#[tokio::test]
async fn immutable_revisions_cas_retries_and_writer_authority() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "owner", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
    let now = agent_mail::now()?;
    let first = store.record_create(&writer, draft(), now).await?;
    assert_eq!(first, store.record_create(&writer, draft(), now + 1).await?);
    assert!(
        store
            .record_update(&owner, "contract", correction(), now)
            .await
            .is_err()
    );
    let updated = store
        .record_update(&writer, "contract", correction(), now + 1)
        .await?;
    assert_eq!(updated.supersedes, Some(1));
    assert_eq!(
        updated,
        store
            .record_update(&writer, "contract", correction(), now + 2)
            .await?
    );
    let mut conflicting = correction();
    conflicting.body = "Another correction".into();
    assert!(
        store
            .record_update(&writer, "contract", conflicting, now)
            .await
            .is_err()
    );
    assert_eq!(store.record_show(&owner, "contract", Some(1)).await?, first);
    assert_eq!(store.record_show(&owner, "contract", None).await?, updated);
    let history = store.record_history(&owner, "contract", Some(2)).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].revision, 1);
    let db = support::pool(&store).await?;
    assert!(
        sqlx::query("UPDATE record_revisions SET body='tamper'")
            .execute(&db)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM record_revisions")
            .execute(&db)
            .await
            .is_err()
    );
    store.register("g", "writer", true).await?;
    assert!(store.record_show(&writer, "contract", None).await.is_err());
    Ok(())
}
#[tokio::test]
async fn successor_recovers_pinned_contract_without_mailbox_impersonation() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    for participant in ["writer", "old-owner", "successor"] {
        store.register("g", participant, false).await?;
    }
    let writer = store.mailbox("g", "writer").await?;
    let successor = store.mailbox("g", "successor").await?;
    let now = agent_mail::now()?;
    store.record_create(&writer, draft(), now).await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "Implement contract".into(),
                owner: "successor".into(),
                state: TaskState::Active,
                next_action: "Read contract".into(),
                deadline: None,
                evidence: vec![],
            },
            now,
        )
        .await?;
    let target = RecordTarget::Task("task".into());
    store
        .record_link(&writer, &target, "contract", 1, now)
        .await?;
    store
        .record_link(&writer, &target, "contract", 1, now)
        .await?;
    assert!(
        store
            .record_link(&successor, &target, "contract", 1, now)
            .await
            .is_err()
    );
    store
        .record_update(&writer, "contract", correction(), now + 1)
        .await?;
    let links = store.record_links(&successor, &target).await?;
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].revision, 1);
    assert_eq!(links[0].current_revision, 2);
    assert_eq!(
        store
            .record_show(&successor, &links[0].id, Some(links[0].revision))
            .await?
            .body,
        "Original contract"
    );
    let message = store
        .publish(
            &writer,
            Publish {
                recipients: vec!["old-owner".into()],
                key: "private".into(),
                summary: "Private".into(),
                body: "Private reasoning".into(),
                due_after: None,
                reply_to: None,
                work_id: None,
            },
            now,
        )
        .await?;
    let mail_target = RecordTarget::Message(message);
    store
        .record_link(&writer, &mail_target, "contract", 1, now)
        .await?;
    assert!(store.record_links(&successor, &mail_target).await.is_err());
    store.enroll("other", None).await?;
    store.register("other", "stranger", false).await?;
    let stranger = store.mailbox("other", "stranger").await?;
    assert!(
        store
            .record_show(&stranger, "contract", None)
            .await
            .is_err()
    );
    let db = support::pool(&store).await?;
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE state='pending'")
        .fetch_one(&db)
        .await?;
    assert_eq!(pending, 1);
    Ok(())
}
#[tokio::test]
async fn list_history_and_contents_enforce_bounds() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let now = agent_mail::now()?;
    for i in 0..8 {
        let mut d = draft();
        d.id = format!("record{i}");
        store.record_create(&writer, d, now).await?;
    }
    let page = store.record_list(&writer, "").await?;
    assert_eq!(page.len(), 6);
    assert_eq!(store.record_list(&writer, &page[5].id).await?.len(), 2);
    let mut big = draft();
    big.body = "é".repeat(32769);
    assert!(store.record_create(&writer, big, now).await.is_err());
    store.record_create(&writer, draft(), now).await?;
    for revision in 1..24 {
        let mut u = correction();
        u.revision = revision;
        store
            .record_update(&writer, "contract", u, now + revision)
            .await?;
    }
    let history = store.record_history(&writer, "contract", None).await?;
    assert_eq!(history.len(), 20);
    let rest = store
        .record_history(&writer, "contract", Some(history[19].revision))
        .await?;
    assert_eq!(rest.len(), 4);
    Ok(())
}

#[tokio::test]
async fn remote_successor_reads_pinned_revision_and_rejects_forged_snapshot() -> Result<()> {
    use agent_mail::relay::{Event, Exchange};
    let home_dir = tempfile::tempdir()?;
    let remote_dir = tempfile::tempdir()?;
    let home = Store::open(home_dir.path(), true).await?;
    let remote = Store::open(remote_dir.path(), true).await?;
    for store in [&home, &remote] {
        store.enroll("g", None).await?;
    }
    home.register("g", "writer", false).await?;
    remote.register("g", "successor", false).await?;
    let home_id = agent_mail::relay::machine(&home.machine_id().await?)?;
    let remote_id = agent_mail::relay::machine(&remote.machine_id().await?)?;
    remote.set_home("g", home_id).await?;
    let now = agent_mail::now()?;
    home.route("g", "successor", remote_id, now).await?;
    let writer = home.mailbox("g", "writer").await?;
    let owner = remote.mailbox("g", "successor").await?;
    home.record_create(&writer, draft(), now).await?;
    home.work_create(
        &writer,
        WorkDraft {
            id: "remote-task".into(),
            scope: "Implement contract".into(),
            owner: "successor".into(),
            state: TaskState::Active,
            next_action: "Read contract".into(),
            deadline: None,
            evidence: vec![],
        },
        now,
    )
    .await?;
    let target = RecordTarget::Task("remote-task".into());
    home.record_link(&writer, &target, "contract", 1, now)
        .await?;
    home.record_update(&writer, "contract", correction(), now + 1)
        .await?;
    let events = home.export_for(remote_id).await?;
    remote
        .exchange(
            home_id,
            Exchange {
                incoming: events.clone(),
                ack: vec![],
            },
            now + 2,
        )
        .await?;
    remote
        .exchange(
            home_id,
            Exchange {
                incoming: events.clone(),
                ack: vec![],
            },
            now + 3,
        )
        .await?;
    let links = remote.record_links(&owner, &target).await?;
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].revision, 1);
    assert_eq!(links[0].current_revision, 2);
    assert_eq!(
        remote.record_show(&owner, "contract", Some(1)).await?.body,
        "Original contract"
    );
    assert_eq!(remote.record_list(&owner, "").await?.len(), 1);
    assert_eq!(
        remote.record_history(&owner, "contract", None).await?.len(),
        2
    );
    assert!(
        remote
            .record_update(&owner, "contract", correction(), now)
            .await
            .is_err()
    );
    let mut forged = events
        .into_iter()
        .find(|e| matches!(e.event, Event::RecordSnapshot(_)))
        .unwrap();
    forged.event_id = uuid::Uuid::new_v4();
    forged.origin = remote_id;
    forged.destination = home_id;
    assert!(
        home.exchange(
            remote_id,
            Exchange {
                incoming: vec![forged],
                ack: vec![]
            },
            now
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_corrections_preserve_one_winner() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let now = agent_mail::now()?;
    store.record_create(&writer, draft(), now).await?;
    let left = correction();
    let mut right = correction();
    right.body = "Competing text".into();
    let (a, b) = tokio::join!(
        store.record_update(&writer, "contract", left, now + 1),
        store.record_update(&writer, "contract", right, now + 1)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        store.record_history(&writer, "contract", None).await?.len(),
        2
    );
    assert_eq!(
        store.record_show(&writer, "contract", None).await?.revision,
        2
    );
    Ok(())
}

#[tokio::test]
async fn multiple_maximum_size_snapshots_drain_within_wire_budget() -> Result<()> {
    use agent_mail::relay::Exchange;
    let home_dir = tempfile::tempdir()?;
    let remote_dir = tempfile::tempdir()?;
    let home = Store::open(home_dir.path(), true).await?;
    let remote = Store::open(remote_dir.path(), true).await?;
    for store in [&home, &remote] {
        store.enroll("g", None).await?;
    }
    home.register("g", "writer", false).await?;
    remote.register("g", "reader", false).await?;
    let home_id = agent_mail::relay::machine(&home.machine_id().await?)?;
    let remote_id = agent_mail::relay::machine(&remote.machine_id().await?)?;
    remote.set_home("g", home_id).await?;
    let now = agent_mail::now()?;
    home.route("g", "reader", remote_id, now).await?;
    let writer = home.mailbox("g", "writer").await?;
    let reader = remote.mailbox("g", "reader").await?;
    for i in 0..8 {
        let mut d = draft();
        d.id = format!("large{i}");
        d.body = "\\".repeat(agent_mail::records::RECORD_BODY_LIMIT);
        home.record_create(&writer, d, now).await?;
    }
    let mut transferred = 0;
    while home.outbox_status().await?.0 > 0 {
        let events = home.export_for(remote_id).await?;
        assert!(!events.is_empty());
        assert!(events.len() < 8);
        assert!(
            serde_json::to_vec(&Exchange {
                incoming: home.export().await?,
                ack: vec![uuid::Uuid::new_v4(); 16]
            })?
            .len()
                <= 256 * 1024
        );
        let request = Exchange {
            incoming: events.clone(),
            ack: vec![],
        };
        assert!(serde_json::to_vec(&request)?.len() <= 256 * 1024);
        transferred += events.len();
        let receipt = remote.exchange(home_id, request, now).await?;
        home.exchange(
            remote_id,
            Exchange {
                incoming: vec![],
                ack: receipt.ack,
            },
            now,
        )
        .await?;
    }
    assert_eq!(transferred, 8);
    assert_eq!(
        remote
            .record_show(&reader, "large7", None)
            .await?
            .body
            .len(),
        agent_mail::records::RECORD_BODY_LIMIT
    );
    Ok(())
}
