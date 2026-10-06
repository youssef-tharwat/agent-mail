//! Artifact identity, integrity, retention and consistent backups.
mod support;
use agent_mail::{
    artifacts::{ArtifactAccess, ArtifactDraft, ArtifactLimits, ArtifactLink, ResourceLocation},
    states::TaskState,
    store::Store,
    work::WorkDraft,
};
use anyhow::Result;
fn draft(id: &str) -> ArtifactDraft {
    ArtifactDraft {
        id: id.into(),
        location: ResourceLocation::Managed,
        digest: None,
        size: None,
        media_type: Some("text/plain".into()),
        provenance: Some("test witness".into()),
    }
}
async fn setup() -> Result<(tempfile::TempDir, Store, agent_mail::store::Mailbox)> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "a", false).await?;
    let actor = store.mailbox("g", "a").await?;
    Ok((temp, store, actor))
}
#[tokio::test]
async fn dedup_roundtrip_integrity_limits_and_retention() -> Result<()> {
    let (_temp, store, actor) = setup().await?;
    let limits = ArtifactLimits::default();
    let bytes = b"a representative repeated log entry\n".repeat(10000);
    let (first, second) = tokio::join!(
        store.artifact_ingest(&actor, draft("one"), bytes.as_slice(), &limits, 100),
        store.artifact_ingest(&actor, draft("two"), bytes.as_slice(), &limits, 100)
    );
    let first = first?;
    let second = second?;
    assert_eq!(first.resource.digest, second.resource.digest);
    let mut fetched = Vec::new();
    store
        .artifact_fetch(&actor, "one", &mut fetched, &limits)
        .await?;
    assert_eq!(fetched, bytes);
    let stats = store.artifact_stats(&actor).await?;
    assert_eq!(stats.logical_bytes, bytes.len() as u64 * 2);
    assert_eq!(stats.unique_original_bytes, bytes.len() as u64);
    assert!(stats.physical_stored_bytes < bytes.len() as u64 / 10);
    store
        .work_create(
            &actor,
            WorkDraft {
                id: "task".into(),
                scope: "Review evidence".into(),
                owner: "a".into(),
                state: TaskState::Open,
                next_action: "Inspect".into(),
                deadline: None,
                evidence: vec!["old external evidence".into()],
            },
            100,
        )
        .await?;
    store
        .artifact_link(&actor, "one", ArtifactLink::Task { id: "task".into() }, 101)
        .await?;
    assert!(
        store
            .artifact_prune(&actor, 0, false, 200)
            .await?
            .digests
            .is_empty()
    );
    store
        .artifact_unlink(&actor, "one", ArtifactLink::Task { id: "task".into() }, 201)
        .await?;
    store.artifact_pin(&actor, "two", true, 202).await?;
    assert!(
        store
            .artifact_prune(&actor, 0, false, 300)
            .await?
            .digests
            .is_empty()
    );
    store.artifact_pin(&actor, "two", false, 301).await?;
    assert_eq!(
        store
            .artifact_prune(&actor, 0, true, 400)
            .await?
            .digests
            .len(),
        1
    );
    store
        .artifact_fetch(&actor, "one", std::io::sink(), &limits)
        .await?;
    assert_eq!(
        store
            .artifact_prune(&actor, 0, false, 400)
            .await?
            .digests
            .len(),
        1
    );
    assert!(matches!(
        store.artifact_check(&actor, "one", &limits).await?,
        ArtifactAccess::Unavailable { .. }
    ));
    assert!(
        store
            .artifact_link(&actor, "one", ArtifactLink::Task { id: "task".into() }, 500)
            .await
            .is_err()
    );
    store
        .artifact_ingest(&actor, draft("one"), bytes.as_slice(), &limits, 501)
        .await?;
    let tiny = ArtifactLimits {
        decoded_bytes: 3,
        ..limits.clone()
    };
    assert!(
        store
            .artifact_ingest(&actor, draft("oversized"), bytes.as_slice(), &tiny, 502)
            .await
            .is_err()
    );
    let quota = ArtifactLimits {
        quota_bytes: 0,
        ..limits.clone()
    };
    assert!(
        store
            .artifact_ingest(&actor, draft("quota"), b"different".as_slice(), &quota, 503)
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn identity_codec_corruption_and_backup_restore() -> Result<()> {
    let (temp, store, actor) = setup().await?;
    let limits = ArtifactLimits::default();
    let bytes = b"small uncompressed exact witness\0\xff";
    store
        .artifact_ingest(&actor, draft("small"), bytes.as_slice(), &limits, 10)
        .await?;
    store.artifact_pin(&actor, "small", true, 11).await?;
    let parent = tempfile::tempdir()?;
    let backup = parent.path().join("backup");
    store.artifact_backup(&backup, &limits).await?;
    let restored = parent.path().join("restored");
    Store::artifact_restore(&backup, &restored, &limits).await?;
    let other = Store::open(&restored, false).await?;
    let other_actor = other.mailbox("g", "a").await?;
    let mut fetched = Vec::new();
    other
        .artifact_fetch(&other_actor, "small", &mut fetched, &limits)
        .await?;
    assert_eq!(fetched, bytes);
    assert!(
        Store::artifact_restore(temp.path(), &parent.path().join("sqlite-only"), &limits)
            .await
            .is_err()
    );
    let objects = temp.path().join("artifacts/objects");
    let group = std::fs::read_dir(objects)?.next().unwrap()?.path();
    let object = std::fs::read_dir(group)?.next().unwrap()?.path();
    std::fs::write(&object, b"corrupt")?;
    assert!(matches!(
        store.artifact_check(&actor, "small", &limits).await?,
        ArtifactAccess::IntegrityFailure { .. }
    ));
    let pool = support::pool(&other).await?;
    sqlx::query("UPDATE artifact_blobs SET codec='future-v9'")
        .execute(&pool)
        .await?;
    assert!(matches!(
        other.artifact_check(&other_actor, "small", &limits).await?,
        ArtifactAccess::Unsupported { .. }
    ));
    Ok(())
}
#[tokio::test]
async fn references_are_explicit_and_group_actor_is_checked() -> Result<()> {
    let (_temp, store, actor) = setup().await?;
    let mut external = draft("legacy");
    external.location = ResourceLocation::Legacy {
        reference: "/sender/local/log".into(),
    };
    store
        .artifact_register(&actor, external.clone(), 10)
        .await?;
    assert!(matches!(
        store
            .artifact_check(&actor, "legacy", &ArtifactLimits::default())
            .await?,
        ArtifactAccess::Unsupported { .. }
    ));
    assert_eq!(
        store
            .artifact_register(&actor, external.clone(), 11)
            .await?
            .created,
        10
    );
    external.location = ResourceLocation::Repository {
        repository: "https://github.com/owner/repo".into(),
        revision: "abc123".into(),
        path: "logs/output.txt".into(),
    };
    external.id = "repo".into();
    store
        .artifact_register(&actor, external.clone(), 12)
        .await?;
    external.location = ResourceLocation::External {
        uri: "https://secret@example.com/log".into(),
    };
    external.id = "secret".into();
    assert!(store.artifact_register(&actor, external, 13).await.is_err());
    store
        .update_agent("g", "a", 1, agent_mail::states::AgentState::Retired, "done")
        .await?;
    assert!(store.artifact_show(&actor, "legacy").await.is_err());
    Ok(())
}
#[tokio::test]
async fn lifecycle_footprint_stabilizes_with_protected_witness() -> Result<()> {
    let (_temp, store, actor) = setup().await?;
    let limits = ArtifactLimits::default();
    store
        .artifact_ingest(
            &actor,
            draft("protected"),
            b"retain this witness".as_slice(),
            &limits,
            1,
        )
        .await?;
    store.artifact_pin(&actor, "protected", true, 2).await?;
    for iteration in 0..20 {
        let bytes = format!("ephemeral unique witness {iteration}");
        store
            .artifact_ingest(
                &actor,
                draft(&format!("ephemeral-{iteration}")),
                bytes.as_bytes(),
                &limits,
                10 + iteration,
            )
            .await?;
        store
            .artifact_prune(&actor, 0, false, 100 + iteration)
            .await?;
    }
    assert_eq!(
        store.artifact_stats(&actor).await?.physical_stored_bytes,
        b"retain this witness".len() as u64
    );
    store
        .artifact_fetch(&actor, "protected", std::io::sink(), &limits)
        .await?;
    Ok(())
}
#[tokio::test]
async fn failed_ingest_leaves_no_reference_and_prune_does_not_race_link() -> Result<()> {
    struct Broken;
    impl std::io::Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("interrupted producer"))
        }
    }
    let (_temp, store, actor) = setup().await?;
    let limits = ArtifactLimits::default();
    assert!(
        store
            .artifact_ingest(&actor, draft("broken"), Broken, &limits, 10)
            .await
            .is_err()
    );
    assert!(store.artifact_show(&actor, "broken").await.is_err());
    assert_eq!(store.artifact_stats(&actor).await?.temporary_bytes, 0);
    store
        .artifact_ingest(
            &actor,
            draft("race"),
            b"protect concurrent bytes".as_slice(),
            &limits,
            11,
        )
        .await?;
    store
        .work_create(
            &actor,
            WorkDraft {
                id: "race-task".into(),
                scope: "Retain".into(),
                owner: "a".into(),
                state: TaskState::Open,
                next_action: "Review".into(),
                deadline: None,
                evidence: vec![],
            },
            12,
        )
        .await?;
    let (linked, pruned) = tokio::join!(
        store.artifact_link(
            &actor,
            "race",
            ArtifactLink::Task {
                id: "race-task".into()
            },
            20
        ),
        store.artifact_prune(&actor, 0, false, 20)
    );
    let pruned = pruned?;
    if linked.is_ok() {
        assert!(pruned.digests.is_empty());
        store
            .artifact_fetch(&actor, "race", std::io::sink(), &limits)
            .await?;
    } else {
        assert_eq!(pruned.digests.len(), 1);
    }
    Ok(())
}
#[tokio::test]
#[ignore = "capacity measurement; run explicitly with --ignored --nocapture"]
async fn artifact_capacity_measurement() -> Result<()> {
    let (_temp, store, actor) = setup().await?;
    let limits = ArtifactLimits::default();
    let mut fixtures = vec![
        (
            "logs",
            b"INFO compile worker completed task 123\n".repeat(30000),
        ),
        (
            "json",
            br#"{"task":"build","status":"done","duration":42}\n"#.repeat(25000),
        ),
        (
            "diff",
            b"@@ -1,2 +1,2 @@\n-old implementation\n+new implementation\n".repeat(20000),
        ),
        ("small", b"short exact witness".to_vec()),
    ];
    let text = b"already compressed binary evidence\n".repeat(30000);
    fixtures.push(("compressed", zstd::stream::encode_all(text.as_slice(), 3)?));
    let binary: Vec<u8> = (0..1000000u64)
        .map(|n| {
            let mut x = n.wrapping_add(0x9e3779b97f4a7c15);
            x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
            (x ^ (x >> 31)) as u8
        })
        .collect();
    fixtures.push(("binary", binary));
    for (kind, bytes) in &fixtures {
        let start = std::time::Instant::now();
        store
            .artifact_ingest(&actor, draft(kind), bytes.as_slice(), &limits, 10)
            .await?;
        let write = start.elapsed();
        let read = std::time::Instant::now();
        store
            .artifact_fetch(&actor, kind, std::io::sink(), &limits)
            .await?;
        println!(
            "{kind}: original={} write_us={} read_us={}",
            bytes.len(),
            write.as_micros(),
            read.elapsed().as_micros()
        );
    }
    let (a, b) = tokio::join!(
        store.artifact_ingest(
            &actor,
            draft("repeat-a"),
            fixtures[0].1.as_slice(),
            &limits,
            11
        ),
        store.artifact_ingest(
            &actor,
            draft("repeat-b"),
            fixtures[0].1.as_slice(),
            &limits,
            11
        )
    );
    a?;
    b?;
    println!(
        "stats={}",
        serde_json::to_string(&store.artifact_stats(&actor).await?)?
    );
    let parent = tempfile::tempdir()?;
    let backup = parent.path().join("backup");
    let start = std::time::Instant::now();
    store.artifact_backup(&backup, &limits).await?;
    println!("backup_us={}", start.elapsed().as_micros());
    let start = std::time::Instant::now();
    Store::artifact_restore(&backup, &parent.path().join("restore"), &limits).await?;
    println!("restore_us={}", start.elapsed().as_micros());
    Ok(())
}
#[tokio::test]
async fn task_writer_and_producer_control_retention_and_accepted_evidence_stays() -> Result<()> {
    let (_temp, store, actor) = setup().await?;
    store.register("g", "other", false).await?;
    let other = store.mailbox("g", "other").await?;
    let limits = ArtifactLimits::default();
    store
        .artifact_ingest(
            &actor,
            draft("authority"),
            b"review witness".as_slice(),
            &limits,
            10,
        )
        .await?;
    store
        .work_create(
            &actor,
            WorkDraft {
                id: "accepted".into(),
                scope: "Review".into(),
                owner: "other".into(),
                state: TaskState::Open,
                next_action: "Inspect".into(),
                deadline: None,
                evidence: vec![],
            },
            11,
        )
        .await?;
    let target = ArtifactLink::Task {
        id: "accepted".into(),
    };
    assert!(
        store
            .artifact_link(&other, "authority", target.clone(), 12)
            .await
            .is_err()
    );
    assert!(
        store
            .artifact_pin(&other, "authority", true, 12)
            .await
            .is_err()
    );
    store
        .artifact_link(&actor, "authority", target.clone(), 13)
        .await?;
    store
        .update_work(
            &actor,
            "accepted",
            agent_mail::work::WorkUpdate {
                version: 1,
                reason: "Reviewed exact witness".into(),
                patch: agent_mail::work::WorkPatch {
                    state: Some(TaskState::Accepted),
                    accepted_revision: Some(Some("review-1".into())),
                    ..Default::default()
                },
                resolve_message: None,
            },
            14,
        )
        .await?;
    assert!(
        store
            .artifact_unlink(&actor, "authority", target, 15)
            .await
            .is_err()
    );
    assert!(
        store
            .artifact_prune(&actor, 0, false, 100)
            .await?
            .digests
            .is_empty()
    );
    let tiny = ArtifactLimits {
        temporary_bytes: 1,
        ..limits
    };
    assert!(
        store
            .artifact_fetch(&actor, "authority", std::io::sink(), &tiny)
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn temporary_orphans_are_charged_and_incompressible_output_is_bounded() -> Result<()> {
    let (temp, store, actor) = setup().await?;
    let tmp = temp.path().join("artifacts/tmp");
    std::fs::create_dir_all(&tmp)?;
    std::fs::write(tmp.join("interrupted-upload"), vec![0u8; 4096])?;
    let limits = ArtifactLimits {
        temporary_bytes: 8192,
        ..Default::default()
    };
    assert!(
        store
            .artifact_ingest(
                &actor,
                draft("budget"),
                vec![1u8; 3000].as_slice(),
                &limits,
                10
            )
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::metadata(tmp.join("interrupted-upload"))?.len(),
        4096
    );
    std::fs::remove_file(tmp.join("interrupted-upload"))?;
    let binary: Vec<u8> = (0..10000u64)
        .map(|n| {
            let mut x = n.wrapping_add(0x9e3779b97f4a7c15);
            x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
            (x ^ (x >> 31)) as u8
        })
        .collect();
    let limits = ArtifactLimits {
        temporary_bytes: 20000,
        ..Default::default()
    };
    store
        .artifact_ingest(&actor, draft("binary"), binary.as_slice(), &limits, 11)
        .await?;
    let mut actual = Vec::new();
    store
        .artifact_fetch(&actor, "binary", &mut actual, &limits)
        .await?;
    assert_eq!(actual, binary);
    assert_eq!(store.artifact_stats(&actor).await?.temporary_bytes, 0);
    Ok(())
}
#[tokio::test]
async fn backup_refuses_required_missing_objects_and_unknown_format() -> Result<()> {
    let (_temp, store, actor) = setup().await?;
    let limits = ArtifactLimits::default();
    store
        .artifact_ingest(
            &actor,
            draft("required"),
            b"required witness".as_slice(),
            &limits,
            10,
        )
        .await?;
    store.artifact_pin(&actor, "required", true, 11).await?;
    let pool = support::pool(&store).await?;
    sqlx::query("UPDATE artifact_blobs SET format_version=99")
        .execute(&pool)
        .await?;
    assert!(matches!(
        store.artifact_check(&actor, "required", &limits).await?,
        ArtifactAccess::Unsupported { .. }
    ));
    sqlx::query("DELETE FROM artifact_blobs")
        .execute(&pool)
        .await?;
    let parent = tempfile::tempdir()?;
    assert!(
        store
            .artifact_backup(&parent.path().join("incomplete"), &limits)
            .await
            .is_err()
    );
    assert!(!parent.path().join("incomplete").exists());
    Ok(())
}
#[tokio::test]
async fn paginated_targets_and_record_message_inspection_preserve_visibility() -> Result<()> {
    let (_temp, store, actor) = setup().await?;
    for name in ["recipient", "outsider"] {
        store.register("g", name, false).await?;
    }
    let recipient = store.mailbox("g", "recipient").await?;
    let outsider = store.mailbox("g", "outsider").await?;
    let limits = ArtifactLimits::default();
    store
        .artifact_ingest(
            &actor,
            draft("inspect"),
            b"linked witness".as_slice(),
            &limits,
            10,
        )
        .await?;
    let message = store
        .publish(
            &actor,
            agent_mail::store::Publish {
                intent: agent_mail::states::MessageIntent::Request,
                recipients: vec!["recipient".into()],
                key: "artifact-inspect".into(),
                summary: "Review".into(),
                body: "Inspect evidence".into(),
                due_after: None,
                context: agent_mail::mail_context::ContextSource::NewConversation,
            },
            11,
        )
        .await?;
    store
        .artifact_link(&actor, "inspect", ArtifactLink::Message { id: message }, 12)
        .await?;
    store
        .record_create(
            &actor,
            agent_mail::records::RecordDraft {
                id: "artifact-contract".into(),
                title: "Contract".into(),
                body: "Exact interface".into(),
                summary: "Review".into(),
            },
            13,
        )
        .await?;
    let revision = ArtifactLink::RecordRevision {
        id: "artifact-contract".into(),
        version: 1,
    };
    store
        .artifact_link(&actor, "inspect", revision.clone(), 14)
        .await?;
    assert_eq!(
        store
            .artifact_links_for_message(&actor, message)
            .await?
            .len(),
        1
    );
    assert_eq!(
        store
            .artifact_links_for_message(&recipient, message)
            .await?
            .len(),
        1
    );
    assert!(
        store
            .artifact_links_for_message(&outsider, message)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .artifact_links_for_record(&actor, "artifact-contract", 1)
            .await?
            .len(),
        1
    );
    assert!(
        store
            .artifact_unlink(&actor, "inspect", revision, 15)
            .await
            .is_err()
    );
    let first = store.artifact_targets(&actor, "inspect", None, 1).await?;
    assert_eq!(first["links"].as_array().unwrap().len(), 1);
    let cursor = first["next_cursor"].as_str().unwrap();
    let second = store
        .artifact_targets(&actor, "inspect", Some(cursor), 1)
        .await?;
    assert!(second["next_cursor"].is_null());
    assert!(
        store
            .artifact_targets(&recipient, "inspect", Some(cursor), 1)
            .await
            .is_err()
    );
    assert_eq!(
        store.artifact_targets(&actor, "inspect", None, 10).await?["links"][0]["kind"],
        "message_global"
    );
    Ok(())
}
#[tokio::test]
async fn orphan_pruning_dry_run_matches_audited_deletions_and_grace() -> Result<()> {
    let (temp, store, actor) = setup().await?;
    let limits = ArtifactLimits::default();
    store
        .artifact_ingest(
            &actor,
            draft("retain"),
            b"protected evidence".as_slice(),
            &limits,
            10,
        )
        .await?;
    store.artifact_pin(&actor, "retain", true, 11).await?;
    let objects = temp.path().join("artifacts/objects");
    let scope = std::fs::read_dir(objects)?.next().unwrap()?.path();
    let orphan = scope.join("0".repeat(64));
    std::fs::write(&orphan, b"12345")?;
    let tmp = temp.path().join("artifacts/tmp");
    std::fs::create_dir_all(&tmp)?;
    let old = tmp.join("old-spool");
    let fresh = tmp.join("fresh-spool");
    std::fs::write(&old, b"1234")?;
    std::fs::write(&fresh, b"123456")?;
    for (path, seconds) in [(&orphan, 10), (&old, 10), (&fresh, 95)] {
        std::fs::File::open(path)?.set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)),
        )?;
    }
    assert_eq!(store.artifact_stats(&actor).await?.reclaimable_bytes, 15);
    let dry = store.artifact_prune(&actor, 20, true, 100).await?;
    assert!(dry.digests.is_empty());
    assert_eq!(dry.orphan_digests.len(), 1);
    assert_eq!(dry.temporary_files, vec!["old-spool"]);
    assert_eq!(dry.reclaimed_bytes, 9);
    assert!(old.exists() && orphan.exists() && fresh.exists());
    let applied = store.artifact_prune(&actor, 20, false, 100).await?;
    assert_eq!(applied.orphan_digests, dry.orphan_digests);
    assert_eq!(applied.temporary_files, dry.temporary_files);
    assert_eq!(applied.reclaimed_bytes, dry.reclaimed_bytes);
    assert!(!old.exists() && !orphan.exists() && fresh.exists());
    store
        .artifact_fetch(&actor, "retain", std::io::sink(), &limits)
        .await?;
    let pool = support::pool(&store).await?;
    let (action, details): (String, String) =
        sqlx::query_as("SELECT action,details FROM artifact_audit ORDER BY sequence DESC LIMIT 1")
            .fetch_one(&pool)
            .await?;
    assert_eq!(action, "prune-completed");
    let details: serde_json::Value = serde_json::from_str(&details)?;
    assert_eq!(details["reclaimed_bytes"], 9);
    assert_eq!(details["orphan_digests"].as_array().unwrap().len(), 1);
    assert_eq!(details["temporary_files"], serde_json::json!(["old-spool"]));
    Ok(())
}
