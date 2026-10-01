//! End-to-end recovery of exact shared contracts and retained typed evidence.
use agent_mail::{
    artifacts::{ArtifactDraft, ArtifactLimits, ArtifactLink, ResourceLocation},
    records::{RecordDraft, RecordTarget},
    store::Store,
    work::WorkDraft,
};
use anyhow::Result;

#[tokio::test]
async fn replacement_owner_recovers_exact_contract_and_evidence_with_own_identity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Store::open(root.path(), true).await?;
    store.enroll("project", None).await?;
    store.register("project", "coordinator", false).await?;
    store.register("project", "worker", false).await?;
    let coordinator = store.mailbox("project", "coordinator").await?;
    store
        .work_create(
            &coordinator,
            WorkDraft {
                id: "lane".into(),
                scope: "Implement contract".into(),
                owner: "worker".into(),
                state: agent_mail::states::TaskState::Open,
                next_action: "Read governing revision".into(),
                deadline: None,
                evidence: vec!["legacy-local.log".into()],
            },
            100,
        )
        .await?;
    store
        .record_create(
            &coordinator,
            RecordDraft {
                id: "contract".into(),
                title: "Contract".into(),
                body: "Exact frozen agreement".into(),
                summary: "Read revision one".into(),
            },
            101,
        )
        .await?;
    store
        .record_link(
            &coordinator,
            &RecordTarget::Task("lane".into()),
            "contract",
            1,
            102,
        )
        .await?;
    let artifact = store
        .artifact_ingest(
            &coordinator,
            ArtifactDraft {
                id: "evidence".into(),
                location: ResourceLocation::Managed,
                digest: None,
                media_type: Some("text/plain".into()),
                size: None,
                provenance: Some("test run".into()),
            },
            "exact bytes".as_bytes(),
            &ArtifactLimits::default(),
            103,
        )
        .await?;
    store
        .artifact_link(
            &coordinator,
            "evidence",
            ArtifactLink::Task { id: "lane".into() },
            104,
        )
        .await?;
    let old_worker = store.mailbox("project", "worker").await?;
    store.register("project", "worker", true).await?;
    assert!(
        store
            .context_value(&old_worker, String::new(), 0)
            .await
            .is_err()
    );
    let worker = store.mailbox("project", "worker").await?;
    let context = store.context_value(&worker, String::new(), 0).await?;
    assert!(serde_json::to_vec(&context)?.len() <= 4096);
    assert_eq!(context["records"][0]["record"], "contract");
    assert_eq!(context["records"][0]["revision"], 1);
    assert_eq!(context["artifacts"][0]["artifact"], "evidence");
    assert_eq!(
        store.record_show(&worker, "contract", Some(1)).await?.body,
        "Exact frozen agreement"
    );
    let mut bytes = Vec::new();
    store
        .artifact_fetch(&worker, "evidence", &mut bytes, &ArtifactLimits::default())
        .await?;
    assert_eq!(bytes, b"exact bytes");
    assert!(artifact.resource.digest.unwrap().starts_with("sha256:"));
    assert_eq!(
        store.work_show(&worker, "lane").await?.state,
        agent_mail::states::TaskState::Open
    );
    store.close().await;
    Ok(())
}
