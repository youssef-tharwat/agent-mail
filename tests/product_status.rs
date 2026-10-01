//! Real status regression fixture: saved observe policy with no registered agents.
use agent_mail::{
    followup::{Mode, Policy},
    store::Store,
};
use anyhow::Result;

#[tokio::test]
async fn empty_group_keeps_saved_observe_and_operator_gap_visible() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("observe-empty", None).await?;
    store
        .configure_followups(
            "observe-empty",
            &Policy {
                mode: Mode::Observe,
                interval_seconds: 900,
                max_seconds: 3600,
                notifier: None,
            },
            agent_mail::now()?,
        )
        .await?;
    let before = serde_json::to_value(store.followup_policy("observe-empty").await?)?;
    let summary = agent_mail::status::summary(&store, Some("observe-empty")).await?;
    println!("{summary}");
    assert!(summary.contains("follow-through: observe"), "{summary}");
    assert!(
        summary.contains("Operator route: unconfigured"),
        "{summary}"
    );
    assert!(
        summary.contains("Follow-through:"),
        "pending counts must remain visible: {summary}"
    );
    assert!(summary.contains("No agents yet"));
    let report = agent_mail::status::report(&store, Some("observe-empty")).await?;
    assert_eq!(report["followup_policy"][0]["mode"], "observe");
    assert_eq!(
        report["followup_policy"][0]["operator_notifier_configured"],
        false
    );
    assert_eq!(
        before,
        serde_json::to_value(store.followup_policy("observe-empty").await?)?
    );
    store.close().await;
    Ok(())
}
