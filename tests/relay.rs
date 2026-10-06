//! Regression coverage for relay behavior.
mod support;
use agent_mail::{
    herdr::{Agent, Session},
    relay::{Envelope, Exchange, Receipt, machine},
    store::{Publish, Store},
    work::WorkDraft,
};
use anyhow::Result;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
use tempfile::TempDir;
use tokio::{io::AsyncWriteExt, process::Command};

struct Node {
    _dir: TempDir,
    store: Store,
}

struct FakeSsh {
    _dir: TempDir,
    path: String,
    remote_state: PathBuf,
}

impl FakeSsh {
    fn new(remote: &Node) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("agent-mail-ssh-")
            .tempdir_in("/tmp")?;
        let ssh = dir.path().join("ssh");
        fs::write(
            &ssh,
            "#!/bin/sh\nshift 6\nexec \"$AGENT_MAIL_TEST_BIN\" --state-dir \"$AGENT_MAIL_TEST_REMOTE_STATE\" \"$@\"\n",
        )?;
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700))?;
        let path = format!("{}:{}", dir.path().display(), std::env::var("PATH")?);
        Ok(Self {
            _dir: dir,
            path,
            remote_state: remote.store.root().to_path_buf(),
        })
    }

    fn command(&self, home: &Node, args: &[&str]) -> Command {
        let binary = env!("CARGO_BIN_EXE_agent-mail");
        let mut command = Command::new(binary);
        command
            .args(["--state-dir", home.store.root().to_str().unwrap()])
            .args(args)
            .env("PATH", &self.path)
            .env("AGENT_MAIL_TEST_BIN", binary)
            .env("AGENT_MAIL_TEST_REMOTE_STATE", &self.remote_state);
        command
    }
}

fn agent(pane: &str) -> Agent {
    Agent {
        pane_id: pane.into(),
        terminal_id: format!("terminal-{pane}"),
        agent: Some("codex".into()),
        agent_session: Some(Session {
            agent: "codex".into(),
            kind: agent_mail::states::SessionKind::Id,
            value: format!("session-{pane}"),
        }),
        agent_status: agent_mail::herdr::AgentStatus::Idle,
        interactive_ready: Some(true),
        launch_pending: false,
        cwd: None,
    }
}

impl Node {
    async fn new(pane: &str, participant: &str) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("agent-mail-relay-")
            .tempdir_in("/tmp")?;
        let store = Store::open(&dir.path().join("state"), true).await?;
        store
            .enroll("g", Some(&dir.path().join("herdr.sock")))
            .await?;
        store.bind("g", participant, &agent(pane), false).await?;
        store.close().await;
        let store = Store::open(&dir.path().join("state"), false).await?;
        Ok(Self { _dir: dir, store })
    }

    async fn id(&self) -> Result<uuid::Uuid> {
        machine(&self.store.machine_id().await?)
    }
}

fn message(to: &str, key: &str, work_id: Option<&str>) -> Publish {
    Publish {
        intent: agent_mail::states::MessageIntent::Request,
        recipients: vec![to.into()],
        key: key.into(),
        summary: "A bounded request".into(),
        body: "See revision abc123".into(),
        due_after: Some(900),
        reply_to: None,
        work_id: work_id.map(str::to_owned),
    }
}

async fn transfer(from: &Node, to: &Node, at: i64) -> Result<()> {
    let outgoing = if from.store.group("g").await?.home_machine == to.id().await?.to_string() {
        from.store.export().await?
    } else {
        from.store.export_for(to.id().await?).await?
    };
    let receipt = to
        .store
        .exchange(
            from.id().await?,
            Exchange {
                capabilities: agent_mail::relay::capabilities(),
                incoming: outgoing.clone(),
                ack: vec![],
            },
            at,
        )
        .await?;
    // A lost acknowledgement forces a replay of the same batch.
    let replay = to
        .store
        .exchange(
            from.id().await?,
            Exchange {
                capabilities: agent_mail::relay::capabilities(),
                incoming: outgoing,
                ack: vec![],
            },
            at + 1,
        )
        .await?;
    assert_eq!(receipt.ack.len(), replay.ack.len());
    from.store
        .exchange(
            to.id().await?,
            Exchange {
                capabilities: agent_mail::relay::capabilities(),
                incoming: vec![],
                ack: replay.ack,
            },
            at + 2,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn intent_requires_negotiated_support_and_preserves_notice_and_final_response() -> Result<()>
{
    use agent_mail::states::MessageIntent;
    let home = Node::new("home", "coordinator").await?;
    let remote = Node::new("remote", "worker").await?;
    remote.store.set_home("g", home.id().await?).await?;
    home.store
        .route("g", "worker", remote.id().await?, 100)
        .await?;
    let coordinator = home.store.mailbox("g", "coordinator").await?;
    let worker = remote.store.mailbox("g", "worker").await?;
    let mut notice = message("worker", "notice", None);
    notice.intent = MessageIntent::Notice;
    notice.due_after = None;
    assert!(
        home.store
            .publish(&coordinator, notice.clone(), 100)
            .await
            .is_err()
    );
    let pool = support::pool(&home.store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages")
            .fetch_one(&pool)
            .await?,
        0,
        "unsupported traffic must roll back"
    );
    home.store
        .exchange(
            remote.id().await?,
            Exchange {
                capabilities: agent_mail::relay::capabilities(),
                incoming: vec![],
                ack: vec![],
            },
            101,
        )
        .await?;
    home.store.publish(&coordinator, notice, 102).await?;
    transfer(&home, &remote, 103).await?;
    assert!(
        remote
            .store
            .attention_snapshot(&worker)
            .await?
            .items
            .is_empty()
    );
    let note = remote.store.inbox(&worker, 0).await?.remove(0);
    assert_eq!(
        remote.store.message(&worker, note.id).await?.intent,
        MessageIntent::Notice
    );
    let request = home
        .store
        .publish(&coordinator, message("worker", "request", None), 110)
        .await?;
    transfer(&home, &remote, 111).await?;
    let incoming = remote.store.inbox(&worker, 0).await?.remove(0);
    remote
        .store
        .resolve(
            &worker,
            incoming.id,
            "answer",
            Some(("Ready".into(), "Evidence supplied".into())),
            112,
        )
        .await?;
    transfer(&remote, &home, 113).await?;
    let answer = home.store.inbox(&coordinator, 0).await?.remove(0);
    assert_eq!(
        home.store.message(&coordinator, answer.id).await?.intent,
        MessageIntent::Response
    );
    assert!(
        home.store
            .resolve(&coordinator, answer.id, "ack", None, 114)
            .await
            .is_err()
    );
    assert_eq!(
        home.store
            .wait_mail(&coordinator, request, None)
            .await?
            .pending,
        0
    );
    let first_reply = home
        .store
        .wait_mail_for(
            &coordinator,
            request,
            None,
            agent_mail::followup::MailPredicate::FirstReply,
        )
        .await?;
    assert_eq!(first_reply.outcome, agent_mail::watch::WaitOutcome::Reply);
    assert_eq!(first_reply.replies, 1);
    Ok(())
}

#[tokio::test]
async fn offline_mail_reply_resolution_and_snapshot_survive_replay() -> Result<()> {
    let home = Node::new("same-pane", "coordinator").await?;
    let remote = Node::new("same-pane", "worker").await?;
    remote.store.set_home("g", home.id().await?).await?;
    home.store
        .route("g", "worker", remote.id().await?, 1_000)
        .await?;
    remote
        .store
        .route("g", "coordinator", home.id().await?, 1_000)
        .await?;
    let coordinator = home.store.mailbox("g", "coordinator").await?;
    let worker = remote.store.mailbox("g", "worker").await?;

    home.store
        .work_create(
            &coordinator,
            WorkDraft {
                id: "lane-a".into(),
                scope: "Implement feature".into(),
                owner: "worker".into(),
                state: agent_mail::states::TaskState::Active,
                next_action: "Inspect contract".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    let id = home
        .store
        .publish(
            &coordinator,
            message("worker", "request", Some("lane-a")),
            1001,
        )
        .await?;
    assert_eq!(remote.store.inbox(&worker, 0).await?.len(), 0);
    assert_eq!(home.store.outbox_status().await?.0, 2);
    transfer(&home, &remote, 1002).await?;
    assert_eq!(home.store.outbox_status().await?.0, 0);
    let assigned = remote.store.work_show(&worker, "lane-a").await?;
    assert_eq!(assigned.owner, "worker");
    assert_eq!(assigned.synced_at, Some(1002));
    assert_eq!(assigned.linked_messages.len(), 1);
    assert!(
        remote
            .store
            .work_create(
                &worker,
                WorkDraft {
                    id: "illegal".into(),
                    scope: "not home".into(),
                    owner: "worker".into(),
                    state: agent_mail::states::TaskState::Active,
                    next_action: "none".into(),
                    deadline: None,
                    evidence: vec![],
                },
                1003
            )
            .await
            .is_err()
    );

    let incoming = remote.store.inbox(&worker, 0).await?;
    assert_eq!(incoming.len(), 1);
    let reply = remote
        .store
        .resolve(
            &worker,
            incoming[0].id,
            "handled",
            Some(("reply".into(), "R".repeat(8 * 1024))),
            1004,
        )
        .await?;
    assert!(reply.is_some());
    assert_eq!(remote.store.outbox_status().await?.0, 2);
    transfer(&remote, &home, 1005).await?;
    assert_eq!(home.store.outbox_status().await?.0, 0);
    assert_eq!(home.store.inbox(&coordinator, 0).await?.len(), 1);
    let recipient = home.store.mailbox("g", "worker").await?;
    let state = sqlx::query!(
        "SELECT state FROM deliveries WHERE message=? AND recipient=?",
        id,
        recipient.id
    )
    .fetch_one(&support::pool(&home.store).await?)
    .await?
    .state;
    assert_eq!(state, "resolved");
    let later = home
        .store
        .publish(&coordinator, message("worker", "cancel", None), 1010)
        .await?;
    transfer(&home, &remote, 1011).await?;
    home.store.withdraw(&coordinator, later, 1_000).await?;
    transfer(&home, &remote, 1012).await?;
    let withdrawn = remote.store.inbox(&worker, 0).await?;
    assert!(withdrawn.is_empty());
    Ok(())
}

#[tokio::test]
async fn home_relays_between_remote_nodes_once() -> Result<()> {
    let home = Node::new("home", "coordinator").await?;
    let left = Node::new("same-pane", "left").await?;
    let right = Node::new("same-pane", "right").await?;
    left.store.set_home("g", home.id().await?).await?;
    right.store.set_home("g", home.id().await?).await?;
    home.store
        .route("g", "left", left.id().await?, 1_000)
        .await?;
    home.store
        .route("g", "right", right.id().await?, 1_000)
        .await?;
    left.store
        .route("g", "right", right.id().await?, 1_000)
        .await?;
    let sender = left.store.mailbox("g", "left").await?;
    let recipient = right.store.mailbox("g", "right").await?;
    left.store
        .publish(&sender, message("right", "cross", None), 1000)
        .await?;
    transfer(&left, &home, 1001).await?;
    assert_eq!(right.store.inbox(&recipient, 0).await?.len(), 0);
    transfer(&home, &right, 1002).await?;
    assert_eq!(right.store.inbox(&recipient, 0).await?.len(), 1);
    assert_eq!(
        right
            .store
            .message(&recipient, right.store.inbox(&recipient, 0).await?[0].id)
            .await?
            .sender,
        "left"
    );
    let incoming = right.store.inbox(&recipient, 0).await?[0].id;
    right
        .store
        .resolve(
            &recipient,
            incoming,
            "done",
            Some(("back".into(), "Complete".into())),
            1003,
        )
        .await?;
    transfer(&right, &home, 1004).await?;
    transfer(&home, &left, 1005).await?;
    assert_eq!(left.store.inbox(&sender, 0).await?.len(), 1);
    let remote_recipient = left.store.mailbox("g", "right").await?;
    let original = sqlx::query!("SELECT d.state FROM deliveries d JOIN messages m ON m.id=d.message WHERE m.dedup_key='cross' AND d.recipient=?", remote_recipient.id)
        .fetch_one(&support::pool(&left.store).await?).await?;
    assert_eq!(original.state, "resolved");
    Ok(())
}

#[tokio::test]
async fn bridge_commands_exchange_json_across_processes() -> Result<()> {
    let home = Node::new("home", "coordinator").await?;
    let remote = Node::new("remote", "worker").await?;
    remote.store.set_home("g", home.id().await?).await?;
    home.store
        .route("g", "worker", remote.id().await?, 1_000)
        .await?;
    let sender = home.store.mailbox("g", "coordinator").await?;
    home.store
        .publish(&sender, message("worker", "bridge", None), 1000)
        .await?;
    let binary = env!("CARGO_BIN_EXE_agent-mail");
    let export = Command::new(binary)
        .args([
            "--state-dir",
            home.store.root().to_str().unwrap(),
            "adapter",
            "bridge",
            "export",
        ])
        .output()
        .await?;
    assert!(
        export.status.success(),
        "{}",
        String::from_utf8_lossy(&export.stderr)
    );
    let incoming: Vec<Envelope> = serde_json::from_slice(&export.stdout)?;
    assert_eq!(incoming.len(), 1);
    let input = serde_json::to_vec(&Exchange {
        capabilities: agent_mail::relay::capabilities(),
        incoming,
        ack: vec![],
    })?;
    let mut child = Command::new(binary)
        .args([
            "--state-dir",
            remote.store.root().to_str().unwrap(),
            "adapter",
            "bridge",
            "exchange",
            "--source",
            &home.id().await?.to_string(),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    child.stdin.take().unwrap().write_all(&input).await?;
    let output = child.wait_with_output().await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: Receipt = serde_json::from_slice(&output.stdout)?;
    assert_eq!(receipt.ack.len(), 1);
    assert_eq!(
        remote
            .store
            .inbox(&remote.store.mailbox("g", "worker").await?, 0)
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn explicit_sync_uses_ssh_stdio_and_clears_both_outboxes() -> Result<()> {
    let home = Node::new("home", "coordinator").await?;
    let remote = Node::new("remote", "worker").await?;
    let home_id = home.id().await?;
    let remote_id = remote.id().await?;
    remote.store.set_home("g", home_id).await?;
    home.store.route("g", "worker", remote_id, 1_000).await?;
    remote
        .store
        .route("g", "coordinator", home_id, 1_000)
        .await?;
    home.store.add_peer(remote_id, "test-remote").await?;

    let fake_ssh = FakeSsh::new(&remote)?;
    let remote_id = remote_id.to_string();

    let coordinator = home.store.mailbox("g", "coordinator").await?;
    let worker = remote.store.mailbox("g", "worker").await?;
    home.store
        .publish(&coordinator, message("worker", "ssh-request", None), 1000)
        .await?;
    let output = fake_ssh
        .command(&home, &["remote", "sync", &remote_id])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(home.store.outbox_status().await?.0, 0);
    let incoming = remote.store.inbox(&worker, 0).await?;
    assert_eq!(incoming.len(), 1);

    remote
        .store
        .resolve(
            &worker,
            incoming[0].id,
            "handled",
            Some(("ssh-reply".into(), "Done".into())),
            1001,
        )
        .await?;
    let output = fake_ssh
        .command(&home, &["remote", "sync", &remote_id])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(remote.store.outbox_status().await?.0, 0);
    assert_eq!(home.store.inbox(&coordinator, 0).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn worker_sync_requires_opt_in_and_target_change_revokes_it() -> Result<()> {
    let home = Node::new("home", "coordinator").await?;
    let remote = Node::new("remote", "worker").await?;
    let home_id = home.id().await?;
    let remote_id = remote.id().await?;
    remote.store.set_home("g", home_id).await?;
    home.store.route("g", "worker", remote_id, 1_000).await?;
    remote
        .store
        .route("g", "coordinator", home_id, 1_000)
        .await?;
    home.store.add_peer(remote_id, "test-remote").await?;
    let fake_ssh = FakeSsh::new(&remote)?;
    let remote_id_text = remote_id.to_string();
    let coordinator = home.store.mailbox("g", "coordinator").await?;
    let worker = remote.store.mailbox("g", "worker").await?;

    home.store
        .publish(&coordinator, message("worker", "auto-request", None), 1000)
        .await?;
    let output = fake_ssh
        .command(&home, &["service", "run", "--once"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(remote.store.inbox(&worker, 0).await?.is_empty());
    assert_eq!(home.store.outbox_status().await?.0, 1);
    assert!(!home.store.peers_status().await?[0].auto_sync);

    let output = fake_ssh
        .command(&home, &["remote", "auto-sync", &remote_id_text, "enable"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(home.store.peers_status().await?[0].auto_sync);
    let output = fake_ssh
        .command(&home, &["service", "run", "--once"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(remote.store.inbox(&worker, 0).await?.len(), 1);
    assert_eq!(home.store.outbox_status().await?.0, 0);

    remote
        .store
        .publish(&worker, message("coordinator", "remote-result", None), 1001)
        .await?;
    home.store.add_peer(remote_id, "changed-target").await?;
    assert!(!home.store.peers_status().await?[0].auto_sync);
    let output = fake_ssh
        .command(&home, &["service", "run", "--once"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(home.store.inbox(&coordinator, 0).await?.is_empty());
    assert_eq!(remote.store.outbox_status().await?.0, 1);

    home.store.set_auto_sync(remote_id, true).await?;
    let output = fake_ssh
        .command(&home, &["service", "run", "--once"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(home.store.inbox(&coordinator, 0).await?.len(), 1);
    assert_eq!(remote.store.outbox_status().await?.0, 0);
    let output = fake_ssh
        .command(&home, &["remote", "auto-sync", &remote_id_text, "disable"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!home.store.peers_status().await?[0].auto_sync);
    Ok(())
}

#[tokio::test]
async fn withdrawal_uses_the_supplied_timestamp_for_relay_events() -> Result<()> {
    let home = Node::new("coordinator-pane", "coordinator").await?;
    let remote = Node::new("worker-pane", "worker").await?;
    let remote_id = remote.id().await?;
    home.store.route("g", "worker", remote_id, 100).await?;
    let actor = home.store.caller("g", "coordinator-pane").await?;
    let id = home
        .store
        .publish(&actor, message("worker", "withdraw-clock", None), 200)
        .await?;
    home.store.withdraw(&actor, id, 12_345).await?;
    let created: i64 = sqlx::query_scalar(
        "SELECT created FROM outbox WHERE json_extract(payload, '$.event.kind')='withdrawal'",
    )
    .fetch_one(&support::pool(&home.store).await?)
    .await?;
    assert_eq!(created, 12_345);
    Ok(())
}
