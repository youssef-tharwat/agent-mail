use agent_mail::{
    herdr::{Agent, Session},
    relay::{Envelope, Exchange, Receipt, machine},
    store::{DatabaseGuard, Publish, Store},
    work::WorkDraft,
};
use anyhow::Result;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
use tempfile::TempDir;
use tokio::{io::AsyncWriteExt, process::Command};

struct Node {
    _dir: TempDir,
    store: Store,
    _guard: DatabaseGuard,
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
            remote_state: remote.store.root.clone(),
        })
    }

    fn command(&self, home: &Node, args: &[&str]) -> Command {
        let binary = env!("CARGO_BIN_EXE_agent-mail");
        let mut command = Command::new(binary);
        command
            .args(["--state-dir", home.store.root.to_str().unwrap()])
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
            kind: "id".into(),
            value: format!("session-{pane}"),
        }),
        agent_status: "idle".into(),
        interactive_ready: true,
        launch_pending: false,
        cwd: None,
    }
}

impl Node {
    async fn new(pane: &str, participant: &str) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("agent-mail-relay-")
            .tempdir_in("/tmp")?;
        let (store, guard) = Store::open(&dir.path().join("state"), true).await?;
        store
            .enroll("g", &dir.path().join("herdr.sock").to_string_lossy())
            .await?;
        store.bind("g", participant, &agent(pane), false).await?;
        store.pool.close().await;
        drop(guard);
        let (store, guard) = Store::open(&dir.path().join("state"), false).await?;
        Ok(Self {
            _dir: dir,
            store,
            _guard: guard,
        })
    }

    async fn id(&self) -> Result<uuid::Uuid> {
        machine(&self.store.machine_id().await?)
    }
}

fn message(to: &str, key: &str, work_id: Option<&str>) -> Publish {
    Publish {
        recipients: vec![to.into()],
        key: key.into(),
        summary: "A bounded request".into(),
        body: "See revision abc123".into(),
        due_after: 900,
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
                incoming: vec![],
                ack: replay.ack,
            },
            at + 2,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn offline_mail_reply_resolution_and_snapshot_survive_replay() -> Result<()> {
    let home = Node::new("same-pane", "coordinator").await?;
    let remote = Node::new("same-pane", "worker").await?;
    remote.store.set_home("g", home.id().await?).await?;
    home.store.route("g", "worker", remote.id().await?).await?;
    remote
        .store
        .route("g", "coordinator", home.id().await?)
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
                state: "active".into(),
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
                    state: "active".into(),
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
    .fetch_one(&home.store.pool)
    .await?
    .state;
    assert_eq!(state, "resolved");
    let later = home
        .store
        .publish(&coordinator, message("worker", "cancel", None), 1010)
        .await?;
    transfer(&home, &remote, 1011).await?;
    home.store.withdraw(&coordinator, later).await?;
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
    home.store.route("g", "left", left.id().await?).await?;
    home.store.route("g", "right", right.id().await?).await?;
    left.store.route("g", "right", right.id().await?).await?;
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
        .fetch_one(&left.store.pool).await?;
    assert_eq!(original.state, "resolved");
    Ok(())
}

#[tokio::test]
async fn bridge_commands_exchange_json_across_processes() -> Result<()> {
    let home = Node::new("home", "coordinator").await?;
    let remote = Node::new("remote", "worker").await?;
    remote.store.set_home("g", home.id().await?).await?;
    home.store.route("g", "worker", remote.id().await?).await?;
    let sender = home.store.mailbox("g", "coordinator").await?;
    home.store
        .publish(&sender, message("worker", "bridge", None), 1000)
        .await?;
    let binary = env!("CARGO_BIN_EXE_agent-mail");
    let export = Command::new(binary)
        .args([
            "--state-dir",
            home.store.root.to_str().unwrap(),
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
        incoming,
        ack: vec![],
    })?;
    let mut child = Command::new(binary)
        .args([
            "--state-dir",
            remote.store.root.to_str().unwrap(),
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
    home.store.route("g", "worker", remote_id).await?;
    remote.store.route("g", "coordinator", home_id).await?;
    home.store.add_peer(remote_id, "test-remote").await?;

    let fake_ssh = FakeSsh::new(&remote)?;
    let remote_id = remote_id.to_string();

    let coordinator = home.store.mailbox("g", "coordinator").await?;
    let worker = remote.store.mailbox("g", "worker").await?;
    home.store
        .publish(&coordinator, message("worker", "ssh-request", None), 1000)
        .await?;
    let output = fake_ssh
        .command(&home, &["sync", "--peer", &remote_id])
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
        .command(&home, &["sync", "--peer", &remote_id])
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
    home.store.route("g", "worker", remote_id).await?;
    remote.store.route("g", "coordinator", home_id).await?;
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
        .command(&home, &["auto-sync", "--peer", &remote_id_text, "--enable"])
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
        .command(
            &home,
            &["auto-sync", "--peer", &remote_id_text, "--disable"],
        )
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
