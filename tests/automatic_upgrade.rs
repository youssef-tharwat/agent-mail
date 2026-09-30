//! Process-level worker handoff and interrupted-upgrade recovery.
use agent_mail::{service, store::Store, stream};
use anyhow::{Result, ensure};
use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

struct WorkerCleanup(PathBuf);
impl Drop for WorkerCleanup {
    fn drop(&mut self) {
        // The replacement is detached. Ask whichever worker now owns the store to
        // drain rather than signalling a PID that might have been reused.
        if let Ok(mut socket) = std::os::unix::net::UnixStream::connect(stream::socket(&self.0)) {
            let _ = socket.write_all(
                b"{\"method\":\"prepare_upgrade\",\"version\":1,\"target_version\":\"999.0.0\"}\n",
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

#[tokio::test]
async fn interrupted_handoff_restarts_running_worker_and_preserves_group_state() -> Result<()> {
    let temp = tempfile::Builder::new()
        .prefix("am-handoff-")
        .tempdir_in("/tmp")?;
    let root = temp.path().canonicalize()?;
    let binary = env!("CARGO_BIN_EXE_agent-mail");
    let store = Store::open(&root, true).await?;
    store.enroll("g", None).await?;
    let credential = store.register("g", "owner", false).await?;
    store.pause("g", true).await?;
    store.close().await;
    let mut old = Command::new(binary)
        .arg("--state-dir")
        .arg(&root)
        .args(["service", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _cleanup = WorkerCleanup(root.clone());
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while !stream::socket(&root).exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if ready.is_err() {
        let _ = old.kill();
    }
    ready?;
    // Simulate a durable handoff intent left before worker shutdown. The next
    // command must finish recovery even though the schema itself is current.
    std::fs::write(
        root.join("upgrade.json"),
        serde_json::to_vec(&serde_json::json!({"rollback":binary,"managed":false}))?,
    )?;
    // Prevent spawning the replacement after the original has drained. Failure
    // must retain enough intent for a later command to restore delivery.
    std::fs::create_dir(root.join("service.log"))?;
    let failed = tokio::process::Command::new(binary)
        .arg("--state-dir")
        .arg(&root)
        .args(["init", "g"])
        .output()
        .await?;
    assert!(!failed.status.success());
    assert!(old.wait()?.success());
    assert!(!service::running(&root));
    assert!(root.join("upgrade.json").exists());
    std::fs::remove_dir(root.join("service.log"))?;
    let output = tokio::process::Command::new(binary)
        .arg("--state-dir")
        .arg(&root)
        .args(["init", "g"])
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(service::running(&root));
    assert!(!root.join("upgrade.json").exists());
    let store = Store::open(&root, false).await?;
    assert_eq!(
        store.authenticate("g", Some(&credential)).await?.name,
        "owner"
    );
    assert_eq!(store.group("g").await?.paused, 1);
    store.close().await;
    drop(_cleanup);
    assert!(!service::running(&root));
    Ok(())
}

#[tokio::test]
async fn upgrade_drains_a_watch_with_its_resume_cursor_intact() -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let temp = tempfile::Builder::new()
        .prefix("am-watch-upgrade-")
        .tempdir_in("/tmp")?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.pause("g", true).await?;
    let credential = store.register("g", "owner", false).await?;
    let actor = store.authenticate("g", Some(&credential)).await?;
    let state = store.clone();
    let worker = tokio::spawn(async move {
        let result = service::run(&state, false).await;
        state.close().await;
        result
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !stream::socket(store.root()).exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;
    let mut watch = store.watch(&actor, None).await?;
    let cursor = watch.cursor();
    let mut control = tokio::net::UnixStream::connect(stream::socket(store.root())).await?;
    control
        .write_all(
            format!(
                "{{\"method\":\"prepare_upgrade\",\"version\":1,\"target_version\":\"{}\"}}\n",
                env!("CARGO_PKG_VERSION")
            )
            .as_bytes(),
        )
        .await?;
    let mut response = String::new();
    BufReader::new(control).read_line(&mut response).await?;
    assert!(response.contains("upgrade_accepted"));
    let error = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await?
        .unwrap_err();
    assert!(error.to_string().contains("upgrade"));
    assert_eq!(watch.cursor(), cursor);
    tokio::time::timeout(Duration::from_secs(5), worker).await???;
    assert!(!service::running(store.root()));
    drop(watch);
    store.close().await;
    Ok(())
}
