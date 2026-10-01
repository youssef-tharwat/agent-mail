//! Integration tests for native framing, worker input bounds, and protected worker startup.

use agent_mail::managed_runtime::{
    NATIVE_FRAME_LIMIT, NativeClient, NativeFrame, read_native_frame,
};
use std::io::{BufReader, Cursor};

#[test]
fn native_frames_preserve_unknown_fields_without_inventing_a_session() {
    let frame = NativeFrame::decode(
        NativeClient::Codex,
        br#"{"type":"turn.completed","session_id":"untrusted","unknown":{"cost":1}}"#,
    )
    .unwrap();
    assert_eq!(frame.session(), None);
    assert_eq!(frame.payload["unknown"]["cost"], 1);
    let codex = NativeFrame::decode(
        NativeClient::Codex,
        br#"{"type":"thread.started","thread_id":"real-session"}"#,
    )
    .unwrap();
    assert_eq!(codex.session(), Some("real-session"));
    let claude = NativeFrame::decode(
        NativeClient::Claude,
        br#"{"type":"system","subtype":"init","session_id":"native-session"}"#,
    )
    .unwrap();
    assert_eq!(claude.session(), Some("native-session"));
}

#[test]
fn framing_rejects_partial_invalid_and_oversized_output() {
    for bytes in [
        b"{}".to_vec(),
        b"[]\n".to_vec(),
        b"not-json\n".to_vec(),
        vec![b'x'; NATIVE_FRAME_LIMIT + 1],
    ] {
        let mut reader = BufReader::new(Cursor::new(bytes));
        assert!(read_native_frame(&mut reader, NativeClient::Codex).is_err());
    }
    let mut reader = Cursor::new(b"{}\n{\"type\":\"unknown\"}\n");
    assert!(
        read_native_frame(&mut reader, NativeClient::Claude)
            .unwrap()
            .is_some()
    );
    assert!(
        read_native_frame(&mut reader, NativeClient::Claude)
            .unwrap()
            .is_some()
    );
    assert!(
        read_native_frame(&mut reader, NativeClient::Claude)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn worker_transport_rejects_invalid_partial_and_oversized_input() {
    for bytes in [
        b"[]".to_vec(),
        b"{\"correlation\":".to_vec(),
        vec![b'x'; 8193],
    ] {
        assert!(
            agent_mail::managed_runtime::read_worker_input(bytes.as_slice())
                .await
                .is_err()
        );
    }
    let (reader, _unfinished_writer) = tokio::io::duplex(32);
    assert!(
        agent_mail::managed_runtime::read_worker_input(reader)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn worker_store_coexists_with_a_live_service_without_setup_or_upgrade() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let setup = agent_mail::store::Store::open(&root, true).await.unwrap();
    setup.close().await;
    let service = agent_mail::store::Store::open(&root, false).await.unwrap();
    let _worker_lock = agent_mail::service::WorkerLock::acquire(&root).unwrap();
    let worker = agent_mail::managed_runtime::open_worker_store(&root)
        .await
        .unwrap();
    assert_eq!(worker.root(), root);
    let groups = worker.groups().await.unwrap();
    assert!(groups.is_empty());
    worker.close().await;
    assert!(service.groups().await.unwrap().is_empty());
    service.close().await;
}

#[tokio::test]
async fn hidden_worker_cli_has_finite_input_failure_and_no_discovered_state() {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("absent-state");
    for body in [Some(b"{".to_vec()), Some(vec![b'x'; 8193]), None] {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
            .args(["__managed-worker-v1", "--root"])
            .arg(&root)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        if let Some(bytes) = body {
            if let Err(error) = input.write_all(&bytes).await {
                assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
            }
            if let Err(error) = input.shutdown().await {
                assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
            }
        }
        // For None, deliberately retain the open pipe: the worker must exit on its own deadline.
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(5), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
        drop(input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!root.exists());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("managed worker failed"));
        assert!(!stderr.contains("migrating schema"));
    }
}
