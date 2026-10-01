//! Public hidden transport refusal controls; these are not native qualification.
use agent_mail::{managed_runtime::run_managed_contained_worker, store::Store};

#[tokio::test]
async fn contained_role_rejects_unbound_or_oversized_requests_before_launch() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(temp.path(), true).await.unwrap();
    for bytes in [b"{}".to_vec(), b"[]".to_vec(), vec![b'x'; 8193]] {
        assert!(run_managed_contained_worker(&store, &bytes).await.is_err());
    }
    store.close().await;
}
