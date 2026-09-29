//! Regression coverage for resource lifetimes behavior.
use agent_mail::{
    service,
    store::Store,
    stream::{self, Server},
};
use anyhow::Result;
use std::time::Duration;

#[tokio::test]
async fn schema_lock_follows_last_clone_even_after_pool_close() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    let clone = store.clone();
    drop(store);
    assert!(Store::open(dir.path(), true).await.is_err());
    assert_eq!(clone.group("g").await?.socket, None);
    clone.close().await;

    let store = Store::open(dir.path(), false).await?;
    let clone = store.clone();
    store.close().await;
    assert!(Store::open(dir.path(), true).await.is_err());
    drop(clone);
    Store::open(dir.path(), true).await?.close().await;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_subscribers_before_releasing_worker_lock() -> Result<()> {
    let dir = tempfile::Builder::new()
        .prefix("am-lifetime-")
        .tempdir_in("/tmp")?;
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    let credential = store.register("g", "owner", false).await?;
    let actor = store.authenticate("g", Some(&credential)).await?;
    let server = Server::start(store.clone())?;
    assert!(Server::start(store.clone()).is_err());
    let mut reader = stream::connect(&store, &actor, 0).await?;
    assert!(matches!(
        stream::next(&mut reader).await?,
        stream::Frame::Ready { .. }
    ));
    server.shutdown().await?;
    assert!(!service::running(store.root()));
    assert!(stream::next(&mut reader).await.is_err());
    let replacement = Server::start(store.clone())?;
    tokio::task::yield_now().await;
    assert!(stream::socket(store.root()).exists());
    assert!(service::running(store.root()));
    replacement.shutdown().await?;
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn cancellation_keeps_lock_until_endpoint_cleanup() -> Result<()> {
    let dir = tempfile::Builder::new()
        .prefix("am-cancel-")
        .tempdir_in("/tmp")?;
    let store = Store::open(dir.path(), true).await?;
    let server = Server::start(store.clone())?;
    drop(server);
    let replacement = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(server) = Server::start(store.clone()) {
                break server;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::task::yield_now().await;
    assert!(stream::socket(store.root()).exists());
    replacement.shutdown().await?;
    store.close().await;
    Ok(())
}
