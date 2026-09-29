//! Independent SQL connections for persistence assertions in integration tests.
use agent_mail::store::Store;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

pub async fn pool(store: &Store) -> anyhow::Result<SqlitePool> {
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(store.root().join("mail.db"))
                .foreign_keys(true),
        )
        .await?)
}
