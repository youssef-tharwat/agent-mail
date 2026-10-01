//! Long-lived native completion subscriptions. Reconnection replays a bounded
//! history page; unavailable/older clients retain the durable timer safety net.
use crate::{
    codex::{Client, ThreadStatus},
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::Row;
use std::{collections::HashMap, path::Path, time::Duration};
use tokio::task::JoinHandle;

#[derive(Default)]
pub(crate) struct Monitors {
    jobs: HashMap<String, JoinHandle<Result<()>>>,
}
impl Drop for Monitors {
    fn drop(&mut self) {
        for job in self.jobs.values() {
            job.abort();
        }
    }
}
impl Monitors {
    pub(crate) async fn refresh(&mut self, store: &Store) -> Result<()> {
        let rows = sqlx::query("SELECT b.group_name,b.name,b.binding_version,w.socket,w.thread FROM runtime_wakes w JOIN mailboxes b ON b.id=w.recipient AND b.binding_version=w.binding_version JOIN groups g ON g.name=b.group_name JOIN followup_policy p ON p.group_name=g.name WHERE w.runtime='codex' AND p.mode='enabled' AND g.paused=0 AND b.agent_state='registered'")
            .fetch_all(store.pool()).await?;
        let mut current = std::collections::HashSet::new();
        for row in rows {
            let group: String = row.get("group_name");
            let name: String = row.get("name");
            let socket: String = row.get("socket");
            let session: String = row.get("thread");
            let key = format!(
                "{group}/{name}/{}/{socket}/{session}",
                row.get::<i64, _>("binding_version")
            );
            current.insert(key.clone());
            if self.jobs.get(&key).is_some_and(|job| !job.is_finished()) {
                continue;
            }
            if let Some(job) = self.jobs.remove(&key) {
                let _ = job.await;
            }
            let actor = store.mailbox(&group, &name).await?;
            let store = store.clone();
            self.jobs.insert(key, tokio::spawn(async move {
                loop {
                    if let Err(error) = monitor(&store, &actor, &socket, &session).await {
                        eprintln!("agent-mail: lifecycle {}/{} unavailable; timer recovery remains active: {error:#}", actor.group_name, actor.name);
                    }
                    // Reconnection is recovery, not a deadline for agent progress.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }));
        }
        self.jobs.retain(|key, job| {
            if current.contains(key) {
                true
            } else {
                job.abort();
                false
            }
        });
        Ok(())
    }
}

async fn monitor(store: &Store, actor: &Mailbox, socket: &str, session: &str) -> Result<()> {
    let thread = session.parse()?;
    let mut client = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = Client::connect(Path::new(socket)).await?;
        ensure!(
            matches!(
                client.thread(thread).await?.status,
                ThreadStatus::Idle | ThreadStatus::Active
            ),
            "thread is not loaded"
        );
        // Joining an already loaded thread subscribes this connection. No runtime
        // config overrides or answers to approval/tool requests are ever sent.
        client
            .call(
                "thread/resume",
                json!({"threadId":thread,"excludeTurns":true}),
            )
            .await?;
        Ok::<_, anyhow::Error>(client)
    })
    .await
    .context("lifecycle subscription timed out")??;
    let history = tokio::time::timeout(
        Duration::from_secs(5),
        client.call(
            "thread/turns/list",
            json!({"threadId":thread,"itemsView":"notLoaded","limit":20,"sortDirection":"desc"}),
        ),
    )
    .await;
    if let Ok(Ok(history)) = history {
        if let Some(turns) = history["data"].as_array() {
            for turn in turns.iter().rev() {
                recover_input(store, actor, session, &mut client, turn).await?;
                observe_turn(store, actor, socket, session, turn).await?;
            }
        }
    }
    loop {
        let event = client.notification().await?;
        if event["params"]["threadId"] != session {
            continue;
        }
        match event["method"].as_str() {
            Some("item/completed") => {
                let params = &event["params"];
                if let Some(turn) = params["turnId"].as_str() {
                    correlate(store, actor, session, turn, &params["item"]).await?;
                }
            }
            Some("turn/completed") => {
                recover_input(store, actor, session, &mut client, &event["params"]["turn"]).await?;
                observe_turn(store, actor, socket, session, &event["params"]["turn"]).await?
            }
            _ => {}
        }
    }
}

async fn recover_input(
    store: &Store,
    actor: &Mailbox,
    session: &str,
    client: &mut Client,
    turn: &Value,
) -> Result<()> {
    let Some(id) = turn["id"].as_str() else {
        return Ok(());
    };
    if turn["status"] != "completed" {
        return Ok(());
    }
    let missing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM turn_offers WHERE recipient=? AND binding_version=? AND runtime='codex' AND session=? AND turn IS NULL AND state='offered')")
        .bind(actor.id).bind(actor.binding_version).bind(session).fetch_one(store.pool()).await?;
    if !missing {
        return Ok(());
    }
    // A queued delivery starts a turn. Recover only its first input, never hydrate
    // the tool-output history of twenty potentially large coding turns.
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        client.call(
            "thread/items/list",
            json!({"threadId":session,"turnId":id,"limit":1,"sortDirection":"asc"}),
        ),
    )
    .await;
    if let Ok(Ok(page)) = result {
        if let Some(entry) = page["data"].as_array().and_then(|items| items.first()) {
            if entry["turnId"] == id {
                correlate(store, actor, session, id, &entry["item"]).await?;
            }
        }
    }
    Ok(())
}

async fn correlate(
    store: &Store,
    actor: &Mailbox,
    session: &str,
    turn: &str,
    item: &Value,
) -> Result<()> {
    if item["type"] != "userMessage" {
        return Ok(());
    }
    let Some(id) = item["clientId"].as_str() else {
        return Ok(());
    };
    sqlx::query("UPDATE turn_offers SET turn=? WHERE id=? AND recipient=? AND binding_version=? AND runtime='codex' AND session=? AND state='offered' AND (turn IS NULL OR turn=?)")
        .bind(turn).bind(id).bind(actor.id).bind(actor.binding_version).bind(session).bind(turn).execute(store.pool()).await?;
    Ok(())
}
async fn observe_turn(
    store: &Store,
    actor: &Mailbox,
    socket: &str,
    session: &str,
    turn: &Value,
) -> Result<()> {
    let Some(turn_id) = turn["id"].as_str() else {
        return Ok(());
    };
    if let Some(items) = turn["items"].as_array() {
        for item in items {
            correlate(store, actor, session, turn_id, item).await?;
        }
    }
    if turn["status"] != "completed" {
        return Ok(());
    }
    let mut tx = store.pool().begin().await?;
    Store::lock_actor(&mut tx, actor).await?;
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_wakes WHERE recipient=? AND binding_version=? AND runtime='codex' AND socket=? AND thread=?)")
        .bind(actor.id).bind(actor.binding_version).bind(socket).bind(session).fetch_one(&mut *tx).await?;
    if !current {
        return Ok(());
    }
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM turn_offers WHERE recipient=? AND binding_version=? AND runtime='codex' AND session=? AND turn=? AND state='offered'")
        .bind(actor.id).bind(actor.binding_version).bind(session).bind(turn_id).fetch_all(&mut *tx).await?;
    for id in &ids {
        crate::turns::complete_tx(&mut tx, actor, id, session, turn_id, crate::now()?).await?;
    }
    tx.commit().await?;
    if !ids.is_empty() {
        crate::stream::hint(store.root()).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{states::TaskState, work::WorkDraft};
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::UnixListener;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn live_and_replayed_completion_require_the_correlated_user_input() -> Result<()> {
        for (replay, client_id, status, expected) in [
            (false, "offer", "completed", 1_i64),
            (true, "offer", "completed", 1),
            (false, "unrelated-input", "completed", 0),
            (false, "offer", "interrupted", 0),
        ] {
            let temp = tempfile::Builder::new()
                .prefix("am-turn-")
                .tempdir_in("/tmp")?;
            let store = Store::open(temp.path(), true).await?;
            store.enroll("fleet", None).await?;
            store.register("fleet", "agent", false).await?;
            let actor = store.mailbox("fleet", "agent").await?;
            let time = crate::now()?;
            store
                .work_create(
                    &actor,
                    WorkDraft {
                        id: "work".into(),
                        scope: "Review".into(),
                        owner: "agent".into(),
                        state: TaskState::Active,
                        next_action: "Inspect evidence".into(),
                        deadline: None,
                        evidence: vec![],
                    },
                    time,
                )
                .await?;
            let session = uuid::Uuid::new_v4().to_string();
            let socket = temp.path().join("codex.sock");
            let socket_str = socket.to_str().unwrap();
            sqlx::query("INSERT INTO runtime_wakes(recipient,binding_version,socket,thread,runtime) VALUES(?,?,?,?,'codex')")
                .bind(actor.id).bind(actor.binding_version).bind(socket_str).bind(&session).execute(store.pool()).await?;
            let delivery = store.delivery(&actor, None, 0).await?;
            let mut tx = store.pool().begin().await?;
            crate::turns::offer_tx(
                &mut tx,
                &actor,
                "offer",
                "codex",
                &session,
                &delivery.events,
                time,
            )
            .await?;
            tx.commit().await?;
            let listener = UnixListener::bind(&socket)?;
            let thread = session.clone();
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await?;
                let mut ws = tokio_tungstenite::accept_async(io).await?;
                let item =
                    json!({"id":"input","type":"userMessage","clientId":client_id,"content":[]});
                let turn = json!({"id":"turn","status":status,"items":[],"itemsView":"notLoaded"});
                while let Some(frame) = ws.next().await {
                    let Message::Text(text) = frame? else {
                        continue;
                    };
                    let request: Value = serde_json::from_str(&text)?;
                    if request.get("id").is_none() {
                        continue;
                    }
                    let history = request["method"] == "thread/turns/list";
                    let result = match request["method"].as_str().unwrap() {
                        "initialize" => json!({}),
                        "thread/read" => {
                            json!({"thread":{"id":thread,"ephemeral":false,"status":{"type":"idle"}}})
                        }
                        "thread/resume" => {
                            assert_eq!(
                                request["params"],
                                json!({"threadId":thread,"excludeTurns":true})
                            );
                            json!({})
                        }
                        "thread/turns/list" => {
                            assert_eq!(request["params"]["itemsView"], "notLoaded");
                            json!({"data":if replay {vec![turn.clone()]} else {vec![]}})
                        }
                        "thread/items/list" => {
                            assert_eq!(request["params"]["limit"], 1);
                            json!({"data":[{"turnId":"turn","item":item}]})
                        }
                        method => anyhow::bail!("unexpected monitor request: {method}"),
                    };
                    ws.send(Message::Text(
                        json!({"id":request["id"],"result":result})
                            .to_string()
                            .into(),
                    ))
                    .await?;
                    if history {
                        if !replay {
                            ws.send(Message::Text(json!({"method":"item/completed","params":{"threadId":thread,"turnId":"turn","item":item}}).to_string().into())).await?;
                            for _ in 0..2 {
                                // Completion payloads may omit items. The earlier
                                // input event is durably correlated with this turn.
                                ws.send(Message::Text(json!({"method":"turn/completed","params":{"threadId":thread,"turn":{"id":"turn","status":status,"items":[]}}}).to_string().into())).await?;
                            }
                        }
                        if !replay {
                            ws.close(None).await?;
                            break;
                        }
                    }
                    if request["method"] == "thread/items/list" {
                        ws.close(None).await?;
                        break;
                    }
                }
                Ok::<_, anyhow::Error>(())
            });
            // A closed transport is expected after all proof frames are consumed.
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                monitor(&store, &actor, socket_str, &session),
            )
            .await?;
            assert!(result.is_err());
            server.await??;
            let stage: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='work'")
                .fetch_one(store.pool())
                .await?;
            assert_eq!(
                stage, expected,
                "replay={replay}, client={client_id}, status={status}"
            );
            assert_eq!(
                store.work_show(&actor, "work").await?.state,
                TaskState::Active
            );
        }
        Ok(())
    }
}
