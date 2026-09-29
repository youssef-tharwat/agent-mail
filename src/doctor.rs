//! Read-only diagnostics: configuration is evidence, not proof of hook trust.
use crate::{identity::Binding, store::Store};
use serde::Serialize;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use uuid::Uuid;

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Pass,
    Fail,
    Warning,
    Unknown,
}
#[derive(Debug, Serialize)]
pub struct Check {
    pub check: &'static str,
    pub status: Level,
    pub detail: Value,
    pub next_action: Option<&'static str>,
}
#[derive(Debug, Serialize, Default)]
pub struct Report {
    pub checks: Vec<Check>,
}
impl Report {
    pub fn failed(&self) -> bool {
        self.checks.iter().any(|c| c.status == Level::Fail)
    }
    fn add(
        &mut self,
        check: &'static str,
        status: Level,
        detail: impl Into<Value>,
        next_action: Option<&'static str>,
    ) {
        self.checks.push(Check {
            check,
            status,
            detail: detail.into(),
            next_action,
        });
    }
}
/// Never initializes state, rotates identities, or starts an agent.
pub async fn inspect(
    root: &Path,
    group: &str,
    name: Option<&str>,
    session: Option<&Uuid>,
) -> Report {
    let mut report = Report::default();
    if !root.join("mail.db").is_file() {
        report.add(
            "database",
            Level::Fail,
            "Mail database is missing",
            Some("Run agent-mail setup with this state directory"),
        );
        return report;
    }
    let (store, _guard) = match Store::open(root, false).await {
        Ok(pair) => pair,
        Err(_) => {
            report.add(
                "database",
                Level::Fail,
                "Database cannot be opened with the current schema",
                Some("Stop the worker, back up state, and run agent-mail setup to migrate"),
            );
            return report;
        }
    };
    report.add(
        "database",
        Level::Pass,
        "Current schema opens successfully",
        None,
    );
    let config = match store.group(group).await {
        Ok(c) => c,
        Err(_) => {
            report.add(
                "group",
                Level::Fail,
                "Group is not configured",
                Some("Run agent-mail setup --group GROUP"),
            );
            return report;
        }
    };
    report.add(
        "group",
        Level::Pass,
        json!({"name":group,"auto_prompt":config.auto_prompt}),
        None,
    );
    if crate::service::running(root) {
        report.add("worker", Level::Pass, "Worker lock is held", None);
    } else {
        report.add(
            "worker",
            Level::Fail,
            "Worker is stopped",
            Some("Run agent-mail service run"),
        );
    }
    let actor = match (name, session) {
        (Some(name), None) => store.mailbox(group, name).await,
        (_, _) => store.authenticate(group, session).await,
    };
    let actor = match actor {
        Ok(actor) if name.is_none_or(|n| n == actor.name) => actor,
        _ => {
            report.add("identity",Level::Fail,"No matching current participant identity",Some("Select --name PARTICIPANT as operator, or supply its current AGENT_MAIL_SESSION"));
            return report;
        }
    };
    report.add("identity",Level::Pass,json!({"participant":actor.name,"generation":actor.binding_version,"runtime":actor.binding.runtime()}),None);
    let stream = tokio::time::timeout(Duration::from_secs(3), async {
        let mut reader = crate::stream::connect(&store, &actor, 0).await?;
        anyhow::ensure!(
            matches!(
                crate::stream::next(&mut reader).await?,
                crate::stream::Frame::Ready { .. }
            ),
            "subscription rejected"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await;
    if matches!(stream, Ok(Ok(()))) {
        report.add(
            "stream",
            Level::Pass,
            "Authenticated version 1 subscription accepted",
            None,
        );
    } else {
        report.add(
            "stream",
            Level::Fail,
            "Stream unavailable or identity rejected",
            Some("Start the worker and verify the participant binding"),
        );
    }
    let endpoint = sqlx::query!(
        "SELECT socket,thread,binding_version FROM codex_wakes WHERE recipient=?",
        actor.id
    )
    .fetch_optional(&store.pool)
    .await;
    match endpoint {
        Ok(Some(e)) if e.binding_version != actor.binding_version => report.add(
            "endpoint",
            Level::Fail,
            "Endpoint belongs to a previous binding",
            Some("Attach the current participant to its intended runtime session"),
        ),
        Ok(Some(e)) => {
            let probe = match Uuid::parse_str(&e.thread) {
                Ok(thread) => crate::codex::probe(Path::new(&e.socket), thread).await,
                Err(e) => Err(e.into()),
            };
            match probe {
                Ok(info) if info["state"] == "not_loaded" || info["state"] == "system_error" => report.add(
                    "endpoint", Level::Fail, info,
                    Some("Open or resume the intended thread in its Codex client, then rerun doctor"),
                ),
                Ok(info) => report.add("endpoint", Level::Pass, info, None),
                Err(_) => report.add(
                    "endpoint",
                    Level::Fail,
                    "Codex session or queue capability is unavailable",
                    Some("Verify the persistent thread and app-server, then repeat attach-codex"),
                ),
            }
        }
        Ok(None) if matches!(actor.binding, Binding::Herdr(_)) => {
            let binding = actor.binding.herdr().expect("matched Herdr binding");
            let live = match config.socket {
                Some(socket) => crate::herdr::agent(Path::new(&socket), &binding.pane).await,
                None => Err(anyhow::anyhow!("missing socket")),
            };
            match live {Ok(live) if live.matches(&actor)=>report.add("endpoint",Level::Pass,json!({"runtime":"herdr","state":live.agent_status,"auto_prompt":config.auto_prompt}),None),_=>report.add("endpoint",Level::Fail,"Herdr identity is unavailable or changed",Some("Verify the pane and explicitly rebind its current session"))}
        }
        Ok(None) => report.add(
            "endpoint",
            Level::Warning,
            "Hooks can recover context but cannot wake an idle standalone agent",
            Some("Attach a supported runtime endpoint for idle delivery"),
        ),
        Err(_) => report.add(
            "endpoint",
            Level::Fail,
            "Endpoint query failed",
            Some("Inspect database health"),
        ),
    }
    report.add(
        "hook_trust",
        Level::Unknown,
        "Hook trust is controlled by the agent client; configuration alone cannot prove it",
        Some("Verify hooks through the client's normal trust flow and a real recovery boundary"),
    );
    report
}
