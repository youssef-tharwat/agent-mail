//! Read-only diagnostics for configuration, credentials, and runtime endpoints.
//!
//! [`inspect`] opens existing state and probes configured endpoints without creating
//! registrations or starting agent turns. Individual failures become report entries;
//! configuration alone cannot prove that a client trusts or consumes hook output.

use crate::{identity::Binding, store::Store};
use serde::Serialize;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use uuid::Uuid;

/// The diagnostic outcome and degree of certainty.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    /// The inspected condition was confirmed.
    Pass,
    /// The inspected condition conclusively failed.
    Fail,
    /// A condition requires attention without proving failure.
    Warning,
    /// Available evidence cannot determine the outcome.
    Unknown,
}
/// One diagnostic observation and an optional corrective action.
#[derive(Debug, Serialize)]
pub struct Check {
    /// Stable identifier of the diagnostic being performed.
    pub check: &'static str,
    /// Outcome of this diagnostic.
    pub status: Level,
    /// Evidence or explanation associated with this diagnostic.
    pub detail: Value,
    /// Suggested corrective action when the diagnostic needs attention.
    pub next_action: Option<&'static str>,
}
/// The ordered results of a configuration and endpoint inspection.
#[derive(Debug, Serialize, Default)]
pub struct Report {
    /// Individual diagnostic results in evaluation order.
    pub checks: Vec<Check>,
}
impl Report {
    /// Check whether any diagnostic conclusively failed.
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
/// Inspect existing configuration and runtime endpoints without starting agent turns.
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
            Some("Run agent-mail init GROUP with this state directory"),
        );
        return report;
    }
    let store = match Store::open(root, false).await {
        Ok(pair) => pair,
        Err(_) => {
            report.add(
                "database",
                Level::Fail,
                "Database cannot be opened with the current schema",
                Some("Use the upgraded binary and run agent-mail init GROUP; migration and worker handoff are automatic"),
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
    let selected = if group.is_empty() {
        match store.select_group(None, session, false).await {
            Ok(group) => group,
            Err(error) => {
                report.add(
                    "group",
                    Level::Fail,
                    error.to_string(),
                    Some("Select the intended group with --group GROUP"),
                );
                return report;
            }
        }
    } else {
        group.to_owned()
    };
    let group = selected.as_str();
    let config = match store.group(group).await {
        Ok(c) => c,
        Err(_) => {
            report.add(
                "group",
                Level::Fail,
                "Group is not configured",
                Some("Run agent-mail init GROUP"),
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
            report.add(
                "identity",
                Level::Fail,
                "No matching current agent identity",
                Some("Use agent-mail status --check NAME as operator"),
            );
            return report;
        }
    };
    report.add("identity",Level::Pass,json!({"participant":actor.name,"generation":actor.binding_version,"runtime":actor.binding.runtime()}),None);
    if actor.state == crate::states::AgentState::Retired {
        report.add(
            "registration",
            Level::Fail,
            "Agent is retired",
            Some("Restore the registration explicitly before launching or binding"),
        );
        return report;
    }
    if matches!(actor.binding, Binding::Herdr(_)) {
        let enabled = match config.socket.as_deref() {
            Some(socket) => crate::herdr::plugin_enabled(socket).await,
            None => Err(anyhow::anyhow!("missing Herdr socket")),
        };
        match enabled {
            Ok(true) => report.add("herdr_plugin", Level::Pass, "Mail plugin is enabled", None),
            Ok(false) => report.add("herdr_plugin", Level::Fail, "plugin_disabled", Some("Run herdr plugin enable youssef-tharwat.agent-mail in the intended Herdr session")),
            Err(_) => report.add("herdr_plugin", Level::Fail, "Cannot verify Mail plugin enablement", Some("Verify the configured Herdr socket and plugin installation")),
        }
        report.add("herdr_delivery", if config.paused == 0 && config.auto_prompt != 0 { Level::Pass } else { Level::Fail },
            json!({"paused":config.paused != 0,"auto_prompt":config.auto_prompt != 0}),
            if config.paused == 0 && config.auto_prompt != 0 { None } else { Some("Resume this group and configure its Herdr prompt policy before relying on automatic delivery") });
    }
    if actor.binding.herdr().is_some() {
        match sqlx::query!("SELECT b.attempts AS 'attempts!: i64',b.next_wake AS 'next_wake!: i64',b.wake_attempted AS 'wake_attempted!: i64',(SELECT MAX(e.id) FROM herdr_wake_events e WHERE e.recipient=b.id) AS 'latest?: i64' FROM mailboxes b WHERE b.id=? AND b.binding_version=?",actor.id,actor.binding_version).fetch_one(store.pool()).await {
            Ok(row) => {
                let fresh=row.latest.is_some_and(|id| id>row.wake_attempted);
                let attempts=if fresh {0} else {row.attempts};
                let exhausted=row.latest.is_some() && attempts>=3;
                report.add("herdr_wake",if exhausted {Level::Fail} else {Level::Pass},
                    json!({"pending_event":row.latest,"attempted_event":row.wake_attempted,"attempts":attempts,"next_wake":row.next_wake,"new_generation":fresh}),
                    if exhausted {Some("Inspect and repair the endpoint, then run agent-mail agent retry NAME")} else {None});
            }
            Err(_) => report.add("herdr_wake",Level::Fail,"Cannot inspect delivery retry state",Some("Inspect database health and the current binding")),
        }
    }
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
            "Authenticated event subscription accepted",
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
        "SELECT runtime,socket,thread,binding_version FROM runtime_wakes WHERE recipient=?",
        actor.id
    )
    .fetch_optional(store.pool())
    .await;
    match endpoint {
        Ok(Some(e)) if e.binding_version != actor.binding_version => report.add(
            "endpoint",
            Level::Fail,
            "Endpoint belongs to a previous binding",
            Some("Attach the current participant to its intended runtime session"),
        ),
        Ok(Some(e)) => {
            let inbox = store.claude_inbox(&actor).await;
            let probe = match inbox {
                Ok(Some(inbox)) => {
                    crate::claude_inbox::verify(&store, Path::new(&e.socket), &inbox).map(|()| {
                        json!({
                            "transport": "claude_inbox",
                            "receipt": "UserPromptSubmit hook",
                            "state": inbox.activity,
                            "socket_identity_verified": true,
                            "runtime_acceptance": "confirmed only after a matching delivery hook"
                        })
                    })
                }
                Err(error) => Err(error),
                Ok(None) => match Uuid::parse_str(&e.thread) {
                    Ok(thread) => match e.runtime.parse::<crate::states::NativeRuntime>() {
                        Ok(kind) => crate::native::probe(kind, Path::new(&e.socket), thread).await,
                        Err(error) => Err(error),
                    },
                    Err(e) => Err(e.into()),
                },
            };
            match probe {
                Ok(info) if info["state"] == "not_loaded" || info["state"] == "system_error" => report.add(
                    "endpoint", Level::Fail, info,
                    Some("Open or resume the intended thread in its Native client, then rerun status --check"),
                ),
                Ok(info) => report.add("endpoint", Level::Pass, info, None),
                Err(_) => report.add(
                    "endpoint",
                    Level::Fail,
                    "Native session or queue capability is unavailable",
                    Some("Verify the persistent thread and app-server, then repeat the matching attach command"),
                ),
            }
        }
        Ok(None) if matches!(actor.binding, Binding::Herdr(_)) => {
            let binding = actor.binding.herdr().expect("matched Herdr binding");
            let live = match config.socket {
                Some(socket) => crate::herdr::agent(Path::new(&socket), &binding.pane).await,
                None => Err(anyhow::anyhow!("missing socket")),
            };
            match live {Ok(live) if live.matches(&actor)=>report.add("endpoint",if live.ready(){Level::Pass}else{Level::Warning},json!({"runtime":"herdr","state":live.agent_status,"ready":live.ready(),"ineligible_reason":live.readiness_reason(),"interactive_ready":live.interactive_ready,"auto_prompt":config.auto_prompt}),None),_=>report.add("endpoint",Level::Fail,"Herdr identity is unavailable or changed",Some("Verify the pane and explicitly rebind its current session"))}
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
    match store
        .delivery_status(&actor, crate::now().unwrap_or_default())
        .await
    {
        Ok(info) => report.add(
            "delivery",
            if info.ready {
                Level::Pass
            } else {
                Level::Warning
            },
            json!(info),
            None,
        ),
        Err(_) => report.add(
            "delivery",
            Level::Fail,
            "Cannot read delivery evidence",
            Some("Inspect database health"),
        ),
    }
    match store.launch_readiness(&actor).await {
        Ok(info) => {
            let observed = info.state == crate::states::RecoveryState::HookObserved;
            report.add("recovery", if observed { Level::Pass } else { Level::Unknown }, json!(info),
                if observed { None } else { Some("Launch the agent and review hooks through the client's normal trust flow; no hook has been observed for this launch") });
        }
        Err(_) => report.add(
            "recovery",
            Level::Fail,
            "Cannot read hook evidence",
            Some("Inspect database health"),
        ),
    }
    match store
        .followup_status_for(
            Some(group),
            crate::now().unwrap_or_default(),
            Some(actor.id),
        )
        .await
    {
        Ok(value) => {
            let needs_attention = value["totals"]["due"].as_i64().unwrap_or(0) > 0
                || value["totals"]["escalated"].as_i64().unwrap_or(0) > 0;
            report.add("followthrough",if needs_attention{Level::Warning}else{Level::Pass},json!({"items":value["items"],"totals":value["totals"],"operator_notifications":value["operator_notifications"],"more":value["more"],"remote_followup":"unsupported","reported_progress_is_verified":false}),if needs_attention{Some("Inspect the current source under the responsible identity; record an outcome or checkpoint. Status inspection does not handle it.")}else{None});
        }
        Err(_) => report.add(
            "followthrough",
            Level::Unknown,
            "Cannot inspect follow-through",
            Some("Inspect database health"),
        ),
    }
    report
}
