//! Keep fleet diagnostics scoped; shared service details are explicitly installation-wide.
use crate::{now, service, store::Store};
use anyhow::Result;
use serde_json::{Value, json};

async fn policies(store: &Store, group: Option<&str>) -> Result<Vec<Value>> {
    let mut policies = Vec::new();
    for config in store.groups().await? {
        if group.is_some_and(|name| name != config.name) {
            continue;
        }
        let policy = store.followup_policy(&config.name).await?;
        policies.push(json!({
            "group": config.name, "mode": policy.mode, "paused": config.paused != 0,
            "interval_seconds": policy.interval_seconds, "max_seconds": policy.max_seconds,
            "operator_notifier_configured": policy.notifier.is_some(),
            "herdr_operator_route_configured": config.socket.is_some(),
        }));
    }
    Ok(policies)
}

fn scoped_rows(mut value: Value, group: Option<&str>) -> Value {
    if let (Some(group), Some(rows)) = (group, value.as_array_mut()) {
        rows.retain(|row| row["group"].as_str() == Some(group));
    }
    value
}

/// Report one fleet, or every fleet when explicitly requested.
/// Filters bounded database queries before limits so another fleet cannot hide records.
/// # Errors
/// Invalid group, database, filesystem or status-snapshot decoding failure.
pub async fn report(store: &Store, group: Option<&str>) -> Result<Value> {
    if let Some(group) = group {
        store.group(group).await?;
    }
    let root = store.root();
    let path = root.join("service-status.json");
    let scan: Value = if path.exists() {
        serde_json::from_slice(&std::fs::read(path)?)?
    } else {
        Value::Null
    };
    // Free-form global errors and relay details can mention other fleets. Only
    // the explicit operator overview includes them verbatim.
    let scan = if group.is_some() && !scan.is_null() {
        json!({"checked_at":scan["checked_at"],"observations":scoped_rows(scan["observations"].clone(),group),"service_error":scan.get("error").is_some()})
    } else {
        scan
    };
    let groups = store
        .groups()
        .await?
        .into_iter()
        .filter(|g| group.is_none_or(|name| g.name == name))
        .collect::<Vec<_>>();
    let pending = store
        .pending()
        .await?
        .into_iter()
        .filter(|p| group.is_none_or(|name| p.group_name == name))
        .collect::<Vec<_>>();
    let native = scoped_rows(store.native_status().await?, group);
    let codex = native
        .as_array()
        .into_iter()
        .flatten()
        .filter(|e| e["runtime"] == "codex")
        .collect::<Vec<_>>();
    let mut result = json!({
        "schema_version":1,
        "state_dir":root.canonicalize()?,"group":group,"all_groups":group.is_none(),
        "service_running":service::running(root),"now":now()?,"last_scan_age_seconds":scan["checked_at"].as_i64().map(|t|now().unwrap_or(t).saturating_sub(t).max(0)),"groups":groups,"inboxes":pending,
        "delivery":store.delivery_statuses(group,now()?).await?,
        "notifications":store.notification_status_for(group).await?,
        "runtime_policy":scoped_rows(store.runtime_policy_status().await?,group),
        "followup_policy":policies(store,group).await?,
        "followup":store.followup_status(group,now()?).await?,"native":native,"codex":codex,"attention":store.attention_for(group,now()?).await?,"last_scan":scan
    });
    if group.is_none() {
        let (pending, oldest) = store.outbox_status().await?;
        result["peers"] = serde_json::to_value(store.peers_status().await?)?;
        result["outbox_pending"] = json!(pending);
        result["outbox_oldest"] = json!(oldest);
    }
    Ok(result)
}

/// Render the common health view without loading event history or transport diagnostics.
/// # Errors
/// The group, current identity records or readiness evidence cannot be read.
pub async fn summary(store: &Store, group: Option<&str>) -> Result<String> {
    use crate::states::DeliveryReadiness as State;
    use std::fmt::Write;
    if let Some(group) = group {
        store.group(group).await?;
    }
    let time = now()?;
    let states = store.delivery_statuses(group, time).await?;
    let mut output = format!(
        "{}\nDelivery worker: {}\n",
        group.map_or_else(|| "All groups".to_owned(), |g| format!("Group: {g}")),
        if service::running(store.root()) {
            "running"
        } else {
            "stopped"
        }
    );
    for policy in policies(store, group).await? {
        let name = policy["group"].as_str().unwrap_or("unknown");
        let mode = policy["mode"].as_str().unwrap_or("unknown");
        writeln!(
            output,
            "\n{name} follow-through: {mode}{}",
            if policy["paused"] == true {
                " (group paused)"
            } else {
                ""
            }
        )?;
        if mode == "observe" {
            output.push_str("  Saved observe policy: obligations remain visible; automatic follow-ups are disabled.\n");
        }
        let route = if policy["operator_notifier_configured"] == true {
            "notifier configured; receipt/handling is reported separately"
        } else if policy["herdr_operator_route_configured"] == true {
            "Herdr route configured; independent notifier absent"
        } else {
            "unconfigured; inspect escalations in status --json"
        };
        writeln!(output, "  Operator route: {route}")?;
    }
    if states.is_empty() {
        output.push_str("\nNo agents yet. Start one with: agent-mail run NAME -- claude\n");
    }
    let labels: Vec<_> = states
        .iter()
        .map(|s| {
            if group.is_none() {
                format!("{}/{}", s.group, s.agent)
            } else {
                s.agent.clone()
            }
        })
        .collect();
    let width = labels.iter().map(String::len).max().unwrap_or(5).max(5);
    if !states.is_empty() {
        writeln!(output, "\n{:<width$}  DELIVERY", "AGENT")?;
    }
    for (label, status) in labels.iter().zip(&states) {
        let detail = match status.state {
            State::Verified => "Ready".into(),
            State::Verifying if status.attempts == 0 => {
                "Verifying · waiting for idle client".into()
            }
            State::Verifying => status.next_attempt_at.map_or_else(
                || "Verifying · awaiting acknowledgment".into(),
                |t| format!("Verifying · retry in {}s", (t - time).max(0)),
            ),
            State::Unverified => "Unverified · check pending".into(),
            State::Expired => "Unverified · check expired".into(),
            State::MissingEndpoint => "Unavailable · no runtime attached".into(),
            State::WorkerStopped => "Unavailable · worker stopped".into(),
            State::Paused => "Paused".into(),
            State::NotifyOnly => "Notify-only · no idle wake".into(),
            State::Retired => "Retired".into(),
            State::Unavailable => "Unavailable · connection unhealthy".into(),
            State::RemoteUnsupported => "Remote · check on home machine".into(),
            State::Unknown => "Unknown · diagnostics failed".into(),
        };
        writeln!(output, "{label:<width$}  {detail}")?;
    }
    let followup = store.followup_status(group, time).await?;
    writeln!(
        output,
        "\nFollow-through: {} pending, {} due, {} escalated{}",
        followup["totals"]["pending"],
        followup["totals"]["due"],
        followup["totals"]["escalated"],
        if followup["more"] == true {
            " (detail list truncated; responsible agents can page attention list)"
        } else {
            ""
        }
    )?;
    output.push_str(
        "Delivery readiness does not establish execution capability or task acceptance.\n",
    );
    output.push_str("Task/cleanup details: agent-mail task inspect ID · execution: agent-mail task execution show ID\n");
    output.push_str("\nDetails: agent-mail status --check NAME · JSON: agent-mail status --json");
    if group.is_none() {
        output
            .push_str("\nSelect a group for details: agent-mail --group GROUP status --check NAME");
    }
    Ok(output)
}

/// Read one bounded page visible to the authenticated owner/writer, including terminal work.
/// Model and execution facts are separate observations; this is never admission authority.
/// No retrieval, checkpoint, repair or source mutation is requested.
/// # Errors
/// Invalid actor, foreign home, malformed graph, or unavailable owner projection fails explicitly.
pub async fn task_tracking(
    store: &Store,
    actor: &crate::store::Mailbox,
    after: &str,
    limit: u32,
) -> Result<Value> {
    let started_at = now()?;
    let page = store.task_tracking_page(actor, after, limit).await?;
    let model_observed_at = now()?;
    let mut items = Vec::with_capacity(page.items.len());
    for task in page.items {
        let execution = store.execution_inspect(actor, &task.work.id).await?;
        items.push(json!({"task":task,"execution":execution,"execution_observed_at":now()?}));
    }
    Ok(json!({
        "schema_version":1,"group":actor.group_name,"agent":actor.name,
        "started_at":started_at,"model_observed_at":model_observed_at,"finished_at":now()?,
        "atomic_snapshot":false,"coverage":"owned_or_written_home_tasks_all_states",
        "items":items,"next_cursor":page.next_cursor,"has_more":page.has_more,
    }))
}

/// Render only the returned task page; no omitted row is treated as absent.
/// # Errors
/// Formatting output fails.
pub fn task_tracking_summary(page: &Value) -> Result<String> {
    use std::fmt::Write;
    let mut output = String::from(
        "\nTask tracking (this agent's home tasks; separate model/execution observations):\n",
    );
    let items = page["items"].as_array();
    if items.is_none_or(Vec::is_empty) {
        output.push_str("  No tasks in this page.\n");
    }
    for item in items.into_iter().flatten() {
        let task = &item["task"];
        let work = &task["work"];
        let execution = &item["execution"];
        let text = |value: &Value| value.as_str().unwrap_or("unknown").to_owned();
        writeln!(
            output,
            "  {} v{} · {} · {} · owner {} · writer {}",
            text(&work["id"]),
            work["version"],
            text(&work["state"]),
            if task["model"].is_null() {
                "legacy/untracked"
            } else {
                "contracted"
            },
            text(&work["owner"]),
            text(&work["writer"])
        )?;
        writeln!(output, "    Next: {}", text(&work["next_action"]))?;
        if !task["readiness"]["causes"]
            .as_array()
            .is_some_and(Vec::is_empty)
        {
            writeln!(output, "    Model holds: {}", task["readiness"]["causes"])?;
        }
        if execution["attempt"].is_object() {
            writeln!(
                output,
                "    Attempt: {} · {} (retained even for terminal business state)",
                execution["attempt"]["attempt"], execution["attempt_state"]
            )?;
        }
        writeln!(
            output,
            "    Execution hold: {} · next examination: {}",
            task["execution_hold"], execution["due_at"]
        )?;
        if !execution["causes"].as_array().is_some_and(Vec::is_empty) {
            writeln!(output, "    Execution causes: {}", execution["causes"])?;
        }
    }
    if page["has_more"] == true {
        writeln!(
            output,
            "  More tasks: task list --details --after {}",
            page["next_cursor"]
        )?;
    }
    output.push_str("  Inspect a task for full budget/cost details; these observations grant no execution permission.\n");
    Ok(output)
}
