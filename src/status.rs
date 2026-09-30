//! Keep fleet diagnostics scoped; shared service details are explicitly installation-wide.
use crate::{now, service, store::Store};
use anyhow::Result;
use serde_json::{Value, json};

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
        "state_dir":root.canonicalize()?,"group":group,"all_groups":group.is_none(),
        "service_running":service::running(root),"now":now()?,"last_scan_age_seconds":scan["checked_at"].as_i64().map(|t|now().unwrap_or(t).saturating_sub(t).max(0)),"groups":groups,"inboxes":pending,
        "delivery":store.delivery_statuses(group,now()?).await?,
        "notifications":store.notification_status_for(group).await?,
        "runtime_policy":scoped_rows(store.runtime_policy_status().await?,group),
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
    if states.is_empty() {
        output.push_str("\nNo agents yet. Start one with: agent-mail run NAME -- claude");
        return Ok(output);
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
    writeln!(output, "\n{:<width$}  DELIVERY", "AGENT")?;
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
    output.push_str("\nDetails: agent-mail status --check NAME · JSON: agent-mail status --json");
    if group.is_none() {
        output
            .push_str("\nSelect a group for details: agent-mail --group GROUP status --check NAME");
    }
    Ok(output)
}
