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
        "service_running":service::running(root),"now":now()?,"groups":groups,"inboxes":pending,
        "notifications":store.notification_status_for(group).await?,
        "runtime_policy":scoped_rows(store.runtime_policy_status().await?,group),
        "native":native,"codex":codex,"attention":store.attention_for(group,now()?).await?,"last_scan":scan
    });
    if group.is_none() {
        let (pending, oldest) = store.outbox_status().await?;
        result["peers"] = serde_json::to_value(store.peers_status().await?)?;
        result["outbox_pending"] = json!(pending);
        result["outbox_oldest"] = json!(oldest);
    }
    Ok(result)
}
