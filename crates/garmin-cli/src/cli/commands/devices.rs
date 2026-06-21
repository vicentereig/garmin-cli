//! Device commands for garmin-cli

use std::collections::HashMap;

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::db::models::Activity;
use crate::error::Result;
use crate::storage::default_storage_path;
use crate::storage::ParquetStore;

use super::auth::refresh_token;

/// List registered devices
pub async fn list(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (_, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new();

    let path = "/device-service/deviceregistration/devices";

    let devices: Vec<serde_json::Value> = client.get_json(&oauth2, path).await?;

    if devices.is_empty() {
        println!("No devices found.");
        return Ok(());
    }

    println!(
        "{:<20} {:<25} {:<15} {:<12}",
        "Device", "Model", "Software", "Last Sync"
    );
    println!("{}", "-".repeat(75));

    for device in &devices {
        let name = device
            .get("displayName")
            .or_else(|| device.get("deviceTypeName"))
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown");

        let model = device
            .get("partNumber")
            .and_then(|v| v.as_str())
            .unwrap_or("-");

        let software = device
            .get("currentFirmwareVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("-");

        let last_sync = device
            .get("lastSyncTime")
            .and_then(|v| v.as_str())
            .map(|s| s.split('T').next().unwrap_or(s))
            .unwrap_or("-");

        println!(
            "{:<20} {:<25} {:<15} {:<12}",
            truncate(name, 19),
            truncate(model, 24),
            truncate(software, 14),
            last_sync
        );
    }

    println!("\nTotal: {} device(s)", devices.len());

    Ok(())
}

/// Get device info
pub async fn get(device_id: &str, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (_, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new();

    let path = format!(
        "/device-service/deviceservice/device-info/settings/{}",
        device_id
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("{}", serde_json::to_string_pretty(&data)?);

    Ok(())
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Show device history from synced activities
pub async fn history(storage_path: Option<String>) -> Result<()> {
    let storage_path = storage_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    let activities_path = storage_path.join("activities");
    if !activities_path.exists() {
        println!("No activities found at: {}", activities_path.display());
        println!("Run 'garmin sync run' to sync your activities first.");
        return Ok(());
    }

    let store = ParquetStore::new(&storage_path);
    let mut devices = collect_device_history(store.read_activities()?);

    if devices.is_empty() {
        println!("No device history found in synced activities.");
        println!("Make sure you have synced activities with 'garmin sync run'.");
        return Ok(());
    }

    println!("Device history (from synced activities):\n");
    println!(
        "{:<15} {:<12} {:<12} {:<12} {:>10}",
        "Device ID", "Type PK", "First Used", "Last Used", "Activities"
    );
    println!("{}", "-".repeat(65));

    devices.sort_by_key(|device| device.first_activity);

    for device in &devices {
        let device_type_str = device
            .device_type
            .map(|device_type| device_type.to_string())
            .unwrap_or_else(|| "-".to_string());
        let first_date = device.first_activity.date_naive().to_string();
        let last_date = device.last_activity.date_naive().to_string();

        println!(
            "{:<15} {:<12} {:<12} {:<12} {:>10}",
            truncate(&device.device_id, 14),
            truncate(&device_type_str, 11),
            first_date,
            last_date,
            device.activity_count
        );
    }

    println!("\nTotal: {} device(s) found", devices.len());

    // Show a note about device type lookup
    println!("\nNote: Device Type PK can be looked up in Garmin's device database.");
    println!("Common types: 37010=Forerunner 955, 30380=Edge 810, etc.");

    Ok(())
}

#[derive(Debug)]
struct DeviceHistory {
    device_id: String,
    device_type: Option<i64>,
    first_activity: chrono::DateTime<chrono::Utc>,
    last_activity: chrono::DateTime<chrono::Utc>,
    activity_count: usize,
}

fn collect_device_history(activities: Vec<Activity>) -> Vec<DeviceHistory> {
    let mut devices: HashMap<(String, Option<i64>), DeviceHistory> = HashMap::new();

    for activity in activities {
        let Some(start_time) = activity.start_time_local else {
            continue;
        };
        let Some((device_id, device_type)) = activity_device_metadata(&activity) else {
            continue;
        };

        let entry = devices
            .entry((device_id.clone(), device_type))
            .or_insert_with(|| DeviceHistory {
                device_id,
                device_type,
                first_activity: start_time,
                last_activity: start_time,
                activity_count: 0,
            });

        entry.first_activity = entry.first_activity.min(start_time);
        entry.last_activity = entry.last_activity.max(start_time);
        entry.activity_count += 1;
    }

    devices.into_values().collect()
}

fn activity_device_metadata(activity: &Activity) -> Option<(String, Option<i64>)> {
    let metadata = activity
        .raw_json
        .as_ref()?
        .get("metadataDTO")?
        .get("deviceMetaDataDTO")?;

    let device_id = metadata.get("deviceId").and_then(json_value_to_string)?;
    let device_type = metadata.get("deviceTypePk").and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
    });

    Some((device_id, device_type))
}

fn json_value_to_string(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
}
