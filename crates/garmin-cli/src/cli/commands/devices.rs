//! Device commands for garmin-cli

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::db::default_db_path;
use crate::error::Result;
use crate::Database;

use super::auth::refresh_token;

/// List registered devices
pub async fn list(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

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
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

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
pub async fn history(db_path: Option<String>) -> Result<()> {
    let db_path = db_path.unwrap_or_else(|| default_db_path().unwrap());

    if !std::path::Path::new(&db_path).exists() {
        println!("No database found at: {}", db_path);
        println!("Run 'garmin sync run' to sync your activities first.");
        return Ok(());
    }

    let db = Database::open(&db_path)?;
    let conn = db.connection();
    let conn = conn.lock().unwrap();

    // Query unique devices from activity metadata
    let mut stmt = conn
        .prepare(
            "SELECT
                json_extract_string(raw_json, '$.metadataDTO.deviceMetaDataDTO.deviceId') as device_id,
                json_extract(raw_json, '$.metadataDTO.deviceMetaDataDTO.deviceTypePk') as device_type,
                MIN(start_time_local) as first_activity,
                MAX(start_time_local) as last_activity,
                COUNT(*) as activity_count
            FROM activities
            WHERE raw_json IS NOT NULL
              AND json_extract_string(raw_json, '$.metadataDTO.deviceMetaDataDTO.deviceId') IS NOT NULL
            GROUP BY 1, 2
            ORDER BY first_activity",
        )
        .map_err(|e| crate::GarminError::Database(e.to_string()))?;

    #[allow(clippy::type_complexity)]
    let devices: Vec<(
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
        i64,
    )> = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|e| crate::GarminError::Database(e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();

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

    for (device_id, device_type, first, last, count) in &devices {
        let device_id_str = device_id.as_deref().unwrap_or("-");
        let device_type_str = device_type
            .map(|t| t.to_string())
            .unwrap_or_else(|| "-".to_string());
        let first_date = first
            .as_ref()
            .and_then(|s| s.split(' ').next())
            .unwrap_or("-");
        let last_date = last
            .as_ref()
            .and_then(|s| s.split(' ').next())
            .unwrap_or("-");

        println!(
            "{:<15} {:<12} {:<12} {:<12} {:>10}",
            truncate(device_id_str, 14),
            truncate(&device_type_str, 11),
            first_date,
            last_date,
            count
        );
    }

    println!("\nTotal: {} device(s) found", devices.len());

    // Show a note about device type lookup
    println!("\nNote: Device Type PK can be looked up in Garmin's device database.");
    println!("Common types: 37010=Forerunner 955, 30380=Edge 810, etc.");

    Ok(())
}
