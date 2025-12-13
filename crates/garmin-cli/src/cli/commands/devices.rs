//! Device commands for garmin-cli

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::error::Result;

use super::auth::refresh_token;

/// List registered devices
pub async fn list(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

    let path = "/device-service/deviceregistration/devices";

    let devices: Vec<serde_json::Value> = client.get_json(&oauth2, &path).await?;

    if devices.is_empty() {
        println!("No devices found.");
        return Ok(());
    }

    println!("{:<20} {:<25} {:<15} {:<12}", "Device", "Model", "Software", "Last Sync");
    println!("{}", "-".repeat(75));

    for device in &devices {
        let name = device.get("displayName")
            .or_else(|| device.get("deviceTypeName"))
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown");

        let model = device.get("partNumber")
            .and_then(|v| v.as_str())
            .unwrap_or("-");

        let software = device.get("currentFirmwareVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("-");

        let last_sync = device.get("lastSyncTime")
            .and_then(|v| v.as_str())
            .map(|s| s.split('T').next().unwrap_or(s))
            .unwrap_or("-");

        println!("{:<20} {:<25} {:<15} {:<12}",
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

    let path = format!("/device-service/deviceservice/device-info/settings/{}", device_id);

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
