//! Activity commands for garmin-cli

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::error::{GarminError, Result};
use crate::models::ActivitySummary;
use std::path::Path;

use super::auth::refresh_token;

/// List activities
pub async fn list(limit: u32, start: u32, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let path = format!(
        "/activitylist-service/activities/search/activities?limit={}&start={}",
        limit, start
    );

    let activities: Vec<ActivitySummary> = client.get_json(&oauth2, &path).await?;

    if activities.is_empty() {
        println!("No activities found.");
        return Ok(());
    }

    // Print header
    println!(
        "{:<12} {:<10} {:<15} {:>10} {:>12} {:>8}",
        "ID", "Date", "Type", "Distance", "Duration", "HR"
    );
    println!("{}", "-".repeat(75));

    // Print each activity
    for activity in &activities {
        let distance = activity
            .distance_km()
            .map(|d| format!("{:.2} km", d))
            .unwrap_or_else(|| "-".to_string());

        let hr = activity
            .average_hr
            .map(|h| format!("{:.0}", h))
            .unwrap_or_else(|| "-".to_string());

        println!(
            "{:<12} {:<10} {:<15} {:>10} {:>12} {:>8}",
            activity.activity_id,
            activity.date(),
            truncate(&activity.type_key(), 15),
            distance,
            activity.duration_formatted(),
            hr
        );
    }

    println!("\nShowing {} activities", activities.len());

    Ok(())
}

/// Get activity details
pub async fn get(id: u64, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let path = format!("/activity-service/activity/{}", id);

    let activity: serde_json::Value = client.get_json(&oauth2, &path).await?;

    // Pretty print the JSON
    println!("{}", serde_json::to_string_pretty(&activity)?);

    Ok(())
}

/// Download activity file
pub async fn download(
    id: u64,
    format: &str,
    output: Option<String>,
    profile: Option<String>,
) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

    // Build path based on format
    let (path, extension) = match format.to_lowercase().as_str() {
        "fit" => (format!("/download-service/files/activity/{}", id), "zip"),
        "gpx" => (
            format!("/download-service/export/gpx/activity/{}", id),
            "gpx",
        ),
        "tcx" => (
            format!("/download-service/export/tcx/activity/{}", id),
            "tcx",
        ),
        "kml" => (
            format!("/download-service/export/kml/activity/{}", id),
            "kml",
        ),
        _ => {
            return Err(GarminError::invalid_response(format!(
                "Unknown format: {}. Supported: fit, gpx, tcx, kml",
                format
            )));
        }
    };

    println!(
        "Downloading activity {} as {}...",
        id,
        format.to_uppercase()
    );

    let bytes = client.download(&oauth2, &path).await?;

    // Determine output path
    let output_path = output.unwrap_or_else(|| format!("activity_{}.{}", id, extension));

    // Write to file
    tokio::fs::write(&output_path, &bytes)
        .await
        .map_err(|e| GarminError::invalid_response(format!("Failed to write file: {}", e)))?;

    println!("Saved to: {}", output_path);
    println!("Size: {} bytes", bytes.len());

    Ok(())
}

/// Upload activity file
pub async fn upload(file: &str, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let file_path = Path::new(file);

    if !file_path.exists() {
        return Err(GarminError::invalid_response(format!(
            "File not found: {}",
            file
        )));
    }

    let client = GarminClient::new(&oauth1.domain);

    println!("Uploading {}...", file);

    let result = client
        .upload(&oauth2, "/upload-service/upload/.fit", file_path)
        .await?;

    // Try to extract useful info from response
    if let Some(detailed) = result.get("detailedImportResult") {
        if let Some(successes) = detailed.get("successes").and_then(|s| s.as_array()) {
            if !successes.is_empty() {
                println!("Upload successful!");
                for success in successes {
                    if let Some(id) = success.get("internalId").and_then(|i| i.as_u64()) {
                        println!("Created activity ID: {}", id);
                    }
                }
                return Ok(());
            }
        }

        if let Some(failures) = detailed.get("failures").and_then(|f| f.as_array()) {
            if !failures.is_empty() {
                println!("Upload had failures:");
                for failure in failures {
                    println!("  {}", failure);
                }
            }
        }
    }

    // Fallback: print full response
    println!("Response: {}", serde_json::to_string_pretty(&result)?);

    Ok(())
}

/// Truncate string to max length
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("very long string here", 10), "very lo...");
    }
}
