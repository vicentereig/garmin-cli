//! Weight and body composition commands for garmin-cli

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::error::Result;
use chrono::{Duration, Local};

use super::auth::refresh_token;

/// List weight entries in a date range
pub async fn list(from: Option<String>, to: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

    // Default to last 30 days if no dates provided
    let today = Local::now().date_naive();
    let end_date = to.unwrap_or_else(|| today.format("%Y-%m-%d").to_string());
    let start_date = from.unwrap_or_else(|| (today - Duration::days(30)).format("%Y-%m-%d").to_string());

    let path = format!(
        "/weight-service/weight/dateRange?startDate={}&endDate={}",
        start_date, end_date
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    // Extract weight entries from response
    if let Some(entries) = data.get("dateWeightList").and_then(|v| v.as_array()) {
        if entries.is_empty() {
            println!("No weight entries found.");
            return Ok(());
        }

        println!("{:<12} {:>10} {:>10} {:>8} {:>8}", "Date", "Weight", "BMI", "Fat %", "Muscle %");
        println!("{}", "-".repeat(55));

        for entry in entries {
            let date = entry.get("calendarDate")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let weight = entry.get("weight")
                .and_then(|v| v.as_f64())
                .map(|w| format!("{:.1} kg", w / 1000.0))
                .unwrap_or_else(|| "-".to_string());

            let bmi = entry.get("bmi")
                .and_then(|v| v.as_f64())
                .map(|b| format!("{:.1}", b))
                .unwrap_or_else(|| "-".to_string());

            let body_fat = entry.get("bodyFat")
                .and_then(|v| v.as_f64())
                .map(|f| format!("{:.1}", f))
                .unwrap_or_else(|| "-".to_string());

            let muscle_mass = entry.get("muscleMass")
                .and_then(|v| v.as_f64())
                .map(|m| format!("{:.1}", m / 1000.0))
                .unwrap_or_else(|| "-".to_string());

            println!("{:<12} {:>10} {:>10} {:>8} {:>8}", date, weight, bmi, body_fat, muscle_mass);
        }

        println!("\nShowing {} entries from {} to {}", entries.len(), start_date, end_date);
    } else {
        // Try alternative response format
        println!("{}", serde_json::to_string_pretty(&data)?);
    }

    Ok(())
}

/// Add a weight entry
pub async fn add(weight: f64, unit: &str, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

    // Convert to grams (Garmin API uses grams)
    let weight_grams = match unit.to_lowercase().as_str() {
        "kg" => (weight * 1000.0) as i64,
        "lbs" | "lb" => (weight * 453.592) as i64,
        _ => {
            return Err(crate::error::GarminError::invalid_response(
                "Invalid unit. Use 'kg' or 'lbs'"
            ));
        }
    };

    let today = Local::now().format("%Y-%m-%d").to_string();
    let timestamp = Local::now().timestamp_millis();

    let body = serde_json::json!({
        "dateTimestamp": timestamp,
        "gmtTimestamp": timestamp,
        "weight": weight_grams,
        "unitKey": "kg",
        "calendarDate": today
    });

    let path = "/weight-service/user-weight";

    let response = client.post_json(&oauth2, path, &body).await?;

    println!("Weight entry added: {:.1} {} on {}", weight, unit, today);

    if let Some(id) = response.get("samplePk").and_then(|v| v.as_i64()) {
        println!("Entry ID: {}", id);
    }

    Ok(())
}

/// Get latest weight
pub async fn latest(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

    let path = "/weight-service/weight/latest";

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    let date = data.get("calendarDate")
        .and_then(|v| v.as_str())
        .unwrap_or("-");

    let weight = data.get("weight")
        .and_then(|v| v.as_f64())
        .map(|w| w / 1000.0);

    let bmi = data.get("bmi").and_then(|v| v.as_f64());
    let body_fat = data.get("bodyFat").and_then(|v| v.as_f64());

    println!("Latest Weight Entry");
    println!("{}", "-".repeat(30));
    println!("Date:     {}", date);
    println!("Weight:   {}", weight.map(|w| format!("{:.1} kg", w)).unwrap_or("-".to_string()));
    println!("BMI:      {}", bmi.map(|b| format!("{:.1}", b)).unwrap_or("-".to_string()));
    println!("Body Fat: {}", body_fat.map(|f| format!("{:.1}%", f)).unwrap_or("-".to_string()));

    Ok(())
}
