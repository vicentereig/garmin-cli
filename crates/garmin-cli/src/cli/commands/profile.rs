//! Profile commands for garmin-cli

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::error::Result;

use super::auth::refresh_token;

/// Show user profile
pub async fn show(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

    let social: serde_json::Value = client.get_json(&oauth2, "/userprofile-service/socialProfile").await?;

    println!("User Profile");
    println!("{}", "-".repeat(40));

    if let Some(name) = social.get("displayName").and_then(|v| v.as_str()) {
        println!("Display Name: {}", name);
    }

    if let Some(name) = social.get("fullName").and_then(|v| v.as_str()) {
        println!("Full Name:    {}", name);
    }

    if let Some(location) = social.get("location").and_then(|v| v.as_str()) {
        if !location.is_empty() {
            println!("Location:     {}", location);
        }
    }

    if let Some(id) = social.get("userProfilePK").and_then(|v| v.as_i64()) {
        println!("Profile ID:   {}", id);
    }

    Ok(())
}

/// Show user settings
pub async fn settings(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);

    let settings: serde_json::Value = client.get_json(&oauth2, "/userprofile-service/userprofile/user-settings").await?;

    println!("User Settings");
    println!("{}", "-".repeat(40));

    if let Some(units) = settings.get("measurementSystem").and_then(|v| v.as_str()) {
        println!("Units:        {}", units);
    }

    if let Some(format) = settings.get("dateFormat").and_then(|v| v.as_str()) {
        println!("Date Format:  {}", format);
    }

    if let Some(format) = settings.get("timeFormat").and_then(|v| v.as_str()) {
        println!("Time Format:  {}", format);
    }

    if let Some(tz) = settings.get("timezone").and_then(|v| v.as_str()) {
        println!("Timezone:     {}", tz);
    }

    if let Some(start) = settings.get("firstDayOfWeek").and_then(|v| v.as_str()) {
        println!("Week Starts:  {}", start);
    }

    // Physical info
    println!();
    println!("Physical");
    println!("{}", "-".repeat(40));

    if let Some(height) = settings.get("height").and_then(|v| v.as_f64()) {
        println!("Height:       {:.0} cm", height);
    }

    if let Some(weight) = settings.get("weight").and_then(|v| v.as_f64()) {
        println!("Weight:       {:.1} kg", weight / 1000.0);
    }

    if let Some(gender) = settings.get("gender").and_then(|v| v.as_str()) {
        println!("Gender:       {}", gender);
    }

    if let Some(dob) = settings.get("birthDate").and_then(|v| v.as_str()) {
        println!("Birth Date:   {}", dob);
    }

    Ok(())
}
