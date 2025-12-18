//! Health commands for garmin-cli

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::error::Result;
use chrono::{Duration, Local};

use super::auth::refresh_token;

/// Get daily summary for a date
pub async fn summary(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let display_name = get_display_name(&client, &oauth2).await?;
    let path = format!(
        "/usersummary-service/usersummary/daily/{}?calendarDate={}",
        display_name, date
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Daily Summary for {}", date);
    println!("{}", "-".repeat(40));

    if let Some(steps) = data.get("totalSteps").and_then(|v| v.as_i64()) {
        let goal = data.get("dailyStepGoal").and_then(|v| v.as_i64()).unwrap_or(10000);
        let pct = (steps as f64 / goal as f64 * 100.0) as i64;
        println!("Steps:           {:>6} / {} ({}%)", steps, goal, pct);
    }

    if let Some(cals) = data.get("totalKilocalories").and_then(|v| v.as_f64()) {
        println!("Calories:        {:>6.0} kcal", cals);
    }

    if let Some(active) = data.get("activeKilocalories").and_then(|v| v.as_f64()) {
        println!("Active Calories: {:>6.0} kcal", active);
    }

    if let Some(dist) = data.get("totalDistanceMeters").and_then(|v| v.as_i64()) {
        println!("Distance:        {:>6.2} km", dist as f64 / 1000.0);
    }

    if let Some(floors) = data.get("floorsAscended").and_then(|v| v.as_i64()) {
        let goal = data.get("floorsAscendedGoal").and_then(|v| v.as_i64()).unwrap_or(10);
        println!("Floors:          {:>6} / {}", floors, goal);
    }

    if let Some(mins) = data.get("highlyActiveSeconds").and_then(|v| v.as_i64()) {
        println!("Active Minutes:  {:>6}", mins / 60);
    }

    if let Some(stress) = data.get("averageStressLevel").and_then(|v| v.as_i64()) {
        println!("Avg Stress:      {:>6}", stress);
    }

    if let Some(rhr) = data.get("restingHeartRate").and_then(|v| v.as_i64()) {
        println!("Resting HR:      {:>6} bpm", rhr);
    }

    Ok(())
}

/// Get user's display name from profile
async fn get_display_name(client: &GarminClient, oauth2: &crate::client::OAuth2Token) -> Result<String> {
    // Try social profile endpoint
    let profile: serde_json::Value = client.get_json(oauth2, "/userprofile-service/socialProfile").await?;
    profile.get("displayName")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| crate::error::GarminError::invalid_response("Could not get display name"))
}

/// Get sleep data for a date
pub async fn sleep(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    // Get display name for sleep endpoint
    let display_name = get_display_name(&client, &oauth2).await?;

    let path = format!(
        "/wellness-service/wellness/dailySleepData/{}?date={}&nonSleepBufferMinutes=60",
        display_name, date
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    // Sleep data is nested under dailySleepDTO
    let sleep_dto = data.get("dailySleepDTO").unwrap_or(&data);

    print_sleep_summary(sleep_dto);

    Ok(())
}

/// Get sleep data for multiple days
pub async fn sleep_range(days: u32, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();

    // Get display name for sleep endpoint
    let display_name = get_display_name(&client, &oauth2).await?;

    println!("{:<12} {:>8} {:>8} {:>8} {:>8} {:>6}", "Date", "Total", "Deep", "Light", "REM", "Score");
    println!("{}", "-".repeat(58));

    for i in 0..days {
        let date = today - Duration::days(i as i64);
        let path = format!(
            "/wellness-service/wellness/dailySleepData/{}?date={}&nonSleepBufferMinutes=60",
            display_name, date
        );

        match client.get_json::<serde_json::Value>(&oauth2, &path).await {
            Ok(data) => {
                // Sleep data is nested under dailySleepDTO
                let sleep_dto = data.get("dailySleepDTO").unwrap_or(&data);

                // Calculate total from components since sleepTimeSeconds might not exist
                let deep = sleep_dto.get("deepSleepSeconds").and_then(|v| v.as_i64());
                let light = sleep_dto.get("lightSleepSeconds").and_then(|v| v.as_i64());
                let rem = sleep_dto.get("remSleepSeconds").and_then(|v| v.as_i64());

                let total_secs = deep.unwrap_or(0) + light.unwrap_or(0) + rem.unwrap_or(0);
                let total = if total_secs > 0 {
                    format_duration(Some(total_secs))
                } else {
                    "-".to_string()
                };

                let score = sleep_dto.get("sleepScores")
                    .and_then(|s| s.get("overall"))
                    .and_then(|o| o.get("value"))
                    .and_then(|v| v.as_i64())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string());

                println!("{:<12} {:>8} {:>8} {:>8} {:>8} {:>6}", date, total,
                    format_duration(deep), format_duration(light), format_duration(rem), score);
            }
            Err(_) => {
                println!("{:<12} {:>8}", date, "no data");
            }
        }
    }

    Ok(())
}

/// Get stress data for a date
pub async fn stress(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    // dailyStress endpoint: /wellness-service/wellness/dailyStress/{date}
    let path = format!("/wellness-service/wellness/dailyStress/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    let avg = data.get("avgStressLevel").and_then(|v| v.as_i64());
    let max = data.get("maxStressLevel").and_then(|v| v.as_i64());

    println!("Stress for {}", date);
    println!("{}", "-".repeat(50));
    println!("Average: {}  Max: {}",
        avg.map(|v| v.to_string()).unwrap_or("-".to_string()),
        max.map(|v| v.to_string()).unwrap_or("-".to_string()));
    println!();

    // Parse hourly stress from stressValuesArray
    if let Some(values) = data.get("stressValuesArray").and_then(|v| v.as_array()) {
        // Group by hour and calculate averages
        let mut hourly: std::collections::BTreeMap<u32, Vec<i64>> = std::collections::BTreeMap::new();

        for entry in values {
            if let Some(arr) = entry.as_array() {
                if arr.len() >= 2 {
                    let timestamp_ms = arr[0].as_i64().unwrap_or(0);
                    let stress_val = arr[1].as_i64().unwrap_or(-1);

                    if stress_val >= 0 {
                        // Convert timestamp to hour (local time)
                        let dt = chrono::DateTime::from_timestamp_millis(timestamp_ms)
                            .map(|dt| dt.with_timezone(&chrono::Local));

                        if let Some(local_dt) = dt {
                            let hour = local_dt.format("%H").to_string().parse::<u32>().unwrap_or(0);
                            hourly.entry(hour).or_default().push(stress_val);
                        }
                    }
                }
            }
        }

        println!("{:<6} {:>6} {:>6} {:>6}  {}", "Hour", "Avg", "Min", "Max", "Level");
        println!("{}", "-".repeat(50));

        for (hour, vals) in &hourly {
            if !vals.is_empty() {
                let avg: i64 = vals.iter().sum::<i64>() / vals.len() as i64;
                let min = *vals.iter().min().unwrap_or(&0);
                let max = *vals.iter().max().unwrap_or(&0);

                let bar = stress_bar(avg);
                let level = stress_level(avg);

                println!("{:02}:00  {:>6} {:>6} {:>6}  {} {}", hour, avg, min, max, bar, level);
            }
        }
    }

    Ok(())
}

fn stress_bar(level: i64) -> String {
    let blocks = (level / 10) as usize;
    let bar: String = "█".repeat(blocks.min(10));
    format!("{:<10}", bar)
}

fn stress_level(level: i64) -> &'static str {
    match level {
        0..=25 => "Rest",
        26..=50 => "Low",
        51..=75 => "Medium",
        _ => "High",
    }
}

/// Get stress data for multiple days
pub async fn stress_range(days: u32, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();

    println!("{:<12} {:>6} {:>6}", "Date", "Avg", "Max");
    println!("{}", "-".repeat(28));

    for i in 0..days {
        let date = today - Duration::days(i as i64);
        // dailyStress endpoint: /wellness-service/wellness/dailyStress/{date}
        let path = format!("/wellness-service/wellness/dailyStress/{}", date);

        match client.get_json::<serde_json::Value>(&oauth2, &path).await {
            Ok(data) => {
                let avg = data.get("avgStressLevel")
                    .and_then(|v| v.as_i64())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let max = data.get("maxStressLevel")
                    .and_then(|v| v.as_i64())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string());

                println!("{:<12} {:>6} {:>6}", date, avg, max);
            }
            Err(_) => {
                println!("{:<12} {:>6}", date, "-");
            }
        }
    }

    Ok(())
}

/// Get body battery data for a date
pub async fn body_battery(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!(
        "/wellness-service/wellness/bodyBattery/reports/daily?startDate={}&endDate={}",
        date, date
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Body Battery for {}", date);
    println!("{}", "-".repeat(40));

    // Body battery returns an array of daily values
    if let Some(arr) = data.as_array() {
        for day in arr {
            if let Some(charged) = day.get("charged").and_then(|v| v.as_i64()) {
                println!("Charged:    +{}", charged);
            }
            if let Some(drained) = day.get("drained").and_then(|v| v.as_i64()) {
                println!("Drained:    -{}", drained);
            }
            if let Some(start) = day.get("startTimestampGMT").and_then(|v| v.as_i64()) {
                if let Some(dt) = chrono::DateTime::from_timestamp_millis(start) {
                    let local = dt.with_timezone(&chrono::Local);
                    if let Some(val) = day.get("startValue").and_then(|v| v.as_i64()) {
                        println!("Start:      {} at {}", val, local.format("%H:%M"));
                    }
                }
            }
            if let Some(end) = day.get("endTimestampGMT").and_then(|v| v.as_i64()) {
                if let Some(dt) = chrono::DateTime::from_timestamp_millis(end) {
                    let local = dt.with_timezone(&chrono::Local);
                    if let Some(val) = day.get("endValue").and_then(|v| v.as_i64()) {
                        println!("End:        {} at {}", val, local.format("%H:%M"));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Get body battery data for multiple days
pub async fn body_battery_range(days: u32, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();
    let start_date = today - Duration::days(days as i64 - 1);

    let path = format!(
        "/wellness-service/wellness/bodyBattery/reports/daily?startDate={}&endDate={}",
        start_date, today
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("{:<12} {:>8} {:>8}", "Date", "Charged", "Drained");
    println!("{}", "-".repeat(32));

    if let Some(arr) = data.as_array() {
        // API already returns in date order, just reverse to show most recent first
        for day in arr.iter().rev() {
            // Date field varies - try date, then calendarDate
            let date_str = day.get("date")
                .and_then(|v| v.as_str())
                .or_else(|| day.get("calendarDate").and_then(|v| v.as_str()))
                .unwrap_or("-");

            let charged = day.get("charged").and_then(|v| v.as_i64())
                .map(|v| format!("+{}", v)).unwrap_or_else(|| "-".to_string());
            let drained = day.get("drained").and_then(|v| v.as_i64())
                .map(|v| format!("-{}", v)).unwrap_or_else(|| "-".to_string());

            println!("{:<12} {:>8} {:>8}", date_str, charged, drained);
        }
    }

    Ok(())
}

/// Get heart rate data for a date
pub async fn heart_rate(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let display_name = get_display_name(&client, &oauth2).await?;
    let path = format!(
        "/wellness-service/wellness/dailyHeartRate/{}?date={}",
        display_name, date
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Heart Rate for {}", date);
    println!("{}", "-".repeat(40));

    if let Some(rhr) = data.get("restingHeartRate").and_then(|v| v.as_i64()) {
        println!("Resting HR:  {} bpm", rhr);
    }

    if let Some(max) = data.get("maxHeartRate").and_then(|v| v.as_i64()) {
        println!("Max HR:      {} bpm", max);
    }

    if let Some(min) = data.get("minHeartRate").and_then(|v| v.as_i64()) {
        println!("Min HR:      {} bpm", min);
    }

    // Show HR zones if available
    if let Some(zones) = data.get("heartRateZones").and_then(|v| v.as_array()) {
        println!();
        println!("Heart Rate Zones:");
        for zone in zones {
            if let (Some(zone_num), Some(mins)) = (
                zone.get("zoneNumber").and_then(|v| v.as_i64()),
                zone.get("secsInZone").and_then(|v| v.as_i64()),
            ) {
                if mins > 0 {
                    println!("  Zone {}: {}m", zone_num, mins / 60);
                }
            }
        }
    }

    Ok(())
}

fn resolve_date(date: Option<String>) -> Result<String> {
    match date {
        Some(d) => Ok(d),
        None => Ok(Local::now().format("%Y-%m-%d").to_string()),
    }
}

fn format_duration(seconds: Option<i64>) -> String {
    match seconds {
        Some(s) => {
            let hours = s / 3600;
            let mins = (s % 3600) / 60;
            format!("{}h{:02}m", hours, mins)
        }
        None => "-".to_string(),
    }
}


/// Get calorie data for a date range
pub async fn calories(days: Option<u32>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let display_name = get_display_name(&client, &oauth2).await?;
    let today = Local::now().date_naive();
    let num_days = days.unwrap_or(10);

    println!("{:<12} {:>8} {:>8} {:>8} {:>8}", "Date", "Total", "Active", "BMR", "Food");
    println!("{}", "-".repeat(50));

    let mut total_cals: i64 = 0;
    let mut total_active: i64 = 0;
    let mut count = 0;

    for i in 0..num_days {
        let date = today - Duration::days(i as i64);
        let path = format!(
            "/usersummary-service/usersummary/daily/{}?calendarDate={}",
            display_name, date.format("%Y-%m-%d")
        );

        match client.get_json::<serde_json::Value>(&oauth2, &path).await {
            Ok(data) => {
                let total = data.get("totalKilocalories").and_then(|v| v.as_f64()).unwrap_or(0.0) as i64;
                let active = data.get("activeKilocalories").and_then(|v| v.as_f64()).unwrap_or(0.0) as i64;
                let bmr = data.get("bmrKilocalories").and_then(|v| v.as_f64()).unwrap_or(0.0) as i64;
                let food = data.get("consumedKilocalories").and_then(|v| v.as_f64()).map(|f| f as i64);

                let food_str = food.map(|f| format!("{}", f)).unwrap_or("-".to_string());

                println!("{:<12} {:>8} {:>8} {:>8} {:>8}", date, total, active, bmr, food_str);

                total_cals += total;
                total_active += active;
                count += 1;
            }
            Err(_) => {
                println!("{:<12} {:>8}", date, "-");
            }
        }
    }

    if count > 0 {
        println!("{}", "-".repeat(50));
        println!("{:<12} {:>8} {:>8}", "Average", total_cals / count, total_active / count);
    }

    Ok(())
}

/// Get VO2 max and performance metrics
pub async fn vo2max(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/metrics-service/metrics/maxmet/daily/{}/{}", date, date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("VO2 Max / Max Metrics for {}", date);
    println!("{}", "-".repeat(40));

    if let Some(arr) = data.as_array() {
        for entry in arr {
            // Generic (running) VO2 max
            if let Some(generic) = entry.get("generic") {
                if let Some(vo2) = generic.get("vo2MaxPreciseValue").and_then(|v| v.as_f64()) {
                    println!("VO2 Max (Run):   {:.1} ml/kg/min", vo2);
                }
            }
            // Cycling VO2 max
            if let Some(cycling) = entry.get("cycling") {
                if let Some(vo2) = cycling.get("vo2MaxPreciseValue").and_then(|v| v.as_f64()) {
                    println!("VO2 Max (Cycle): {:.1} ml/kg/min", vo2);
                }
            }
            // Heat/Altitude acclimation
            if let Some(accl) = entry.get("heatAltitudeAcclimation") {
                if let Some(heat) = accl.get("heatAcclimationPercentage").and_then(|v| v.as_i64()) {
                    if heat > 0 {
                        println!("Heat Acclim:     {}%", heat);
                    }
                }
                if let Some(alt) = accl.get("altitudeAcclimation").and_then(|v| v.as_i64()) {
                    if alt > 0 {
                        println!("Alt Acclim:      {}%", alt);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Get training readiness score
pub async fn training_readiness(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/metrics-service/metrics/trainingreadiness/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Training Readiness for {}", date);
    println!("{}", "-".repeat(40));

    // Get first (most recent) entry from array
    let entry = data.as_array().and_then(|arr| arr.first()).unwrap_or(&data);

    if let Some(score) = entry.get("score").and_then(|v| v.as_i64()) {
        let level = entry.get("level").and_then(|v| v.as_str()).unwrap_or("Unknown");
        println!("Score:           {} ({})", score, level);
    }

    if let Some(feedback) = entry.get("feedbackShort").and_then(|v| v.as_str()) {
        let display = feedback.replace('_', " ").to_lowercase();
        println!("Status:          {}", display);
    }

    if let Some(sleep) = entry.get("sleepScore").and_then(|v| v.as_i64()) {
        println!("Sleep Score:     {}", sleep);
    }

    if let Some(recovery) = entry.get("recoveryTime").and_then(|v| v.as_i64()) {
        let hours = recovery / 60;
        let mins = recovery % 60;
        println!("Recovery Time:   {}h {}m", hours, mins);
    }

    if let Some(hrv) = entry.get("hrvWeeklyAverage").and_then(|v| v.as_i64()) {
        println!("HRV Weekly Avg:  {} ms", hrv);
    }

    if let Some(load) = entry.get("acuteLoad").and_then(|v| v.as_f64()) {
        println!("Acute Load:      {:.0}", load);
    }

    Ok(())
}

/// Get training status
pub async fn training_status(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/metrics-service/metrics/trainingstatus/aggregated/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Training Status for {}", date);
    println!("{}", "-".repeat(40));

    // Get first entry if array
    let entry = data.as_array().and_then(|arr| arr.first()).unwrap_or(&data);

    if let Some(status) = entry.get("trainingStatusPhrase").and_then(|v| v.as_str()) {
        println!("Status:          {}", status);
    }

    if let Some(load) = entry.get("weeklyTrainingLoad").and_then(|v| v.as_f64()) {
        println!("Weekly Load:     {:.0}", load);
    }

    if let Some(load7) = entry.get("sevenDayLoad").and_then(|v| v.as_f64()) {
        println!("7-Day Load:      {:.0}", load7);
    }

    if let Some(load_status) = entry.get("loadStatus").and_then(|v| v.as_str()) {
        println!("Load Status:     {}", load_status);
    }

    if let Some(focus) = entry.get("trainingLoadBalance").and_then(|v| v.as_str()) {
        println!("Focus:           {}", focus);
    }

    if let Some(vo2) = entry.get("mostRecentVO2Max").and_then(|v| v.as_f64()) {
        println!("VO2 Max:         {:.1}", vo2);
    }

    if let Some(chronic) = entry.get("chronicTrainingLoad").and_then(|v| v.as_f64()) {
        println!("Chronic Load:    {:.0}", chronic);
    }

    Ok(())
}

/// Get HRV data
pub async fn hrv(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/hrv-service/hrv/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("HRV Data for {}", date);
    println!("{}", "-".repeat(40));

    if let Some(summary) = data.get("hrvSummary") {
        if let Some(weekly) = summary.get("weeklyAvg").and_then(|v| v.as_i64()) {
            println!("Weekly Avg:      {} ms", weekly);
        }
        if let Some(last_night) = summary.get("lastNight").and_then(|v| v.as_i64()) {
            println!("Last Night:      {} ms", last_night);
        }
        if let Some(status) = summary.get("status").and_then(|v| v.as_str()) {
            println!("Status:          {}", status);
        }
        if let Some(baseline) = summary.get("baseline") {
            if let (Some(low), Some(high)) = (
                baseline.get("lowUpper").and_then(|v| v.as_i64()),
                baseline.get("balancedUpper").and_then(|v| v.as_i64()),
            ) {
                println!("Baseline:        {}-{} ms", low, high);
            }
        }
    }

    Ok(())
}

/// Get fitness age
pub async fn fitness_age(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/fitnessage-service/fitnessage/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Fitness Age for {}", date);
    println!("{}", "-".repeat(40));

    if let Some(age) = data.get("fitnessAge").and_then(|v| v.as_f64()) {
        println!("Fitness Age:     {:.0} years", age);
    }

    if let Some(chrono) = data.get("chronologicalAge").and_then(|v| v.as_f64()) {
        println!("Actual Age:      {:.0} years", chrono);
    }

    if let Some(vo2) = data.get("vo2Max").and_then(|v| v.as_f64()) {
        println!("VO2 Max:         {:.1} ml/kg/min", vo2);
    }

    if let Some(bmi) = data.get("bmi").and_then(|v| v.as_f64()) {
        println!("BMI:             {:.1}", bmi);
    }

    if let Some(rhr) = data.get("restingHeartRate").and_then(|v| v.as_i64()) {
        println!("Resting HR:      {} bpm", rhr);
    }

    if let Some(vigorous) = data.get("vigorousActivityMinutes").and_then(|v| v.as_i64()) {
        println!("Vigorous Mins:   {} min/week", vigorous);
    }

    Ok(())
}

/// Get daily steps for a date range
pub async fn steps(days: Option<u32>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();
    let num_days = days.unwrap_or(10);
    let start_date = today - Duration::days(num_days as i64 - 1);

    let path = format!(
        "/usersummary-service/stats/steps/daily/{}/{}",
        start_date.format("%Y-%m-%d"),
        today.format("%Y-%m-%d")
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("{:<12} {:>8} {:>8} {:>6} {:>10}", "Date", "Steps", "Goal", "%", "Distance");
    println!("{}", "-".repeat(50));

    let mut total_steps: i64 = 0;
    let mut total_goal: i64 = 0;
    let mut count = 0;

    if let Some(entries) = data.as_array() {
        for entry in entries.iter().rev() {
            let date = entry.get("calendarDate")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let steps_val = entry.get("totalSteps").and_then(|v| v.as_i64()).unwrap_or(0);
            let goal = entry.get("stepGoal").and_then(|v| v.as_i64()).unwrap_or(10000);
            let distance = entry.get("totalDistance")
                .and_then(|v| v.as_f64())
                .map(|d| d / 1000.0);

            let pct = if goal > 0 { (steps_val as f64 / goal as f64 * 100.0) as i64 } else { 0 };

            let dist_str = distance.map(|d| format!("{:.2} km", d)).unwrap_or("-".to_string());

            println!("{:<12} {:>8} {:>8} {:>5}% {:>10}",
                date, steps_val, goal, pct, dist_str);

            total_steps += steps_val;
            total_goal += goal;
            count += 1;
        }
    }

    if count > 0 {
        println!("{}", "-".repeat(50));
        let avg_steps = total_steps / count;
        let avg_goal = total_goal / count;
        let avg_pct = if avg_goal > 0 { (avg_steps as f64 / avg_goal as f64 * 100.0) as i64 } else { 0 };
        println!("{:<12} {:>8} {:>8} {:>5}%", "Average", avg_steps, avg_goal, avg_pct);
    }

    Ok(())
}

fn print_sleep_summary(data: &serde_json::Value) {
    let deep = data.get("deepSleepSeconds").and_then(|v| v.as_i64());
    let light = data.get("lightSleepSeconds").and_then(|v| v.as_i64());
    let rem = data.get("remSleepSeconds").and_then(|v| v.as_i64());
    let awake = data.get("awakeSleepSeconds").and_then(|v| v.as_i64());

    // Calculate total from components
    let total = deep.unwrap_or(0) + light.unwrap_or(0) + rem.unwrap_or(0);
    let total_opt = if total > 0 { Some(total) } else { None };

    println!("Sleep Summary");
    println!("{}", "-".repeat(30));
    println!("Total Sleep:  {}", format_duration(total_opt));
    println!("Deep Sleep:   {}", format_duration(deep));
    println!("Light Sleep:  {}", format_duration(light));
    println!("REM Sleep:    {}", format_duration(rem));
    println!("Awake:        {}", format_duration(awake));

    if let Some(score) = data.get("sleepScores").and_then(|s| s.get("overall")).and_then(|o| o.get("value")).and_then(|v| v.as_i64()) {
        println!("Sleep Score:  {}", score);
    }
}

/// Get lactate threshold data
pub async fn lactate_threshold(days: Option<u32>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();
    let num_days = days.unwrap_or(90);
    let start_date = today - Duration::days(num_days as i64 - 1);

    // Fetch lactate threshold heart rate history
    let hr_path = format!(
        "/biometric-service/stats/lactateThresholdHeartRate/range/{}/{}?sport=RUNNING&aggregation=daily&aggregationStrategy=LATEST",
        start_date.format("%Y-%m-%d"),
        today.format("%Y-%m-%d")
    );

    // Fetch lactate threshold speed history
    let speed_path = format!(
        "/biometric-service/stats/lactateThresholdSpeed/range/{}/{}?sport=RUNNING&aggregation=daily&aggregationStrategy=LATEST",
        start_date.format("%Y-%m-%d"),
        today.format("%Y-%m-%d")
    );

    let hr_data: serde_json::Value = client.get_json(&oauth2, &hr_path).await.unwrap_or(serde_json::Value::Null);
    let speed_data: serde_json::Value = client.get_json(&oauth2, &speed_path).await.unwrap_or(serde_json::Value::Null);

    println!("Lactate Threshold History ({} days)", num_days);
    println!("{}", "-".repeat(50));

    // Combine HR and speed data by date
    // API returns: {"from": "date", "value": number}
    let mut entries: std::collections::BTreeMap<String, (Option<i64>, Option<f64>)> = std::collections::BTreeMap::new();

    if let Some(arr) = hr_data.as_array() {
        for entry in arr {
            if let Some(date) = entry.get("from").and_then(|v| v.as_str()) {
                let hr = entry.get("value").and_then(|v| v.as_f64()).map(|v| v as i64);
                entries.entry(date.to_string()).or_insert((None, None)).0 = hr;
            }
        }
    }

    if let Some(arr) = speed_data.as_array() {
        for entry in arr {
            if let Some(date) = entry.get("from").and_then(|v| v.as_str()) {
                // Speed value needs conversion: pace_sec_per_km = 100 / value
                let speed = entry.get("value").and_then(|v| v.as_f64());
                entries.entry(date.to_string()).or_insert((None, None)).1 = speed;
            }
        }
    }

    if entries.is_empty() {
        println!("No lactate threshold data found");
        println!();
        println!("Tip: Lactate threshold is estimated from:");
        println!("  - Guided lactate threshold test");
        println!("  - Running activities with heart rate");
        return Ok(());
    }

    println!("{:<12} {:>8} {:>10}", "Date", "HR (bpm)", "Pace");
    println!("{}", "-".repeat(35));

    for (date, (hr, speed)) in entries.iter().rev() {
        let hr_str = hr.map(|h| format!("{}", h)).unwrap_or("-".to_string());
        let pace_str = speed.map(|s| {
            // Convert: value is stored as speed factor, pace = 100/value sec/km
            let pace_sec_per_km = 100.0 / s;
            let pace_min = (pace_sec_per_km / 60.0).floor() as i64;
            let pace_sec = (pace_sec_per_km % 60.0) as i64;
            format!("{}:{:02}/km", pace_min, pace_sec)
        }).unwrap_or("-".to_string());

        println!("{:<12} {:>8} {:>10}", date, hr_str, pace_str);
    }

    Ok(())
}

/// Get race predictions
pub async fn race_predictions(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);
    let display_name = get_display_name(&client, &oauth2).await?;

    let path = format!("/metrics-service/metrics/racepredictions/latest/{}", display_name);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Race Predictions for {}", date);
    println!("{}", "-".repeat(50));
    println!("{:<16} {:>12} {:>14}", "Race", "Time", "Pace");
    println!("{}", "-".repeat(50));

    // API returns flat structure: time5K, time10K, timeHalfMarathon, timeMarathon
    let races = [
        ("time5K", "5K", 5.0),
        ("time10K", "10K", 10.0),
        ("timeHalfMarathon", "Half Marathon", 21.0975),
        ("timeMarathon", "Marathon", 42.195),
    ];

    for (field, label, distance_km) in races {
        if let Some(time) = data.get(field).and_then(|v| v.as_f64()) {
            let formatted = format_race_time(time);
            let pace_sec = time / distance_km;
            let pace = format_pace(pace_sec);
            println!("{:<16} {:>12} {:>14}", label, formatted, pace);
        }
    }

    Ok(())
}

fn format_race_time(seconds: f64) -> String {
    let total_secs = seconds as i64;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if hours > 0 {
        format!("{}:{:02}:{:02}", hours, mins, secs)
    } else {
        format!("{}:{:02}", mins, secs)
    }
}

fn format_pace(sec_per_km: f64) -> String {
    let mins = (sec_per_km / 60.0).floor() as i64;
    let secs = (sec_per_km % 60.0) as i64;
    format!("{}:{:02}/km", mins, secs)
}

/// Get endurance score
pub async fn endurance_score(days: Option<u32>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();
    let num_days = days.unwrap_or(30);
    let start_date = today - Duration::days(num_days as i64 - 1);

    let path = format!(
        "/metrics-service/metrics/endurancescore?startDate={}&endDate={}&aggregation=daily",
        start_date.format("%Y-%m-%d"),
        today.format("%Y-%m-%d")
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Endurance Score");
    println!("{}", "-".repeat(50));

    if let Some(arr) = data.as_array() {
        if arr.is_empty() {
            println!("No endurance score data available");
            return Ok(());
        }

        println!("{:<12} {:>6} {:>10} {:>8} {:>8} {:>8}", "Date", "Score", "Class", "VO2", "Train", "Activity");
        println!("{}", "-".repeat(50));

        for entry in arr.iter().rev().take(10) {
            let date = entry.get("calendarDate").and_then(|v| v.as_str()).unwrap_or("-");
            let score = entry.get("overallScore").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let class = entry.get("classification").and_then(|v| v.as_str()).unwrap_or("-");
            let vo2 = entry.get("vo2MaxFactor").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let train = entry.get("trainingHistoryFactor").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let activity = entry.get("activityHistoryFactor").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());

            println!("{:<12} {:>6} {:>10} {:>8} {:>8} {:>8}", date, score, class, vo2, train, activity);
        }
    }

    Ok(())
}

/// Get hill score
pub async fn hill_score(days: Option<u32>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();
    let num_days = days.unwrap_or(30);
    let start_date = today - Duration::days(num_days as i64 - 1);

    let path = format!(
        "/metrics-service/metrics/hillscore?startDate={}&endDate={}&aggregation=daily",
        start_date.format("%Y-%m-%d"),
        today.format("%Y-%m-%d")
    );

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Hill Score");
    println!("{}", "-".repeat(50));

    if let Some(arr) = data.as_array() {
        if arr.is_empty() {
            println!("No hill score data available");
            return Ok(());
        }

        println!("{:<12} {:>6} {:>10} {:>8} {:>8} {:>8}", "Date", "Score", "Class", "Str", "End", "Pwr");
        println!("{}", "-".repeat(50));

        for entry in arr.iter().rev().take(10) {
            let date = entry.get("calendarDate").and_then(|v| v.as_str()).unwrap_or("-");
            let score = entry.get("overallScore").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let class = entry.get("classification").and_then(|v| v.as_str()).unwrap_or("-");
            let strength = entry.get("strengthFactor").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let endurance = entry.get("enduranceFactor").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let power = entry.get("powerFactor").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());

            println!("{:<12} {:>6} {:>10} {:>8} {:>8} {:>8}", date, score, class, strength, endurance, power);
        }
    }

    Ok(())
}

/// Get SpO2 (blood oxygen) data
pub async fn spo2(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/wellness-service/wellness/daily/spo2/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("SpO2 (Blood Oxygen) for {}", date);
    println!("{}", "-".repeat(40));

    if let Some(avg) = data.get("averageSpO2").and_then(|v| v.as_i64()) {
        println!("Average:     {}%", avg);
    }

    if let Some(low) = data.get("lowestSpO2").and_then(|v| v.as_i64()) {
        println!("Lowest:      {}%", low);
    }

    if let Some(latest) = data.get("latestSpO2").and_then(|v| v.as_i64()) {
        println!("Latest:      {}%", latest);
    }

    Ok(())
}

/// Get respiration data
pub async fn respiration(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/wellness-service/wellness/daily/respiration/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Respiration for {}", date);
    println!("{}", "-".repeat(40));

    if let Some(waking) = data.get("avgWakingRespirationValue").and_then(|v| v.as_f64()) {
        println!("Avg Waking:  {:.1} brpm", waking);
    }

    if let Some(sleep) = data.get("avgSleepRespirationValue").and_then(|v| v.as_f64()) {
        println!("Avg Sleep:   {:.1} brpm", sleep);
    }

    if let Some(high) = data.get("highestRespirationValue").and_then(|v| v.as_f64()) {
        println!("Highest:     {:.1} brpm", high);
    }

    if let Some(low) = data.get("lowestRespirationValue").and_then(|v| v.as_f64()) {
        println!("Lowest:      {:.1} brpm", low);
    }

    Ok(())
}

/// Get intensity minutes
pub async fn intensity_minutes(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/wellness-service/wellness/daily/im/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Intensity Minutes for {}", date);
    println!("{}", "-".repeat(40));

    let moderate = data.get("moderateIntensityMinutes").and_then(|v| v.as_i64()).unwrap_or(0);
    let vigorous = data.get("vigorousIntensityMinutes").and_then(|v| v.as_i64()).unwrap_or(0);
    let total = data.get("totalIntensityMinutes").and_then(|v| v.as_i64()).unwrap_or(0);
    let goal = data.get("weeklyGoal").and_then(|v| v.as_i64()).unwrap_or(150);

    println!("Moderate:    {} min", moderate);
    println!("Vigorous:    {} min (x2 = {})", vigorous, vigorous * 2);
    println!("Total:       {} min", total);
    println!("Weekly Goal: {} min", goal);

    let pct = (total as f64 / goal as f64 * 100.0) as i64;
    println!("Progress:    {}%", pct);

    Ok(())
}

/// Get blood pressure data
pub async fn blood_pressure(from: Option<String>, to: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();
    let end_date = to.unwrap_or_else(|| today.format("%Y-%m-%d").to_string());
    let start_date = from.unwrap_or_else(|| (today - Duration::days(30)).format("%Y-%m-%d").to_string());

    let path = format!("/bloodpressure-service/bloodpressure/range/{}/{}", start_date, end_date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Blood Pressure ({} to {})", start_date, end_date);
    println!("{}", "-".repeat(50));

    if let Some(measurements) = data.get("measurementSummaries").and_then(|v| v.as_array()) {
        if measurements.is_empty() {
            println!("No blood pressure measurements found");
            return Ok(());
        }

        println!("{:<20} {:>8} {:>8} {:>6}", "Date/Time", "Systolic", "Diastolic", "Pulse");
        println!("{}", "-".repeat(50));

        for entry in measurements {
            let timestamp = entry.get("measurementTimestampLocal").and_then(|v| v.as_i64());
            let dt = timestamp.and_then(|t| chrono::DateTime::from_timestamp_millis(t))
                .map(|dt| dt.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| "-".to_string());

            let systolic = entry.get("systolic").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let diastolic = entry.get("diastolic").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());
            let pulse = entry.get("pulse").and_then(|v| v.as_i64()).map(|s| s.to_string()).unwrap_or("-".to_string());

            let classification = classify_bp(
                entry.get("systolic").and_then(|v| v.as_i64()),
                entry.get("diastolic").and_then(|v| v.as_i64())
            );

            println!("{:<20} {:>8} {:>8} {:>6}  {}", dt, systolic, diastolic, pulse, classification);
        }
    }

    Ok(())
}

fn classify_bp(systolic: Option<i64>, diastolic: Option<i64>) -> &'static str {
    match (systolic, diastolic) {
        (Some(s), Some(d)) => {
            if s < 120 && d < 80 { "Normal" }
            else if s < 130 && d < 80 { "Elevated" }
            else if s < 140 || d < 90 { "High Stage 1" }
            else if s >= 140 || d >= 90 { "High Stage 2" }
            else { "Unknown" }
        }
        _ => "Unknown"
    }
}

/// Get hydration data
pub async fn hydration(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);

    let path = format!("/usersummary-service/usersummary/hydration/daily/{}", date);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Hydration for {}", date);
    println!("{}", "-".repeat(40));

    let value = data.get("valueInML").and_then(|v| v.as_i64()).unwrap_or(0);
    let goal = data.get("goalInML").and_then(|v| v.as_i64()).unwrap_or(2500);

    println!("Intake:      {} ml ({:.1} L)", value, value as f64 / 1000.0);
    println!("Goal:        {} ml ({:.1} L)", goal, goal as f64 / 1000.0);

    let pct = (value as f64 / goal as f64 * 100.0) as i64;
    println!("Progress:    {}%", pct);

    if let Some(sweat) = data.get("sweatLossInML").and_then(|v| v.as_i64()) {
        println!("Sweat Loss:  {} ml", sweat);
    }

    if let Some(activity) = data.get("activityIntakeInML").and_then(|v| v.as_i64()) {
        println!("Activity:    {} ml", activity);
    }

    Ok(())
}

/// Get performance summary - all key performance metrics at once
pub async fn performance_summary(date: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let date = resolve_date(date)?;
    let client = GarminClient::new(&oauth1.domain);
    let display_name = get_display_name(&client, &oauth2).await?;

    println!("Performance Summary for {}", date);
    println!("{}", "=".repeat(50));

    // VO2 Max
    let vo2_path = format!("/metrics-service/metrics/maxmet/daily/{}/{}", date, date);
    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &vo2_path).await {
        if let Some(arr) = data.as_array() {
            if let Some(entry) = arr.first() {
                if let Some(generic) = entry.get("generic") {
                    if let Some(vo2) = generic.get("vo2MaxPreciseValue").and_then(|v| v.as_f64()) {
                        println!("VO2 Max:             {:.1} ml/kg/min", vo2);
                    }
                }
            }
        }
    }

    // Fitness Age
    let fitness_path = format!("/fitnessage-service/fitnessage/{}", date);
    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &fitness_path).await {
        let fitness_age = data.get("fitnessAge").and_then(|v| v.as_f64());
        let chrono_age = data.get("chronologicalAge").and_then(|v| v.as_f64());
        if let (Some(fa), Some(ca)) = (fitness_age, chrono_age) {
            println!("Fitness Age:         {:.0} years (actual: {:.0})", fa, ca);
        }
    }

    // Training Status
    let status_path = format!("/metrics-service/metrics/trainingstatus/aggregated/{}", date);
    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &status_path).await {
        let entry = data.as_array().and_then(|arr| arr.first()).unwrap_or(&data);
        if let Some(status) = entry.get("trainingStatusPhrase").and_then(|v| v.as_str()) {
            println!("Training Status:     {}", status);
        }
        if let Some(load) = entry.get("weeklyTrainingLoad").and_then(|v| v.as_f64()) {
            let load_status = entry.get("loadStatus").and_then(|v| v.as_str()).unwrap_or("");
            println!("Training Load:       {:.0} ({})", load, load_status);
        }
    }

    // Training Readiness
    let readiness_path = format!("/metrics-service/metrics/trainingreadiness/{}", date);
    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &readiness_path).await {
        let entry = data.as_array().and_then(|arr| arr.first()).unwrap_or(&data);
        if let Some(score) = entry.get("score").and_then(|v| v.as_i64()) {
            let level = entry.get("level").and_then(|v| v.as_str()).unwrap_or("");
            println!("Training Readiness:  {} ({})", score, level);
        }
    }

    println!();
    println!("Lactate Threshold");
    println!("{}", "-".repeat(30));

    // Get most recent LT data (search back 90 days)
    let today = chrono::Local::now().date_naive();
    let start = today - Duration::days(90);
    let lt_hr_range_path = format!(
        "/biometric-service/stats/lactateThresholdHeartRate/range/{}/{}?sport=RUNNING&aggregation=daily&aggregationStrategy=LATEST",
        start.format("%Y-%m-%d"), today.format("%Y-%m-%d")
    );
    let lt_speed_range_path = format!(
        "/biometric-service/stats/lactateThresholdSpeed/range/{}/{}?sport=RUNNING&aggregation=daily&aggregationStrategy=LATEST",
        start.format("%Y-%m-%d"), today.format("%Y-%m-%d")
    );

    let mut lt_hr: Option<i64> = None;
    let mut lt_pace: Option<String> = None;
    let mut lt_date: Option<String> = None;

    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &lt_hr_range_path).await {
        if let Some(arr) = data.as_array() {
            if let Some(last) = arr.last() {
                lt_hr = last.get("value").and_then(|v| v.as_f64()).map(|v| v as i64);
                lt_date = last.get("from").and_then(|v| v.as_str()).map(|s| s.to_string());
            }
        }
    }

    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &lt_speed_range_path).await {
        if let Some(arr) = data.as_array() {
            if let Some(last) = arr.last() {
                if let Some(speed) = last.get("value").and_then(|v| v.as_f64()) {
                    let pace_sec_per_km = 100.0 / speed;
                    let pace_min = (pace_sec_per_km / 60.0).floor() as i64;
                    let pace_sec = (pace_sec_per_km % 60.0) as i64;
                    lt_pace = Some(format!("{}:{:02}/km", pace_min, pace_sec));
                }
            }
        }
    }

    if let Some(hr) = lt_hr {
        println!("  Heart Rate:        {} bpm", hr);
    }
    if let Some(pace) = lt_pace {
        println!("  Pace:              {}", pace);
    }
    if let Some(d) = lt_date {
        println!("  Last Updated:      {}", d);
    }

    // Endurance & Hill Scores
    println!();
    println!("Scores");
    println!("{}", "-".repeat(30));

    let end_path = format!(
        "/metrics-service/metrics/endurancescore?startDate={}&endDate={}&aggregation=daily",
        start.format("%Y-%m-%d"), today.format("%Y-%m-%d")
    );
    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &end_path).await {
        if let Some(arr) = data.as_array() {
            if let Some(last) = arr.last() {
                let score = last.get("overallScore").and_then(|v| v.as_i64());
                let class = last.get("classification").and_then(|v| v.as_str());
                if let (Some(s), Some(c)) = (score, class) {
                    println!("  Endurance:         {} ({})", s, c);
                }
            }
        }
    }

    let hill_path = format!(
        "/metrics-service/metrics/hillscore?startDate={}&endDate={}&aggregation=daily",
        start.format("%Y-%m-%d"), today.format("%Y-%m-%d")
    );
    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &hill_path).await {
        if let Some(arr) = data.as_array() {
            if let Some(last) = arr.last() {
                let score = last.get("overallScore").and_then(|v| v.as_i64());
                let class = last.get("classification").and_then(|v| v.as_str());
                if let (Some(s), Some(c)) = (score, class) {
                    println!("  Hill:              {} ({})", s, c);
                }
            }
        }
    }

    // Race Predictions
    println!();
    println!("Race Predictions");
    println!("{}", "-".repeat(30));

    let race_path = format!("/metrics-service/metrics/racepredictions/latest/{}", display_name);
    match client.get_json::<serde_json::Value>(&oauth2, &race_path).await {
        Ok(data) => {
            let races = [
                ("time5K", "5K", 5.0),
                ("time10K", "10K", 10.0),
                ("timeHalfMarathon", "Half Marathon", 21.0975),
                ("timeMarathon", "Marathon", 42.195),
            ];
            for (field, label, distance_km) in races {
                if let Some(time) = data.get(field).and_then(|v| v.as_f64()) {
                    let formatted = format_race_time(time);
                    let pace_sec = time / distance_km;
                    let pace = format_pace(pace_sec);
                    println!("  {:<16}  {:>10}  ({})", label, formatted, pace);
                }
            }
        }
        Err(_) => {
            println!("  (Race predictions not available)");
        }
    }

    // Personal Records summary
    println!();
    println!("Personal Records");
    println!("{}", "-".repeat(30));

    let pr_path = format!("/personalrecord-service/personalrecord/prs/{}", display_name);
    if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &pr_path).await {
        // Time-based record type IDs: 3=5K, 4=10K, 5=HM, 6=Marathon
        let time_types = [3_i64, 4, 5, 6];
        let type_names: std::collections::HashMap<i64, &str> = [
            (3, "5K"), (4, "10K"), (5, "Half Marathon"), (6, "Marathon"),
        ].into_iter().collect();

        if let Some(records) = data.as_array() {
            let mut shown = 0;
            for record in records {
                if shown >= 4 { break; }
                let type_id = record.get("typeId").and_then(|v| v.as_i64()).unwrap_or(0);
                if time_types.contains(&type_id) {
                    if let Some(v) = record.get("value").and_then(|v| v.as_f64()) {
                        let name = type_names.get(&type_id).unwrap_or(&"Unknown");
                        let formatted = format_race_time(v);
                        println!("  {:<16}  {:>10}", name, formatted);
                        shown += 1;
                    }
                }
            }
            if shown == 0 {
                println!("  (No records found)");
            }
        }
    }

    Ok(())
}

/// Get personal records
pub async fn personal_records(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let display_name = get_display_name(&client, &oauth2).await?;

    let path = format!("/personalrecord-service/personalrecord/prs/{}", display_name);

    let data: serde_json::Value = client.get_json(&oauth2, &path).await?;

    println!("Personal Records");
    println!("{}", "-".repeat(70));

    // API returns array directly, typeId maps to record type
    let type_names: std::collections::HashMap<i64, (&str, &str)> = [
        (3, ("Fastest 5K", "time")),
        (4, ("Fastest 10K", "time")),
        (5, ("Fastest Half Marathon", "time")),
        (6, ("Fastest Marathon", "time")),
        (7, ("Longest Run", "distance")),
        (8, ("Longest Ride", "distance")),
        (9, ("Longest Ride Time", "time")),
        (11, ("Max Elevation Gain", "elevation")),
        (12, ("Most Steps (Day)", "steps")),
        (13, ("Most Steps (Week)", "steps")),
        (14, ("Most Steps (Month)", "steps")),
        (17, ("Longest Swim", "distance_m")),
        (18, ("Fastest 100m Swim", "time")),
    ].into_iter().collect();

    if let Some(records) = data.as_array() {
        if records.is_empty() {
            println!("No personal records found");
            return Ok(());
        }

        for record in records {
            let type_id = record.get("typeId").and_then(|v| v.as_i64()).unwrap_or(0);
            let value = record.get("value").and_then(|v| v.as_f64());
            let activity_name = record.get("activityName").and_then(|v| v.as_str());
            let date = record.get("actStartDateTimeInGMTFormatted").and_then(|v| v.as_str())
                .map(|d| d.split('T').next().unwrap_or(d))
                .unwrap_or("-");

            if let Some((name, format_type)) = type_names.get(&type_id) {
                let formatted_value = match *format_type {
                    "time" => value.map(|v| format_race_time(v)).unwrap_or("-".to_string()),
                    "distance" => value.map(|v| format!("{:.2} km", v / 1000.0)).unwrap_or("-".to_string()),
                    "distance_m" => value.map(|v| format!("{:.0} m", v)).unwrap_or("-".to_string()),
                    "elevation" => value.map(|v| format!("{:.0} m", v)).unwrap_or("-".to_string()),
                    "steps" => value.map(|v| format!("{:.0}", v)).unwrap_or("-".to_string()),
                    _ => value.map(|v| format!("{:.0}", v)).unwrap_or("-".to_string())
                };

                let activity = activity_name.unwrap_or("-");
                println!("{:<22} {:>12}  {}  {}", name, formatted_value, date, activity);
            }
        }
    }

    Ok(())
}

/// Get health insights analyzing sleep/stress correlations
pub async fn insights(days: u32, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let client = GarminClient::new(&oauth1.domain);
    let today = Local::now().date_naive();
    let display_name = get_display_name(&client, &oauth2).await?;

    // Collect sleep and stress data
    let mut sleep_data: Vec<(String, i64, i64, i64, i64)> = vec![]; // date, total, deep, rem, score
    let mut stress_data: Vec<(String, i64)> = vec![]; // date, avg_stress

    // Fetch sleep data
    for i in 0..days {
        let date = today - Duration::days(i as i64);
        let path = format!(
            "/wellness-service/wellness/dailySleepData/{}?date={}&nonSleepBufferMinutes=60",
            display_name, date
        );

        if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &path).await {
            let sleep_dto = data.get("dailySleepDTO").unwrap_or(&data);
            let total = sleep_dto.get("sleepTimeSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
            let deep = sleep_dto.get("deepSleepSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
            let rem = sleep_dto.get("remSleepSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
            let score = sleep_dto.get("sleepScores")
                .and_then(|s| s.get("overall"))
                .and_then(|o| o.get("value"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            if total > 0 {
                sleep_data.push((date.to_string(), total, deep, rem, score));
            }
        }
    }

    // Fetch stress data
    for i in 0..days {
        let date = today - Duration::days(i as i64);
        let path = format!("/wellness-service/wellness/dailyStress/{}", date);

        if let Ok(data) = client.get_json::<serde_json::Value>(&oauth2, &path).await {
            let avg = data.get("avgStressLevel").and_then(|v| v.as_i64()).unwrap_or(0);
            if avg > 0 {
                stress_data.push((date.to_string(), avg));
            }
        }
    }

    // Calculate insights
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                     HEALTH INSIGHTS ({} days)                     ║", days);
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    // 1. Restorative Sleep Ratio
    let mut total_ratio = 0.0;
    let mut last_night_ratio = 0.0;
    let mut count = 0;

    for (i, (_date, total, deep, rem, _score)) in sleep_data.iter().enumerate() {
        if *total > 0 {
            let ratio = (*deep + *rem) as f64 / *total as f64 * 100.0;
            total_ratio += ratio;
            count += 1;
            if i == 0 {
                last_night_ratio = ratio;
            }
        }
    }

    let avg_ratio = if count > 0 { total_ratio / count as f64 } else { 0.0 };

    let ratio_status = if last_night_ratio < 30.0 { "⚠️" } else if last_night_ratio > 45.0 { "✓" } else { "" };

    println!("🧠 RESTORATIVE SLEEP RATIO");
    println!("   Your avg: {:.0}%  |  Target: >45%  |  Last night: {:.0}% {}",
        avg_ratio, last_night_ratio, ratio_status);
    println!();

    // 2. Sleep-Stress Correlation
    let mut low_restorative_stress: Vec<i64> = vec![];
    let mut high_restorative_stress: Vec<i64> = vec![];

    // Match sleep nights with next-day stress
    for i in 1..sleep_data.len().min(stress_data.len()) {
        let (_date, total, deep, rem, _score) = &sleep_data[i];
        if *total > 0 {
            let ratio = (*deep + *rem) as f64 / *total as f64 * 100.0;
            let next_day_stress = stress_data[i - 1].1;

            if ratio < 30.0 {
                low_restorative_stress.push(next_day_stress);
            } else if ratio > 45.0 {
                high_restorative_stress.push(next_day_stress);
            }
        }
    }

    let low_avg = if low_restorative_stress.is_empty() {
        0.0
    } else {
        low_restorative_stress.iter().sum::<i64>() as f64 / low_restorative_stress.len() as f64
    };

    let high_avg = if high_restorative_stress.is_empty() {
        0.0
    } else {
        high_restorative_stress.iter().sum::<i64>() as f64 / high_restorative_stress.len() as f64
    };

    println!("😰 STRESS CORRELATION");
    if !low_restorative_stress.is_empty() {
        println!("   Low restorative (<30%) → avg next-day stress: {:.0}", low_avg);
    }
    if !high_restorative_stress.is_empty() {
        println!("   High restorative (>45%) → avg next-day stress: {:.0}", high_avg);
    }
    if low_restorative_stress.is_empty() && high_restorative_stress.is_empty() {
        println!("   (Insufficient data for correlation)");
    }
    println!();

    // 3. Today's Prediction
    let prediction = if last_night_ratio < 30.0 {
        "HIGH (35-45 avg expected)"
    } else if last_night_ratio < 45.0 {
        "MODERATE (25-35 avg expected)"
    } else {
        "LOW (15-25 avg expected)"
    };

    let last_night_restorative = if !sleep_data.is_empty() {
        let (_, total, deep, rem, _) = &sleep_data[0];
        let restorative_mins = (*deep + *rem) / 60;
        format!("{}m restorative, {:.0}%", restorative_mins, last_night_ratio)
    } else {
        "no data".to_string()
    };

    println!("🎯 TODAY'S PREDICTION");
    println!("   Based on last night ({}):", last_night_restorative);
    println!("   Expected stress: {}", prediction);
    println!();

    // 4. Best/Worst Days
    if sleep_data.len() >= 3 {
        let mut sleep_with_ratio: Vec<(&str, f64, i64)> = sleep_data.iter()
            .map(|(date, total, deep, rem, score)| {
                let ratio = if *total > 0 { (*deep + *rem) as f64 / *total as f64 * 100.0 } else { 0.0 };
                (date.as_str(), ratio, *score)
            })
            .collect();

        sleep_with_ratio.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        println!("📊 SLEEP QUALITY RANKING (by restorative %)");
        println!("   Best:  {} ({:.0}% restorative, score {})",
            sleep_with_ratio[0].0, sleep_with_ratio[0].1, sleep_with_ratio[0].2);
        println!("   Worst: {} ({:.0}% restorative, score {})",
            sleep_with_ratio.last().unwrap().0,
            sleep_with_ratio.last().unwrap().1,
            sleep_with_ratio.last().unwrap().2);
    }

    Ok(())
}

