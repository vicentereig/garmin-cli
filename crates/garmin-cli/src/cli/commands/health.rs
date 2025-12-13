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

