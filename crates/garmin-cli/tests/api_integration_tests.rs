//! Integration tests for Garmin API client
//!
//! These tests use wiremock to mock API responses with recorded fixtures.

use garmin_cli::client::GarminClient;
use garmin_cli::client::OAuth2Token;
use garmin_cli::models::ActivitySummary;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Create a test OAuth2 token
fn test_token() -> OAuth2Token {
    OAuth2Token {
        scope: "test".to_string(),
        jti: "test-jti".to_string(),
        token_type: "Bearer".to_string(),
        access_token: "test-access-token".to_string(),
        refresh_token: "test-refresh-token".to_string(),
        expires_in: 3600,
        expires_at: chrono::Utc::now().timestamp() + 3600,
        refresh_token_expires_in: 86400,
        refresh_token_expires_at: chrono::Utc::now().timestamp() + 86400,
    }
}

/// Create a GarminClient that points to the mock server
fn test_client(mock_server: &MockServer) -> GarminClient {
    GarminClient::new_with_base_url(&mock_server.uri())
}

mod stress_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_stress_data() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/stress_2025-12-04.json");

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/dailyStress/2025-12-04"))
            .and(header("Authorization", "Bearer test-access-token"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(&token, "/wellness-service/wellness/dailyStress/2025-12-04")
            .await
            .expect("Failed to get stress data");

        assert_eq!(result["avgStressLevel"], 34);
        assert_eq!(result["maxStressLevel"], 98);
        assert!(result["stressValuesArray"].is_array());
    }

    #[tokio::test]
    async fn test_stress_values_array_parsing() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/stress_2025-12-04.json");

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/dailyStress/2025-12-04"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(&token, "/wellness-service/wellness/dailyStress/2025-12-04")
            .await
            .expect("Failed to get stress data");

        let values = result["stressValuesArray"].as_array().unwrap();
        assert!(!values.is_empty());

        // Check first entry has timestamp and stress level
        let first = values[0].as_array().unwrap();
        assert_eq!(first.len(), 2);
        assert!(first[0].is_i64()); // timestamp
        assert!(first[1].is_i64()); // stress level
    }
}

mod sleep_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_sleep_data() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/sleep_2025-12-04.json");

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/dailySleepData/TestUser"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(&token, "/wellness-service/wellness/dailySleepData/TestUser?date=2025-12-04&nonSleepBufferMinutes=60")
            .await
            .expect("Failed to get sleep data");

        let sleep_dto = &result["dailySleepDTO"];
        assert_eq!(sleep_dto["deepSleepSeconds"], 8100);
        assert_eq!(sleep_dto["lightSleepSeconds"], 15300);
        assert_eq!(sleep_dto["remSleepSeconds"], 8520);
    }

    #[tokio::test]
    async fn test_sleep_score_parsing() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/sleep_2025-12-04.json");

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/dailySleepData/TestUser"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/wellness-service/wellness/dailySleepData/TestUser?date=2025-12-04",
            )
            .await
            .expect("Failed to get sleep data");

        let score = result["dailySleepDTO"]["sleepScores"]["overall"]["value"]
            .as_i64()
            .unwrap();
        assert_eq!(score, 88);
    }

    #[tokio::test]
    async fn test_sleep_total_calculation() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/sleep_2025-12-04.json")).unwrap();
        let sleep_dto = &fixture["dailySleepDTO"];

        let deep = sleep_dto["deepSleepSeconds"].as_i64().unwrap();
        let light = sleep_dto["lightSleepSeconds"].as_i64().unwrap();
        let rem = sleep_dto["remSleepSeconds"].as_i64().unwrap();

        let total = deep + light + rem;

        // 8100 + 15300 + 8520 = 31920 seconds = 8h52m
        assert_eq!(total, 31920);
        assert_eq!(total / 3600, 8); // 8 hours
    }
}

mod activity_tests {
    use super::*;

    #[tokio::test]
    async fn test_list_activities() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/activities_list.json");

        Mock::given(method("GET"))
            .and(path("/activitylist-service/activities/search/activities"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let activities: Vec<ActivitySummary> = client
            .get_json(
                &token,
                "/activitylist-service/activities/search/activities?limit=10&start=0",
            )
            .await
            .expect("Failed to list activities");

        assert_eq!(activities.len(), 2);
        assert_eq!(activities[0].activity_id, 21247810009);
        assert_eq!(activities[0].activity_name, Some("Morning Run".to_string()));
    }

    #[tokio::test]
    async fn test_activity_summary_methods() {
        let fixture: Vec<ActivitySummary> =
            serde_json::from_str(include_str!("fixtures/activities_list.json")).unwrap();

        let activity = &fixture[0];

        assert_eq!(activity.display_name(), "Morning Run");
        assert_eq!(activity.type_key(), "running");
        assert_eq!(activity.distance_km(), Some(5.67));
        assert_eq!(activity.duration_formatted(), "29:22");
        assert_eq!(activity.date(), "2025-12-04");
    }

    #[tokio::test]
    async fn test_activity_without_optional_fields() {
        let json = r#"{
            "activityId": 123,
            "activityName": null,
            "activityType": null,
            "distance": null,
            "duration": null
        }"#;

        let activity: ActivitySummary = serde_json::from_str(json).unwrap();

        assert_eq!(activity.display_name(), "Unnamed Activity");
        assert_eq!(activity.type_key(), "unknown");
        assert_eq!(activity.distance_km(), None);
        assert_eq!(activity.duration_formatted(), "-");
    }
}

mod profile_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_social_profile() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/social_profile.json");

        Mock::given(method("GET"))
            .and(path("/userprofile-service/socialProfile"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let profile: serde_json::Value = client
            .get_json(&token, "/userprofile-service/socialProfile")
            .await
            .expect("Failed to get profile");

        assert_eq!(profile["displayName"], "TestUser");
        assert_eq!(profile["fullName"], "Test User");
    }
}

mod weight_tests {
    use super::*;

    #[tokio::test]
    async fn test_list_weight() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/weight_list.json");

        Mock::given(method("GET"))
            .and(path("/weight-service/weight/dateRange"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/weight-service/weight/dateRange?startDate=2025-12-01&endDate=2025-12-13",
            )
            .await
            .expect("Failed to list weight");

        let entries = result["dateWeightList"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["weight"], 80200);
        assert_eq!(entries[0]["calendarDate"], "2025-12-13");
    }

    #[tokio::test]
    async fn test_weight_body_composition() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/weight_list.json")).unwrap();

        let entry = &fixture["dateWeightList"][0];

        assert_eq!(entry["bmi"].as_f64().unwrap(), 24.2);
        assert_eq!(entry["bodyFat"].as_f64().unwrap(), 21.9);
        assert_eq!(entry["muscleMass"].as_i64().unwrap(), 32800);
    }

    #[tokio::test]
    async fn test_weight_total_average() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/weight_list.json")).unwrap();

        let avg = &fixture["totalAverage"];
        assert_eq!(avg["weight"].as_i64().unwrap(), 79950);
        assert_eq!(avg["bmi"].as_f64().unwrap(), 24.15);
    }
}

mod device_tests {
    use super::*;

    #[tokio::test]
    async fn test_list_devices() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/devices_list.json");

        Mock::given(method("GET"))
            .and(path("/device-service/deviceregistration/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let devices: Vec<serde_json::Value> = client
            .get_json(&token, "/device-service/deviceregistration/devices")
            .await
            .expect("Failed to list devices");

        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0]["deviceTypeName"], "Forerunner 955 Solar");
        assert_eq!(devices[0]["deviceId"], 3442975663i64);
    }

    #[tokio::test]
    async fn test_device_firmware_version() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/devices_list.json")).unwrap();

        let watch = &fixture[0];
        assert_eq!(watch["currentFirmwareVersion"], "26.08");
        assert_eq!(watch["partNumber"], "006-B4024-00");

        let scale = &fixture[1];
        assert_eq!(scale["currentFirmwareVersion"], "3.30");
    }

    #[tokio::test]
    async fn test_device_with_optional_last_sync() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/devices_list.json")).unwrap();

        // First device has lastSyncTime
        assert!(fixture[0].get("lastSyncTime").is_some());

        // Second device has no lastSyncTime
        assert!(fixture[1].get("lastSyncTime").is_none());
    }
}

mod settings_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_user_settings() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/user_settings.json");

        Mock::given(method("GET"))
            .and(path("/userprofile-service/userprofile/user-settings"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let settings: serde_json::Value = client
            .get_json(&token, "/userprofile-service/userprofile/user-settings")
            .await
            .expect("Failed to get user settings");

        assert_eq!(settings["measurementSystem"], "metric");
        assert_eq!(settings["dateFormat"], "dd/mm/yyyy");
        assert_eq!(settings["timezone"], "Europe/Madrid");
    }

    #[tokio::test]
    async fn test_user_physical_info() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/user_settings.json")).unwrap();

        assert_eq!(fixture["height"].as_f64().unwrap(), 182.0);
        assert_eq!(fixture["weight"].as_f64().unwrap(), 80200.0);
        assert_eq!(fixture["gender"], "male");
        assert_eq!(fixture["birthDate"], "1990-05-15");
    }
}

mod calories_tests {
    #[tokio::test]
    async fn test_calories_summary_fields() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/calories_summary.json")).unwrap();

        assert_eq!(fixture["totalKilocalories"].as_f64().unwrap(), 2824.0);
        assert_eq!(fixture["activeKilocalories"].as_f64().unwrap(), 715.0);
        assert_eq!(fixture["bmrKilocalories"].as_f64().unwrap(), 2109.0);
        assert!(fixture["consumedKilocalories"].is_null());
    }

    #[tokio::test]
    async fn test_calories_with_steps() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/calories_summary.json")).unwrap();

        assert_eq!(fixture["totalSteps"].as_i64().unwrap(), 20708);
        assert_eq!(fixture["dailyStepGoal"].as_i64().unwrap(), 15000);
    }
}

mod vo2max_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_vo2max() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/vo2max.json");

        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/maxmet/daily/2025-12-10/2025-12-10",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Vec<serde_json::Value> = client
            .get_json(
                &token,
                "/metrics-service/metrics/maxmet/daily/2025-12-10/2025-12-10",
            )
            .await
            .expect("Failed to get VO2 max");

        assert_eq!(result.len(), 1);
        let generic = &result[0]["generic"];
        assert_eq!(generic["vo2MaxPreciseValue"].as_f64().unwrap(), 53.0);
    }

    #[tokio::test]
    async fn test_vo2max_structure() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/vo2max.json")).unwrap();

        let entry = &fixture[0];
        assert!(entry.get("generic").is_some());
        assert!(entry.get("cycling").is_some());
        assert!(entry.get("heatAltitudeAcclimation").is_some());
    }
}

mod training_readiness_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_training_readiness() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/training_readiness.json");

        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/trainingreadiness/2025-12-13",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Vec<serde_json::Value> = client
            .get_json(
                &token,
                "/metrics-service/metrics/trainingreadiness/2025-12-13",
            )
            .await
            .expect("Failed to get training readiness");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["score"].as_i64().unwrap(), 69);
        assert_eq!(result[0]["level"], "MODERATE");
    }

    #[tokio::test]
    async fn test_training_readiness_factors() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/training_readiness.json")).unwrap();

        let entry = &fixture[0];
        assert_eq!(entry["sleepScore"].as_i64().unwrap(), 88);
        assert_eq!(entry["hrvWeeklyAverage"].as_i64().unwrap(), 65);
        assert_eq!(entry["acuteLoad"].as_i64().unwrap(), 314);
        assert_eq!(entry["recoveryTime"].as_i64().unwrap(), 273);
    }
}

mod training_status_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_training_status() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/training_status.json");

        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/trainingstatus/aggregated/2025-12-14",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/metrics-service/metrics/trainingstatus/aggregated/2025-12-14",
            )
            .await
            .expect("Failed to get training status");

        // Verify nested structure exists
        assert!(result.get("mostRecentTrainingStatus").is_some());
        assert!(result.get("mostRecentTrainingLoadBalance").is_some());
        assert!(result.get("mostRecentVO2Max").is_some());
    }

    #[tokio::test]
    async fn test_training_status_nested_extraction() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/training_status.json")).unwrap();

        // Extract training status from nested structure
        let status_data = fixture
            .get("mostRecentTrainingStatus")
            .and_then(|s| s.get("latestTrainingStatusData"))
            .and_then(|d| d.as_object())
            .and_then(|m| m.values().next())
            .expect("Failed to extract training status data");

        assert_eq!(
            status_data["trainingStatusFeedbackPhrase"],
            "UNPRODUCTIVE_4"
        );
        assert_eq!(status_data["calendarDate"], "2025-12-14");
    }

    #[tokio::test]
    async fn test_acute_training_load_dto() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/training_status.json")).unwrap();

        let load_dto = fixture
            .get("mostRecentTrainingStatus")
            .and_then(|s| s.get("latestTrainingStatusData"))
            .and_then(|d| d.as_object())
            .and_then(|m| m.values().next())
            .and_then(|entry| entry.get("acuteTrainingLoadDTO"))
            .expect("Failed to extract acute training load DTO");

        assert_eq!(load_dto["dailyTrainingLoadAcute"].as_i64().unwrap(), 268);
        assert_eq!(load_dto["dailyTrainingLoadChronic"].as_i64().unwrap(), 240);
        assert_eq!(
            load_dto["dailyAcuteChronicWorkloadRatio"].as_f64().unwrap(),
            1.1
        );
        assert_eq!(load_dto["acwrStatus"], "OPTIMAL");
    }

    #[tokio::test]
    async fn test_training_load_balance() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/training_status.json")).unwrap();

        let balance_data = fixture
            .get("mostRecentTrainingLoadBalance")
            .and_then(|b| b.get("metricsTrainingLoadBalanceDTOMap"))
            .and_then(|m| m.as_object())
            .and_then(|m| m.values().next())
            .expect("Failed to extract load balance data");

        assert_eq!(
            balance_data["trainingBalanceFeedbackPhrase"],
            "ANAEROBIC_SHORTAGE"
        );
        assert_eq!(
            balance_data["monthlyLoadAerobicHigh"].as_f64().unwrap(),
            576.51
        );
        assert_eq!(
            balance_data["monthlyLoadAerobicLow"].as_f64().unwrap(),
            323.86
        );
        assert_eq!(
            balance_data["monthlyLoadAnaerobic"].as_f64().unwrap(),
            28.39
        );
    }

    #[tokio::test]
    async fn test_training_load_targets() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/training_status.json")).unwrap();

        let balance_data = fixture
            .get("mostRecentTrainingLoadBalance")
            .and_then(|b| b.get("metricsTrainingLoadBalanceDTOMap"))
            .and_then(|m| m.as_object())
            .and_then(|m| m.values().next())
            .expect("Failed to extract load balance data");

        // Verify target ranges
        assert_eq!(
            balance_data["monthlyLoadAnaerobicTargetMin"]
                .as_i64()
                .unwrap(),
            109
        );
        assert_eq!(
            balance_data["monthlyLoadAnaerobicTargetMax"]
                .as_i64()
                .unwrap(),
            327
        );

        // Anaerobic 28.39 is below target min 109 -> shortage
        let anaerobic = balance_data["monthlyLoadAnaerobic"].as_f64().unwrap();
        let target_min = balance_data["monthlyLoadAnaerobicTargetMin"]
            .as_i64()
            .unwrap() as f64;
        assert!(anaerobic < target_min);
    }

    #[tokio::test]
    async fn test_vo2max_from_training_status() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/training_status.json")).unwrap();

        let vo2 = fixture
            .get("mostRecentVO2Max")
            .and_then(|v| v.get("generic"))
            .and_then(|g| g.get("vo2MaxValue"))
            .and_then(|v| v.as_f64())
            .expect("Failed to extract VO2 max");

        assert_eq!(vo2, 53.0);
    }
}

mod hrv_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_hrv() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/hrv.json");

        Mock::given(method("GET"))
            .and(path("/hrv-service/hrv/2025-12-13"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(&token, "/hrv-service/hrv/2025-12-13")
            .await
            .expect("Failed to get HRV data");

        let summary = &result["hrvSummary"];
        assert_eq!(summary["weeklyAvg"].as_i64().unwrap(), 65);
        assert_eq!(summary["status"], "BALANCED");
    }

    #[tokio::test]
    async fn test_hrv_baseline() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/hrv.json")).unwrap();

        let baseline = &fixture["hrvSummary"]["baseline"];
        assert_eq!(baseline["lowUpper"].as_i64().unwrap(), 61);
        assert_eq!(baseline["balancedUpper"].as_i64().unwrap(), 80);
    }
}

mod fitness_age_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_fitness_age() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/fitness_age.json");

        Mock::given(method("GET"))
            .and(path("/fitnessage-service/fitnessage/2025-12-13"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(&token, "/fitnessage-service/fitnessage/2025-12-13")
            .await
            .expect("Failed to get fitness age");

        assert_eq!(result["fitnessAge"].as_f64().unwrap(), 37.0);
        assert_eq!(result["chronologicalAge"].as_f64().unwrap(), 43.0);
    }

    #[tokio::test]
    async fn test_fitness_age_metrics() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/fitness_age.json")).unwrap();

        assert_eq!(fixture["vo2Max"].as_f64().unwrap(), 53.0);
        assert_eq!(fixture["bmi"].as_f64().unwrap(), 24.2);
        assert_eq!(fixture["restingHeartRate"].as_i64().unwrap(), 43);
        assert_eq!(fixture["vigorousActivityMinutes"].as_i64().unwrap(), 150);
    }
}

mod lactate_threshold_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_lactate_threshold() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/lactate_threshold.json");

        Mock::given(method("GET"))
            .and(path("/biometric-service/biometric/latestLactateThreshold"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/biometric-service/biometric/latestLactateThreshold",
            )
            .await
            .expect("Failed to get lactate threshold");

        assert_eq!(result["lactateThresholdHeartRate"].as_i64().unwrap(), 168);
        assert_eq!(result["lactateThresholdSpeed"].as_f64().unwrap(), 3.85);
        assert_eq!(result["functionalThresholdPower"].as_i64().unwrap(), 280);
    }

    #[tokio::test]
    async fn test_lactate_threshold_pace_calculation() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/lactate_threshold.json")).unwrap();

        // Speed is in m/s, convert to min/km pace
        let speed_ms = fixture["lactateThresholdSpeed"].as_f64().unwrap();
        let pace_sec_per_km = 1000.0 / speed_ms;
        let pace_min = (pace_sec_per_km / 60.0).floor() as i64;
        let pace_sec = (pace_sec_per_km % 60.0) as i64;

        // 3.85 m/s = ~4:20/km
        assert_eq!(pace_min, 4);
        assert!((19..=21).contains(&pace_sec));
    }
}

mod race_predictions_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_race_predictions() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/race_predictions.json");

        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/racepredictions/latest/testuser",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/metrics-service/metrics/racepredictions/latest/testuser",
            )
            .await
            .expect("Failed to get race predictions");

        // API returns flat structure with time5K, time10K, etc.
        assert!(result.get("time5K").is_some());
        assert!(result.get("time10K").is_some());
        assert!(result.get("timeHalfMarathon").is_some());
        assert!(result.get("timeMarathon").is_some());
    }

    #[tokio::test]
    async fn test_race_prediction_time_formatting() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/race_predictions.json")).unwrap();

        // 5K time: 1245 seconds = 20:45
        let time_5k = fixture["time5K"].as_f64().unwrap();
        let mins = (time_5k / 60.0).floor() as i64;
        let secs = (time_5k % 60.0) as i64;
        assert_eq!(mins, 20);
        assert_eq!(secs, 45);
    }
}

mod endurance_score_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_endurance_score() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/endurance_score.json");

        Mock::given(method("GET"))
            .and(path("/metrics-service/metrics/endurancescore"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Vec<serde_json::Value> = client
            .get_json(&token, "/metrics-service/metrics/endurancescore?startDate=2025-12-01&endDate=2025-12-10&aggregation=daily")
            .await
            .expect("Failed to get endurance score");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["overallScore"].as_i64().unwrap(), 72);
        assert_eq!(result[0]["classification"], "GOOD");
    }

    #[tokio::test]
    async fn test_endurance_score_factors() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/endurance_score.json")).unwrap();

        let entry = &fixture[0];
        assert_eq!(entry["vo2MaxFactor"].as_i64().unwrap(), 85);
        assert_eq!(entry["trainingHistoryFactor"].as_i64().unwrap(), 68);
        assert_eq!(entry["activityHistoryFactor"].as_i64().unwrap(), 70);
    }
}

mod hill_score_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_hill_score() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/hill_score.json");

        Mock::given(method("GET"))
            .and(path("/metrics-service/metrics/hillscore"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Vec<serde_json::Value> = client
            .get_json(&token, "/metrics-service/metrics/hillscore?startDate=2025-12-01&endDate=2025-12-10&aggregation=daily")
            .await
            .expect("Failed to get hill score");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["overallScore"].as_i64().unwrap(), 58);
        assert_eq!(result[0]["classification"], "FAIR");
    }

    #[tokio::test]
    async fn test_hill_score_factors() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/hill_score.json")).unwrap();

        let entry = &fixture[0];
        assert_eq!(entry["strengthFactor"].as_i64().unwrap(), 62);
        assert_eq!(entry["enduranceFactor"].as_i64().unwrap(), 55);
        assert_eq!(entry["powerFactor"].as_i64().unwrap(), 58);
    }
}

mod spo2_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_spo2() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/spo2.json");

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/daily/spo2/2025-12-10"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(&token, "/wellness-service/wellness/daily/spo2/2025-12-10")
            .await
            .expect("Failed to get SpO2 data");

        assert_eq!(result["averageSpO2"].as_i64().unwrap(), 96);
        assert_eq!(result["lowestSpO2"].as_i64().unwrap(), 91);
        assert_eq!(result["latestSpO2"].as_i64().unwrap(), 97);
    }

    #[tokio::test]
    async fn test_spo2_hourly_averages() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/spo2.json")).unwrap();

        let hourly = fixture["spO2HourlyAverages"].as_array().unwrap();
        assert!(!hourly.is_empty());
        assert!(hourly[0].get("hour").is_some());
        assert!(hourly[0].get("value").is_some());
    }
}

mod respiration_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_respiration() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/respiration.json");

        Mock::given(method("GET"))
            .and(path(
                "/wellness-service/wellness/daily/respiration/2025-12-10",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/wellness-service/wellness/daily/respiration/2025-12-10",
            )
            .await
            .expect("Failed to get respiration data");

        assert_eq!(result["avgWakingRespirationValue"].as_f64().unwrap(), 15.5);
        assert_eq!(result["avgSleepRespirationValue"].as_f64().unwrap(), 13.2);
    }

    #[tokio::test]
    async fn test_respiration_range() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/respiration.json")).unwrap();

        let high = fixture["highestRespirationValue"].as_f64().unwrap();
        let low = fixture["lowestRespirationValue"].as_f64().unwrap();

        assert_eq!(high, 22.0);
        assert_eq!(low, 11.0);
        assert!(high > low);
    }
}

mod intensity_minutes_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_intensity_minutes() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/intensity_minutes.json");

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/daily/im/2025-12-10"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(&token, "/wellness-service/wellness/daily/im/2025-12-10")
            .await
            .expect("Failed to get intensity minutes");

        assert_eq!(result["weeklyGoal"].as_i64().unwrap(), 150);
        assert_eq!(result["totalIntensityMinutes"].as_i64().unwrap(), 105);
    }

    #[tokio::test]
    async fn test_intensity_minutes_calculation() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/intensity_minutes.json")).unwrap();

        let moderate = fixture["moderateIntensityMinutes"].as_i64().unwrap();
        let vigorous = fixture["vigorousIntensityMinutes"].as_i64().unwrap();
        let total = fixture["totalIntensityMinutes"].as_i64().unwrap();

        // Vigorous counts double
        assert_eq!(moderate + vigorous * 2, total);
    }
}

mod blood_pressure_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_blood_pressure() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/blood_pressure.json");

        Mock::given(method("GET"))
            .and(path(
                "/bloodpressure-service/bloodpressure/range/2025-12-01/2025-12-10",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/bloodpressure-service/bloodpressure/range/2025-12-01/2025-12-10",
            )
            .await
            .expect("Failed to get blood pressure");

        let measurements = result["measurementSummaries"].as_array().unwrap();
        assert_eq!(measurements.len(), 2);
        assert_eq!(measurements[0]["systolic"].as_i64().unwrap(), 118);
        assert_eq!(measurements[0]["diastolic"].as_i64().unwrap(), 75);
    }

    #[tokio::test]
    async fn test_blood_pressure_classification() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/blood_pressure.json")).unwrap();

        let measurements = fixture["measurementSummaries"].as_array().unwrap();
        let entry = &measurements[0];

        let systolic = entry["systolic"].as_i64().unwrap();
        let diastolic = entry["diastolic"].as_i64().unwrap();

        // Normal: < 120 systolic AND < 80 diastolic
        assert!(systolic < 120);
        assert!(diastolic < 80);
    }
}

mod hydration_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_hydration() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/hydration.json");

        Mock::given(method("GET"))
            .and(path(
                "/usersummary-service/usersummary/hydration/daily/2025-12-10",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: serde_json::Value = client
            .get_json(
                &token,
                "/usersummary-service/usersummary/hydration/daily/2025-12-10",
            )
            .await
            .expect("Failed to get hydration data");

        assert_eq!(result["valueInML"].as_i64().unwrap(), 2400);
        assert_eq!(result["goalInML"].as_i64().unwrap(), 2500);
    }

    #[tokio::test]
    async fn test_hydration_percentage() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/hydration.json")).unwrap();

        let value = fixture["valueInML"].as_i64().unwrap() as f64;
        let goal = fixture["goalInML"].as_i64().unwrap() as f64;
        let pct = (value / goal * 100.0) as i64;

        assert_eq!(pct, 96);
    }
}

mod personal_records_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_personal_records() {
        let mock_server = MockServer::start().await;
        let fixture = include_str!("fixtures/personal_records.json");

        Mock::given(method("GET"))
            .and(path("/personalrecord-service/personalrecord/prs/TestUser"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Vec<serde_json::Value> = client
            .get_json(
                &token,
                "/personalrecord-service/personalrecord/prs/TestUser",
            )
            .await
            .expect("Failed to get personal records");

        // API returns array directly with typeId field
        assert_eq!(result.len(), 4);
        assert_eq!(result[0]["typeId"], 3); // 3 = Fastest 5K
    }

    #[tokio::test]
    async fn test_personal_record_time_formatting() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/personal_records.json")).unwrap();

        // 5K PR: 1198 seconds = 19:58
        let time_5k = fixture[0]["value"].as_f64().unwrap();
        let mins = (time_5k / 60.0).floor() as i64;
        let secs = (time_5k % 60.0) as i64;
        assert_eq!(mins, 19);
        assert_eq!(secs, 58);
    }
}

mod insights_tests {
    /// Test restorative sleep ratio calculation
    /// Restorative = (deep + REM) / total * 100
    #[tokio::test]
    async fn test_restorative_sleep_ratio_calculation() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/insights_sleep_7days.json")).unwrap();

        // Dec 18: 13800s total, 1440 deep + 1560 rem = 3000s
        let day1 = &fixture[0]["dailySleepDTO"];
        let total = day1["sleepTimeSeconds"].as_i64().unwrap();
        let deep = day1["deepSleepSeconds"].as_i64().unwrap();
        let rem = day1["remSleepSeconds"].as_i64().unwrap();
        let ratio = (deep + rem) as f64 / total as f64 * 100.0;

        // 3000 / 13800 * 100 = 21.7%
        assert!(
            ratio > 21.0 && ratio < 22.0,
            "Expected ~21.7%, got {}",
            ratio
        );
    }

    #[tokio::test]
    async fn test_high_restorative_sleep_ratio() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/insights_sleep_7days.json")).unwrap();

        // Dec 17: 28200s total, 8400 deep + 6180 rem = 14580s
        let day2 = &fixture[1]["dailySleepDTO"];
        let total = day2["sleepTimeSeconds"].as_i64().unwrap();
        let deep = day2["deepSleepSeconds"].as_i64().unwrap();
        let rem = day2["remSleepSeconds"].as_i64().unwrap();
        let ratio = (deep + rem) as f64 / total as f64 * 100.0;

        // 14580 / 28200 * 100 = 51.7%
        assert!(ratio > 50.0, "Expected >50%, got {}", ratio);
    }

    #[tokio::test]
    async fn test_stress_data_parsing() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/insights_stress_7days.json")).unwrap();

        assert_eq!(fixture.len(), 7);
        assert_eq!(fixture[0]["avgStressLevel"].as_i64().unwrap(), 23);
        assert_eq!(fixture[2]["avgStressLevel"].as_i64().unwrap(), 42); // Dec 16 high stress
    }

    #[tokio::test]
    async fn test_sleep_stress_correlation_pattern() {
        let sleep_fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/insights_sleep_7days.json")).unwrap();
        let stress_fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/insights_stress_7days.json")).unwrap();

        // Build correlation data: each night's restorative % vs next day's stress
        let mut low_restorative_stress: Vec<i64> = vec![];
        let mut high_restorative_stress: Vec<i64> = vec![];

        for i in 1..sleep_fixture.len() {
            let sleep_day = &sleep_fixture[i]["dailySleepDTO"];
            let total = sleep_day["sleepTimeSeconds"].as_i64().unwrap_or(1);
            let deep = sleep_day["deepSleepSeconds"].as_i64().unwrap_or(0);
            let rem = sleep_day["remSleepSeconds"].as_i64().unwrap_or(0);
            let ratio = (deep + rem) as f64 / total as f64 * 100.0;

            // Next day stress (stress data is aligned with sleep data dates)
            let next_day_stress = stress_fixture[i - 1]["avgStressLevel"]
                .as_i64()
                .unwrap_or(0);

            if ratio < 35.0 {
                low_restorative_stress.push(next_day_stress);
            } else if ratio > 45.0 {
                high_restorative_stress.push(next_day_stress);
            }
        }

        // Calculate averages
        let low_avg: f64 = if low_restorative_stress.is_empty() {
            0.0
        } else {
            low_restorative_stress.iter().sum::<i64>() as f64 / low_restorative_stress.len() as f64
        };

        let high_avg: f64 = if high_restorative_stress.is_empty() {
            0.0
        } else {
            high_restorative_stress.iter().sum::<i64>() as f64
                / high_restorative_stress.len() as f64
        };

        // High restorative sleep should correlate with lower stress
        assert!(
            high_avg < low_avg || high_restorative_stress.is_empty(),
            "Expected high restorative avg ({}) < low restorative avg ({})",
            high_avg,
            low_avg
        );
    }

    #[tokio::test]
    async fn test_stress_prediction_categories() {
        // Test categorization logic
        let ratios = [22.0, 35.0, 52.0];
        let expected_predictions = ["HIGH", "MODERATE", "LOW"];

        for (ratio, expected) in ratios.iter().zip(expected_predictions.iter()) {
            let prediction = if *ratio < 30.0 {
                "HIGH"
            } else if *ratio < 45.0 {
                "MODERATE"
            } else {
                "LOW"
            };
            assert_eq!(
                prediction, *expected,
                "Ratio {} should predict {} stress",
                ratio, expected
            );
        }
    }

    #[tokio::test]
    async fn test_average_restorative_ratio() {
        let fixture: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/insights_sleep_7days.json")).unwrap();

        let mut total_ratio = 0.0;
        let mut count = 0;

        for entry in &fixture {
            let sleep_dto = &entry["dailySleepDTO"];
            let total = sleep_dto["sleepTimeSeconds"].as_i64().unwrap_or(0);
            if total == 0 {
                continue;
            }
            let deep = sleep_dto["deepSleepSeconds"].as_i64().unwrap_or(0);
            let rem = sleep_dto["remSleepSeconds"].as_i64().unwrap_or(0);
            let ratio = (deep + rem) as f64 / total as f64 * 100.0;
            total_ratio += ratio;
            count += 1;
        }

        let avg_ratio = total_ratio / count as f64;

        // Should be between 20% and 55%
        assert!(
            avg_ratio > 20.0 && avg_ratio < 55.0,
            "Average ratio {} out of expected range",
            avg_ratio
        );
    }
}

mod error_handling_tests {
    use super::*;

    #[tokio::test]
    async fn test_unauthorized_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Result<serde_json::Value, _> = client.get_json(&token, "/test").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            garmin_cli::error::GarminError::NotAuthenticated
        ));
    }

    #[tokio::test]
    async fn test_rate_limited_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Result<serde_json::Value, _> = client.get_json(&token, "/test").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, garmin_cli::error::GarminError::RateLimited));
    }

    #[tokio::test]
    async fn test_not_found_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Result<serde_json::Value, _> = client.get_json(&token, "/test").await;

        assert!(result.is_err());
    }
}
