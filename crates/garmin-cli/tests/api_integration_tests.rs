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
            .get_json(&token, "/wellness-service/wellness/dailySleepData/TestUser?date=2025-12-04")
            .await
            .expect("Failed to get sleep data");

        let score = result["dailySleepDTO"]["sleepScores"]["overall"]["value"]
            .as_i64()
            .unwrap();
        assert_eq!(score, 88);
    }

    #[tokio::test]
    async fn test_sleep_total_calculation() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/sleep_2025-12-04.json")).unwrap();
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
            .get_json(&token, "/activitylist-service/activities/search/activities?limit=10&start=0")
            .await
            .expect("Failed to list activities");

        assert_eq!(activities.len(), 2);
        assert_eq!(activities[0].activity_id, 21247810009);
        assert_eq!(activities[0].activity_name, Some("Morning Run".to_string()));
    }

    #[tokio::test]
    async fn test_activity_summary_methods() {
        let fixture: Vec<ActivitySummary> = serde_json::from_str(include_str!("fixtures/activities_list.json")).unwrap();

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
            .get_json(&token, "/weight-service/weight/dateRange?startDate=2025-12-01&endDate=2025-12-13")
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
            .and(path("/metrics-service/metrics/maxmet/daily/2025-12-10/2025-12-10"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Vec<serde_json::Value> = client
            .get_json(&token, "/metrics-service/metrics/maxmet/daily/2025-12-10/2025-12-10")
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
            .and(path("/metrics-service/metrics/trainingreadiness/2025-12-13"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&mock_server)
            .await;

        let client = test_client(&mock_server);
        let token = test_token();

        let result: Vec<serde_json::Value> = client
            .get_json(&token, "/metrics-service/metrics/trainingreadiness/2025-12-13")
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
        assert!(matches!(err, garmin_cli::error::GarminError::NotAuthenticated));
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
