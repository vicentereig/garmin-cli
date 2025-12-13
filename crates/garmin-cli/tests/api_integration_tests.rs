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
