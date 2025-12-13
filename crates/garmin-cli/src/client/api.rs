//! Garmin Connect API client for authenticated requests
//!
//! This module provides a high-level client for making authenticated requests
//! to the Garmin Connect API using OAuth2 bearer tokens.

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT};
use reqwest::{multipart, Client, Response, StatusCode};
use serde::de::DeserializeOwned;
use std::path::Path;

use crate::client::tokens::OAuth2Token;
use crate::error::{GarminError, Result};

/// User agent for Connect API requests
const API_USER_AGENT: &str = "GCM-iOS-5.7.2.1";

/// Garmin Connect API client
pub struct GarminClient {
    client: Client,
    base_url: String,
}

impl GarminClient {
    /// Create a new API client for the given domain
    pub fn new(domain: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: format!("https://connectapi.{}", domain),
        }
    }

    /// Create a new API client with a custom base URL (for testing)
    #[doc(hidden)]
    pub fn new_with_base_url(base_url: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: base_url.to_string(),
        }
    }

    /// Build the full URL for a given path
    fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Build headers with authorization
    fn build_headers(&self, token: &OAuth2Token) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(API_USER_AGENT));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&token.authorization_header()).unwrap(),
        );
        headers
    }

    /// Make an authenticated GET request and return the response
    pub async fn get(&self, token: &OAuth2Token, path: &str) -> Result<Response> {
        let url = self.build_url(path);
        let headers = self.build_headers(token);

        let response = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(GarminError::Http)?;

        self.handle_response_status(response).await
    }

    /// Make an authenticated GET request and deserialize JSON response
    pub async fn get_json<T: DeserializeOwned>(&self, token: &OAuth2Token, path: &str) -> Result<T> {
        let response = self.get(token, path).await?;
        response.json().await.map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse JSON response: {}", e))
        })
    }

    /// Make an authenticated GET request and return raw bytes (for file downloads)
    pub async fn download(&self, token: &OAuth2Token, path: &str) -> Result<Bytes> {
        let response = self.get(token, path).await?;
        response.bytes().await.map_err(GarminError::Http)
    }

    /// Upload a file using multipart form data
    pub async fn upload(&self, token: &OAuth2Token, path: &str, file_path: &Path) -> Result<serde_json::Value> {
        let url = self.build_url(path);
        let headers = self.build_headers(token);

        // Read the file
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("activity.fit")
            .to_string();

        let file_bytes = tokio::fs::read(file_path)
            .await
            .map_err(|e| GarminError::invalid_response(format!("Failed to read file: {}", e)))?;

        // Create multipart form
        let part = multipart::Part::bytes(file_bytes)
            .file_name(file_name)
            .mime_str("application/octet-stream")
            .map_err(|e| GarminError::invalid_response(format!("Invalid MIME type: {}", e)))?;

        let form = multipart::Form::new().part("file", part);

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .multipart(form)
            .send()
            .await
            .map_err(GarminError::Http)?;

        let response = self.handle_response_status(response).await?;
        response.json().await.map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse upload response: {}", e))
        })
    }

    /// Handle response status codes and convert to errors
    async fn handle_response_status(&self, response: Response) -> Result<Response> {
        let status = response.status();

        match status {
            StatusCode::OK | StatusCode::CREATED | StatusCode::ACCEPTED | StatusCode::NO_CONTENT => {
                Ok(response)
            }
            StatusCode::UNAUTHORIZED => Err(GarminError::NotAuthenticated),
            StatusCode::TOO_MANY_REQUESTS => Err(GarminError::RateLimited),
            StatusCode::NOT_FOUND => {
                Err(GarminError::invalid_response("Resource not found"))
            }
            _ => {
                let body = response.text().await.unwrap_or_default();
                Err(GarminError::invalid_response(format!(
                    "API error {}: {}",
                    status, body
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_url() {
        let client = GarminClient::new("garmin.com");
        assert_eq!(
            client.build_url("/activity-service/activity/123"),
            "https://connectapi.garmin.com/activity-service/activity/123"
        );
    }

    #[test]
    fn test_client_creation() {
        let client = GarminClient::new("garmin.com");
        assert_eq!(client.base_url, "https://connectapi.garmin.com");
    }
}
