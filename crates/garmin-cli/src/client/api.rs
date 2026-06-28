//! Garmin Connect API client for authenticated requests
//!
//! This module provides a high-level client for making authenticated requests
//! to the Garmin Connect API using OAuth2 bearer tokens.

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT};
use reqwest::{multipart, Client, Response, StatusCode};
use serde::de::DeserializeOwned;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::client::tokens::OAuth2Token;
use crate::error::{GarminError, Result};

/// User agent for Connect API requests
const API_USER_AGENT: &str = "GCM-iOS-5.7.2.1";

/// Garmin Connect API client
#[derive(Clone)]
pub struct GarminClient {
    client: Client,
    base_url: String,
    logger: Option<RequestLogger>,
}

/// Shared sync/request log writer.
#[derive(Clone)]
pub struct RequestLogger {
    inner: Arc<Mutex<std::fs::File>>,
    verbose: bool,
}

impl RequestLogger {
    /// Create a log writer.
    pub fn new(file: std::fs::File, verbose: bool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(file)),
            verbose,
        }
    }

    /// Whether full JSON response logging is enabled.
    pub fn verbose(&self) -> bool {
        self.verbose
    }

    /// Write a single timestamped line.
    pub fn log(&self, message: impl AsRef<str>) {
        let now = chrono::Utc::now().to_rfc3339();
        if let Ok(mut file) = self.inner.lock() {
            let _ = writeln!(file, "[{}] {}", now, message.as_ref());
        }
    }

    fn log_response(&self, method: &str, path: &str, status: StatusCode, body: &str) {
        if self.verbose {
            self.log(format!(
                "{} {} -> {}\n{}",
                method,
                path,
                status,
                if body.is_empty() { "<empty>" } else { body }
            ));
        }
    }
}

impl GarminClient {
    /// Create a new API client for Garmin Connect.
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: "https://connectapi.garmin.com".to_string(),
            logger: None,
        }
    }

    /// Attach a shared request logger.
    pub fn with_logger(mut self, logger: Option<RequestLogger>) -> Self {
        self.logger = logger;
        self
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
            logger: None,
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
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        token: &OAuth2Token,
        path: &str,
    ) -> Result<T> {
        let response = self.get(token, path).await?;
        let status = response.status();
        let text = response.text().await.map_err(GarminError::Http)?;
        if let Some(logger) = &self.logger {
            logger.log_response("GET", path, status, &text);
        }
        serde_json::from_str(&text).map_err(|e| {
            if let Some(logger) = &self.logger {
                logger.log(format!(
                    "Malformed JSON response from {} (status {}): {}; body preview: {}",
                    path,
                    status,
                    e,
                    response_preview(&text)
                ));
            }
            GarminError::invalid_response(format!(
                "Failed to parse JSON response from {} (status {}): {}; body preview: {}",
                path,
                status,
                e,
                response_preview(&text)
            ))
        })
    }

    /// Make an authenticated GET request and deserialize an optional JSON response.
    ///
    /// Garmin returns 204/empty bodies for some optional historical datasets.
    /// Callers should use this only where "no body" means "no data".
    pub async fn get_optional_json<T: DeserializeOwned>(
        &self,
        token: &OAuth2Token,
        path: &str,
    ) -> Result<Option<T>> {
        let response = self.get(token, path).await?;
        let status = response.status();
        let text = response.text().await.map_err(GarminError::Http)?;
        if let Some(logger) = &self.logger {
            logger.log_response("GET", path, status, &text);
        }

        if status == StatusCode::NO_CONTENT || text.trim().is_empty() {
            return Ok(None);
        }

        serde_json::from_str(&text).map(Some).map_err(|e| {
            if let Some(logger) = &self.logger {
                logger.log(format!(
                    "Malformed JSON response from {} (status {}): {}; body preview: {}",
                    path,
                    status,
                    e,
                    response_preview(&text)
                ));
            }
            GarminError::invalid_response(format!(
                "Failed to parse JSON response from {} (status {}): {}; body preview: {}",
                path,
                status,
                e,
                response_preview(&text)
            ))
        })
    }

    /// Make an authenticated POST request with JSON body
    pub async fn post_json(
        &self,
        token: &OAuth2Token,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = self.build_url(path);
        let headers = self.build_headers(token);

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(body)
            .send()
            .await
            .map_err(GarminError::Http)?;

        let response = self.handle_response_status(response).await?;
        let status = response.status();
        let text = response.text().await.map_err(GarminError::Http)?;
        if let Some(logger) = &self.logger {
            logger.log_response("POST", path, status, &text);
        }
        serde_json::from_str(&text).map_err(|e| {
            if let Some(logger) = &self.logger {
                logger.log(format!(
                    "Malformed JSON response from {} (status {}): {}; body preview: {}",
                    path,
                    status,
                    e,
                    response_preview(&text)
                ));
            }
            GarminError::invalid_response(format!(
                "Failed to parse JSON response from {} (status {}): {}; body preview: {}",
                path,
                status,
                e,
                response_preview(&text)
            ))
        })
    }

    /// Make an authenticated PUT request with JSON body
    pub async fn put_json(
        &self,
        token: &OAuth2Token,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = self.build_url(path);
        let headers = self.build_headers(token);

        let response = self
            .client
            .put(&url)
            .headers(headers)
            .json(body)
            .send()
            .await
            .map_err(GarminError::Http)?;

        let response = self.handle_response_status(response).await?;
        // Garmin may answer an activity update with an empty body; treat that as success.
        let text = response.text().await.unwrap_or_default();
        if text.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse JSON response: {}", e))
        })
    }

    /// Make an authenticated GET request and return raw bytes (for file downloads)
    pub async fn download(&self, token: &OAuth2Token, path: &str) -> Result<Bytes> {
        let response = self.get(token, path).await?;
        response.bytes().await.map_err(GarminError::Http)
    }

    /// Upload a file using multipart form data
    pub async fn upload(
        &self,
        token: &OAuth2Token,
        path: &str,
        file_path: &Path,
    ) -> Result<serde_json::Value> {
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
            StatusCode::OK
            | StatusCode::CREATED
            | StatusCode::ACCEPTED
            | StatusCode::NO_CONTENT => Ok(response),
            StatusCode::UNAUTHORIZED => Err(GarminError::NotAuthenticated),
            StatusCode::TOO_MANY_REQUESTS => Err(GarminError::RateLimited),
            StatusCode::NOT_FOUND => {
                let url = response.url().to_string();
                Err(GarminError::NotFound(url))
            }
            _ => {
                let status_code = status.as_u16();
                let body = response.text().await.unwrap_or_default();
                Err(GarminError::Api {
                    status: status_code,
                    message: body,
                })
            }
        }
    }
}

fn response_preview(text: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 160;
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let preview = compact.chars().take(MAX_PREVIEW_CHARS).collect::<String>();
    if compact.chars().count() > MAX_PREVIEW_CHARS {
        format!("{}...", preview)
    } else if preview.is_empty() {
        "<empty>".to_string()
    } else {
        preview
    }
}

impl Default for GarminClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::OAuth2Token;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_build_url() {
        let client = GarminClient::new();
        assert_eq!(
            client.build_url("/activity-service/activity/123"),
            "https://connectapi.garmin.com/activity-service/activity/123"
        );
    }

    #[test]
    fn test_client_creation() {
        let client = GarminClient::new();
        assert_eq!(client.base_url, "https://connectapi.garmin.com");
    }

    #[tokio::test]
    async fn test_get_optional_json_treats_204_as_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/optional"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let client = GarminClient::new_with_base_url(&server.uri());
        let value: Option<serde_json::Value> = client
            .get_optional_json(&test_token(), "/optional")
            .await
            .unwrap();

        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_get_json_error_includes_body_preview() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/bad-json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>not json</html>"))
            .mount(&server)
            .await;

        let client = GarminClient::new_with_base_url(&server.uri());
        let error = client
            .get_json::<serde_json::Value>(&test_token(), "/bad-json")
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("/bad-json"));
        assert!(message.contains("<html>not json</html>"));
    }

    fn test_token() -> OAuth2Token {
        OAuth2Token {
            scope: "test".to_string(),
            jti: "jti".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "access".to_string(),
            refresh_token: "refresh".to_string(),
            expires_in: 3600,
            expires_at: 0,
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: 0,
        }
    }
}
