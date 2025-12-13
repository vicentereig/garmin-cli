//! Garmin SSO Authentication
//!
//! Implements the Garmin Connect SSO login flow, ported from the Python Garth library.

use crate::client::oauth1::{parse_oauth_response, OAuth1Signer, OAuthConsumer, OAuthToken};
use crate::client::tokens::{OAuth1Token, OAuth2Token};
use crate::error::{GarminError, Result};
use regex::Regex;
use reqwest::cookie::Jar;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, REFERER, USER_AGENT};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default Garmin domain
const DEFAULT_DOMAIN: &str = "garmin.com";

/// User agent mimicking the Garmin mobile app
const MOBILE_USER_AGENT: &str = "com.garmin.android.apps.connectmobile";

/// User agent for Connect API requests
const API_USER_AGENT: &str = "GCM-iOS-5.7.2.1";

/// URL to fetch OAuth consumer credentials
const OAUTH_CONSUMER_URL: &str = "https://thegarth.s3.amazonaws.com/oauth_consumer.json";

/// OAuth consumer credentials fetched from Garth's S3
#[derive(Debug, Deserialize)]
struct OAuthConsumerResponse {
    consumer_key: String,
    consumer_secret: String,
}

/// SSO Client for Garmin authentication
pub struct SsoClient {
    client: Client,
    domain: String,
    last_url: Option<String>,
}

impl SsoClient {
    /// Create a new SSO client
    pub fn new(domain: Option<&str>) -> Result<Self> {
        let cookie_jar = Arc::new(Jar::default());
        let client = Client::builder()
            .cookie_provider(cookie_jar)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(GarminError::Http)?;

        Ok(Self {
            client,
            domain: domain.unwrap_or(DEFAULT_DOMAIN).to_string(),
            last_url: None,
        })
    }

    /// Perform full login flow
    pub async fn login(
        &mut self,
        email: &str,
        password: &str,
        mfa_callback: Option<impl FnOnce() -> String>,
    ) -> Result<(OAuth1Token, OAuth2Token)> {
        // Step 1: Initialize session and get CSRF token
        let csrf_token = self.init_session_and_get_csrf().await?;

        // Step 2: Submit login form
        let login_result = self.submit_login(email, password, &csrf_token).await?;

        // Step 3: Handle MFA if required
        let ticket = match login_result {
            LoginResult::Success(ticket) => ticket,
            LoginResult::MfaRequired => {
                let mfa_code = mfa_callback
                    .ok_or_else(|| GarminError::MfaRequired)?();
                self.submit_mfa(&mfa_code, &csrf_token).await?
            }
        };

        // Step 4: Exchange ticket for OAuth1 token
        let oauth1 = self.get_oauth1_token(&ticket).await?;

        // Step 5: Exchange OAuth1 for OAuth2 token
        let oauth2 = self.exchange_oauth1_for_oauth2(&oauth1).await?;

        Ok((oauth1, oauth2))
    }

    /// Initialize session and extract CSRF token
    async fn init_session_and_get_csrf(&mut self) -> Result<String> {
        let sso_base = format!("https://sso.{}/sso", self.domain);
        let sso_embed = format!("{}/embed", sso_base);

        // First request to set cookies (gauthHost points to SSO without /embed)
        let embed_params = [
            ("id", "gauth-widget"),
            ("embedWidget", "true"),
            ("gauthHost", sso_base.as_str()),
        ];

        let resp = self.client
            .get(&sso_embed)
            .query(&embed_params)
            .header(USER_AGENT, API_USER_AGENT)
            .send()
            .await
            .map_err(GarminError::Http)?;

        // Consume the response body
        let _ = resp.text().await;

        // Second request to get CSRF token (gauthHost now points to SSO_EMBED)
        let signin_url = format!("{}/signin", sso_base);
        let signin_params = [
            ("id", "gauth-widget"),
            ("embedWidget", "true"),
            ("gauthHost", sso_embed.as_str()),
            ("service", sso_embed.as_str()),
            ("source", sso_embed.as_str()),
            ("redirectAfterAccountLoginUrl", sso_embed.as_str()),
            ("redirectAfterAccountCreationUrl", sso_embed.as_str()),
        ];

        let response = self
            .client
            .get(&signin_url)
            .query(&signin_params)
            .header(USER_AGENT, API_USER_AGENT)
            .send()
            .await
            .map_err(GarminError::Http)?;

        self.last_url = Some(response.url().to_string());
        let html = response.text().await.map_err(GarminError::Http)?;

        extract_csrf_token(&html)
    }

    /// Submit login form with email and password
    async fn submit_login(
        &mut self,
        email: &str,
        password: &str,
        csrf_token: &str,
    ) -> Result<LoginResult> {
        let sso_base = format!("https://sso.{}/sso", self.domain);
        let sso_embed = format!("{}/embed", sso_base);
        let signin_url = format!("{}/signin", sso_base);

        let signin_params = [
            ("id", "gauth-widget"),
            ("embedWidget", "true"),
            ("gauthHost", sso_embed.as_str()),
            ("service", sso_embed.as_str()),
            ("source", sso_embed.as_str()),
            ("redirectAfterAccountLoginUrl", sso_embed.as_str()),
            ("redirectAfterAccountCreationUrl", sso_embed.as_str()),
        ];

        let form_data = [
            ("username", email),
            ("password", password),
            ("embed", "true"),
            ("_csrf", csrf_token),
        ];

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(API_USER_AGENT));
        if let Some(ref referer) = self.last_url {
            headers.insert(REFERER, HeaderValue::from_str(referer).unwrap());
        }
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );

        let response = self
            .client
            .post(&signin_url)
            .query(&signin_params)
            .headers(headers)
            .form(&form_data)
            .send()
            .await
            .map_err(GarminError::Http)?;

        self.last_url = Some(response.url().to_string());
        let html = response.text().await.map_err(GarminError::Http)?;

        // Check response title
        let title = extract_title(&html)?;

        if title.contains("MFA") {
            Ok(LoginResult::MfaRequired)
        } else if title == "Success" {
            let ticket = extract_ticket(&html)?;
            Ok(LoginResult::Success(ticket))
        } else {
            Err(GarminError::auth(format!("Unexpected login response: {}", title)))
        }
    }

    /// Submit MFA code
    async fn submit_mfa(&mut self, mfa_code: &str, csrf_token: &str) -> Result<String> {
        let sso_base = format!("https://sso.{}/sso", self.domain);
        let sso_embed = format!("{}/embed", sso_base);
        let mfa_url = format!("{}/verifyMFA/loginEnterMfaCode", sso_base);

        let signin_params = [
            ("id", "gauth-widget"),
            ("embedWidget", "true"),
            ("gauthHost", sso_embed.as_str()),
            ("service", sso_embed.as_str()),
            ("source", sso_embed.as_str()),
            ("redirectAfterAccountLoginUrl", sso_embed.as_str()),
            ("redirectAfterAccountCreationUrl", sso_embed.as_str()),
        ];

        let form_data = [
            ("mfa-code", mfa_code),
            ("embed", "true"),
            ("_csrf", csrf_token),
            ("fromPage", "setupEnterMfaCode"),
        ];

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(API_USER_AGENT));
        if let Some(ref referer) = self.last_url {
            headers.insert(REFERER, HeaderValue::from_str(referer).unwrap());
        }

        let response = self
            .client
            .post(&mfa_url)
            .query(&signin_params)
            .headers(headers)
            .form(&form_data)
            .send()
            .await
            .map_err(GarminError::Http)?;

        let html = response.text().await.map_err(GarminError::Http)?;
        let title = extract_title(&html)?;

        if title == "Success" {
            extract_ticket(&html)
        } else {
            Err(GarminError::auth(format!("MFA verification failed: {}", title)))
        }
    }

    /// Exchange ticket for OAuth1 token
    async fn get_oauth1_token(&self, ticket: &str) -> Result<OAuth1Token> {
        // Fetch OAuth consumer credentials
        let consumer = self.fetch_oauth_consumer().await?;

        let base_url = format!(
            "https://connectapi.{}/oauth-service/oauth/",
            self.domain
        );
        let login_url = format!("https://sso.{}/sso/embed", self.domain);
        let url = format!(
            "{}preauthorized?ticket={}&login-url={}&accepts-mfa-tokens=true",
            base_url, ticket, login_url
        );

        // Create OAuth1 signer with just consumer credentials
        let signer = OAuth1Signer::new(OAuthConsumer {
            key: consumer.consumer_key.clone(),
            secret: consumer.consumer_secret.clone(),
        });

        let auth_header = signer.sign("GET", &url, &[]);

        // IMPORTANT: Use a NEW client without cookies for OAuth1 requests
        // This matches Python's behavior where OAuth1Session is separate from SSO session
        let oauth_client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(GarminError::Http)?;

        let response = oauth_client
            .get(&url)
            .header(USER_AGENT, MOBILE_USER_AGENT)
            .header("Authorization", auth_header)
            .send()
            .await
            .map_err(GarminError::Http)?;

        let status = response.status();

        if !status.is_success() {
            return Err(GarminError::auth(format!(
                "Failed to get OAuth1 token: {}",
                status
            )));
        }

        let body = response.text().await.map_err(GarminError::Http)?;
        let params = parse_oauth_response(&body);

        let oauth_token = params
            .get("oauth_token")
            .ok_or_else(|| GarminError::invalid_response("Missing oauth_token"))?
            .clone();
        let oauth_token_secret = params
            .get("oauth_token_secret")
            .ok_or_else(|| GarminError::invalid_response("Missing oauth_token_secret"))?
            .clone();
        let mfa_token = params.get("mfa_token").cloned();

        let mut token = OAuth1Token::new(oauth_token, oauth_token_secret)
            .with_domain(&self.domain);

        if let Some(mfa) = mfa_token {
            token = token.with_mfa(mfa, None);
        }

        Ok(token)
    }

    /// Exchange OAuth1 token for OAuth2 token
    async fn exchange_oauth1_for_oauth2(&self, oauth1: &OAuth1Token) -> Result<OAuth2Token> {
        let consumer = self.fetch_oauth_consumer().await?;

        let url = format!(
            "https://connectapi.{}/oauth-service/oauth/exchange/user/2.0",
            self.domain
        );

        // Create OAuth1 signer with token
        let signer = OAuth1Signer::new(OAuthConsumer {
            key: consumer.consumer_key.clone(),
            secret: consumer.consumer_secret.clone(),
        })
        .with_token(OAuthToken {
            token: oauth1.oauth_token.clone(),
            secret: oauth1.oauth_token_secret.clone(),
        });

        let params: Vec<(String, String)> = if let Some(ref mfa_token) = oauth1.mfa_token {
            vec![("mfa_token".to_string(), mfa_token.clone())]
        } else {
            vec![]
        };

        let auth_header = signer.sign("POST", &url, &params);

        // Use a separate client without cookies (matches Python's OAuth1Session behavior)
        let oauth_client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(GarminError::Http)?;

        let mut request = oauth_client
            .post(&url)
            .header(USER_AGENT, MOBILE_USER_AGENT)
            .header("Authorization", auth_header)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded");

        if let Some(ref mfa_token) = oauth1.mfa_token {
            request = request.form(&[("mfa_token", mfa_token)]);
        }

        let response = request.send().await.map_err(GarminError::Http)?;

        let status = response.status();

        if !status.is_success() {
            return Err(GarminError::auth(format!(
                "Failed to exchange OAuth1 for OAuth2: {}",
                status
            )));
        }

        let mut token: OAuth2Token = response.json().await
            .map_err(|e| GarminError::invalid_response(format!("Failed to parse OAuth2 token: {}", e)))?;

        // Set expiration timestamps
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        token.expires_at = now + token.expires_in;
        token.refresh_token_expires_at = now + token.refresh_token_expires_in;

        Ok(token)
    }

    /// Fetch OAuth consumer credentials from Garth's S3
    async fn fetch_oauth_consumer(&self) -> Result<OAuthConsumerResponse> {
        let response = self
            .client
            .get(OAUTH_CONSUMER_URL)
            .send()
            .await
            .map_err(GarminError::Http)?;

        response
            .json()
            .await
            .map_err(|e| GarminError::invalid_response(format!("Failed to parse OAuth consumer: {}", e)))
    }

    /// Refresh OAuth2 token using OAuth1 token
    pub async fn refresh_oauth2(&self, oauth1: &OAuth1Token) -> Result<OAuth2Token> {
        self.exchange_oauth1_for_oauth2(oauth1).await
    }
}

/// Result of login attempt
enum LoginResult {
    Success(String), // ticket
    MfaRequired,
}

/// Extract CSRF token from HTML
fn extract_csrf_token(html: &str) -> Result<String> {
    let re = Regex::new(r#"name="_csrf"\s+value="([^"]+)""#).unwrap();
    re.captures(html)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| GarminError::invalid_response("Could not find CSRF token"))
}

/// Extract page title from HTML
fn extract_title(html: &str) -> Result<String> {
    let re = Regex::new(r"<title>([^<]+)</title>").unwrap();
    re.captures(html)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| GarminError::invalid_response("Could not find page title"))
}

/// Extract ticket from success HTML
fn extract_ticket(html: &str) -> Result<String> {
    let re = Regex::new(r#"embed\?ticket=([^"]+)""#).unwrap();
    re.captures(html)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| GarminError::invalid_response("Could not find ticket in response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_csrf_token() {
        let html = r#"<input type="hidden" name="_csrf" value="abc123token">"#;
        let token = extract_csrf_token(html).unwrap();
        assert_eq!(token, "abc123token");
    }

    #[test]
    fn test_extract_csrf_token_missing() {
        let html = r#"<html><body>No token here</body></html>"#;
        let result = extract_csrf_token(html);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_title() {
        let html = r#"<html><head><title>Success</title></head></html>"#;
        let title = extract_title(html).unwrap();
        assert_eq!(title, "Success");
    }

    #[test]
    fn test_extract_title_mfa() {
        let html = r#"<html><head><title>GARMIN > MFA Challenge</title></head></html>"#;
        let title = extract_title(html).unwrap();
        assert!(title.contains("MFA"));
    }

    #[test]
    fn test_extract_ticket() {
        let html = r#"<a href="embed?ticket=ST-12345-abc">Continue</a>"#;
        let ticket = extract_ticket(html).unwrap();
        assert_eq!(ticket, "ST-12345-abc");
    }

    #[test]
    fn test_extract_ticket_missing() {
        let html = r#"<html><body>No ticket</body></html>"#;
        let result = extract_ticket(html);
        assert!(result.is_err());
    }

    #[test]
    fn test_sso_client_creation() {
        let client = SsoClient::new(None);
        assert!(client.is_ok());
    }

    #[test]
    fn test_sso_client_with_custom_domain() {
        let client = SsoClient::new(Some("garmin.cn")).unwrap();
        assert_eq!(client.domain, "garmin.cn");
    }
}
