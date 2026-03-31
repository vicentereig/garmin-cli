//! Garmin SSO Authentication
//!
//! Implements the Garmin Connect SSO login flow using the mobile JSON API.
//!
//! ## Background
//!
//! The previous implementation scraped `<title>` tags from Garmin's SSO HTML
//! page to determine login status (matching `garth < 0.8.0`). Garmin changed
//! their SSO page structure, breaking this approach.
//!
//! This implementation ports the approach from `garth 0.8.0`, which switched
//! to Garmin's mobile JSON API endpoints:
//! - `GET  /mobile/sso/en/sign-in` — establish session cookies
//! - `POST /mobile/api/login` — submit credentials, receive ticket or MFA challenge
//! - `POST /mobile/api/mfa/verifyCode` — verify MFA code if required
//!
//! Reference: <https://github.com/matin/garth/blob/main/garth/sso.py>

use crate::client::oauth1::{parse_oauth_response, OAuth1Signer, OAuthConsumer, OAuthToken};
use crate::client::tokens::{OAuth1Token, OAuth2Token};
use crate::error::{GarminError, Result};
use reqwest::cookie::Jar;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default Garmin domain
const DEFAULT_DOMAIN: &str = "garmin.com";

/// Client ID for mobile API login
const CLIENT_ID: &str = "GCM_ANDROID_DARK";

/// User agent mimicking the Garmin mobile app (for OAuth endpoints)
const MOBILE_USER_AGENT: &str = "com.garmin.android.apps.connectmobile";

/// Browser-like user agent for SSO pages (avoids Cloudflare challenges)
const SSO_USER_AGENT: &str = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";

/// URL to fetch OAuth consumer credentials
const OAUTH_CONSUMER_URL: &str = "https://thegarth.s3.amazonaws.com/oauth_consumer.json";

/// OAuth consumer credentials fetched from Garth's S3
#[derive(Debug, Deserialize)]
struct OAuthConsumerResponse {
    consumer_key: String,
    consumer_secret: String,
}

/// SSO login request body
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequest<'a> {
    username: &'a str,
    password: &'a str,
    remember_me: bool,
    captcha_token: &'a str,
}

/// SSO MFA verification request body
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MfaVerifyRequest<'a> {
    mfa_method: &'a str,
    mfa_verification_code: &'a str,
    remember_my_browser: bool,
    reconsent_list: Vec<String>,
    mfa_setup: bool,
}

/// SSO response status
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SsoResponseStatus {
    #[serde(rename = "type")]
    response_type: String,
    #[serde(default)]
    message: String,
}

/// SSO login/MFA response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SsoResponse {
    #[serde(default)]
    response_status: Option<SsoResponseStatus>,
    #[serde(default)]
    service_ticket_id: Option<String>,
    #[serde(default)]
    customer_mfa_info: Option<MfaInfo>,
}

/// MFA information from login response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MfaInfo {
    #[serde(default)]
    mfa_last_method_used: Option<String>,
}

/// SSO Client for Garmin authentication
pub struct SsoClient {
    client: Client,
    domain: String,
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
        })
    }

    /// Build SSO page headers (browser-like to avoid Cloudflare)
    fn sso_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(SSO_USER_AGENT));
        headers.insert(
            "Accept",
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            ),
        );
        headers.insert(
            "Accept-Language",
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        headers.insert("Sec-Fetch-Mode", HeaderValue::from_static("navigate"));
        headers.insert("Sec-Fetch-Dest", HeaderValue::from_static("document"));
        headers
    }

    /// Perform full login flow using mobile JSON API
    pub async fn login(
        &mut self,
        email: &str,
        password: &str,
        mfa_callback: Option<impl FnOnce() -> String>,
    ) -> Result<(OAuth1Token, OAuth2Token)> {
        let service_url = format!(
            "https://mobile.integration.{}/gcm/android",
            self.domain
        );
        let login_params = [
            ("clientId", CLIENT_ID),
            ("locale", "en-US"),
            ("service", service_url.as_str()),
        ];

        // Step 1: Set cookies by visiting the sign-in page
        let sign_in_url = format!("https://sso.{}/mobile/sso/en/sign-in", self.domain);
        let mut headers = Self::sso_headers();
        headers.insert("Sec-Fetch-Site", HeaderValue::from_static("none"));

        let _ = self
            .client
            .get(&sign_in_url)
            .query(&[("clientId", CLIENT_ID)])
            .headers(headers)
            .send()
            .await
            .map_err(GarminError::Http)?
            .text()
            .await;

        // Step 2: Submit login via JSON API
        let login_url = format!("https://sso.{}/mobile/api/login", self.domain);
        let login_body = LoginRequest {
            username: email,
            password,
            remember_me: false,
            captcha_token: "",
        };

        let response = self
            .client
            .post(&login_url)
            .query(&login_params)
            .headers(Self::sso_headers())
            .json(&login_body)
            .send()
            .await
            .map_err(GarminError::Http)?;

        let status_code = response.status();
        if status_code.as_u16() == 429 {
            return Err(GarminError::auth(
                "Rate limited by Garmin (429). Too many login attempts. Wait 15-30 minutes and try again.".to_string()
            ));
        }
        if !status_code.is_success() && status_code.as_u16() != 200 {
            let body = response.text().await.unwrap_or_default();
            return Err(GarminError::auth(format!(
                "SSO HTTP {}: {}", status_code, &body[..body.len().min(200)]
            )));
        }

        let body_text = response.text().await.map_err(GarminError::Http)?;

        let sso_resp: SsoResponse = serde_json::from_str(&body_text).map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse SSO response: {} | body: {}", e, &body_text[..body_text.len().min(200)]))
        })?;

        let resp_type = sso_resp
            .response_status
            .as_ref()
            .map(|s| s.response_type.as_str())
            .unwrap_or("UNKNOWN");

        let ticket = match resp_type {
            "SUCCESSFUL" => sso_resp
                .service_ticket_id
                .ok_or_else(|| GarminError::invalid_response("Missing serviceTicketId"))?,

            "MFA_REQUIRED" => {
                let mfa_method = sso_resp
                    .customer_mfa_info
                    .and_then(|info| info.mfa_last_method_used)
                    .unwrap_or_else(|| "email".to_string());

                let mfa_code = mfa_callback
                    .ok_or_else(|| GarminError::MfaRequired)?();

                self.submit_mfa(&mfa_code, &mfa_method, &login_params)
                    .await?
            }

            _ => {
                let message = sso_resp
                    .response_status
                    .map(|s| {
                        if s.message.is_empty() {
                            s.response_type
                        } else {
                            format!("{}: {}", s.response_type, s.message)
                        }
                    })
                    .unwrap_or_else(|| "Unknown error".to_string());
                return Err(GarminError::auth(format!("SSO error: {}", message)));
            }
        };

        // Step 3: Complete login (set Cloudflare LB cookie, get OAuth tokens)
        self.complete_login(&ticket).await
    }

    /// Submit MFA verification code via JSON API
    async fn submit_mfa(
        &self,
        mfa_code: &str,
        mfa_method: &str,
        login_params: &[(&str, &str)],
    ) -> Result<String> {
        let mfa_url = format!("https://sso.{}/mobile/api/mfa/verifyCode", self.domain);
        let mfa_body = MfaVerifyRequest {
            mfa_method,
            mfa_verification_code: mfa_code,
            remember_my_browser: false,
            reconsent_list: vec![],
            mfa_setup: false,
        };

        let response = self
            .client
            .post(&mfa_url)
            .query(login_params)
            .headers(Self::sso_headers())
            .json(&mfa_body)
            .send()
            .await
            .map_err(GarminError::Http)?;

        let sso_resp: SsoResponse = response.json().await.map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse MFA response: {}", e))
        })?;

        let resp_type = sso_resp
            .response_status
            .as_ref()
            .map(|s| s.response_type.as_str())
            .unwrap_or("UNKNOWN");

        if resp_type != "SUCCESSFUL" {
            let message = sso_resp
                .response_status
                .map(|s| s.message)
                .unwrap_or_default();
            return Err(GarminError::auth(format!(
                "MFA verification failed: {}",
                message
            )));
        }

        sso_resp
            .service_ticket_id
            .ok_or_else(|| GarminError::invalid_response("Missing serviceTicketId after MFA"))
    }

    /// Complete login: set Cloudflare LB cookie and exchange for OAuth tokens
    async fn complete_login(&self, ticket: &str) -> Result<(OAuth1Token, OAuth2Token)> {
        // Best-effort: set Cloudflare LB cookie for backend pinning
        let portal_url = format!("https://sso.{}/portal/sso/embed", self.domain);
        let mut headers = Self::sso_headers();
        headers.insert("Sec-Fetch-Site", HeaderValue::from_static("same-origin"));
        let _ = self.client.get(&portal_url).headers(headers).send().await;

        // Exchange ticket for OAuth1 token
        let oauth1 = self.get_oauth1_token(ticket).await?;

        // Exchange OAuth1 for OAuth2 token
        let oauth2 = self.exchange_oauth1_for_oauth2(&oauth1, true).await?;

        Ok((oauth1, oauth2))
    }

    /// Exchange ticket for OAuth1 token
    async fn get_oauth1_token(&self, ticket: &str) -> Result<OAuth1Token> {
        let consumer = self.fetch_oauth_consumer().await?;

        let base_url = format!(
            "https://connectapi.{}/oauth-service/oauth/",
            self.domain
        );
        let login_url = format!(
            "https://mobile.integration.{}/gcm/android",
            self.domain
        );
        let url = format!(
            "{}preauthorized?ticket={}&login-url={}&accepts-mfa-tokens=true",
            base_url, ticket, login_url
        );

        let signer = OAuth1Signer::new(OAuthConsumer {
            key: consumer.consumer_key.clone(),
            secret: consumer.consumer_secret.clone(),
        });

        let auth_header = signer.sign("GET", &url, &[]);

        // Use a separate client without cookies (matches garth's OAuth1Session behavior)
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

        let mut token =
            OAuth1Token::new(oauth_token, oauth_token_secret).with_domain(&self.domain);

        if let Some(mfa) = mfa_token {
            token = token.with_mfa(mfa, None);
        }

        Ok(token)
    }

    /// Exchange OAuth1 token for OAuth2 token
    async fn exchange_oauth1_for_oauth2(
        &self,
        oauth1: &OAuth1Token,
        login: bool,
    ) -> Result<OAuth2Token> {
        let consumer = self.fetch_oauth_consumer().await?;

        let url = format!(
            "https://connectapi.{}/oauth-service/oauth/exchange/user/2.0",
            self.domain
        );

        let signer = OAuth1Signer::new(OAuthConsumer {
            key: consumer.consumer_key.clone(),
            secret: consumer.consumer_secret.clone(),
        })
        .with_token(OAuthToken {
            token: oauth1.oauth_token.clone(),
            secret: oauth1.oauth_token_secret.clone(),
        });

        let mut form_params: Vec<(String, String)> = vec![];
        if login {
            form_params.push((
                "audience".to_string(),
                "GARMIN_CONNECT_MOBILE_ANDROID_DI".to_string(),
            ));
        }
        if let Some(ref mfa_token) = oauth1.mfa_token {
            form_params.push(("mfa_token".to_string(), mfa_token.clone()));
        }

        let auth_header = signer.sign("POST", &url, &form_params);

        let oauth_client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(GarminError::Http)?;

        let mut request = oauth_client
            .post(&url)
            .header(USER_AGENT, MOBILE_USER_AGENT)
            .header("Authorization", auth_header)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded");

        if !form_params.is_empty() {
            request = request.form(&form_params);
        }

        let response = request.send().await.map_err(GarminError::Http)?;

        let status = response.status();
        if !status.is_success() {
            return Err(GarminError::auth(format!(
                "Failed to exchange OAuth1 for OAuth2: {}",
                status
            )));
        }

        let mut token: OAuth2Token = response.json().await.map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse OAuth2 token: {}", e))
        })?;

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

        response.json().await.map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse OAuth consumer: {}", e))
        })
    }

    /// Refresh OAuth2 token using OAuth1 token
    pub async fn refresh_oauth2(&self, oauth1: &OAuth1Token) -> Result<OAuth2Token> {
        self.exchange_oauth1_for_oauth2(oauth1, false).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_parse_successful_sso_response() {
        let json = r#"{
            "responseStatus": {"type": "SUCCESSFUL", "message": ""},
            "serviceTicketId": "ST-12345-abc"
        }"#;
        let resp: SsoResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            resp.response_status.unwrap().response_type,
            "SUCCESSFUL"
        );
        assert_eq!(resp.service_ticket_id.unwrap(), "ST-12345-abc");
    }

    #[test]
    fn test_parse_mfa_required_response() {
        let json = r#"{
            "responseStatus": {"type": "MFA_REQUIRED", "message": ""},
            "customerMfaInfo": {"mfaLastMethodUsed": "email"}
        }"#;
        let resp: SsoResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            resp.response_status.unwrap().response_type,
            "MFA_REQUIRED"
        );
        assert_eq!(
            resp.customer_mfa_info.unwrap().mfa_last_method_used.unwrap(),
            "email"
        );
    }

    #[test]
    fn test_parse_failed_sso_response() {
        let json = r#"{
            "responseStatus": {"type": "FAIL", "message": "Invalid credentials"}
        }"#;
        let resp: SsoResponse = serde_json::from_str(json).unwrap();
        let status = resp.response_status.unwrap();
        assert_eq!(status.response_type, "FAIL");
        assert_eq!(status.message, "Invalid credentials");
    }

    #[test]
    fn test_parse_missing_response_status() {
        // Handles unexpected/empty JSON gracefully
        let json = r#"{}"#;
        let resp: SsoResponse = serde_json::from_str(json).unwrap();
        assert!(resp.response_status.is_none());
        assert!(resp.service_ticket_id.is_none());
    }
}
