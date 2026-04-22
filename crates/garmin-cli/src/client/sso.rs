//! Garmin SSO Authentication
//!
//! Garmin's login behavior changed in March 2026. Browser logins may still work
//! while programmatic mobile auth endpoints reject non-browser clients. This
//! implementation now tries multiple login strategies and prefers native DI OAuth
//! bearer tokens when a service ticket is obtained.

use crate::client::oauth1::{parse_oauth_response, OAuth1Signer, OAuthConsumer, OAuthToken};
use crate::client::tokens::{OAuth1Token, OAuth2Token};
use crate::error::{GarminError, Result};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine as _,
};
use rand::Rng;
use regex::Regex;
use reqwest::cookie::Jar;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Default Garmin domain.
const DEFAULT_DOMAIN: &str = "garmin.com";

/// Mobile iOS auth flow constants.
const IOS_CLIENT_ID: &str = "GCM_IOS_DARK";
const IOS_SERVICE_URL: &str = "https://mobile.integration.garmin.com/gcm/ios";
const IOS_LOGIN_USER_AGENT: &str = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";

/// Portal/browser flow constants.
const PORTAL_CLIENT_ID: &str = "GarminConnect";
const PORTAL_SERVICE_URL: &str = "https://connect.garmin.com/app";
const PORTAL_LOGIN_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/// Native DI token exchange constants.
const DI_TOKEN_URL: &str = "https://diauth.garmin.com/di-oauth2-service/oauth/token";
const DI_GRANT_TYPE: &str =
    "https://connectapi.garmin.com/di-oauth2-service/oauth/grant/service_ticket";
const DI_API_USER_AGENT: &str = "GCM-Android-5.23";
const DI_X_GARMIN_USER_AGENT: &str =
    "com.garmin.android.apps.connectmobile/5.23; ; Google/sdk_gphone64_arm64/google; Android/33; Dalvik/2.1.0";
const DI_CLIENT_IDS: &[&str] = &[
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2025Q2",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2024Q4",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI",
    "GARMIN_CONNECT_MOBILE_IOS_DI",
];

/// Legacy OAuth1 exchange fallback.
const LEGACY_MOBILE_USER_AGENT: &str = "com.garmin.android.apps.connectmobile";
const OAUTH_CONSUMER_URL: &str = "https://thegarth.s3.amazonaws.com/oauth_consumer.json";

/// Anti-WAF delays for browser-like flows.
const PORTAL_DELAY_MIN_S: f64 = 10.0;
const PORTAL_DELAY_MAX_S: f64 = 20.0;
const WIDGET_DELAY_MIN_S: f64 = 3.0;
const WIDGET_DELAY_MAX_S: f64 = 8.0;

#[derive(Debug, Deserialize)]
struct OAuthConsumerResponse {
    consumer_key: String,
    consumer_secret: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequest<'a> {
    username: &'a str,
    password: &'a str,
    remember_me: bool,
    captcha_token: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MfaVerifyRequest<'a> {
    mfa_method: &'a str,
    mfa_verification_code: &'a str,
    remember_my_browser: bool,
    reconsent_list: Vec<String>,
    mfa_setup: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SsoResponseStatus {
    #[serde(rename = "type")]
    response_type: String,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SsoResponse {
    #[serde(default)]
    response_status: Option<SsoResponseStatus>,
    #[serde(default)]
    service_ticket_id: Option<String>,
    #[serde(default)]
    customer_mfa_info: Option<MfaInfo>,
    #[serde(default)]
    error: Option<SsoErrorResponse>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MfaInfo {
    #[serde(default)]
    mfa_last_method_used: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SsoErrorResponse {
    #[serde(rename = "status-code")]
    status_code: String,
    #[serde(default)]
    message: String,
    #[serde(rename = "request-id", default)]
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    jti: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    refresh_token_expires_in: Option<i64>,
}

#[derive(Clone, Copy)]
enum JsonLoginFlow {
    Mobile,
    Portal,
}

struct LoginStrategySuccess {
    ticket: String,
    service_url: String,
}

/// SSO Client for Garmin authentication.
pub struct SsoClient {
    client: Client,
}

impl SsoClient {
    /// Create a new SSO client.
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(GarminError::Http)?;

        Ok(Self { client })
    }

    fn new_session_client() -> Result<Client> {
        let cookie_jar = Arc::new(Jar::default());
        Client::builder()
            .cookie_provider(cookie_jar)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(GarminError::Http)
    }

    fn json_login_params(flow: JsonLoginFlow) -> [(&'static str, &'static str); 3] {
        match flow {
            JsonLoginFlow::Mobile => [
                ("clientId", IOS_CLIENT_ID),
                ("locale", "en-US"),
                ("service", IOS_SERVICE_URL),
            ],
            JsonLoginFlow::Portal => [
                ("clientId", PORTAL_CLIENT_ID),
                ("locale", "en-US"),
                ("service", PORTAL_SERVICE_URL),
            ],
        }
    }

    fn flow_service_url(flow: JsonLoginFlow) -> &'static str {
        match flow {
            JsonLoginFlow::Mobile => IOS_SERVICE_URL,
            JsonLoginFlow::Portal => PORTAL_SERVICE_URL,
        }
    }

    fn flow_path(flow: JsonLoginFlow) -> &'static str {
        match flow {
            JsonLoginFlow::Mobile => "mobile",
            JsonLoginFlow::Portal => "portal",
        }
    }

    fn mobile_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(IOS_LOGIN_USER_AGENT));
        headers.insert(
            "Accept",
            HeaderValue::from_static("application/json, text/plain, */*"),
        );
        headers.insert(
            "Accept-Language",
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert("Origin", HeaderValue::from_static("https://sso.garmin.com"));
        headers
    }

    fn portal_page_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(PORTAL_LOGIN_USER_AGENT));
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
        headers
    }

    fn portal_api_headers(referer: &str) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(PORTAL_LOGIN_USER_AGENT));
        headers.insert(
            "Accept",
            HeaderValue::from_static("application/json, text/plain, */*"),
        );
        headers.insert(
            "Accept-Language",
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert("Origin", HeaderValue::from_static("https://sso.garmin.com"));
        headers.insert(
            "Referer",
            HeaderValue::from_str(referer)
                .map_err(|e| GarminError::invalid_response(format!("Invalid referer: {}", e)))?,
        );
        Ok(headers)
    }

    fn native_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(DI_API_USER_AGENT));
        headers.insert(
            "X-Garmin-User-Agent",
            HeaderValue::from_static(DI_X_GARMIN_USER_AGENT),
        );
        headers.insert(
            "X-Garmin-Paired-App-Version",
            HeaderValue::from_static("10861"),
        );
        headers.insert(
            "X-Garmin-Client-Platform",
            HeaderValue::from_static("Android"),
        );
        headers.insert("X-App-Ver", HeaderValue::from_static("10861"));
        headers.insert("X-Lang", HeaderValue::from_static("en"));
        headers.insert("X-GCExperience", HeaderValue::from_static("GC5"));
        headers.insert(
            "Accept-Language",
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        headers
    }

    /// Perform login using multiple SSO flows.
    pub async fn login<F>(
        &mut self,
        email: &str,
        password: &str,
        mut mfa_callback: Option<F>,
    ) -> Result<(OAuth1Token, OAuth2Token)>
    where
        F: FnMut() -> String,
    {
        let mut failures = Vec::new();

        match self
            .login_mobile_ios(email, password, mfa_callback.as_mut())
            .await
        {
            Ok(success) => return self.complete_login(&success.ticket, &success.service_url).await,
            Err(err @ GarminError::Authentication(_)) | Err(err @ GarminError::MfaRequired) => {
                return Err(err);
            }
            Err(err) => failures.push(format!("mobile: {}", Self::summarize_strategy_error(&err))),
        }

        match self.login_widget(email, password, mfa_callback.as_mut()).await {
            Ok(success) => return self.complete_login(&success.ticket, &success.service_url).await,
            Err(err @ GarminError::Authentication(_)) | Err(err @ GarminError::MfaRequired) => {
                return Err(err);
            }
            Err(err) => failures.push(format!("widget: {}", Self::summarize_strategy_error(&err))),
        }

        match self
            .login_portal(email, password, mfa_callback.as_mut())
            .await
        {
            Ok(success) => return self.complete_login(&success.ticket, &success.service_url).await,
            Err(err @ GarminError::Authentication(_)) | Err(err @ GarminError::MfaRequired) => {
                return Err(err);
            }
            Err(err) => failures.push(format!("portal: {}", Self::summarize_strategy_error(&err))),
        }

        Err(GarminError::auth(format!(
            "Garmin rejected every non-browser login strategy. Browser login can still work while the CLI fails because Garmin/Cloudflare now fingerprints the client, not just your credentials. Strategy results: {}",
            failures.join("; ")
        )))
    }

    async fn login_mobile_ios<F>(
        &self,
        email: &str,
        password: &str,
        mfa_callback: Option<&mut F>,
    ) -> Result<LoginStrategySuccess>
    where
        F: FnMut() -> String,
    {
        let session = Self::new_session_client()?;
        let login_body = LoginRequest {
            username: email,
            password,
            remember_me: true,
            captcha_token: "",
        };
        let headers = Self::mobile_headers();

        let response = session
            .post(format!("https://sso.{}/mobile/api/login", DEFAULT_DOMAIN))
            .query(&Self::json_login_params(JsonLoginFlow::Mobile))
            .headers(headers.clone())
            .json(&login_body)
            .send()
            .await
            .map_err(GarminError::Http)?;

        self.handle_json_login_response(
            &session,
            response,
            JsonLoginFlow::Mobile,
            headers,
            mfa_callback,
        )
        .await
    }

    async fn login_widget<F>(
        &self,
        email: &str,
        password: &str,
        mfa_callback: Option<&mut F>,
    ) -> Result<LoginStrategySuccess>
    where
        F: FnMut() -> String,
    {
        let session = Self::new_session_client()?;
        let sso_base = format!("https://sso.{}/sso", DEFAULT_DOMAIN);
        let embed_url = format!("{}/embed", sso_base);
        let embed_params = [
            ("id", "gauth-widget"),
            ("embedWidget", "true"),
            ("gauthHost", sso_base.as_str()),
        ];

        let response = session
            .get(&embed_url)
            .query(&embed_params)
            .headers(Self::portal_page_headers())
            .send()
            .await
            .map_err(GarminError::Http)?;

        if Self::is_rate_limited_response(&response, "") {
            return Err(GarminError::RateLimited);
        }
        if !response.status().is_success() {
            return Err(GarminError::Other(format!(
                "widget embed returned {}",
                response.status()
            )));
        }

        let signin_params = [
            ("id", "gauth-widget"),
            ("embedWidget", "true"),
            ("gauthHost", embed_url.as_str()),
            ("service", embed_url.as_str()),
            ("source", embed_url.as_str()),
            ("redirectAfterAccountLoginUrl", embed_url.as_str()),
            ("redirectAfterAccountCreationUrl", embed_url.as_str()),
        ];

        let signin_response = session
            .get(format!("https://sso.{}/sso/signin", DEFAULT_DOMAIN))
            .query(&signin_params)
            .header("Referer", &embed_url)
            .headers(Self::portal_page_headers())
            .send()
            .await
            .map_err(GarminError::Http)?;

        let signin_status = signin_response.status();
        let signin_body = signin_response.text().await.unwrap_or_default();
        if signin_status == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(&signin_body) {
            return Err(GarminError::RateLimited);
        }
        if !signin_status.is_success() {
            return Err(GarminError::Other(format!(
                "widget signin returned {}",
                signin_status
            )));
        }

        let csrf = Self::extract_csrf(&signin_body)
            .ok_or_else(|| GarminError::Other("widget login missing CSRF token".to_string()))?;

        Self::delay_between(WIDGET_DELAY_MIN_S, WIDGET_DELAY_MAX_S).await;

        let post_response = session
            .post(format!("https://sso.{}/sso/signin", DEFAULT_DOMAIN))
            .query(&signin_params)
            .header("Referer", &embed_url)
            .form(&[
                ("username", email),
                ("password", password),
                ("embed", "true"),
                ("_csrf", csrf.as_str()),
            ])
            .send()
            .await
            .map_err(GarminError::Http)?;

        let post_url = post_response.url().to_string();
        let post_status = post_response.status();
        let post_body = post_response.text().await.unwrap_or_default();

        if post_status == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(&post_body) {
            return Err(GarminError::RateLimited);
        }

        let title = Self::extract_title(&post_body).unwrap_or_default();
        let title_lower = title.to_lowercase();
        if ["bad gateway", "service unavailable", "cloudflare", "502", "503"]
            .iter()
            .any(|hint| title_lower.contains(hint))
        {
            return Err(GarminError::Other(format!(
                "widget login blocked by Garmin/Cloudflare ({})",
                title
            )));
        }
        if ["locked", "invalid", "incorrect", "account error"]
            .iter()
            .any(|hint| title_lower.contains(hint))
        {
            return Err(GarminError::auth(format!(
                "Widget authentication failed: {}",
                title
            )));
        }
        if title.contains("MFA") {
            let callback = mfa_callback.ok_or(GarminError::MfaRequired)?;
            let mfa_code = callback();
            return self
                .submit_widget_mfa(
                    &session,
                    &mfa_code,
                    &signin_params,
                    &post_url,
                    &post_body,
                    &embed_url,
                )
                .await;
        }
        if title != "Success" {
            return Err(GarminError::Other(format!(
                "widget login unexpected title '{}'",
                title
            )));
        }

        let ticket = Self::extract_widget_ticket(&post_body).ok_or_else(|| {
            GarminError::Other("widget login missing service ticket".to_string())
        })?;

        Ok(LoginStrategySuccess {
            ticket,
            service_url: embed_url,
        })
    }

    async fn login_portal<F>(
        &self,
        email: &str,
        password: &str,
        mfa_callback: Option<&mut F>,
    ) -> Result<LoginStrategySuccess>
    where
        F: FnMut() -> String,
    {
        let session = Self::new_session_client()?;
        let signin_url = format!("https://sso.{}/portal/sso/en-US/sign-in", DEFAULT_DOMAIN);
        let get_response = session
            .get(&signin_url)
            .query(&[
                ("clientId", PORTAL_CLIENT_ID),
                ("service", PORTAL_SERVICE_URL),
            ])
            .headers(Self::portal_page_headers())
            .send()
            .await
            .map_err(GarminError::Http)?;

        if Self::is_rate_limited_response(&get_response, "") {
            return Err(GarminError::RateLimited);
        }
        if !get_response.status().is_success() {
            return Err(GarminError::Other(format!(
                "portal signin returned {}",
                get_response.status()
            )));
        }

        Self::delay_between(PORTAL_DELAY_MIN_S, PORTAL_DELAY_MAX_S).await;

        let referer = format!(
            "{}?clientId={}&service={}",
            signin_url, PORTAL_CLIENT_ID, PORTAL_SERVICE_URL
        );
        let headers = Self::portal_api_headers(&referer)?;
        let login_body = LoginRequest {
            username: email,
            password,
            remember_me: true,
            captcha_token: "",
        };

        let response = session
            .post(format!("https://sso.{}/portal/api/login", DEFAULT_DOMAIN))
            .query(&Self::json_login_params(JsonLoginFlow::Portal))
            .headers(headers.clone())
            .json(&login_body)
            .send()
            .await
            .map_err(GarminError::Http)?;

        self.handle_json_login_response(
            &session,
            response,
            JsonLoginFlow::Portal,
            headers,
            mfa_callback,
        )
        .await
    }

    async fn handle_json_login_response<F>(
        &self,
        session: &Client,
        response: Response,
        flow: JsonLoginFlow,
        headers: HeaderMap,
        mfa_callback: Option<&mut F>,
    ) -> Result<LoginStrategySuccess>
    where
        F: FnMut() -> String,
    {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if status == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(&body) {
            return Err(GarminError::RateLimited);
        }

        if !status.is_success() {
            return Err(GarminError::Other(format!(
                "{} login HTTP {}",
                Self::flow_path(flow),
                status
            )));
        }

        let sso_resp: SsoResponse = serde_json::from_str(&body).map_err(|e| {
            GarminError::Other(format!(
                "{} login returned non-JSON or unexpected JSON: {}",
                Self::flow_path(flow),
                e
            ))
        })?;

        let resp_type = sso_resp
            .response_status
            .as_ref()
            .map(|status| status.response_type.as_str())
            .unwrap_or("UNKNOWN");

        match resp_type {
            "SUCCESSFUL" => Ok(LoginStrategySuccess {
                ticket: sso_resp
                    .service_ticket_id
                    .ok_or_else(|| GarminError::invalid_response("Missing serviceTicketId"))?,
                service_url: Self::flow_service_url(flow).to_string(),
            }),
            "MFA_REQUIRED" => {
                let callback = mfa_callback.ok_or(GarminError::MfaRequired)?;
                let mfa_method = sso_resp
                    .customer_mfa_info
                    .and_then(|info| info.mfa_last_method_used)
                    .unwrap_or_else(|| "email".to_string());
                let mfa_code = callback();
                self.submit_json_mfa(session, flow, &mfa_code, &mfa_method, headers)
                    .await
            }
            "INVALID_USERNAME_PASSWORD" => {
                Err(GarminError::auth("Invalid username or password"))
            }
            _ => Err(GarminError::Other(format!(
                "{} login failed: {}",
                Self::flow_path(flow),
                Self::describe_sso_response(&sso_resp, &body)
            ))),
        }
    }

    async fn submit_json_mfa(
        &self,
        session: &Client,
        flow: JsonLoginFlow,
        mfa_code: &str,
        mfa_method: &str,
        headers: HeaderMap,
    ) -> Result<LoginStrategySuccess> {
        let body = MfaVerifyRequest {
            mfa_method,
            mfa_verification_code: mfa_code,
            remember_my_browser: true,
            reconsent_list: vec![],
            mfa_setup: false,
        };

        let primary = flow;
        let secondary = match flow {
            JsonLoginFlow::Mobile => JsonLoginFlow::Portal,
            JsonLoginFlow::Portal => JsonLoginFlow::Mobile,
        };

        for candidate in [primary, secondary] {
            let response = session
                .post(format!(
                    "https://sso.{}/{}/api/mfa/verifyCode",
                    DEFAULT_DOMAIN,
                    Self::flow_path(candidate)
                ))
                .query(&Self::json_login_params(candidate))
                .headers(headers.clone())
                .json(&body)
                .send()
                .await
                .map_err(GarminError::Http)?;

            let status = response.status();
            let response_body = response.text().await.unwrap_or_default();
            if status == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(&response_body) {
                continue;
            }
            if !status.is_success() {
                continue;
            }

            let sso_resp: SsoResponse = match serde_json::from_str(&response_body) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            let resp_type = sso_resp
                .response_status
                .as_ref()
                .map(|status| status.response_type.as_str())
                .unwrap_or("UNKNOWN");

            if resp_type == "SUCCESSFUL" {
                return Ok(LoginStrategySuccess {
                    ticket: sso_resp.service_ticket_id.ok_or_else(|| {
                        GarminError::invalid_response("Missing serviceTicketId after MFA")
                    })?,
                    service_url: Self::flow_service_url(flow).to_string(),
                });
            }
        }

        Err(GarminError::auth(
            "MFA verification was blocked by Garmin/Cloudflare. Browser login can still work while non-browser auth fails.",
        ))
    }

    async fn submit_widget_mfa(
        &self,
        session: &Client,
        mfa_code: &str,
        signin_params: &[(&str, &str)],
        referer: &str,
        widget_body: &str,
        embed_url: &str,
    ) -> Result<LoginStrategySuccess> {
        let csrf = Self::extract_csrf(widget_body)
            .ok_or_else(|| GarminError::auth("Widget MFA missing CSRF token"))?;

        let response = session
            .post(format!(
                "https://sso.{}/sso/verifyMFA/loginEnterMfaCode",
                DEFAULT_DOMAIN
            ))
            .query(signin_params)
            .header("Referer", referer)
            .form(&[
                ("mfa-code", mfa_code),
                ("embed", "true"),
                ("_csrf", csrf.as_str()),
                ("fromPage", "setupEnterMfaCode"),
            ])
            .send()
            .await
            .map_err(GarminError::Http)?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(&body) {
            return Err(GarminError::auth(
                "Widget MFA verification was blocked by Garmin/Cloudflare.",
            ));
        }

        let title = Self::extract_title(&body).unwrap_or_default();
        if title != "Success" {
            return Err(GarminError::auth(format!("Widget MFA failed: {}", title)));
        }

        let ticket = Self::extract_widget_ticket(&body)
            .ok_or_else(|| GarminError::auth("Widget MFA missing service ticket"))?;

        Ok(LoginStrategySuccess {
            ticket,
            service_url: embed_url.to_string(),
        })
    }

    async fn complete_login(
        &self,
        ticket: &str,
        service_url: &str,
    ) -> Result<(OAuth1Token, OAuth2Token)> {
        match self.exchange_service_ticket_for_di(ticket, service_url).await {
            Ok(oauth2) => Ok((OAuth1Token::new(String::new(), String::new()), oauth2)),
            Err(_) => self.complete_login_legacy(ticket).await,
        }
    }

    async fn exchange_service_ticket_for_di(
        &self,
        ticket: &str,
        service_url: &str,
    ) -> Result<OAuth2Token> {
        let mut saw_rate_limit = false;
        let mut last_error = String::new();

        for client_id in DI_CLIENT_IDS {
            let auth_header = format!("Basic {}", STANDARD.encode(format!("{}:", client_id)));
            let response = self
                .client
                .post(DI_TOKEN_URL)
                .headers(Self::native_headers())
                .header("Authorization", auth_header)
                .header("Accept", "application/json,text/html;q=0.9,*/*;q=0.8")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header("Cache-Control", "no-cache")
                .form(&[
                    ("client_id", *client_id),
                    ("service_ticket", ticket),
                    ("grant_type", DI_GRANT_TYPE),
                    ("service_url", service_url),
                ])
                .send()
                .await
                .map_err(GarminError::Http)?;

            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            if status == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(&body) {
                saw_rate_limit = true;
                continue;
            }
            if !status.is_success() {
                last_error = format!("{} returned HTTP {}", client_id, status);
                continue;
            }

            let token_response: DiTokenResponse = match serde_json::from_str(&body) {
                Ok(parsed) => parsed,
                Err(err) => {
                    last_error = format!("{} returned invalid JSON: {}", client_id, err);
                    continue;
                }
            };

            let now = Self::now_unix();
            let expires_in = token_response.expires_in.unwrap_or(3600);
            let refresh_token_expires_in = token_response.refresh_token_expires_in.unwrap_or(0);
            let parsed_client_id = Self::extract_client_id_from_jwt(&token_response.access_token);

            return Ok(OAuth2Token {
                scope: token_response.scope.unwrap_or_default(),
                jti: token_response.jti.unwrap_or_default(),
                token_type: token_response
                    .token_type
                    .unwrap_or_else(|| "Bearer".to_string()),
                access_token: token_response.access_token,
                refresh_token: token_response.refresh_token,
                expires_in,
                expires_at: now + expires_in,
                refresh_token_expires_in,
                refresh_token_expires_at: if refresh_token_expires_in > 0 {
                    now + refresh_token_expires_in
                } else {
                    0
                },
                client_id: Some(parsed_client_id.unwrap_or_else(|| (*client_id).to_string())),
            });
        }

        if saw_rate_limit {
            return Err(GarminError::RateLimited);
        }

        Err(GarminError::Other(format!(
            "DI token exchange failed: {}",
            last_error
        )))
    }

    async fn refresh_di_oauth2(&self, oauth2: &OAuth2Token) -> Result<OAuth2Token> {
        let client_id = oauth2.client_id.as_deref().ok_or_else(|| {
            GarminError::auth("Stored session is missing DI client metadata. Log in again.")
        })?;
        if oauth2.refresh_token.is_empty() {
            return Err(GarminError::auth(
                "Stored session is missing a refresh token. Log in again.",
            ));
        }

        let auth_header = format!("Basic {}", STANDARD.encode(format!("{}:", client_id)));
        let response = self
            .client
            .post(DI_TOKEN_URL)
            .headers(Self::native_headers())
            .header("Authorization", auth_header)
            .header("Accept", "application/json")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cache-Control", "no-cache")
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", client_id),
                ("refresh_token", oauth2.refresh_token.as_str()),
            ])
            .send()
            .await
            .map_err(GarminError::Http)?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(&body) {
            return Err(GarminError::RateLimited);
        }
        if !status.is_success() {
            return Err(GarminError::auth(format!(
                "Failed to refresh DI token: HTTP {}",
                status
            )));
        }

        let token_response: DiTokenResponse = serde_json::from_str(&body).map_err(|e| {
            GarminError::invalid_response(format!("Failed to parse refreshed DI token: {}", e))
        })?;
        let now = Self::now_unix();
        let expires_in = token_response.expires_in.unwrap_or(3600);
        let refresh_token_expires_in = token_response.refresh_token_expires_in.unwrap_or(0);
        let access_token = token_response.access_token;
        let parsed_client_id = Self::extract_client_id_from_jwt(&access_token);

        Ok(OAuth2Token {
            scope: token_response.scope.unwrap_or_default(),
            jti: token_response.jti.unwrap_or_default(),
            token_type: token_response
                .token_type
                .unwrap_or_else(|| "Bearer".to_string()),
            access_token,
            refresh_token: if token_response.refresh_token.is_empty() {
                oauth2.refresh_token.clone()
            } else {
                token_response.refresh_token
            },
            expires_in,
            expires_at: now + expires_in,
            refresh_token_expires_in,
            refresh_token_expires_at: if refresh_token_expires_in > 0 {
                now + refresh_token_expires_in
            } else {
                oauth2.refresh_token_expires_at
            },
            client_id: Some(parsed_client_id.unwrap_or_else(|| client_id.to_string())),
        })
    }

    async fn complete_login_legacy(&self, ticket: &str) -> Result<(OAuth1Token, OAuth2Token)> {
        let oauth1 = self.get_oauth1_token(ticket).await?;
        let oauth2 = self.exchange_oauth1_for_oauth2_legacy(&oauth1, true).await?;
        Ok((oauth1, oauth2))
    }

    async fn get_oauth1_token(&self, ticket: &str) -> Result<OAuth1Token> {
        let consumer = self.fetch_oauth_consumer().await?;

        let base_url = format!("https://connectapi.{}/oauth-service/oauth/", DEFAULT_DOMAIN);
        let login_url = format!("https://mobile.integration.{}/gcm/android", DEFAULT_DOMAIN);
        let url = format!(
            "{}preauthorized?ticket={}&login-url={}&accepts-mfa-tokens=true",
            base_url, ticket, login_url
        );

        let signer = OAuth1Signer::new(OAuthConsumer {
            key: consumer.consumer_key.clone(),
            secret: consumer.consumer_secret.clone(),
        });

        let auth_header = signer.sign("GET", &url, &[]);

        let response = self
            .client
            .get(&url)
            .header(USER_AGENT, LEGACY_MOBILE_USER_AGENT)
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

        let mut token = OAuth1Token::new(oauth_token, oauth_token_secret);
        if let Some(mfa) = mfa_token {
            token = token.with_mfa(mfa, None);
        }

        Ok(token)
    }

    async fn exchange_oauth1_for_oauth2_legacy(
        &self,
        oauth1: &OAuth1Token,
        login: bool,
    ) -> Result<OAuth2Token> {
        let consumer = self.fetch_oauth_consumer().await?;
        let url = format!(
            "https://connectapi.{}/oauth-service/oauth/exchange/user/2.0",
            DEFAULT_DOMAIN
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
        let mut request = self
            .client
            .post(&url)
            .header(USER_AGENT, LEGACY_MOBILE_USER_AGENT)
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

        let now = Self::now_unix();
        token.expires_at = now + token.expires_in;
        token.refresh_token_expires_at = now + token.refresh_token_expires_in;
        token.client_id = None;

        Ok(token)
    }

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

    /// Refresh OAuth2 token. Prefer DI refresh, then fall back to the legacy
    /// OAuth1 exchange for existing saved sessions.
    pub async fn refresh_oauth2(
        &self,
        oauth1: &OAuth1Token,
        oauth2: &OAuth2Token,
    ) -> Result<OAuth2Token> {
        if oauth2.client_id.is_some() && !oauth2.refresh_token.is_empty() {
            return self.refresh_di_oauth2(oauth2).await;
        }

        if !oauth1.oauth_token.is_empty() && !oauth1.oauth_token_secret.is_empty() {
            return self.exchange_oauth1_for_oauth2_legacy(oauth1, false).await;
        }

        Err(GarminError::auth(
            "Stored session cannot be refreshed automatically. Please run 'garmin auth login' again.",
        ))
    }

    fn summarize_strategy_error(error: &GarminError) -> String {
        match error {
            GarminError::RateLimited => {
                "Garmin returned 429 or a rate-limit-style block".to_string()
            }
            GarminError::Other(message) => message.clone(),
            _ => error.to_string(),
        }
    }

    fn describe_sso_response(response: &SsoResponse, raw_body: &str) -> String {
        if let Some(status) = &response.response_status {
            if status.message.is_empty() {
                return status.response_type.clone();
            }
            return format!("{}: {}", status.response_type, status.message);
        }

        if let Some(error) = &response.error {
            return Self::format_sso_error(error);
        }

        Self::truncate(raw_body, 200)
    }

    fn format_sso_error(error: &SsoErrorResponse) -> String {
        let mut details = format!("status {}", error.status_code);
        if !error.message.is_empty() && error.message != "{}" {
            details.push_str(&format!(": {}", error.message));
        }
        if let Some(request_id) = &error.request_id {
            details.push_str(&format!(" (request-id {})", request_id));
        }
        details
    }

    fn is_rate_limited_response(response: &Response, body: &str) -> bool {
        response.status() == StatusCode::TOO_MANY_REQUESTS || Self::body_is_429(body)
    }

    fn body_is_429(body: &str) -> bool {
        serde_json::from_str::<SsoResponse>(body)
            .ok()
            .and_then(|resp| resp.error)
            .map(|error| error.status_code == "429")
            .unwrap_or(false)
    }

    fn extract_csrf(html: &str) -> Option<String> {
        static REGEX: OnceLock<Regex> = OnceLock::new();
        REGEX
            .get_or_init(|| Regex::new(r#"name="_csrf"\s+value="([^"]+)""#).unwrap())
            .captures(html)
            .and_then(|captures| captures.get(1).map(|m| m.as_str().to_string()))
    }

    fn extract_title(html: &str) -> Option<String> {
        static REGEX: OnceLock<Regex> = OnceLock::new();
        REGEX
            .get_or_init(|| Regex::new(r"(?is)<title>\s*(.*?)\s*</title>").unwrap())
            .captures(html)
            .and_then(|captures| captures.get(1).map(|m| m.as_str().trim().to_string()))
    }

    fn extract_widget_ticket(html: &str) -> Option<String> {
        static REGEX: OnceLock<Regex> = OnceLock::new();
        REGEX
            .get_or_init(|| Regex::new(r#"embed\?ticket=([^"&]+)"#).unwrap())
            .captures(html)
            .and_then(|captures| captures.get(1).map(|m| m.as_str().to_string()))
    }

    fn extract_client_id_from_jwt(token: &str) -> Option<String> {
        let payload = token.split('.').nth(1)?;
        let decoded = URL_SAFE_NO_PAD.decode(payload.as_bytes()).ok()?;
        let json: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
        json.get("client_id")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
    }

    async fn delay_between(min_seconds: f64, max_seconds: f64) {
        let delay = rand::thread_rng().gen_range(min_seconds..=max_seconds);
        tokio::time::sleep(Duration::from_secs_f64(delay)).await;
    }

    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    fn truncate(text: &str, max_len: usize) -> String {
        text.chars().take(max_len).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_successful_sso_response() {
        let json = r#"{
            "responseStatus": {"type": "SUCCESSFUL", "message": ""},
            "serviceTicketId": "ST-12345-abc"
        }"#;
        let resp: SsoResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.response_status.unwrap().response_type, "SUCCESSFUL");
        assert_eq!(resp.service_ticket_id.unwrap(), "ST-12345-abc");
    }

    #[test]
    fn test_parse_mfa_required_response() {
        let json = r#"{
            "responseStatus": {"type": "MFA_REQUIRED", "message": ""},
            "customerMfaInfo": {"mfaLastMethodUsed": "email"}
        }"#;
        let resp: SsoResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.response_status.unwrap().response_type, "MFA_REQUIRED");
        assert_eq!(
            resp.customer_mfa_info
                .unwrap()
                .mfa_last_method_used
                .unwrap(),
            "email"
        );
    }

    #[test]
    fn test_extract_client_id_from_jwt() {
        let payload = URL_SAFE_NO_PAD.encode(r#"{"client_id":"GARMIN_CONNECT_MOBILE_ANDROID_DI"}"#);
        let jwt = format!("header.{}.sig", payload);
        assert_eq!(
            SsoClient::extract_client_id_from_jwt(&jwt),
            Some("GARMIN_CONNECT_MOBILE_ANDROID_DI".to_string())
        );
    }

    #[test]
    fn test_body_is_429() {
        let body = r#"{"error":{"status-code":"429","message":"{}","request-id":"abc"}}"#;
        assert!(SsoClient::body_is_429(body));
    }

    #[test]
    fn test_extract_widget_ticket() {
        let html = r#"<html><body><a href="embed?ticket=ST-abc123">continue</a></body></html>"#;
        assert_eq!(
            SsoClient::extract_widget_ticket(html),
            Some("ST-abc123".to_string())
        );
    }
}
