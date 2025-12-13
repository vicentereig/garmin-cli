use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// OAuth1 token obtained after initial SSO authentication.
/// Long-lived (~1 year), used to obtain OAuth2 tokens.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OAuth1Token {
    pub oauth_token: String,
    pub oauth_token_secret: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mfa_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mfa_expiration_timestamp: Option<DateTime<Utc>>,
    #[serde(default = "default_domain")]
    pub domain: String,
}

fn default_domain() -> String {
    "garmin.com".to_string()
}

impl OAuth1Token {
    pub fn new(oauth_token: String, oauth_token_secret: String) -> Self {
        Self {
            oauth_token,
            oauth_token_secret,
            mfa_token: None,
            mfa_expiration_timestamp: None,
            domain: default_domain(),
        }
    }

    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = domain.into();
        self
    }

    pub fn with_mfa(mut self, mfa_token: String, expiration: Option<DateTime<Utc>>) -> Self {
        self.mfa_token = Some(mfa_token);
        self.mfa_expiration_timestamp = expiration;
        self
    }
}

/// OAuth2 Bearer token for API requests.
/// Short-lived, automatically refreshed using OAuth1 token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OAuth2Token {
    pub scope: String,
    pub jti: String,
    pub token_type: String,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
    #[serde(default)]
    pub expires_at: i64,
    pub refresh_token_expires_in: i64,
    #[serde(default)]
    pub refresh_token_expires_at: i64,
}

impl OAuth2Token {
    /// Check if the access token has expired.
    pub fn is_expired(&self) -> bool {
        let now = Utc::now().timestamp();
        self.expires_at < now
    }

    /// Check if the refresh token has expired.
    pub fn is_refresh_expired(&self) -> bool {
        let now = Utc::now().timestamp();
        self.refresh_token_expires_at < now
    }

    /// Returns the Authorization header value.
    pub fn authorization_header(&self) -> String {
        format!("{} {}", self.token_type, self.access_token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth1_token_new() {
        let token = OAuth1Token::new(
            "test_oauth_token".to_string(),
            "test_oauth_secret".to_string(),
        );

        assert_eq!(token.oauth_token, "test_oauth_token");
        assert_eq!(token.oauth_token_secret, "test_oauth_secret");
        assert_eq!(token.domain, "garmin.com");
        assert!(token.mfa_token.is_none());
        assert!(token.mfa_expiration_timestamp.is_none());
    }

    #[test]
    fn test_oauth1_token_with_domain() {
        let token = OAuth1Token::new(
            "test_oauth_token".to_string(),
            "test_oauth_secret".to_string(),
        )
        .with_domain("garmin.cn");

        assert_eq!(token.domain, "garmin.cn");
    }

    #[test]
    fn test_oauth1_token_with_mfa() {
        let expiration = Utc::now();
        let token = OAuth1Token::new(
            "test_oauth_token".to_string(),
            "test_oauth_secret".to_string(),
        )
        .with_mfa("mfa_token_123".to_string(), Some(expiration));

        assert_eq!(token.mfa_token, Some("mfa_token_123".to_string()));
        assert_eq!(token.mfa_expiration_timestamp, Some(expiration));
    }

    #[test]
    fn test_oauth1_token_serialization() {
        let token = OAuth1Token::new(
            "test_oauth_token".to_string(),
            "test_oauth_secret".to_string(),
        );

        let json = serde_json::to_string(&token).unwrap();
        let deserialized: OAuth1Token = serde_json::from_str(&json).unwrap();

        assert_eq!(token, deserialized);
    }

    #[test]
    fn test_oauth2_token_is_expired() {
        let expired_token = OAuth2Token {
            scope: "test".to_string(),
            jti: "jti123".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "access123".to_string(),
            refresh_token: "refresh123".to_string(),
            expires_in: 3600,
            expires_at: 0, // Expired (epoch)
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: Utc::now().timestamp() + 86400,
        };

        assert!(expired_token.is_expired());
    }

    #[test]
    fn test_oauth2_token_not_expired() {
        let valid_token = OAuth2Token {
            scope: "test".to_string(),
            jti: "jti123".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "access123".to_string(),
            refresh_token: "refresh123".to_string(),
            expires_in: 3600,
            expires_at: Utc::now().timestamp() + 3600, // 1 hour from now
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: Utc::now().timestamp() + 86400,
        };

        assert!(!valid_token.is_expired());
    }

    #[test]
    fn test_oauth2_token_refresh_expired() {
        let token = OAuth2Token {
            scope: "test".to_string(),
            jti: "jti123".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "access123".to_string(),
            refresh_token: "refresh123".to_string(),
            expires_in: 3600,
            expires_at: Utc::now().timestamp() + 3600,
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: 0, // Refresh token expired
        };

        assert!(token.is_refresh_expired());
    }

    #[test]
    fn test_oauth2_token_authorization_header() {
        let token = OAuth2Token {
            scope: "test".to_string(),
            jti: "jti123".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "my_access_token".to_string(),
            refresh_token: "refresh123".to_string(),
            expires_in: 3600,
            expires_at: Utc::now().timestamp() + 3600,
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: Utc::now().timestamp() + 86400,
        };

        assert_eq!(token.authorization_header(), "Bearer my_access_token");
    }

    #[test]
    fn test_oauth2_token_serialization() {
        let token = OAuth2Token {
            scope: "test".to_string(),
            jti: "jti123".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "access123".to_string(),
            refresh_token: "refresh123".to_string(),
            expires_in: 3600,
            expires_at: 1700000000,
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: 1700086400,
        };

        let json = serde_json::to_string(&token).unwrap();
        let deserialized: OAuth2Token = serde_json::from_str(&json).unwrap();

        assert_eq!(token, deserialized);
    }
}
