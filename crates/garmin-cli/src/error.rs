use thiserror::Error;

/// Main error type for garmin-cli
#[derive(Error, Debug)]
pub enum GarminError {
    #[error("Authentication error: {0}")]
    Authentication(String),

    #[error("Authentication required. Please run 'garmin auth login' first.")]
    NotAuthenticated,

    #[error("MFA required")]
    MfaRequired,

    #[error("Rate limited. Please wait before retrying.")]
    RateLimited,

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("API error {status}: {message}")]
    Api { status: u16, message: String },

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Keyring error: {0}")]
    Keyring(String),

    #[error("Invalid date format: {0}. Expected YYYY-MM-DD")]
    InvalidDateFormat(String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, GarminError>;

impl GarminError {
    /// Create an authentication error from a message
    pub fn auth(msg: impl Into<String>) -> Self {
        Self::Authentication(msg.into())
    }

    /// Create a configuration error from a message
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create an invalid response error from a message
    pub fn invalid_response(msg: impl Into<String>) -> Self {
        Self::InvalidResponse(msg.into())
    }

    /// Create an invalid parameter error from a message
    pub fn invalid_param(msg: impl Into<String>) -> Self {
        Self::InvalidParameter(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GarminError::Authentication("Invalid credentials".to_string());
        assert_eq!(err.to_string(), "Authentication error: Invalid credentials");
    }

    #[test]
    fn test_not_authenticated_error() {
        let err = GarminError::NotAuthenticated;
        assert!(err.to_string().contains("garmin auth login"));
    }

    #[test]
    fn test_rate_limited_error() {
        let err = GarminError::RateLimited;
        assert!(err.to_string().contains("Rate limited"));
    }

    #[test]
    fn test_invalid_date_format_error() {
        let err = GarminError::InvalidDateFormat("not-a-date".to_string());
        assert!(err.to_string().contains("not-a-date"));
        assert!(err.to_string().contains("YYYY-MM-DD"));
    }

    #[test]
    fn test_error_constructors() {
        let auth_err = GarminError::auth("test auth");
        assert!(matches!(auth_err, GarminError::Authentication(_)));

        let config_err = GarminError::config("test config");
        assert!(matches!(config_err, GarminError::Config(_)));

        let response_err = GarminError::invalid_response("bad response");
        assert!(matches!(response_err, GarminError::InvalidResponse(_)));

        let param_err = GarminError::invalid_param("bad param");
        assert!(matches!(param_err, GarminError::InvalidParameter(_)));
    }
}
