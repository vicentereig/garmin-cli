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

#[derive(Debug, PartialEq, Eq)]
struct DbLockInfo {
    path: Option<String>,
    holder: Option<String>,
    pid: Option<u32>,
}

fn parse_db_lock_info(message: &str) -> Option<DbLockInfo> {
    let mut info = DbLockInfo {
        path: None,
        holder: None,
        pid: None,
    };

    if message.contains("Could not set lock on file") {
        let path_start = message.find("file \"").map(|idx| idx + 6);
        let path_end = path_start.and_then(|start| message[start..].find('"').map(|end| start + end));
        if let (Some(start), Some(end)) = (path_start, path_end) {
            info.path = Some(message[start..end].to_string());
        }

        let holder_start = message.find("Conflicting lock is held in ").map(|idx| idx + 31);
        let holder_end = holder_start
            .and_then(|start| message[start..].find(" (PID ").map(|end| start + end));
        if let (Some(start), Some(end)) = (holder_start, holder_end) {
            info.holder = Some(message[start..end].to_string());
        }

        let pid_start = message.find("(PID ").map(|idx| idx + 5);
        let pid_end = pid_start.and_then(|start| message[start..].find(')').map(|end| start + end));
        if let (Some(start), Some(end)) = (pid_start, pid_end) {
            info.pid = message[start..end].parse::<u32>().ok();
        }

        return Some(info);
    }

    if message.contains("database is locked") || message.contains("database is busy") {
        return Some(info);
    }

    None
}

/// Provide a friendlier, user-focused error message when possible.
pub fn format_user_error(error: &GarminError) -> String {
    match error {
        GarminError::Database(message) => {
            if let Some(info) = parse_db_lock_info(message) {
                let mut details = String::from(
                    "Database is locked by another garmin process. If another sync is running, wait for it to finish or stop it, then retry.",
                );

                if let Some(pid) = info.pid {
                    details.push_str(&format!(" (PID {})", pid));
                }

                if let Some(holder) = info.holder {
                    details.push_str(&format!("\nLock holder: {}", holder));
                }

                if let Some(path) = info.path {
                    details.push_str(&format!("\nLocked file: {}", path));
                }

                return details;
            }
            error.to_string()
        }
        _ => error.to_string(),
    }
}

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

    #[test]
    fn test_format_user_error_duckdb_lock() {
        let msg = "IO Error: Could not set lock on file \"/Users/vicente/Library/Application Support/garmin/garmin.duckdb\": Conflicting lock is held in /opt/homebrew/Cellar/garmin/1.0.5/bin/garmin (PID 31358) by user vicente.";
        let err = GarminError::Database(msg.to_string());
        let formatted = format_user_error(&err);

        assert!(formatted.contains("Database is locked"));
        assert!(formatted.contains("PID 31358"));
        assert!(formatted.contains("garmin.duckdb"));
    }

    #[test]
    fn test_format_user_error_sqlite_lock() {
        let msg = "database is locked";
        let err = GarminError::Database(msg.to_string());
        let formatted = format_user_error(&err);

        assert!(formatted.contains("Database is locked"));
    }
}
