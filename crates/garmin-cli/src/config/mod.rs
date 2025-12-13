mod credentials;

pub use credentials::CredentialStore;

use crate::error::{GarminError, Result};
use std::path::PathBuf;

/// Default configuration directory name
const CONFIG_DIR_NAME: &str = "garmin";

/// Get the configuration directory path
/// Returns ~/.config/garmin on Unix, ~/Library/Application Support/garmin on macOS
pub fn config_dir() -> Result<PathBuf> {
    dirs::config_dir()
        .map(|p| p.join(CONFIG_DIR_NAME))
        .ok_or_else(|| GarminError::config("Could not determine config directory"))
}

/// Get the data directory path for storing tokens
/// Returns ~/.local/share/garmin on Unix, ~/Library/Application Support/garmin on macOS
pub fn data_dir() -> Result<PathBuf> {
    dirs::data_dir()
        .map(|p| p.join(CONFIG_DIR_NAME))
        .ok_or_else(|| GarminError::config("Could not determine data directory"))
}

/// Ensure a directory exists, creating it if necessary
pub fn ensure_dir(path: &PathBuf) -> Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_dir_exists() {
        let dir = config_dir();
        assert!(dir.is_ok());
        let path = dir.unwrap();
        assert!(path.ends_with("garmin"));
    }

    #[test]
    fn test_data_dir_exists() {
        let dir = data_dir();
        assert!(dir.is_ok());
        let path = dir.unwrap();
        assert!(path.ends_with("garmin"));
    }
}
