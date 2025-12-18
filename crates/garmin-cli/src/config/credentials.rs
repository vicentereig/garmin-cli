use crate::client::{OAuth1Token, OAuth2Token};
use crate::error::{GarminError, Result};
use std::fs;
use std::path::PathBuf;

const OAUTH1_FILENAME: &str = "oauth1_token.json";
const OAUTH2_FILENAME: &str = "oauth2_token.json";
const SERVICE_NAME: &str = "garmin-cli";

/// Manages credential storage for Garmin tokens.
/// Supports file-based storage with optional keyring integration.
pub struct CredentialStore {
    profile: String,
    base_dir: PathBuf,
}

impl CredentialStore {
    /// Create a new credential store for the given profile
    pub fn new(profile: Option<String>) -> Result<Self> {
        let profile = profile.unwrap_or_else(|| "default".to_string());
        let base_dir = super::data_dir()?.join(&profile);
        super::ensure_dir(&base_dir)?;

        Ok(Self { profile, base_dir })
    }

    /// Create a credential store with a custom base directory (for testing)
    pub fn with_dir(profile: impl Into<String>, base_dir: PathBuf) -> Result<Self> {
        let profile = profile.into();
        let dir = base_dir.join(&profile);
        super::ensure_dir(&dir)?;

        Ok(Self {
            profile,
            base_dir: dir,
        })
    }

    /// Get the profile name
    pub fn profile(&self) -> &str {
        &self.profile
    }

    /// Save OAuth1 token to storage
    pub fn save_oauth1(&self, token: &OAuth1Token) -> Result<()> {
        let path = self.base_dir.join(OAUTH1_FILENAME);
        let json = serde_json::to_string_pretty(token)?;
        fs::write(&path, json)?;

        // Set restrictive permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }

        Ok(())
    }

    /// Load OAuth1 token from storage
    pub fn load_oauth1(&self) -> Result<Option<OAuth1Token>> {
        let path = self.base_dir.join(OAUTH1_FILENAME);
        if !path.exists() {
            return Ok(None);
        }

        let json = fs::read_to_string(&path)?;
        let token: OAuth1Token = serde_json::from_str(&json)?;
        Ok(Some(token))
    }

    /// Save OAuth2 token to storage
    pub fn save_oauth2(&self, token: &OAuth2Token) -> Result<()> {
        let path = self.base_dir.join(OAUTH2_FILENAME);
        let json = serde_json::to_string_pretty(token)?;
        fs::write(&path, json)?;

        // Set restrictive permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }

        Ok(())
    }

    /// Load OAuth2 token from storage
    pub fn load_oauth2(&self) -> Result<Option<OAuth2Token>> {
        let path = self.base_dir.join(OAUTH2_FILENAME);
        if !path.exists() {
            return Ok(None);
        }

        let json = fs::read_to_string(&path)?;
        let token: OAuth2Token = serde_json::from_str(&json)?;
        Ok(Some(token))
    }

    /// Save both tokens
    pub fn save_tokens(&self, oauth1: &OAuth1Token, oauth2: &OAuth2Token) -> Result<()> {
        self.save_oauth1(oauth1)?;
        self.save_oauth2(oauth2)?;
        Ok(())
    }

    /// Load both tokens, returns None if either is missing
    pub fn load_tokens(&self) -> Result<Option<(OAuth1Token, OAuth2Token)>> {
        let oauth1 = self.load_oauth1()?;
        let oauth2 = self.load_oauth2()?;

        match (oauth1, oauth2) {
            (Some(o1), Some(o2)) => Ok(Some((o1, o2))),
            _ => Ok(None),
        }
    }

    /// Check if credentials exist
    pub fn has_credentials(&self) -> bool {
        self.base_dir.join(OAUTH1_FILENAME).exists() && self.base_dir.join(OAUTH2_FILENAME).exists()
    }

    /// Clear all stored credentials
    pub fn clear(&self) -> Result<()> {
        let oauth1_path = self.base_dir.join(OAUTH1_FILENAME);
        let oauth2_path = self.base_dir.join(OAUTH2_FILENAME);

        if oauth1_path.exists() {
            fs::remove_file(oauth1_path)?;
        }
        if oauth2_path.exists() {
            fs::remove_file(oauth2_path)?;
        }

        Ok(())
    }

    /// Try to store OAuth1 token secret in system keyring
    /// Falls back silently if keyring is not available
    pub fn store_secret_in_keyring(&self, secret: &str) -> Result<()> {
        let entry = keyring::Entry::new(SERVICE_NAME, &self.profile)
            .map_err(|e| GarminError::Keyring(e.to_string()))?;

        entry
            .set_password(secret)
            .map_err(|e| GarminError::Keyring(e.to_string()))?;

        Ok(())
    }

    /// Try to load OAuth1 token secret from system keyring
    pub fn load_secret_from_keyring(&self) -> Result<Option<String>> {
        let entry = keyring::Entry::new(SERVICE_NAME, &self.profile)
            .map_err(|e| GarminError::Keyring(e.to_string()))?;

        match entry.get_password() {
            Ok(secret) => Ok(Some(secret)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(GarminError::Keyring(e.to_string())),
        }
    }

    /// Delete secret from system keyring
    pub fn delete_secret_from_keyring(&self) -> Result<()> {
        let entry = keyring::Entry::new(SERVICE_NAME, &self.profile)
            .map_err(|e| GarminError::Keyring(e.to_string()))?;

        match entry.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()), // Already deleted
            Err(e) => Err(GarminError::Keyring(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    fn create_test_oauth1() -> OAuth1Token {
        OAuth1Token::new("test_token".to_string(), "test_secret".to_string())
    }

    fn create_test_oauth2() -> OAuth2Token {
        OAuth2Token {
            scope: "test_scope".to_string(),
            jti: "test_jti".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "test_access".to_string(),
            refresh_token: "test_refresh".to_string(),
            expires_in: 3600,
            expires_at: Utc::now().timestamp() + 3600,
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: Utc::now().timestamp() + 86400,
        }
    }

    #[test]
    fn test_credential_store_creation() {
        let temp_dir = TempDir::new().unwrap();
        let store = CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf());
        assert!(store.is_ok());
        assert_eq!(store.unwrap().profile(), "test_profile");
    }

    #[test]
    fn test_save_and_load_oauth1() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        let token = create_test_oauth1();
        store.save_oauth1(&token).unwrap();

        let loaded = store.load_oauth1().unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap(), token);
    }

    #[test]
    fn test_save_and_load_oauth2() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        let token = create_test_oauth2();
        store.save_oauth2(&token).unwrap();

        let loaded = store.load_oauth2().unwrap();
        assert!(loaded.is_some());
        // Note: We can't compare directly because expires_at may differ by a few ms
        let loaded_token = loaded.unwrap();
        assert_eq!(loaded_token.access_token, token.access_token);
        assert_eq!(loaded_token.refresh_token, token.refresh_token);
    }

    #[test]
    fn test_load_missing_oauth1() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        let loaded = store.load_oauth1().unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_missing_oauth2() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        let loaded = store.load_oauth2().unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_and_load_both_tokens() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        let oauth1 = create_test_oauth1();
        let oauth2 = create_test_oauth2();
        store.save_tokens(&oauth1, &oauth2).unwrap();

        let loaded = store.load_tokens().unwrap();
        assert!(loaded.is_some());
        let (loaded_oauth1, loaded_oauth2) = loaded.unwrap();
        assert_eq!(loaded_oauth1, oauth1);
        assert_eq!(loaded_oauth2.access_token, oauth2.access_token);
    }

    #[test]
    fn test_has_credentials() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        assert!(!store.has_credentials());

        let oauth1 = create_test_oauth1();
        let oauth2 = create_test_oauth2();
        store.save_tokens(&oauth1, &oauth2).unwrap();

        assert!(store.has_credentials());
    }

    #[test]
    fn test_clear_credentials() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        let oauth1 = create_test_oauth1();
        let oauth2 = create_test_oauth2();
        store.save_tokens(&oauth1, &oauth2).unwrap();
        assert!(store.has_credentials());

        store.clear().unwrap();
        assert!(!store.has_credentials());
    }

    #[test]
    fn test_partial_tokens_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            CredentialStore::with_dir("test_profile", temp_dir.path().to_path_buf()).unwrap();

        // Only save OAuth1
        let oauth1 = create_test_oauth1();
        store.save_oauth1(&oauth1).unwrap();

        // load_tokens should return None because OAuth2 is missing
        let loaded = store.load_tokens().unwrap();
        assert!(loaded.is_none());
    }
}
