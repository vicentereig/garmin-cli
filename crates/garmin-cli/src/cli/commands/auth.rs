//! Authentication commands for garmin-cli

use crate::client::{OAuth1Token, OAuth2Token, SsoClient};
use crate::config::CredentialStore;
use crate::error::{GarminError, Result};
use std::io::{self, Write};

/// Execute the login command
pub async fn login(email: Option<String>, profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile.clone())?;

    // Check if already logged in
    if store.has_credentials() {
        if let Some((_, oauth2)) = store.load_tokens()? {
            if !oauth2.is_expired() {
                println!("Already logged in. Use 'garmin auth logout' to log out first.");
                return Ok(());
            }
        }
    }

    // Get email
    let email = match email {
        Some(e) => e,
        None => {
            print!("Email: ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            input.trim().to_string()
        }
    };

    // Get password
    let password = rpassword_prompt("Password: ")?;

    println!("Logging in...");

    // Perform login
    let mut sso_client = SsoClient::new(None)?;
    let (oauth1, oauth2) = sso_client
        .login(&email, &password, Some(|| prompt_mfa()))
        .await?;

    // Save tokens
    store.save_tokens(&oauth1, &oauth2)?;

    println!("Successfully logged in!");
    println!("Profile: {}", store.profile());

    Ok(())
}

/// Execute the logout command
pub async fn logout(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;

    if !store.has_credentials() {
        println!("Not logged in.");
        return Ok(());
    }

    store.clear()?;
    // Also try to clear keyring (ignore errors)
    let _ = store.delete_secret_from_keyring();

    println!("Successfully logged out.");
    Ok(())
}

/// Execute the status command
pub async fn status(profile: Option<String>) -> Result<()> {
    let store = CredentialStore::new(profile)?;

    if !store.has_credentials() {
        println!("Status: Not logged in");
        println!("Run 'garmin auth login' to authenticate.");
        return Ok(());
    }

    match store.load_tokens()? {
        Some((oauth1, oauth2)) => {
            println!("Status: Logged in");
            println!("Profile: {}", store.profile());
            println!("Domain: {}", oauth1.domain);

            if oauth2.is_expired() {
                println!("Access Token: Expired (will refresh on next request)");
            } else {
                let expires_in = oauth2.expires_at - chrono::Utc::now().timestamp();
                if expires_in > 3600 {
                    println!("Access Token: Valid (expires in {} hours)", expires_in / 3600);
                } else if expires_in > 60 {
                    println!("Access Token: Valid (expires in {} minutes)", expires_in / 60);
                } else {
                    println!("Access Token: Valid (expires in {} seconds)", expires_in);
                }
            }

            if oauth1.mfa_token.is_some() {
                println!("MFA: Enabled");
            }
        }
        None => {
            println!("Status: Credentials corrupted");
            println!("Run 'garmin auth logout' then 'garmin auth login' to fix.");
        }
    }

    Ok(())
}

/// Refresh OAuth2 token using OAuth1 token
pub async fn refresh_token(store: &CredentialStore) -> Result<(OAuth1Token, OAuth2Token)> {
    let (oauth1, oauth2) = store
        .load_tokens()?
        .ok_or(GarminError::NotAuthenticated)?;

    if !oauth2.is_expired() {
        return Ok((oauth1, oauth2));
    }

    println!("Refreshing access token...");
    let sso_client = SsoClient::new(Some(&oauth1.domain))?;
    let new_oauth2 = sso_client.refresh_oauth2(&oauth1).await?;

    store.save_oauth2(&new_oauth2)?;

    Ok((oauth1, new_oauth2))
}

/// Prompt for password without echoing
fn rpassword_prompt(prompt: &str) -> Result<String> {
    print!("{}", prompt);
    io::stdout().flush()?;

    // Use rpassword if available, otherwise just read normally (less secure)
    let password = rpassword::read_password()
        .map_err(|e| GarminError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;

    Ok(password)
}

/// Prompt for MFA code
fn prompt_mfa() -> String {
    print!("MFA Code: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}

#[cfg(test)]
mod tests {
    // Integration tests would go here, but they require actual credentials
    // For unit testing, we'd use wiremock to mock the SSO endpoints
}
