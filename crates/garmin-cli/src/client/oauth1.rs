//! OAuth1 request signing implementation
//!
//! This module implements OAuth1 request signing as required by Garmin's API.
//! Based on the oauth1-request specification.

use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use sha1::Sha1;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

/// Characters that need to be percent-encoded in OAuth1
const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// OAuth1 consumer credentials
#[derive(Debug, Clone)]
pub struct OAuthConsumer {
    pub key: String,
    pub secret: String,
}

/// OAuth1 token credentials (after authentication)
#[derive(Debug, Clone, Default)]
pub struct OAuthToken {
    pub token: String,
    pub secret: String,
}

/// OAuth1 signer for requests
pub struct OAuth1Signer {
    consumer: OAuthConsumer,
    token: Option<OAuthToken>,
}

impl OAuth1Signer {
    /// Create a new OAuth1 signer with consumer credentials
    pub fn new(consumer: OAuthConsumer) -> Self {
        Self {
            consumer,
            token: None,
        }
    }

    /// Set the token credentials
    pub fn with_token(mut self, token: OAuthToken) -> Self {
        self.token = Some(token);
        self
    }

    /// Generate OAuth1 authorization header for a request
    ///
    /// The URL can include query parameters - they will be properly extracted
    /// and included in the signature calculation per OAuth1 spec.
    pub fn sign(
        &self,
        method: &str,
        url: &str,
        extra_params: &[(String, String)],
    ) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();

        let nonce = generate_nonce();

        self.sign_with_timestamp_nonce(method, url, extra_params, &timestamp, &nonce)
    }

    /// Generate OAuth1 authorization header with specific timestamp and nonce (for testing)
    pub fn sign_with_timestamp_nonce(
        &self,
        method: &str,
        url: &str,
        extra_params: &[(String, String)],
        timestamp: &str,
        nonce: &str,
    ) -> String {
        // Parse URL to extract base URL and query params
        let parsed_url = Url::parse(url).expect("Invalid URL");
        let base_url = format!(
            "{}://{}{}",
            parsed_url.scheme(),
            parsed_url.host_str().unwrap_or(""),
            parsed_url.path()
        );

        // Extract query parameters from URL
        let url_params: Vec<(String, String)> = parsed_url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let mut oauth_params: BTreeMap<String, String> = BTreeMap::new();
        oauth_params.insert("oauth_consumer_key".to_string(), self.consumer.key.clone());
        oauth_params.insert("oauth_nonce".to_string(), nonce.to_string());
        oauth_params.insert("oauth_signature_method".to_string(), "HMAC-SHA1".to_string());
        oauth_params.insert("oauth_timestamp".to_string(), timestamp.to_string());
        oauth_params.insert("oauth_version".to_string(), "1.0".to_string());

        if let Some(ref token) = self.token {
            oauth_params.insert("oauth_token".to_string(), token.token.clone());
        }

        // Calculate signature using base URL (without query params)
        // but including all params (URL query + extra + oauth) in signature
        let signature = self.calculate_signature(method, &base_url, &url_params, extra_params, &oauth_params);
        oauth_params.insert("oauth_signature".to_string(), signature);

        // Build Authorization header
        let auth_params: Vec<String> = oauth_params
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, percent_encode(v)))
            .collect();

        format!("OAuth {}", auth_params.join(", "))
    }

    fn calculate_signature(
        &self,
        method: &str,
        base_url: &str,
        url_params: &[(String, String)],
        extra_params: &[(String, String)],
        oauth_params: &BTreeMap<String, String>,
    ) -> String {
        // Combine all parameters (URL query params + extra params + oauth params)
        let mut all_params: BTreeMap<String, String> = oauth_params.clone();
        for (k, v) in url_params {
            all_params.insert(k.clone(), v.clone());
        }
        for (k, v) in extra_params {
            all_params.insert(k.clone(), v.clone());
        }

        // Build parameter string (sorted by key)
        let param_string: String = all_params
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        // Build base string: METHOD&BASE_URL&PARAMS
        let base_string = format!(
            "{}&{}&{}",
            method.to_uppercase(),
            percent_encode(base_url),
            percent_encode(&param_string)
        );

        // Build signing key: consumer_secret&token_secret
        let token_secret = self
            .token
            .as_ref()
            .map(|t| t.secret.as_str())
            .unwrap_or("");
        let signing_key = format!("{}&{}", percent_encode(&self.consumer.secret), percent_encode(token_secret));

        // Calculate HMAC-SHA1
        let mut mac = Hmac::<Sha1>::new_from_slice(signing_key.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(base_string.as_bytes());
        let result = mac.finalize();

        // Base64 encode
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, result.into_bytes())
    }
}

/// Percent-encode a string according to OAuth1 spec
fn percent_encode(s: &str) -> String {
    utf8_percent_encode(s, ENCODE_SET).to_string()
}

/// Generate a random nonce for OAuth1
fn generate_nonce() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 16] = rand::Rng::gen(&mut rng);
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Parse OAuth1 response (URL-encoded key=value pairs)
pub fn parse_oauth_response(response: &str) -> BTreeMap<String, String> {
    response
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) => Some((
                    urlencoding::decode(key).unwrap_or_default().into_owned(),
                    urlencoding::decode(value).unwrap_or_default().into_owned(),
                )),
                _ => None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth_consumer_creation() {
        let consumer = OAuthConsumer {
            key: "test_key".to_string(),
            secret: "test_secret".to_string(),
        };
        assert_eq!(consumer.key, "test_key");
        assert_eq!(consumer.secret, "test_secret");
    }

    #[test]
    fn test_oauth1_signer_creation() {
        let consumer = OAuthConsumer {
            key: "test_key".to_string(),
            secret: "test_secret".to_string(),
        };
        let signer = OAuth1Signer::new(consumer);
        assert!(signer.token.is_none());
    }

    #[test]
    fn test_oauth1_signer_with_token() {
        let consumer = OAuthConsumer {
            key: "test_key".to_string(),
            secret: "test_secret".to_string(),
        };
        let token = OAuthToken {
            token: "token123".to_string(),
            secret: "tokensecret".to_string(),
        };
        let signer = OAuth1Signer::new(consumer).with_token(token);
        assert!(signer.token.is_some());
        assert_eq!(signer.token.as_ref().unwrap().token, "token123");
    }

    #[test]
    fn test_sign_generates_authorization_header() {
        let consumer = OAuthConsumer {
            key: "test_consumer_key".to_string(),
            secret: "test_consumer_secret".to_string(),
        };
        let signer = OAuth1Signer::new(consumer);

        let auth_header = signer.sign_with_timestamp_nonce(
            "GET",
            "https://example.com/api/test",
            &[],
            "1234567890",
            "abc123nonce",
        );

        assert!(auth_header.starts_with("OAuth "));
        assert!(auth_header.contains("oauth_consumer_key=\"test_consumer_key\""));
        assert!(auth_header.contains("oauth_signature_method=\"HMAC-SHA1\""));
        assert!(auth_header.contains("oauth_timestamp=\"1234567890\""));
        assert!(auth_header.contains("oauth_nonce=\"abc123nonce\""));
        assert!(auth_header.contains("oauth_version=\"1.0\""));
        assert!(auth_header.contains("oauth_signature="));
    }

    #[test]
    fn test_sign_with_token_includes_oauth_token() {
        let consumer = OAuthConsumer {
            key: "consumer_key".to_string(),
            secret: "consumer_secret".to_string(),
        };
        let token = OAuthToken {
            token: "user_token".to_string(),
            secret: "user_secret".to_string(),
        };
        let signer = OAuth1Signer::new(consumer).with_token(token);

        let auth_header = signer.sign_with_timestamp_nonce(
            "GET",
            "https://example.com/api",
            &[],
            "1234567890",
            "nonce123",
        );

        assert!(auth_header.contains("oauth_token=\"user_token\""));
    }

    #[test]
    fn test_percent_encode() {
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("foo=bar&baz"), "foo%3Dbar%26baz");
        assert_eq!(percent_encode("simple"), "simple");
    }

    #[test]
    fn test_parse_oauth_response() {
        let response = "oauth_token=abc123&oauth_token_secret=xyz789&mfa_token=mfa456";
        let parsed = parse_oauth_response(response);

        assert_eq!(parsed.get("oauth_token"), Some(&"abc123".to_string()));
        assert_eq!(parsed.get("oauth_token_secret"), Some(&"xyz789".to_string()));
        assert_eq!(parsed.get("mfa_token"), Some(&"mfa456".to_string()));
    }

    #[test]
    fn test_parse_oauth_response_with_encoded_values() {
        let response = "key=value%20with%20spaces&other=normal";
        let parsed = parse_oauth_response(response);

        assert_eq!(parsed.get("key"), Some(&"value with spaces".to_string()));
        assert_eq!(parsed.get("other"), Some(&"normal".to_string()));
    }

    #[test]
    fn test_nonce_generation() {
        let nonce1 = generate_nonce();
        let nonce2 = generate_nonce();

        assert_eq!(nonce1.len(), 32); // 16 bytes = 32 hex chars
        assert_ne!(nonce1, nonce2); // Should be random
    }

    #[test]
    fn test_sign_with_url_query_params() {
        // Test that URL query parameters are properly included in signature
        let consumer = OAuthConsumer {
            key: "dpf43f3p2l4k3l03".to_string(),
            secret: "kd94hf93k423kf44".to_string(),
        };
        let token = OAuthToken {
            token: "nnch734d00sl2jdk".to_string(),
            secret: "pfkkdhi9sl3r4s00".to_string(),
        };
        let signer = OAuth1Signer::new(consumer).with_token(token);

        // URL with query parameters
        let url = "http://photos.example.net/photos?file=vacation.jpg&size=original";

        let auth_header = signer.sign_with_timestamp_nonce(
            "GET",
            url,
            &[],
            "1191242096",
            "kllo9940pd9333jh",
        );

        // The signature should include both URL params and OAuth params
        assert!(auth_header.contains("oauth_signature="));
        assert!(auth_header.starts_with("OAuth "));
        // Verify exact signature matches Python reference implementation
        assert!(
            auth_header.contains("oauth_signature=\"tR3%2BTy81lMeYAr%2FFid0kMTYa%2FWM%3D\""),
            "Expected signature tR3+Ty81lMeYAr/Fid0kMTYa/WM= (url-encoded), got: {}",
            auth_header
        );
    }

    #[test]
    fn test_sign_url_with_query_params_extracts_base_url() {
        let consumer = OAuthConsumer {
            key: "consumer".to_string(),
            secret: "secret".to_string(),
        };
        let signer = OAuth1Signer::new(consumer);

        // Test with URL containing query params
        let url = "https://api.example.com/path?ticket=ST-123&login-url=https://sso.example.com/embed";

        let auth_header = signer.sign_with_timestamp_nonce(
            "GET",
            url,
            &[],
            "1234567890",
            "testnonce",
        );

        // Should produce a valid OAuth header
        assert!(auth_header.starts_with("OAuth "));
        assert!(auth_header.contains("oauth_consumer_key=\"consumer\""));
    }
}
