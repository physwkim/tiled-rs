//! External OIDC bearer-token validation.
//!
//! Plugs Microsoft Entra ID, Auth0, Keycloak, etc. in front of tiled
//! without storing user credentials in the auth DB. The server fetches
//! the IdP's JWKS once (cached for the lifetime of the process by
//! default), and verifies incoming bearer JWTs against the matching
//! public key. Successful validation produces a `(provider, sub)`
//! identity that `AuthDb::ensure_principal` upserts on first sight —
//! same code path the password / device flows use.
//!
//! Mirrors tiled#1364 (EntraAuthenticator) + tiled#1343
//! (ProxiedOIDCAuthenticator).

#![cfg(feature = "oidc")]

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{AuthError, Result};

/// One configured upstream IdP.
#[derive(Debug, Clone)]
pub struct OidcProvider {
    /// Provider name surfaced to clients (e.g. "entra", "auth0"). Becomes
    /// the `provider` part of the resulting Identity.
    pub name: String,
    /// JWKS URL the validator fetches public keys from.
    pub jwks_url: String,
    /// Required `iss` claim — rejects tokens issued elsewhere.
    pub issuer: String,
    /// Required `aud` claim. Must be non-empty — `ExternalOidcValidator::new`
    /// rejects an empty list (OIDC Core §3.1.3.7 #3 / RFC 8725 §3.1).
    pub audiences: Vec<String>,
    /// Override the JWT claim used as the principal subject. Defaults
    /// to `sub`; some IdPs prefer `oid` (Entra) or `email`.
    pub subject_claim: String,
    /// Allowed signing algorithms. When non-empty, tokens whose `alg`
    /// header is not in this list are rejected before signature
    /// verification. When empty, the algorithm is derived from the
    /// matched JWK's `alg` field (RS256/ES256 fallback by key type).
    /// Never populated from the attacker-controlled token header.
    pub algorithms: Vec<Algorithm>,
}

#[derive(Debug, Clone, Deserialize)]
struct JwksDocument {
    keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Deserialize)]
struct Jwk {
    kid: Option<String>,
    kty: String,
    n: Option<String>,
    e: Option<String>,
    x: Option<String>,
    y: Option<String>,
    #[serde(rename = "use")]
    use_: Option<String>,
    /// Signing algorithm declared by the IdP in the JWKS. Used to pin
    /// the algorithm at the key-fetch layer so we never rely on the
    /// attacker-controlled token header.
    alg: Option<Algorithm>,
}

struct CachedKeys {
    keys: HashMap<String, (DecodingKey, Algorithm)>,
    expires_at: DateTime<Utc>,
}

/// Resolves bearer tokens against one or more external IdPs. Cheap to
/// clone — keys are stored behind an `Arc<RwLock>`.
#[derive(Clone)]
pub struct ExternalOidcValidator {
    providers: Arc<Vec<OidcProvider>>,
    cache: Arc<RwLock<HashMap<String, CachedKeys>>>,
    cache_ttl: Duration,
    http: reqwest::Client,
}

impl std::fmt::Debug for ExternalOidcValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalOidcValidator")
            .field("providers", &self.providers.len())
            .field("cache_ttl", &self.cache_ttl)
            .finish()
    }
}

impl ExternalOidcValidator {
    /// Construct the validator. Returns an error if any provider has an empty
    /// `audiences` list — audience validation is mandatory per OIDC Core
    /// §3.1.3.7 #3 and RFC 8725 §3.1; omitting it allows cross-relying-party
    /// token replay attacks.
    pub fn new(providers: Vec<OidcProvider>) -> Result<Self> {
        for p in &providers {
            if p.audiences.is_empty() {
                return Err(AuthError::Validation(format!(
                    "OIDC provider '{}': `audiences` must not be empty \
                     (OIDC Core §3.1.3.7 #3 / RFC 8725 §3.1 require aud validation)",
                    p.name
                )));
            }
        }
        Ok(Self {
            providers: Arc::new(providers),
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::hours(1),
            http: reqwest::Client::new(),
        })
    }

    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = ttl;
        self
    }

    /// Try each configured provider's `iss` claim against the token; on
    /// match, fetch the JWKS (cached) and validate the signature. Returns
    /// the (provider, sub) tuple on success.
    pub async fn validate(&self, token: &str) -> Result<ValidatedToken> {
        let header = decode_header(token).map_err(AuthError::from)?;
        let kid = header
            .kid
            .clone()
            .ok_or_else(|| AuthError::Unauthorized("token has no kid".into()))?;

        // Pre-decode WITHOUT signature verification just to read the
        // `iss` claim and pick the provider. We then re-decode with the
        // matching key + full validation.
        let payload_b64 = token
            .split('.')
            .nth(1)
            .ok_or_else(|| AuthError::Unauthorized("malformed token".into()))?;
        let payload = base64_url_decode(payload_b64)?;
        let payload_value: serde_json::Value =
            serde_json::from_slice(&payload).map_err(AuthError::from)?;
        let issuer = payload_value
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::Unauthorized("token missing iss".into()))?;

        let provider = self
            .providers
            .iter()
            .find(|p| p.issuer == issuer)
            .ok_or_else(|| AuthError::Unauthorized(format!("unknown token issuer: {issuer}")))?;

        let (key, jwk_alg) = self.fetch_key(provider, &kid).await?;
        // Pin algorithms from provider config or JWKS — never from the
        // attacker-controlled token header (alg-confusion defence).
        let algorithms: Vec<Algorithm> = if provider.algorithms.is_empty() {
            vec![jwk_alg]
        } else {
            provider.algorithms.clone()
        };
        let validation = build_validation(provider, algorithms);

        let claims = decode::<serde_json::Value>(token, &key, &validation)
            .map_err(AuthError::from)?
            .claims;
        let sub = claims
            .get(&provider.subject_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AuthError::Unauthorized(format!("token missing claim '{}'", provider.subject_claim))
            })?
            .to_string();
        Ok(ValidatedToken {
            provider: provider.name.clone(),
            sub,
            claims,
        })
    }

    async fn fetch_key(
        &self,
        provider: &OidcProvider,
        kid: &str,
    ) -> Result<(DecodingKey, Algorithm)> {
        if let Some(cached) = self.cache.read().await.get(&provider.name)
            && cached.expires_at > Utc::now()
            && let Some(k) = cached.keys.get(kid)
        {
            return Ok(k.clone());
        }
        // Cache miss / expiry → re-fetch.
        let response = self
            .http
            .get(&provider.jwks_url)
            .send()
            .await
            .map_err(|e| AuthError::Validation(format!("fetch jwks: {e}")))?;
        let jwks: JwksDocument = response
            .json()
            .await
            .map_err(|e| AuthError::Validation(format!("decode jwks: {e}")))?;
        let mut by_kid = HashMap::new();
        for jwk in &jwks.keys {
            if let Some(decoder) = jwk_to_decoding_key(jwk)
                && let Some(kid) = jwk.kid.clone()
            {
                by_kid.insert(kid, decoder);
            }
        }
        let cached = CachedKeys {
            keys: by_kid,
            expires_at: Utc::now() + self.cache_ttl,
        };
        let key = cached
            .keys
            .get(kid)
            .cloned()
            .ok_or_else(|| AuthError::Unauthorized(format!("kid {kid} not in JWKS")))?;
        self.cache
            .write()
            .await
            .insert(provider.name.clone(), cached);
        Ok(key)
    }
}

/// Result of a successful external-OIDC validation.
#[derive(Debug, Clone, Serialize)]
pub struct ValidatedToken {
    pub provider: String,
    pub sub: String,
    pub claims: serde_json::Value,
}

/// Build the JWT `Validation` for a provider. `exp`, `iss`, `aud`, and
/// `nbf` (not-before) are all enforced; `algorithms` is pinned by the caller
/// from the JWKS/config, never from the attacker-controlled token header.
/// Factored out so the not-before enforcement is unit-testable without a
/// live JWKS fetch. `algorithms` must be non-empty (guaranteed by the
/// caller).
fn build_validation(provider: &OidcProvider, algorithms: Vec<Algorithm>) -> Validation {
    let mut validation = Validation::new(algorithms[0]);
    validation.algorithms = algorithms;
    validation.set_issuer(&[&provider.issuer]);
    // Enforce `nbf`: a token presented before its not-before time is
    // rejected. jsonwebtoken defaults `validate_nbf` to false; Python's
    // authlib validates nbf by default.
    validation.validate_nbf = true;
    // Non-empty audiences is guaranteed by ExternalOidcValidator::new.
    let refs: Vec<&str> = provider.audiences.iter().map(|s| s.as_str()).collect();
    validation.set_audience(&refs);
    validation
}

/// Returns the decoding key and the signing algorithm to use.
///
/// Algorithm priority: JWK `alg` field (IdP-declared) > kty-based
/// default (RS256 for RSA, ES256 for EC). The caller pins
/// `validation.algorithms` to this value so the token `alg` header
/// can never promote a weaker or wrong algorithm.
fn jwk_to_decoding_key(jwk: &Jwk) -> Option<(DecodingKey, Algorithm)> {
    if jwk.use_.as_deref() == Some("enc") {
        return None;
    }
    match jwk.kty.as_str() {
        "RSA" => {
            let (n, e) = (jwk.n.as_ref()?, jwk.e.as_ref()?);
            let key = DecodingKey::from_rsa_components(n, e).ok()?;
            let alg = jwk.alg.unwrap_or(Algorithm::RS256);
            Some((key, alg))
        }
        "EC" => {
            let (x, y) = (jwk.x.as_ref()?, jwk.y.as_ref()?);
            let key = DecodingKey::from_ec_components(x, y).ok()?;
            let alg = jwk.alg.unwrap_or(Algorithm::ES256);
            Some((key, alg))
        }
        _ => None,
    }
}

fn base64_url_decode(s: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|e| AuthError::Validation(format!("base64: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    fn test_provider() -> OidcProvider {
        OidcProvider {
            name: "test".into(),
            jwks_url: "https://example.test/jwks".into(),
            issuer: "https://issuer.test/".into(),
            audiences: vec!["tiled".into()],
            subject_claim: "sub".into(),
            algorithms: vec![Algorithm::HS256],
        }
    }

    fn hs256_token(nbf_offset_secs: i64, secret: &[u8]) -> String {
        let now = Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": "https://issuer.test/",
            "aud": "tiled",
            "sub": "alice",
            "exp": now + 3600,
            "nbf": now + nbf_offset_secs,
        });
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    /// Finding 4: `nbf` (not-before) must be enforced. A token presented
    /// before its not-before time is rejected; one past its nbf validates.
    #[test]
    fn validation_enforces_nbf() {
        let secret = b"unit-test-secret-for-nbf-check!!";
        let validation = build_validation(&test_provider(), vec![Algorithm::HS256]);
        assert!(validation.validate_nbf, "nbf enforcement must be enabled");

        let key = DecodingKey::from_secret(secret);

        // nbf an hour in the future (well beyond jsonwebtoken's default
        // leeway) → rejected.
        let future = hs256_token(3600, secret);
        assert!(
            decode::<serde_json::Value>(&future, &key, &validation).is_err(),
            "token presented before its nbf must be rejected"
        );

        // nbf in the past → accepted (iss/aud/exp all valid).
        let past = hs256_token(-60, secret);
        assert!(
            decode::<serde_json::Value>(&past, &key, &validation).is_ok(),
            "token past its nbf must validate"
        );
    }
}
