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
use crate::scopes::{Scope, ScopeSet};

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
    /// Entra-style scope translation. Maps an upstream OAuth2 scope (as it
    /// appears, space-separated, in the token's `scp` claim) to the set of
    /// tiled scopes it grants. Mirrors Python
    /// `EntraAuthenticator.scopes_map` (`authenticators.py:321,404-422`).
    ///
    /// - **Empty** (the default for a plain OIDC provider): no translation;
    ///   the session's scopes come solely from the principal's role.
    /// - **Non-empty** (Entra-style): the validator translates the token's
    ///   `scp` claim into tiled scopes (each `scp` entry looked up here;
    ///   unmapped entries are dropped). A token with **no** `scp` claim is
    ///   granted the union of every mapped scope — Entra would not have
    ///   issued the token had the user lacked the requested scopes.
    pub scopes_map: HashMap<String, Vec<Scope>>,
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

    /// Pre-seed the JWKS key cache so tests can skip the network fetch.
    /// The key is stored under `provider_name` / `kid` with the given
    /// algorithm. Call this on the validator before minting test tokens.
    ///
    /// Only intended for test helpers in dependent crates; not for
    /// production use.
    #[doc(hidden)]
    pub async fn inject_key_for_test(
        &self,
        provider_name: &str,
        kid: &str,
        key: DecodingKey,
        alg: Algorithm,
    ) {
        let mut cache_map = HashMap::new();
        cache_map.insert(kid.to_string(), (key, alg));
        let cached = CachedKeys {
            keys: cache_map,
            expires_at: Utc::now() + Duration::hours(1),
        };
        self.cache
            .write()
            .await
            .insert(provider_name.to_string(), cached);
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
        let scopes = if provider.scopes_map.is_empty() {
            None
        } else {
            Some(translate_scp(&provider.scopes_map, &claims))
        };
        Ok(ValidatedToken {
            provider: provider.name.clone(),
            sub,
            claims,
            scopes,
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
    /// Tiled scopes translated from the token's `scp` claim via the
    /// provider's `scopes_map`. `None` when the provider has no `scopes_map`
    /// (a plain OIDC provider) — the caller derives scopes from the
    /// principal's role alone. `Some(set)` for Entra-style providers — the
    /// caller unions these with the role scopes (Python `get_current_scopes`:
    /// `token_scopes | role_scopes`, `authentication.py:434`).
    pub scopes: Option<ScopeSet>,
}

/// Translate a token's `scp` claim into tiled scopes via a provider's
/// `scopes_map`. Mirrors `EntraAuthenticator.decode_token`
/// (`authenticators.py:404-422`):
///
/// - When the `scp` claim is present and non-empty, each space-separated
///   entry is looked up in `scopes_map`; mapped tiled scopes are unioned,
///   and an entry with no mapping is dropped (Python logs an "Unmapped Entra
///   scope" warning and continues).
/// - When the `scp` claim is absent or empty, **all** mapped scopes are
///   granted (the union of every `scopes_map` value): Entra would not have
///   issued the token if the user lacked the requested scopes.
fn translate_scp(scopes_map: &HashMap<String, Vec<Scope>>, claims: &serde_json::Value) -> ScopeSet {
    let scp_raw = claims.get("scp").and_then(|v| v.as_str()).unwrap_or("");
    let mut out = ScopeSet::new();
    if scp_raw.is_empty() {
        for mapped in scopes_map.values() {
            for &scope in mapped {
                out.insert(scope);
            }
        }
    } else {
        for entry in scp_raw.split(' ').filter(|s| !s.is_empty()) {
            match scopes_map.get(entry) {
                Some(mapped) => {
                    for &scope in mapped {
                        out.insert(scope);
                    }
                }
                None => {
                    tracing::warn!(
                        target: "tiled.auth",
                        "unmapped Entra scope in 'scp' claim: {entry}"
                    );
                }
            }
        }
    }
    out
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
            scopes_map: HashMap::new(),
        }
    }

    /// Build an HS256 token carrying a `kid` header (so `validate` can pick
    /// the injected key) and an optional `scp` claim.
    fn entra_token(secret: &[u8], kid: &str, scp: Option<&str>) -> String {
        let now = Utc::now().timestamp();
        let mut claims = serde_json::json!({
            "iss": "https://issuer.test/",
            "aud": "tiled",
            "sub": "alice",
            "exp": now + 3600,
            "nbf": now - 60,
        });
        if let Some(s) = scp {
            claims["scp"] = serde_json::Value::String(s.to_string());
        }
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());
        encode(&header, &claims, &EncodingKey::from_secret(secret)).unwrap()
    }

    async fn validator_with(
        scopes_map: HashMap<String, Vec<Scope>>,
        secret: &[u8],
        kid: &str,
    ) -> ExternalOidcValidator {
        let mut provider = test_provider();
        provider.scopes_map = scopes_map;
        let v = ExternalOidcValidator::new(vec![provider]).unwrap();
        v.inject_key_for_test(
            "test",
            kid,
            DecodingKey::from_secret(secret),
            Algorithm::HS256,
        )
        .await;
        v
    }

    fn entra_scopes_map() -> HashMap<String, Vec<Scope>> {
        let mut map = HashMap::new();
        map.insert(
            "api://app/read".to_string(),
            vec![Scope::ReadMetadata, Scope::ReadData],
        );
        map.insert("api://app/write".to_string(), vec![Scope::WriteData]);
        map
    }

    /// #1360: `translate_scp` mirrors `EntraAuthenticator.decode_token`
    /// (`authenticators.py:404-422`) at its three boundaries: a present `scp`
    /// grants only mapped entries (unmapped dropped); an absent OR empty `scp`
    /// grants the union of every mapped scope.
    #[test]
    fn translate_scp_boundaries() {
        let map = entra_scopes_map();

        // Present scp: mapped entry grants its scopes; unmapped entry dropped.
        let granted = translate_scp(&map, &serde_json::json!({"scp": "api://app/read bogus"}));
        assert!(granted.contains(Scope::ReadMetadata));
        assert!(granted.contains(Scope::ReadData));
        assert!(
            !granted.contains(Scope::WriteData),
            "an ungranted/unmapped scp entry must not leak a scope"
        );

        // Absent scp: union of all mapped scopes.
        let all = translate_scp(&map, &serde_json::json!({}));
        assert!(all.contains(Scope::ReadMetadata));
        assert!(all.contains(Scope::ReadData));
        assert!(all.contains(Scope::WriteData));

        // Empty scp string is treated like absent (Python: `if scp_raw:`).
        let empty = translate_scp(&map, &serde_json::json!({"scp": ""}));
        assert!(empty.contains(Scope::WriteData));
    }

    /// #1360: an Entra-style provider (non-empty `scopes_map`) translates the
    /// token's `scp` claim end-to-end through `validate`.
    #[tokio::test]
    async fn validate_translates_entra_scp_to_tiled_scopes() {
        let secret = b"entra-scope-translation-secret!!";
        let v = validator_with(entra_scopes_map(), secret, "k1").await;
        let token = entra_token(secret, "k1", Some("api://app/read api://app/unknown"));

        let scopes = v
            .validate(&token)
            .await
            .unwrap()
            .scopes
            .expect("provider with scopes_map must translate scopes");
        assert!(scopes.contains(Scope::ReadMetadata));
        assert!(scopes.contains(Scope::ReadData));
        assert!(
            !scopes.contains(Scope::WriteData),
            "scopes not granted by scp must not appear"
        );
    }

    /// #1360: a token with no `scp` claim is granted the union of all mapped
    /// scopes (Entra would not have issued it without the requested grants).
    #[tokio::test]
    async fn validate_without_scp_grants_all_mapped_scopes() {
        let secret = b"entra-no-scp-claim-secret-bytes!";
        let v = validator_with(entra_scopes_map(), secret, "k1").await;
        let token = entra_token(secret, "k1", None);

        let scopes = v.validate(&token).await.unwrap().scopes.unwrap();
        assert!(scopes.contains(Scope::ReadMetadata));
        assert!(scopes.contains(Scope::ReadData));
        assert!(scopes.contains(Scope::WriteData));
    }

    /// A plain OIDC provider (empty `scopes_map`) performs no translation;
    /// `ValidatedToken.scopes` is `None` and the caller falls back to role
    /// scopes alone — current behaviour is preserved.
    #[tokio::test]
    async fn validate_plain_provider_yields_no_token_scopes() {
        let secret = b"plain-oidc-provider-secret-bytes";
        let v = validator_with(HashMap::new(), secret, "k2").await;
        let token = entra_token(secret, "k2", Some("api://app/read"));

        assert!(
            v.validate(&token).await.unwrap().scopes.is_none(),
            "a provider without a scopes_map must not translate token scopes"
        );
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
