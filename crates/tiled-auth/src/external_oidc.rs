//! External OIDC bearer-token validation + authorization-code flow (#1178).
//!
//! ## Bearer validation
//!
//! Plugs Microsoft Entra ID, Auth0, Keycloak, etc. in front of tiled
//! without storing user credentials in the auth DB. The server fetches
//! the IdP's JWKS once (cached for the lifetime of the process by
//! default), and verifies incoming bearer JWTs against the matching
//! public key. Successful validation produces a `(provider, sub)`
//! identity that `AuthDb::ensure_principal` upserts on first sight.
//!
//! Mirrors tiled#1364 (EntraAuthenticator) + tiled#1343
//! (ProxiedOIDCAuthenticator).
//!
//! ## Authorization-code + PKCE flow (#1178)
//!
//! `OidcProvider` may carry code-flow config (`client_id`,
//! `authorization_endpoint`, `token_endpoint`). When those fields are
//! set, `ExternalOidcValidator::build_authorize_url` builds a
//! browser-redirect URL with PKCE S256 + nonce (OIDC Core §3.1.2.1) and
//! returns an [`AuthorizeRedirect`] carrying the verifier/nonce/state the
//! caller persists. After the IdP redirects back with `?code=…&state=…`,
//! `exchange_code_flow` (given the recovered [`crate::OidcFlowState`])
//! exchanges the code at `token_endpoint`, validates the returned `id_token`
//! (same JWKS machinery + nonce check per OIDC Core §3.1.3.7 #11), and returns
//! the principal identity.
//!
//! **Pending-state ownership (G6)**: the PKCE state between `/authorize` and
//! `/callback` lives in the auth DB ([`crate::AuthDb::create_oidc_flow_state`]
//! / [`crate::AuthDb::take_oidc_flow_state`]), NOT in this validator. That makes
//! the flow survive restarts and multi-process / load-balanced deployments
//! where the two requests land on different processes. This validator is
//! stateless w.r.t. the flow — it only builds URLs and exchanges codes.

#![cfg(feature = "oidc")]

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::error::{AuthError, Result};
use crate::oidc_flow::OidcFlowState;
use crate::scopes::{Scope, ScopeSet};

/// How a provider derives the principal subject (and username) from a verified
/// token. Mirrors the split between Python's `OIDCAuthenticator` and
/// `EntraAuthenticator` (`authenticators.py`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdentityMapping {
    /// The subject is the provider's `subject_claim` value verbatim (plain
    /// OIDC). No username is derived.
    #[default]
    Standard,
    /// Microsoft Entra ID: the subject is a stable
    /// `uuid5(NAMESPACE_URL, "{iss}|{sub}")` (the raw Entra `sub` is opaque and
    /// per-application, so it is not a safe cross-tenant identity), and a
    /// human-readable username is derived from the token claims (nameID /
    /// preferred_username / upn / email). The returned `claims` are enriched
    /// with `entra_sub` / `entra_username` / `user`, mirroring
    /// `EntraAuthenticator.decode_token`.
    Entra,
}

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
    /// to `sub`; some IdPs prefer `oid` (Entra) or `email`. Ignored when
    /// `identity_mapping` is [`IdentityMapping::Entra`] (which always derives
    /// the subject from the `sub` claim via uuid5).
    pub subject_claim: String,
    /// How the principal subject (and username) is derived from a verified
    /// token. [`IdentityMapping::Standard`] uses `subject_claim` verbatim;
    /// [`IdentityMapping::Entra`] derives a stable uuid5 subject plus a
    /// human-readable username. Defaults to `Standard`.
    pub identity_mapping: IdentityMapping,
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

    // ---- Authorization-code + PKCE flow (optional, tiled#1178) ----
    //
    // When `client_id`, `authorization_endpoint`, and `token_endpoint` are
    // all `Some`, the provider also supports the browser-redirect
    // authorization-code flow. `None` on any keeps it bearer-only.
    /// OAuth2 `client_id` registered at the IdP. Required for code flow.
    /// `None` = bearer-only provider.
    pub client_id: Option<String>,
    /// OAuth2 `client_secret`. `None` or `Some("")` → PKCE-only public-
    /// client mode (no secret sent to the token endpoint). Confidential
    /// clients set this alongside PKCE.
    pub client_secret: Option<String>,
    /// IdP's authorization endpoint URL. Required for code flow.
    pub authorization_endpoint: Option<String>,
    /// IdP's token endpoint URL. Required for code flow.
    pub token_endpoint: Option<String>,
    /// Extra OAuth2 scopes appended to the `openid offline_access` baseline in
    /// every authorization-code token POST (both the browser and device flows).
    /// This is the Entra resource-scope mechanism (e.g.
    /// `["api://<client-id>/access_as_user"]`): Entra only issues an
    /// `access_token` whose `aud` matches the requested resource scope, so any
    /// scope a downstream OBO exchange uses as its `assertion` audience MUST be
    /// requested here — requesting it only on the authorize redirect is not
    /// sufficient (Entra does not carry redirect scopes into the token POST).
    /// Empty for a plain OIDC provider. Mirrors Python
    /// `EntraAuthenticator.extra_scopes` (authenticators.py:326,460) consumed by
    /// `exchange_code` (authenticators.py:522-532).
    pub extra_scopes: Vec<String>,
    /// IdP's RP-initiated-logout (end-session) endpoint from OIDC discovery
    /// (`end_session_endpoint`). Advertised to clients as
    /// `authentication.links.logout` so they can end the upstream IdP session
    /// (OIDC RP-Initiated Logout 1.0). `None` → clients use tiled's own logout
    /// route. Mirrors Python `OIDCAuthenticator.end_session_endpoint`
    /// (authenticators.py:203).
    pub end_session_endpoint: Option<String>,
    /// After a successful code-flow callback, redirect the browser here
    /// with `access_token` and `refresh_token` as query params.
    /// `None` → return the tokens as JSON (API-client mode).
    /// Mirrors Python `OIDCAuthenticator.redirect_on_success`.
    pub redirect_on_success: Option<String>,
    /// On authentication failure in the code-flow callback, redirect here.
    /// `None` → return HTTP 401 JSON.
    pub redirect_on_failure: Option<String>,
}

/// Endpoints discovered from an OpenID Connect Discovery document
/// (`<issuer>/.well-known/openid-configuration`, OIDC Discovery 1.0 §3/§4).
///
/// Lets an operator configure a single `well_known_uri` instead of spelling
/// out `issuer`, `jwks_url`, `authorization_endpoint`, and `token_endpoint`
/// by hand — mirrors Python tiled's `OIDCAuthenticator(well_known_uri=…)`,
/// which derives the same endpoints from this document. Only the fields the
/// validator needs are captured; every other member is ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcDiscovery {
    /// REQUIRED. The provider's issuer identifier; must equal the `iss` claim
    /// of tokens it signs (becomes [`OidcProvider::issuer`]).
    pub issuer: String,
    /// REQUIRED. URL of the provider's JWKS — its signing keys
    /// (becomes [`OidcProvider::jwks_url`]).
    pub jwks_uri: String,
    /// REQUIRED for the authorization-code flow; some bearer-only providers
    /// omit it. Populates [`OidcProvider::authorization_endpoint`].
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    /// REQUIRED unless the provider only supports the implicit flow.
    /// Populates [`OidcProvider::token_endpoint`].
    #[serde(default)]
    pub token_endpoint: Option<String>,
    /// OPTIONAL. The provider's RP-initiated-logout endpoint (OIDC
    /// RP-Initiated Logout 1.0). Populates
    /// [`OidcProvider::end_session_endpoint`]; advertised as
    /// `authentication.links.logout`.
    #[serde(default)]
    pub end_session_endpoint: Option<String>,
}

/// Fetch and parse an OIDC Discovery document from `well_known_uri`.
///
/// `well_known_uri` must be the full discovery URL — typically
/// `<issuer>/.well-known/openid-configuration` — matching Python tiled's
/// `OIDCAuthenticator(well_known_uri=…)` (it is requested verbatim, not
/// derived from an issuer base). The returned endpoints build an
/// [`OidcProvider`] so operators configure one URL instead of four.
pub async fn discover_oidc(well_known_uri: &str) -> Result<OidcDiscovery> {
    let http = reqwest::Client::new();
    let resp = http.get(well_known_uri).send().await.map_err(|e| {
        AuthError::Validation(format!("OIDC discovery fetch '{well_known_uri}': {e}"))
    })?;
    let status = resp.status();
    if !status.is_success() {
        return Err(AuthError::Validation(format!(
            "OIDC discovery '{well_known_uri}' returned HTTP {status}"
        )));
    }
    let body = resp.bytes().await.map_err(|e| {
        AuthError::Validation(format!("OIDC discovery '{well_known_uri}' read body: {e}"))
    })?;
    parse_oidc_discovery(&body)
        .map_err(|e| AuthError::Validation(format!("OIDC discovery '{well_known_uri}': {e}")))
}

/// Parse a discovery-document body. Split out from [`discover_oidc`] so the
/// JSON shape is unit-testable without a network fetch.
fn parse_oidc_discovery(body: &[u8]) -> std::result::Result<OidcDiscovery, serde_json::Error> {
    serde_json::from_slice(body)
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

// ---------------------------------------------------------------------------
// Authorization-code redirect
// ---------------------------------------------------------------------------

/// Result of [`ExternalOidcValidator::build_authorize_url`]: the redirect URL
/// to send the browser to, plus the server-side PKCE state the caller MUST
/// persist (via [`crate::AuthDb::create_oidc_flow_state`], keyed by `state`) so
/// the `/callback` can recover it. Persistence is the DB's job — the single
/// owner of pending-flow state (G6) — not this validator's.
///
/// The `state` is a cryptographically random token, so guessing it is
/// infeasible; it doubles as CSRF protection per OAuth 2.0 §10.12 /
/// RFC 6819 §4.4.1.8.
#[derive(Debug, Clone)]
pub struct AuthorizeRedirect {
    /// Full IdP authorization URL (becomes the `Location:` on the 302).
    pub url: String,
    /// Random OAuth2 `state` — CSRF token and the DB lookup key. Only its hash
    /// is persisted; the raw value is echoed back by the browser on callback.
    pub state: String,
    /// PKCE code verifier (43 base64url chars, RFC 7636 §4.1) to persist; sent
    /// to the token endpoint so the IdP can verify the S256 `code_challenge`.
    pub code_verifier: String,
    /// OIDC nonce to persist; checked against the returned `id_token`
    /// (OIDC Core §3.1.3.7 #11).
    pub nonce: String,
}

// ---------------------------------------------------------------------------
// ExternalOidcValidator
// ---------------------------------------------------------------------------

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

    /// Return the configured providers. Used by server routes that need
    /// to look up per-provider code-flow config (e.g. `redirect_on_success`).
    pub fn providers(&self) -> &[OidcProvider] {
        &self.providers
    }

    // ---------------------------------------------------------------------------
    // Bearer validation
    // ---------------------------------------------------------------------------

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
        finalize_token(provider, claims)
    }

    // ---------------------------------------------------------------------------
    // Authorization-code + PKCE flow (#1178)
    // ---------------------------------------------------------------------------

    /// Build the authorization redirect URL for a code-flow provider.
    ///
    /// Generates a PKCE code verifier + S256 challenge, a random OAuth2
    /// `state` parameter, and a random OIDC `nonce`. Returns an
    /// [`AuthorizeRedirect`] carrying the URL (to send as `Location: …` in a
    /// 302) plus the verifier/nonce/state; the caller persists that state via
    /// [`crate::AuthDb::create_oidc_flow_state`] so `exchange_code_flow` can
    /// recover it on the callback. This method does no persistence itself.
    pub fn build_authorize_url(
        &self,
        provider_name: &str,
        redirect_uri: &str,
    ) -> Result<AuthorizeRedirect> {
        let provider = self
            .providers
            .iter()
            .find(|p| p.name == provider_name)
            .ok_or_else(|| {
                AuthError::Validation(format!("unknown OIDC provider '{provider_name}'"))
            })?;
        let client_id = provider.client_id.as_deref().ok_or_else(|| {
            AuthError::Validation(format!(
                "provider '{provider_name}' is not configured for the authorization-code flow \
                 (client_id is required)"
            ))
        })?;
        let auth_endpoint = provider.authorization_endpoint.as_deref().ok_or_else(|| {
            AuthError::Validation(format!(
                "provider '{provider_name}': authorization_endpoint is not configured"
            ))
        })?;

        let (code_verifier, code_challenge) = gen_pkce_pair();
        let nonce = gen_nonce();
        let state = gen_state();

        let mut url = reqwest::Url::parse(auth_endpoint).map_err(|e| {
            AuthError::Validation(format!("bad authorization_endpoint '{auth_endpoint}': {e}"))
        })?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("scope", "openid offline_access")
            .append_pair("state", &state)
            .append_pair("code_challenge", &code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("nonce", &nonce)
            .append_pair("prompt", "login");

        Ok(AuthorizeRedirect {
            url: url.to_string(),
            state,
            code_verifier,
            nonce,
        })
    }

    /// Exchange an authorization code for tokens, validate the `id_token`,
    /// and return the principal identity.
    ///
    /// `flow` is the server-side PKCE state the caller recovered (and consumed)
    /// from [`crate::AuthDb::take_oidc_flow_state`] for this callback's `state`
    /// — that store enforces single-use and expiry, so by the time we are here
    /// the state is valid and already removed. This method holds no flow state
    /// itself.
    pub async fn exchange_code_flow(
        &self,
        flow: &OidcFlowState,
        code: &str,
        redirect_uri: &str,
    ) -> Result<CodeFlowSession> {
        let provider = self
            .providers
            .iter()
            .find(|p| p.name == flow.provider)
            .ok_or_else(|| {
                AuthError::Validation(format!(
                    "OIDC provider '{}' disappeared from config",
                    flow.provider
                ))
            })?;

        let client_id = provider.client_id.as_deref().ok_or_else(|| {
            AuthError::Validation("client_id not configured for code flow".into())
        })?;
        let token_endpoint = provider.token_endpoint.as_deref().ok_or_else(|| {
            AuthError::Validation("token_endpoint not configured for code flow".into())
        })?;

        // Build the token-endpoint form. Always include PKCE verifier; add
        // client_secret only when non-empty (confidential-client mode).
        let secret_clone = provider.client_secret.clone().filter(|s| !s.is_empty());
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "authorization_code"),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
            ("code", code),
            ("code_verifier", &flow.code_verifier),
        ];
        if let Some(ref secret) = secret_clone {
            form.push(("client_secret", secret.as_str()));
        }

        let body = self
            .post_token_request(token_endpoint, &form, &provider.extra_scopes)
            .await?;

        let id_token = body["id_token"].as_str().ok_or_else(|| {
            AuthError::Validation("token response is missing the 'id_token' field".into())
        })?;

        let token = self
            .validate_id_token(provider, id_token, Some(&flow.nonce))
            .await?;
        // Capture the upstream IdP tokens for OBO (Entra only; `{}` otherwise),
        // mirroring EntraAuthenticator.authenticate (authenticators.py:497-502).
        let session_state = build_session_state(provider, &body);
        Ok(CodeFlowSession {
            token,
            session_state,
        })
    }

    /// POST a form to a provider's `token_endpoint`, parse the JSON body, and
    /// surface a non-2xx response as `Unauthorized`. Shared by the PKCE browser
    /// flow ([`Self::exchange_code_flow`]) and the IdP-brokered device flow
    /// ([`Self::exchange_device_code`]) — the per-flow forms differ only in the
    /// PKCE `code_verifier` (browser-only).
    ///
    /// This single owner of every authorization-code exchange explicitly sets
    /// the token-POST `scope` to `openid offline_access` plus the provider's
    /// [`OidcProvider::extra_scopes`]. `offline_access` makes the IdP return a
    /// `refresh_token` unconditionally (required for the G3 OBO refresh, which
    /// renews the upstream tokens silently); the extra scopes are the Entra
    /// resource scopes a downstream OBO exchange needs in the `access_token`
    /// `aud`. Setting it here (not at each caller) keeps the rule uniform and
    /// makes it impossible for a code-exchange path to omit it. Safe even when
    /// the authorize URL already requested these scopes (the IdP ignores
    /// duplicates). Mirrors Python `exchange_code` (`authenticators.py:530`),
    /// which builds the same `" ".join(sorted(scopes))` for both flows.
    async fn post_token_request(
        &self,
        token_endpoint: &str,
        form: &[(&str, &str)],
        extra_scopes: &[String],
    ) -> Result<serde_json::Value> {
        let scope = build_token_scope(extra_scopes);
        let mut form = form.to_vec();
        form.push(("scope", &scope));
        let resp = self
            .http
            .post(token_endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|e| AuthError::Validation(format!("token endpoint request failed: {e}")))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| AuthError::Validation(format!("failed to parse token response: {e}")))?;

        if !status.is_success() {
            let err_msg = body["error"].as_str().unwrap_or("unknown_error");
            let err_desc = body["error_description"].as_str().unwrap_or("");
            return Err(AuthError::Unauthorized(format!(
                "token endpoint returned {status}: {err_msg} — {err_desc}"
            )));
        }
        Ok(body)
    }

    /// Build the IdP authorization URL for the **device** flow. Unlike
    /// [`Self::build_authorize_url`] (PKCE browser flow) this carries no PKCE
    /// challenge, no nonce, and no server-side pending state — the device flow
    /// tracks its own state in the `pending_sessions` table, and the redirect
    /// lands on the `/device_code` route where the user enters their user_code.
    /// Mirrors Python `device_code_authorize_route` (authentication.py:991).
    pub fn build_device_authorize_url(
        &self,
        provider_name: &str,
        redirect_uri: &str,
    ) -> Result<String> {
        let provider = self
            .providers
            .iter()
            .find(|p| p.name == provider_name)
            .ok_or_else(|| {
                AuthError::Validation(format!("unknown OIDC provider '{provider_name}'"))
            })?;
        let client_id = provider.client_id.as_deref().ok_or_else(|| {
            AuthError::Validation(format!(
                "provider '{provider_name}' is not configured for the device flow \
                 (client_id is required)"
            ))
        })?;
        let auth_endpoint = provider.authorization_endpoint.as_deref().ok_or_else(|| {
            AuthError::Validation(format!(
                "provider '{provider_name}': authorization_endpoint is not configured"
            ))
        })?;

        let mut url = reqwest::Url::parse(auth_endpoint).map_err(|e| {
            AuthError::Validation(format!("bad authorization_endpoint '{auth_endpoint}': {e}"))
        })?;
        url.query_pairs_mut()
            .append_pair("client_id", client_id)
            .append_pair("response_type", "code")
            .append_pair("scope", "openid")
            .append_pair("redirect_uri", redirect_uri);
        Ok(url.to_string())
    }

    /// Exchange an authorization code obtained via the IdP-brokered **device**
    /// flow for tokens, validate the id_token (no nonce — see
    /// [`Self::validate_id_token`]), and return the principal identity plus OBO
    /// session state. Mirrors Python `OIDCAuthenticator.authenticate`
    /// (authenticators.py:222) as used by the device-code submit route.
    ///
    /// `redirect_uri` MUST equal the one sent in
    /// [`Self::build_device_authorize_url`] (the `/device_code` route URL); the
    /// token endpoint validates that they match.
    pub async fn exchange_device_code(
        &self,
        provider_name: &str,
        code: &str,
        redirect_uri: &str,
    ) -> Result<CodeFlowSession> {
        let provider = self
            .providers
            .iter()
            .find(|p| p.name == provider_name)
            .ok_or_else(|| {
                AuthError::Validation(format!("unknown OIDC provider '{provider_name}'"))
            })?;
        let client_id = provider.client_id.as_deref().ok_or_else(|| {
            AuthError::Validation("client_id not configured for device flow".into())
        })?;
        let token_endpoint = provider.token_endpoint.as_deref().ok_or_else(|| {
            AuthError::Validation("token_endpoint not configured for device flow".into())
        })?;

        // No PKCE verifier (the device authorize URL carries no challenge);
        // add client_secret only in confidential-client mode.
        let secret_clone = provider.client_secret.clone().filter(|s| !s.is_empty());
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "authorization_code"),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
            ("code", code),
        ];
        if let Some(ref secret) = secret_clone {
            form.push(("client_secret", secret.as_str()));
        }

        let body = self
            .post_token_request(token_endpoint, &form, &provider.extra_scopes)
            .await?;

        let id_token = body["id_token"].as_str().ok_or_else(|| {
            AuthError::Validation("token response is missing the 'id_token' field".into())
        })?;

        let token = self.validate_id_token(provider, id_token, None).await?;
        // Capture the upstream IdP tokens for OBO (Entra only; `{}` otherwise),
        // identical to the browser flow.
        let session_state = build_session_state(provider, &body);
        Ok(CodeFlowSession {
            token,
            session_state,
        })
    }

    /// Validate an id_token returned from the token endpoint.
    ///
    /// Applies the same iss / aud / exp / nbf / alg checks as bearer
    /// validation. When `expected_nonce` is `Some`, also enforces the `nonce`
    /// claim (OIDC Core §3.1.3.7 #11) — the PKCE browser flow passes the nonce
    /// it sent in the authorize request. The IdP-brokered device flow has no
    /// nonce round-trip (Python's device flow omits it too), so it passes
    /// `None` and the nonce check is skipped. The `kid` header field is
    /// required; IdPs that omit it are not supported.
    async fn validate_id_token(
        &self,
        provider: &OidcProvider,
        id_token: &str,
        expected_nonce: Option<&str>,
    ) -> Result<ValidatedToken> {
        let header = decode_header(id_token).map_err(AuthError::from)?;
        let kid = header.kid.ok_or_else(|| {
            AuthError::Unauthorized(
                "id_token has no 'kid' header — the IdP must include a key ID for \
                 signature-key selection; kid-less id_tokens are not supported"
                    .into(),
            )
        })?;

        let (key, jwk_alg) = self.fetch_key(provider, &kid).await?;
        let algorithms = if provider.algorithms.is_empty() {
            vec![jwk_alg]
        } else {
            provider.algorithms.clone()
        };
        let validation = build_validation(provider, algorithms);

        let claims = decode::<serde_json::Value>(id_token, &key, &validation)
            .map_err(AuthError::from)?
            .claims;

        // OIDC Core §3.1.3.7 #11: when a nonce was sent (PKCE browser flow),
        // it MUST be present and MUST equal the value sent in the authorization
        // request. The device flow sends no nonce and passes `None` here.
        if let Some(expected_nonce) = expected_nonce {
            let nonce_claim = claims
                .get("nonce")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    AuthError::Unauthorized("id_token is missing the 'nonce' claim".into())
                })?;
            if nonce_claim != expected_nonce {
                return Err(AuthError::Unauthorized(
                    "id_token nonce does not match the nonce sent in the authorization \
                     request — possible replay or CSRF attempt"
                        .into(),
                ));
            }
        }

        finalize_token(provider, claims)
    }

    // ---------------------------------------------------------------------------
    // JWKS key cache
    // ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Public result type
// ---------------------------------------------------------------------------

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

/// Outcome of a completed authorization-code exchange ([`ExternalOidcValidator::exchange_code_flow`]):
/// the validated identity plus the OBO session state to persist.
///
/// `session_state` is a JSON object embedded verbatim in the resulting tiled
/// access token's `state` claim (mirrors Python `UserSessionState.state` →
/// `Session.state`). For an [`IdentityMapping::Entra`] provider it carries the
/// upstream `entra_access_token` / `entra_refresh_token` (when the IdP returned
/// them) so downstream services can perform on-behalf-of exchanges; for every
/// other provider it is the empty object `{}`.
#[derive(Debug, Clone)]
pub struct CodeFlowSession {
    pub token: ValidatedToken,
    pub session_state: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the [`ValidatedToken`] from verified `claims`. The single owner of
/// identity derivation, shared by the bearer ([`ExternalOidcValidator::validate`])
/// and code-flow ([`ExternalOidcValidator::validate_id_token`]) paths so the
/// provider's [`IdentityMapping`] applies uniformly to both — no path can
/// derive the subject differently.
fn finalize_token(
    provider: &OidcProvider,
    mut claims: serde_json::Value,
) -> Result<ValidatedToken> {
    // Compute scopes from the original `scp` claim before any Entra enrichment
    // mutates `claims` (enrichment never touches `scp`, but keep it explicit).
    let scopes = if provider.scopes_map.is_empty() {
        None
    } else {
        Some(translate_scp(&provider.scopes_map, &claims))
    };
    let sub = match provider.identity_mapping {
        IdentityMapping::Standard => claims
            .get(&provider.subject_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AuthError::Unauthorized(format!("token missing claim '{}'", provider.subject_claim))
            })?
            .to_string(),
        IdentityMapping::Entra => derive_entra_identity(&mut claims)?,
    };
    Ok(ValidatedToken {
        provider: provider.name.clone(),
        sub,
        claims,
        scopes,
    })
}

/// Entra identity derivation. Mirrors `EntraAuthenticator.decode_token`
/// (`authenticators.py:354-402`): replace the opaque per-tenant `sub` with a
/// stable `uuid5(NAMESPACE_URL, "{iss}|{sub}")` (`.hex` form — 32 lowercase hex
/// digits, no dashes), and derive a human-readable username, enriching `claims`
/// with `entra_sub` / `entra_username` / `user` the same way upstream does.
/// Returns the derived (uuid5) subject.
fn derive_entra_identity(claims: &mut serde_json::Value) -> Result<String> {
    let original_sub = claims
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AuthError::Unauthorized("Entra token missing 'sub' claim".into()))?
        .to_string();
    let issuer = claims
        .get("iss")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let uuid_sub = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{issuer}|{original_sub}").as_bytes(),
    )
    .simple()
    .to_string();

    // Human-readable username: nameID → preferred_username → upn → email,
    // normalized; fall back to the original opaque sub when none is present.
    let entra_username = entra_username_claim(claims);
    let user = match entra_username.as_deref() {
        Some(raw) => normalize_entra_username(raw),
        None => {
            tracing::warn!(
                sub = %original_sub,
                "EntraAuthenticator: no human-readable username claim found \
                 (checked nameID, preferred_username, upn, email); falling back to Entra sub"
            );
            original_sub.clone()
        }
    };

    if let Some(obj) = claims.as_object_mut() {
        obj.insert("sub".into(), serde_json::Value::String(uuid_sub.clone()));
        obj.insert("entra_sub".into(), serde_json::Value::String(original_sub));
        if let Some(eu) = entra_username {
            obj.insert("entra_username".into(), serde_json::Value::String(eu));
        }
        obj.insert("user".into(), serde_json::Value::String(user));
    }
    Ok(uuid_sub)
}

/// First non-empty Entra username claim, in Python's priority order
/// (`authenticators.py:375-380`): nameID → preferred_username → upn → email.
/// Empty strings are skipped (they are falsy in Python's `or` chain).
fn entra_username_claim(claims: &serde_json::Value) -> Option<String> {
    ["nameID", "preferred_username", "upn", "email"]
        .iter()
        .find_map(|k| {
            claims
                .get(*k)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
        })
        .map(str::to_string)
}

/// Normalize a raw username claim the way Python does
/// (`authenticators.py:382-387`): strip surrounding whitespace, then reduce
/// `DOMAIN\user` → `user` (after the last backslash) or `user@domain` → `user`
/// (before the first `@`). Backslash takes precedence over `@`.
fn normalize_entra_username(raw: &str) -> String {
    let trimmed = raw.trim();
    if let Some(idx) = trimmed.rfind('\\') {
        trimmed[idx + 1..].to_string()
    } else if let Some(idx) = trimmed.find('@') {
        trimmed[..idx].to_string()
    } else {
        trimmed.to_string()
    }
}

/// Build the `scope` value for an authorization-code token POST: the
/// `openid offline_access` baseline unioned with `extra_scopes`, then sorted and
/// space-joined. Mirrors Python `exchange_code`'s
/// `" ".join(sorted({"openid", "offline_access"} | extra_scopes))`
/// (authenticators.py:530-542) — a [`BTreeSet`] gives the same sorted, de-
/// duplicated set (a duplicate `openid`/`offline_access` in `extra_scopes`
/// collapses, exactly as Python's set does).
fn build_token_scope(extra_scopes: &[String]) -> String {
    let mut scopes: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    scopes.insert("openid");
    scopes.insert("offline_access");
    for s in extra_scopes {
        scopes.insert(s.as_str());
    }
    scopes.into_iter().collect::<Vec<_>>().join(" ")
}

/// Build the OBO session state from a code-flow token-endpoint response.
///
/// Mirrors `EntraAuthenticator.authenticate` (authenticators.py:497-502): for
/// an [`IdentityMapping::Entra`] provider, store whichever of
/// `access_token` / `refresh_token` the IdP returned under
/// `entra_access_token` / `entra_refresh_token`. Every other provider — and a
/// response lacking those fields — yields the empty object `{}` (Python's
/// `OIDCAuthenticator.authenticate` returns an empty state). The single owner
/// of OBO-state derivation; no other code constructs these keys.
fn build_session_state(
    provider: &OidcProvider,
    token_response: &serde_json::Value,
) -> serde_json::Value {
    let mut state = serde_json::Map::new();
    if provider.identity_mapping == IdentityMapping::Entra {
        if let Some(at) = token_response.get("access_token").and_then(|v| v.as_str()) {
            state.insert(
                "entra_access_token".into(),
                serde_json::Value::String(at.to_string()),
            );
        }
        if let Some(rt) = token_response.get("refresh_token").and_then(|v| v.as_str()) {
            state.insert(
                "entra_refresh_token".into(),
                serde_json::Value::String(rt.to_string()),
            );
        }
    }
    serde_json::Value::Object(state)
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

// ---------------------------------------------------------------------------
// PKCE helpers
// ---------------------------------------------------------------------------

/// Generate a PKCE code verifier (43 URL-safe base64 chars, RFC 7636 §4.1)
/// and its S256 code challenge (SHA-256 of the verifier, base64url-encoded).
fn gen_pkce_pair() -> (String, String) {
    use base64::Engine;
    let buf: [u8; 32] = rand::random();
    let verifier = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf);
    let hash = Sha256::digest(verifier.as_bytes());
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);
    (verifier, challenge)
}

/// Generate a cryptographically random OIDC nonce (base64url of 16 bytes).
fn gen_nonce() -> String {
    use base64::Engine;
    let buf: [u8; 16] = rand::random();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Generate a cryptographically random OAuth2 `state` parameter
/// (base64url of 16 bytes).
fn gen_state() -> String {
    use base64::Engine;
    let buf: [u8; 16] = rand::random();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    #[test]
    fn parse_oidc_discovery_extracts_endpoints_and_ignores_extra() {
        // A representative `.well-known/openid-configuration` body with the
        // four fields we read plus extra members an IdP advertises.
        let body = br#"{
            "issuer": "https://idp.test/",
            "authorization_endpoint": "https://idp.test/authorize",
            "token_endpoint": "https://idp.test/token",
            "end_session_endpoint": "https://idp.test/logout",
            "jwks_uri": "https://idp.test/keys",
            "userinfo_endpoint": "https://idp.test/userinfo",
            "response_types_supported": ["code"],
            "id_token_signing_alg_values_supported": ["RS256"]
        }"#;
        let d = parse_oidc_discovery(body).expect("well-formed discovery doc must parse");
        assert_eq!(d.issuer, "https://idp.test/");
        assert_eq!(d.jwks_uri, "https://idp.test/keys");
        assert_eq!(
            d.authorization_endpoint.as_deref(),
            Some("https://idp.test/authorize")
        );
        assert_eq!(d.token_endpoint.as_deref(), Some("https://idp.test/token"));
        assert_eq!(
            d.end_session_endpoint.as_deref(),
            Some("https://idp.test/logout")
        );
    }

    #[test]
    fn parse_oidc_discovery_optional_endpoints_default_none() {
        // A bearer-only provider may omit authorization/token/end-session.
        let body = br#"{"issuer":"https://idp.test/","jwks_uri":"https://idp.test/keys"}"#;
        let d = parse_oidc_discovery(body).expect("issuer + jwks_uri suffice");
        assert_eq!(d.issuer, "https://idp.test/");
        assert_eq!(d.jwks_uri, "https://idp.test/keys");
        assert!(d.authorization_endpoint.is_none());
        assert!(d.token_endpoint.is_none());
        assert!(d.end_session_endpoint.is_none());
    }

    #[test]
    fn parse_oidc_discovery_rejects_missing_jwks_uri() {
        // `jwks_uri` is required — without it the validator has no keys.
        let body = br#"{"issuer":"https://idp.test/"}"#;
        assert!(
            parse_oidc_discovery(body).is_err(),
            "a discovery doc without jwks_uri must be rejected"
        );
    }

    // --- Entra identity mapping (G2) ---

    #[test]
    fn entra_username_claim_priority_and_empty_skipped() {
        let c = serde_json::json!({"preferred_username":"pref","upn":"upnval","email":"e@x"});
        assert_eq!(entra_username_claim(&c).as_deref(), Some("pref"));
        let c2 = serde_json::json!({"upn":"upnval","email":"e@x"});
        assert_eq!(entra_username_claim(&c2).as_deref(), Some("upnval"));
        let c3 = serde_json::json!({"email":"e@x.com"});
        assert_eq!(entra_username_claim(&c3).as_deref(), Some("e@x.com"));
        // nameID has the highest priority.
        let c4 = serde_json::json!({"nameID":"nm","preferred_username":"pref"});
        assert_eq!(entra_username_claim(&c4).as_deref(), Some("nm"));
        // Empty strings are falsy → skipped in favour of the next claim.
        let c5 = serde_json::json!({"preferred_username":"","upn":"realupn"});
        assert_eq!(entra_username_claim(&c5).as_deref(), Some("realupn"));
        // No username claim at all.
        let c6 = serde_json::json!({"sub":"x"});
        assert!(entra_username_claim(&c6).is_none());
    }

    #[test]
    fn normalize_entra_username_strips_domain_and_whitespace() {
        assert_eq!(normalize_entra_username("user@domain.com"), "user");
        assert_eq!(normalize_entra_username("CONTOSO\\bob"), "bob");
        assert_eq!(normalize_entra_username("  spaced  "), "spaced");
        // Backslash takes precedence over '@'.
        assert_eq!(normalize_entra_username("dom\\u@x.com"), "u@x.com");
        // A plain username is unchanged.
        assert_eq!(normalize_entra_username("dallan"), "dallan");
    }

    #[test]
    fn finalize_token_entra_derives_uuid5_sub_and_username() {
        let mut provider = test_provider();
        provider.identity_mapping = IdentityMapping::Entra;
        let claims = serde_json::json!({
            "sub": "opaque-entra-oid-123",
            "iss": "https://login.microsoftonline.com/TENANT/v2.0",
            "preferred_username": "Alice@contoso.com",
        });
        let vt = finalize_token(&provider, claims).unwrap();
        let expected = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            b"https://login.microsoftonline.com/TENANT/v2.0|opaque-entra-oid-123",
        )
        .simple()
        .to_string();
        assert_eq!(vt.sub, expected, "sub must be the uuid5(iss|sub) hex");
        assert_eq!(vt.sub.len(), 32, "uuid5 .hex form is 32 chars, no dashes");
        assert_ne!(vt.sub, "opaque-entra-oid-123");
        // claims enriched exactly like Python decode_token.
        assert_eq!(vt.claims["sub"].as_str(), Some(expected.as_str()));
        assert_eq!(
            vt.claims["entra_sub"].as_str(),
            Some("opaque-entra-oid-123")
        );
        assert_eq!(
            vt.claims["entra_username"].as_str(),
            Some("Alice@contoso.com")
        );
        assert_eq!(vt.claims["user"].as_str(), Some("Alice"));
    }

    #[test]
    fn finalize_token_entra_falls_back_to_sub_without_username_claim() {
        let mut provider = test_provider();
        provider.identity_mapping = IdentityMapping::Entra;
        let claims = serde_json::json!({"sub": "oid-9", "iss": "https://i.test/"});
        let vt = finalize_token(&provider, claims).unwrap();
        assert_eq!(
            vt.claims["user"].as_str(),
            Some("oid-9"),
            "username falls back to the Entra sub when no human-readable claim exists"
        );
        assert_eq!(vt.claims["entra_sub"].as_str(), Some("oid-9"));
        assert!(
            vt.claims.get("entra_username").is_none(),
            "no entra_username is stored when no claim was found"
        );
    }

    #[test]
    fn finalize_token_entra_requires_sub_claim() {
        let mut provider = test_provider();
        provider.identity_mapping = IdentityMapping::Entra;
        let claims = serde_json::json!({"iss": "https://i.test/", "preferred_username": "x"});
        assert!(
            finalize_token(&provider, claims).is_err(),
            "Entra mapping needs the 'sub' claim to derive the uuid5 subject"
        );
    }

    #[test]
    fn finalize_token_standard_uses_subject_claim_verbatim() {
        let provider = test_provider(); // Standard, subject_claim = "sub"
        let claims = serde_json::json!({"sub": "raw-subject", "iss": "https://issuer.test/"});
        let vt = finalize_token(&provider, claims).unwrap();
        assert_eq!(vt.sub, "raw-subject");
        // Standard mode must not enrich claims with Entra fields.
        assert!(vt.claims.get("user").is_none());
        assert!(vt.claims.get("entra_sub").is_none());
    }

    // --- G3 OBO session state ---

    #[test]
    fn build_session_state_entra_stores_both_obo_tokens() {
        let mut provider = test_provider();
        provider.identity_mapping = IdentityMapping::Entra;
        let token_response = serde_json::json!({
            "id_token": "ignored",
            "access_token": "upstream-access",
            "refresh_token": "upstream-refresh",
            "token_type": "Bearer",
        });
        let state = build_session_state(&provider, &token_response);
        assert_eq!(
            state["entra_access_token"].as_str(),
            Some("upstream-access")
        );
        assert_eq!(
            state["entra_refresh_token"].as_str(),
            Some("upstream-refresh")
        );
        // Only the two OBO keys — nothing else from the response leaks in.
        assert_eq!(state.as_object().unwrap().len(), 2);
    }

    #[test]
    fn build_session_state_entra_stores_only_present_tokens() {
        let mut provider = test_provider();
        provider.identity_mapping = IdentityMapping::Entra;
        // No refresh_token in the response (IdP did not issue offline_access).
        let token_response = serde_json::json!({"access_token": "only-access"});
        let state = build_session_state(&provider, &token_response);
        assert_eq!(state["entra_access_token"].as_str(), Some("only-access"));
        assert!(
            state.get("entra_refresh_token").is_none(),
            "an absent refresh_token must not be stored"
        );
    }

    // --- G3 token-POST scope (offline_access + Entra extra_scopes) ---

    #[test]
    fn build_token_scope_baseline_is_sorted_openid_offline_access() {
        // No extra scopes → exactly Python's sorted({"openid","offline_access"}).
        assert_eq!(build_token_scope(&[]), "offline_access openid");
    }

    #[test]
    fn build_token_scope_unions_and_sorts_extra_scopes() {
        let extra = vec![
            "api://tiled-api/access_as_user".to_string(),
            "User.Read".to_string(),
        ];
        // Sorted union of the baseline + extras (BTreeSet order).
        assert_eq!(
            build_token_scope(&extra),
            "User.Read api://tiled-api/access_as_user offline_access openid"
        );
    }

    #[test]
    fn build_token_scope_dedups_baseline_collisions() {
        // A duplicate baseline scope in extra_scopes collapses (set semantics),
        // matching Python's `{"openid","offline_access"} | extra_scopes`.
        let extra = vec!["openid".to_string(), "offline_access".to_string()];
        assert_eq!(build_token_scope(&extra), "offline_access openid");
    }

    #[test]
    fn build_session_state_standard_is_empty_object() {
        let provider = test_provider(); // Standard mapping
        let token_response = serde_json::json!({
            "access_token": "upstream-access",
            "refresh_token": "upstream-refresh",
        });
        let state = build_session_state(&provider, &token_response);
        assert_eq!(
            state,
            serde_json::json!({}),
            "a non-Entra provider stores no OBO tokens (Python OIDCAuthenticator returns empty)"
        );
    }

    fn test_provider() -> OidcProvider {
        OidcProvider {
            name: "test".into(),
            jwks_url: "https://example.test/jwks".into(),
            issuer: "https://issuer.test/".into(),
            audiences: vec!["tiled".into()],
            subject_claim: "sub".into(),
            identity_mapping: IdentityMapping::Standard,
            algorithms: vec![Algorithm::HS256],
            scopes_map: HashMap::new(),
            client_id: None,
            client_secret: None,
            authorization_endpoint: None,
            token_endpoint: None,
            extra_scopes: Vec::new(),
            end_session_endpoint: None,
            redirect_on_success: None,
            redirect_on_failure: None,
        }
    }

    fn code_flow_provider() -> OidcProvider {
        OidcProvider {
            name: "code-idp".into(),
            jwks_url: "https://code-idp.test/jwks".into(),
            issuer: "https://code-idp.test/".into(),
            audiences: vec!["tiled-code-client".into()],
            subject_claim: "sub".into(),
            identity_mapping: IdentityMapping::Standard,
            algorithms: vec![Algorithm::HS256],
            scopes_map: HashMap::new(),
            client_id: Some("tiled-code-client".into()),
            client_secret: None,
            authorization_endpoint: Some("https://code-idp.test/authorize".into()),
            token_endpoint: Some("https://code-idp.test/token".into()),
            extra_scopes: Vec::new(),
            end_session_endpoint: None,
            redirect_on_success: None,
            redirect_on_failure: None,
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

    // -----------------------------------------------------------------------
    // build_authorize_url
    //
    // The pending-state STORE is now DB-backed ([`crate::AuthDb`]); its
    // lifecycle (insert / single-use consume / expiry) is covered by the
    // `oidc_flow_*` tests in tiled-auth/tests/sqlite_auth.rs. Here we only test
    // the pure URL builder and the state it hands back for the caller to persist.
    // -----------------------------------------------------------------------

    #[test]
    fn build_authorize_url_structure() {
        let validator = ExternalOidcValidator::new(vec![code_flow_provider()]).unwrap();
        let redirect = validator
            .build_authorize_url("code-idp", "https://tiled.example.com/callback")
            .unwrap();
        let url = reqwest::Url::parse(&redirect.url).expect("returned URL must be valid");

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("code-idp.test"));
        assert_eq!(url.path(), "/authorize");

        let params: HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(
            params.get("response_type").map(|s| s.as_ref()),
            Some("code")
        );
        assert_eq!(
            params.get("client_id").map(|s| s.as_ref()),
            Some("tiled-code-client")
        );
        assert_eq!(
            params.get("redirect_uri").map(|s| s.as_ref()),
            Some("https://tiled.example.com/callback")
        );
        assert_eq!(
            params.get("code_challenge_method").map(|s| s.as_ref()),
            Some("S256")
        );
        assert!(
            params.contains_key("code_challenge"),
            "code_challenge must be present"
        );
        assert!(params.contains_key("state"), "state must be present");
        assert!(params.contains_key("nonce"), "nonce must be present");
        assert_eq!(params.get("prompt").map(|s| s.as_ref()), Some("login"));
    }

    #[test]
    fn build_authorize_url_rejects_bearer_only_provider() {
        let validator = ExternalOidcValidator::new(vec![test_provider()]).unwrap();
        let err = validator
            .build_authorize_url("test", "https://tiled.example.com/callback")
            .unwrap_err();
        assert!(
            err.to_string().contains("client_id"),
            "error should mention missing client_id"
        );
    }

    #[test]
    fn build_authorize_url_returns_state_to_persist() {
        let validator = ExternalOidcValidator::new(vec![code_flow_provider()]).unwrap();
        let redirect = validator
            .build_authorize_url("code-idp", "https://tiled.example.com/callback")
            .unwrap();
        // The verifier/nonce/state the caller must persist are returned, not
        // stored internally; the `state` returned must equal the one in the URL.
        assert!(!redirect.code_verifier.is_empty());
        assert!(!redirect.nonce.is_empty());
        assert!(!redirect.state.is_empty());
        let url = reqwest::Url::parse(&redirect.url).unwrap();
        let params: HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(
            params.get("state").map(|s| s.as_ref()),
            Some(redirect.state.as_str()),
            "returned state must match the URL's state param"
        );
        assert_eq!(
            params.get("nonce").map(|s| s.as_ref()),
            Some(redirect.nonce.as_str()),
            "returned nonce must match the URL's nonce param"
        );
    }

    #[test]
    fn pkce_pair_challenge_is_s256_of_verifier() {
        use base64::Engine;
        let (verifier, challenge) = gen_pkce_pair();
        let hash = Sha256::digest(verifier.as_bytes());
        let expected = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);
        assert_eq!(challenge, expected, "code_challenge must be S256(verifier)");
    }

    // -----------------------------------------------------------------------
    // Bearer validation (existing tests, preserved)
    // -----------------------------------------------------------------------

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
