//! JWT issuance + verification (HS256).
//!
//! Mirrors Python tiled's tokens. Two claim shapes:
//! - [`AccessClaims`] short-lived bearer the client sends on every request.
//! - [`RefreshClaims`] long-lived, used to mint a new access token when the
//!   old one expires.
//!
//! Both carry the session UUID; revoking the session in the auth DB
//! invalidates every still-cached JWT instantly.

use chrono::{Duration, Utc};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::Deserialize;
use serde::{Serialize, de::DeserializeOwned};

use crate::auth::error::{AuthError, Result};
use crate::auth::scopes::ScopeSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessClaims {
    /// Subject — Principal UUID.
    pub sub: String,
    /// Session UUID; checked against `sessions.revoked` before honouring
    /// the token.
    pub sid: String,
    /// Issued-at (unix seconds).
    pub iat: i64,
    /// Expiry (unix seconds).
    pub exp: i64,
    /// Scope strings the bearer is allowed to exercise.
    #[serde(default)]
    pub scopes: ScopeSet,
    /// OBO session state — mirrors the `state` column of the session named by
    /// `sid` (Python tiled embeds `session.state` here, authentication.py:857).
    /// Carries the upstream IdP access/refresh tokens for an Entra code-flow
    /// session; `{}` otherwise. `default` keeps pre-`state` tokens decodable
    /// (they yield `null`, treated as "no state" like Python's `.get("state")`).
    #[serde(default)]
    pub state: serde_json::Value,
    /// `"access"` — enforced by `verify_access` so a refresh token can't
    /// be presented as an access token (symmetric with `RefreshClaims.typ`).
    pub typ: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshClaims {
    pub sub: String,
    pub sid: String,
    pub iat: i64,
    pub exp: i64,
    /// `"refresh"` — used so a refresh token can't be presented as an
    /// access token by accident.
    pub typ: String,
}

#[derive(Clone)]
pub struct Issuer {
    /// Signs every new token — derived from the *first* secret (Python tiled
    /// encodes with `secret_keys[0]`, authentication.py:866).
    encoding: EncodingKey,
    /// One verifier per configured secret, in order. A token is accepted if it
    /// verifies against any of them, so a token signed by a rotated-out key
    /// still validates until that key is dropped (Python tiled's `decode_token`
    /// loops over `secret_keys`, authentication.py:165-172).
    decoding: Vec<DecodingKey>,
    pub access_ttl: Duration,
    pub refresh_ttl: Duration,
}

impl std::fmt::Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issuer")
            .field("access_ttl", &self.access_ttl)
            .field("refresh_ttl", &self.refresh_ttl)
            .field("encoding", &"<redacted>")
            .field("decoding", &"<redacted>")
            .finish()
    }
}

impl Issuer {
    /// Single-secret convenience: no rotation. `secret` is HMAC-SHA256 keying
    /// material. Equivalent to [`Issuer::with_secrets`] with a one-element list.
    pub fn new(secret: &[u8]) -> Result<Self> {
        Self::with_secrets(std::slice::from_ref(&secret))
    }

    /// Build an Issuer over an ordered list of HMAC-SHA256 secrets to support
    /// key rotation, mirroring Python tiled's `secret_keys` /
    /// `TILED_SECRET_KEYS`: the **first** secret signs every new token, and a
    /// presented token is accepted if it verifies against **any** secret (tried
    /// in order). To rotate, prepend the new secret and keep the old one until
    /// all tokens signed by it have expired, then drop it.
    pub fn with_secrets(secrets: &[&[u8]]) -> Result<Self> {
        if secrets.is_empty() {
            return Err(AuthError::Validation(
                "at least one JWT secret is required".into(),
            ));
        }
        // HMAC-SHA256 keying material must be at least 32 bytes (256 bits) —
        // the SHA-256 block/output width. Python tiled defaults to
        // secrets.token_hex(32); we reject weaker keys outright rather than
        // silently accept a brute-forceable secret. Every rotation key is held
        // to the same floor — a weak old key is as exploitable as a weak new one.
        for secret in secrets {
            if secret.len() < 32 {
                return Err(AuthError::Validation(format!(
                    "JWT secret must be at least 32 bytes, got {}",
                    secret.len()
                )));
            }
        }
        Ok(Self {
            encoding: EncodingKey::from_secret(secrets[0]),
            decoding: secrets
                .iter()
                .map(|s| DecodingKey::from_secret(s))
                .collect(),
            access_ttl: Duration::minutes(15),
            refresh_ttl: Duration::days(7),
        })
    }

    /// Verify `token` against each rotation key in order. Returns the claims on
    /// the first key that accepts it. An *expired* token short-circuits: once a
    /// key verifies the signature, a failed `exp` check is final — we do NOT try
    /// the remaining keys (Python tiled re-raises `ExpiredSignatureError` from
    /// inside the loop, authentication.py:169-170, rather than treating expiry
    /// as a wrong-key miss). A signature mismatch falls through to the next key.
    fn decode_rotating<T: DeserializeOwned>(&self, token: &str, v: &Validation) -> Result<T> {
        let mut last: Option<jsonwebtoken::errors::Error> = None;
        for key in &self.decoding {
            match decode::<T>(token, key, v) {
                Ok(data) => return Ok(data.claims),
                Err(e) if matches!(e.kind(), ErrorKind::ExpiredSignature) => return Err(e.into()),
                Err(e) => last = Some(e),
            }
        }
        // `decoding` is never empty (with_secrets rejects that), so `last` is
        // always populated here; the fallback is a defensive non-panic.
        Err(last
            .map(Into::into)
            .unwrap_or_else(|| AuthError::Unauthorized("no JWT verification keys".into())))
    }

    pub fn with_ttls(mut self, access: Duration, refresh: Duration) -> Self {
        self.access_ttl = access;
        self.refresh_ttl = refresh;
        self
    }

    pub fn issue_access(
        &self,
        principal_uuid: &str,
        session_uuid: &str,
        scopes: ScopeSet,
        state: serde_json::Value,
    ) -> Result<String> {
        let now = Utc::now();
        let claims = AccessClaims {
            sub: principal_uuid.into(),
            sid: session_uuid.into(),
            iat: now.timestamp(),
            exp: (now + self.access_ttl).timestamp(),
            scopes,
            state,
            typ: "access".into(),
        };
        let token = encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)?;
        Ok(token)
    }

    pub fn issue_refresh(&self, principal_uuid: &str, session_uuid: &str) -> Result<String> {
        let now = Utc::now();
        let claims = RefreshClaims {
            sub: principal_uuid.into(),
            sid: session_uuid.into(),
            iat: now.timestamp(),
            exp: (now + self.refresh_ttl).timestamp(),
            typ: "refresh".into(),
        };
        let token = encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)?;
        Ok(token)
    }

    pub fn verify_access(&self, token: &str) -> Result<AccessClaims> {
        let mut v = Validation::new(Algorithm::HS256);
        // Parity with Python tiled: `jose.jwt.decode` is called with no
        // leeway, so exp is checked exactly (default leeway 0). jsonwebtoken
        // defaults to 60s; pin it to 0 so a token Python would reject as
        // expired is not accepted here.
        v.leeway = 0;
        // auth-L1: `iat` is intentionally NOT validated and `validate_nbf`
        // stays false. Python tiled's `create_access_token`/`create_refresh_token`
        // (authentication.py) set only `exp` + `type` — never `iat` or `nbf` —
        // so python-jose's "verify when present" never fires for tiled tokens.
        // This Issuer only ever verifies tokens it signed itself (always a past
        // `iat`, no `nbf`); external IdP tokens go through ExternalOidcValidator.
        // Adding an iat/nbf gate here would diverge from Python and guard an
        // input this path cannot legitimately receive. The signature + `exp`
        // are the security boundary.
        let claims: AccessClaims = self.decode_rotating(token, &v)?;
        if claims.typ != "access" {
            return Err(AuthError::Unauthorized("not an access token".into()));
        }
        Ok(claims)
    }

    pub fn verify_refresh(&self, token: &str) -> Result<RefreshClaims> {
        let mut v = Validation::new(Algorithm::HS256);
        // Parity with Python tiled: `jose.jwt.decode` is called with no
        // leeway, so exp is checked exactly (default leeway 0). jsonwebtoken
        // defaults to 60s; pin it to 0 so a token Python would reject as
        // expired is not accepted here. `iat`/`nbf` are intentionally not
        // validated here for the same reason as `verify_access` (auth-L1).
        v.leeway = 0;
        let claims: RefreshClaims = self.decode_rotating(token, &v)?;
        if claims.typ != "refresh" {
            return Err(AuthError::Unauthorized("not a refresh token".into()));
        }
        Ok(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::scopes::Scope;

    #[test]
    fn access_token_roundtrip() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        let scopes = ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData]);
        let token = issuer
            .issue_access("p-uuid", "s-uuid", scopes.clone(), serde_json::json!({}))
            .unwrap();
        let claims = issuer.verify_access(&token).unwrap();
        assert_eq!(claims.sub, "p-uuid");
        assert_eq!(claims.sid, "s-uuid");
        assert_eq!(claims.scopes, scopes);
        assert_eq!(claims.state, serde_json::json!({}));
    }

    // G3 OBO: the session `state` round-trips through the access token's
    // `state` claim verbatim (Python embeds session.state, authentication.py:857).
    #[test]
    fn access_token_carries_obo_state() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        let state = serde_json::json!({
            "entra_access_token": "eyJ-upstream-access",
            "entra_refresh_token": "0.AX-upstream-refresh",
        });
        let token = issuer
            .issue_access("p", "s", ScopeSet::default(), state.clone())
            .unwrap();
        let claims = issuer.verify_access(&token).unwrap();
        assert_eq!(
            claims.state, state,
            "the OBO state must survive issue → verify unchanged"
        );
        assert_eq!(
            claims
                .state
                .pointer("/entra_access_token")
                .and_then(|v| v.as_str()),
            Some("eyJ-upstream-access")
        );
    }

    // A token minted before the `state` claim existed (or any token omitting
    // it) must still decode — `#[serde(default)]` yields `null`, treated as
    // "no state" like Python's `decoded.get("state")`.
    #[test]
    fn access_token_without_state_claim_decodes_to_null() {
        // Hand-encode an access token lacking `state`, matching the pre-G3 shape.
        let now = Utc::now();
        #[derive(serde::Serialize)]
        struct LegacyAccess {
            sub: String,
            sid: String,
            iat: i64,
            exp: i64,
            scopes: ScopeSet,
            typ: String,
        }
        let legacy = LegacyAccess {
            sub: "p".into(),
            sid: "s".into(),
            iat: now.timestamp(),
            exp: (now + Duration::minutes(15)).timestamp(),
            scopes: ScopeSet::default(),
            typ: "access".into(),
        };
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        let token = encode(&Header::new(Algorithm::HS256), &legacy, &issuer.encoding).unwrap();
        let claims = issuer.verify_access(&token).unwrap();
        assert_eq!(claims.state, serde_json::Value::Null);
    }

    #[test]
    fn refresh_token_typ_enforced() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        let access = issuer
            .issue_access("p", "s", ScopeSet::default(), serde_json::json!({}))
            .unwrap();
        // Presenting an access token as a refresh fails on `typ`.
        assert!(matches!(
            issuer.verify_refresh(&access).unwrap_err(),
            AuthError::Jwt(_) | AuthError::Unauthorized(_)
        ));
    }

    #[test]
    fn access_token_typ_enforced() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        let refresh = issuer.issue_refresh("p", "s").unwrap();
        // Presenting a refresh token as an access token must fail on `typ`.
        assert!(matches!(
            issuer.verify_access(&refresh).unwrap_err(),
            AuthError::Jwt(_) | AuthError::Unauthorized(_)
        ));
    }

    // auth-M1: leeway must be 0 (Python parity), not 5s. A token that expired
    // 3s ago was accepted under the old 5s leeway; with leeway 0 it must be
    // rejected — same as Python, whose jose.jwt.decode uses no leeway.
    // Negative TTL puts exp deterministically in the past (no sleep, no flake).

    #[test]
    fn access_token_expired_within_old_leeway_is_rejected() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!")
            .unwrap()
            .with_ttls(Duration::seconds(-3), Duration::days(7));
        let token = issuer
            .issue_access("p", "s", ScopeSet::default(), serde_json::json!({}))
            .unwrap();
        assert!(
            issuer.verify_access(&token).is_err(),
            "token expired 3s ago must be rejected with leeway=0 (was accepted under 5s leeway)"
        );
    }

    #[test]
    fn refresh_token_expired_within_old_leeway_is_rejected() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!")
            .unwrap()
            .with_ttls(Duration::minutes(15), Duration::seconds(-3));
        let token = issuer.issue_refresh("p", "s").unwrap();
        assert!(
            issuer.verify_refresh(&token).is_err(),
            "refresh token expired 3s ago must be rejected with leeway=0"
        );
    }

    // auth-H1: JWT signing-secret rotation. The first secret signs; every
    // secret verifies (Python tiled secret_keys[0] encodes, decode_token loops).
    const OLD: &[u8] = b"old-rotation-secret-key-thirtytwo-bytes!";
    const NEW: &[u8] = b"new-rotation-secret-key-thirtytwo-bytes!";

    #[test]
    fn rotated_out_secret_still_verifies_and_new_signs() {
        // A token minted before rotation, signed with the now-old secret.
        let token_old = Issuer::new(OLD)
            .unwrap()
            .issue_access("p", "s", ScopeSet::default(), serde_json::json!({}))
            .unwrap();

        // After rotation the list is [NEW, OLD]: the pre-rotation token still
        // verifies (OLD is still present)…
        let rotated = Issuer::with_secrets(&[NEW, OLD]).unwrap();
        assert!(
            rotated.verify_access(&token_old).is_ok(),
            "a token signed by a rotated-out key must still verify until the key is dropped"
        );

        // …and a freshly issued token is signed with NEW (the first key):
        let token_new = rotated
            .issue_access("p", "s", ScopeSet::default(), serde_json::json!({}))
            .unwrap();
        assert!(
            Issuer::new(NEW).unwrap().verify_access(&token_new).is_ok(),
            "new tokens must verify under the first (signing) key"
        );
        assert!(
            Issuer::new(OLD).unwrap().verify_access(&token_new).is_err(),
            "new tokens must NOT verify under the old key — it does not sign"
        );

        // Once OLD is dropped, the pre-rotation token stops verifying.
        assert!(
            Issuer::new(NEW).unwrap().verify_access(&token_old).is_err(),
            "dropping the old key invalidates tokens it signed"
        );
    }

    #[test]
    fn expired_token_under_rotation_reports_expired_not_invalid() {
        // Expired token signed by the first rotation key. The expiry must
        // short-circuit the key loop and surface as ExpiredSignature (so the
        // server answers "expired, refresh"), not be masked as a wrong-key miss
        // by a later signature mismatch.
        let issuer = Issuer::with_secrets(&[NEW, OLD])
            .unwrap()
            .with_ttls(Duration::seconds(-3), Duration::days(7));
        let token = issuer
            .issue_access("p", "s", ScopeSet::default(), serde_json::json!({}))
            .unwrap();
        match issuer.verify_access(&token).unwrap_err() {
            AuthError::Jwt(e) => assert!(
                matches!(e.kind(), ErrorKind::ExpiredSignature),
                "expired token must surface as ExpiredSignature, got {:?}",
                e.kind()
            ),
            other => panic!("expected Jwt(ExpiredSignature), got {other:?}"),
        }
    }

    #[test]
    fn with_secrets_validates_inputs() {
        assert!(
            Issuer::with_secrets(&[]).is_err(),
            "an empty secret list must be rejected"
        );
        let short: &[u8] = b"too-short";
        assert!(
            Issuer::with_secrets(&[NEW, short]).is_err(),
            "a sub-32-byte rotation key must be rejected even if another key is valid"
        );
    }
}
