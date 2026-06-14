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
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

use crate::error::{AuthError, Result};
use crate::scopes::ScopeSet;

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
    encoding: EncodingKey,
    decoding: DecodingKey,
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
    /// `secret` is HMAC-SHA256 keying material.
    pub fn new(secret: &[u8]) -> Result<Self> {
        // HMAC-SHA256 keying material must be at least 32 bytes (256 bits) —
        // the SHA-256 block/output width. Python tiled defaults to
        // secrets.token_hex(32); we reject weaker keys outright rather than
        // silently accept a brute-forceable secret.
        if secret.len() < 32 {
            return Err(AuthError::Validation(format!(
                "JWT secret must be at least 32 bytes, got {}",
                secret.len()
            )));
        }
        Ok(Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
            access_ttl: Duration::minutes(15),
            refresh_ttl: Duration::days(7),
        })
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
    ) -> Result<String> {
        let now = Utc::now();
        let claims = AccessClaims {
            sub: principal_uuid.into(),
            sid: session_uuid.into(),
            iat: now.timestamp(),
            exp: (now + self.access_ttl).timestamp(),
            scopes,
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
        v.leeway = 5;
        let data = decode::<AccessClaims>(token, &self.decoding, &v)?;
        if data.claims.typ != "access" {
            return Err(AuthError::Unauthorized("not an access token".into()));
        }
        Ok(data.claims)
    }

    pub fn verify_refresh(&self, token: &str) -> Result<RefreshClaims> {
        let mut v = Validation::new(Algorithm::HS256);
        v.leeway = 5;
        let data = decode::<RefreshClaims>(token, &self.decoding, &v)?;
        if data.claims.typ != "refresh" {
            return Err(AuthError::Unauthorized("not a refresh token".into()));
        }
        Ok(data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scopes::Scope;

    #[test]
    fn access_token_roundtrip() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        let scopes = ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData]);
        let token = issuer
            .issue_access("p-uuid", "s-uuid", scopes.clone())
            .unwrap();
        let claims = issuer.verify_access(&token).unwrap();
        assert_eq!(claims.sub, "p-uuid");
        assert_eq!(claims.sid, "s-uuid");
        assert_eq!(claims.scopes, scopes);
    }

    #[test]
    fn refresh_token_typ_enforced() {
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        let access = issuer.issue_access("p", "s", ScopeSet::default()).unwrap();
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
}
