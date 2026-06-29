//! Authenticators — strategies that resolve `(username, secret) → Identity`.
//!
//! Built-ins:
//! - [`DummyAuthenticator`] — username/password pairs from a config map
//!   (and secrets hashed with Argon2id at construction time, not stored
//!   plaintext on disk).
//! - [`ProxiedHeaderAuthenticator`] — trusts a pre-authenticated header
//!   set by an upstream reverse proxy (e.g. `X-Forwarded-User`). Only
//!   safe when `trust_forwarded_headers` is on AND the proxy strips that
//!   header from incoming requests.

use std::collections::HashMap;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use async_trait::async_trait;
use axum::http::HeaderMap;

use crate::auth::error::{AuthError, Result};

/// Provider tag set on the resulting `Identity` (`provider, sub`).
#[derive(Debug, Clone)]
pub struct Subject {
    pub provider: String,
    pub sub: String,
}

#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Mount-point name used in URLs (`/auth/{name}/...`).
    fn name(&self) -> &str;

    /// Validate `(username, secret)`. Implementations should be timing-
    /// safe — Argon2id verify is the standard tool. On success return the
    /// `(provider, sub)` tuple identifying the user.
    async fn authenticate(&self, username: &str, secret: &str) -> Result<Subject>;
}

/// Header-based authenticator — the upstream proxy has already verified
/// the user; we just trust the header.
pub struct ProxiedHeaderAuthenticator {
    /// Header to read the user identifier from. Defaults to
    /// `x-forwarded-user`.
    pub header: String,
    /// Provider name to embed in the resulting Identity.
    pub provider_name: String,
}

impl Default for ProxiedHeaderAuthenticator {
    fn default() -> Self {
        Self {
            header: "x-forwarded-user".into(),
            provider_name: "proxied".into(),
        }
    }
}

impl ProxiedHeaderAuthenticator {
    pub fn extract(&self, headers: &HeaderMap) -> Option<Subject> {
        let value = headers.get(self.header.as_str())?;
        let s = value.to_str().ok()?;
        Some(Subject {
            provider: self.provider_name.clone(),
            sub: s.to_string(),
        })
    }
}

#[async_trait]
impl Authenticator for ProxiedHeaderAuthenticator {
    fn name(&self) -> &str {
        "proxied"
    }
    async fn authenticate(&self, _username: &str, _secret: &str) -> Result<Subject> {
        Err(AuthError::Validation(
            "ProxiedHeaderAuthenticator does not support /auth/login; the proxy populates the identity header".into(),
        ))
    }
}

/// Username/password authenticator backed by a static map. Useful for
/// development + small deployments. Each entry's password is hashed with
/// Argon2id at construction time so a leaked process memory dump doesn't
/// reveal plaintext credentials.
pub struct DummyAuthenticator {
    name: String,
    /// `username -> Argon2id-hashed password`.
    users: HashMap<String, String>,
}

impl DummyAuthenticator {
    pub fn new(provider_name: impl Into<String>) -> Self {
        Self {
            name: provider_name.into(),
            users: HashMap::new(),
        }
    }

    pub fn add_user(&mut self, username: &str, plaintext_password: &str) -> Result<()> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(plaintext_password.as_bytes(), &salt)
            .map_err(|e| AuthError::Hash(e.to_string()))?
            .to_string();
        self.users.insert(username.to_string(), hash);
        Ok(())
    }
}

#[async_trait]
impl Authenticator for DummyAuthenticator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn authenticate(&self, username: &str, secret: &str) -> Result<Subject> {
        let stored = self
            .users
            .get(username)
            .ok_or_else(|| AuthError::Unauthorized("invalid username or password".into()))?;
        let parsed = PasswordHash::new(stored)
            .map_err(|e| AuthError::Hash(format!("stored hash unreadable: {e}")))?;
        match Argon2::default().verify_password(secret.as_bytes(), &parsed) {
            Ok(_) => Ok(Subject {
                provider: self.name.clone(),
                sub: username.to_string(),
            }),
            Err(_) => Err(AuthError::Unauthorized(
                "invalid username or password".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dummy_accepts_correct_password() {
        let mut a = DummyAuthenticator::new("dummy");
        a.add_user("alice", "s3cret!").unwrap();
        let subject = a.authenticate("alice", "s3cret!").await.unwrap();
        assert_eq!(subject.provider, "dummy");
        assert_eq!(subject.sub, "alice");
    }

    #[tokio::test]
    async fn dummy_rejects_bad_password() {
        let mut a = DummyAuthenticator::new("dummy");
        a.add_user("alice", "s3cret!").unwrap();
        let err = a.authenticate("alice", "wrong").await.unwrap_err();
        assert!(matches!(err, AuthError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn dummy_rejects_unknown_user() {
        let a = DummyAuthenticator::new("dummy");
        let err = a.authenticate("nobody", "x").await.unwrap_err();
        assert!(matches!(err, AuthError::Unauthorized(_)));
    }

    #[test]
    fn proxied_extracts_header() {
        let p = ProxiedHeaderAuthenticator::default();
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-user", "alice@example.com".parse().unwrap());
        let s = p.extract(&h).unwrap();
        assert_eq!(s.sub, "alice@example.com");
    }
}
