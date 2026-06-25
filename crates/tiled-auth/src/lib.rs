//! tiled-rs authentication & authorization.
//!
//! Mirrors `tiled.authn_database` + `tiled.server.authentication`. Backends
//! supported (matching the catalog crate) are SQLite and Postgres via
//! `sqlx`. Top-level entry points:
//!
//! - [`AuthDb`] / [`AuthDb::connect`] — open the auth DB, run migrations.
//! - [`scopes::Scope`] — granular permissions enforced per route.
//! - [`api_key`] — multi-user API key CRUD with Argon2id hashing.
//! - [`jwt::Issuer`] — sign/verify access + refresh tokens.
//! - [`session`] — session lifecycle (issue, refresh, revoke).
//! - [`authenticator`] — trait + built-in `DummyAuthenticator` (env/config
//!   user list) + `ProxiedHeaderAuthenticator` (trusted proxy header).
//! - [`device_code`] — OAuth2 device-code grant state machine.

pub mod access_tags;
pub mod api_key;
pub mod authenticator;
pub mod db;
pub mod device_code;
pub mod error;
#[cfg(feature = "oidc")]
pub mod external_oidc;
pub mod jwt;
pub mod migrate;
pub mod principal;
pub mod scopes;
pub mod session;

pub use api_key::{ApiKeyCreate, ApiKeyRecord, KeyMaterial};
pub use authenticator::{Authenticator, DummyAuthenticator, ProxiedHeaderAuthenticator};
pub use db::AuthDb;
pub use error::{AuthError, Result};
#[cfg(feature = "oidc")]
pub use external_oidc::{ExternalOidcValidator, OidcProvider, ValidatedToken};
pub use jwt::{AccessClaims, Issuer, RefreshClaims};
pub use principal::{Identity, IdentityView, Principal, PrincipalDetail};
pub use scopes::{Scope, ScopeSet};
pub use session::{SessionRecord, SessionStore};
