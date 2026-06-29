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
pub mod external_oidc;
pub mod jwt;
/// LDAP username/password authenticator. Gated on `ldap` (pure-Rust `ldap3`).
#[cfg(feature = "ldap")]
pub mod ldap;
pub mod migrate;
/// OIDC authorization-code (PKCE browser) flow — DB-backed pending-state store
/// (G6). The `0008_add_oidc_flow_states` migration runs unconditionally.
pub mod oidc_flow;
/// PAM (Pluggable Authentication Modules) username/password authenticator.
/// Gated on `pam`, which links the system `libpam`.
#[cfg(feature = "pam")]
pub mod pam;
/// IdP-brokered OAuth2 device-code flow (pending-session store). The
/// `0007_add_pending_sessions` migration runs unconditionally.
pub mod pending_session;
pub mod principal;
#[cfg(feature = "saml")]
pub mod saml;
pub mod scopes;
pub mod session;

pub use api_key::{ApiKeyCreate, ApiKeyRecord, KeyMaterial};
pub use authenticator::{Authenticator, DummyAuthenticator, ProxiedHeaderAuthenticator, Subject};
pub use db::AuthDb;
pub use error::{AuthError, Result};
pub use external_oidc::{
    AuthorizeRedirect, CodeFlowSession, ExternalOidcValidator, IdentityMapping, OidcDiscovery,
    OidcProvider, ValidatedToken, discover_oidc,
};
pub use jwt::{AccessClaims, Issuer, RefreshClaims};
#[cfg(feature = "ldap")]
pub use ldap::{LdapAuthenticator, LdapConfig};
pub use oidc_flow::OidcFlowState;
#[cfg(feature = "pam")]
pub use pam::{PamAuthenticator, PamConfig};
pub use pending_session::{PendingSessionInit, PendingSessionRecord, PendingSessionStatus};
pub use principal::{Identity, IdentityView, Principal, PrincipalDetail};
#[cfg(feature = "saml")]
pub use saml::{PendingSamlStore, SamlConfig, SamlProvider};
pub use scopes::{Scope, ScopeSet};
pub use session::{SessionRecord, SessionStore};
