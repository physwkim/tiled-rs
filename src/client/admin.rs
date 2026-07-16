//! `Admin` — administrative accessor for principal and per-principal API-key
//! management, scoped off [`Context::admin`](crate::client::Context::admin).
//!
//! Mirrors the upstream `tiled.client.context.Admin` class (`context.py:1211`),
//! which groups the admin-only endpoints as a sub-object rather than as flat
//! methods on the client. Kept in its own module so the transport-focused
//! `context.rs` does not balloon and the admin surface stays cohesive.
//!
//! Every call requires the caller to hold the relevant admin scope
//! (`read:principals`, `write:principals`, `admin:apikeys`); the server answers
//! 403 (surfaced as [`ClientError::PermissionDenied`](crate::client::ClientError::PermissionDenied))
//! otherwise. Like the API-key methods on `Context`, these requests bypass the
//! response cache — administrative state must never be served from cache.

use reqwest::Method;

use crate::client::context::{ApiKeyCreated, Context};
use crate::client::error::Result;
use crate::client::utils::{JSON_MIME_TYPE, decode_response, handle_error};

/// Administrative view over a [`Context`], obtained via
/// [`Context::admin`](crate::client::Context::admin).
///
/// Borrows the parent context, so it is a zero-cost handle that shares its
/// transport, auth state, and CSRF token.
pub struct Admin<'a> {
    context: &'a Context,
}

impl<'a> Admin<'a> {
    pub(crate) fn new(context: &'a Context) -> Self {
        Self { context }
    }

    /// List principals (users and services) in the authentication database,
    /// paginated (`GET /api/v1/auth/principal`).
    ///
    /// Mirrors Python `Admin.list_principals` (`context.py:1218`). The Rust
    /// server takes the page window as `page[offset]` / `page[limit]` query
    /// parameters (JSON:API style) rather than upstream's flat `offset` /
    /// `limit`. Requires the `read:principals` scope.
    pub async fn list_principals(&self, offset: i64, limit: i64) -> Result<Vec<PrincipalView>> {
        let mut url = self.context.api_uri().join("auth/principal")?;
        url.query_pairs_mut()
            .append_pair("page[offset]", &offset.to_string())
            .append_pair("page[limit]", &limit.to_string());
        let req = self
            .context
            .request(Method::GET, &url)
            .await?
            .header(reqwest::header::ACCEPT, JSON_MIME_TYPE);
        let resp = self.context.send_with_auth(req).await?;
        self.context.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        decode_response::<Vec<PrincipalView>>(resp).await
    }

    /// Show one principal by UUID (`GET /api/v1/auth/principal/{uuid}`).
    ///
    /// Mirrors Python `Admin.show_principal` (`context.py:1232`). Requires the
    /// `read:principals` scope; the server answers 404 (surfaced as
    /// `ClientError::Server { status: 404, .. }`) when no principal has that
    /// UUID.
    pub async fn show_principal(&self, uuid: &str) -> Result<PrincipalView> {
        let url = self
            .context
            .api_uri()
            .join(&format!("auth/principal/{uuid}"))?;
        let req = self
            .context
            .request(Method::GET, &url)
            .await?
            .header(reqwest::header::ACCEPT, JSON_MIME_TYPE);
        let resp = self.context.send_with_auth(req).await?;
        self.context.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        decode_response::<PrincipalView>(resp).await
    }

    /// Create a new service principal with the given role
    /// (`POST /api/v1/auth/principal?role=<role>`).
    ///
    /// Mirrors Python `Admin.create_service_principal` (`context.py:1280`). The
    /// role travels as a query parameter and the request carries no body.
    /// Requires the `write:principals` scope.
    pub async fn create_service_principal(&self, role: &str) -> Result<PrincipalView> {
        let mut url = self.context.api_uri().join("auth/principal")?;
        url.query_pairs_mut().append_pair("role", role);
        let req = self
            .context
            .request(Method::POST, &url)
            .await?
            .header(reqwest::header::ACCEPT, JSON_MIME_TYPE);
        let req = self.context.add_csrf(req).await;
        let resp = self.context.send_with_auth(req).await?;
        self.context.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        decode_response::<PrincipalView>(resp).await
    }

    /// Generate a new API key for another principal
    /// (`POST /api/v1/auth/principal/{uuid}/apikey`).
    ///
    /// Mirrors Python `Admin.create_api_key` (`context.py:1242`). As with
    /// [`Context::create_api_key`], the Rust server takes `expires_in_seconds`
    /// (i64) rather than upstream's `expires_in` and has no `access_tags`.
    /// `scopes = None` defaults the key to the `inherit` metascope — the target
    /// principal's *current* role scopes, resolved at use time. Requires the
    /// `admin:apikeys` scope.
    pub async fn create_api_key(
        &self,
        uuid: &str,
        scopes: Option<Vec<String>>,
        expires_in_seconds: Option<i64>,
        note: Option<String>,
    ) -> Result<ApiKeyCreated> {
        let url = self
            .context
            .api_uri()
            .join(&format!("auth/principal/{uuid}/apikey"))?;
        let body = serde_json::json!({
            "scopes": scopes,
            "expires_in_seconds": expires_in_seconds,
            "note": note,
        });
        let req = self.context.request(Method::POST, &url).await?.json(&body);
        let req = self.context.add_csrf(req).await;
        let resp = self.context.send_with_auth(req).await?;
        self.context.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        decode_response::<ApiKeyCreated>(resp).await
    }

    /// Revoke an API key belonging to any principal
    /// (`DELETE /api/v1/auth/principal/{uuid}/apikey?first_eight=<...>`).
    ///
    /// Mirrors Python `Admin.revoke_api_key` (`context.py:1303`): `uuid`
    /// identifies the owning principal (guarding against revoking the wrong
    /// key) and `first_eight` — truncated to eight characters, as upstream —
    /// selects the key. Requires the `admin:apikeys` scope.
    pub async fn revoke_api_key(&self, uuid: &str, first_eight: &str) -> Result<()> {
        let first_eight = first_eight.get(..8).unwrap_or(first_eight);
        let mut url = self
            .context
            .api_uri()
            .join(&format!("auth/principal/{uuid}/apikey"))?;
        url.query_pairs_mut()
            .append_pair("first_eight", first_eight);
        let req = self.context.request(Method::DELETE, &url).await?;
        let req = self.context.add_csrf(req).await;
        let resp = self.context.send_with_auth(req).await?;
        self.context.maybe_capture_csrf(&resp).await;
        handle_error(resp).await?;
        Ok(())
    }
}

/// One identity linked to a principal, as embedded in [`PrincipalView`].
/// Mirrors the server's `IdentityView` (`auth/principal.rs`): the public `id`
/// is the upstream subject (`sub`), not an internal row primary key.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AdminIdentity {
    /// Upstream subject (`sub`) — the public identity handle.
    pub id: String,
    /// Provider name (e.g. `dummy`, `entra`).
    pub provider: String,
    /// Most recent login through this identity, if any.
    pub latest_login: Option<chrono::DateTime<chrono::Utc>>,
}

/// A principal (user or service) together with its linked identities, returned
/// by the admin principal endpoints. Mirrors the server's `PrincipalDetail`
/// serializer (`auth/principal.rs`): the public handle is the `uuid`, and the
/// internal row id is never exposed. The Rust server does not serialize
/// `api_keys` / `sessions` on this view (a divergence from upstream Python's
/// `schemas.Principal`), so they are absent here too.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PrincipalView {
    /// Stable public identifier for the principal.
    pub uuid: String,
    /// `"user"` or `"service"`.
    #[serde(rename = "type")]
    pub principal_type: String,
    /// Role controlling the principal's scope ceiling (`"user"` / `"admin"`).
    pub role: String,
    /// Linked identities (empty for a freshly created service principal).
    pub identities: Vec<AdminIdentity>,
}
