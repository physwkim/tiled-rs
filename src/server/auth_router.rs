//! `/auth/...` HTTP endpoints — login, refresh, logout, device-code,
//! whoami. All routes are mounted under `/api/v1/auth/...` so they share
//! the rest of the API's middleware stack (CORS, tracing, etc.) but are
//! exempt from the api_key middleware (otherwise you couldn't login).
//!
//! Endpoint shapes mirror Python tiled's `/auth/*` routes so existing
//! clients keep working.

use std::sync::Arc;

use crate::auth::{ApiKeyCreate, ScopeSet};
use axum::{
    Json,
    extract::{Form, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::server::auth_context::AuthContext;
use crate::server::error::ServerError;
use crate::server::state::AppState;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct TokensResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: &'static str,
    pub expires_in: i64,
    /// Identity of the just-logged-in principal. SPA reads this to show
    /// the username in the header without an extra `/whoami` round-trip.
    /// `None` for endpoints that don't issue identity (refresh, device).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityPayload>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IdentityPayload {
    pub id: String,
    pub provider: String,
}

fn require_auth_db(
    state: &AppState,
) -> Result<(&crate::auth::AuthDb, &crate::auth::Issuer), ServerError> {
    let db = state
        .auth_db
        .as_ref()
        .ok_or_else(|| ServerError::Validation("server has no auth DB; auth disabled".into()))?;
    let issuer = state
        .issuer
        .as_ref()
        .ok_or_else(|| ServerError::Internal("auth_db set without issuer".into()))?;
    Ok((db, issuer))
}

fn lookup_authenticator(
    state: &AppState,
    name: &str,
) -> Option<Arc<dyn crate::auth::Authenticator>> {
    state
        .authenticators
        .iter()
        .find(|a| a.name() == name)
        .cloned()
}

pub async fn login(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Json(body): Json<LoginRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let (db, issuer) = require_auth_db(&state)?;
    let auth = lookup_authenticator(&state, &provider)
        .ok_or_else(|| ServerError::NotFound(format!("authenticator '{provider}' not found")))?;
    let subject = auth
        .authenticate(&body.username, &body.password)
        .await
        .map_err(map_auth_err)?;
    let (principal, identity) = db
        .ensure_principal(&subject.provider, &subject.sub)
        .await
        .map_err(map_auth_err)?;
    db.touch_identity_login(identity.id).await.ok();
    // Derive scopes from the principal's role (Python parity: create_default_roles
    // in authn_database/core.py), then intersect with the operator cap so
    // `default_login_scopes` can still restrict all sessions server-wide.
    let role_scopes = crate::auth::ScopeSet::for_role(&principal.role);
    let scopes = role_scopes.intersect(&state.default_login_scopes);
    let session = db
        .create_session(
            principal.id,
            scopes.clone(),
            Utc::now() + issuer.session_ttl,
            // Non-OIDC login: no upstream tokens to carry.
            serde_json::json!({}),
        )
        .await
        .map_err(map_auth_err)?;
    let access = issuer
        .issue_access(
            &principal.uuid,
            &session.uuid,
            scopes,
            session.state.clone(),
        )
        .map_err(map_auth_err)?;
    let refresh = issuer
        .issue_refresh(&principal.uuid, &session.uuid)
        .map_err(map_auth_err)?;
    Ok(Json(TokensResponse {
        access_token: access,
        refresh_token: refresh,
        token_type: "Bearer",
        expires_in: issuer.access_ttl.num_seconds(),
        identity: Some(IdentityPayload {
            id: subject.sub.clone(),
            provider: subject.provider.clone(),
        }),
    }))
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

// Same opaque message for revoked, expired, and missing sessions — deliberately
// does not reveal which condition triggered it (Python parity: slide_session).
const OPAQUE_SESSION_ERR: &str = "Session has expired. Please re-authenticate.";

pub async fn refresh(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let (db, issuer) = require_auth_db(&state)?;
    let claims = issuer
        .verify_refresh(&body.refresh_token)
        .map_err(map_auth_err)?;
    let session = db.lookup_session(&claims.sid).await.map_err(|e| match e {
        crate::auth::AuthError::NotFound(_) => ServerError::Unauthorized(OPAQUE_SESSION_ERR.into()),
        other => map_auth_err(other),
    })?;
    if session.revoked || session.expiration_time <= Utc::now() {
        return Err(ServerError::Unauthorized(OPAQUE_SESSION_ERR.into()));
    }
    db.touch_session(&claims.sid).await.ok();
    db.increment_refresh_count(&claims.sid).await.ok();
    // Re-derive scopes from the principal's *current* role on every refresh
    // (Python parity: slide_session, authentication.py:1526-1528) so a role
    // downgrade or a tightened `default_login_scopes` takes effect on the next
    // refresh instead of surviving until the session hard-expires. Cap by the
    // stored `session.scopes` so a deliberately-narrowed session (e.g. one
    // minted for a specific apikey) is never *widened* back to the role max.
    let principal = db
        .get_principal(session.principal_id)
        .await
        .map_err(map_auth_err)?
        .ok_or_else(|| ServerError::Unauthorized(OPAQUE_SESSION_ERR.into()))?;
    let scopes =
        crate::server::app::mint_session_scopes(&principal, &state).intersect(&session.scopes);
    // Re-embed the stored OBO session state unchanged so the upstream tokens
    // survive refresh (Python re-reads session.state in create_tokens_from_session).
    let access = issuer
        .issue_access(&claims.sub, &claims.sid, scopes, session.state.clone())
        .map_err(map_auth_err)?;
    let new_refresh = issuer
        .issue_refresh(&claims.sub, &claims.sid)
        .map_err(map_auth_err)?;
    Ok(Json(serde_json::json!({
        "access_token": access,
        "expires_in": issuer.access_ttl.num_seconds(),
        "refresh_token": new_refresh,
        "refresh_token_expires_in": issuer.refresh_ttl.num_seconds(),
        "token_type": "bearer",
    })))
}

pub async fn logout(
    State(state): State<AppState>,
    auth: AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    if auth.kind != crate::server::auth_context::AuthKind::Session {
        return Err(ServerError::Unauthorized(
            "only session bearers can logout".into(),
        ));
    }
    // The session UUID is encoded in the JWT; we need the session ID to
    // revoke. The middleware already validated it; pass it through via
    // request extension. For simplicity, look up the session by user
    // principal — for now we revoke ALL sessions of the principal.
    let (db, _) = require_auth_db(&state)?;
    let principal = auth
        .principal
        .ok_or_else(|| ServerError::Internal("session auth without principal".into()))?;
    db.revoke_all_sessions(principal.id)
        .await
        .map_err(map_auth_err)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn whoami(auth: AuthContext) -> Result<impl IntoResponse, ServerError> {
    let principal_uuid = auth
        .principal
        .as_ref()
        .map(|p| p.uuid.clone())
        .unwrap_or_else(|| "anonymous".into());
    Ok(Json(serde_json::json!({
        "principal": principal_uuid,
        "kind": format!("{:?}", auth.kind),
        "scopes": auth.scopes.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    })))
}

// ---------------------------------------------------------------------------
// Device code grant
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct DeviceCodeInitiateResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub interval: i64,
    pub expires_in: i64,
}

pub async fn device_initiate(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let (db, _issuer) = require_auth_db(&state)?;
    let dc = db
        .initiate_device_code(Duration::minutes(15), Duration::seconds(5))
        .await
        .map_err(map_auth_err)?;

    // Build a verification URI that points at our own server.
    let base = state.resolve_base_url(&headers);
    let resp = DeviceCodeInitiateResponse {
        device_code: dc.device_code,
        // Display the canonical code in its dashed `XXXXXXXX-XXXXXXXX` form;
        // the approval boundary normalizes case/dashes back out.
        user_code: crate::auth::device_code::format_user_code(&dc.user_code),
        verification_uri: format!("{}/api/v1/auth/device/approve", base.trim_end_matches('/')),
        interval: dc.interval_seconds.into(),
        expires_in: 15 * 60,
    };
    Ok(Json(resp))
}

#[derive(Debug, Deserialize)]
pub struct DeviceTokenRequest {
    pub device_code: String,
}

pub async fn device_token(
    State(state): State<AppState>,
    Json(body): Json<DeviceTokenRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let (db, issuer) = require_auth_db(&state)?;
    let status = db
        .poll_device_code(&body.device_code)
        .await
        .map_err(map_auth_err)?;
    use crate::auth::device_code::DeviceStatus::*;
    match status {
        Pending => Err(ServerError::Unauthorized("authorization_pending".into())),
        SlowDown => Err(ServerError::Unauthorized("slow_down".into())),
        Expired => Err(ServerError::Unauthorized("expired_token".into())),
        Granted(principal_id) => {
            let principal = db
                .get_principal(principal_id)
                .await
                .map_err(map_auth_err)?
                .ok_or_else(|| ServerError::Internal("granted principal vanished".into()))?;
            let role_scopes = crate::auth::ScopeSet::for_role(&principal.role);
            let scopes = role_scopes.intersect(&state.default_login_scopes);
            let session = db
                .create_session(
                    principal_id,
                    scopes.clone(),
                    Utc::now() + issuer.session_ttl,
                    // Device-code grant: no upstream tokens to carry.
                    serde_json::json!({}),
                )
                .await
                .map_err(map_auth_err)?;
            let access = issuer
                .issue_access(
                    &principal.uuid,
                    &session.uuid,
                    scopes,
                    session.state.clone(),
                )
                .map_err(map_auth_err)?;
            let refresh = issuer
                .issue_refresh(&principal.uuid, &session.uuid)
                .map_err(map_auth_err)?;
            Ok(Json(TokensResponse {
                access_token: access,
                refresh_token: refresh,
                token_type: "Bearer",
                expires_in: issuer.access_ttl.num_seconds(),
                identity: None,
            }))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeviceApproveRequest {
    pub user_code: String,
    /// External OIDC bearer token for approving via an external IdP
    /// (e.g. Entra, Auth0, Keycloak). When present, the server validates
    /// this token via `state.external_oidc` before approving the device
    /// code. Requires `external_oidc` to be configured in `AppState`.
    ///
    /// TRUST BOUNDARY: this field is attacker-influenceable. Full
    /// validation (exp/iss/aud enforced; alg pinned from JWKS/config,
    /// never from the token header) MUST complete before any principal
    /// is created or any device code is approved.
    #[serde(default)]
    pub oidc_token: Option<String>,
}

pub async fn device_approve(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<DeviceApproveRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let (db, _) = require_auth_db(&state)?;

    // TRUST BOUNDARY: the OIDC token, when present, is an
    // attacker-influenceable input. Full validation (exp/iss/aud
    // enforced; alg pinned from JWKS/config, never from the token
    // header) is performed by `validate()` BEFORE `ensure_principal`
    // or `approve_device_code` are ever called.
    //
    // This handler is mounted in public_auth (outside the auth
    // middleware) so it can accept either a session bearer token
    // (validated below via `resolve_header_auth`) or an external OIDC
    // token in the request body. Both paths authenticate the approving
    // principal before any DB mutation.
    let principal_id = if let Some(ref token) = body.oidc_token {
        let validator = state.external_oidc.as_ref().ok_or_else(|| {
            ServerError::Validation(
                "oidc_token submitted but no external OIDC validator is configured".into(),
            )
        })?;
        let validated = validator.validate(token).await.map_err(map_auth_err)?;
        let (principal, identity) = db
            .ensure_principal(&validated.provider, &validated.sub)
            .await
            .map_err(map_auth_err)?;
        db.touch_identity_login(identity.id).await.ok();
        principal.id
    } else {
        // Session path: resolve from the Authorization header (Bearer
        // or Apikey). `resolve_header_auth` returns None when no valid
        // credential is supplied.
        let auth = crate::server::app::resolve_header_auth(&state, &headers)
            .await
            .ok_or_else(|| {
                ServerError::Unauthorized("login required to approve a device code".into())
            })?;
        auth.principal
            .ok_or_else(|| {
                ServerError::Unauthorized("login required to approve a device code".into())
            })?
            .id
    };

    db.approve_device_code(&body.user_code, principal_id)
        .await
        .map_err(map_auth_err)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// API key CRUD (multi-user mode)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ApiKeyCreateRequest {
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    #[serde(default)]
    pub expires_in_seconds: Option<i64>,
}

pub async fn api_key_create(
    State(state): State<AppState>,
    auth: AuthContext,
    Json(req): Json<ApiKeyCreateRequest>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::CreateApiKeys)?;
    let principal = auth
        .principal
        .clone()
        .ok_or_else(|| ServerError::Unauthorized("login required to create an api key".into()))?;
    let (db, _) = require_auth_db(&state)?;
    let scopes = match req.scopes {
        None => auth.scopes.clone(),
        Some(names) => {
            let mut set = ScopeSet::default();
            for name in names {
                let s = crate::auth::Scope::parse(&name)
                    .ok_or_else(|| ServerError::Validation(format!("unknown scope: {name}")))?;
                if !auth.scopes.contains(s) {
                    return Err(ServerError::Forbidden(format!(
                        "cannot grant a scope ({}) you don't hold",
                        s.as_str()
                    )));
                }
                set.insert(s);
            }
            set
        }
    };
    let exp = req
        .expires_in_seconds
        .map(|s| Utc::now() + Duration::seconds(s));
    let material = db
        .create_api_key(ApiKeyCreate {
            principal_id: principal.id,
            note: req.note,
            scopes,
            expiration_time: exp,
        })
        .await
        .map_err(map_auth_err)?;
    Ok(Json(serde_json::json!({
        "secret": material.secret,
        "first_eight": material.record.first_eight,
        "scopes": material.record.scopes.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        "expiration_time": material.record.expiration_time,
    })))
}

pub async fn api_key_list(
    State(state): State<AppState>,
    auth: AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    let (db, _) = require_auth_db(&state)?;
    let principal_id = auth.principal.as_ref().map(|p| p.id);
    let keys = db.list_api_keys(principal_id).await.map_err(map_auth_err)?;
    Ok(Json(
        keys.into_iter()
            .map(|k| {
                serde_json::json!({
                    "id": k.id,
                    "first_eight": k.first_eight,
                    "note": k.note,
                    "scopes": k.scopes.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    "expiration_time": k.expiration_time,
                    "time_created": k.time_created,
                    "latest_activity": k.latest_activity,
                })
            })
            .collect::<Vec<_>>(),
    ))
}

pub async fn api_key_revoke(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(first_eight): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::RevokeApiKeys)?;
    let (db, _) = require_auth_db(&state)?;
    let principal = auth
        .principal
        .clone()
        .ok_or_else(|| ServerError::Unauthorized("login required to revoke an api key".into()))?;
    // Look up the key owner before revoking so a non-admin can't drop a
    // key that belongs to someone else (just having ApiKeyRevoke scope on
    // your own session would otherwise be enough).
    let candidates = db
        .list_api_keys(Some(principal.id))
        .await
        .map_err(map_auth_err)?;
    let is_admin = auth.scopes.contains(crate::auth::Scope::AdminApiKeys);
    let allowed = candidates.iter().any(|k| k.first_eight == first_eight) || is_admin;
    if !allowed {
        return Err(ServerError::Forbidden(
            "api key does not belong to this principal".into(),
        ));
    }
    let caller_id = if is_admin { None } else { Some(principal.id) };
    let _ = db
        .revoke_api_key(&first_eight, caller_id)
        .await
        .map_err(map_auth_err)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SessionRevokeByTokenRequest {
    pub refresh_token: String,
}

/// `POST /api/v1/auth/session/revoke`
///
/// Revoke a session identified by the supplied refresh token without requiring
/// a full Bearer/Apikey credential — the signed refresh token IS the ownership
/// proof (mirrors Python's `revoke_session`, `authentication.py:1437`).
/// Mounted in `public_auth` so it is reachable without a prior session
/// (same pattern as `POST /auth/refresh`).
pub async fn session_revoke_by_token(
    State(state): State<AppState>,
    Json(body): Json<SessionRevokeByTokenRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let (db, issuer) = require_auth_db(&state)?;
    let claims = issuer
        .verify_refresh(&body.refresh_token)
        .map_err(map_auth_err)?;
    let session = db.lookup_session(&claims.sid).await.map_err(|e| match e {
        crate::auth::AuthError::NotFound(_) => {
            ServerError::Validation(format!("No session {}", claims.sid))
        }
        other => map_auth_err(other),
    })?;
    if session.revoked {
        return Err(ServerError::Validation(format!(
            "No session {}",
            claims.sid
        )));
    }
    db.revoke_session(&claims.sid).await.map_err(map_auth_err)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/v1/auth/session/revoke/{session_id}`
///
/// Revoke a specific session by its UUID. The caller must own the session
/// (their principal UUID must match the session's principal). Mirrors Python's
/// `revoke_session_by_id` (`authentication.py:1462`). 404 when the session
/// does not exist or belongs to another principal.
pub async fn session_revoke_by_id(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(session_id): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let (db, _) = require_auth_db(&state)?;
    let principal = auth
        .principal
        .ok_or_else(|| ServerError::Unauthorized("login required to revoke a session".into()))?;
    let session = db.lookup_session(&session_id).await.map_err(|e| match e {
        crate::auth::AuthError::NotFound(_) => ServerError::NotFound(
            "Session does not exist or requester has insufficient permissions".into(),
        ),
        other => map_auth_err(other),
    })?;
    if session.principal_id != principal.id {
        return Err(ServerError::NotFound(
            "Session does not exist or requester has insufficient permissions".into(),
        ));
    }
    db.revoke_session(&session_id).await.map_err(map_auth_err)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Current API key info
// ---------------------------------------------------------------------------

/// `GET /api/v1/auth/apikey`
///
/// Return metadata about the API key used to authenticate the current request.
/// Mirrors Python's `current_apikey_info` (`authentication.py:1584`). Returns
/// 401 when the request was not authenticated via an API key.
pub async fn current_apikey_info(
    State(state): State<AppState>,
    auth: AuthContext,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    if auth.kind != crate::server::auth_context::AuthKind::ApiKey {
        return Err(ServerError::Unauthorized(
            "No API key was provided with this request.".into(),
        ));
    }
    let (db, _) = require_auth_db(&state)?;
    // Re-extract the raw key from the Authorization header (same key the
    // middleware already verified — re-verification is the parity-correct
    // approach used by Python's lookup_valid_api_key).
    let key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Apikey "))
        .ok_or_else(|| {
            ServerError::Unauthorized("No API key was provided with this request.".into())
        })?
        .to_string();
    let record = db
        .verify_api_key(&key)
        .await
        .map_err(|_| ServerError::Unauthorized("Invalid API key".into()))?;
    Ok(Json(serde_json::json!({
        "id": record.id,
        "first_eight": record.first_eight,
        "note": record.note,
        "scopes": record.scopes.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        "expiration_time": record.expiration_time,
        "time_created": record.time_created,
        "latest_activity": record.latest_activity,
    })))
}

// ---------------------------------------------------------------------------
// Principal CRUD (multi-user mode, admin-gated)
// ---------------------------------------------------------------------------

/// `GET /api/v1/auth/principal`
///
/// List all principals (users and services), paginated. Requires the
/// `read:principals` scope — mirrors Python's `principal_list`
/// (`authentication.py:1243`).
#[derive(Debug, Deserialize)]
pub struct PrincipalListQuery {
    #[serde(rename = "page[offset]", default)]
    pub offset: i64,
    #[serde(rename = "page[limit]", default = "default_page_limit")]
    pub limit: i64,
}

fn default_page_limit() -> i64 {
    100
}

pub async fn list_principals(
    State(state): State<AppState>,
    auth: AuthContext,
    Query(q): Query<PrincipalListQuery>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadPrincipals)?;
    let (db, _) = require_auth_db(&state)?;
    let limit = q.limit.min(200);
    let principals = db
        .list_principals(q.offset, limit)
        .await
        .map_err(map_auth_err)?;
    Ok(Json(principals))
}

#[derive(Debug, Deserialize)]
pub struct CreateServicePrincipalQuery {
    #[serde(default = "default_role")]
    pub role: String,
}

fn default_role() -> String {
    "user".into()
}

/// `POST /api/v1/auth/principal?role=<role>`
///
/// Create a new service principal. Requires `write:principals` scope —
/// mirrors Python's `Security(check_scopes, scopes=["write:principals"])`
/// on the same endpoint (authentication.py:1295).
pub async fn create_service_principal(
    State(state): State<AppState>,
    auth: AuthContext,
    Query(q): Query<CreateServicePrincipalQuery>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WritePrincipals)?;
    let (db, _) = require_auth_db(&state)?;
    let principal = db
        .create_service_principal(&q.role)
        .await
        .map_err(map_auth_err)?;
    // Return the full principal view. A freshly created service principal has
    // no linked identities, matching Python's POST which reloads the principal
    // with `selectinload(Principal.identities)` and serializes the empty list
    // (authentication.py:1307-1320).
    Ok(Json(crate::auth::PrincipalDetail::new(
        principal,
        Vec::new(),
    )))
}

// ---------------------------------------------------------------------------
// Admin per-principal API key management
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct AdminApiKeyRevokeQuery {
    pub first_eight: String,
}

/// `DELETE /api/v1/auth/principal/{uuid}/apikey?first_eight=...`
///
/// Allow admins to delete any user's API key. Requires `admin:apikeys` scope.
/// Mirrors Python's `revoke_apikey_for_principal` (`authentication.py:1363`).
/// Returns 404 when the principal doesn't exist or has no key with that prefix.
pub async fn admin_revoke_principal_apikey(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(uuid): Path<String>,
    Query(q): Query<AdminApiKeyRevokeQuery>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::AdminApiKeys)?;
    let (db, _) = require_auth_db(&state)?;
    // Get the full Principal row (including internal id) to scope the DELETE.
    let principal = db
        .get_principal_by_uuid(&uuid)
        .await
        .map_err(map_auth_err)?
        .ok_or_else(|| {
            ServerError::NotFound(format!("The principal {uuid} has no such API key."))
        })?;
    // Revoke the key scoped to this principal's id so we only delete keys
    // that actually belong to them (mirrors Python: `api_key_orm.principal.uuid != uuid`).
    let _ = db
        .revoke_api_key(&q.first_eight, Some(principal.id))
        .await
        .map_err(|e| match e {
            crate::auth::AuthError::NotFound(_) => {
                ServerError::NotFound(format!("The principal {uuid} has no such API key."))
            }
            other => map_auth_err(other),
        })?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/v1/auth/principal/{uuid}/apikey`
///
/// Generate an API key for an arbitrary principal. Requires `admin:apikeys`
/// scope. Mirrors Python's `apikey_for_principal` (`authentication.py:1394`).
/// Validates that the requested scopes are a subset of the target principal's
/// role scopes (just as `api_key_create` validates against the caller's scopes).
pub async fn admin_create_principal_apikey(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(uuid): Path<String>,
    Json(req): Json<ApiKeyCreateRequest>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::AdminApiKeys)?;
    let (db, _) = require_auth_db(&state)?;
    let target_principal = db
        .get_principal_by_uuid(&uuid)
        .await
        .map_err(map_auth_err)?
        .ok_or_else(|| {
            ServerError::NotFound(format!(
                "Principal {uuid} does not exist or insufficient permissions."
            ))
        })?;
    // Validate requested scopes against target principal's role ceiling
    // (mirrors Python generate_apikey: `scopes must be a subset of principal_scopes | {"inherit"}`).
    let role_scopes = crate::auth::ScopeSet::for_role(&target_principal.role);
    let scopes = match req.scopes {
        None => {
            // Default: inherit — expands to role scopes at use time.
            let mut set = ScopeSet::default();
            set.insert(crate::auth::Scope::Inherit);
            set
        }
        Some(names) => {
            let mut set = ScopeSet::default();
            for name in names {
                let s = crate::auth::Scope::parse(&name)
                    .ok_or_else(|| ServerError::Validation(format!("unknown scope: {name}")))?;
                if s != crate::auth::Scope::Inherit && !role_scopes.contains(s) {
                    return Err(ServerError::Forbidden(format!(
                        "Requested scopes {name:?} must be a subset of the principal's scopes."
                    )));
                }
                set.insert(s);
            }
            set
        }
    };
    let exp = req
        .expires_in_seconds
        .map(|s| chrono::Utc::now() + Duration::seconds(s));
    let material = db
        .create_api_key(crate::auth::ApiKeyCreate {
            principal_id: target_principal.id,
            note: req.note,
            scopes,
            expiration_time: exp,
        })
        .await
        .map_err(map_auth_err)?;
    Ok(Json(serde_json::json!({
        "secret": material.secret,
        "first_eight": material.record.first_eight,
        "scopes": material.record.scopes.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        "expiration_time": material.record.expiration_time,
    })))
}

/// `GET /api/v1/auth/principal/{uuid}`
///
/// Return one principal together with its linked identities. Requires the
/// `read:principals` scope — mirrors Python's
/// `Security(check_scopes, scopes=["read:principals"])` on the same endpoint
/// (authentication.py:1332). 404 when no principal has the given uuid.
pub async fn get_principal(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(uuid): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadPrincipals)?;
    let (db, _) = require_auth_db(&state)?;
    let detail = db
        .get_principal_detail(&uuid)
        .await
        .map_err(map_auth_err)?
        .ok_or_else(|| ServerError::NotFound(format!("No such Principal {uuid}")))?;
    Ok(Json(detail))
}

// ---------------------------------------------------------------------------
// OIDC authorization-code flow (#1178)
// ---------------------------------------------------------------------------

/// Minutes a server-side PKCE flow state stays valid between `/authorize` and
/// `/callback` (matches the former in-memory store's 10-minute expiry).
const OIDC_FLOW_TTL_MINUTES: i64 = 10;

/// `GET /api/v1/auth/provider/{provider}/authorize`
///
/// Redirect the browser to the IdP's authorization endpoint with PKCE S256,
/// nonce, and state. The server-side PKCE state is persisted in the auth DB
/// (single owner; survives across processes — G6) for consumption by the
/// `/callback` route. Mirrors Python `authorize_redirect_route`
/// (authentication.py:954-976).
pub async fn oidc_authorize(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let validator = state.external_oidc.as_ref().ok_or_else(|| {
        ServerError::Validation(
            "OIDC code flow requested but no external_oidc is configured".into(),
        )
    })?;
    let base = state.resolve_base_url(&headers);
    let redirect_uri = format!(
        "{}/api/v1/auth/provider/{}/callback",
        base.trim_end_matches('/'),
        provider
    );
    let redirect = validator
        .build_authorize_url(&provider, &redirect_uri)
        .map_err(map_auth_err)?;
    // Persist the PKCE state so the callback — which may land on a different
    // process behind a load balancer — can recover and consume it (G6).
    let (db, _issuer) = require_auth_db(&state)?;
    db.create_oidc_flow_state(
        &redirect.state,
        &provider,
        &redirect.code_verifier,
        &redirect.nonce,
        Duration::minutes(OIDC_FLOW_TTL_MINUTES),
    )
    .await
    .map_err(map_auth_err)?;
    Ok(Redirect::to(&redirect.url))
}

/// Query parameters received on the callback URL.
#[derive(Debug, Deserialize)]
pub struct OidcCallbackParams {
    pub code: Option<String>,
    pub state: Option<String>,
    /// IdP-reported error (e.g. `access_denied`).
    pub error: Option<String>,
    pub error_description: Option<String>,
}

/// `GET /api/v1/auth/provider/{provider}/callback`
///
/// Receives the authorization code from the IdP, exchanges it for an
/// id_token at `token_endpoint`, validates the id_token (same JWKS
/// machinery + nonce check), upserts the principal, and issues tiled
/// access + refresh tokens. Mirrors Python `auth_code_route`
/// (authentication.py:977-1049).
pub async fn oidc_callback(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(params): Query<OidcCallbackParams>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    // Handle IdP-reported errors (e.g. user denied the authorization).
    if let Some(ref err) = params.error {
        let desc = params
            .error_description
            .as_deref()
            .unwrap_or("no description");
        return Err(ServerError::Unauthorized(format!(
            "IdP returned error '{err}': {desc}"
        )));
    }

    let code = params.code.as_deref().ok_or_else(|| {
        ServerError::Validation("callback is missing the 'code' parameter".into())
    })?;
    let state_param = params.state.as_deref().ok_or_else(|| {
        ServerError::Validation("callback is missing the 'state' parameter".into())
    })?;

    let validator = state.external_oidc.as_ref().ok_or_else(|| {
        ServerError::Validation("OIDC code flow used but no external_oidc is configured".into())
    })?;
    let (db, issuer) = require_auth_db(&state)?;

    let base = state.resolve_base_url(&headers);
    let redirect_uri = format!(
        "{}/api/v1/auth/provider/{}/callback",
        base.trim_end_matches('/'),
        provider
    );

    // Recover and consume the server-side PKCE state (single use; expiry
    // enforced in the DB — G6). Unknown / expired / replayed `state` → 401.
    let flow = db
        .take_oidc_flow_state(state_param)
        .await
        .map_err(map_auth_err)?
        .ok_or_else(|| {
            ServerError::Unauthorized(
                "unknown or expired authorization state — the state parameter was tampered \
                 with, the authorize window elapsed, or the callback was replayed"
                    .into(),
            )
        })?;

    let code_flow = validator
        .exchange_code_flow(&flow, code, &redirect_uri)
        .await
        .map_err(map_auth_err)?;
    let validated = code_flow.token;

    let (principal, identity) = db
        .ensure_principal(&validated.provider, &validated.sub)
        .await
        .map_err(map_auth_err)?;
    db.touch_identity_login(identity.id).await.ok();

    let role_scopes = crate::auth::ScopeSet::for_role(&principal.role);
    let scopes = role_scopes.intersect(&state.default_login_scopes);

    // Persist the OBO session state (Entra access/refresh tokens for an Entra
    // provider; `{}` otherwise) so it is embedded in the access token below and
    // re-embedded on every refresh — Python EntraAuthenticator (authentication.py:857).
    let session = db
        .create_session(
            principal.id,
            scopes.clone(),
            Utc::now() + issuer.session_ttl,
            code_flow.session_state,
        )
        .await
        .map_err(map_auth_err)?;
    let access = issuer
        .issue_access(
            &principal.uuid,
            &session.uuid,
            scopes,
            session.state.clone(),
        )
        .map_err(map_auth_err)?;
    let refresh = issuer
        .issue_refresh(&principal.uuid, &session.uuid)
        .map_err(map_auth_err)?;

    let tokens = TokensResponse {
        access_token: access.clone(),
        refresh_token: refresh.clone(),
        token_type: "Bearer",
        expires_in: issuer.access_ttl.num_seconds(),
        identity: Some(IdentityPayload {
            id: validated.sub.clone(),
            provider: validated.provider.clone(),
        }),
    };

    // redirect_on_success: redirect the browser back to the UI with tokens
    // as query params. Mirrors Python OIDCAuthenticator behaviour
    // (authentication.py:1023-1041).
    let redirect_on_success = validator
        .providers()
        .iter()
        .find(|p| p.name == provider)
        .and_then(|p| p.redirect_on_success.clone());

    if let Some(base_redir) = redirect_on_success {
        use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
        let encoded_access = utf8_percent_encode(&access, NON_ALPHANUMERIC).to_string();
        let encoded_refresh = utf8_percent_encode(&refresh, NON_ALPHANUMERIC).to_string();
        let redir = format!(
            "{}?access_token={}&refresh_token={}",
            base_redir, encoded_access, encoded_refresh
        );
        return Ok(Redirect::to(&redir).into_response());
    }

    Ok(Json(tokens).into_response())
}

// ---------------------------------------------------------------------------
// IdP-brokered device-code flow (external OIDC)
//
// A device login brokered through an EXTERNAL OIDC provider: the CLI polls
// tiled while the user completes the IdP's authorization-code flow in a
// browser. DISTINCT from the native `device_*` routes above, which approve a
// device code against a LOCAL tiled principal. Mirrors Python tiled's
// per-provider device routes (authentication.py:980-1133). The pending state
// lives in the `pending_sessions` table (crate::auth::pending_session).
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct OidcDeviceAuthorizeResponse {
    /// URL the user opens in a browser (the IdP's authorization endpoint).
    pub authorization_uri: String,
    /// URL the CLI polls for tokens (this server's `/token` route).
    pub verification_uri: String,
    pub interval: i64,
    pub device_code: String,
    pub expires_in: i64,
    pub user_code: String,
}

/// `POST /api/v1/auth/provider/{provider}/authorize`
///
/// Start an IdP-brokered device login: mint a pending session and return the
/// IdP `authorization_uri` (for the user's browser) plus the `verification_uri`
/// (for the CLI to poll). Mirrors Python `device_code_authorize_route`
/// (authentication.py:980).
pub async fn oidc_device_authorize(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let validator = state.external_oidc.as_ref().ok_or_else(|| {
        ServerError::Validation(
            "OIDC device flow requested but no external_oidc is configured".into(),
        )
    })?;
    let (db, _issuer) = require_auth_db(&state)?;
    let base = state.resolve_base_url(&headers);
    let base = base.trim_end_matches('/');
    // The redirect_uri the IdP sends the user back to (where they enter their
    // user_code). It MUST match the redirect_uri used at code exchange time.
    let device_code_uri = format!("{base}/api/v1/auth/provider/{provider}/device_code");
    let verification_uri = format!("{base}/api/v1/auth/provider/{provider}/token");

    // Validate provider config (and build the URL) BEFORE creating the pending
    // row, so a misconfigured provider never leaves an orphan pending session.
    let authorization_uri = validator
        .build_device_authorize_url(&provider, &device_code_uri)
        .map_err(map_auth_err)?;

    let init = db
        .create_pending_session(Duration::minutes(15))
        .await
        .map_err(map_auth_err)?;

    Ok(Json(OidcDeviceAuthorizeResponse {
        authorization_uri,
        verification_uri,
        interval: 5,
        device_code: init.device_code,
        expires_in: 15 * 60,
        // Display the canonical code in its dashed `XXXX-XXXX` form; the submit
        // route normalizes case/dashes back out.
        user_code: crate::auth::device_code::format_user_code(&init.user_code),
    }))
}

#[derive(Debug, Deserialize)]
pub struct OidcDeviceCodeFormQuery {
    /// The IdP authorization code, received on the redirect from the IdP.
    pub code: String,
}

/// `GET /api/v1/auth/provider/{provider}/device_code?code=...`
///
/// Serve the HTML form where the user enters their `user_code` after the IdP
/// redirects them here. Mirrors Python `device_code_user_code_form_route`
/// (authentication.py:1012).
pub async fn oidc_device_code_form(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(q): Query<OidcDeviceCodeFormQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let base = state.resolve_base_url(&headers);
    let action = format!(
        "{}/api/v1/auth/provider/{}/device_code",
        base.trim_end_matches('/'),
        provider
    );
    Html(device_code_form_html(&action, &q.code, None))
}

#[derive(Debug, Deserialize)]
pub struct OidcDeviceCodeSubmit {
    /// IdP authorization code (carried by the form's hidden field).
    pub code: String,
    /// The user-entered code (any case, dashes optional).
    pub user_code: String,
}

/// `POST /api/v1/auth/provider/{provider}/device_code`
///
/// Process the user-code form: look up the pending session, exchange the IdP
/// code for an identity, create a tiled session, and bind it to the pending
/// session so the CLI's next `/token` poll succeeds. Mirrors Python
/// `device_code_user_code_submit_route` (authentication.py:1031).
pub async fn oidc_device_code_submit(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    headers: HeaderMap,
    Form(body): Form<OidcDeviceCodeSubmit>,
) -> Result<Response, ServerError> {
    let validator = state.external_oidc.as_ref().ok_or_else(|| {
        ServerError::Validation("OIDC device flow used but no external_oidc is configured".into())
    })?;
    let (db, issuer) = require_auth_db(&state)?;
    let base = state.resolve_base_url(&headers);
    let base = base.trim_end_matches('/');
    let action = format!("{base}/api/v1/auth/provider/{provider}/device_code");
    // The redirect_uri presented at code exchange MUST match the one sent in
    // the authorize step (the /device_code route URL).
    let redirect_uri = action.clone();

    // Invalid or expired user_code → re-render the form with an error (401),
    // exactly as Python does.
    let pending = match db
        .lookup_valid_pending_session_by_user_code(&body.user_code)
        .await
    {
        Ok(p) => p,
        Err(_) => {
            let html = device_code_form_html(
                &action,
                &body.code,
                Some(
                    "Invalid user code. It may have been mistyped, or the pending \
                     request may have expired.",
                ),
            );
            return Ok((StatusCode::UNAUTHORIZED, Html(html)).into_response());
        }
    };

    // Exchange the IdP authorization code (no PKCE/nonce — device flow). A
    // failure here means the user_code was right but the IdP rejected the code.
    let code_flow = match validator
        .exchange_device_code(&provider, &body.code, &redirect_uri)
        .await
    {
        Ok(cf) => cf,
        Err(_) => {
            let html = device_code_failure_html(
                "User code was correct but authentication with third party failed. \
                 Ask administrator to see logs for details.",
            );
            return Ok((StatusCode::UNAUTHORIZED, Html(html)).into_response());
        }
    };
    let validated = code_flow.token;

    let (principal, identity) = db
        .ensure_principal(&validated.provider, &validated.sub)
        .await
        .map_err(map_auth_err)?;
    db.touch_identity_login(identity.id).await.ok();

    let role_scopes = crate::auth::ScopeSet::for_role(&principal.role);
    let scopes = role_scopes.intersect(&state.default_login_scopes);

    // Persist the OBO session state (Entra tokens or `{}`) so it rides every
    // access token minted for this session, identical to the browser flow.
    let session = db
        .create_session(
            principal.id,
            scopes,
            Utc::now() + issuer.session_ttl,
            code_flow.session_state,
        )
        .await
        .map_err(map_auth_err)?;

    db.bind_pending_session(&pending.hashed_device_code, session.id)
        .await
        .map_err(map_auth_err)?;

    Ok(Html(device_code_success_html(5)).into_response())
}

/// `POST /api/v1/auth/provider/{provider}/token`
///
/// The CLI polls this with its `device_code`. Returns `400 {"detail":
/// {"error": "authorization_pending"}}` until the browser-side login completes,
/// then mints and returns the session's tokens (single use — the pending row is
/// deleted). Mirrors Python `device_code_token_route` (authentication.py:1097).
pub async fn oidc_device_token(
    State(state): State<AppState>,
    Path(_provider): Path<String>,
    Json(body): Json<DeviceTokenRequest>,
) -> Result<Response, ServerError> {
    let (db, issuer) = require_auth_db(&state)?;
    match db.poll_pending_session(&body.device_code).await {
        Ok(crate::auth::PendingSessionStatus::AuthorizationPending) => Ok((
            StatusCode::BAD_REQUEST,
            // The client polls until `/detail/error == authorization_pending`;
            // this shape mirrors FastAPI's HTTPException(400, {"error": ...}).
            Json(serde_json::json!({"detail": {"error": "authorization_pending"}})),
        )
            .into_response()),
        Ok(crate::auth::PendingSessionStatus::Fulfilled(session_id)) => {
            let session = db
                .lookup_session_by_id(session_id)
                .await
                .map_err(map_auth_err)?;
            let principal = db
                .get_principal(session.principal_id)
                .await
                .map_err(map_auth_err)?
                .ok_or_else(|| {
                    ServerError::Internal("pending session references a missing principal".into())
                })?;
            let access = issuer
                .issue_access(
                    &principal.uuid,
                    &session.uuid,
                    session.scopes.clone(),
                    session.state.clone(),
                )
                .map_err(map_auth_err)?;
            let refresh = issuer
                .issue_refresh(&principal.uuid, &session.uuid)
                .map_err(map_auth_err)?;
            Ok(Json(TokensResponse {
                access_token: access,
                refresh_token: refresh,
                token_type: "Bearer",
                expires_in: issuer.access_ttl.num_seconds(),
                identity: None,
            })
            .into_response())
        }
        // Malformed (non-hex) device_code.
        Err(crate::auth::AuthError::Unauthorized(msg)) => Err(ServerError::Unauthorized(msg)),
        // Absent or expired pending session.
        Err(_) => Err(ServerError::NotFound(
            "No such device_code. The pending request may have expired.".into(),
        )),
    }
}

/// Minimal HTML-attribute / text escape for values interpolated into the
/// device-code pages (the IdP `code` and error messages).
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// User-code entry form. Mirrors `device_code_form.html`: the IdP `code` rides
/// in a hidden field (the real source the submit route reads), and an optional
/// error message is shown above the form.
fn device_code_form_html(action: &str, code: &str, message: Option<&str>) -> String {
    let msg_block = match message {
        Some(m) => format!(
            "<p style=\"color:#b00;font-weight:bold\">{}</p>",
            html_escape(m)
        ),
        None => String::new(),
    };
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <title>Authorize Tiled Session</title></head><body>\
         <h1>Authorize Tiled Session</h1>{msg_block}\
         <form action=\"{action}\" method=\"post\">\
         <label for=\"user_code\">Enter code</label> \
         <input type=\"text\" id=\"user_code\" name=\"user_code\" />\
         <input type=\"hidden\" id=\"code\" name=\"code\" value=\"{code}\" />\
         <input type=\"submit\" value=\"Enter\" /></form></body></html>",
        action = html_escape(action),
        code = html_escape(code),
    )
}

/// Shown after a successful user-code submission. Mirrors
/// `device_code_success.html`.
fn device_code_success_html(interval: i64) -> String {
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <title>Success</title></head><body><h1>Success</h1>\
         <p>Return to your Tiled application. Within {interval} seconds it should \
         be successfully logged in.</p></body></html>"
    )
}

/// Shown when the user_code was valid but the IdP exchange failed. Mirrors
/// `device_code_failure.html`.
fn device_code_failure_html(message: &str) -> String {
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <title>Failed</title></head><body><h1>Failed</h1><p>{}</p></body></html>",
        html_escape(message)
    )
}

// ---------------------------------------------------------------------------
// SAML 2.0 SP-initiated routes
// ---------------------------------------------------------------------------

/// `GET /api/v1/auth/saml/{provider}/login`
///
/// Builds an SP-initiated `AuthnRequest` and HTTP-Redirect-binds it to the
/// configured IdP SSO URL.  The generated request ID is stored in the
/// `PendingSamlStore` so the corresponding ACS callback can verify
/// `InResponseTo` (anti-replay).
///
/// Mirrors Python `SAMLAuthenticator` login route (authenticators.py:558-564).
#[cfg(feature = "saml")]
pub async fn saml_login(
    State(state): State<AppState>,
    Path(provider): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let sp = state
        .saml_providers
        .iter()
        .find(|p| p.name == provider)
        .cloned()
        .ok_or_else(|| {
            ServerError::NotFound(format!("SAML provider '{provider}' is not configured"))
        })?;

    let (redirect_url, _request_id) = sp.build_redirect_url().map_err(map_auth_err)?;

    Ok(Redirect::to(&redirect_url))
}

/// Form body received on the SAML Assertion Consumer Service endpoint.
#[cfg(feature = "saml")]
#[derive(Debug, serde::Deserialize)]
pub struct AcsFormBody {
    /// Base64-encoded `<samlp:Response>` element posted by the IdP.
    #[serde(rename = "SAMLResponse")]
    pub saml_response: String,
}

/// `POST /api/v1/auth/saml/{provider}/acs`
///
/// Assertion Consumer Service endpoint.  Receives the IdP-posted
/// `SAMLResponse`, enforces XML signature validation against the configured
/// IdP certificate, checks `InResponseTo` against the pending request store
/// (anti-replay / CSRF), extracts the configured attribute as the principal
/// identifier, upserts the principal, and issues tiled access + refresh tokens.
///
/// Mirrors Python `SAMLAuthenticator.authenticate` (authenticators.py:566-579).
#[cfg(feature = "saml")]
pub async fn saml_acs(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    axum::Form(body): axum::Form<AcsFormBody>,
) -> Result<impl IntoResponse, ServerError> {
    let sp = state
        .saml_providers
        .iter()
        .find(|p| p.name == provider)
        .cloned()
        .ok_or_else(|| {
            ServerError::NotFound(format!("SAML provider '{provider}' is not configured"))
        })?;

    let (db, issuer) = require_auth_db(&state)?;

    let subject = sp
        .validate_response(&body.saml_response)
        .map_err(map_auth_err)?;

    let (principal, identity) = db
        .ensure_principal(&subject.provider, &subject.sub)
        .await
        .map_err(map_auth_err)?;
    db.touch_identity_login(identity.id).await.ok();

    let role_scopes = crate::auth::ScopeSet::for_role(&principal.role);
    let scopes = role_scopes.intersect(&state.default_login_scopes);

    let session = db
        .create_session(
            principal.id,
            scopes.clone(),
            Utc::now() + issuer.session_ttl,
            // Non-OIDC login: no upstream tokens to carry.
            serde_json::json!({}),
        )
        .await
        .map_err(map_auth_err)?;
    let access = issuer
        .issue_access(
            &principal.uuid,
            &session.uuid,
            scopes,
            session.state.clone(),
        )
        .map_err(map_auth_err)?;
    let refresh = issuer
        .issue_refresh(&principal.uuid, &session.uuid)
        .map_err(map_auth_err)?;

    let tokens = TokensResponse {
        access_token: access,
        refresh_token: refresh,
        token_type: "Bearer",
        expires_in: issuer.access_ttl.num_seconds(),
        identity: Some(IdentityPayload {
            id: subject.sub.clone(),
            provider: subject.provider.clone(),
        }),
    };

    Ok(Json(tokens).into_response())
}

fn map_auth_err(e: crate::auth::AuthError) -> ServerError {
    use crate::auth::AuthError as AE;
    match e {
        AE::NotFound(m) => ServerError::NotFound(m),
        AE::Validation(m) => ServerError::Validation(m),
        AE::Conflict(m) => ServerError::Validation(m),
        AE::Unauthorized(m) => ServerError::Unauthorized(m),
        AE::Forbidden(m) => ServerError::Forbidden(m),
        AE::Expired => ServerError::Unauthorized("expired".into()),
        AE::Revoked => ServerError::Unauthorized("revoked".into()),
        // JWT verification failures (bad signature, expired, wrong iss/aud,
        // alg mismatch) are 401 — they indicate a forged or invalid token,
        // not a server fault. Mapping to Internal would wrongly surface an
        // HTTP 500 to the caller and obscure the rejection reason.
        AE::Jwt(e) => ServerError::Unauthorized(e.to_string()),
        other => ServerError::Internal(other.to_string()),
    }
}
