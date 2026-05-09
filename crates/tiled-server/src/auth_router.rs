//! `/auth/...` HTTP endpoints — login, refresh, logout, device-code,
//! whoami. All routes are mounted under `/api/v1/auth/...` so they share
//! the rest of the API's middleware stack (CORS, tracing, etc.) but are
//! exempt from the api_key middleware (otherwise you couldn't login).
//!
//! Endpoint shapes mirror Python tiled's `/auth/*` routes so existing
//! clients keep working.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use tiled_auth::{ApiKeyCreate, ScopeSet};

use crate::auth_context::AuthContext;
use crate::error::ServerError;
use crate::state::AppState;

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

fn require_auth_db(state: &AppState) -> Result<(&tiled_auth::AuthDb, &tiled_auth::Issuer), ServerError> {
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

fn lookup_authenticator<'a>(
    state: &'a AppState,
    name: &str,
) -> Option<Arc<dyn tiled_auth::Authenticator>> {
    state.authenticators.iter().find(|a| a.name() == name).cloned()
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
    // Login-issued scopes default to AppState::default_login_scopes so an
    // operator can lock down what a fresh session inherits without
    // touching every endpoint. API keys can still narrow the set.
    let scopes = state.default_login_scopes.clone();
    let session = db
        .create_session(principal.id, scopes.clone(), Utc::now() + issuer.refresh_ttl)
        .await
        .map_err(map_auth_err)?;
    let access = issuer
        .issue_access(&principal.uuid, &session.uuid, scopes)
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

pub async fn refresh(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let (db, issuer) = require_auth_db(&state)?;
    let claims = issuer
        .verify_refresh(&body.refresh_token)
        .map_err(map_auth_err)?;
    let session = db.lookup_session(&claims.sid).await.map_err(map_auth_err)?;
    if session.revoked || session.expiration_time <= Utc::now() {
        return Err(ServerError::Unauthorized("session revoked or expired".into()));
    }
    let access = issuer
        .issue_access(&claims.sub, &claims.sid, session.scopes)
        .map_err(map_auth_err)?;
    Ok(Json(serde_json::json!({
        "access_token": access,
        "token_type": "Bearer",
        "expires_in": issuer.access_ttl.num_seconds(),
    })))
}

pub async fn logout(
    State(state): State<AppState>,
    auth: AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    if auth.kind != crate::auth_context::AuthKind::Session {
        return Err(ServerError::Unauthorized(
            "only session bearers can logout".into(),
        ));
    }
    // The session UUID is encoded in the JWT; we need the session ID to
    // revoke. The middleware already validated it; pass it through via
    // request extension. For simplicity, look up the session by user
    // principal — for now we revoke ALL sessions of the principal.
    let (db, _) = require_auth_db(&state)?;
    let principal = auth.principal.ok_or_else(|| {
        ServerError::Internal("session auth without principal".into())
    })?;
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
        user_code: dc.user_code,
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
    use tiled_auth::device_code::DeviceStatus::*;
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
            let scopes = state.default_login_scopes.clone();
            let session = db
                .create_session(principal_id, scopes.clone(), Utc::now() + issuer.refresh_ttl)
                .await
                .map_err(map_auth_err)?;
            let access = issuer
                .issue_access(&principal.uuid, &session.uuid, scopes)
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
}

pub async fn device_approve(
    State(state): State<AppState>,
    auth: AuthContext,
    Json(body): Json<DeviceApproveRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let principal = auth.principal.ok_or_else(|| {
        ServerError::Unauthorized("login required to approve a device code".into())
    })?;
    let (db, _) = require_auth_db(&state)?;
    db.approve_device_code(&body.user_code, principal.id)
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
    auth.require(tiled_auth::Scope::ApiKeyCreate)?;
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
                let s = tiled_auth::Scope::parse(&name).ok_or_else(|| {
                    ServerError::Validation(format!("unknown scope: {name}"))
                })?;
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
    let keys = db
        .list_api_keys(principal_id)
        .await
        .map_err(map_auth_err)?;
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
    auth.require(tiled_auth::Scope::ApiKeyRevoke)?;
    let (db, _) = require_auth_db(&state)?;
    let principal = auth.principal.clone().ok_or_else(|| {
        ServerError::Unauthorized("login required to revoke an api key".into())
    })?;
    // Look up the key owner before revoking so a non-admin can't drop a
    // key that belongs to someone else (just having ApiKeyRevoke scope on
    // your own session would otherwise be enough).
    let candidates = db
        .list_api_keys(Some(principal.id))
        .await
        .map_err(map_auth_err)?;
    let allowed = candidates.iter().any(|k| k.first_eight == first_eight)
        || auth.scopes.contains(tiled_auth::Scope::Admin);
    if !allowed {
        return Err(ServerError::Forbidden(
            "api key does not belong to this principal".into(),
        ));
    }
    let _ = db
        .revoke_api_key(&first_eight)
        .await
        .map_err(map_auth_err)?;
    Ok(StatusCode::NO_CONTENT)
}

fn map_auth_err(e: tiled_auth::AuthError) -> ServerError {
    use tiled_auth::AuthError as AE;
    match e {
        AE::NotFound(m) => ServerError::NotFound(m),
        AE::Validation(m) => ServerError::Validation(m),
        AE::Conflict(m) => ServerError::Validation(m),
        AE::Unauthorized(m) => ServerError::Unauthorized(m),
        AE::Forbidden(m) => ServerError::Forbidden(m),
        AE::Expired => ServerError::Unauthorized("expired".into()),
        AE::Revoked => ServerError::Unauthorized("revoked".into()),
        other => ServerError::Internal(other.to_string()),
    }
}
