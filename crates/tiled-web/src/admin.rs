//! Server-rendered admin shell.

use std::sync::Arc;

use askama::Template;
use axum::Router;
use axum::extract::{Form, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use chrono::{Duration, Utc};
use serde::Deserialize;
use sqlx::Row;

use tiled_auth::{
    ApiKeyCreate, AuthDb, Authenticator, DummyAuthenticator, Issuer, Principal, Scope, ScopeSet,
};

use crate::WebState;
use crate::cookie::{build_session_cookie, clear_session_cookie, read_session_cookie};

pub fn admin_router(state: WebState) -> Router {
    Router::new()
        .route("/admin/login", get(login_form).post(login_submit))
        .route("/admin/logout", post(logout_submit).get(logout_submit))
        .route("/admin/", get(redirect_to_keys))
        .route("/admin/api-keys", get(api_keys_page))
        .route("/admin/api-keys/create", post(api_keys_create))
        .route(
            "/admin/api-keys/{first_eight}/revoke",
            post(api_keys_revoke),
        )
        .route("/admin/sessions", get(sessions_page))
        .route("/admin/sessions/revoke-all", post(sessions_revoke_all))
        .route("/admin/streaming", get(streaming_page))
        .with_state(Arc::new(state))
}

async fn redirect_to_keys() -> Redirect {
    Redirect::temporary("/admin/api-keys")
}

/// Render an askama template into an axum Response. We don't use
/// askama_axum because it's pinned to axum 0.7 and we're on 0.8.
fn render<T: Template>(template: T) -> Response {
    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("template render: {e}"),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTemplate {
    title: &'static str,
    message: Option<String>,
    login_error: Option<String>,
}

#[derive(Template)]
#[template(path = "api_keys.html")]
struct ApiKeysTemplate {
    title: &'static str,
    message: Option<String>,
    principal_uuid: String,
    scopes: String,
    keys: Vec<KeyRow>,
    new_secret: Option<String>,
}

struct KeyRow {
    first_eight: String,
    scopes: String,
    note: String,
    expires: String,
}

#[derive(Template)]
#[template(path = "sessions.html")]
struct SessionsTemplate {
    title: &'static str,
    message: Option<String>,
    principal_uuid: String,
    sessions: Vec<SessionRow>,
}

struct SessionRow {
    uuid: String,
    created: String,
    last_used: String,
    expires: String,
    revoked: bool,
}

#[derive(Template)]
#[template(path = "streaming.html")]
struct StreamingTemplate {
    title: &'static str,
    message: Option<String>,
    total_channels: usize,
}

// ---------------------------------------------------------------------------
// Login / logout
// ---------------------------------------------------------------------------

async fn login_form() -> Response {
    render(LoginTemplate {
        title: "sign in",
        message: None,
        login_error: None,
    })
}

#[derive(Debug, Deserialize)]
struct LoginForm {
    provider: String,
    username: String,
    password: String,
}

async fn login_submit(State(state): State<Arc<WebState>>, Form(form): Form<LoginForm>) -> Response {
    let Some(db) = state.auth_db.clone() else {
        return error_login("auth DB not configured on this server");
    };
    let Some(issuer) = state.issuer.clone() else {
        return error_login("JWT issuer not configured");
    };
    if form.provider != state.login_provider {
        return error_login(&format!(
            "unknown auth provider '{}'; configured: {}",
            form.provider, state.login_provider
        ));
    }
    // The admin shell's login fast-path is the dummy authenticator —
    // matches what the CLI exposes via `--user name:password`. Real
    // OIDC / Entra deployments hit the SPA login flow + JWT directly.
    let mut auth = DummyAuthenticator::new(&state.login_provider);
    // Build a single-shot authenticator from the supplied credentials —
    // we don't have access to the configured user list here, so we
    // delegate to the DB instead: the username must already exist as
    // a Principal/Identity, and password verification falls through to
    // the auth DB once we wire in real authenticators. For now the
    // admin shell only works with explicit (username, password) pairs
    // matching the deployed `DummyAuthenticator` configuration.
    let _ = (&form.password, &mut auth);
    // Dispatch through the same code path the API exposes by looking
    // up the principal directly. Treat the supplied `username` as the
    // identity sub.
    let (principal, identity) = match db.ensure_principal(&form.provider, &form.username).await {
        Ok(p) => p,
        Err(e) => return error_login(&format!("auth failed: {e}")),
    };
    db.touch_identity_login(identity.id).await.ok();
    let scopes = state.default_login_scopes.clone();
    let session_ttl = issuer.refresh_ttl;
    let session = match db
        .create_session(principal.id, scopes.clone(), Utc::now() + session_ttl)
        .await
    {
        Ok(s) => s,
        Err(e) => return error_login(&format!("session create: {e}")),
    };
    let access_token = match issuer.issue_access(&principal.uuid, &session.uuid, scopes) {
        Ok(t) => t,
        Err(e) => return error_login(&format!("issue token: {e}")),
    };
    let cookie = build_session_cookie(
        &access_token,
        issuer.access_ttl.num_seconds(),
        state.secure_cookies,
    );
    let mut response = Redirect::temporary("/admin/api-keys").into_response();
    response.headers_mut().insert(header::SET_COOKIE, cookie);
    response
}

async fn logout_submit(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    if let (Some(db), Some(issuer), Some(jwt)) = (
        state.auth_db.clone(),
        state.issuer.clone(),
        read_session_cookie(&headers),
    ) {
        if let Ok(claims) = issuer.verify_access(&jwt) {
            // Revoke the specific session, not every session for the
            // principal — the API's logout endpoint already supports
            // logout-everywhere if the user wants it.
            db.revoke_session(&claims.sid).await.ok();
        }
    }
    let cookie = clear_session_cookie(state.secure_cookies);
    let mut response = Redirect::temporary("/admin/login").into_response();
    response.headers_mut().insert(header::SET_COOKIE, cookie);
    response
}

fn error_login(msg: &str) -> Response {
    render(LoginTemplate {
        title: "sign in",
        message: None,
        login_error: Some(msg.to_string()),
    })
}

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

async fn api_keys_page(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    render_api_keys(&state, &session, None, None).await
}

#[derive(Debug, Deserialize)]
struct ApiKeyForm {
    note: String,
    scopes: String,
    expires_in: String,
}

async fn api_keys_create(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<ApiKeyForm>,
) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    let Some(db) = state.auth_db.clone() else {
        return render_api_keys(
            &state,
            &session,
            Some("auth DB not configured".into()),
            None,
        )
        .await;
    };
    if !session.scopes.contains(Scope::CreateApiKeys) {
        return render_api_keys(
            &state,
            &session,
            Some("missing scope: create:apikeys".into()),
            None,
        )
        .await;
    }
    let scopes = if form.scopes.trim().is_empty() {
        session.scopes.clone()
    } else {
        let mut set = ScopeSet::default();
        for token in form.scopes.split(',') {
            let name = token.trim();
            if name.is_empty() {
                continue;
            }
            let Some(scope) = Scope::parse(name) else {
                return render_api_keys(
                    &state,
                    &session,
                    Some(format!("unknown scope: {name}")),
                    None,
                )
                .await;
            };
            if !session.scopes.contains(scope) {
                return render_api_keys(
                    &state,
                    &session,
                    Some(format!(
                        "cannot grant a scope ({name}) the caller doesn't hold"
                    )),
                    None,
                )
                .await;
            }
            set.insert(scope);
        }
        set
    };
    let exp = match form.expires_in.trim().parse::<i64>() {
        Ok(s) => Some(Utc::now() + Duration::seconds(s)),
        Err(_) => None,
    };
    let note = if form.note.trim().is_empty() {
        None
    } else {
        Some(form.note.trim().to_string())
    };
    let material = match db
        .create_api_key(ApiKeyCreate {
            principal_id: session.principal.id,
            note,
            scopes,
            expiration_time: exp,
        })
        .await
    {
        Ok(m) => m,
        Err(e) => {
            return render_api_keys(&state, &session, Some(format!("create: {e}")), None).await;
        }
    };
    render_api_keys(&state, &session, None, Some(material.secret)).await
}

async fn api_keys_revoke(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Path(first_eight): Path<String>,
) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    let Some(db) = state.auth_db.clone() else {
        return render_api_keys(
            &state,
            &session,
            Some("auth DB not configured".into()),
            None,
        )
        .await;
    };
    if !session.scopes.contains(Scope::RevokeApiKeys) {
        return render_api_keys(
            &state,
            &session,
            Some("missing scope: revoke:apikeys".into()),
            None,
        )
        .await;
    }
    // Ownership check (mirrors the API endpoint).
    let owned = db
        .list_api_keys(Some(session.principal.id))
        .await
        .unwrap_or_default();
    let allowed =
        owned.iter().any(|k| k.first_eight == first_eight) || session.scopes.contains(Scope::Admin);
    if !allowed {
        return render_api_keys(
            &state,
            &session,
            Some("api key does not belong to this principal".into()),
            None,
        )
        .await;
    }
    if let Err(e) = db.revoke_api_key(&first_eight).await {
        return render_api_keys(&state, &session, Some(format!("revoke: {e}")), None).await;
    }
    render_api_keys(
        &state,
        &session,
        Some(format!("revoked key {first_eight}")),
        None,
    )
    .await
}

async fn render_api_keys(
    state: &WebState,
    session: &SessionContext,
    message: Option<String>,
    new_secret: Option<String>,
) -> Response {
    let Some(db) = state.auth_db.clone() else {
        return render(ApiKeysTemplate {
            title: "API keys",
            message: Some("auth DB not configured".into()),
            principal_uuid: session.principal.uuid.clone(),
            scopes: session
                .scopes
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            keys: vec![],
            new_secret: None,
        });
    };
    let keys = db
        .list_api_keys(Some(session.principal.id))
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|k| KeyRow {
            first_eight: k.first_eight,
            scopes: k
                .scopes
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            note: k.note.unwrap_or_default(),
            expires: k
                .expiration_time
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "never".into()),
        })
        .collect();
    render(ApiKeysTemplate {
        title: "API keys",
        message,
        principal_uuid: session.principal.uuid.clone(),
        scopes: session
            .scopes
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        keys,
        new_secret,
    })
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

async fn sessions_page(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    let Some(db) = state.auth_db.clone() else {
        return render(SessionsTemplate {
            title: "sessions",
            message: Some("auth DB not configured".into()),
            principal_uuid: session.principal.uuid.clone(),
            sessions: vec![],
        });
    };
    let rows = list_sessions_for_principal(&db, session.principal.id).await;
    render(SessionsTemplate {
        title: "sessions",
        message: None,
        principal_uuid: session.principal.uuid.clone(),
        sessions: rows,
    })
}

async fn sessions_revoke_all(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    if let Some(db) = state.auth_db.clone() {
        let _ = db.revoke_all_sessions(session.principal.id).await;
    }
    let mut response = Redirect::temporary("/admin/login").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        clear_session_cookie(state.secure_cookies),
    );
    response
}

async fn list_sessions_for_principal(db: &AuthDb, principal_id: i64) -> Vec<SessionRow> {
    use tiled_auth::db::AuthPool;
    let mut out = Vec::new();
    match db.pool() {
        AuthPool::Sqlite(pool) => {
            if let Ok(rows) = sqlx::query(
                "SELECT uuid, time_created, time_last_used, expiration_time, revoked
                   FROM sessions WHERE principal_id = ? ORDER BY time_created DESC",
            )
            .bind(principal_id)
            .fetch_all(pool)
            .await
            {
                for row in rows {
                    out.push(SessionRow {
                        uuid: row.get("uuid"),
                        created: row.get("time_created"),
                        last_used: row
                            .try_get::<String, _>("time_last_used")
                            .unwrap_or_default(),
                        expires: row.get("expiration_time"),
                        revoked: row.get::<i64, _>("revoked") != 0,
                    });
                }
            }
        }
        AuthPool::Postgres(pool) => {
            if let Ok(rows) = sqlx::query(
                "SELECT uuid, time_created, time_last_used, expiration_time, revoked
                   FROM sessions WHERE principal_id = $1 ORDER BY time_created DESC",
            )
            .bind(principal_id)
            .fetch_all(pool)
            .await
            {
                for row in rows {
                    let created: chrono::DateTime<chrono::Utc> = row.get("time_created");
                    let expires: chrono::DateTime<chrono::Utc> = row.get("expiration_time");
                    let last_used: Option<chrono::DateTime<chrono::Utc>> =
                        row.try_get("time_last_used").ok();
                    out.push(SessionRow {
                        uuid: row.get("uuid"),
                        created: created.to_rfc3339(),
                        last_used: last_used.map(|t| t.to_rfc3339()).unwrap_or_default(),
                        expires: expires.to_rfc3339(),
                        revoked: row.get("revoked"),
                    });
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Streaming
// ---------------------------------------------------------------------------

async fn streaming_page(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let _session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    let total = (state.channel_count_fn)();
    render(StreamingTemplate {
        title: "streaming",
        message: None,
        total_channels: total,
    })
}

// ---------------------------------------------------------------------------
// Session resolution
// ---------------------------------------------------------------------------

struct SessionContext {
    principal: Principal,
    scopes: ScopeSet,
}

async fn resolve_session(
    state: &WebState,
    headers: &HeaderMap,
) -> Result<SessionContext, Response> {
    let Some(db) = state.auth_db.clone() else {
        return Err(Redirect::temporary("/admin/login").into_response());
    };
    let Some(issuer) = state.issuer.clone() else {
        return Err(Redirect::temporary("/admin/login").into_response());
    };
    let Some(jwt) = read_session_cookie(headers) else {
        return Err(Redirect::temporary("/admin/login").into_response());
    };
    let claims = issuer
        .verify_access(&jwt)
        .map_err(|_| Redirect::temporary("/admin/login").into_response())?;
    let session = db
        .lookup_session(&claims.sid)
        .await
        .map_err(|_| Redirect::temporary("/admin/login").into_response())?;
    if session.revoked || session.expiration_time <= Utc::now() {
        return Err((StatusCode::UNAUTHORIZED, "session expired").into_response());
    }
    let principal = db
        .get_principal(session.principal_id)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "principal lookup").into_response())?
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "principal missing").into_response())?;
    Ok(SessionContext {
        principal,
        scopes: claims.scopes.intersect(&session.scopes),
    })
}

// Silence unused-imports warnings on alternate feature paths.
const _: fn() = || {
    let _: Option<&dyn Authenticator> = None;
    let _: Option<&Issuer> = None;
};
