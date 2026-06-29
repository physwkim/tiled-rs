//! REST API for managing webhooks (upstream tiled PR #1353).
//!
//! Endpoints:
//! * `POST   /api/v1/webhooks/target/{*path}`  — register a webhook on a node
//! * `GET    /api/v1/webhooks/target/{*path}`  — list webhooks registered on a node
//! * `DELETE /api/v1/webhooks/{webhook_id}`    — remove
//! * `GET    /api/v1/webhooks/history/{webhook_id}` — recent delivery history
//!
//! Write endpoints require `write:webhooks`. Read endpoints require
//! `read:webhooks`. By default both scopes are admin-only.

use axum::Json;
use axum::extract::{OriginalUri, Path, State};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::catalog::webhook::WebhookCreate;

use crate::server::auth_context::AuthContext;
use crate::server::error::ServerError;
use crate::server::extractors::PathSegments;
use crate::server::state::AppState;
use crate::server::webhook_dispatch::WebhookConfig;

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub url: String,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub events: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct WebhookResponse {
    pub id: i64,
    pub node_id: i64,
    pub url: String,
    pub events: Option<Vec<String>>,
    pub active: bool,
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// Whether a signing secret is set. The secret itself is never
    /// returned — caller stores it client-side at registration time.
    pub has_secret: bool,
}

impl From<crate::catalog::orm::Webhook> for WebhookResponse {
    fn from(w: crate::catalog::orm::Webhook) -> Self {
        Self {
            id: w.id,
            node_id: w.node_id,
            url: w.url,
            events: w.events,
            active: w.active,
            time_created: w.time_created,
            has_secret: w.secret.is_some(),
        }
    }
}

fn require_catalog(state: &AppState) -> Result<&crate::catalog::db::Catalog, ServerError> {
    state.catalog.as_ref().ok_or_else(|| {
        ServerError::Validation("server has no catalog DB; webhooks not supported".into())
    })
}

fn parse_path_segments(uri: &axum::http::Uri, prefix: &str) -> Vec<String> {
    PathSegments::from_raw_path(uri.path(), prefix).0
}

pub async fn register(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: AuthContext,
    Json(req): Json<RegisterRequest>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteWebhooks)?;
    let cfg = state.webhook_config.as_ref().cloned().unwrap_or_default();
    cfg.validate_url(&req.url)?;
    let segments = parse_path_segments(&uri, "/api/v1/webhooks/target/");
    let catalog = require_catalog(&state)?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    let created = catalog
        .create_webhook(WebhookCreate {
            node_id: node.id,
            url: req.url,
            secret: req.secret,
            events: req.events,
        })
        .await
        .map_err(map_err)?;
    Ok(Json(WebhookResponse::from(created)))
}

pub async fn list(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadWebhooks)?;
    let segments = parse_path_segments(&uri, "/api/v1/webhooks/target/");
    let catalog = require_catalog(&state)?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    let webhooks = catalog
        .list_webhooks_for_node(node.id)
        .await
        .map_err(map_err)?;
    Ok(Json(
        webhooks
            .into_iter()
            .map(WebhookResponse::from)
            .collect::<Vec<_>>(),
    ))
}

pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    auth: AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteWebhooks)?;
    let catalog = require_catalog(&state)?;
    let removed = catalog.delete_webhook(id).await.map_err(map_err)?;
    if !removed {
        return Err(ServerError::NotFound(format!("webhook {id} not found")));
    }
    Ok(Json(serde_json::json!({"deleted": id})))
}

pub async fn history(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    auth: AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadWebhooks)?;
    let catalog = require_catalog(&state)?;
    if catalog.get_webhook(id).await.map_err(map_err)?.is_none() {
        return Err(ServerError::NotFound(format!("webhook {id} not found")));
    }
    let deliveries = catalog
        .list_deliveries_for_webhook(id, 100)
        .await
        .map_err(map_err)?;
    Ok(Json(deliveries))
}

fn map_err(e: crate::catalog::error::CatalogError) -> ServerError {
    use crate::catalog::error::CatalogError;
    match e {
        CatalogError::NotFound(m) => ServerError::NotFound(m),
        CatalogError::Validation(m) => ServerError::Validation(m),
        other => ServerError::Internal(other.to_string()),
    }
}

impl WebhookConfig {
    fn validate_url(&self, url: &str) -> Result<(), ServerError> {
        let parsed = url::Url::parse(url)
            .map_err(|e| ServerError::Validation(format!("Invalid webhook URL: {e}")))?;
        if !self.allow_http && parsed.scheme() != "https" {
            return Err(ServerError::Validation(
                "Webhook URL must use HTTPS (set webhook.allow_http=true to override)".into(),
            ));
        }
        if !self.allow_private_addresses
            && let Some(host) = parsed.host_str()
            && is_private_or_reserved(host)
        {
            return Err(ServerError::Validation(
                "Webhook URL targets a private/reserved address \
                 (set webhook.allow_private_addresses=true to override)"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// True for IP literals or hostnames that resolve to (or are) loopback,
/// link-local, RFC 1918 private, or otherwise non-public ranges. We
/// only inspect the URL's host text — DNS-rebinding-safe SSRF needs an
/// egress proxy, called out in the upstream PR's docs.
fn is_private_or_reserved(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // Strip surrounding `[]` for IPv6 literals, then try IP parse.
    let trimmed = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
        return ip_is_blocked(ip);
    }
    // Not an IP literal — caller didn't enable allow_private_addresses,
    // so we trust the URL's host string but warn that DNS rebinding can
    // still hit private space at request time. (Not enforced here.)
    false
}

fn ip_is_blocked(ip: std::net::IpAddr) -> bool {
    use std::net::{Ipv4Addr, Ipv6Addr};
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.octets()[0] == 0
                // 100.64.0.0/10 (CGNAT)
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xC0) == 64)
                || v4 == Ipv4Addr::new(169, 254, 169, 254) // EC2 metadata
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                // ULA fc00::/7 + link-local fe80::/10
                || (v6.segments()[0] & 0xFE00) == 0xFC00
                || (v6.segments()[0] & 0xFFC0) == 0xFE80
                || v6 == Ipv6Addr::LOCALHOST
        }
    }
}
