//! Browser-facing surface for tiled-rs.
//!
//! Two halves:
//! - **SPA shell** (`/`, `/static/*`): a placeholder bundle that
//!   operators replace with the prebuilt
//!   [bluesky/tiled](https://github.com/bluesky/tiled) WebUI assets.
//!   Drop the bundle into `crates/tiled-web/assets/spa/` and rebuild,
//!   or pass `--web-assets-dir <path>` to point at a directory on disk.
//! - **Admin shell** (`/admin/*`): server-rendered HTML covering API
//!   key management, session listing, and streaming bus inspection.
//!   No JS framework — works without scripts enabled.
//!
//! Auth: the admin shell uses cookie-based sessions (`tiled_session=...
//! HttpOnly`) so a browser doesn't have to juggle bearer tokens. The
//! cookie value is a JWT signed by the same `Issuer` the API uses, but
//! the cookie authenticates **only** the server-rendered `/admin/*`
//! pages (via the admin shell's own session resolver). The API auth
//! middleware never reads `tiled_session` — it accepts the token only as
//! `Authorization: Bearer`, `Apikey`/`?api_key=`, or the proxied header.

mod admin;
mod assets;
mod cookie;

pub use admin::admin_router;
pub use assets::spa_router_with;
pub use cookie::{SESSION_COOKIE, build_session_cookie, clear_session_cookie};

use std::sync::Arc;

use axum::Router;

use tiled_auth::{AuthDb, Authenticator, Issuer, ScopeSet};

/// Minimal slice of server state the web layer needs. Constructed by
/// the host (`tiled-server::AppState`) and passed in. Avoids the
/// `tiled-web` ↔ `tiled-server` cycle a direct AppState dependency would
/// create.
#[derive(Clone)]
pub struct WebState {
    pub auth_db: Option<AuthDb>,
    pub issuer: Option<Issuer>,
    pub default_login_scopes: ScopeSet,
    /// Provider name shown on the login form (e.g. "dummy", "entra").
    pub login_provider: String,
    /// Closure that reports the current StreamingBus channel count.
    pub channel_count_fn: Arc<dyn Fn() -> usize + Send + Sync>,
    /// Set Secure on the session cookie. Off for plain-HTTP demos; on
    /// once the deployment serves HTTPS (or sits behind a TLS proxy
    /// with `--trust-proxy`).
    pub secure_cookies: bool,
    /// Optional directory the SPA assets are served from on disk. When
    /// `None`, the compiled-in placeholder bundle is used. Operators
    /// drop the prebuilt bluesky/tiled WebUI bundle here to swap the
    /// UI without recompiling.
    pub assets_dir: Option<std::path::PathBuf>,
    /// Spec-view config returned by `GET /settings.json`. Operators
    /// can populate this from YAML config to register external viewers
    /// for specific tag specs (e.g. {"BlueskyRunV1": "https://…"}).
    /// Mirrors upstream tiled PR #1349's `spec_views` settings entry.
    pub spec_views: Vec<SpecViewEntry>,
    /// Authenticator for the `/admin/login` form. `None` disables the
    /// admin login — every attempt returns auth-failed.
    pub authenticator: Option<Arc<dyn Authenticator>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SpecViewEntry {
    /// Spec name to match against `attributes.specs[].name`.
    pub spec: String,
    /// External viewer URL. May contain `{path}` and `{metadata}`
    /// placeholders that the SPA substitutes before navigating.
    pub url: String,
    /// Display label for the link/button. Defaults to "Open in <spec>".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl std::fmt::Debug for WebState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebState")
            .field("auth_db", &self.auth_db.as_ref().map(|_| "<set>"))
            .field("issuer", &self.issuer.as_ref().map(|_| "<set>"))
            .field("login_provider", &self.login_provider)
            .field("secure_cookies", &self.secure_cookies)
            .field(
                "authenticator",
                &self.authenticator.as_ref().map(|_| "<set>"),
            )
            .finish()
    }
}

/// Build the combined web router. Mount under whatever prefix you like;
/// the SPA fallback only fires for paths without a `/api/` or `/admin/`
/// prefix.
pub fn build_router(state: WebState) -> Router {
    let spa = assets::spa_router_with(state.assets_dir.clone());
    let settings_json = build_settings_json(&state.spec_views);
    let settings = Router::new().route(
        "/settings.json",
        axum::routing::get(move || {
            let body = settings_json.clone();
            async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    body,
                )
            }
        }),
    );
    let admin = admin::admin_router(state);
    spa.merge(settings).merge(admin)
}

fn build_settings_json(spec_views: &[SpecViewEntry]) -> String {
    serde_json::to_string(&serde_json::json!({
        "spec_views": spec_views,
    }))
    .unwrap_or_else(|_| "{\"spec_views\":[]}".into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tiled_auth::ScopeSet;

    use super::*;

    fn minimal_state(login_provider: &str) -> WebState {
        WebState {
            auth_db: None,
            issuer: None,
            default_login_scopes: ScopeSet::default(),
            login_provider: login_provider.into(),
            channel_count_fn: Arc::new(|| 0),
            secure_cookies: false,
            assets_dir: None,
            spec_views: Vec::new(),
            authenticator: None,
        }
    }

    #[test]
    fn build_router_accepts_alphanumeric_provider() {
        let _ = build_router(minimal_state("dummy"));
        let _ = build_router(minimal_state("my-provider_1"));
    }

    #[test]
    #[should_panic(expected = "login_provider must contain only ASCII")]
    fn build_router_panics_on_control_char_in_provider() {
        let _ = build_router(minimal_state("bad\x00provider"));
    }

    #[test]
    #[should_panic(expected = "login_provider must contain only ASCII")]
    fn build_router_panics_on_space_in_provider() {
        let _ = build_router(minimal_state("bad provider"));
    }
}
