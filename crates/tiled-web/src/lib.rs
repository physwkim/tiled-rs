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
//! cookie value is a JWT signed by the same `Issuer` the API uses, so
//! the regular middleware already understands it.

mod admin;
mod assets;
mod cookie;

pub use admin::admin_router;
pub use assets::spa_router_with;
pub use cookie::{SESSION_COOKIE, build_session_cookie, clear_session_cookie};

use std::sync::Arc;

use axum::Router;

use tiled_auth::{AuthDb, Issuer, ScopeSet};

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
}

impl std::fmt::Debug for WebState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebState")
            .field("auth_db", &self.auth_db.as_ref().map(|_| "<set>"))
            .field("issuer", &self.issuer.as_ref().map(|_| "<set>"))
            .field("login_provider", &self.login_provider)
            .field("secure_cookies", &self.secure_cookies)
            .finish()
    }
}

/// Build the combined web router. Mount under whatever prefix you like;
/// the SPA fallback only fires for paths without a `/api/` or `/admin/`
/// prefix.
pub fn build_router(state: WebState) -> Router {
    let spa = assets::spa_router_with(state.assets_dir.clone());
    let admin = admin::admin_router(state);
    spa.merge(admin)
}
