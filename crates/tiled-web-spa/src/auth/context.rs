//! Reactive auth state held in Leptos context.
//!
//! Components read it via `use_auth()`. `provide_auth()` wires the
//! state into the `<Router>` subtree at app boot.

use leptos::prelude::*;

use super::store;
use super::types::{ProviderInfo, UserIdentity};

#[derive(Clone, Copy)]
pub struct AuthState {
    /// The list of authentication providers advertised by the server.
    /// Empty until `/api/v1/` resolves; afterwards drives the login UI.
    pub providers: RwSignal<Vec<ProviderInfo>>,
    /// `true` once the server's `/api/v1/` response has been seen.
    /// Login page waits on this to know which providers to render.
    pub loaded: RwSignal<bool>,
    /// Whether `authentication.required` was set on the server.
    pub required: RwSignal<bool>,
    /// Cached identity of the logged-in user. `None` while anonymous.
    pub identity: RwSignal<Option<UserIdentity>>,
    /// Reactive copy of the access token. Bumping it forces dependent
    /// resources (e.g. `Authorization` header capture) to re-fetch.
    pub access_token: RwSignal<Option<String>>,
}

impl AuthState {
    fn new() -> Self {
        Self {
            providers: RwSignal::new(Vec::new()),
            loaded: RwSignal::new(false),
            required: RwSignal::new(false),
            identity: RwSignal::new(store::get_identity()),
            access_token: RwSignal::new(store::get_access()),
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.access_token.with(|t| t.is_some())
    }

    /// Persist tokens to localStorage and update reactive state. Call
    /// after a successful login or refresh.
    pub fn record_login(
        &self,
        access: &str,
        refresh: &str,
        identity: Option<UserIdentity>,
    ) {
        store::save_tokens(access, refresh, identity.as_ref());
        self.access_token.set(Some(access.to_string()));
        if let Some(id) = identity {
            self.identity.set(Some(id));
        }
    }

    /// Update only the access token (refresh-grant path).
    pub fn record_refresh(&self, access: &str) {
        store::save_access(access);
        self.access_token.set(Some(access.to_string()));
    }

    /// Drop in-memory + persisted credentials. Use on logout or when
    /// the refresh path returns 401.
    pub fn clear(&self) {
        store::clear();
        self.access_token.set(None);
        self.identity.set(None);
    }
}

pub fn provide_auth() {
    provide_context(AuthState::new());
}

pub fn use_auth() -> AuthState {
    use_context::<AuthState>()
        .expect("AuthState must be provided at the app root via provide_auth()")
}
