//! Browser-side authentication for the SPA.
//!
//! Mirrors upstream tiled (PR #1350): JWT bearer tokens stored in
//! localStorage, attached to every API call as `Authorization: Bearer
//! <access>`, refreshed on 401 against `/api/v1/auth/refresh`. The
//! login flow itself lives in `pages/login.rs` — this module owns the
//! token store + reactive auth state used across the app.

pub mod context;
pub mod store;
pub mod types;

pub use context::{AuthState, provide_auth, use_auth};
pub use types::ProviderInfo;
