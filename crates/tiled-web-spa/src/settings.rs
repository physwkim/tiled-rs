//! Operator-supplied UI settings — fetched once from `/settings.json`
//! at app boot and exposed through Leptos context.
//!
//! Mirrors upstream tiled PR #1349's `spec_views` configuration: each
//! entry maps a *spec name* (matching `attributes.specs[].name`) to an
//! external viewer URL. The SPA renders a link / button per matched
//! spec on the catalog detail page; the URL may include `{path}` and
//! `{metadata}` placeholders that the SPA substitutes at click-time
//! (path = the resource segments, metadata = JSON-encoded
//! `attributes.metadata` per #1365).
//!
//! We do NOT implement upstream's dynamic `<script>` injection that
//! registers React components on `window.__TILED_SPEC_VIEWS__` — our
//! SPA is WASM/Leptos, so React-bundle plugins aren't usable. The
//! link-based contract is a pragmatic substitute that still lets
//! operators stitch in external viewers.

use gloo_net::http::Request;
use leptos::prelude::*;
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub spec_views: Vec<SpecView>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SpecView {
    pub spec: String,
    pub url: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Copy)]
pub struct SettingsState {
    pub spec_views: RwSignal<Vec<SpecView>>,
    pub loaded: RwSignal<bool>,
}

impl SettingsState {
    fn new() -> Self {
        Self {
            spec_views: RwSignal::new(Vec::new()),
            loaded: RwSignal::new(false),
        }
    }
}

pub fn provide_settings() {
    provide_context(SettingsState::new());
}

pub fn use_settings() -> SettingsState {
    use_context::<SettingsState>()
        .expect("SettingsState must be provided at the app root")
}

/// Fetch `/settings.json`. The endpoint is public on the server — no
/// bearer required — so this skips the auth context entirely.
pub async fn fetch_settings() -> Result<Settings, String> {
    let resp = Request::get("/settings.json")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json().await.map_err(|e| e.to_string())
}

/// Substitute `{path}` and `{metadata}` placeholders in the spec view URL.
/// `metadata_json` is the URL-encoded JSON of the resource's metadata
/// (or empty string if absent). Anything not matching either token
/// passes through verbatim.
pub fn render_url(template: &str, path: &str, metadata_json: &str) -> String {
    template
        .replace("{path}", path)
        .replace("{metadata}", metadata_json)
}
