//! localStorage-backed token store. Pure persistence — no Leptos
//! reactivity here; that lives in `context.rs`.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use gloo_storage::{LocalStorage, Storage};
use serde::{Deserialize, Serialize};

use super::types::UserIdentity;

const ACCESS_KEY: &str = "tiled_access_token";
const REFRESH_KEY: &str = "tiled_refresh_token";
const IDENTITY_KEY: &str = "tiled_identity";

pub fn save_tokens(access: &str, refresh: &str, identity: Option<&UserIdentity>) {
    let _ = LocalStorage::set(ACCESS_KEY, access);
    let _ = LocalStorage::set(REFRESH_KEY, refresh);
    if let Some(id) = identity {
        let _ = LocalStorage::set(IDENTITY_KEY, id);
    }
}

pub fn save_access(access: &str) {
    let _ = LocalStorage::set(ACCESS_KEY, access);
}

pub fn get_access() -> Option<String> {
    LocalStorage::get(ACCESS_KEY).ok()
}

pub fn get_refresh() -> Option<String> {
    LocalStorage::get(REFRESH_KEY).ok()
}

pub fn get_identity() -> Option<UserIdentity> {
    LocalStorage::get(IDENTITY_KEY).ok()
}

pub fn clear() {
    LocalStorage::delete(ACCESS_KEY);
    LocalStorage::delete(REFRESH_KEY);
    LocalStorage::delete(IDENTITY_KEY);
}

/// JWT `exp` claim (seconds since epoch). Returns `None` if the token
/// can't be parsed — caller should treat that as "expired/unknown" and
/// fall back to reactive 401 → refresh.
#[derive(Debug, Deserialize, Serialize)]
struct JwtPayload {
    #[serde(default)]
    exp: Option<i64>,
}

pub fn access_exp(token: &str) -> Option<i64> {
    let payload_b64 = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let payload: JwtPayload = serde_json::from_slice(&bytes).ok()?;
    payload.exp
}

/// `true` if the token expires within `buffer_secs` from now (or has
/// already expired). Used by the auth context to decide whether to
/// kick off a proactive refresh on app boot.
pub fn access_is_stale(token: &str, buffer_secs: i64) -> bool {
    let Some(exp) = access_exp(token) else {
        return true;
    };
    let now = (js_sys::Date::now() / 1000.0) as i64;
    now + buffer_secs >= exp
}
