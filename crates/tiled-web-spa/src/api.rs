//! Thin client wrappers around the tiled-rs HTTP API.
//!
//! Uses gloo-net's fetch + a small bearer-token attaching helper that
//! mirrors upstream tiled's axios interceptor (PR #1350): every request
//! gets `Authorization: Bearer <access>` if the auth context has a
//! token, and a 401 transparently triggers `/auth/refresh` once before
//! the call is retried.

use gloo_net::http::{Request, RequestBuilder, Response};
use leptos::prelude::*;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Deserialize;

use crate::auth::types::{LoginResponse, RefreshResponse};
use crate::auth::{AuthState, store};

/// Characters to percent-encode inside a path segment (per RFC 3986
/// `pchar` minus `unreserved` and `sub-delims`). Identical to the
/// tiled-client `PATH_SEGMENT` set — the SPA had the same defect as old
/// client M1.
const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'#')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b'%');

/// Percent-encode each `/`-separated segment of a node path so a key
/// containing `?`, `#`, `/`, `%`, a space, etc. cannot reshape the URL.
/// The separators between segments are preserved; an empty path stays empty.
fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|seg| utf8_percent_encode(seg, PATH_SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// Single-shot bearer-attaching GET. Caller passes the auth state so we
/// can both attach the latest token and update it on refresh.
async fn authed_get(state: &AuthState, url: &str) -> Result<Response, String> {
    send_with_refresh(state, || build_get(state, url)).await
}

fn build_get(state: &AuthState, url: &str) -> RequestBuilder {
    let mut req = Request::get(url);
    if let Some(token) = state.access_token.get_untracked() {
        req = req.header("Authorization", &format!("Bearer {token}"));
    }
    req
}

/// Run `make_request` once; if the response is 401 and a refresh token
/// is available, exchange it for a new access token and retry exactly
/// once. On second 401 (or no refresh token) clear auth state and let
/// the caller surface the error.
async fn send_with_refresh<F>(state: &AuthState, make_request: F) -> Result<Response, String>
where
    F: Fn() -> RequestBuilder,
{
    let resp = make_request().send().await.map_err(|e| e.to_string())?;
    if resp.status() != 401 {
        return Ok(resp);
    }
    let Some(refresh) = store::get_refresh() else {
        state.clear();
        return Ok(resp);
    };
    if !try_refresh(state, &refresh).await {
        state.clear();
        return Ok(resp);
    }
    make_request().send().await.map_err(|e| e.to_string())
}

async fn try_refresh(state: &AuthState, refresh: &str) -> bool {
    let body = serde_json::json!({ "refresh_token": refresh });
    let req = match Request::post("/api/v1/auth/refresh").json(&body) {
        Ok(r) => r,
        Err(_) => return false,
    };
    let Ok(resp) = req.send().await else {
        return false;
    };
    if !resp.ok() {
        return false;
    }
    let Ok(parsed): Result<RefreshResponse, _> = resp.json().await else {
        return false;
    };
    state.record_refresh(&parsed.access_token);
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct AboutResponse {
    pub api_version: u32,
    pub library_version: String,
    pub queries: Vec<String>,
    #[serde(default)]
    pub authentication: AboutAuthentication,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AboutAuthentication {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub providers: Vec<crate::auth::ProviderInfo>,
}

pub async fn fetch_about(state: &AuthState) -> Result<AboutResponse, String> {
    let resp = authed_get(state, "/api/v1/").await?;
    resp.json().await.map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceEnvelope {
    pub data: ResourceData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceData {
    pub id: String,
    pub attributes: ResourceAttributes,
    #[serde(default)]
    pub links: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceAttributes {
    pub structure_family: Option<String>,
    #[serde(default)]
    pub specs: Vec<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub structure: Option<serde_json::Value>,
    #[serde(default)]
    pub ancestors: Vec<String>,
}

pub async fn fetch_metadata(state: &AuthState, path: &str) -> Result<ResourceEnvelope, String> {
    let url = if path.is_empty() {
        "/api/v1/metadata/".to_string()
    } else {
        format!("/api/v1/metadata/{}", encode_path(path))
    };
    let resp = authed_get(state, &url).await?;
    resp.json().await.map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchEnvelope {
    pub data: Vec<ResourceData>,
    #[serde(default)]
    pub meta: serde_json::Value,
    #[serde(default)]
    pub links: SearchLinks,
}

/// Pagination links from a search response. Only `next` is consumed — the
/// SPA walks it forward until the server stops supplying one.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SearchLinks {
    #[serde(default)]
    pub next: Option<String>,
}

/// Fetch every child of a container. The path is percent-encoded per segment,
/// and the server-supplied `links.next` cursor is followed until exhausted, so
/// a container with more than one page of children is no longer silently
/// truncated at 100 items. (Page-by-page UI navigation — upstream PR #1392 —
/// is a separate feature; this loads the full child set into one envelope.)
pub async fn fetch_children(state: &AuthState, path: &str) -> Result<SearchEnvelope, String> {
    let encoded = encode_path(path);
    let mut url = if encoded.is_empty() {
        "/api/v1/search/?page[limit]=100".to_string()
    } else {
        format!("/api/v1/search/{encoded}?page[limit]=100")
    };

    let mut all: Vec<ResourceData> = Vec::new();
    let mut meta = serde_json::Value::Null;
    loop {
        let resp = authed_get(state, &url).await?;
        let page: SearchEnvelope = resp.json().await.map_err(|e| e.to_string())?;
        if meta.is_null() {
            meta = page.meta;
        }
        let got = page.data.len();
        all.extend(page.data);
        // Advance only while the cursor moves forward and the last page
        // actually returned rows — guards against a server that hands back a
        // stuck `next` link (otherwise this would loop forever).
        match page.links.next {
            Some(next) if got > 0 => url = next,
            _ => break,
        }
    }

    Ok(SearchEnvelope {
        data: all,
        meta,
        links: SearchLinks::default(),
    })
}

/// Fetch a binary payload (raw bytes / image) with the bearer token
/// attached + 401-refresh handling. Used by the array viewer for both
/// `Accept: image/png` and `Accept: application/octet-stream` paths.
pub async fn fetch_bytes(state: &AuthState, url: &str) -> Result<Vec<u8>, String> {
    let resp = authed_get(state, url).await?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.binary().await.map_err(|e| e.to_string())
}

/// POST `/api/v1/auth/{provider}/login` with username + password.
pub async fn login(
    state: &AuthState,
    auth_endpoint: &str,
    username: &str,
    password: &str,
) -> Result<LoginResponse, String> {
    let body = serde_json::json!({
        "username": username,
        "password": password,
    });
    let resp = Request::post(auth_endpoint)
        .json(&body)
        .map_err(|e| e.to_string())?
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.ok() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("login failed ({status}): {body}"));
    }
    let parsed: LoginResponse = resp.json().await.map_err(|e| e.to_string())?;
    state.record_login(
        &parsed.access_token,
        &parsed.refresh_token,
        parsed.identity.clone(),
    );
    Ok(parsed)
}

/// POST `/api/v1/auth/logout` with the bearer token. Best-effort —
/// regardless of server outcome we drop local state.
pub async fn logout(state: &AuthState) {
    if let Some(token) = state.access_token.get_untracked() {
        let _ = Request::post("/api/v1/auth/logout")
            .header("Authorization", &format!("Bearer {token}"))
            .send()
            .await;
    }
    state.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    // client-L1: a key with URL-significant characters must be encoded so it
    // cannot reshape the request URL; the `/` hierarchy separators are kept.
    #[test]
    fn encode_path_encodes_special_chars_per_segment() {
        assert_eq!(encode_path(""), "");
        assert_eq!(encode_path("a/b/c"), "a/b/c");
        // Within a key: space, ?, #, % are all encoded.
        assert_eq!(encode_path("a b"), "a%20b");
        assert_eq!(encode_path("scan?1"), "scan%3F1");
        assert_eq!(encode_path("we#1"), "we%231");
        assert_eq!(encode_path("100%"), "100%25");
        // A `/` inside a key is encoded so it does not look like a separator,
        // while genuine separators between segments stay literal.
        assert_eq!(encode_path("a/b c/d#e"), "a/b%20c/d%23e");
    }
}
