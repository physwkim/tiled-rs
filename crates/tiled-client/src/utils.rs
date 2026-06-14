//! Helpers shared across client modules.
//!
//! Mirrors `tiled/client/utils.py` — MIME type constants, error decoding,
//! retry policy.

use std::time::Duration;

use reqwest::Response;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::de::DeserializeOwned;

use crate::error::{ClientError, Result};

pub const MSGPACK_MIME_TYPE: &str = "application/x-msgpack";
pub const JSON_MIME_TYPE: &str = "application/json";
pub const ARROW_FILE_MIME_TYPE: &str = "application/vnd.apache.arrow.file";
pub const OCTET_STREAM_MIME_TYPE: &str = "application/octet-stream";

pub const USER_AGENT_VALUE: &str = concat!("rust-tiled/", env!("CARGO_PKG_VERSION"));

/// Default retry attempts (matches Python `TILED_RETRY_ATTEMPTS`).
pub const DEFAULT_RETRY_ATTEMPTS: u32 = 10;
/// Default retry timeout in seconds (matches Python `TILED_RETRY_TIMEOUT`).
pub const DEFAULT_RETRY_TIMEOUT_SECS: f64 = 45.0;

/// Build the standard set of request headers used on every call.
///
/// Authorization is set separately by `Context::send_with_auth` so it can
/// orchestrate the OIDC refresh-on-401 dance.
pub fn default_headers(_api_key: Option<&str>) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));
    Ok(headers)
}

/// Add `Accept: application/x-msgpack` to a request headers map.
pub fn accept_msgpack() -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(ACCEPT, HeaderValue::from_static(MSGPACK_MIME_TYPE));
    h
}

/// Decode either MessagePack or JSON body based on the response Content-Type.
///
/// Tiled servers prefer msgpack; clients fall back to JSON if the server
/// did not honor the Accept header.
pub async fn decode_response<T: DeserializeOwned>(resp: Response) -> Result<T> {
    let ctype = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = resp.bytes().await?;
    if ctype.starts_with(MSGPACK_MIME_TYPE) {
        rmp_serde::from_slice(&bytes).map_err(ClientError::from)
    } else {
        serde_json::from_slice(&bytes).map_err(ClientError::from)
    }
}

/// Convert a non-success response into a structured `ClientError`.
///
/// `304 Not Modified` is also returned as `Ok` so the caller can serve the
/// cached body. `5xx` responses are warn-logged before returning the error.
///
/// Mirrors `tiled.client.utils.handle_error` — reads `detail` from JSON if
/// available, captures the `x-tiled-request-id` correlation header, and maps
/// 410 GONE to `KeyNotFound` like the Python client does.
pub async fn handle_error(resp: Response) -> Result<Response> {
    let status_code = resp.status();
    if status_code.is_success() || status_code.as_u16() == 304 {
        return Ok(resp);
    }
    let status = status_code.as_u16();
    let correlation_id = resp
        .headers()
        .get("x-tiled-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let ctype = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = resp.text().await.unwrap_or_default();

    if status == 410 {
        return Err(ClientError::KeyNotFound(format!("broken link: {body}")));
    }
    if status == 401 {
        return Err(ClientError::AuthRequired(format!("{status}: {body}")));
    }
    if status == 403 {
        return Err(ClientError::PermissionDenied(format!("{status}: {body}")));
    }

    let detail = if ctype.starts_with(JSON_MIME_TYPE) {
        serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("detail").and_then(|d| d.as_str()).map(String::from))
            .unwrap_or(body)
    } else {
        body
    };

    if status >= 500 {
        tracing::warn!(
            target: "tiled.client",
            status,
            ?correlation_id,
            detail = %detail,
            "server error"
        );
    }
    Err(ClientError::Server {
        status,
        detail,
        correlation_id,
    })
}

/// Retry on transient errors (connection refused, timeout, 5xx).
///
/// Mirrors `stamina.retry_context` from the Python client. Uses linear
/// exponential backoff (250ms, 500ms, 1s, 2s, ...) capped to the configured
/// timeout.
pub async fn retry<F, Fut, T>(mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay_ms: u64 = 250;
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs_f64(DEFAULT_RETRY_TIMEOUT_SECS);
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if !is_transient(&e) => return Err(e),
            Err(e) => {
                attempt += 1;
                if attempt >= DEFAULT_RETRY_ATTEMPTS || tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tracing::warn!(target: "tiled.client", attempt, %e, "retrying");
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms.saturating_mul(2)).min(8_000);
            }
        }
    }
}

fn is_transient(err: &ClientError) -> bool {
    match err {
        ClientError::Http(e) => e.is_timeout() || e.is_connect() || e.is_request(),
        ClientError::Server { status, .. } => *status >= 500,
        _ => false,
    }
}
