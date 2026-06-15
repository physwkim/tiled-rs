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
    // Capture `Retry-After` before the body read consumes `resp`. Only
    // meaningful for 429 (see below); parsed lazily there.
    let retry_after_header = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
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
    // Honor `Retry-After` only for 429, matching Python `should_retry`, which
    // reads the header for `TOO_MANY_REQUESTS` and ignores it for 5xx.
    let retry_after = if status == 429 {
        parse_retry_after(retry_after_header.as_deref())
    } else {
        None
    };
    Err(ClientError::Server {
        status,
        detail,
        correlation_id,
        retry_after,
    })
}

/// Parse a `Retry-After` header value into a delay.
///
/// Matches Python's `float(retry_after)`: only the delta-seconds form is
/// honored. A non-numeric value (e.g. the HTTP-date form) or a negative /
/// non-finite value yields `None`, so the caller falls back to the default
/// backoff schedule.
fn parse_retry_after(raw: Option<&str>) -> Option<Duration> {
    let secs: f64 = raw?.trim().parse().ok()?;
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs))
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
                let now = tokio::time::Instant::now();
                if attempt >= DEFAULT_RETRY_ATTEMPTS || now >= deadline {
                    return Err(e);
                }
                // A 429 carries a parsed `Retry-After`; honor it as this
                // attempt's wait (Python returns the float to stamina as the
                // backoff), else use the default exponential schedule. Bound a
                // server-controlled `Retry-After` to the remaining retry budget
                // so a large value can't park the client past the timeout.
                let retry_after = match &e {
                    ClientError::Server { retry_after, .. } => *retry_after,
                    _ => None,
                };
                let delay = match retry_after {
                    Some(d) => d.min(deadline.saturating_duration_since(now)),
                    None => Duration::from_millis(delay_ms),
                };
                tracing::warn!(target: "tiled.client", attempt, %e, "retrying");
                tokio::time::sleep(delay).await;
                delay_ms = (delay_ms.saturating_mul(2)).min(8_000);
            }
        }
    }
}

fn is_transient(err: &ClientError) -> bool {
    match err {
        ClientError::Http(e) => e.is_timeout() || e.is_connect() || e.is_request(),
        // 429 is transient too (Python `should_retry`): a rate limiter / load
        // balancer expects the client to back off and retry, honoring
        // `Retry-After`. Without this, the client fails fast where Python
        // recovers.
        ClientError::Server { status, .. } => *status == 429 || *status >= 500,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn server(status: u16, retry_after: Option<Duration>) -> ClientError {
        ClientError::Server {
            status,
            detail: String::new(),
            correlation_id: None,
            retry_after,
        }
    }

    #[test]
    fn transient_classification_includes_429() {
        assert!(is_transient(&server(429, None)));
        assert!(is_transient(&server(500, None)));
        assert!(is_transient(&server(503, None)));
        assert!(!is_transient(&server(400, None)));
        assert!(!is_transient(&server(404, None)));
        assert!(!is_transient(&server(409, None)));
    }

    #[test]
    fn parse_retry_after_numeric_only() {
        assert_eq!(parse_retry_after(Some("2")), Some(Duration::from_secs(2)));
        assert_eq!(
            parse_retry_after(Some(" 2.5 ")),
            Some(Duration::from_secs_f64(2.5))
        );
        assert_eq!(parse_retry_after(Some("0")), Some(Duration::ZERO));
        // HTTP-date form is not honored (matches Python's `float()`), and a
        // negative / garbage value falls back to default backoff.
        assert_eq!(
            parse_retry_after(Some("Wed, 21 Oct 2015 07:28:00 GMT")),
            None
        );
        assert_eq!(parse_retry_after(Some("-1")), None);
        assert_eq!(parse_retry_after(Some("abc")), None);
        assert_eq!(parse_retry_after(None), None);
    }

    #[tokio::test]
    async fn retry_retries_429_then_succeeds() {
        let calls = AtomicU32::new(0);
        let out: i32 = retry(|| async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First attempt is rate-limited with a Retry-After.
                Err(server(429, Some(Duration::from_millis(5))))
            } else {
                Ok(42)
            }
        })
        .await
        .expect("a 429 with Retry-After must be retried, not failed fast");
        assert_eq!(out, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retry_does_not_retry_non_transient() {
        let calls = AtomicU32::new(0);
        let err = retry(|| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<i32, _>(server(400, None))
        })
        .await
        .expect_err("a 400 is not transient");
        assert!(matches!(err, ClientError::Server { status: 400, .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
