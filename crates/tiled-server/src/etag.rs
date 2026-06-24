//! Conditional-GET support via `ETag` / `If-None-Match` (HTTP RFC 9110 §8.8.3).
//!
//! The Rust client ships a full revalidation cache (`tiled-client` cache.rs,
//! context.rs:420-483): it stores each response's `ETag` and replays it as
//! `If-None-Match` on the next read, serving its cached body when the server
//! answers `304 Not Modified`. The server emitted no `ETag`, so that path was
//! dead — every navigation re-downloaded the full body.
//!
//! This middleware mirrors upstream `tiled`'s metadata-response ETag
//! (`core.py:728-735`, `md5(content)`): for a successful GET whose response is
//! `application/json` (metadata / search / distinct / about — the small,
//! frequently re-fetched navigation payloads), it hashes the response body,
//! attaches a strong `ETag`, and returns `304` (empty body, ETag retained) when
//! the client's `If-None-Match` matches exactly (upstream compares with `==`).
//! Non-JSON data bodies (array / table / container exports) are left untouched —
//! upstream hashes those via a separate payload `tokenize()` path
//! (`core.py:421-429`), deferred here.
//!
//! Hashing adds no asymptotic cost: axum's `Json` extractor has already
//! materialised the whole body in memory before this layer runs, so we only
//! re-read bytes we already hold. The digest algorithm is an implementation
//! detail — the client only round-trips the opaque validator for equality, so
//! `Sha256` (already a `tiled-server` dependency) stands in for upstream's md5.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use sha2::{Digest, Sha256};

/// Attach an `ETag` to successful JSON GET responses and short-circuit matching
/// `If-None-Match` requests to `304 Not Modified`. See the module docs.
pub async fn etag_json_responses(req: Request, next: Next) -> Response {
    let is_get = req.method() == Method::GET;
    // Capture the client's validator before the request is consumed by `next`.
    let if_none_match = req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let resp = next.run(req).await;

    // Only successful GET responses are conditional-GET cacheable, and only
    // JSON bodies take this path (data exports use a different upstream ETag).
    if !is_get || resp.status() != StatusCode::OK {
        return resp;
    }
    let is_json = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|c| c.starts_with("application/json"));
    if !is_json {
        return resp;
    }

    let (mut parts, body) = resp.into_parts();
    // The JSON body is already fully buffered upstream of this layer, so
    // collecting it cannot stream unboundedly; `usize::MAX` is safe here.
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            // A body we cannot read back can't be returned intact, so surface a
            // 500 rather than silently truncate. Unreachable for an
            // already-materialised JSON body, but handled explicitly.
            parts.status = StatusCode::INTERNAL_SERVER_ERROR;
            return Response::from_parts(
                parts,
                Body::from("failed to read response body for ETag"),
            );
        }
    };

    // Strong validator, quoted per RFC 9110 §8.8.3. Hex + quotes is always
    // valid header ASCII, so `from_str` cannot fail.
    let etag = {
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        format!("\"{:x}\"", hasher.finalize())
    };
    if let Ok(value) = HeaderValue::from_str(&etag) {
        parts.headers.insert(header::ETAG, value);
    }

    if if_none_match.as_deref() == Some(etag.as_str()) {
        // A 304 carries the validators but no body (RFC 9110 §15.4.5).
        parts.status = StatusCode::NOT_MODIFIED;
        parts.headers.remove(header::CONTENT_LENGTH);
        return Response::from_parts(parts, Body::empty());
    }

    Response::from_parts(parts, Body::from(bytes))
}
