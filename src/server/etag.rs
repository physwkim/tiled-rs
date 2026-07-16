//! Conditional-GET support via `ETag` / `If-None-Match` (HTTP RFC 9110 §8.8.3).
//!
//! The Rust client ships a full revalidation cache (`tiled-client` cache.rs,
//! context.rs:420-483): it stores each response's `ETag` and replays it as
//! `If-None-Match` on the next read, serving its cached body when the server
//! answers `304 Not Modified`. The server emitted no `ETag`, so that path was
//! dead — every navigation re-downloaded the full body.
//!
//! This middleware mirrors upstream `tiled`'s response ETag: metadata / search /
//! distinct / about responses hash via `md5(content)` (`core.py:728-735`), and
//! data-export responses (array/table/sparse/ragged/awkward/container full,
//! block, partition — and raw asset downloads, which Starlette's `FileResponse`
//! ETags from file stat) hash via `tokenize((payload, media_type))`
//! (`core.py:426-429`). Both are "does the representation still match" checks,
//! so this layer applies one uniform rule regardless of content type: for any
//! successful (`200 OK`) GET response, hash the body, attach a strong `ETag`,
//! and return `304` (empty body, ETag retained) when the client's
//! `If-None-Match` matches exactly (upstream compares with `==`).
//!
//! Deviation from upstream: Python's `tokenize()` hashes the pre-serialization
//! payload, so a cache hit on a data route skips the (potentially expensive)
//! serialize step entirely. This layer runs *after* the handler and hashes the
//! already-serialized body, so the encode cost is always paid — only the
//! network transfer is saved on a `304`. A `206 Partial Content` response
//! (`Range` request) is left untouched: `serve_with_range` has already sliced
//! the body by the time this layer sees it, so hashing the slice would produce
//! a validator for the wrong (partial) representation; computing the ETag from
//! the pre-slice full body would need restructuring the data handlers to hash
//! before calling `serve_with_range`, deferred as a follow-up.
//!
//! Hashing adds no asymptotic cost beyond what the handler already paid: the
//! body is already fully materialised in memory (by `Json`, by the
//! serializer, or by `serve_with_range`) before this layer runs, so it only
//! re-reads bytes already held, bounded by the same `response_bytesize_limit`
//! the handlers enforce. The digest algorithm is an implementation detail —
//! the client only round-trips the opaque validator for equality, so `Sha256`
//! (already a `tiled-server` dependency) stands in for upstream's md5.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use sha2::{Digest, Sha256};

/// Attach an `ETag` to successful GET responses and short-circuit matching
/// `If-None-Match` requests to `304 Not Modified`. See the module docs.
pub async fn etag_get_responses(req: Request, next: Next) -> Response {
    let is_get = req.method() == Method::GET;
    // Capture the client's validator before the request is consumed by `next`.
    let if_none_match = req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let resp = next.run(req).await;

    // Only a full (`200 OK`) GET response is conditional-GET cacheable here —
    // a `206 Partial Content` slice is excluded (see module docs).
    if !is_get || resp.status() != StatusCode::OK {
        return resp;
    }

    let (mut parts, body) = resp.into_parts();
    // The body is already fully buffered upstream of this layer (`Json`, a
    // serializer, or `serve_with_range`), so collecting it cannot stream
    // unboundedly; `usize::MAX` is safe here.
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            // A body we cannot read back can't be returned intact, so surface a
            // 500 rather than silently truncate. Unreachable for an
            // already-materialised body, but handled explicitly.
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
