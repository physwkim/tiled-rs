//! Blosc2 content-encoding middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's `CompressionMiddleware` / `CompressionRegistry`
//! (`tiled/server/compression.py`, `tiled/media_type_registration.py`):
//! when the client advertises `blosc2` in `Accept-Encoding` and the response
//! body is eligible (>= 500 bytes, media type in the blosc2-eligible set),
//! the body is compressed and `Content-Encoding: blosc2` is set.
//!
//! This layer must sit **inside** (closer to the handler than) tower-http's
//! `CompressionLayer`.  When blosc2 compression is applied the `CompressionLayer`
//! sees `Content-Encoding: blosc2` already present and skips its gzip/zstd
//! pass, preventing double-encoding.

use axum::body::Body;
use axum::extract::Request;
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use blosc2_pure_rs::{BLOSC_NOSHUFFLE, BLOSC2_MAX_OVERHEAD, blosc1_compress, blosc1_decompress};

/// Minimum response body size (bytes) to trigger blosc2 compression.
/// Mirrors Python's `CompressionMiddleware(minimum_size=500)`.
pub const MINIMUM_SIZE: usize = 500;

/// Media types for which blosc2 is offered.  Python's `media_type_registration.py`
/// registers blosc2 only for `application/octet-stream` and the Arrow file
/// type — limit to the same set so negotiation matches Python exactly.
const BLOSC2_ELIGIBLE: &[&str] = &[
    "application/octet-stream",
    "application/vnd.apache.arrow.file",
];

/// Compress `src` into a blosc2 chunk, returning the compressed bytes.
///
/// Uses clevel=5, no shuffle, typesize=1 — appropriate for generic binary
/// data without a known numeric typesize.  The chunk header encodes these
/// parameters so the decompressor always uses the right settings.
pub(crate) fn compress(src: &[u8]) -> Option<Vec<u8>> {
    let dest_cap = src.len() + BLOSC2_MAX_OVERHEAD;
    let mut dest = vec![0u8; dest_cap];
    match blosc1_compress(5, BLOSC_NOSHUFFLE, 1, src, &mut dest) {
        Ok(n) if n > 0 => {
            dest.truncate(n);
            Some(dest)
        }
        _ => None,
    }
}

/// Decompress a blosc2 chunk produced by `compress` (or Python's
/// `blosc2.compress()`).  Returns an error string on failure.
pub fn decompress(src: &[u8]) -> Result<Vec<u8>, String> {
    let (nbytes, _, _) =
        blosc2_pure_rs::blosc1_cbuffer_sizes(src).map_err(|e| format!("blosc2 header: {e}"))?;
    let mut dest = vec![0u8; nbytes];
    blosc1_decompress(src, &mut dest).map_err(|e| format!("blosc2 decompress: {e}"))?;
    Ok(dest)
}

/// Axum middleware that applies blosc2 content-encoding to eligible responses.
pub async fn blosc2_compress_middleware(request: Request, next: Next) -> Response {
    let wants_blosc2 = request
        .headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .any(|enc| enc.trim().eq_ignore_ascii_case("blosc2"))
        })
        .unwrap_or(false);

    let response = next.run(request).await;

    if !wants_blosc2 {
        return response;
    }

    // Skip if the response is already content-encoded (e.g. the handler
    // set its own encoding, or a nested middleware already ran).
    if response.headers().contains_key(header::CONTENT_ENCODING) {
        return response;
    }

    let eligible = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            let media_type = v.split(';').next().unwrap_or("").trim();
            BLOSC2_ELIGIBLE.contains(&media_type)
        })
        .unwrap_or(false);

    if !eligible {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return Response::from_parts(parts, Body::empty()),
    };

    if body_bytes.len() < MINIMUM_SIZE {
        return Response::from_parts(parts, Body::from(body_bytes));
    }

    match compress(&body_bytes) {
        Some(compressed) => {
            let n = compressed.len();
            parts
                .headers
                .insert(header::CONTENT_ENCODING, "blosc2".parse().unwrap());
            parts
                .headers
                .insert(header::CONTENT_LENGTH, n.to_string().parse().unwrap());
            if let Ok(v) = "Accept-Encoding".parse() {
                parts.headers.append(header::VARY, v);
            }
            Response::from_parts(parts, Body::from(compressed))
        }
        None => Response::from_parts(parts, Body::from(body_bytes)),
    }
}
