//! Blosc2 content-encoding middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's `CompressionMiddleware` / `CompressionRegistry`
//! (`tiled/server/compression.py`, `tiled/media_type_registration.py`):
//! when the client advertises `blosc2` in `Accept-Encoding` and the response
//! body is eligible (>= 1000 bytes, media type in the blosc2-eligible set),
//! the body is compressed and `Content-Encoding: blosc2` is set.
//!
//! This layer is the innermost (closest to the handler) of the four
//! content-encoding middlewares, so it gets first crack at the response — blosc2
//! is the highest-priority encoding (`blosc2 > lz4 > zstd > gzip`). The lz4,
//! zstd, and gzip middlewares each yield to a `Content-Encoding` blosc2 already
//! set, preventing double-encoding.

use axum::extract::Request;
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use blosc2_pure_rs::{BLOSC_NOSHUFFLE, BLOSC2_MAX_OVERHEAD, blosc1_compress, blosc1_decompress};

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

    // Grab the request-scoped Server-Timing accumulator before the request is
    // consumed, so the compress phase can be recorded on the way back out.
    let timing = crate::server::server_timing::timing_from_request(&request);

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

    // Floor, ratio gate, compress timing, and header emission all live in the
    // shared owner. `compress` returns None if blosc2's C path fails, in which
    // case the body is sent identity-encoded.
    crate::server::compression::apply_encoding(response, timing, "blosc2", compress).await
}
