//! lz4 content-encoding middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's `CompressionMiddleware` / `CompressionRegistry` lz4
//! arm (`tiled/server/compression.py`, `tiled/media_type_registration.py:289-343`):
//! when the client advertises `lz4` in `Accept-Encoding` and the response body
//! is eligible (>= 500 bytes, media type in the lz4-eligible set), the body is
//! compressed and `Content-Encoding: lz4` is set.
//!
//! ## Wire format
//!
//! Upstream compresses with `lz4.block.compress(data)` (python-lz4's *block*
//! API, not the frame API), which with its default `store_size=True` prepends
//! the uncompressed length as a 4-byte little-endian `u32` ahead of the raw
//! LZ4 block. `lz4_flex::block::compress_prepend_size` produces the exact same
//! layout (4-byte LE u32 size + raw block), so a python-lz4 `lz4.block.decompress`
//! decodes our output and vice versa. (This is the block-with-size-prefix
//! convention shared by python-lz4 and lz4_flex; it is NOT the LZ4 frame format.)
//!
//! ## Negotiation priority
//!
//! Upstream's registry prefers the *last-registered* encoding for a media type
//! (`CompressionRegistry.encodings` returns the registrations reversed), and
//! lz4 is registered after gzip and zstd but before blosc2. So the priority is
//! `blosc2 > lz4 > zstd > gzip`. This middleware is layered so it runs on the
//! response **after** the blosc2 middleware (which sets `Content-Encoding` first
//! for the octet-stream/arrow types it handles) and **before** tower-http's
//! `CompressionLayer` (gzip/zstd) — reproducing that ordering: lz4 yields to an
//! already-set blosc2 encoding and preempts gzip/zstd.

use axum::body::Body;
use axum::extract::Request;
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;

use crate::core::media_type::mime;
use crate::server::server_timing::timing_from_request;

/// Minimum response body size (bytes) to trigger lz4 compression.
/// Mirrors Python's `CompressionMiddleware(minimum_size=500)`.
pub const MINIMUM_SIZE: usize = 500;

/// Media types for which lz4 is offered. Matches the set registered for lz4 in
/// Python's `media_type_registration.py:333-342` exactly.
const LZ4_ELIGIBLE: &[&str] = &[
    mime::JSON,
    mime::MSGPACK,
    mime::OCTET_STREAM,
    mime::ARROW_FILE,
    mime::EXCEL_XLSX,
    mime::CSV,
    mime::HTML,
    mime::PLAIN,
];

/// Compress `src` into an lz4 block with a 4-byte little-endian uncompressed
/// size prefix, matching python-lz4's `lz4.block.compress(data)` default.
pub fn compress(src: &[u8]) -> Vec<u8> {
    lz4_flex::block::compress_prepend_size(src)
}

/// Decompress an lz4 block produced by [`compress`] (or python-lz4's
/// `lz4.block.compress`). Returns an error string on malformed input.
pub fn decompress(src: &[u8]) -> Result<Vec<u8>, String> {
    lz4_flex::block::decompress_size_prepended(src).map_err(|e| format!("lz4 decompress: {e}"))
}

/// Axum middleware that applies lz4 content-encoding to eligible responses.
pub async fn lz4_compress_middleware(request: Request, next: Next) -> Response {
    let wants_lz4 = request
        .headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .any(|enc| enc.trim().eq_ignore_ascii_case("lz4"))
        })
        .unwrap_or(false);

    // Grab the request-scoped Server-Timing accumulator before the request is
    // consumed, so the compress phase can be recorded on the way back out.
    let timing = timing_from_request(&request);

    let response = next.run(request).await;

    if !wants_lz4 {
        return response;
    }

    // Skip if the response is already content-encoded (e.g. the blosc2
    // middleware, which runs first, already set an encoding).
    if response.headers().contains_key(header::CONTENT_ENCODING) {
        return response;
    }

    let eligible = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            let media_type = v.split(';').next().unwrap_or("").trim();
            LZ4_ELIGIBLE.contains(&media_type)
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

    let t0 = std::time::Instant::now();
    let compressed = compress(&body_bytes);
    let dur = t0.elapsed().as_secs_f64();
    let n = compressed.len();

    // Only keep the compression if it saves enough to be worth the client's
    // decompression cost (upstream compression.py:87-93). Otherwise send the
    // original body identity-encoded, with no Content-Encoding and no compress
    // Server-Timing phase recorded.
    if !crate::server::compression::worth_compressing(body_bytes.len(), n) {
        return Response::from_parts(parts, Body::from(body_bytes));
    }

    if let Some(timing) = &timing {
        let ratio = body_bytes.len() as f64 / n as f64;
        timing.record("compress", &[("dur", dur), ("ratio", ratio)]);
    }

    parts
        .headers
        .insert(header::CONTENT_ENCODING, "lz4".parse().unwrap());
    parts
        .headers
        .insert(header::CONTENT_LENGTH, n.to_string().parse().unwrap());
    if let Ok(v) = "Accept-Encoding".parse() {
        parts.headers.append(header::VARY, v);
    }
    Response::from_parts(parts, Body::from(compressed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_decompress_round_trip() {
        let original: Vec<u8> = (0..2000u32).flat_map(|i| i.to_le_bytes()).collect();
        let compressed = compress(&original);
        // The 4-byte size prefix means the frame is never shorter than 4 bytes.
        assert!(compressed.len() >= 4);
        let restored = decompress(&compressed).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn size_prefix_is_little_endian_uncompressed_length() {
        // python-lz4 block format (store_size=True) stores the uncompressed
        // length as a 4-byte little-endian u32 prefix; lz4_flex must match.
        let original = vec![7u8; 1234];
        let compressed = compress(&original);
        let prefix = u32::from_le_bytes(compressed[..4].try_into().unwrap());
        assert_eq!(prefix as usize, original.len());
    }

    #[test]
    fn decompress_rejects_garbage() {
        // Size prefix claims 8 bytes uncompressed, but the block (0xff) is a
        // malformed token — must error, not panic. (A small prefix keeps the
        // decoder's pre-allocation bounded.)
        assert!(decompress(&[0x08, 0x00, 0x00, 0x00, 0xff]).is_err());
        // Truncated: fewer than the 4 size-prefix bytes.
        assert!(decompress(&[0x00, 0x01]).is_err());
    }
}
