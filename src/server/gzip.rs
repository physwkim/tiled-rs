//! gzip content-encoding middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's gzip arm (`tiled/media_type_registration.py:210-234`,
//! applied through the shared `CompressionResponder` in
//! `tiled/server/compression.py`): when the client advertises `gzip` in
//! `Accept-Encoding` and the response body is eligible (>= `MINIMUM_SIZE` bytes,
//! media type in the gzip-eligible set), the body is compressed and
//! `Content-Encoding: gzip` is set.
//!
//! ## Compression level
//!
//! Upstream registers gzip at level 9 for `application/json` and
//! `application/x-msgpack`, and level 1 for the six bulk types (octet-stream,
//! arrow, xlsx, csv, plain, html) because "High compression is extremely slow
//! (~60 seconds) on large array data". This module preserves that
//! per-media-type split.
//!
//! ## Negotiation priority
//!
//! gzip is the *first-registered* (thus least preferred) encoding upstream —
//! `CompressionRegistry.encodings` reverses registration order — so the priority
//! is `blosc2 > lz4 > zstd > gzip`. This middleware is the outermost of the four
//! content-encoding layers, so it runs *last* on the response: it yields to any
//! `Content-Encoding` that blosc2, lz4, or zstd already set and compresses only
//! when none of them did.

use std::io::Write;

use axum::extract::Request;
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use flate2::Compression;
use flate2::write::GzEncoder;

use crate::core::media_type::mime;
use crate::server::server_timing::timing_from_request;

/// gzip level for `application/json` / `application/x-msgpack`
/// (`media_type_registration.py:214-218`).
const GZIP_LEVEL_JSON: u32 = 9;
/// gzip level for the bulk media types (octet-stream, arrow, xlsx, csv, plain,
/// html) — low, because high gzip is very slow on large arrays
/// (`media_type_registration.py:228-234`).
const GZIP_LEVEL_BULK: u32 = 1;

/// The gzip compression level for `media_type`, or `None` if gzip is not offered
/// for it. Doubles as the eligibility check: upstream registers gzip for exactly
/// these eight media types.
fn gzip_level(media_type: &str) -> Option<u32> {
    match media_type {
        mime::JSON | mime::MSGPACK => Some(GZIP_LEVEL_JSON),
        mime::OCTET_STREAM
        | mime::ARROW_FILE
        | mime::EXCEL_XLSX
        | mime::CSV
        | mime::HTML
        | mime::PLAIN => Some(GZIP_LEVEL_BULK),
        _ => None,
    }
}

/// Compress `src` into a gzip stream (RFC 1952) at `level`, matching Python's
/// `gzip.GzipFile(mode="wb", compresslevel=level)`.
pub fn compress(src: &[u8], level: u32) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
    // Writing to an in-memory Vec performs no I/O, so neither call can fail.
    encoder
        .write_all(src)
        .expect("gzip write to Vec is infallible");
    encoder.finish().expect("gzip finish to Vec is infallible")
}

/// Decompress a gzip stream produced by [`compress`] (or Python's gzip arm).
/// Returns an error string on malformed input.
pub fn decompress(src: &[u8]) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(src);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| format!("gzip decompress: {e}"))?;
    Ok(out)
}

/// Axum middleware that applies gzip content-encoding to eligible responses.
pub async fn gzip_compress_middleware(request: Request, next: Next) -> Response {
    let wants_gzip = request
        .headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .any(|enc| enc.trim().eq_ignore_ascii_case("gzip"))
        })
        .unwrap_or(false);

    // Grab the request-scoped Server-Timing accumulator before the request is
    // consumed, so the compress phase can be recorded on the way back out.
    let timing = timing_from_request(&request);

    let response = next.run(request).await;

    if !wants_gzip {
        return response;
    }

    // Skip if a higher-priority encoding (blosc2/lz4/zstd) already set one.
    if response.headers().contains_key(header::CONTENT_ENCODING) {
        return response;
    }

    // The level lookup is also the eligibility check: `None` means gzip is not
    // offered for this media type.
    let level = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| gzip_level(v.split(';').next().unwrap_or("").trim()));

    let Some(level) = level else {
        return response;
    };

    crate::server::compression::apply_encoding(response, timing, "gzip", |b| {
        Some(compress(b, level))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_decompress_round_trip() {
        let original: Vec<u8> = (0..2000u32).flat_map(|i| i.to_le_bytes()).collect();
        let compressed = compress(&original, GZIP_LEVEL_BULK);
        let restored = decompress(&compressed).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn gzip_level_matches_upstream_registration() {
        // Level 9 for json/msgpack, level 1 for the six bulk types, None for
        // anything unregistered.
        assert_eq!(gzip_level(mime::JSON), Some(9));
        assert_eq!(gzip_level(mime::MSGPACK), Some(9));
        assert_eq!(gzip_level(mime::OCTET_STREAM), Some(1));
        assert_eq!(gzip_level(mime::ARROW_FILE), Some(1));
        assert_eq!(gzip_level(mime::EXCEL_XLSX), Some(1));
        assert_eq!(gzip_level(mime::CSV), Some(1));
        assert_eq!(gzip_level(mime::HTML), Some(1));
        assert_eq!(gzip_level(mime::PLAIN), Some(1));
        assert_eq!(gzip_level(mime::PARQUET), None);
        assert_eq!(gzip_level("image/png"), None);
    }

    #[test]
    fn decompress_rejects_garbage() {
        // No gzip magic bytes → error, not panic.
        assert!(decompress(&[0x00, 0x01, 0x02, 0x03]).is_err());
    }
}
