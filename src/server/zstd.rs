//! zstd content-encoding middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's zstd arm (`tiled/media_type_registration.py:236-287`,
//! applied through the shared `CompressionResponder` in
//! `tiled/server/compression.py`): when the client advertises `zstd` in
//! `Accept-Encoding` and the response body is eligible (>= `MINIMUM_SIZE` bytes,
//! media type in the zstd-eligible set), the body is compressed and
//! `Content-Encoding: zstd` is set.
//!
//! Upstream compresses at level 3 (cribbed from the dask config) into a single
//! zstd frame; `zstd::bulk::compress(src, 3)` produces the same standard frame,
//! which python-zstandard and httpx's zstd decoder both decode.
//!
//! ## Negotiation priority
//!
//! zstd is registered after gzip but before lz4 and blosc2 upstream, so the
//! priority is `blosc2 > lz4 > zstd > gzip` (`CompressionRegistry.encodings`
//! reverses registration order). That ordering is enforced by the single
//! [`compress_middleware`](crate::server::compression::compress_middleware)
//! owner, which reaches zstd only after blosc2 and lz4 have been declined for
//! the media type; it preempts gzip. This module supplies zstd's eligibility set
//! and (de)compression.

use crate::core::media_type::mime;

/// zstd compression level. Upstream uses 3 (`media_type_registration.py:244`).
const ZSTD_LEVEL: i32 = 3;

/// Media types for which zstd is offered. Matches the set registered for zstd in
/// `media_type_registration.py:277-286` exactly.
const ZSTD_ELIGIBLE: &[&str] = &[
    mime::JSON,
    mime::MSGPACK,
    mime::OCTET_STREAM,
    mime::ARROW_FILE,
    mime::EXCEL_XLSX,
    mime::CSV,
    mime::HTML,
    mime::PLAIN,
];

/// Whether zstd is offered for `media_type`. Consulted by the single
/// negotiation owner (`compression::negotiate`).
pub(crate) fn eligible(media_type: &str) -> bool {
    ZSTD_ELIGIBLE.contains(&media_type)
}

/// Compress `src` into a single standard zstd frame at [`ZSTD_LEVEL`], matching
/// Python's `zstandard.ZstdCompressor(level=3).stream_writer(...)` output.
pub fn compress(src: &[u8]) -> Vec<u8> {
    // In-memory compression at a valid level performs no I/O and cannot fail.
    zstd::bulk::compress(src, ZSTD_LEVEL).expect("zstd compress to Vec is infallible")
}

/// Decompress a zstd frame produced by [`compress`] (or Python's zstd arm).
/// Returns an error string on malformed input.
pub fn decompress(src: &[u8]) -> Result<Vec<u8>, String> {
    zstd::decode_all(src).map_err(|e| format!("zstd decompress: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_decompress_round_trip() {
        let original: Vec<u8> = (0..2000u32).flat_map(|i| i.to_le_bytes()).collect();
        let compressed = compress(&original);
        let restored = decompress(&compressed).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn compress_emits_a_zstd_frame() {
        // zstd frames start with the 4-byte magic number 0xFD2FB528 (LE).
        let compressed = compress(&vec![7u8; 1234]);
        assert_eq!(&compressed[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
    }

    #[test]
    fn decompress_rejects_garbage() {
        // No zstd magic → error, not panic.
        assert!(decompress(&[0x00, 0x01, 0x02, 0x03]).is_err());
    }
}
