//! lz4 content-encoding middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's `CompressionMiddleware` / `CompressionRegistry` lz4
//! arm (`tiled/server/compression.py`, `tiled/media_type_registration.py:289-343`):
//! when the client advertises `lz4` in `Accept-Encoding` and the response body
//! is eligible (>= 1000 bytes, media type in the lz4-eligible set), the body is
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
//! `blosc2 > lz4 > zstd > gzip`. That ordering is enforced by the single
//! [`compress_middleware`](crate::server::compression::compress_middleware)
//! owner, which tries blosc2 first, then lz4, then zstd, then gzip. This module
//! supplies lz4's eligibility set and (de)compression.

use crate::core::media_type::mime;

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

/// Whether lz4 is offered for `media_type`. Consulted by the single negotiation
/// owner (`compression::negotiate`).
pub(crate) fn eligible(media_type: &str) -> bool {
    LZ4_ELIGIBLE.contains(&media_type)
}

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
