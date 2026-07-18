//! Blosc2 content-encoding middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's `CompressionMiddleware` / `CompressionRegistry`
//! (`tiled/server/compression.py`, `tiled/media_type_registration.py`):
//! when the client advertises `blosc2` in `Accept-Encoding` and the response
//! body is eligible (>= 1000 bytes, media type in the blosc2-eligible set),
//! the body is compressed and `Content-Encoding: blosc2` is set.
//!
//! blosc2 is the highest-priority encoding (`blosc2 > lz4 > zstd > gzip`); the
//! single [`compress_middleware`](crate::server::compression::compress_middleware)
//! owner negotiates it first for the octet-stream / Arrow types it covers. This
//! module supplies blosc2's eligibility set and (de)compression; the negotiation
//! decision lives in `compression.rs`.

use blosc2_pure_rs::{BLOSC_NOSHUFFLE, BLOSC2_MAX_OVERHEAD, blosc1_compress, blosc1_decompress};

/// Media types for which blosc2 is offered.  Python's `media_type_registration.py`
/// registers blosc2 only for `application/octet-stream` and the Arrow file
/// type — limit to the same set so negotiation matches Python exactly.
const BLOSC2_ELIGIBLE: &[&str] = &[
    "application/octet-stream",
    "application/vnd.apache.arrow.file",
];

/// Whether blosc2 is offered for `media_type`. Consulted by the single
/// negotiation owner (`compression::negotiate`).
pub(crate) fn eligible(media_type: &str) -> bool {
    BLOSC2_ELIGIBLE.contains(&media_type)
}

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
