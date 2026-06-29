//! Blosc2 decompression helper for the tiled HTTP client.
//!
//! Mirrors Python's `Blosc2Decoder` in `tiled/client/decoders.py`:
//! accumulates the full response body then calls `blosc2.decompress(data)`.
//! reqwest does not auto-decode `Content-Encoding: blosc2`, so the client
//! must detect the header and call this explicitly.

use crate::client::error::{ClientError, Result};

/// Decompress a blosc2 chunk (produced by the server's blosc2 middleware or
/// by Python's `blosc2.compress()`).
///
/// Reads the original uncompressed size from the chunk header, allocates the
/// output buffer, and decompresses in one pass — matching Python's
/// `Blosc2Decoder.flush()` which calls `blosc2.decompress(data)`.
pub fn decompress(data: &[u8]) -> Result<bytes::Bytes> {
    let (nbytes, _, _) = blosc2_pure_rs::blosc1_cbuffer_sizes(data)
        .map_err(|e| ClientError::Invalid(format!("blosc2 header: {e}")))?;
    let mut dest = vec![0u8; nbytes];
    blosc2_pure_rs::blosc1_decompress(data, &mut dest)
        .map_err(|e| ClientError::Invalid(format!("blosc2 decompress: {e}")))?;
    Ok(bytes::Bytes::from(dest))
}

/// `Accept-Encoding` value advertising blosc2 to the server.
///
/// Sent on data-fetch requests (array blocks, Arrow IPC tables) so the server
/// can pick blosc2 over gzip/zstd when the response media type is eligible.
pub const ACCEPT_ENCODING_BLOSC2: &str = "blosc2";
