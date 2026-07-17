//! Shared content-encoding negotiation helpers, mirroring Python tiled's
//! `tiled/server/compression.py`.
//!
//! The four content-encoding middlewares (blosc2, lz4, zstd, gzip) all route
//! their candidate body through [`apply_encoding`], which owns the minimum-size
//! floor, the ratio gate, the `compress` Server-Timing phase, and the
//! `Content-Encoding`/`Content-Length`/`Vary` header emission. Upstream keeps
//! this logic in one `CompressionResponder`
//! (`tiled/server/compression.py:70-107`); centralizing it here makes the rules
//! uniform by construction, so no encoder can drift apart — the reason the
//! per-encoder `minimum_size` had already diverged (500 vs the app's 1000)
//! before this single owner existed.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header;
use axum::response::Response;

use crate::server::server_timing::ServerTiming;

/// Minimum response body size (bytes) below which no encoder compresses. The
/// running app overrides `CompressionMiddleware`'s 500-byte class default
/// (`compression.py:11`) with `minimum_size=1000` in `app.add_middleware(...)`
/// (`tiled/server/app.py:760-764`), so the effective floor is 1000. Single
/// owner for every content-encoding middleware.
pub const MINIMUM_SIZE: usize = 1000;

/// Buffer `response`'s body and apply `encoding` to it if it clears the shared
/// [`MINIMUM_SIZE`] floor and the [`worth_compressing`] ratio gate.
///
/// This is the single owner of the floor, the ratio gate, the `compress`
/// Server-Timing phase, and the `Content-Encoding`/`Content-Length`/`Vary`
/// header emission — so blosc2, lz4, zstd, and gzip apply them identically by
/// construction (upstream's shared `CompressionResponder`,
/// `tiled/server/compression.py:70-107`).
///
/// The caller has already established that the client wants `encoding`, no
/// `Content-Encoding` is set yet, and the media type is eligible. `compress`
/// returns the compressed bytes, or `None` if this encoder cannot compress the
/// body (blosc2's C path can fail) — in which case the original is sent
/// identity-encoded.
pub(crate) async fn apply_encoding(
    response: Response,
    timing: Option<Arc<ServerTiming>>,
    encoding: &'static str,
    compress: impl FnOnce(&[u8]) -> Option<Vec<u8>>,
) -> Response {
    let (mut parts, body) = response.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        // The body stream errored mid-flight and the original bytes are gone;
        // send an empty body rather than a partial one. Drop the handler's
        // Content-Length first — keeping it would advertise a length we no
        // longer deliver, framing the empty body as truncated.
        Err(_) => {
            parts.headers.remove(header::CONTENT_LENGTH);
            return Response::from_parts(parts, Body::empty());
        }
    };

    if body_bytes.len() < MINIMUM_SIZE {
        return Response::from_parts(parts, Body::from(body_bytes));
    }

    let t0 = std::time::Instant::now();
    let compressed = compress(&body_bytes);
    let dur = t0.elapsed().as_secs_f64();

    let Some(compressed) = compressed else {
        return Response::from_parts(parts, Body::from(body_bytes));
    };
    let n = compressed.len();

    // Keep the compression only if it saves enough to be worth the client's
    // decompression cost. Otherwise send the original body identity-encoded,
    // with no Content-Encoding and no compress Server-Timing phase recorded.
    if !worth_compressing(body_bytes.len(), n) {
        return Response::from_parts(parts, Body::from(body_bytes));
    }

    if let Some(timing) = &timing {
        let ratio = body_bytes.len() as f64 / n as f64;
        timing.record("compress", &[("dur", dur), ("ratio", ratio)]);
    }
    parts
        .headers
        .insert(header::CONTENT_ENCODING, encoding.parse().unwrap());
    parts
        .headers
        .insert(header::CONTENT_LENGTH, n.to_string().parse().unwrap());
    if let Ok(v) = "Accept-Encoding".parse() {
        parts.headers.append(header::VARY, v);
    }
    Response::from_parts(parts, Body::from(compressed))
}

/// Decide whether a just-computed compression is worth keeping.
///
/// Upstream (`tiled/server/compression.py:87-93`) always compresses first, then
/// keeps the result only if `compression_ratio = original / compressed`
/// exceeds `THRESHOLD = 1 / 0.9` — i.e. the compressed body must be smaller
/// than 90% of the original, otherwise "the savings isn't worth the
/// decompression time" and the original is sent identity-encoded (with no
/// `Content-Encoding` and no `compress` Server-Timing phase recorded). This
/// gate lives in the shared responder upstream, so it applies identically to
/// every encoder; this helper is its single Rust owner.
pub fn worth_compressing(original_len: usize, compressed_len: usize) -> bool {
    // THRESHOLD = 1 / 0.9. `compressed_len` is never 0 in practice (every
    // encoder emits at least a header/size prefix), so the ratio is finite.
    original_len as f64 / compressed_len as f64 > 1.0 / 0.9
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_boundary() {
        // Exactly 90%: ratio = 1000/900 = 1.111... which is NOT strictly
        // greater than 1/0.9 = 1.111..., so it is NOT worth keeping.
        assert!(!worth_compressing(1000, 900));
        // Better than 90%: clearly worth keeping.
        assert!(worth_compressing(1000, 500));
        // Worse than 90% (barely compressed): not worth keeping.
        assert!(!worth_compressing(1000, 950));
        // Expanded (incompressible + overhead): not worth keeping.
        assert!(!worth_compressing(1000, 1010));
    }

    /// When the response body stream errors mid-flight, `apply_encoding` sends
    /// an empty body and must drop the handler's now-false Content-Length so the
    /// empty body is not framed as a truncated 1600-byte one.
    #[tokio::test]
    async fn body_stream_error_drops_stale_content_length() {
        // A body whose single frame is an error, so `to_bytes` fails.
        let err_body = Body::from_stream(futures::stream::once(async {
            Err::<bytes::Bytes, std::io::Error>(std::io::Error::other("boom"))
        }));
        let mut response = Response::new(err_body);
        response
            .headers_mut()
            .insert(header::CONTENT_LENGTH, "1600".parse().unwrap());

        // compress must never be called (we bail before it); assert if it is.
        let out = apply_encoding(response, None, "gzip", |_| {
            panic!("compress must not run on a body-stream error")
        })
        .await;

        assert!(
            out.headers().get(header::CONTENT_LENGTH).is_none(),
            "stale Content-Length must be dropped on body error"
        );
        assert!(out.headers().get(header::CONTENT_ENCODING).is_none());
        let bytes = axum::body::to_bytes(out.into_body(), usize::MAX)
            .await
            .expect("empty body reads cleanly");
        assert!(
            bytes.is_empty(),
            "body must be empty after the stream error"
        );
    }
}
