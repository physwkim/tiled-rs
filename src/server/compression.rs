//! Shared content-encoding negotiation helpers, mirroring Python tiled's
//! `tiled/server/compression.py`.
//!
//! The blosc2 and lz4 content-encoding middlewares both apply the *same* ratio
//! floor to a candidate compressed body. Keeping the rule in one place makes it
//! uniform by construction — a single owner for the "is this compression worth
//! keeping?" decision, so the two encoders can never drift apart. (The
//! minimum-size floor is enforced separately by each middleware before it
//! bothers to compress at all.)

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
}
