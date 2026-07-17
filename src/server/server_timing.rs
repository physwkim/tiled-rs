//! `Server-Timing` response-header middleware for the tiled HTTP server.
//!
//! Mirrors Python tiled's always-on `capture_metrics` middleware
//! (`tiled/server/app.py:855-888`) and the `record_timing` helper
//! (`tiled/server/utils.py:21-28`): a per-request metrics accumulator is
//! stashed in request state, phases record their durations into it, and the
//! outermost middleware serializes the accumulator into the standard
//! `Server-Timing` header on the way out.
//!
//! Format parity with upstream: each phase is emitted as
//! `key;metric=value[;metric=value...]`, phases joined by `, `. Durations are
//! stored in seconds (as `record_timing` does) and rendered in milliseconds
//! (`dur = value * 1000`, one decimal) because the Server-Timing spec uses
//! milliseconds; every other metric (e.g. compression `ratio`) is rendered
//! as-is with one decimal. See
//! <https://w3c.github.io/server-timing/#the-server-timing-header-field>.
//!
//! Phases the Rust request path can measure honestly today: `app` (total
//! application time, measured here) and `compress` (measured in the blosc2 and
//! lz4 content-encoding middlewares, which record `dur` and `ratio`). Phases
//! that upstream also emits but that have no clean Rust middleware seam yet
//! (`acl`, `read`, `pack`, `tok`) are deliberately NOT faked — they live
//! inside handlers/serialization with no request-scoped seam here.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;

/// Canonical `Server-Timing` header name.
static SERVER_TIMING: HeaderName = HeaderName::from_static("server-timing");

/// One recorded phase: a key plus its ordered `(metric, value)` pairs.
struct Phase {
    key: String,
    /// `(metric_name, value)`. `dur` values are in seconds (rendered as ms);
    /// all other metrics are rendered verbatim.
    metrics: Vec<(String, f64)>,
}

/// Per-request metrics accumulator, shared via request extensions so inner
/// middlewares (compression) can record into the same instance the outermost
/// middleware serializes.
#[derive(Default)]
pub struct ServerTiming {
    phases: Mutex<Vec<Phase>>,
}

impl ServerTiming {
    /// Record one phase with its metrics. Insertion order is preserved, so the
    /// header reflects the order phases completed (mirrors upstream, where the
    /// `compress` key lands during the response and `app` is finalized last).
    pub fn record(&self, key: &str, metrics: &[(&str, f64)]) {
        let phase = Phase {
            key: key.to_string(),
            metrics: metrics
                .iter()
                .map(|(m, v)| ((*m).to_string(), *v))
                .collect(),
        };
        // A poisoned lock only means another request-scoped writer panicked
        // mid-record; recover the guard rather than propagate the panic into
        // an unrelated request's response path.
        let mut phases = self.phases.lock().unwrap_or_else(|e| e.into_inner());
        phases.push(phase);
    }

    /// Serialize to the `Server-Timing` header value, matching upstream's
    /// `capture_metrics` formatting exactly.
    fn to_header(&self) -> String {
        let phases = self.phases.lock().unwrap_or_else(|e| e.into_inner());
        phases
            .iter()
            .map(|phase| {
                let subs = phase
                    .metrics
                    .iter()
                    .map(|(metric, value)| {
                        if metric == "dur" {
                            // Stored in seconds, emitted in milliseconds.
                            format!("{metric}={:.1}", value * 1000.0)
                        } else {
                            format!("{metric}={value:.1}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(";");
                format!("{};{subs}", phase.key)
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Retrieve the request-scoped [`ServerTiming`] accumulator, if the middleware
/// stack installed one. Compression middlewares call this before consuming the
/// request so they can record the `compress` phase on the way back out.
pub fn timing_from_request(request: &Request) -> Option<Arc<ServerTiming>> {
    request.extensions().get::<Arc<ServerTiming>>().cloned()
}

/// Outermost middleware: installs the per-request accumulator, measures total
/// application time (`app`), and stamps the `Server-Timing` header on every
/// response. Mirrors `capture_metrics` in `tiled/server/app.py`.
pub async fn server_timing_middleware(mut request: Request, next: Next) -> Response {
    let timing = Arc::new(ServerTiming::default());
    request.extensions_mut().insert(timing.clone());

    let t0 = Instant::now();
    let mut response = next.run(request).await;
    // Units: seconds, matching record_timing; rendered as ms in to_header.
    timing.record("app", &[("dur", t0.elapsed().as_secs_f64())]);

    if let Ok(value) = HeaderValue::from_str(&timing.to_header()) {
        response.headers_mut().insert(SERVER_TIMING.clone(), value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_only_header_matches_upstream_format() {
        let timing = ServerTiming::default();
        timing.record("app", &[("dur", 0.0012)]);
        // 0.0012 s * 1000 = 1.2 ms.
        assert_eq!(timing.to_header(), "app;dur=1.2");
    }

    #[test]
    fn compress_phase_renders_dur_in_ms_and_ratio_verbatim() {
        let timing = ServerTiming::default();
        timing.record("compress", &[("dur", 0.0005), ("ratio", 3.44)]);
        timing.record("app", &[("dur", 0.010)]);
        // compress recorded first, app last (insertion order); dur→ms, ratio verbatim.
        assert_eq!(
            timing.to_header(),
            "compress;dur=0.5;ratio=3.4, app;dur=10.0"
        );
    }
}
