//! `TILED_EXPLAIN_SQL` debug aid — env-gated SQL plan emission.
//!
//! Port of upstream `tiled/catalog/explain.py`. When the environment variable
//! `TILED_EXPLAIN_SQL` is set to a non-zero integer, each catalog search query
//! is run through the backend's plan explainer first (`EXPLAIN QUERY PLAN` on
//! SQLite, `EXPLAIN` on PostgreSQL) and the resulting plan is emitted via
//! tracing before the real query runs. Disabled by default.
//!
//! Upstream wraps `AsyncSession.execute` so every statement is explained; here
//! the gate is read once at [`Catalog`](crate::catalog::Catalog) construction
//! and stored as a plain bool field, so the disabled hot path is a single
//! branch with no per-query environment read or allocation. The plan-emitting
//! machinery lives in [`crate::catalog::search`], where the SQL and its bound
//! parameters are assembled.

/// Interpret the raw `TILED_EXPLAIN_SQL` value.
///
/// Mirrors upstream `bool(int(os.getenv("TILED_EXPLAIN_SQL", "0") or "0"))`:
/// any non-zero integer enables the aid; unset, empty, `"0"`, or a
/// non-integer value leaves it disabled (upstream would raise on a
/// non-integer — we treat that leniently as "off" so a typo never crashes
/// catalog construction).
pub(crate) fn parse_flag(raw: Option<&str>) -> bool {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<i64>().ok())
        .map(|n| n != 0)
        .unwrap_or(false)
}

/// Read the `TILED_EXPLAIN_SQL` gate from the environment. Evaluated once per
/// catalog construction (never on the query hot path), so it need not be
/// cached.
pub(crate) fn env_flag() -> bool {
    parse_flag(std::env::var("TILED_EXPLAIN_SQL").ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::parse_flag;

    // One case per boundary of the upstream `bool(int(... or "0"))` rule.
    #[test]
    fn unset_is_disabled() {
        assert!(!parse_flag(None));
    }

    #[test]
    fn empty_and_whitespace_are_disabled() {
        assert!(!parse_flag(Some("")));
        assert!(!parse_flag(Some("   ")));
    }

    #[test]
    fn zero_is_disabled() {
        assert!(!parse_flag(Some("0")));
        assert!(!parse_flag(Some(" 0 ")));
    }

    #[test]
    fn nonzero_integer_is_enabled() {
        assert!(parse_flag(Some("1")));
        assert!(parse_flag(Some("2")));
        assert!(parse_flag(Some("-1")));
    }

    #[test]
    fn non_integer_is_disabled() {
        // Upstream `int("abc")` would raise; we treat it leniently as off.
        assert!(!parse_flag(Some("true")));
        assert!(!parse_flag(Some("yes")));
    }
}
