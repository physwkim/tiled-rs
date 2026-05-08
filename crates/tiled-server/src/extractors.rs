//! Custom Axum extractors to reduce handler boilerplate.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use percent_encoding::percent_decode_str;

use crate::state::AppState;

/// Extracts the resolved base URL from request headers and AppState.
pub struct BaseUrl(pub String);

impl FromRequestParts<AppState> for BaseUrl {
    type Rejection = std::convert::Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let url = state.resolve_base_url(&parts.headers);
        std::future::ready(Ok(BaseUrl(url)))
    }
}

/// Path segment list extracted from the raw URI.
///
/// Splits the raw (un-decoded) URI path on literal `/` then percent-decodes
/// each segment individually. This preserves keys that contain `%2F`
/// (encoded slash) inside a single segment — `axum::Path<String>` would
/// otherwise decode them up front and silently break the lookup.
///
/// The list does **not** include the leading API prefix; callers point this
/// at a sub-prefix to extract.
pub struct PathSegments(pub Vec<String>);

impl PathSegments {
    /// Build segments from the request's raw path, after stripping the given
    /// prefix (typically `/api/v1/metadata/` or similar).
    pub fn from_raw_path(raw_path: &str, prefix: &str) -> Self {
        let stripped = raw_path.strip_prefix(prefix).unwrap_or(raw_path);
        let segments = stripped
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
            .collect();
        Self(segments)
    }
}
