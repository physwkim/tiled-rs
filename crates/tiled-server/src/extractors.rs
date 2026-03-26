//! Custom Axum extractors to reduce handler boilerplate.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

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
