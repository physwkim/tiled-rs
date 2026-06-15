//! Server error types and Axum error response conversion.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use tiled_core::schemas;

/// Server error type.
#[derive(Debug)]
pub enum ServerError {
    NotFound(String),
    Validation(String),
    /// The request conflicts with the current state of the resource. Maps to
    /// HTTP 409 to match Python tiled, which raises `WouldDeleteData` and
    /// answers `HTTP_409_CONFLICT` (app.py:367-374) when a delete would orphan
    /// internally-managed data sources.
    Conflict(String),
    Internal(String),
    UnsupportedMediaType(String),
    /// The client requested (via `?format=` or `Accept`) a media type this
    /// structure family cannot serialize. Maps to HTTP 406 to match Python
    /// tiled, which raises `UnsupportedMediaTypes` and answers
    /// `HTTP_406_NOT_ACCEPTABLE` (router.py:642-643, core.py:374-419).
    NotAcceptable(String),
    Unauthorized(String),
    Forbidden(String),
    /// Decoded response payload exceeds the configured
    /// `response_bytesize_limit`. Maps to 400 to match Python tiled
    /// (router.py raises HTTP_400_BAD_REQUEST before serialization).
    ResponseTooLarge(String),
    /// A search query used a variant the target node cannot evaluate. Maps to
    /// 400 to match Python tiled, which raises `UnsupportedQueryType` and
    /// answers HTTP 400 (app.py:355-365). The message is the full Python
    /// detail string (`The query type {name!r} is not supported on this
    /// node.`).
    UnsupportedQuery(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Not found: {msg}"),
            Self::Validation(msg) => write!(f, "Validation error: {msg}"),
            Self::Conflict(msg) => write!(f, "Conflict: {msg}"),
            Self::Internal(msg) => write!(f, "Internal error: {msg}"),
            Self::UnsupportedMediaType(msg) => write!(f, "Unsupported media type: {msg}"),
            Self::NotAcceptable(msg) => write!(f, "Not acceptable: {msg}"),
            Self::Unauthorized(msg) => write!(f, "Unauthorized: {msg}"),
            Self::Forbidden(msg) => write!(f, "Forbidden: {msg}"),
            Self::ResponseTooLarge(msg) => write!(f, "Response too large: {msg}"),
            Self::UnsupportedQuery(msg) => write!(f, "Unsupported query: {msg}"),
        }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        // Consume self — no clone needed.
        let (status, code, message) = match self {
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, 404, msg),
            Self::Validation(msg) => (StatusCode::UNPROCESSABLE_ENTITY, 422, msg),
            Self::Conflict(msg) => (StatusCode::CONFLICT, 409, msg),
            // Internal errors carry MongoDB driver text, filesystem paths,
            // and similar details that should not reach unauthenticated
            // clients. Log the full message server-side and return a
            // generic body so operators still see the root cause in logs.
            Self::Internal(msg) => {
                tracing::error!(target: "tiled.server", "internal error: {msg}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    500,
                    "Internal server error".to_string(),
                )
            }
            Self::UnsupportedMediaType(msg) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, 415, msg),
            Self::NotAcceptable(msg) => (StatusCode::NOT_ACCEPTABLE, 406, msg),
            Self::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, 401, msg),
            Self::Forbidden(msg) => (StatusCode::FORBIDDEN, 403, msg),
            Self::ResponseTooLarge(msg) => (StatusCode::BAD_REQUEST, 400, msg),
            Self::UnsupportedQuery(msg) => (StatusCode::BAD_REQUEST, 400, msg),
        };

        let body = schemas::Response::<()> {
            data: None,
            error: Some(schemas::Error { code, message }),
            links: None,
            meta: None,
        };

        (status, axum::Json(body)).into_response()
    }
}

impl From<tiled_core::queries::UnsupportedQuery> for ServerError {
    fn from(err: tiled_core::queries::UnsupportedQuery) -> Self {
        // `Display` renders the full Python detail string, which becomes the
        // 400 body message.
        Self::UnsupportedQuery(err.to_string())
    }
}

impl From<tiled_core::TiledError> for ServerError {
    fn from(err: tiled_core::TiledError) -> Self {
        match err {
            tiled_core::TiledError::NotFound(msg) => Self::NotFound(msg),
            tiled_core::TiledError::Validation(msg) => Self::Validation(msg),
            tiled_core::TiledError::UnsupportedMediaType(msg) => Self::UnsupportedMediaType(msg),
            tiled_core::TiledError::Internal(msg) => Self::Internal(msg),
            other => Self::Internal(other.to_string()),
        }
    }
}
