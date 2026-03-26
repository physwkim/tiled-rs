//! Server error types and Axum error response conversion.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use tiled_core::schemas;

/// Server error type.
#[derive(Debug)]
pub enum ServerError {
    NotFound(String),
    Validation(String),
    Internal(String),
    UnsupportedMediaType(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Not found: {msg}"),
            Self::Validation(msg) => write!(f, "Validation error: {msg}"),
            Self::Internal(msg) => write!(f, "Internal error: {msg}"),
            Self::UnsupportedMediaType(msg) => write!(f, "Unsupported media type: {msg}"),
        }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        // Consume self — no clone needed.
        let (status, code, message) = match self {
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, 404, msg),
            Self::Validation(msg) => (StatusCode::UNPROCESSABLE_ENTITY, 422, msg),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, 500, msg),
            Self::UnsupportedMediaType(msg) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, 415, msg),
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

impl From<tiled_core::TiledError> for ServerError {
    fn from(err: tiled_core::TiledError) -> Self {
        match err {
            tiled_core::TiledError::NotFound(msg) => Self::NotFound(msg),
            tiled_core::TiledError::Validation(msg) => Self::Validation(msg),
            tiled_core::TiledError::UnsupportedMediaType(msg) => Self::UnsupportedMediaType(msg),
            other => Self::Internal(other.to_string()),
        }
    }
}
