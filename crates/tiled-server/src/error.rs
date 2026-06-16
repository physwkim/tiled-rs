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
    /// HTTP 409 to match Python tiled, which raises `Conflicts` /
    /// `WouldDeleteData` and answers `HTTP_409_CONFLICT` (app.py:350-374) — for
    /// deleting a non-empty container, or a subtree holding internally-managed
    /// data sources.
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
    /// A search query filter could not be decoded: a required field was absent
    /// or a present value failed to parse. Maps to HTTP 400 to match Python
    /// tiled, whose `apply_search` catches `QueryValueError` and answers
    /// `HTTP_400_BAD_REQUEST` (tiled/server/core.py:180-184). Distinct from
    /// [`Self::UnsupportedQuery`] (query *type* not evaluable on this node);
    /// this is a malformed *value* for a recognised query type.
    InvalidQuery(String),
    /// The path resolved to a real node, but its structure family does not
    /// match the one this route requires (e.g. `GET /array/full/<a-table>`).
    /// Maps to HTTP 404 to match Python tiled, which resolves the node with a
    /// `structure_families` filter and answers `HTTP_404_NOT_FOUND` on a
    /// mismatch (tiled/server/dependencies.py:138-149), and likewise maps
    /// `WrongTypeForRoute` → 404 (tiled/server/router.py:393-394). Distinct
    /// from [`Self::Validation`] (422): a wrong-type-for-route is "no such
    /// thing at this route", not "your request body/params are invalid".
    WrongType(String),
    /// An index (block index, partition number) was out of the valid range.
    /// Maps to HTTP 400 to match Python tiled, which catches `IndexError` from
    /// `read_block` / `read_partition` and answers `HTTP_400_BAD_REQUEST`
    /// (router.py:609-613, 1176-1179). Distinct from [`Self::Validation`]
    /// (422): the request is structurally valid but names a non-existent slot.
    BadRequest(String),
    /// The target node does not support this operation. Maps to HTTP 405 to
    /// match Python tiled, which raises `HTTP_405_METHOD_NOT_ALLOWED` when an
    /// entry lacks the requested capability — e.g. `get_distinct`
    /// (router.py:444-447) — as opposed to the route itself not existing.
    MethodNotAllowed(String),
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
            Self::InvalidQuery(msg) => write!(f, "Invalid query: {msg}"),
            Self::WrongType(msg) => write!(f, "Wrong type for route: {msg}"),
            Self::BadRequest(msg) => write!(f, "Bad request: {msg}"),
            Self::MethodNotAllowed(msg) => write!(f, "Method not allowed: {msg}"),
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
            Self::InvalidQuery(msg) => (StatusCode::BAD_REQUEST, 400, msg),
            Self::WrongType(msg) => (StatusCode::NOT_FOUND, 404, msg),
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, 400, msg),
            Self::MethodNotAllowed(msg) => (StatusCode::METHOD_NOT_ALLOWED, 405, msg),
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

impl From<tiled_core::queries::QueryDecodeError> for ServerError {
    fn from(err: tiled_core::queries::QueryDecodeError) -> Self {
        // A malformed query filter → 400, parity with Python `apply_search`
        // catching `QueryValueError` (tiled/server/core.py:180-184).
        Self::InvalidQuery(err.to_string())
    }
}

impl From<tiled_core::TiledError> for ServerError {
    fn from(err: tiled_core::TiledError) -> Self {
        match err {
            tiled_core::TiledError::NotFound(msg) => Self::NotFound(msg),
            tiled_core::TiledError::Validation(msg) => Self::Validation(msg),
            // A query variant the node's search path can't evaluate → 400,
            // parity with Python tiled's UnsupportedQueryType. The async
            // ContainerAdapter::search carries this in its TiledError Result.
            tiled_core::TiledError::UnsupportedQuery(msg) => Self::UnsupportedQuery(msg),
            tiled_core::TiledError::UnsupportedMediaType(msg) => Self::UnsupportedMediaType(msg),
            tiled_core::TiledError::Internal(msg) => Self::Internal(msg),
            other => Self::Internal(other.to_string()),
        }
    }
}
