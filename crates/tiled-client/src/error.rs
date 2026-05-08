//! Client error types.
//!
//! Mirrors the exception surface from `tiled/client/utils.py` (`ClientError`,
//! `handle_error`) — but uses idiomatic `Result<T, ClientError>` instead of
//! Python's `raise_for_status` pattern.

use thiserror::Error;

/// Errors that can occur when calling the Tiled HTTP API.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),

    #[error("JSON decode error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("MessagePack decode error: {0}")]
    MsgPack(#[from] rmp_serde::decode::Error),

    #[error("Arrow IPC error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("server returned status {status}: {detail}")]
    Server {
        status: u16,
        detail: String,
        correlation_id: Option<String>,
    },

    #[error("authentication required: {0}")]
    AuthRequired(String),

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("structure mismatch: expected {expected}, got {got}")]
    StructureMismatch { expected: String, got: String },

    #[error("missing link '{0}' in server response")]
    MissingLink(String),

    #[error("invalid response: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;
