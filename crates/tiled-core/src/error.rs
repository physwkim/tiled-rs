use thiserror::Error;

#[derive(Error, Debug)]
pub enum TiledError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Unsupported media type: {0}")]
    UnsupportedMediaType(String),

    #[error("Invalid slice: {0}")]
    InvalidSlice(String),

    #[error("Invalid dtype: {0}")]
    InvalidDType(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, TiledError>;
