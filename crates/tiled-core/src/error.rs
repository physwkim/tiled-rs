use thiserror::Error;

#[derive(Error, Debug)]
pub enum TiledError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Validation error: {0}")]
    Validation(String),

    /// A query variant this adapter's search path cannot evaluate. The
    /// server maps this to HTTP 400 (parity with Python tiled's
    /// `UnsupportedQueryType`). Distinct from [`Self::Validation`] so the
    /// async `ContainerAdapter::search` (which also does fallible IO) can
    /// carry "unsupported query" and "IO failed" in the one `Result` type.
    #[error("Unsupported query: {0}")]
    UnsupportedQuery(String),

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

    /// A write that conflicts with existing state — e.g. a duplicate ragged
    /// `chunk_index`. The server maps this to HTTP 409 (parity with Python
    /// tiled's `Conflicts`, which a duplicate-chunk write raises).
    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

impl From<crate::queries::UnsupportedQuery> for TiledError {
    fn from(err: crate::queries::UnsupportedQuery) -> Self {
        // Carry the canonical parity detail string from `UnsupportedQuery`'s
        // `Display` ("The query type '{name}' is not supported on this
        // node."), not the bare type name in `err.0`. This is the
        // client-facing 400 detail and must match the server-side
        // `From<UnsupportedQuery> for ServerError` path, which also uses
        // `to_string()`.
        TiledError::UnsupportedQuery(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TiledError>;
