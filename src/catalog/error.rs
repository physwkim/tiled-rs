//! Catalog error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    /// Deleting this subtree would orphan internally-managed storage. Raised
    /// by `delete_node` when `external_only` is set and any descendant data
    /// source is not `external`. Mirrors Python `WouldDeleteData`.
    #[error("would delete data: {0}")]
    WouldDeleteData(String),

    #[error("validation: {0}")]
    Validation(String),

    /// A query variant the SQL search path cannot evaluate (e.g. `Regex` on
    /// SQLite, which has no native regex operator). Mirrors Python tiled's
    /// `UnsupportedQueryType` → HTTP 400.
    #[error("unsupported query: {0}")]
    UnsupportedQuery(String),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CatalogError>;

/// Bridge catalog errors into the core error type so the async
/// [`ContainerAdapter`](crate::core::adapters::ContainerAdapter) methods (which
/// return [`crate::core::error::Result`]) can `?` on SQL calls. The server then
/// maps the resulting `TiledError` to an HTTP status via its own
/// `From<TiledError> for ServerError`.
impl From<CatalogError> for crate::core::TiledError {
    fn from(err: CatalogError) -> Self {
        use crate::core::TiledError as TE;
        match err {
            CatalogError::Database(e) => TE::Database(e.to_string()),
            CatalogError::Migration(m) => TE::Internal(m),
            CatalogError::NotFound(m) => TE::NotFound(m),
            CatalogError::Conflict(m) => TE::Validation(m),
            CatalogError::WouldDeleteData(m) => TE::Validation(m),
            CatalogError::Validation(m) => TE::Validation(m),
            CatalogError::UnsupportedQuery(m) => TE::UnsupportedQuery(m),
            CatalogError::Json(e) => TE::Json(e),
            CatalogError::Io(e) => TE::Io(e),
        }
    }
}
