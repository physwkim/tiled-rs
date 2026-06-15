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

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CatalogError>;
