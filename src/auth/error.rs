use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration: {0}")]
    Migration(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("validation: {0}")]
    Validation(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("expired")]
    Expired,

    #[error("revoked")]
    Revoked,

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("hash: {0}")]
    Hash(String),

    #[error("jwt: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, AuthError>;
