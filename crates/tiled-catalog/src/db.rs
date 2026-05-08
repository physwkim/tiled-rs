//! Pool wrapper that handles both SQLite and Postgres uniformly.
//!
//! Each public method on [`Catalog`] dispatches on the active variant. The
//! enum stays `Clone` because both inner pools are reference-counted handles.

use std::str::FromStr;
use std::time::Duration;

use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Postgres, Sqlite};

use crate::error::{CatalogError, Result};

/// Active database backend.
#[derive(Clone)]
pub enum DbPool {
    Sqlite(Pool<Sqlite>),
    Postgres(Pool<Postgres>),
}

impl std::fmt::Debug for DbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(_) => f.write_str("DbPool::Sqlite(<pool>)"),
            Self::Postgres(_) => f.write_str("DbPool::Postgres(<pool>)"),
        }
    }
}

impl DbPool {
    pub fn is_sqlite(&self) -> bool {
        matches!(self, Self::Sqlite(_))
    }

    pub fn is_postgres(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }
}

/// Top-level catalog handle.
#[derive(Clone, Debug)]
pub struct Catalog {
    pool: DbPool,
}

impl Catalog {
    /// Open from a `DbPool` already prepared by the caller (tests).
    pub fn from_pool(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Connect to the catalog DB referenced by `uri`.
    ///
    /// `uri` schemes: `sqlite://...`, `sqlite:` (in-memory),
    /// `postgres://...`, `postgresql://...`.
    pub async fn connect(uri: &str) -> Result<Self> {
        let pool = if uri.starts_with("sqlite:") {
            // `create_if_missing(true)` so a fresh `tiled serve catalog
            // --temp <path>` works without a separate `init` step.
            let opts = SqliteConnectOptions::from_str(uri)
                .map_err(CatalogError::from)?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .busy_timeout(Duration::from_secs(5))
                // Lower the chatty per-statement INFO logs sqlx emits.
                .log_statements(tracing::log::LevelFilter::Debug);
            let pool = SqlitePoolOptions::new()
                .max_connections(8)
                .connect_with(opts)
                .await?;
            DbPool::Sqlite(pool)
        } else if uri.starts_with("postgres://") || uri.starts_with("postgresql://") {
            let opts = PgConnectOptions::from_str(uri)
                .map_err(CatalogError::from)?
                .log_statements(tracing::log::LevelFilter::Debug);
            let pool = PgPoolOptions::new()
                .max_connections(16)
                .connect_with(opts)
                .await?;
            DbPool::Postgres(pool)
        } else {
            return Err(CatalogError::Validation(format!(
                "catalog DB uri must start with sqlite:, postgres://, or postgresql://; got {uri}"
            )));
        };

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}
