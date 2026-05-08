//! SQLite/Postgres pool wrapper for the auth DB.

use std::str::FromStr;
use std::time::Duration;

use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Postgres, Sqlite};

use crate::error::{AuthError, Result};

#[derive(Clone)]
pub enum AuthPool {
    Sqlite(Pool<Sqlite>),
    Postgres(Pool<Postgres>),
}

impl std::fmt::Debug for AuthPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(_) => f.write_str("AuthPool::Sqlite(<pool>)"),
            Self::Postgres(_) => f.write_str("AuthPool::Postgres(<pool>)"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AuthDb {
    pool: AuthPool,
}

impl AuthDb {
    pub fn from_pool(pool: AuthPool) -> Self {
        Self { pool }
    }

    pub async fn connect(uri: &str) -> Result<Self> {
        let pool = if uri.starts_with("sqlite:") {
            let opts = SqliteConnectOptions::from_str(uri)
                .map_err(AuthError::from)?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .busy_timeout(Duration::from_secs(5))
                .log_statements(tracing::log::LevelFilter::Debug);
            let pool = SqlitePoolOptions::new()
                .max_connections(8)
                .connect_with(opts)
                .await?;
            AuthPool::Sqlite(pool)
        } else if uri.starts_with("postgres://") || uri.starts_with("postgresql://") {
            let opts = PgConnectOptions::from_str(uri)
                .map_err(AuthError::from)?
                .log_statements(tracing::log::LevelFilter::Debug);
            let pool = PgPoolOptions::new()
                .max_connections(16)
                .connect_with(opts)
                .await?;
            AuthPool::Postgres(pool)
        } else {
            return Err(AuthError::Validation(format!(
                "auth DB uri must start with sqlite:, postgres://, or postgresql://; got {uri}"
            )));
        };
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &AuthPool {
        &self.pool
    }
}
