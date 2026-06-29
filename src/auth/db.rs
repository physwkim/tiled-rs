//! SQLite/Postgres pool wrapper for the auth DB.

use std::str::FromStr;
use std::time::Duration;

use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Postgres, Sqlite};

use crate::auth::error::{AuthError, Result};

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
        // Gate SQL statement logging behind an explicit env-var opt-in so
        // session UUIDs and principal rows don't appear in production debug
        // logs by default.
        let sql_log = if std::env::var("TILED_AUTH_LOG_SQL").is_ok() {
            tracing::log::LevelFilter::Debug
        } else {
            tracing::log::LevelFilter::Off
        };
        let pool = if uri.starts_with("sqlite:") {
            let opts = SqliteConnectOptions::from_str(uri)
                .map_err(AuthError::from)?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .busy_timeout(Duration::from_secs(5))
                .log_statements(sql_log);
            let pool = SqlitePoolOptions::new()
                .max_connections(8)
                .connect_with(opts)
                .await?;
            AuthPool::Sqlite(pool)
        } else if uri.starts_with("postgres://") || uri.starts_with("postgresql://") {
            let opts = PgConnectOptions::from_str(uri)
                .map_err(AuthError::from)?
                .log_statements(sql_log);
            let pool = PgPoolOptions::new()
                .max_connections(16)
                .connect_with(opts)
                .await?;
            AuthPool::Postgres(pool)
        } else {
            return Err(AuthError::Validation(format!(
                "auth DB uri must start with sqlite:, postgres://, or postgresql://; got {}",
                redact_uri(uri)
            )));
        };
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &AuthPool {
        &self.pool
    }
}

/// Redact credentials from a DB URI for safe use in error messages and logs.
///
/// `postgres://user:pass@host:5432/db` → `postgres://host:5432`
/// `sqlite:./auth.db`                  → `sqlite:`
fn redact_uri(uri: &str) -> String {
    if let Some((scheme, rest)) = uri.split_once("://") {
        // Strip userinfo (user:pass@); keep only host[:port].
        let authority = rest.split('@').next_back().unwrap_or(rest);
        let host = authority.split('/').next().unwrap_or(authority);
        format!("{scheme}://{host}")
    } else {
        // Opaque URI (e.g. sqlite:./path) — show only the scheme prefix.
        let scheme = uri.split(':').next().unwrap_or("<unknown>");
        format!("{scheme}:")
    }
}
