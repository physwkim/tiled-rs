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

/// Containment policy for the physical removal of internally-managed
/// (`management != external`) `file://` assets during [`Catalog::delete_node`].
///
/// Mirrors the server read-side `FileLeafResolver` scope (tiled-server
/// `file_resolver.rs`): the backing file of a managed asset may be removed only
/// when it resolves — symlinks included — to a location under one of the
/// configured storage directories. An empty [`Restricted`](DeleteScope::Restricted)
/// list permits nothing (deny-by-default); [`Unrestricted`](DeleteScope::Unrestricted)
/// is the explicit, audited opt-out.
///
/// The default for a bare [`Catalog`] (embedded / test use) is `Unrestricted`,
/// preserving direct-library behaviour. The server narrows it via
/// [`Catalog::with_managed_delete_dirs`], wired from the same
/// `--allowed-data-dir` directories the read-side resolver allows, so a client
/// cannot register a managed asset pointing outside storage and then delete an
/// arbitrary file off disk.
#[derive(Clone, Debug)]
pub enum DeleteScope {
    /// Remove any managed asset path with no containment check — the explicit
    /// opt-out (server `--allow-unrestricted-reads`), never a server default.
    Unrestricted,
    /// Remove a managed asset's file only when it lives under one of these
    /// directories. An empty list removes nothing (deny-by-default).
    Restricted(Vec<std::path::PathBuf>),
}

/// Top-level catalog handle.
#[derive(Clone, Debug)]
pub struct Catalog {
    pool: DbPool,
    /// Containment policy for managed-asset physical deletion. See
    /// [`DeleteScope`]. Defaults to `Unrestricted`; the server restricts it.
    delete_scope: DeleteScope,
}

impl Catalog {
    /// Open from a `DbPool` already prepared by the caller (tests).
    pub fn from_pool(pool: DbPool) -> Self {
        Self {
            pool,
            delete_scope: DeleteScope::Unrestricted,
        }
    }

    /// Restrict the physical deletion of internally-managed assets to files
    /// under `dirs` (deny-by-default: an empty list permits no managed-file
    /// removal). The server wires this from `--allowed-data-dir`, the same
    /// directories the read-side resolver allows, so a managed asset registered
    /// with a `data_uri` outside storage cannot be turned into an
    /// arbitrary-file delete. Consuming builder so it composes with
    /// [`Catalog::connect`]/[`Catalog::from_pool`].
    pub fn with_managed_delete_dirs(mut self, dirs: Vec<std::path::PathBuf>) -> Self {
        self.delete_scope = DeleteScope::Restricted(dirs);
        self
    }

    /// The managed-asset deletion containment policy. Read by `delete_node`.
    pub(crate) fn delete_scope(&self) -> &DeleteScope {
        &self.delete_scope
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

        Ok(Self {
            pool,
            delete_scope: DeleteScope::Unrestricted,
        })
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}
