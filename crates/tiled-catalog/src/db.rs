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
    /// Directories under which the server may *create* managed storage for
    /// internally-managed data sources (the `writable_storage` of Python
    /// tiled). Empty (the default) means the server creates no managed
    /// storage — `POST /metadata` with a managed data source is refused. The
    /// CLI wires this from `--writable-storage`, and these dirs are also
    /// folded into the read allow-list so a freshly-created file is readable.
    writable_storage: Vec<std::path::PathBuf>,
}

impl Catalog {
    /// Open from a `DbPool` already prepared by the caller (tests).
    pub fn from_pool(pool: DbPool) -> Self {
        Self {
            pool,
            delete_scope: DeleteScope::Unrestricted,
            writable_storage: Vec::new(),
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

    /// Configure the directories under which the server creates managed
    /// storage (the `writable_storage` of Python tiled). The server wires
    /// this from `--writable-storage`. Consuming builder so it composes with
    /// [`Catalog::connect`]/[`Catalog::from_pool`] alongside
    /// [`Catalog::with_managed_delete_dirs`].
    pub fn with_writable_storage(mut self, dirs: Vec<std::path::PathBuf>) -> Self {
        self.writable_storage = dirs;
        self
    }

    /// Directories the server may create managed storage under. Empty means
    /// managed-storage creation is disabled. Read by the create-node handler
    /// (to generate `data_uri`s + skeletons) and the file leaf resolver (to
    /// decide which resolved assets are writable). `pub` so the server crate's
    /// resolver can consult it.
    pub fn writable_storage(&self) -> &[std::path::PathBuf] {
        &self.writable_storage
    }

    /// Connect to the catalog DB referenced by `uri`.
    ///
    /// `uri` schemes: `sqlite://...`, `sqlite:` (in-memory),
    /// `postgres://...`, `postgresql://...`.
    ///
    /// Uses built-in pool defaults (8 connections for SQLite, 16 for Postgres).
    /// Call [`Catalog::connect_with_pool_size`] to override.
    pub async fn connect(uri: &str) -> Result<Self> {
        Self::connect_inner(uri, None).await
    }

    /// Like [`Catalog::connect`] but sets `PoolOptions::max_connections` to
    /// `max_connections`. Mirrors `CatalogConfig.catalog_pool_size` from the
    /// Python tiled config (`config.py`, default 5).
    pub async fn connect_with_pool_size(uri: &str, max_connections: u32) -> Result<Self> {
        Self::connect_inner(uri, Some(max_connections)).await
    }

    async fn connect_inner(uri: &str, max_connections: Option<u32>) -> Result<Self> {
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
                .max_connections(max_connections.unwrap_or(8))
                .connect_with(opts)
                .await?;
            DbPool::Sqlite(pool)
        } else if uri.starts_with("postgres://") || uri.starts_with("postgresql://") {
            let opts = PgConnectOptions::from_str(uri)
                .map_err(CatalogError::from)?
                .log_statements(tracing::log::LevelFilter::Debug);
            let pool = PgPoolOptions::new()
                .max_connections(max_connections.unwrap_or(16))
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
            writable_storage: Vec::new(),
        })
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}
