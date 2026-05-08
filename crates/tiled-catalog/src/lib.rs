//! tiled-rs persistent catalog backend.
//!
//! Mirrors `tiled.catalog` from Python tiled — a SQL-backed tree of nodes
//! with `DataSource`s and `Asset`s, supporting create/read/update/delete and
//! search. Two storage dialects are supported:
//!
//! - **SQLite** (`feature = "sqlite"`, default) — single-file backing store
//!   used by `tiled serve catalog --temp` and small deployments.
//! - **PostgreSQL** (`feature = "postgres"`) — clustered/multi-writer
//!   deployments. Uses `jsonb` columns + GIN indexes so `metadata` queries
//!   stay fast at scale.
//!
//! The public entry point is [`Catalog`], which exposes both raw CRUD methods
//! and an [`tiled_core::adapters::ContainerAdapter`] view via
//! [`CatalogAdapter`].

pub mod adapter;
pub mod data_source;
pub mod db;
pub mod error;
pub mod migrate;
pub mod node;
pub mod orm;
pub mod search;

pub use adapter::CatalogAdapter;
pub use data_source::{AssetRecord, DataSourceRecord};
pub use db::{Catalog, DbPool};
pub use error::{CatalogError, Result};
pub use node::{NodeRecord, RegisterRequest};
