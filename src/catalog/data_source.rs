//! `data_sources` + `assets` CRUD.

use std::collections::HashMap;

use serde_json::Value;
use sqlx::Row;

use crate::catalog::db::{Catalog, DbPool};
use crate::catalog::error::Result;
use crate::catalog::orm::{Asset, DataSource};

pub type DataSourceRecord = DataSource;
pub type AssetRecord = Asset;

/// Fields the caller supplies to `create_data_source`. Asset list is
/// inserted in the same transaction so FK validity holds atomically.
#[derive(Debug, Clone)]
pub struct DataSourceSpec {
    pub structure_family: String,
    pub structure: Value,
    pub mimetype: String,
    pub parameters: Value,
    pub management: String,
    pub assets: Vec<AssetSpec>,
}

#[derive(Debug, Clone)]
pub struct AssetSpec {
    pub data_uri: String,
    pub is_directory: bool,
    pub parameter: String,
    pub num: Option<i32>,
}

impl Catalog {
    pub async fn list_data_sources(&self, node_id: i64) -> Result<Vec<DataSource>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT id, node_id, structure_family, structure, mimetype,
                            parameters, management
                       FROM data_sources WHERE node_id = ?
                       ORDER BY id",
                )
                .bind(node_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(ds_from_sqlite_row).collect()
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, node_id, structure_family, structure, mimetype,
                            parameters, management
                       FROM data_sources WHERE node_id = $1
                       ORDER BY id",
                )
                .bind(node_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(ds_from_postgres_row).collect()
            }
        }
    }

    pub async fn list_assets(&self, data_source_id: i64) -> Result<Vec<Asset>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT id, data_source_id, data_uri, is_directory, parameter, num
                       FROM assets WHERE data_source_id = ?
                       ORDER BY COALESCE(num, 0), id",
                )
                .bind(data_source_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(asset_from_sqlite_row).collect()
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, data_source_id, data_uri, is_directory, parameter, num
                       FROM assets WHERE data_source_id = $1
                       ORDER BY COALESCE(num, 0), id",
                )
                .bind(data_source_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(asset_from_postgres_row).collect()
            }
        }
    }

    /// Batch-load the data sources (each with its assets) for a page of nodes
    /// in **two** queries total — one `data_sources WHERE node_id IN (...)`, one
    /// `assets WHERE data_source_id IN (...)` — rather than a per-row query per
    /// node. Returns a map `node_id -> [(data_source, its assets)]`, each list
    /// ordered by `data_sources.id` and each asset list by `(num, id)`, matching
    /// [`list_data_sources`](Self::list_data_sources) / [`list_assets`](Self::list_assets).
    ///
    /// A node with no data sources is simply absent from the map (the caller
    /// defaults it to an empty list). Feeds the `?include_data_sources=true`
    /// search path (`CatalogAdapter::search_page`); the single-node metadata
    /// route stays on the per-node `list_data_sources`/`list_assets` calls.
    pub async fn list_data_sources_for_nodes(
        &self,
        node_ids: &[i64],
    ) -> Result<HashMap<i64, Vec<(DataSource, Vec<Asset>)>>> {
        let mut out: HashMap<i64, Vec<(DataSource, Vec<Asset>)>> = HashMap::new();
        if node_ids.is_empty() {
            return Ok(out);
        }
        // (1) All data sources for these nodes, ordered so each node's list is
        // in id order (parity with the per-node `list_data_sources`).
        let data_sources: Vec<DataSource> = match self.pool() {
            DbPool::Sqlite(pool) => {
                let sql = format!(
                    "SELECT id, node_id, structure_family, structure, mimetype,
                            parameters, management
                       FROM data_sources WHERE node_id IN ({})
                       ORDER BY node_id, id",
                    sqlite_placeholders(node_ids.len())
                );
                let mut q = sqlx::query(&sql);
                for id in node_ids {
                    q = q.bind(id);
                }
                q.fetch_all(pool)
                    .await?
                    .iter()
                    .map(ds_from_sqlite_row)
                    .collect::<Result<_>>()?
            }
            DbPool::Postgres(pool) => {
                let sql = format!(
                    "SELECT id, node_id, structure_family, structure, mimetype,
                            parameters, management
                       FROM data_sources WHERE node_id IN ({})
                       ORDER BY node_id, id",
                    postgres_placeholders(node_ids.len(), 1)
                );
                let mut q = sqlx::query(&sql);
                for id in node_ids {
                    q = q.bind(id);
                }
                q.fetch_all(pool)
                    .await?
                    .iter()
                    .map(ds_from_postgres_row)
                    .collect::<Result<_>>()?
            }
        };
        if data_sources.is_empty() {
            return Ok(out);
        }
        // (2) All assets for those data sources, ordered by (num, id) so each
        // data source's asset list matches the per-source `list_assets`.
        let ds_ids: Vec<i64> = data_sources.iter().map(|ds| ds.id).collect();
        let assets: Vec<Asset> = match self.pool() {
            DbPool::Sqlite(pool) => {
                let sql = format!(
                    "SELECT id, data_source_id, data_uri, is_directory, parameter, num
                       FROM assets WHERE data_source_id IN ({})
                       ORDER BY data_source_id, COALESCE(num, 0), id",
                    sqlite_placeholders(ds_ids.len())
                );
                let mut q = sqlx::query(&sql);
                for id in &ds_ids {
                    q = q.bind(id);
                }
                q.fetch_all(pool)
                    .await?
                    .iter()
                    .map(asset_from_sqlite_row)
                    .collect::<Result<_>>()?
            }
            DbPool::Postgres(pool) => {
                let sql = format!(
                    "SELECT id, data_source_id, data_uri, is_directory, parameter, num
                       FROM assets WHERE data_source_id IN ({})
                       ORDER BY data_source_id, COALESCE(num, 0), id",
                    postgres_placeholders(ds_ids.len(), 1)
                );
                let mut q = sqlx::query(&sql);
                for id in &ds_ids {
                    q = q.bind(id);
                }
                q.fetch_all(pool)
                    .await?
                    .iter()
                    .map(asset_from_postgres_row)
                    .collect::<Result<_>>()?
            }
        };
        // Group assets under their data source, then data sources under their
        // node. The SQL ORDER BY already fixed the per-list order, so pushing in
        // scan order preserves it.
        let mut assets_by_ds: HashMap<i64, Vec<Asset>> = HashMap::new();
        for a in assets {
            assets_by_ds.entry(a.data_source_id).or_default().push(a);
        }
        for ds in data_sources {
            let node_id = ds.node_id;
            let ds_assets = assets_by_ds.remove(&ds.id).unwrap_or_default();
            out.entry(node_id).or_default().push((ds, ds_assets));
        }
        Ok(out)
    }

    /// Look up a single asset by id, scoped to `node_id`: the asset must belong
    /// to one of the node's data sources. Mirrors Python
    /// `CatalogNodeAdapter.asset_by_id` (catalog/adapter.py:412-430), which joins
    /// asset → data_source → node and filters on `node.id == self.node.id`.
    /// Returns `None` when no asset with that id belongs to the node — so a
    /// client cannot download another node's files by passing a foreign asset
    /// id to this node's path. (The Rust schema attaches each asset to one
    /// data_source via a direct FK rather than Python's association table, so the
    /// join is one hop shorter but node-scoping is identical.)
    pub async fn asset_by_id(&self, node_id: i64, asset_id: i64) -> Result<Option<Asset>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT a.id, a.data_source_id, a.data_uri, a.is_directory,
                            a.parameter, a.num
                       FROM assets a
                       JOIN data_sources ds ON a.data_source_id = ds.id
                      WHERE ds.node_id = ? AND a.id = ?",
                )
                .bind(node_id)
                .bind(asset_id)
                .fetch_optional(pool)
                .await?;
                row.as_ref().map(asset_from_sqlite_row).transpose()
            }
            DbPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT a.id, a.data_source_id, a.data_uri, a.is_directory,
                            a.parameter, a.num
                       FROM assets a
                       JOIN data_sources ds ON a.data_source_id = ds.id
                      WHERE ds.node_id = $1 AND a.id = $2",
                )
                .bind(node_id)
                .bind(asset_id)
                .fetch_optional(pool)
                .await?;
                row.as_ref().map(asset_from_postgres_row).transpose()
            }
        }
    }

    pub async fn create_data_source(
        &self,
        node_id: i64,
        spec: DataSourceSpec,
    ) -> Result<DataSource> {
        crate::catalog::node::validate_structure(&spec.structure)?;
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let mut tx = pool.begin().await?;
                let row = sqlx::query(
                    "INSERT INTO data_sources (node_id, structure_family, structure,
                                                mimetype, parameters, management)
                     VALUES (?, ?, ?, ?, ?, ?)
                     RETURNING id, node_id, structure_family, structure, mimetype,
                               parameters, management",
                )
                .bind(node_id)
                .bind(&spec.structure_family)
                .bind(serde_json::to_string(&spec.structure)?)
                .bind(&spec.mimetype)
                .bind(serde_json::to_string(&spec.parameters)?)
                .bind(&spec.management)
                .fetch_one(&mut *tx)
                .await?;
                let ds = ds_from_sqlite_row(&row)?;
                for a in &spec.assets {
                    sqlx::query(
                        "INSERT INTO assets (data_source_id, data_uri, is_directory,
                                              parameter, num)
                         VALUES (?, ?, ?, ?, ?)",
                    )
                    .bind(ds.id)
                    .bind(&a.data_uri)
                    .bind(a.is_directory as i64)
                    .bind(&a.parameter)
                    .bind(a.num)
                    .execute(&mut *tx)
                    .await?;
                }
                tx.commit().await?;
                Ok(ds)
            }
            DbPool::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                let row = sqlx::query(
                    "INSERT INTO data_sources (node_id, structure_family, structure,
                                                mimetype, parameters, management)
                     VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6)
                     RETURNING id, node_id, structure_family, structure, mimetype,
                               parameters, management",
                )
                .bind(node_id)
                .bind(&spec.structure_family)
                .bind(serde_json::to_string(&spec.structure)?)
                .bind(&spec.mimetype)
                .bind(serde_json::to_string(&spec.parameters)?)
                .bind(&spec.management)
                .fetch_one(&mut *tx)
                .await?;
                let ds = ds_from_postgres_row(&row)?;
                for a in &spec.assets {
                    sqlx::query(
                        "INSERT INTO assets (data_source_id, data_uri, is_directory,
                                              parameter, num)
                         VALUES ($1, $2, $3, $4, $5)",
                    )
                    .bind(ds.id)
                    .bind(&a.data_uri)
                    .bind(a.is_directory)
                    .bind(&a.parameter)
                    .bind(a.num)
                    .execute(&mut *tx)
                    .await?;
                }
                tx.commit().await?;
                Ok(ds)
            }
        }
    }

    /// Replace the structure / parameters on an existing data source. Used
    /// by the PUT `/data_source/...` endpoint when adapter inspection
    /// produces a refined structure.
    pub async fn update_data_source(
        &self,
        data_source_id: i64,
        structure: Value,
        parameters: Value,
    ) -> Result<DataSource> {
        crate::catalog::node::validate_structure(&structure)?;
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "UPDATE data_sources SET structure = ?, parameters = ?
                       WHERE id = ?
                     RETURNING id, node_id, structure_family, structure, mimetype,
                               parameters, management",
                )
                .bind(serde_json::to_string(&structure)?)
                .bind(serde_json::to_string(&parameters)?)
                .bind(data_source_id)
                .fetch_one(pool)
                .await?;
                ds_from_sqlite_row(&row)
            }
            DbPool::Postgres(pool) => {
                let row = sqlx::query(
                    "UPDATE data_sources SET structure = $1::jsonb, parameters = $2::jsonb
                       WHERE id = $3
                     RETURNING id, node_id, structure_family, structure, mimetype,
                               parameters, management",
                )
                .bind(serde_json::to_string(&structure)?)
                .bind(serde_json::to_string(&parameters)?)
                .bind(data_source_id)
                .fetch_one(pool)
                .await?;
                ds_from_postgres_row(&row)
            }
        }
    }
}

fn ds_from_sqlite_row(row: &sqlx::sqlite::SqliteRow) -> Result<DataSource> {
    Ok(DataSource {
        id: row.get("id"),
        node_id: row.get("node_id"),
        structure_family: row.get("structure_family"),
        structure: serde_json::from_str(&row.get::<String, _>("structure"))?,
        mimetype: row.get("mimetype"),
        parameters: serde_json::from_str(&row.get::<String, _>("parameters"))?,
        management: row.get("management"),
    })
}

fn ds_from_postgres_row(row: &sqlx::postgres::PgRow) -> Result<DataSource> {
    Ok(DataSource {
        id: row.get("id"),
        node_id: row.get("node_id"),
        structure_family: row.get("structure_family"),
        structure: row.get("structure"),
        mimetype: row.get("mimetype"),
        parameters: row.get("parameters"),
        management: row.get("management"),
    })
}

fn asset_from_sqlite_row(row: &sqlx::sqlite::SqliteRow) -> Result<Asset> {
    Ok(Asset {
        id: row.get("id"),
        data_source_id: row.get("data_source_id"),
        data_uri: row.get("data_uri"),
        is_directory: row.get::<i64, _>("is_directory") != 0,
        parameter: row.get("parameter"),
        num: row.try_get("num").ok(),
    })
}

fn asset_from_postgres_row(row: &sqlx::postgres::PgRow) -> Result<Asset> {
    Ok(Asset {
        id: row.get("id"),
        data_source_id: row.get("data_source_id"),
        data_uri: row.get("data_uri"),
        is_directory: row.get("is_directory"),
        parameter: row.get("parameter"),
        num: row.try_get("num").ok(),
    })
}

/// `?, ?, …` — `n` SQLite positional placeholders for an `IN (...)` list.
fn sqlite_placeholders(n: usize) -> String {
    vec!["?"; n].join(", ")
}

/// `$start, $start+1, …` — `n` Postgres numbered placeholders starting at
/// `$start`, for an `IN (...)` list.
fn postgres_placeholders(n: usize, start: usize) -> String {
    (start..start + n)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Convert a catalog ORM [`DataSource`] row (plus its asset rows) into the
/// `crate::core::data_source::DataSource` wire type used in API responses.
///
/// Single source of truth for this mapping — both the single-node metadata
/// route and the `?include_data_sources=true` search path go through it, so a
/// data source is shaped identically whichever endpoint returns it.
pub(crate) fn to_core_data_source(
    ds: DataSource,
    assets: Vec<Asset>,
) -> crate::core::data_source::DataSource {
    use crate::core::data_source as core_ds;
    let management = serde_json::from_value(Value::String(ds.management.clone()))
        .unwrap_or(core_ds::Management::Writable);
    let structure_family = ds
        .structure_family
        .parse::<crate::core::structures::StructureFamily>()
        .unwrap_or(crate::core::structures::StructureFamily::Container);
    let core_assets = assets
        .into_iter()
        .map(|a| core_ds::Asset {
            data_uri: a.data_uri,
            is_directory: a.is_directory,
            parameter: if a.parameter.is_empty() {
                None
            } else {
                Some(a.parameter)
            },
            num: a.num.map(|n| n as usize),
            id: Some(a.id),
        })
        .collect();
    core_ds::DataSource {
        structure_family,
        structure: serde_json::from_value::<Option<crate::core::structures::AnyStructure>>(
            ds.structure,
        )
        .ok()
        .flatten(),
        id: Some(ds.id),
        mimetype: if ds.mimetype.is_empty() {
            None
        } else {
            Some(ds.mimetype)
        },
        parameters: ds.parameters,
        properties: Value::Null,
        assets: core_assets,
        management,
    }
}
