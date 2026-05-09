//! `data_sources` + `assets` CRUD.

use serde_json::Value;
use sqlx::Row;

use crate::db::{Catalog, DbPool};
use crate::error::Result;
use crate::orm::{Asset, DataSource};

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

    pub async fn create_data_source(
        &self,
        node_id: i64,
        spec: DataSourceSpec,
    ) -> Result<DataSource> {
        crate::node::validate_structure(&spec.structure)?;
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
        crate::node::validate_structure(&structure)?;
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
