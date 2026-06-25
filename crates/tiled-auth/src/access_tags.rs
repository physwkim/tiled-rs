//! Per-principal and per-API-key access-tag CRUD.
//!
//! `access_tags` is a JSON array of tag strings stored on `principals` and
//! `api_keys`. For principals it represents the full set of tags a user may
//! access. For API keys it is an optional narrowing subset (authn_access_tags).
//!
//! Mirrors Python `TagBasedAccessPolicy` / `get_tags_from_scope` semantics:
//! every tag in `principal.access_tags` confers the policy's `default_scopes`
//! to that principal (access_policies.py:391-398).

use crate::db::{AuthDb, AuthPool};
use crate::error::Result;

impl AuthDb {
    /// Return the access tags granted to a principal (looked up by UUID).
    /// Returns an empty vec when no principal has that UUID or when no tags
    /// have been granted.
    pub async fn get_principal_tags(&self, principal_uuid: &str) -> Result<Vec<String>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row: Option<String> =
                    sqlx::query_scalar("SELECT access_tags FROM principals WHERE uuid = ?")
                        .bind(principal_uuid)
                        .fetch_optional(pool)
                        .await?;
                Ok(row.map(|s| parse_tags_json(&s)).unwrap_or_default())
            }
            AuthPool::Postgres(pool) => {
                let row: Option<serde_json::Value> =
                    sqlx::query_scalar("SELECT access_tags FROM principals WHERE uuid = $1")
                        .bind(principal_uuid)
                        .fetch_optional(pool)
                        .await?;
                Ok(row
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default())
            }
        }
    }

    /// Replace the access_tags on a principal row (looked up by integer id).
    pub async fn set_principal_tags(&self, principal_id: i64, tags: &[String]) -> Result<()> {
        let tags_json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".to_string());
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("UPDATE principals SET access_tags = ? WHERE id = ?")
                    .bind(&tags_json)
                    .bind(principal_id)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE principals SET access_tags = $1::jsonb WHERE id = $2")
                    .bind(&tags_json)
                    .bind(principal_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Return the access tags stored on an API key (looked up by row id).
    /// An empty vec means "no restriction" — the key inherits the principal's
    /// full tag set. A non-empty vec is a narrowing subset (authn_access_tags).
    pub async fn get_api_key_tags(&self, api_key_id: i64) -> Result<Vec<String>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row: Option<String> =
                    sqlx::query_scalar("SELECT access_tags FROM api_keys WHERE id = ?")
                        .bind(api_key_id)
                        .fetch_optional(pool)
                        .await?;
                Ok(row.map(|s| parse_tags_json(&s)).unwrap_or_default())
            }
            AuthPool::Postgres(pool) => {
                let row: Option<serde_json::Value> =
                    sqlx::query_scalar("SELECT access_tags FROM api_keys WHERE id = $1")
                        .bind(api_key_id)
                        .fetch_optional(pool)
                        .await?;
                Ok(row
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default())
            }
        }
    }

    /// Replace the access_tags on an API key row (looked up by integer id).
    pub async fn set_api_key_tags(&self, api_key_id: i64, tags: &[String]) -> Result<()> {
        let tags_json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".to_string());
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("UPDATE api_keys SET access_tags = ? WHERE id = ?")
                    .bind(&tags_json)
                    .bind(api_key_id)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE api_keys SET access_tags = $1::jsonb WHERE id = $2")
                    .bind(&tags_json)
                    .bind(api_key_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Insert a principal row with `uuid` if one does not already exist, then
    /// unconditionally set its `access_tags`. Used to seed an in-memory DB
    /// from a static YAML grants map (CLI config `tag_based` policy).
    pub async fn seed_principal_tags(&self, uuid: &str, tags: &[String]) -> Result<()> {
        let tags_json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".to_string());
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("INSERT OR IGNORE INTO principals (uuid, type) VALUES (?, 'user')")
                    .bind(uuid)
                    .execute(pool)
                    .await?;
                sqlx::query("UPDATE principals SET access_tags = ? WHERE uuid = ?")
                    .bind(&tags_json)
                    .bind(uuid)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO principals (uuid, type) \
                     VALUES ($1, 'user') ON CONFLICT (uuid) DO NOTHING",
                )
                .bind(uuid)
                .execute(pool)
                .await?;
                sqlx::query("UPDATE principals SET access_tags = $1::jsonb WHERE uuid = $2")
                    .bind(&tags_json)
                    .bind(uuid)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
}

fn parse_tags_json(s: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
}
