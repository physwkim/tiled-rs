//! Per-principal and per-API-key access-tag CRUD, plus the tag registry
//! and per-tag scope assignments introduced by migration 0005.
//!
//! `access_tags` is a JSON array of tag strings stored on `principals` and
//! `api_keys`. For principals it represents the full set of tags a user may
//! access. For API keys it is an optional narrowing subset (authn_access_tags).
//!
//! Mirrors Python `TagBasedAccessPolicy` / `get_tags_from_scope` semantics:
//! every tag in `principal.access_tags` confers the policy's `default_scopes`
//! to that principal (access_policies.py:391-398), unless per-tag scopes are
//! configured in the `tag_scopes` table (migration 0005).

use crate::db::{AuthDb, AuthPool};
use crate::error::Result;

impl AuthDb {
    // ---- Principal / API-key access-tag CRUD --------------------------------

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

    // ---- Tag registry (migration 0005) --------------------------------------

    /// True if `tag` has been registered in the `tags` table.
    /// The "public" built-in is NOT stored in the registry; callers must
    /// special-case it before calling this method.
    pub async fn is_tag_defined(&self, tag: &str) -> Result<bool> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row: Option<String> =
                    sqlx::query_scalar("SELECT name FROM tags WHERE name = ?")
                        .bind(tag)
                        .fetch_optional(pool)
                        .await?;
                Ok(row.is_some())
            }
            AuthPool::Postgres(pool) => {
                let row: Option<String> =
                    sqlx::query_scalar("SELECT name FROM tags WHERE name = $1")
                        .bind(tag)
                        .fetch_optional(pool)
                        .await?;
                Ok(row.is_some())
            }
        }
    }

    /// Insert `tag` into the registry. No-op if already present.
    pub async fn define_tag(&self, tag: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("INSERT OR IGNORE INTO tags (name) VALUES (?)")
                    .bind(tag)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("INSERT INTO tags (name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
                    .bind(tag)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Return all registered tag names in alphabetical order.
    pub async fn list_tags(&self) -> Result<Vec<String>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let rows: Vec<String> = sqlx::query_scalar("SELECT name FROM tags ORDER BY name")
                    .fetch_all(pool)
                    .await?;
                Ok(rows)
            }
            AuthPool::Postgres(pool) => {
                let rows: Vec<String> = sqlx::query_scalar("SELECT name FROM tags ORDER BY name")
                    .fetch_all(pool)
                    .await?;
                Ok(rows)
            }
        }
    }

    // ---- Per-tag scope assignments (migration 0005) -------------------------

    /// Return the scope strings assigned to `tag` in `tag_scopes`.
    /// An empty return means no rows exist for this tag — the caller should
    /// fall back to the policy's `default_scopes` for backward compatibility.
    pub async fn get_tag_scopes(&self, tag: &str) -> Result<Vec<String>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let rows: Vec<String> =
                    sqlx::query_scalar("SELECT scope FROM tag_scopes WHERE tag = ?")
                        .bind(tag)
                        .fetch_all(pool)
                        .await?;
                Ok(rows)
            }
            AuthPool::Postgres(pool) => {
                let rows: Vec<String> =
                    sqlx::query_scalar("SELECT scope FROM tag_scopes WHERE tag = $1")
                        .bind(tag)
                        .fetch_all(pool)
                        .await?;
                Ok(rows)
            }
        }
    }

    /// Replace all scope assignments for `tag`. The tag must already exist in
    /// the registry. Passing an empty slice clears all scopes (= use default).
    pub async fn set_tag_scopes(&self, tag: &str, scopes: &[String]) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM tag_scopes WHERE tag = ?")
                    .bind(tag)
                    .execute(pool)
                    .await?;
                for scope in scopes {
                    sqlx::query("INSERT INTO tag_scopes (tag, scope) VALUES (?, ?)")
                        .bind(tag)
                        .bind(scope)
                        .execute(pool)
                        .await?;
                }
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("DELETE FROM tag_scopes WHERE tag = $1")
                    .bind(tag)
                    .execute(pool)
                    .await?;
                for scope in scopes {
                    sqlx::query("INSERT INTO tag_scopes (tag, scope) VALUES ($1, $2)")
                        .bind(tag)
                        .bind(scope)
                        .execute(pool)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Upsert a tag with its scope assignments. Creates the tag in the registry
    /// if absent, then replaces its scopes. Used for seeding in tests and
    /// static config (`seed_tag("team-a", &["read:metadata", "write:metadata"])`).
    pub async fn seed_tag(&self, tag: &str, scopes: &[String]) -> Result<()> {
        self.define_tag(tag).await?;
        self.set_tag_scopes(tag, scopes).await?;
        Ok(())
    }
}

fn parse_tags_json(s: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
}
