//! Principal + Identity records.
//!
//! `Principal` represents an authenticated subject (user or service);
//! `Identity` is the (provider, sub) pair that resolves to it. One
//! principal can have many identities (e.g. password + OIDC linked).

use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::db::{AuthDb, AuthPool};
use crate::error::{AuthError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub id: i64,
    pub uuid: String,
    pub r#type: String, // 'user' | 'service'
    /// Role that determines the principal's scope ceiling at login.
    /// Known roles: `"user"` (default), `"admin"`. See `ScopeSet::for_role`.
    pub role: String,
    pub time_created: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub id: i64,
    pub principal_id: i64,
    pub provider: String,
    pub sub: String,
    pub latest_login: Option<chrono::DateTime<chrono::Utc>>,
}

impl AuthDb {
    /// Find or create a principal keyed by `(provider, sub)`. Used by every
    /// authenticator path: each successful login reaches here so we never
    /// create duplicate principals for the same external identity.
    pub async fn ensure_principal(
        &self,
        provider: &str,
        sub: &str,
    ) -> Result<(Principal, Identity)> {
        if let Some(found) = self.find_identity(provider, sub).await? {
            let principal = self
                .get_principal(found.principal_id)
                .await?
                .ok_or_else(|| AuthError::NotFound("principal vanished".into()))?;
            return Ok((principal, found));
        }
        // Create both rows in a single transaction so a crash mid-create
        // can't leave an Identity pointing at a non-existent Principal.
        let principal = self.create_principal("user").await?;
        let identity = self.create_identity(principal.id, provider, sub).await?;
        Ok((principal, identity))
    }

    pub async fn update_principal_role(&self, principal_id: i64, role: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("UPDATE principals SET role = ? WHERE id = ?")
                    .bind(role)
                    .bind(principal_id)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE principals SET role = $1 WHERE id = $2")
                    .bind(role)
                    .bind(principal_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Create a standalone service principal with the given role. Unlike
    /// [`ensure_principal`], this creates no Identity row: the principal is
    /// accessed only via API keys, not via login. Mirrors Python's
    /// `create_service` in `authn_database/core.py`.
    pub async fn create_service_principal(&self, role: &str) -> Result<Principal> {
        let new_uuid = Uuid::new_v4().to_string();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO principals (uuid, type, role) VALUES (?, 'service', ?)
                     RETURNING id, uuid, type, role, time_created",
                )
                .bind(&new_uuid)
                .bind(role)
                .fetch_one(pool)
                .await?;
                Ok(principal_from_sqlite(&row)?)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO principals (uuid, type, role) VALUES ($1, 'service', $2)
                     RETURNING id, uuid, type, role, time_created",
                )
                .bind(&new_uuid)
                .bind(role)
                .fetch_one(pool)
                .await?;
                Ok(principal_from_postgres(&row)?)
            }
        }
    }

    pub async fn create_principal(&self, kind: &str) -> Result<Principal> {
        let new_uuid = Uuid::new_v4().to_string();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO principals (uuid, type) VALUES (?, ?)
                     RETURNING id, uuid, type, role, time_created",
                )
                .bind(&new_uuid)
                .bind(kind)
                .fetch_one(pool)
                .await?;
                Ok(principal_from_sqlite(&row)?)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO principals (uuid, type) VALUES ($1, $2)
                     RETURNING id, uuid, type, role, time_created",
                )
                .bind(&new_uuid)
                .bind(kind)
                .fetch_one(pool)
                .await?;
                Ok(principal_from_postgres(&row)?)
            }
        }
    }

    pub async fn get_principal(&self, id: i64) -> Result<Option<Principal>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals WHERE id = ?",
                )
                .bind(id)
                .fetch_optional(pool)
                .await?;
                row.map(|r| principal_from_sqlite(&r)).transpose()
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals WHERE id = $1",
                )
                .bind(id)
                .fetch_optional(pool)
                .await?;
                row.map(|r| principal_from_postgres(&r)).transpose()
            }
        }
    }

    pub async fn create_identity(
        &self,
        principal_id: i64,
        provider: &str,
        sub: &str,
    ) -> Result<Identity> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO identities (principal_id, provider, sub)
                     VALUES (?, ?, ?)
                     RETURNING id, principal_id, provider, sub, latest_login",
                )
                .bind(principal_id)
                .bind(provider)
                .bind(sub)
                .fetch_one(pool)
                .await?;
                Ok(identity_from_sqlite(&row)?)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO identities (principal_id, provider, sub)
                     VALUES ($1, $2, $3)
                     RETURNING id, principal_id, provider, sub, latest_login",
                )
                .bind(principal_id)
                .bind(provider)
                .bind(sub)
                .fetch_one(pool)
                .await?;
                Ok(identity_from_postgres(&row)?)
            }
        }
    }

    pub async fn find_identity(&self, provider: &str, sub: &str) -> Result<Option<Identity>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, principal_id, provider, sub, latest_login
                       FROM identities WHERE provider = ? AND sub = ?",
                )
                .bind(provider)
                .bind(sub)
                .fetch_optional(pool)
                .await?;
                row.map(|r| identity_from_sqlite(&r)).transpose()
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, principal_id, provider, sub, latest_login
                       FROM identities WHERE provider = $1 AND sub = $2",
                )
                .bind(provider)
                .bind(sub)
                .fetch_optional(pool)
                .await?;
                row.map(|r| identity_from_postgres(&r)).transpose()
            }
        }
    }

    /// Stamp `latest_login = now` on the given identity. Best-effort; a
    /// failure here doesn't fail login.
    pub async fn touch_identity_login(&self, identity_id: i64) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE identities SET latest_login = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                     WHERE id = ?",
                )
                .bind(identity_id)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE identities SET latest_login = now() WHERE id = $1")
                    .bind(identity_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
}

fn principal_from_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<Principal> {
    Ok(Principal {
        id: row.get("id"),
        uuid: row.get("uuid"),
        r#type: row.get("type"),
        role: row.get("role"),
        time_created: parse_dt_sqlite(row.get::<String, _>("time_created"))?,
    })
}

fn principal_from_postgres(row: &sqlx::postgres::PgRow) -> Result<Principal> {
    Ok(Principal {
        id: row.get("id"),
        uuid: row.get("uuid"),
        r#type: row.get("type"),
        role: row.get("role"),
        time_created: row.get("time_created"),
    })
}

fn identity_from_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<Identity> {
    Ok(Identity {
        id: row.get("id"),
        principal_id: row.get("principal_id"),
        provider: row.get("provider"),
        sub: row.get("sub"),
        latest_login: row
            .try_get::<String, _>("latest_login")
            .ok()
            .and_then(|s| parse_dt_sqlite(s).ok()),
    })
}

fn identity_from_postgres(row: &sqlx::postgres::PgRow) -> Result<Identity> {
    Ok(Identity {
        id: row.get("id"),
        principal_id: row.get("principal_id"),
        provider: row.get("provider"),
        sub: row.get("sub"),
        latest_login: row.try_get("latest_login").ok(),
    })
}

pub(crate) fn parse_dt_sqlite(s: String) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(&s)
        .map(|d| d.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.fZ").map(|n| n.and_utc())
        })
        .map_err(|e| AuthError::Validation(format!("bad timestamp {s}: {e}")))
}
