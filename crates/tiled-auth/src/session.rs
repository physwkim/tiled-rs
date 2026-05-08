//! Session lifecycle.
//!
//! A session row represents one logged-in user agent. When the user
//! `/auth/logout`s the row's `revoked` flag flips to true, and every JWT
//! that names that session UUID stops being honoured immediately — even if
//! the JWT itself has not yet expired.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::db::{AuthDb, AuthPool};
use crate::error::{AuthError, Result};
use crate::principal::parse_dt_sqlite;
use crate::scopes::ScopeSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: i64,
    pub principal_id: i64,
    pub uuid: String,
    pub time_last_used: Option<DateTime<Utc>>,
    pub expiration_time: DateTime<Utc>,
    pub revoked: bool,
    pub scopes: ScopeSet,
    pub time_created: DateTime<Utc>,
}

/// Marker trait implemented by [`AuthDb`] so the rest of the crate can
/// reference "the session store" without leaking sqlx types.
pub trait SessionStore: Send + Sync {}
impl SessionStore for AuthDb {}

impl AuthDb {
    pub async fn create_session(
        &self,
        principal_id: i64,
        scopes: ScopeSet,
        expires_at: DateTime<Utc>,
    ) -> Result<SessionRecord> {
        let new_uuid = Uuid::new_v4().to_string();
        let scopes_json = scopes.to_json();
        let expires_iso = expires_at.to_rfc3339();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO sessions (principal_id, uuid, expiration_time, scopes)
                     VALUES (?, ?, ?, ?)
                     RETURNING id, principal_id, uuid, time_last_used,
                               expiration_time, revoked, scopes, time_created",
                )
                .bind(principal_id)
                .bind(&new_uuid)
                .bind(&expires_iso)
                .bind(&scopes_json)
                .fetch_one(pool)
                .await?;
                session_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO sessions (principal_id, uuid, expiration_time, scopes)
                     VALUES ($1, $2, $3::timestamptz, $4::jsonb)
                     RETURNING id, principal_id, uuid, time_last_used,
                               expiration_time, revoked, scopes, time_created",
                )
                .bind(principal_id)
                .bind(&new_uuid)
                .bind(&expires_iso)
                .bind(&scopes_json)
                .fetch_one(pool)
                .await?;
                session_from_postgres(&row)
            }
        }
    }

    pub async fn lookup_session(&self, uuid: &str) -> Result<SessionRecord> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, principal_id, uuid, time_last_used, expiration_time,
                            revoked, scopes, time_created
                       FROM sessions WHERE uuid = ?",
                )
                .bind(uuid)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound(format!("session {uuid}")))?;
                session_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, principal_id, uuid, time_last_used, expiration_time,
                            revoked, scopes, time_created
                       FROM sessions WHERE uuid = $1",
                )
                .bind(uuid)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound(format!("session {uuid}")))?;
                session_from_postgres(&row)
            }
        }
    }

    /// Mark every session belonging to `principal_id` revoked — used when
    /// the user clicks "logout everywhere".
    pub async fn revoke_all_sessions(&self, principal_id: i64) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("UPDATE sessions SET revoked = 1 WHERE principal_id = ?")
                    .bind(principal_id)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE sessions SET revoked = TRUE WHERE principal_id = $1")
                    .bind(principal_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Mark a session row revoked. Idempotent.
    pub async fn revoke_session(&self, uuid: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("UPDATE sessions SET revoked = 1 WHERE uuid = ?")
                    .bind(uuid)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE sessions SET revoked = TRUE WHERE uuid = $1")
                    .bind(uuid)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Touch `time_last_used = now`. Best-effort — failures are not
    /// surfaced because skipping the touch shouldn't fail the request.
    pub async fn touch_session(&self, uuid: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE sessions SET time_last_used = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       WHERE uuid = ?",
                )
                .bind(uuid)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE sessions SET time_last_used = now() WHERE uuid = $1")
                    .bind(uuid)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
}

fn session_from_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<SessionRecord> {
    let scopes_text: String = row.get("scopes");
    Ok(SessionRecord {
        id: row.get("id"),
        principal_id: row.get("principal_id"),
        uuid: row.get("uuid"),
        time_last_used: row
            .try_get::<String, _>("time_last_used")
            .ok()
            .and_then(|s| parse_dt_sqlite(s).ok()),
        expiration_time: parse_dt_sqlite(row.get::<String, _>("expiration_time"))?,
        revoked: row.get::<i64, _>("revoked") != 0,
        scopes: ScopeSet::from_json(&scopes_text)?,
        time_created: parse_dt_sqlite(row.get::<String, _>("time_created"))?,
    })
}

fn session_from_postgres(row: &sqlx::postgres::PgRow) -> Result<SessionRecord> {
    let scopes_value: serde_json::Value = row.get("scopes");
    let scopes: ScopeSet = serde_json::from_value(scopes_value)?;
    Ok(SessionRecord {
        id: row.get("id"),
        principal_id: row.get("principal_id"),
        uuid: row.get("uuid"),
        time_last_used: row.try_get("time_last_used").ok(),
        expiration_time: row.get("expiration_time"),
        revoked: row.get("revoked"),
        scopes,
        time_created: row.get("time_created"),
    })
}
