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
    /// OBO session state — a JSON object embedded verbatim in every access
    /// token's `state` claim (Python `Session.state`, authentication.py:857).
    /// For an Entra code-flow login it carries `entra_access_token` /
    /// `entra_refresh_token`; `{}` for every other session.
    pub state: serde_json::Value,
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
        state: serde_json::Value,
    ) -> Result<SessionRecord> {
        let new_uuid = Uuid::new_v4().to_string();
        let scopes_json = scopes.to_json();
        let expires_iso = expires_at.to_rfc3339();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                // SQLite stores `state` as TEXT JSON, mirroring `scopes`.
                let state_json = state.to_string();
                let row = sqlx::query(
                    "INSERT INTO sessions (principal_id, uuid, expiration_time, scopes, state)
                     VALUES (?, ?, ?, ?, ?)
                     RETURNING id, principal_id, uuid, time_last_used,
                               expiration_time, revoked, scopes, time_created, state",
                )
                .bind(principal_id)
                .bind(&new_uuid)
                .bind(&expires_iso)
                .bind(&scopes_json)
                .bind(&state_json)
                .fetch_one(pool)
                .await?;
                session_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO sessions (principal_id, uuid, expiration_time, scopes, state)
                     VALUES ($1, $2, $3::timestamptz, $4::jsonb, $5::jsonb)
                     RETURNING id, principal_id, uuid, time_last_used,
                               expiration_time, revoked, scopes, time_created, state",
                )
                .bind(principal_id)
                .bind(&new_uuid)
                .bind(&expires_iso)
                .bind(&scopes_json)
                .bind(&state)
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
                            revoked, scopes, time_created, state
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
                            revoked, scopes, time_created, state
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

    /// Look up a session by its integer primary key. Used by the IdP device
    /// flow's token route, which stores the bound session's `id` (not its
    /// UUID) in `pending_sessions.session_id` (FK to `sessions.id`).
    pub async fn lookup_session_by_id(&self, id: i64) -> Result<SessionRecord> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, principal_id, uuid, time_last_used, expiration_time,
                            revoked, scopes, time_created, state
                       FROM sessions WHERE id = ?",
                )
                .bind(id)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound(format!("session id {id}")))?;
                session_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, principal_id, uuid, time_last_used, expiration_time,
                            revoked, scopes, time_created, state
                       FROM sessions WHERE id = $1",
                )
                .bind(id)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound(format!("session id {id}")))?;
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

    /// Increment `refresh_count` atomically. Best-effort — failures are not
    /// surfaced because skipping the counter shouldn't fail the request.
    pub async fn increment_refresh_count(&self, uuid: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("UPDATE sessions SET refresh_count = refresh_count + 1 WHERE uuid = ?")
                    .bind(uuid)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE sessions SET refresh_count = refresh_count + 1 WHERE uuid = $1",
                )
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
    let state_text: String = row.get("state");
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
        state: serde_json::from_str(&state_text)?,
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
        state: row.get("state"),
    })
}
