//! Principal + Identity records.
//!
//! `Principal` represents an authenticated subject (user or service);
//! `Identity` is the (provider, sub) pair that resolves to it. One
//! principal can have many identities (e.g. password + OIDC linked).

use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::auth::db::{AuthDb, AuthPool};
use crate::auth::error::{AuthError, Result};

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

/// API-facing view of one identity. Mirrors Python `schemas.Identity`
/// (`schemas.py:315`): the public `id` is the upstream **subject** (`sub`),
/// not the internal row primary key, and `(provider, id)` is the unique pair
/// (`orm.py:154`). The internal integer row id and the `principal_id`
/// foreign key are deliberately not exposed.
#[derive(Debug, Clone, Serialize)]
pub struct IdentityView {
    pub id: String,
    pub provider: String,
    pub latest_login: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<Identity> for IdentityView {
    fn from(identity: Identity) -> Self {
        Self {
            id: identity.sub,
            provider: identity.provider,
            latest_login: identity.latest_login,
        }
    }
}

/// API-facing view of a principal together with all of its linked
/// identities. Mirrors the subset of Python `schemas.Principal`
/// (`schemas.py:403`) that tiled-rs models — the internal integer row id is
/// not exposed; the public handle is the `uuid`. The `identities` array is
/// the multi-provider mapping populated via [`AuthDb::get_principal_detail`]
/// (the `selectinload(Principal.identities)` equivalent,
/// `authentication.py:1345`).
#[derive(Debug, Clone, Serialize)]
pub struct PrincipalDetail {
    pub uuid: String,
    pub r#type: String,
    pub role: String,
    pub identities: Vec<IdentityView>,
}

impl PrincipalDetail {
    pub fn new(principal: Principal, identities: Vec<Identity>) -> Self {
        Self {
            uuid: principal.uuid,
            r#type: principal.r#type,
            role: principal.role,
            identities: identities.into_iter().map(IdentityView::from).collect(),
        }
    }
}

impl AuthDb {
    /// Ensure the principal identified by `(provider, id)` has the `"admin"`
    /// role, creating the identity if it does not yet exist. Idempotent:
    /// calling it multiple times for the same identity is safe. Mirrors
    /// Python's `make_admin_by_identity` in `authn_database/core.py`.
    pub async fn make_admin_by_identity(&self, provider: &str, id: &str) -> Result<()> {
        let (principal, _) = self.ensure_principal(provider, id).await?;
        if principal.role != "admin" {
            self.update_principal_role(principal.id, "admin").await?;
        }
        Ok(())
    }

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

    /// Look up a principal by its public `uuid`. Returns the full [`Principal`]
    /// row (including the internal `id`) so callers that need the integer FK
    /// (e.g. to create or revoke API keys on behalf of an admin) can proceed
    /// without a second round-trip. Returns `None` when no principal has the
    /// given uuid.
    pub async fn get_principal_by_uuid(&self, uuid: &str) -> Result<Option<Principal>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals WHERE uuid = ?",
                )
                .bind(uuid)
                .fetch_optional(pool)
                .await?;
                row.map(|r| principal_from_sqlite(&r)).transpose()
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals WHERE uuid = $1",
                )
                .bind(uuid)
                .fetch_optional(pool)
                .await?;
                row.map(|r| principal_from_postgres(&r)).transpose()
            }
        }
    }

    /// List every identity linked to a principal — the
    /// `selectinload(Principal.identities)` equivalent
    /// (`authentication.py:1345`). Ordered by `(provider, sub)` for a stable
    /// response.
    pub async fn list_identities(&self, principal_id: i64) -> Result<Vec<Identity>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT id, principal_id, provider, sub, latest_login
                       FROM identities WHERE principal_id = ? ORDER BY provider, sub",
                )
                .bind(principal_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(identity_from_sqlite).collect()
            }
            AuthPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, principal_id, provider, sub, latest_login
                       FROM identities WHERE principal_id = $1 ORDER BY provider, sub",
                )
                .bind(principal_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(identity_from_postgres).collect()
            }
        }
    }

    /// Load a principal by its public `uuid` together with all of its linked
    /// identities (two queries — the `selectinload` equivalent,
    /// `authentication.py:1340-1351`). Returns `None` when no principal has
    /// the given uuid. Backs the admin-gated `GET /auth/principal/{uuid}`
    /// endpoint.
    pub async fn get_principal_detail(&self, uuid: &str) -> Result<Option<PrincipalDetail>> {
        let principal = match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals WHERE uuid = ?",
                )
                .bind(uuid)
                .fetch_optional(pool)
                .await?;
                row.map(|r| principal_from_sqlite(&r)).transpose()?
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals WHERE uuid = $1",
                )
                .bind(uuid)
                .fetch_optional(pool)
                .await?;
                row.map(|r| principal_from_postgres(&r)).transpose()?
            }
        };
        let Some(principal) = principal else {
            return Ok(None);
        };
        let identities = self.list_identities(principal.id).await?;
        Ok(Some(PrincipalDetail::new(principal, identities)))
    }

    /// List principals (paginated, ordered by id) together with each one's
    /// linked identities — the DB-direct equivalent of the admin
    /// `GET /auth/principal` list endpoint (`authentication.py:1247-1286`,
    /// which uses `selectinload(Principal.identities)`). Backs the
    /// `tiled admin list-principals` CLI command.
    pub async fn list_principals(&self, offset: i64, limit: i64) -> Result<Vec<PrincipalDetail>> {
        let principals: Vec<Principal> = match self.pool() {
            AuthPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals
                       ORDER BY id LIMIT ? OFFSET ?",
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(pool)
                .await?;
                rows.iter()
                    .map(principal_from_sqlite)
                    .collect::<Result<Vec<_>>>()?
            }
            AuthPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, uuid, type, role, time_created FROM principals
                       ORDER BY id LIMIT $1 OFFSET $2",
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(pool)
                .await?;
                rows.iter()
                    .map(principal_from_postgres)
                    .collect::<Result<Vec<_>>>()?
            }
        };
        let mut out = Vec::with_capacity(principals.len());
        for principal in principals {
            let identities = self.list_identities(principal.id).await?;
            out.push(PrincipalDetail::new(principal, identities));
        }
        Ok(out)
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
