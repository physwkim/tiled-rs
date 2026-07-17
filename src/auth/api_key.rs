//! Multi-user API key CRUD with Argon2id hashing.
//!
//! Plaintext keys are random 256-bit secrets, hex-encoded so they're safe
//! in `Authorization` headers and CLI args. Only the hash + first eight
//! characters are stored; the user is responsible for capturing the
//! plaintext at creation time. `verify` does the constant-time hash
//! comparison.

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::auth::db::{AuthDb, AuthPool};
use crate::auth::error::{AuthError, Result};
use crate::auth::scopes::{Scope, ScopeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub id: i64,
    pub principal_id: i64,
    pub first_eight: String,
    pub note: Option<String>,
    pub scopes: ScopeSet,
    pub expiration_time: Option<chrono::DateTime<chrono::Utc>>,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub latest_activity: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional tag restriction for this key. When non-empty the key's
    /// effective tag grant is the INTERSECTION of the principal's tags and
    /// this set (authn_access_tags narrowing, Python access_policies.py:409).
    /// An empty vec means "no restriction" — principal's full tag set applies.
    pub access_tags: Vec<String>,
}

/// What the caller passes when creating a key.
#[derive(Debug, Clone)]
pub struct ApiKeyCreate {
    pub principal_id: i64,
    pub note: Option<String>,
    pub scopes: ScopeSet,
    pub expiration_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional tag restriction to persist on the key (Python
    /// `APIKeyRequestParams.access_tags`). `Some` — including `Some(vec![])` —
    /// means the caller supplied `access_tags`, which upstream forbids
    /// combining with the `inherit` / `admin:apikeys` scopes. `None` means the
    /// field was omitted (no restriction). Stored as a JSON array; an empty
    /// array reads back as "no restriction" (authn_access_tags).
    pub access_tags: Option<Vec<String>>,
}

/// One-time material returned from `create_api_key`. The caller MUST
/// surface `secret` to the human creating the key — it can't be
/// reconstructed from the DB later.
#[derive(Debug, Clone)]
pub struct KeyMaterial {
    pub record: ApiKeyRecord,
    pub secret: String,
}

const KEY_BYTES: usize = 32; // 256 bits

/// Max API keys allowed per principal. Matches Python tiled's `API_KEY_LIMIT`
/// (authentication.py:84). The routes that list keys are unpaginated, so this
/// bounds their response size and guards against key-table abuse.
const API_KEY_LIMIT: i64 = 100;

impl AuthDb {
    pub async fn create_api_key(&self, req: ApiKeyCreate) -> Result<KeyMaterial> {
        // 0a. A tag-restricted key must not also carry the `inherit` or
        //     `admin:apikeys` scope (upstream `scopes_no_tag_restrict`,
        //     authentication.py:1188-1198). `access_tags is not None` triggers
        //     the check — matching upstream, an explicit empty list still
        //     counts. Enforced here — the sole INSERT-owner for api_keys — so
        //     every create path (API route, admin SPA, CLI) is bound by
        //     construction rather than at each caller.
        if req.access_tags.is_some() {
            let offending: Vec<&str> = [Scope::AdminApiKeys, Scope::Inherit]
                .into_iter()
                .filter(|s| req.scopes.contains(*s))
                .map(|s| s.as_str())
                .collect();
            if !offending.is_empty() {
                return Err(AuthError::Forbidden(format!(
                    "Requested scopes contain {offending:?}, which cannot be \
                     combined with access tag restrictions."
                )));
            }
        }

        // 0b. Enforce the per-principal cap BEFORE any work (Python parity:
        //    authentication.py:1207-1221). Counting is done here — the sole
        //    INSERT-owner for api_keys — so every caller path (API routes,
        //    admin SPA, CLI) is bounded by construction rather than at each
        //    route. Count ALL rows for the principal: upstream's `keys_count`
        //    query filters on principal.id alone, with no expiration/revoked
        //    exclusion, so expired keys still count until deleted.
        let existing = self.count_api_keys(req.principal_id).await?;
        if existing >= API_KEY_LIMIT {
            return Err(AuthError::LimitExceeded(format!(
                "This Principal already has {existing} API keys which is greater \
                 than or equal to the maximum number allowed, {API_KEY_LIMIT}. \
                 Some API keys must be deleted before creating new ones."
            )));
        }

        // 1. Generate plaintext secret. Hex so it's URL/header-safe.
        let mut bytes = [0u8; KEY_BYTES];
        rand::thread_rng().fill_bytes(&mut bytes);
        let secret = hex_encode(&bytes);

        // 2. Hash with Argon2id (default params). Per Argon2 best practice,
        //    a fresh salt per key.
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(secret.as_bytes(), &salt)
            .map_err(|e| AuthError::Hash(e.to_string()))?
            .to_string();

        let first_eight = secret.get(..8).unwrap_or(&secret).to_string();
        let scopes_json = req.scopes.to_json();
        let exp_iso = req.expiration_time.map(|t| t.to_rfc3339());
        // Persist access_tags as a JSON array. `None` and `Some(vec![])` both
        // serialize to "[]" — an empty restriction, which the read side treats
        // as "no restriction" and which matches the column default.
        let access_tags_json = serde_json::to_string(req.access_tags.as_deref().unwrap_or(&[]))?;

        let record = match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO api_keys (principal_id, secret_hash, first_eight,
                                            note, scopes, expiration_time, access_tags)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     RETURNING id, principal_id, first_eight, note, scopes,
                               expiration_time, time_created, latest_activity,
                               access_tags",
                )
                .bind(req.principal_id)
                .bind(&hash)
                .bind(&first_eight)
                .bind(&req.note)
                .bind(&scopes_json)
                .bind(&exp_iso)
                .bind(&access_tags_json)
                .fetch_one(pool)
                .await?;
                api_key_from_sqlite(&row)?
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO api_keys (principal_id, secret_hash, first_eight,
                                            note, scopes, expiration_time, access_tags)
                     VALUES ($1, $2, $3, $4, $5::jsonb, $6::timestamptz, $7::jsonb)
                     RETURNING id, principal_id, first_eight, note, scopes,
                               expiration_time, time_created, latest_activity,
                               access_tags",
                )
                .bind(req.principal_id)
                .bind(&hash)
                .bind(&first_eight)
                .bind(&req.note)
                .bind(&scopes_json)
                .bind(&exp_iso)
                .bind(&access_tags_json)
                .fetch_one(pool)
                .await?;
                api_key_from_postgres(&row)?
            }
        };

        Ok(KeyMaterial { record, secret })
    }

    /// Count every API key row owned by `principal_id`. Matches Python tiled's
    /// `keys_count` query (authentication.py:1207-1213): no expiration/revoked
    /// filter, so expired keys count against [`API_KEY_LIMIT`] until deleted.
    async fn count_api_keys(&self, principal_id: i64) -> Result<i64> {
        let n: i64 = match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM api_keys WHERE principal_id = ?")
                    .bind(principal_id)
                    .fetch_one(pool)
                    .await?
            }
            AuthPool::Postgres(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM api_keys WHERE principal_id = $1")
                    .bind(principal_id)
                    .fetch_one(pool)
                    .await?
            }
        };
        Ok(n)
    }

    pub async fn list_api_keys(&self, principal_id: Option<i64>) -> Result<Vec<ApiKeyRecord>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let rows = if let Some(pid) = principal_id {
                    sqlx::query(
                        "SELECT id, principal_id, first_eight, note, scopes,
                                expiration_time, time_created, latest_activity,
                                access_tags
                           FROM api_keys WHERE principal_id = ? ORDER BY id",
                    )
                    .bind(pid)
                    .fetch_all(pool)
                    .await?
                } else {
                    sqlx::query(
                        "SELECT id, principal_id, first_eight, note, scopes,
                                expiration_time, time_created, latest_activity,
                                access_tags
                           FROM api_keys ORDER BY id",
                    )
                    .fetch_all(pool)
                    .await?
                };
                rows.iter().map(api_key_from_sqlite).collect()
            }
            AuthPool::Postgres(pool) => {
                let rows = if let Some(pid) = principal_id {
                    sqlx::query(
                        "SELECT id, principal_id, first_eight, note, scopes,
                                expiration_time, time_created, latest_activity,
                                access_tags
                           FROM api_keys WHERE principal_id = $1 ORDER BY id",
                    )
                    .bind(pid)
                    .fetch_all(pool)
                    .await?
                } else {
                    sqlx::query(
                        "SELECT id, principal_id, first_eight, note, scopes,
                                expiration_time, time_created, latest_activity,
                                access_tags
                           FROM api_keys ORDER BY id",
                    )
                    .fetch_all(pool)
                    .await?
                };
                rows.iter().map(api_key_from_postgres).collect()
            }
        }
    }

    /// Revoke (delete) an API key matching `first_eight`.
    ///
    /// `principal_id = Some(pid)` scopes the DELETE to keys owned by `pid`
    /// (normal-user path). `principal_id = None` is the admin bypass: deletes
    /// any key with the given prefix regardless of owner (matches Python's
    /// `admin:apikeys` path).
    ///
    /// Returns the row that was removed for log/audit purposes.
    pub async fn revoke_api_key(
        &self,
        first_eight: &str,
        principal_id: Option<i64>,
    ) -> Result<ApiKeyRecord> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = if let Some(pid) = principal_id {
                    sqlx::query(
                        "DELETE FROM api_keys
                         WHERE first_eight = ? AND principal_id = ?
                         RETURNING id, principal_id, first_eight, note, scopes,
                                   expiration_time, time_created, latest_activity,
                                   access_tags",
                    )
                    .bind(first_eight)
                    .bind(pid)
                    .fetch_optional(pool)
                    .await?
                } else {
                    sqlx::query(
                        "DELETE FROM api_keys WHERE first_eight = ?
                         RETURNING id, principal_id, first_eight, note, scopes,
                                   expiration_time, time_created, latest_activity,
                                   access_tags",
                    )
                    .bind(first_eight)
                    .fetch_optional(pool)
                    .await?
                };
                let row = row.ok_or_else(|| {
                    AuthError::NotFound(format!("api key with prefix {first_eight}"))
                })?;
                api_key_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = if let Some(pid) = principal_id {
                    sqlx::query(
                        "DELETE FROM api_keys
                         WHERE first_eight = $1 AND principal_id = $2
                         RETURNING id, principal_id, first_eight, note, scopes,
                                   expiration_time, time_created, latest_activity,
                                   access_tags",
                    )
                    .bind(first_eight)
                    .bind(pid)
                    .fetch_optional(pool)
                    .await?
                } else {
                    sqlx::query(
                        "DELETE FROM api_keys WHERE first_eight = $1
                         RETURNING id, principal_id, first_eight, note, scopes,
                                   expiration_time, time_created, latest_activity,
                                   access_tags",
                    )
                    .bind(first_eight)
                    .fetch_optional(pool)
                    .await?
                };
                let row = row.ok_or_else(|| {
                    AuthError::NotFound(format!("api key with prefix {first_eight}"))
                })?;
                api_key_from_postgres(&row)
            }
        }
    }

    /// Verify the supplied plaintext secret. Returns the matching record on
    /// success; on no-match returns `Unauthorized`. Does **not** leak the
    /// reason (timing-safe via Argon2's verify).
    pub async fn verify_api_key(&self, secret: &str) -> Result<ApiKeyRecord> {
        let first_eight = secret.get(..8).unwrap_or(secret).to_string();

        // Fetch all candidates with this prefix. Argon2 hash-verify is
        // O(n) on candidate count; the prefix index keeps n small (a few
        // collisions across users, normally just one).
        let rows = match self.pool() {
            AuthPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT id, principal_id, first_eight, note, scopes,
                            expiration_time, time_created, latest_activity,
                            access_tags, secret_hash
                       FROM api_keys WHERE first_eight = ?",
                )
                .bind(&first_eight)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(|r| {
                        let hash: String = r.get("secret_hash");
                        let rec = api_key_from_sqlite(&r)?;
                        Ok::<_, AuthError>((rec, hash))
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            AuthPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, principal_id, first_eight, note, scopes,
                            expiration_time, time_created, latest_activity,
                            access_tags, secret_hash
                       FROM api_keys WHERE first_eight = $1",
                )
                .bind(&first_eight)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(|r| {
                        let hash: String = r.get("secret_hash");
                        let rec = api_key_from_postgres(&r)?;
                        Ok::<_, AuthError>((rec, hash))
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };
        for (rec, hash_text) in rows {
            let parsed = match PasswordHash::new(&hash_text) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(target: "tiled.auth", "stored hash unreadable: {e}");
                    continue;
                }
            };
            if Argon2::default()
                .verify_password(secret.as_bytes(), &parsed)
                .is_ok()
            {
                if let Some(exp) = rec.expiration_time
                    && exp <= chrono::Utc::now()
                {
                    return Err(AuthError::Expired);
                }
                self.touch_api_key(rec.id).await.ok();
                return Ok(rec);
            }
        }
        Err(AuthError::Unauthorized("api key did not match".into()))
    }

    pub async fn touch_api_key(&self, api_key_id: i64) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE api_keys SET latest_activity = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       WHERE id = ?",
                )
                .bind(api_key_id)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("UPDATE api_keys SET latest_activity = now() WHERE id = $1")
                    .bind(api_key_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn api_key_from_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<ApiKeyRecord> {
    use crate::auth::principal::parse_dt_sqlite;
    let scopes_text: String = row.get("scopes");
    let access_tags_text: String = row.try_get("access_tags").unwrap_or_default();
    let access_tags: Vec<String> = serde_json::from_str(&access_tags_text).unwrap_or_default();
    Ok(ApiKeyRecord {
        id: row.get("id"),
        principal_id: row.get("principal_id"),
        first_eight: row.get("first_eight"),
        note: row.try_get("note").ok(),
        scopes: ScopeSet::from_json(&scopes_text)?,
        expiration_time: row
            .try_get::<String, _>("expiration_time")
            .ok()
            .and_then(|s| parse_dt_sqlite(s).ok()),
        time_created: parse_dt_sqlite(row.get::<String, _>("time_created"))?,
        latest_activity: row
            .try_get::<String, _>("latest_activity")
            .ok()
            .and_then(|s| parse_dt_sqlite(s).ok()),
        access_tags,
    })
}

fn api_key_from_postgres(row: &sqlx::postgres::PgRow) -> Result<ApiKeyRecord> {
    let scopes_value: serde_json::Value = row.get("scopes");
    let scopes: ScopeSet = serde_json::from_value(scopes_value)?;
    let access_tags: Vec<String> = row
        .try_get::<serde_json::Value, _>("access_tags")
        .ok()
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    Ok(ApiKeyRecord {
        id: row.get("id"),
        principal_id: row.get("principal_id"),
        first_eight: row.get("first_eight"),
        note: row.try_get("note").ok(),
        scopes,
        expiration_time: row.try_get("expiration_time").ok(),
        time_created: row.get("time_created"),
        latest_activity: row.try_get("latest_activity").ok(),
        access_tags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mem_db() -> AuthDb {
        let db = AuthDb::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        db.migrate().await.expect("migrations");
        db
    }

    fn create(
        principal_id: i64,
        scopes: ScopeSet,
        access_tags: Option<Vec<String>>,
    ) -> ApiKeyCreate {
        ApiKeyCreate {
            principal_id,
            note: None,
            scopes,
            expiration_time: None,
            access_tags,
        }
    }

    /// The `scopes_no_tag_restrict` guard (upstream authentication.py:1188-1198):
    /// a key that carries `inherit` or `admin:apikeys` may NOT also be
    /// tag-restricted. Enforced at every boundary value of `access_tags`
    /// (`None` / `Some([])` / `Some([tag])`) crossed with each broad scope,
    /// and confirmed not to fire for a narrow scope. Lives in `create_api_key`,
    /// the sole INSERT owner, so it binds every create path.
    #[tokio::test]
    async fn create_api_key_tag_restriction_guard() {
        let db = mem_db().await;
        let p = db.create_principal("user").await.expect("principal");
        let tags = || Some(vec!["team-a".to_string()]);

        // inherit + access_tags → Forbidden.
        let err = db
            .create_api_key(create(p.id, ScopeSet::from_iter([Scope::Inherit]), tags()))
            .await
            .unwrap_err();
        assert!(
            matches!(err, AuthError::Forbidden(_)),
            "inherit + access_tags must be Forbidden, got {err:?}"
        );

        // admin:apikeys + access_tags → Forbidden.
        let err = db
            .create_api_key(create(
                p.id,
                ScopeSet::from_iter([Scope::AdminApiKeys]),
                tags(),
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(err, AuthError::Forbidden(_)),
            "admin:apikeys + access_tags must be Forbidden, got {err:?}"
        );

        // An explicit EMPTY access_tags still counts as "provided" (upstream's
        // `access_tags is not None`), so it also trips the guard.
        let err = db
            .create_api_key(create(
                p.id,
                ScopeSet::from_iter([Scope::Inherit]),
                Some(vec![]),
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(err, AuthError::Forbidden(_)),
            "inherit + empty access_tags must be Forbidden, got {err:?}"
        );

        // Narrow scope + access_tags → allowed.
        let ok = db
            .create_api_key(create(
                p.id,
                ScopeSet::from_iter([Scope::ReadMetadata]),
                tags(),
            ))
            .await;
        assert!(
            ok.is_ok(),
            "narrow scope + access_tags must be allowed, got {ok:?}"
        );

        // inherit WITHOUT access_tags → allowed (guard only fires when the
        // caller supplied access_tags).
        let ok = db
            .create_api_key(create(p.id, ScopeSet::from_iter([Scope::Inherit]), None))
            .await;
        assert!(
            ok.is_ok(),
            "inherit with no access_tags must be allowed, got {ok:?}"
        );
    }

    /// Persistence: a supplied `access_tags` round-trips through the INSERT and
    /// is readable back via `list_api_keys` (the same column the auth
    /// middleware feeds into `authn_access_tags`). `None` persists as an empty
    /// restriction.
    #[tokio::test]
    async fn create_api_key_persists_access_tags() {
        let db = mem_db().await;
        let p = db.create_principal("user").await.expect("principal");

        let restricted = db
            .create_api_key(create(
                p.id,
                ScopeSet::from_iter([Scope::ReadMetadata]),
                Some(vec!["team-a".to_string(), "team-b".to_string()]),
            ))
            .await
            .expect("create restricted key");
        // The returned record carries the tags immediately.
        assert_eq!(restricted.record.access_tags, vec!["team-a", "team-b"]);

        let unrestricted = db
            .create_api_key(create(
                p.id,
                ScopeSet::from_iter([Scope::ReadMetadata]),
                None,
            ))
            .await
            .expect("create unrestricted key");
        assert!(
            unrestricted.record.access_tags.is_empty(),
            "no access_tags persists as an empty (no-restriction) list"
        );

        // Read the tags back from the DB, not just the create return value.
        let keys = db.list_api_keys(Some(p.id)).await.expect("list keys");
        let restricted_read = keys
            .iter()
            .find(|k| k.first_eight == restricted.record.first_eight)
            .expect("restricted key present");
        assert_eq!(restricted_read.access_tags, vec!["team-a", "team-b"]);
        let unrestricted_read = keys
            .iter()
            .find(|k| k.first_eight == unrestricted.record.first_eight)
            .expect("unrestricted key present");
        assert!(unrestricted_read.access_tags.is_empty());
    }
}
