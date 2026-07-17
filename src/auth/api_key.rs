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
    /// Optional tag restriction for this key, with one meaning per value
    /// (authn_access_tags narrowing, Python access_policies.py:409;
    /// get_access_tags_from_api_key, authentication.py:261-263):
    /// - `None` — no restriction (stored SQL NULL); the principal's full tag
    ///   set applies.
    /// - `Some(vec![])` — deny ALL tagged access (stored '[]', upstream
    ///   `set([])`); the intersection with an empty set is empty.
    /// - `Some(tags)` — narrow the effective grant to the INTERSECTION of the
    ///   principal's tags and `tags`.
    pub access_tags: Option<Vec<String>>,
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
    /// field was omitted. Persisted with one meaning per value: `None` → SQL
    /// NULL (no restriction); `Some(vec![])` → '[]' (deny ALL tagged access,
    /// upstream `set([])`); `Some(tags)` → the JSON array (narrow to the
    /// intersection). See [`ApiKeyRecord::access_tags`].
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
        // Persist access_tags with one meaning per value: `None` → SQL NULL
        // (no restriction), `Some(v)` → the JSON array `v` — INCLUDING
        // `Some(vec![])` → '[]', which the read side surfaces as an explicit
        // empty restriction (deny all tagged access, upstream `set([])`). No
        // `unwrap_or(&[])` collapse: `None` and `Some([])` must stay distinct.
        let access_tags_json: Option<String> = req
            .access_tags
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

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
                .bind(access_tags_json.as_deref())
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
                .bind(access_tags_json.as_deref())
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
    // NULL → None (no restriction); '[]' → Some(empty) (deny-all); '[a,b]' →
    // Some([a,b]) (narrow). A malformed value falls back to None to keep login
    // working; it can only arise from out-of-band DB corruption.
    let access_tags: Option<Vec<String>> = row
        .try_get::<Option<String>, _>("access_tags")
        .ok()
        .flatten()
        .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok());
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
    // NULL → None (no restriction); '[]' → Some(empty) (deny-all); '[a,b]' →
    // Some([a,b]) (narrow). Mirrors the sqlite mapping.
    let access_tags: Option<Vec<String>> = row
        .try_get::<Option<serde_json::Value>, _>("access_tags")
        .ok()
        .flatten()
        .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok());
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

    /// Persistence, per invariant boundary: each of the three `access_tags`
    /// values round-trips through the INSERT with one meaning, readable back via
    /// `list_api_keys` (the same column the auth middleware feeds into
    /// `authn_access_tags`):
    /// - `None` (omitted)     → SQL NULL → `None` (no restriction)
    /// - `Some(vec![])`       → '[]'     → `Some(empty)` (deny-all — the fix)
    /// - `Some([team-a,...])` → JSON arr → `Some([team-a,...])` (narrow)
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
        assert_eq!(
            restricted.record.access_tags,
            Some(vec!["team-a".to_string(), "team-b".to_string()])
        );

        let deny_all = db
            .create_api_key(create(
                p.id,
                ScopeSet::from_iter([Scope::ReadMetadata]),
                Some(vec![]),
            ))
            .await
            .expect("create deny-all key");
        assert_eq!(
            deny_all.record.access_tags,
            Some(Vec::<String>::new()),
            "an explicit empty access_tags persists as Some(empty) (deny-all)"
        );

        let unrestricted = db
            .create_api_key(create(
                p.id,
                ScopeSet::from_iter([Scope::ReadMetadata]),
                None,
            ))
            .await
            .expect("create unrestricted key");
        assert_eq!(
            unrestricted.record.access_tags, None,
            "no access_tags persists as NULL (no restriction)"
        );

        // Read the tags back from the DB, not just the create return value.
        let keys = db.list_api_keys(Some(p.id)).await.expect("list keys");
        let read_of = |first_eight: &str| {
            keys.iter()
                .find(|k| k.first_eight == first_eight)
                .expect("key present")
                .access_tags
                .clone()
        };
        assert_eq!(
            read_of(&restricted.record.first_eight),
            Some(vec!["team-a".to_string(), "team-b".to_string()])
        );
        assert_eq!(
            read_of(&deny_all.record.first_eight),
            Some(Vec::<String>::new()),
            "explicit [] reads back as Some(empty), not None"
        );
        assert_eq!(read_of(&unrestricted.record.first_eight), None);
    }

    /// Migration boundary (0010): a pre-migration `access_tags = '[]'` row —
    /// written under the old `NOT NULL DEFAULT '[]'` collapse where `None` and
    /// `Some([])` were indistinguishable — must relax to NULL (no restriction)
    /// so no live key is retroactively locked out. A genuinely restricted
    /// non-empty row survives verbatim. Exercises the real migration runner on
    /// the actual 0010 SQL by seeding the pre-0010 world (0001..0009 marked
    /// applied, api_keys in its old shape) so only 0010 runs.
    #[tokio::test]
    async fn migration_0010_relaxes_legacy_empty_access_tags_to_null() {
        // Pin to a single connection so the raw pre-0010 setup, db.migrate(),
        // and the read-back all share one in-memory database deterministically.
        let db = AuthDb::connect_with_pool_size("sqlite::memory:", 1)
            .await
            .expect("in-memory sqlite");
        let AuthPool::Sqlite(pool) = db.pool().clone() else {
            unreachable!("a sqlite: uri yields a sqlite pool");
        };

        // Bookkeeping table with 0001..0009 already applied, so db.migrate()
        // applies ONLY 0010.
        sqlx::query("CREATE TABLE _tiled_auth_migrations (name TEXT PRIMARY KEY, applied_at TEXT)")
            .execute(&pool)
            .await
            .expect("create bookkeeping table");
        for name in [
            "0001_initial",
            "0002_add_principal_role",
            "0003_add_session_refresh_count",
            "0004_add_access_tags",
            "0005_tag_registry",
            "0006_add_session_state",
            "0007_add_pending_sessions",
            "0008_add_oidc_flow_states",
            "0009_hash_device_code",
        ] {
            sqlx::query("INSERT INTO _tiled_auth_migrations (name) VALUES (?)")
                .bind(name)
                .execute(&pool)
                .await
                .expect("seed applied migration");
        }

        // Pre-0010 tables: a minimal principals FK target and api_keys in its
        // old `access_tags TEXT NOT NULL DEFAULT '[]'` shape.
        sqlx::query("CREATE TABLE principals (id INTEGER PRIMARY KEY AUTOINCREMENT)")
            .execute(&pool)
            .await
            .expect("create principals");
        sqlx::query(
            "CREATE TABLE api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                principal_id INTEGER NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
                secret_hash TEXT NOT NULL,
                first_eight TEXT NOT NULL,
                note TEXT,
                scopes TEXT NOT NULL DEFAULT '[]',
                expiration_time TEXT,
                time_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                latest_activity TEXT,
                access_tags TEXT NOT NULL DEFAULT '[]'
            )",
        )
        .execute(&pool)
        .await
        .expect("create old-shape api_keys");
        sqlx::query("INSERT INTO principals DEFAULT VALUES")
            .execute(&pool)
            .await
            .expect("seed principal");
        // Legacy ambiguous '[]' row + a genuinely restricted row.
        sqlx::query(
            "INSERT INTO api_keys (principal_id, secret_hash, first_eight, scopes,
                                   time_created, access_tags)
             VALUES (1, 'h', 'legacy00', '[]', '2024-01-01T00:00:00.000Z', '[]')",
        )
        .execute(&pool)
        .await
        .expect("seed legacy [] key");
        sqlx::query(
            "INSERT INTO api_keys (principal_id, secret_hash, first_eight, scopes,
                                   time_created, access_tags)
             VALUES (1, 'h', 'teama000', '[]', '2024-01-01T00:00:00.000Z', '[\"team-a\"]')",
        )
        .execute(&pool)
        .await
        .expect("seed restricted key");

        // Apply 0010 (and only 0010) through the real runner.
        db.migrate().await.expect("apply migration 0010");

        let keys = db.list_api_keys(Some(1)).await.expect("list keys");
        let legacy = keys
            .iter()
            .find(|k| k.first_eight == "legacy00")
            .expect("legacy row present");
        assert_eq!(
            legacy.access_tags, None,
            "a legacy '[]' row must relax to NULL (no restriction), not lock the key out"
        );
        let restricted = keys
            .iter()
            .find(|k| k.first_eight == "teama000")
            .expect("restricted row present");
        assert_eq!(
            restricted.access_tags,
            Some(vec!["team-a".to_string()]),
            "a genuine non-empty restriction must survive the migration verbatim"
        );
    }
}
