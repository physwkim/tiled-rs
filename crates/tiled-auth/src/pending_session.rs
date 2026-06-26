//! IdP-brokered OAuth2 device-code flow — pending-session store.
//!
//! Distinct from [`crate::device_code`] (tiled's NATIVE RFC 8628 grant against
//! local principals). This flow brokers a device login through an EXTERNAL OIDC
//! provider: the CLI polls tiled while the user completes the IdP's
//! authorization-code flow in a browser. Mirrors Python tiled's
//! `pending_sessions` table + device-code routes (authentication.py:980-1133,
//! authn_database/core.py:176-220).
//!
//! Lifecycle:
//! 1. `POST /auth/provider/{p}/authorize` → [`AuthDb::create_pending_session`]
//!    mints a `(device_code, user_code)` pair; the row's `session_id` is NULL
//!    ("authorization pending").
//! 2. The user visits the IdP, authenticates, is redirected to
//!    `/auth/provider/{p}/device_code`, and submits their user_code; the server
//!    exchanges the OIDC code, creates a session, and calls
//!    [`AuthDb::bind_pending_session`] to set `session_id` ("fulfilled").
//! 3. The CLI polls `POST /auth/provider/{p}/token`;
//!    [`AuthDb::poll_pending_session`] returns `AuthorizationPending` until step
//!    2 completes, then `Fulfilled` (deleting the row — single use).

use chrono::{DateTime, Duration, Utc};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::db::{AuthDb, AuthPool};
use crate::error::{AuthError, Result};
use crate::principal::parse_dt_sqlite;

/// Bytes of entropy in a device_code. Python: `secrets.token_bytes(32)`.
const DEVICE_CODE_BYTES: usize = 32;
/// Bytes of entropy in a user_code. Python: `secrets.token_hex(4)` renders
/// 4 random bytes as 8 hex chars.
const USER_CODE_BYTES: usize = 4;

/// Secrets returned by [`AuthDb::create_pending_session`] to hand to the
/// client. `device_code` is the raw hex (only its SHA-256 hash is persisted);
/// `user_code` is the canonical (no-dash, uppercase) form.
#[derive(Debug, Clone)]
pub struct PendingSessionInit {
    pub device_code: String,
    pub user_code: String,
}

/// Outcome of [`AuthDb::poll_pending_session`].
#[derive(Debug, Clone)]
pub enum PendingSessionStatus {
    /// The user has not yet completed the browser-side OIDC login.
    AuthorizationPending,
    /// The login completed; carries the bound session's integer id. The
    /// pending-session row has been deleted (single use).
    Fulfilled(i64),
}

/// A pending-session row, as returned by a user_code lookup so the submit
/// route can bind the session it just created.
#[derive(Debug, Clone)]
pub struct PendingSessionRecord {
    pub hashed_device_code: String,
    pub user_code: String,
    pub session_id: Option<i64>,
    pub expiration_time: DateTime<Utc>,
}

impl AuthDb {
    /// Create a pending device-login. Returns the raw `device_code` (hex) and
    /// canonical `user_code` to hand to the client; only the SHA-256 hash of
    /// the device_code is persisted. Mirrors Python `create_pending_session`
    /// (authentication.py:757).
    ///
    /// Python loops up to 3× to dodge a `user_code` collision; we omit that —
    /// with 4 bytes of entropy (2^32) inside the 15-minute TTL window a
    /// collision is astronomically unlikely, and the retry would be defensive
    /// code against an effectively-impossible input.
    pub async fn create_pending_session(&self, ttl: Duration) -> Result<PendingSessionInit> {
        let raw = random_bytes(DEVICE_CODE_BYTES);
        let device_code = hex_encode(&raw);
        let hashed = sha256_hex(&raw);
        let user_code = hex_encode(&random_bytes(USER_CODE_BYTES)).to_uppercase();
        let expires_iso = (Utc::now() + ttl).to_rfc3339();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO pending_sessions (hashed_device_code, user_code, expiration_time)
                     VALUES (?, ?, ?)",
                )
                .bind(&hashed)
                .bind(&user_code)
                .bind(&expires_iso)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO pending_sessions (hashed_device_code, user_code, expiration_time)
                     VALUES ($1, $2, $3::timestamptz)",
                )
                .bind(&hashed)
                .bind(&user_code)
                .bind(&expires_iso)
                .execute(pool)
                .await?;
            }
        }
        Ok(PendingSessionInit {
            device_code,
            user_code,
        })
    }

    /// Look up a not-yet-expired pending session by user_code. The input is
    /// normalized (uppercase, dashes removed, trimmed) before the lookup, so a
    /// user typing the displayed `XXXX-XXXX` form in any case still matches.
    /// Mirrors Python `lookup_valid_pending_session_by_user_code`
    /// (authn_database/core.py:204) + the route's normalization
    /// (authentication.py:1045). Returns `NotFound` when absent or expired.
    pub async fn lookup_valid_pending_session_by_user_code(
        &self,
        user_code: &str,
    ) -> Result<PendingSessionRecord> {
        let user_code = normalize_user_code(user_code);
        let rec = match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT hashed_device_code, user_code, session_id, expiration_time
                       FROM pending_sessions WHERE user_code = ?",
                )
                .bind(&user_code)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound("pending_session".into()))?;
                pending_from_sqlite(&row)?
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT hashed_device_code, user_code, session_id, expiration_time
                       FROM pending_sessions WHERE user_code = $1",
                )
                .bind(&user_code)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound("pending_session".into()))?;
                pending_from_postgres(&row)?
            }
        };
        if Utc::now() > rec.expiration_time {
            return Err(AuthError::NotFound("pending_session".into()));
        }
        Ok(rec)
    }

    /// Bind a fulfilled OIDC login to its pending session by setting
    /// `session_id`. Called after the browser-side login creates a real
    /// session. Mirrors Python `pending_session.session_id = session.id`
    /// (authentication.py:1084). Returns `NotFound` when the row vanished
    /// (e.g. expired and cleaned between lookup and bind).
    pub async fn bind_pending_session(
        &self,
        hashed_device_code: &str,
        session_id: i64,
    ) -> Result<()> {
        let affected = match self.pool() {
            AuthPool::Sqlite(pool) => sqlx::query(
                "UPDATE pending_sessions SET session_id = ? WHERE hashed_device_code = ?",
            )
            .bind(session_id)
            .bind(hashed_device_code)
            .execute(pool)
            .await?
            .rows_affected(),
            AuthPool::Postgres(pool) => sqlx::query(
                "UPDATE pending_sessions SET session_id = $1 WHERE hashed_device_code = $2",
            )
            .bind(session_id)
            .bind(hashed_device_code)
            .execute(pool)
            .await?
            .rows_affected(),
        };
        if affected == 0 {
            return Err(AuthError::NotFound("pending_session".into()));
        }
        Ok(())
    }

    /// Poll a pending session by its raw (hex) device_code. Mirrors Python's
    /// `device_code_token_route` + `lookup_valid_pending_session_by_device_code`
    /// (authentication.py:1097, authn_database/core.py:176):
    /// - invalid hex → `Unauthorized` ("Invalid device code")
    /// - absent or expired → `NotFound`
    /// - present but unbound → `AuthorizationPending`
    /// - bound → `Fulfilled(session_id)`, deleting the row (single use).
    pub async fn poll_pending_session(
        &self,
        device_code_hex: &str,
    ) -> Result<PendingSessionStatus> {
        let raw = hex_decode(device_code_hex)
            .ok_or_else(|| AuthError::Unauthorized("Invalid device code".into()))?;
        let hashed = sha256_hex(&raw);
        let rec = self.lookup_pending_by_hash(&hashed).await?;
        if Utc::now() > rec.expiration_time {
            self.delete_pending_session(&hashed).await.ok();
            return Err(AuthError::NotFound("pending_session".into()));
        }
        match rec.session_id {
            None => Ok(PendingSessionStatus::AuthorizationPending),
            Some(sid) => {
                self.delete_pending_session(&hashed).await?;
                Ok(PendingSessionStatus::Fulfilled(sid))
            }
        }
    }

    async fn lookup_pending_by_hash(&self, hashed: &str) -> Result<PendingSessionRecord> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT hashed_device_code, user_code, session_id, expiration_time
                       FROM pending_sessions WHERE hashed_device_code = ?",
                )
                .bind(hashed)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound("pending_session".into()))?;
                pending_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT hashed_device_code, user_code, session_id, expiration_time
                       FROM pending_sessions WHERE hashed_device_code = $1",
                )
                .bind(hashed)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound("pending_session".into()))?;
                pending_from_postgres(&row)
            }
        }
    }

    async fn delete_pending_session(&self, hashed: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM pending_sessions WHERE hashed_device_code = ?")
                    .bind(hashed)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("DELETE FROM pending_sessions WHERE hashed_device_code = $1")
                    .bind(hashed)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
}

fn random_bytes(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut buf);
    buf
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_encode(&digest)
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

/// Decode an ASCII hex string to bytes. Returns `None` for odd length or any
/// non-hex character — the device_code is hex by construction, so malformed
/// input is a tampered/garbage code (Python: `bytes.fromhex` → 401).
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let nibble = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    for pair in bytes.chunks(2) {
        let hi = nibble(pair[0])?;
        let lo = nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

/// Normalize a user-entered code to its canonical stored form: uppercase,
/// dashes removed, surrounding whitespace trimmed. Mirrors Python's
/// `user_code.upper().replace("-", "").strip()` (authentication.py:1045).
fn normalize_user_code(input: &str) -> String {
    input.to_uppercase().replace('-', "").trim().to_string()
}

fn pending_from_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<PendingSessionRecord> {
    Ok(PendingSessionRecord {
        hashed_device_code: row.get("hashed_device_code"),
        user_code: row.get("user_code"),
        // SQLite's `try_get::<i64,_>` returns 0 for SQL NULL on integer
        // columns, silently masking an unbound session as `Some(0)`; pin the
        // Option type so a NULL stays `None` (see device_code.rs).
        session_id: row.try_get::<Option<i64>, _>("session_id").unwrap_or(None),
        expiration_time: parse_dt_sqlite(row.get::<String, _>("expiration_time"))?,
    })
}

fn pending_from_postgres(row: &sqlx::postgres::PgRow) -> Result<PendingSessionRecord> {
    Ok(PendingSessionRecord {
        hashed_device_code: row.get("hashed_device_code"),
        user_code: row.get("user_code"),
        session_id: row.try_get("session_id").ok().flatten(),
        expiration_time: row.get("expiration_time"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrips() {
        let raw = [0x00u8, 0x0f, 0xa3, 0xff, 0x10];
        assert_eq!(hex_encode(&raw), "000fa3ff10");
        assert_eq!(hex_decode("000fa3ff10"), Some(raw.to_vec()));
        assert_eq!(hex_decode("000FA3FF10"), Some(raw.to_vec()));
    }

    #[test]
    fn hex_decode_rejects_malformed() {
        assert_eq!(hex_decode("abc"), None); // odd length
        assert_eq!(hex_decode("zz"), None); // non-hex
        assert_eq!(hex_decode("0g"), None); // non-hex
    }

    #[test]
    fn normalize_user_code_matches_python() {
        assert_eq!(normalize_user_code("a1b2-c3d4"), "A1B2C3D4");
        assert_eq!(normalize_user_code("  ab-cd  "), "ABCD");
        assert_eq!(normalize_user_code("ABCDEF12"), "ABCDEF12");
    }

    #[test]
    fn sha256_hex_is_stable_and_64_chars() {
        // Known vector: sha256("") = e3b0c442...
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(sha256_hex(b"abc").len(), 64);
    }
}
