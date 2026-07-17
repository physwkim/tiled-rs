//! OAuth2 device-code grant — RFC 8628 state machine.
//!
//! Flow:
//! 1. Client `POST /auth/device/initiate` → server returns
//!    `(device_code, user_code, verification_uri, interval, expires_in)`.
//! 2. Out-of-band, the user opens `verification_uri`, types `user_code`,
//!    and approves with their normal session.
//! 3. Client polls `POST /auth/device/token` with `device_code` until the
//!    server says `pending` → `granted` (returns access+refresh tokens) or
//!    `expired` / `denied`.

use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::auth::db::{AuthDb, AuthPool};
use crate::auth::error::{AuthError, Result};

/// Secrets returned by [`AuthDb::initiate_device_code`] to hand to the client.
/// `device_code` is the RAW hex polling secret — only its SHA-256 hash is
/// persisted (see [`DeviceCodeRecord::hashed_device_code`]), so a DB leak cannot
/// replay it. `user_code` is the canonical (no-dash, uppercase) form. Mirrors
/// the raw/hashed split in [`crate::auth::pending_session::PendingSessionInit`].
#[derive(Debug, Clone)]
pub struct DeviceCodeInit {
    pub device_code: String,
    pub user_code: String,
    pub interval_seconds: i32,
}

/// A device-code row as read back from the DB. The stored secret is the
/// SHA-256 hash of the client's `device_code`, never the raw code.
#[derive(Debug, Clone)]
pub struct DeviceCodeRecord {
    pub id: i64,
    /// SHA-256 hash (hex) of the polling `device_code`. Held in the
    /// `device_codes.device_code` column; the raw code is never persisted.
    pub hashed_device_code: String,
    pub user_code: String,
    pub principal_id: Option<i64>,
    pub expires_at: DateTime<Utc>,
    pub interval_seconds: i32,
    pub last_polled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub enum DeviceStatus {
    Pending,
    Granted(i64), // principal_id
    Expired,
    SlowDown,
}

const DEVICE_CODE_BYTES: usize = 32;
/// Length of the canonical (no-dash) `user_code`. Displayed as
/// `XXXXXXXX-XXXXXXXX` via [`format_user_code`].
const USER_CODE_LEN: usize = 16;

impl AuthDb {
    pub async fn initiate_device_code(
        &self,
        ttl: Duration,
        interval: Duration,
    ) -> Result<DeviceCodeInit> {
        let device_code = random_hex(DEVICE_CODE_BYTES);
        // Only the hash is persisted; the raw `device_code` is returned to the
        // client once below and never stored (mirrors pending_session.rs and
        // upstream authentication.py:758).
        let hashed = sha256_hex(device_code.as_bytes());
        let user_code = random_user_code(USER_CODE_LEN);
        let expires = Utc::now() + ttl;
        // RFC 8628 default is 5s; allow 0 for tests / non-rate-limited paths.
        let interval_s = interval.num_seconds().max(0) as i32;
        let expires_iso = expires.to_rfc3339();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO device_codes (device_code, user_code, expires_at,
                                                interval_seconds)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(&hashed)
                .bind(&user_code)
                .bind(&expires_iso)
                .bind(interval_s)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO device_codes (device_code, user_code, expires_at,
                                                interval_seconds)
                     VALUES ($1, $2, $3::timestamptz, $4)",
                )
                .bind(&hashed)
                .bind(&user_code)
                .bind(&expires_iso)
                .bind(interval_s)
                .execute(pool)
                .await?;
            }
        }
        Ok(DeviceCodeInit {
            device_code,
            user_code,
            interval_seconds: interval_s,
        })
    }

    /// Poll the status of a device code. Updates `last_polled_at` and
    /// enforces the polling `interval_seconds` (returns `SlowDown` if the
    /// caller polled faster than that).
    pub async fn poll_device_code(&self, device_code: &str) -> Result<DeviceStatus> {
        // Hash the incoming raw code once; every DB operation below keys on the
        // hash, matching the at-rest form (never the plaintext).
        let hashed = sha256_hex(device_code.as_bytes());
        let row = self.lookup_device_code_by_hash(&hashed).await?;
        if Utc::now() > row.expires_at {
            self.delete_device_code(&hashed).await.ok();
            return Ok(DeviceStatus::Expired);
        }
        if let Some(last) = row.last_polled_at {
            let next_allowed = last + Duration::seconds(row.interval_seconds as i64);
            if Utc::now() < next_allowed {
                return Ok(DeviceStatus::SlowDown);
            }
        }
        self.touch_device_code(&hashed).await.ok();
        match row.principal_id {
            Some(pid) => {
                self.delete_device_code(&hashed).await.ok();
                Ok(DeviceStatus::Granted(pid))
            }
            None => Ok(DeviceStatus::Pending),
        }
    }

    /// Approve a pending device code by setting `principal_id`. The
    /// authenticated user calls this after typing the `user_code` into the
    /// verification UI.
    ///
    /// First-writer-wins: the WHERE clause requires `principal_id IS NULL`
    /// (not yet approved) and `expires_at > now` (not expired), so a
    /// concurrent or replayed approval returns `Conflict` — RFC 8628: a
    /// granted code is immutable.
    pub async fn approve_device_code(&self, user_code: &str, principal_id: i64) -> Result<()> {
        // Normalize the user-entered code to the canonical stored form so a
        // user who types the displayed (dashed) code in lowercase, or omits
        // the dash, still matches. Python parity: authentication.py:1045
        // (`user_code.upper().replace("-", "").strip()`).
        let user_code = normalize_user_code(user_code);
        let user_code = user_code.as_str();
        let affected: u64 = match self.pool() {
            AuthPool::Sqlite(pool) => {
                let now_iso = Utc::now().to_rfc3339();
                sqlx::query(
                    "UPDATE device_codes SET principal_id = ?
                       WHERE user_code = ? AND principal_id IS NULL AND expires_at > ?",
                )
                .bind(principal_id)
                .bind(user_code)
                .bind(&now_iso)
                .execute(pool)
                .await?
                .rows_affected()
            }
            AuthPool::Postgres(pool) => sqlx::query(
                "UPDATE device_codes SET principal_id = $1
                       WHERE user_code = $2 AND principal_id IS NULL AND expires_at > now()",
            )
            .bind(principal_id)
            .bind(user_code)
            .execute(pool)
            .await?
            .rows_affected(),
        };
        if affected == 0 {
            return Err(AuthError::Conflict(format!(
                "device code not found, already approved, or expired: {user_code}"
            )));
        }
        Ok(())
    }

    /// Look up a device-code row by the SHA-256 hash of the client's code. The
    /// caller ([`Self::poll_device_code`]) hashes the incoming raw code first,
    /// so the plaintext never reaches the query.
    async fn lookup_device_code_by_hash(&self, hashed: &str) -> Result<DeviceCodeRecord> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, device_code, user_code, principal_id, expires_at,
                            interval_seconds, last_polled_at
                       FROM device_codes WHERE device_code = ?",
                )
                .bind(hashed)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound("device_code".into()))?;
                device_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, device_code, user_code, principal_id, expires_at,
                            interval_seconds, last_polled_at
                       FROM device_codes WHERE device_code = $1",
                )
                .bind(hashed)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound("device_code".into()))?;
                device_from_postgres(&row)
            }
        }
    }

    async fn touch_device_code(&self, hashed: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE device_codes SET last_polled_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       WHERE device_code = ?",
                )
                .bind(hashed)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE device_codes SET last_polled_at = now() WHERE device_code = $1",
                )
                .bind(hashed)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn delete_device_code(&self, hashed: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM device_codes WHERE device_code = ?")
                    .bind(hashed)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("DELETE FROM device_codes WHERE device_code = $1")
                    .bind(hashed)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
}

fn random_hex(bytes: usize) -> String {
    use rand::RngCore;
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    hex_encode(&buf)
}

/// SHA-256 of `bytes`, lowercase hex. Used to hash the `device_code` at rest so
/// a DB leak cannot replay it (mirrors `pending_session::sha256_hex`).
fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode(&Sha256::digest(bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// User-friendly code: uppercase alphanumeric, no `0/O/1/I/L` to avoid
/// transcription mistakes. Generated in canonical form — uppercase, no
/// separators — so the stored value matches a normalized lookup. The
/// dashed `XXXXXXXX-XXXXXXXX` form is produced only for display by
/// [`format_user_code`].
fn random_user_code(len: usize) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    let mut rng = rand::thread_rng();
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let i = rng.gen_range(0..ALPHABET.len());
        s.push(ALPHABET[i] as char);
    }
    s
}

/// Format a canonical `user_code` for display by inserting a single dash at
/// the midpoint (e.g. `ABCDEFGH-JKMNPQRS`). Purely cosmetic — the dash and
/// case are stripped again at the approval boundary by
/// `normalize_user_code`. The code is ASCII, so midpoint byte-slicing is
/// safe.
pub fn format_user_code(code: &str) -> String {
    let mid = code.len() / 2;
    if mid == 0 || mid >= code.len() {
        return code.to_string();
    }
    format!("{}-{}", &code[..mid], &code[mid..])
}

/// Normalize a user-entered code to its canonical stored form: uppercase,
/// dashes removed, surrounding whitespace trimmed. Mirrors Python's
/// `user_code.upper().replace("-", "").strip()` (authentication.py:1045).
fn normalize_user_code(input: &str) -> String {
    input.to_uppercase().replace('-', "").trim().to_string()
}

fn device_from_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<DeviceCodeRecord> {
    use crate::auth::principal::parse_dt_sqlite;
    Ok(DeviceCodeRecord {
        id: row.get("id"),
        // The `device_code` column stores the SHA-256 hash (hash-at-rest); the
        // raw code is never persisted. See `initiate_device_code`.
        hashed_device_code: row.get("device_code"),
        user_code: row.get("user_code"),
        // Be explicit about the Option type — sqlx-sqlite's `try_get::<i64, _>`
        // returns 0 for SQL NULL on integer columns, so type inference via
        // the field type silently fills `Some(0)` for an unset principal.
        principal_id: row
            .try_get::<Option<i64>, _>("principal_id")
            .unwrap_or(None),
        expires_at: parse_dt_sqlite(row.get::<String, _>("expires_at"))?,
        interval_seconds: row.get("interval_seconds"),
        last_polled_at: row
            .try_get::<String, _>("last_polled_at")
            .ok()
            .and_then(|s| parse_dt_sqlite(s).ok()),
    })
}

fn device_from_postgres(row: &sqlx::postgres::PgRow) -> Result<DeviceCodeRecord> {
    Ok(DeviceCodeRecord {
        id: row.get("id"),
        // See `device_from_sqlite`: the column holds the SHA-256 hash.
        hashed_device_code: row.get("device_code"),
        user_code: row.get("user_code"),
        principal_id: row.try_get("principal_id").ok(),
        expires_at: row.get("expires_at"),
        interval_seconds: row.get("interval_seconds"),
        last_polled_at: row.try_get("last_polled_at").ok(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_user_code_matches_python() {
        // upper().replace("-", "").strip()
        assert_eq!(normalize_user_code("abcd-efgh"), "ABCDEFGH");
        assert_eq!(normalize_user_code("  ab-cd  "), "ABCD");
        assert_eq!(normalize_user_code("ABCDEFGH"), "ABCDEFGH");
        assert_eq!(normalize_user_code("ab-cd-ef-gh"), "ABCDEFGH");
    }

    #[test]
    fn format_user_code_inserts_midpoint_dash() {
        assert_eq!(format_user_code("ABCDEFGH"), "ABCD-EFGH");
        assert_eq!(format_user_code("ABCDEFGHJKMNPQRS"), "ABCDEFGH-JKMNPQRS");
        // Degenerate lengths are returned unchanged.
        assert_eq!(format_user_code(""), "");
        assert_eq!(format_user_code("A"), "A");
    }

    #[test]
    fn generated_code_is_canonical_and_display_roundtrips() {
        let code = random_user_code(USER_CODE_LEN);
        assert_eq!(code.len(), USER_CODE_LEN, "stored code is canonical length");
        assert!(!code.contains('-'), "stored code has no separators");
        assert_eq!(code, code.to_uppercase(), "stored code is uppercase");
        // The displayed (dashed) form normalizes back to the stored value,
        // so a user typing what they see always matches the lookup.
        assert_eq!(normalize_user_code(&format_user_code(&code)), code);
    }
}
