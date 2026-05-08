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
use sqlx::Row;

use crate::db::{AuthDb, AuthPool};
use crate::error::{AuthError, Result};

#[derive(Debug, Clone)]
pub struct DeviceCodeRecord {
    pub id: i64,
    pub device_code: String,
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
const USER_CODE_LEN: usize = 8;

impl AuthDb {
    pub async fn initiate_device_code(
        &self,
        ttl: Duration,
        interval: Duration,
    ) -> Result<DeviceCodeRecord> {
        let device_code = random_hex(DEVICE_CODE_BYTES);
        let user_code = random_user_code(USER_CODE_LEN);
        let expires = Utc::now() + ttl;
        // RFC 8628 default is 5s; allow 0 for tests / non-rate-limited paths.
        let interval_s = interval.num_seconds().max(0) as i32;
        let expires_iso = expires.to_rfc3339();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO device_codes (device_code, user_code, expires_at,
                                                interval_seconds)
                     VALUES (?, ?, ?, ?)
                     RETURNING id, device_code, user_code, principal_id, expires_at,
                               interval_seconds, last_polled_at",
                )
                .bind(&device_code)
                .bind(&user_code)
                .bind(&expires_iso)
                .bind(interval_s)
                .fetch_one(pool)
                .await?;
                device_from_sqlite(&row)
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO device_codes (device_code, user_code, expires_at,
                                                interval_seconds)
                     VALUES ($1, $2, $3::timestamptz, $4)
                     RETURNING id, device_code, user_code, principal_id, expires_at,
                               interval_seconds, last_polled_at",
                )
                .bind(&device_code)
                .bind(&user_code)
                .bind(&expires_iso)
                .bind(interval_s)
                .fetch_one(pool)
                .await?;
                device_from_postgres(&row)
            }
        }
    }

    /// Poll the status of a device code. Updates `last_polled_at` and
    /// enforces the polling `interval_seconds` (returns `SlowDown` if the
    /// caller polled faster than that).
    pub async fn poll_device_code(&self, device_code: &str) -> Result<DeviceStatus> {
        let row = self.lookup_device_code(device_code).await?;
        if Utc::now() > row.expires_at {
            self.delete_device_code(&row.device_code).await.ok();
            return Ok(DeviceStatus::Expired);
        }
        if let Some(last) = row.last_polled_at {
            let next_allowed = last + Duration::seconds(row.interval_seconds as i64);
            if Utc::now() < next_allowed {
                return Ok(DeviceStatus::SlowDown);
            }
        }
        self.touch_device_code(device_code).await.ok();
        match row.principal_id {
            Some(pid) => {
                self.delete_device_code(device_code).await.ok();
                Ok(DeviceStatus::Granted(pid))
            }
            None => Ok(DeviceStatus::Pending),
        }
    }

    /// Approve a pending device code by setting `principal_id`. The
    /// authenticated user calls this after typing the `user_code` into the
    /// verification UI.
    pub async fn approve_device_code(&self, user_code: &str, principal_id: i64) -> Result<()> {
        let affected: u64 = match self.pool() {
            AuthPool::Sqlite(pool) => sqlx::query(
                "UPDATE device_codes SET principal_id = ? WHERE user_code = ?",
            )
            .bind(principal_id)
            .bind(user_code)
            .execute(pool)
            .await?
            .rows_affected(),
            AuthPool::Postgres(pool) => sqlx::query(
                "UPDATE device_codes SET principal_id = $1 WHERE user_code = $2",
            )
            .bind(principal_id)
            .bind(user_code)
            .execute(pool)
            .await?
            .rows_affected(),
        };
        if affected == 0 {
            return Err(AuthError::NotFound(format!("user_code {user_code}")));
        }
        Ok(())
    }

    pub async fn lookup_device_code(&self, device_code: &str) -> Result<DeviceCodeRecord> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, device_code, user_code, principal_id, expires_at,
                            interval_seconds, last_polled_at
                       FROM device_codes WHERE device_code = ?",
                )
                .bind(device_code)
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
                .bind(device_code)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| AuthError::NotFound("device_code".into()))?;
                device_from_postgres(&row)
            }
        }
    }

    async fn touch_device_code(&self, device_code: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE device_codes SET last_polled_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       WHERE device_code = ?",
                )
                .bind(device_code)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE device_codes SET last_polled_at = now() WHERE device_code = $1",
                )
                .bind(device_code)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn delete_device_code(&self, device_code: &str) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM device_codes WHERE device_code = ?")
                    .bind(device_code)
                    .execute(pool)
                    .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query("DELETE FROM device_codes WHERE device_code = $1")
                    .bind(device_code)
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
    let mut out = String::with_capacity(buf.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in &buf {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// User-friendly code: uppercase alphanumeric, no `0/O/1/I/L` to avoid
/// transcription mistakes. Format: `XXXX-XXXX`.
fn random_user_code(half_len: usize) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    let mut rng = rand::thread_rng();
    let mut left = String::with_capacity(half_len);
    let mut right = String::with_capacity(half_len);
    for s in [&mut left, &mut right] {
        for _ in 0..half_len {
            let i = rng.gen_range(0..ALPHABET.len());
            s.push(ALPHABET[i] as char);
        }
    }
    format!("{left}-{right}")
}

fn device_from_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<DeviceCodeRecord> {
    use crate::principal::parse_dt_sqlite;
    Ok(DeviceCodeRecord {
        id: row.get("id"),
        device_code: row.get("device_code"),
        user_code: row.get("user_code"),
        // Be explicit about the Option type — sqlx-sqlite's `try_get::<i64, _>`
        // returns 0 for SQL NULL on integer columns, so type inference via
        // the field type silently fills `Some(0)` for an unset principal.
        principal_id: row.try_get::<Option<i64>, _>("principal_id").unwrap_or(None),
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
        device_code: row.get("device_code"),
        user_code: row.get("user_code"),
        principal_id: row.try_get("principal_id").ok(),
        expires_at: row.get("expires_at"),
        interval_seconds: row.get("interval_seconds"),
        last_polled_at: row.try_get("last_polled_at").ok(),
    })
}
