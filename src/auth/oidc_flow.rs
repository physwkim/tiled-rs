//! OIDC authorization-code (PKCE browser) flow — DB-backed pending-state store.
//!
//! Server-brokered PKCE state kept between the `/authorize` redirect (created)
//! and the `/callback` completion (consumed, single use). This replaces the
//! former in-memory `PendingAuthStore`: the in-memory map did not survive a
//! restart and broke under a load-balanced / multi-process deployment where the
//! `/callback` may land on a different process than the `/authorize` that
//! created the state (G6).
//!
//! There is **no Python-tiled equivalent**. Python's browser code flow
//! (`authorize_redirect_route` + `OIDCAuthenticator.authenticate`,
//! authentication.py:954 / authenticators.py:222) is a stateless
//! confidential-client exchange — no PKCE, no nonce, no server-side state.
//! tiled-rs brokers PKCE itself (more secure: a public client never needs a
//! shared secret) and therefore must persist the verifier/nonce server-side.
//!
//! Single owner: the [`AuthDb`] row is the *only* place this state lives. The
//! `state` parameter is the client-presented lookup key, so — mirroring the
//! device-code store ([`crate::auth::pending_session`]) — only its SHA-256 hash is
//! persisted; the raw value never touches the DB. Consumption is atomic via
//! `DELETE … RETURNING`, so two racing callbacks cannot both succeed.

use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::auth::db::{AuthDb, AuthPool};
use crate::auth::error::Result;
use crate::auth::principal::parse_dt_sqlite;

/// Server-side state recovered on the `/callback`: everything
/// [`crate::auth::external_oidc::ExternalOidcValidator::exchange_code_flow`] needs to
/// finish the exchange. The expiry is enforced inside
/// [`AuthDb::take_oidc_flow_state`] and not surfaced here.
#[derive(Debug, Clone)]
pub struct OidcFlowState {
    /// Provider that initiated the flow (selects token/JWKS config).
    pub provider: String,
    /// PKCE code verifier (RFC 7636 §4.1).
    pub code_verifier: String,
    /// OIDC nonce expected in the returned `id_token` (OIDC Core §3.1.3.7 #11).
    pub nonce: String,
}

impl AuthDb {
    /// Persist the server-side PKCE flow state created by `/authorize`, keyed by
    /// the SHA-256 hash of the random `state` parameter. `ttl` bounds how long
    /// the user has to complete the IdP login before the row is treated as
    /// expired (the former in-memory store used 10 minutes).
    pub async fn create_oidc_flow_state(
        &self,
        state: &str,
        provider: &str,
        code_verifier: &str,
        nonce: &str,
        ttl: Duration,
    ) -> Result<()> {
        let hashed = sha256_hex(state.as_bytes());
        let expires_iso = (Utc::now() + ttl).to_rfc3339();
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO oidc_flow_states
                       (hashed_state, provider, code_verifier, nonce, expiration_time)
                     VALUES (?, ?, ?, ?, ?)",
                )
                .bind(&hashed)
                .bind(provider)
                .bind(code_verifier)
                .bind(nonce)
                .bind(&expires_iso)
                .execute(pool)
                .await?;
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO oidc_flow_states
                       (hashed_state, provider, code_verifier, nonce, expiration_time)
                     VALUES ($1, $2, $3, $4, $5::timestamptz)",
                )
                .bind(&hashed)
                .bind(provider)
                .bind(code_verifier)
                .bind(nonce)
                .bind(&expires_iso)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    /// Consume the pending flow state for `state` (single use). The row is
    /// DELETED atomically via `DELETE … RETURNING` whether or not it had
    /// expired, so a replayed callback cannot succeed on retry and two racing
    /// callbacks cannot both observe the row. Returns `None` when the `state` is
    /// unknown or already expired.
    pub async fn take_oidc_flow_state(&self, state: &str) -> Result<Option<OidcFlowState>> {
        let hashed = sha256_hex(state.as_bytes());
        let recovered = match self.pool() {
            AuthPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "DELETE FROM oidc_flow_states WHERE hashed_state = ?
                     RETURNING provider, code_verifier, nonce, expiration_time",
                )
                .bind(&hashed)
                .fetch_optional(pool)
                .await?;
                match row {
                    Some(r) => Some((
                        OidcFlowState {
                            provider: r.get("provider"),
                            code_verifier: r.get("code_verifier"),
                            nonce: r.get("nonce"),
                        },
                        parse_dt_sqlite(r.get::<String, _>("expiration_time"))?,
                    )),
                    None => None,
                }
            }
            AuthPool::Postgres(pool) => {
                let row = sqlx::query(
                    "DELETE FROM oidc_flow_states WHERE hashed_state = $1
                     RETURNING provider, code_verifier, nonce, expiration_time",
                )
                .bind(&hashed)
                .fetch_optional(pool)
                .await?;
                row.map(|r| {
                    (
                        OidcFlowState {
                            provider: r.get("provider"),
                            code_verifier: r.get("code_verifier"),
                            nonce: r.get("nonce"),
                        },
                        r.get::<DateTime<Utc>, _>("expiration_time"),
                    )
                })
            }
        };
        match recovered {
            // Present and still valid → hand it back. The row is already gone.
            Some((flow, expires_at)) if Utc::now() <= expires_at => Ok(Some(flow)),
            // Unknown, or found-but-expired: the DELETE above already removed any
            // matching row, so a replay finds nothing.
            _ => Ok(None),
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in digest.iter() {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_is_stable_and_64_chars() {
        // Known vector: sha256("") = e3b0c442…
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(sha256_hex(b"some-state-token").len(), 64);
    }
}
