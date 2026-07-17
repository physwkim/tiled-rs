//! ITEM 5 (Wave-22 backlog): the native RFC-8628 device-code flow must hash
//! its polling bearer secret at rest.
//!
//! `poll_device_code(device_code)` resolves to `Granted(principal_id)` once the
//! code is approved, and the token route mints access+refresh tokens from that
//! — so `device_code` is a redeemable bearer secret. The sibling OIDC-brokered
//! flow (`pending_session.rs`) and upstream (`authentication.py:758`) both
//! persist only the SHA-256 hash of the analogous secret; the native flow stored
//! it in PLAINTEXT (`INSERT INTO device_codes (device_code, ...)` raw). A
//! read-only DB compromise therefore exposed live, replayable device codes.
//!
//! Fix: store `sha256_hex(device_code)` in the `device_codes.device_code`
//! column, return the raw code to the client once at creation, and hash the
//! incoming code before every lookup. `user_code` stays plaintext — it is not a
//! bearer secret (approval is gated by the approver's own authenticated session).

use chrono::Duration;
use sha2::{Digest, Sha256};
use sqlx::Row;

use tiled_rs::auth::AuthDb;

/// SHA-256 → lowercase hex, matching `device_code.rs`/`pending_session.rs`.
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

/// After `initiate_device_code`, the persisted secret must be the SHA-256 hash
/// of the raw `device_code` (never the raw code), and polling with the RAW code
/// must still resolve the row: pending → (approve) → granted.
#[tokio::test]
async fn device_code_is_hashed_at_rest_and_still_polls() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let db = AuthDb::connect(&uri).await.unwrap();
    db.migrate().await.unwrap();
    let (p, _) = db.ensure_principal("dummy", "alice").await.unwrap();

    let dc = db
        .initiate_device_code(Duration::minutes(10), Duration::seconds(0))
        .await
        .unwrap();
    let raw = dc.device_code.clone();

    // Read the stored secret directly from the column. Only its SHA-256 hash may
    // be at rest — never the raw code the client polls with.
    let pool = sqlx::SqlitePool::connect(&uri).await.unwrap();
    let stored: String = sqlx::query("SELECT device_code FROM device_codes")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("device_code");

    assert_ne!(
        stored, raw,
        "raw device_code must NOT be stored in plaintext at rest"
    );
    assert_eq!(
        stored,
        sha256_hex(raw.as_bytes()),
        "stored value must be sha256_hex(device_code)"
    );

    // The raw code still polls: pending → approve → granted (single use).
    assert!(
        matches!(
            db.poll_device_code(&raw).await.unwrap(),
            tiled_rs::auth::device_code::DeviceStatus::Pending
        ),
        "raw code must resolve to Pending before approval"
    );
    db.approve_device_code(&dc.user_code, p.id).await.unwrap();
    match db.poll_device_code(&raw).await.unwrap() {
        tiled_rs::auth::device_code::DeviceStatus::Granted(pid) => assert_eq!(pid, p.id),
        other => panic!("expected Granted after approval, got {other:?}"),
    }
}
