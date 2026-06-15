//! End-to-end auth DB exercises (SQLite).

use chrono::{Duration, Utc};

use tiled_auth::{
    ApiKeyCreate, AuthDb, Authenticator, DummyAuthenticator, Issuer, Scope, ScopeSet,
};

async fn fresh_db() -> (AuthDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let db = AuthDb::connect(&uri).await.unwrap();
    db.migrate().await.unwrap();
    (db, dir)
}

#[tokio::test]
async fn migrate_and_principal_lifecycle() {
    let (db, _dir) = fresh_db().await;
    assert_eq!(
        db.applied_migrations().await.unwrap(),
        vec![
            "0001_initial".to_string(),
            "0002_add_principal_role".to_string(),
            "0003_add_session_refresh_count".to_string(),
        ]
    );

    let (p, ident) = db.ensure_principal("dummy", "alice").await.unwrap();
    assert!(p.id > 0);
    assert_eq!(ident.provider, "dummy");
    assert_eq!(ident.sub, "alice");
    assert_eq!(p.role, "user", "new principals default to 'user' role");

    // Calling again returns the same principal.
    let (p2, _) = db.ensure_principal("dummy", "alice").await.unwrap();
    assert_eq!(p.id, p2.id);
}

#[tokio::test]
async fn api_key_create_verify_revoke() {
    let (db, _dir) = fresh_db().await;
    let (p, _) = db.ensure_principal("dummy", "alice").await.unwrap();

    let scopes = ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData]);
    let material = db
        .create_api_key(ApiKeyCreate {
            principal_id: p.id,
            note: Some("test".into()),
            scopes: scopes.clone(),
            expiration_time: None,
        })
        .await
        .unwrap();
    assert_eq!(material.record.first_eight.len(), 8);
    assert_eq!(material.record.scopes, scopes);
    assert_eq!(material.secret.len(), 64); // 32 bytes hex

    // Verify the plaintext.
    let verified = db.verify_api_key(&material.secret).await.unwrap();
    assert_eq!(verified.id, material.record.id);

    // Wrong key is rejected.
    let err = db.verify_api_key("000000000").await.unwrap_err();
    assert!(matches!(err, tiled_auth::AuthError::Unauthorized(_)));

    // Revoke scoped to owner succeeds.
    db.revoke_api_key(&material.record.first_eight, Some(p.id))
        .await
        .unwrap();
    let err = db.verify_api_key(&material.secret).await.unwrap_err();
    assert!(matches!(err, tiled_auth::AuthError::Unauthorized(_)));
}

#[tokio::test]
async fn revoke_api_key_cross_principal_rejected() {
    let (db, _dir) = fresh_db().await;
    let (alice, _) = db.ensure_principal("dummy", "alice").await.unwrap();
    let (bob, _) = db.ensure_principal("dummy", "bob").await.unwrap();

    let material = db
        .create_api_key(ApiKeyCreate {
            principal_id: alice.id,
            note: None,
            scopes: ScopeSet::default(),
            expiration_time: None,
        })
        .await
        .unwrap();

    // Bob cannot revoke Alice's key by first_eight.
    let err = db
        .revoke_api_key(&material.record.first_eight, Some(bob.id))
        .await
        .unwrap_err();
    assert!(matches!(err, tiled_auth::AuthError::NotFound(_)));

    // Admin bypass (None) can delete any key.
    db.revoke_api_key(&material.record.first_eight, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn session_revocation_blocks_jwt() {
    let (db, _dir) = fresh_db().await;
    let (p, _) = db.ensure_principal("dummy", "alice").await.unwrap();
    let scopes = ScopeSet::from_iter([Scope::ReadMetadata]);
    let session = db
        .create_session(p.id, scopes.clone(), Utc::now() + Duration::hours(1))
        .await
        .unwrap();

    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let token = issuer.issue_access(&p.uuid, &session.uuid, scopes).unwrap();
    let claims = issuer.verify_access(&token).unwrap();
    assert_eq!(claims.sub, p.uuid);

    // Server-side check: lookup_session() should report revoked = true
    // after revocation, and that's what the middleware will read.
    db.revoke_session(&session.uuid).await.unwrap();
    let s = db.lookup_session(&session.uuid).await.unwrap();
    assert!(s.revoked);
}

#[tokio::test]
async fn dummy_authenticator_password_check() {
    let mut auth = DummyAuthenticator::new("dummy");
    auth.add_user("alice", "open-sesame").unwrap();
    let s = auth.authenticate("alice", "open-sesame").await.unwrap();
    assert_eq!(s.sub, "alice");
    auth.authenticate("alice", "wrong").await.unwrap_err();
}

/// Python parity: `create_default_roles` in authn_database/core.py defines
/// the `user` and `admin` roles. Verify the Rust mapping matches.
#[test]
fn role_scopes_user_matches_python_defaults() {
    let user_scopes = ScopeSet::for_role("user");
    // Must include all Python 'user' role scopes.
    for s in [
        Scope::ReadMetadata,
        Scope::ReadData,
        Scope::CreateNode,
        Scope::WriteMetadata,
        Scope::WriteData,
        Scope::DeleteRevision,
        Scope::DeleteNode,
        Scope::CreateApiKeys,
        Scope::RevokeApiKeys,
    ] {
        assert!(user_scopes.contains(s), "user role must contain {s:?}");
    }
    // Must NOT include admin-only scopes.
    for s in [
        Scope::AdminApiKeys,
        Scope::ReadPrincipals,
        Scope::WritePrincipals,
        Scope::Admin,
    ] {
        assert!(!user_scopes.contains(s), "user role must NOT contain {s:?}");
    }
}

#[test]
fn role_scopes_admin_is_full() {
    let admin_scopes = ScopeSet::for_role("admin");
    assert_eq!(
        admin_scopes,
        ScopeSet::full(),
        "admin role must be full scopes"
    );
}

#[test]
fn role_scopes_unknown_is_empty() {
    let unknown = ScopeSet::for_role("superuser");
    assert!(unknown.is_empty(), "unknown role must yield empty scopes");
}

#[tokio::test]
async fn device_code_flow() {
    let (db, _dir) = fresh_db().await;
    let (p, _) = db.ensure_principal("dummy", "alice").await.unwrap();

    let dc = db
        .initiate_device_code(Duration::minutes(10), Duration::seconds(0))
        .await
        .unwrap();
    // Stored code is canonical (no dash, uppercase); display adds the dash.
    assert_eq!(dc.user_code.len(), 16);
    assert!(!dc.user_code.contains('-'));
    assert!(dc.principal_id.is_none());

    // First poll: pending.
    let st = db.poll_device_code(&dc.device_code).await.unwrap();
    assert!(matches!(st, tiled_auth::device_code::DeviceStatus::Pending));

    // Approve, then poll again: granted.
    db.approve_device_code(&dc.user_code, p.id).await.unwrap();
    let st = db.poll_device_code(&dc.device_code).await.unwrap();
    match st {
        tiled_auth::device_code::DeviceStatus::Granted(pid) => assert_eq!(pid, p.id),
        _ => panic!("expected Granted"),
    }

    // After grant, the row is consumed; next poll fails to find it.
    let err = db.poll_device_code(&dc.device_code).await.unwrap_err();
    assert!(matches!(err, tiled_auth::AuthError::NotFound(_)));
}

/// Finding 5: a user who types the *displayed* code (dashed) in lowercase,
/// with surrounding whitespace, must still approve — the stored code is
/// canonical and the lookup normalizes the input (Python parity).
#[tokio::test]
async fn device_code_approve_normalizes_typed_input() {
    let (db, _dir) = fresh_db().await;
    let (p, _) = db.ensure_principal("dummy", "alice").await.unwrap();

    let dc = db
        .initiate_device_code(Duration::minutes(10), Duration::seconds(0))
        .await
        .unwrap();

    // What the user sees (dashed), as they might mistype it.
    let displayed = tiled_auth::device_code::format_user_code(&dc.user_code);
    let typed = format!("  {}  ", displayed.to_lowercase());

    db.approve_device_code(&typed, p.id).await.unwrap();
    let st = db.poll_device_code(&dc.device_code).await.unwrap();
    assert!(
        matches!(st, tiled_auth::device_code::DeviceStatus::Granted(pid) if pid == p.id),
        "normalized approval must grant the device code"
    );
}

#[tokio::test]
async fn device_code_double_approve_rejected() {
    let (db, _dir) = fresh_db().await;
    let (p1, _) = db.ensure_principal("dummy", "alice").await.unwrap();
    let (p2, _) = db.ensure_principal("dummy", "bob").await.unwrap();

    let dc = db
        .initiate_device_code(Duration::minutes(10), Duration::seconds(0))
        .await
        .unwrap();

    // First approval succeeds (first-writer-wins).
    db.approve_device_code(&dc.user_code, p1.id).await.unwrap();

    // Second approval must fail — principal_id is no longer NULL.
    let err = db
        .approve_device_code(&dc.user_code, p2.id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, tiled_auth::AuthError::Conflict(_)),
        "expected Conflict, got {err:?}"
    );
}

#[tokio::test]
async fn device_code_expired_approve_rejected() {
    let (db, _dir) = fresh_db().await;
    let (p, _) = db.ensure_principal("dummy", "alice").await.unwrap();

    // Negative TTL → expires_at is already in the past.
    let dc = db
        .initiate_device_code(Duration::seconds(-10), Duration::seconds(0))
        .await
        .unwrap();

    let err = db
        .approve_device_code(&dc.user_code, p.id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, tiled_auth::AuthError::Conflict(_)),
        "expected Conflict for expired code, got {err:?}"
    );
}
