//! End-to-end auth DB exercises (SQLite).

use chrono::{Duration, Utc};

use tiled_rs::auth::{
    ApiKeyCreate, AuthDb, Authenticator, DummyAuthenticator, Issuer, Scope, ScopeSet,
};

async fn fresh_db() -> (AuthDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let db = AuthDb::connect(&uri).await.unwrap();
    db.migrate().await.unwrap();
    (db, dir)
}

/// auth H2 (#1360 family): a principal exposes its full identity set, and the
/// serialized view mirrors Python `schemas.Principal`/`schemas.Identity`
/// (`schemas.py:315,403`) — each identity's public `id` is the upstream
/// subject (`sub`), and the internal row id / `principal_id` FK never leak.
#[tokio::test]
async fn principal_detail_lists_all_identities() {
    let (db, _dir) = fresh_db().await;

    // One principal linked to two providers (e.g. password + OIDC).
    let (principal, _) = db.ensure_principal("dummy", "alice").await.unwrap();
    db.create_identity(principal.id, "entra", "alice@contoso")
        .await
        .unwrap();

    // list_identities (the selectinload equivalent) returns both, ordered.
    let identities = db.list_identities(principal.id).await.unwrap();
    assert_eq!(
        identities.len(),
        2,
        "both linked identities must be returned"
    );
    assert_eq!(identities[0].provider, "dummy"); // ordered by (provider, sub)
    assert_eq!(identities[1].provider, "entra");

    // get_principal_detail loads the principal + identities by uuid.
    let detail = db
        .get_principal_detail(&principal.uuid)
        .await
        .unwrap()
        .expect("principal exists");
    assert_eq!(detail.uuid, principal.uuid);
    assert_eq!(detail.identities.len(), 2);
    // Public `id` is the subject (sub), not the internal row PK.
    let entra = detail
        .identities
        .iter()
        .find(|i| i.provider == "entra")
        .unwrap();
    assert_eq!(entra.id, "alice@contoso");

    // Serialized shape: only id/provider/latest_login — no internal row id,
    // no principal_id, no `sub` key.
    let json = serde_json::to_value(&detail).unwrap();
    let id0 = json["identities"][0].as_object().unwrap();
    let mut keys: Vec<&str> = id0.keys().map(|s| s.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["id", "latest_login", "provider"]);
    assert!(
        !id0.contains_key("principal_id"),
        "internal principal_id FK must not leak"
    );
    assert!(!id0.contains_key("sub"), "raw `sub` key must not leak");
    assert!(
        json.get("id").is_none(),
        "internal principal row id must not leak at the top level"
    );

    // Unknown uuid → None.
    assert!(
        db.get_principal_detail("no-such-uuid")
            .await
            .unwrap()
            .is_none()
    );
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
            "0004_add_access_tags".to_string(),
            "0005_tag_registry".to_string(),
            "0006_add_session_state".to_string(),
            "0007_add_pending_sessions".to_string(),
            "0008_add_oidc_flow_states".to_string(),
            "0009_hash_device_code".to_string(),
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
            access_tags: None,
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
    assert!(matches!(err, tiled_rs::auth::AuthError::Unauthorized(_)));

    // Revoke scoped to owner succeeds.
    db.revoke_api_key(&material.record.first_eight, Some(p.id))
        .await
        .unwrap();
    let err = db.verify_api_key(&material.secret).await.unwrap_err();
    assert!(matches!(err, tiled_rs::auth::AuthError::Unauthorized(_)));
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
            access_tags: None,
        })
        .await
        .unwrap();

    // Bob cannot revoke Alice's key by first_eight.
    let err = db
        .revoke_api_key(&material.record.first_eight, Some(bob.id))
        .await
        .unwrap_err();
    assert!(matches!(err, tiled_rs::auth::AuthError::NotFound(_)));

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
        .create_session(
            p.id,
            scopes.clone(),
            Utc::now() + Duration::hours(1),
            serde_json::json!({}),
        )
        .await
        .unwrap();

    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let token = issuer
        .issue_access(&p.uuid, &session.uuid, scopes, session.state.clone())
        .unwrap();
    let claims = issuer.verify_access(&token).unwrap();
    assert_eq!(claims.sub, p.uuid);

    // Server-side check: lookup_session() should report revoked = true
    // after revocation, and that's what the middleware will read.
    db.revoke_session(&session.uuid).await.unwrap();
    let s = db.lookup_session(&session.uuid).await.unwrap();
    assert!(s.revoked);
}

// G3 OBO: the session `state` JSON persists through create_session →
// lookup_session unchanged (the upstream Entra tokens a downstream OBO
// exchange reads).
#[tokio::test]
async fn session_state_roundtrips_through_db() {
    let (db, _dir) = fresh_db().await;
    let (p, _) = db.ensure_principal("entra", "opaque-oid").await.unwrap();
    let scopes = ScopeSet::from_iter([Scope::ReadMetadata]);
    let state = serde_json::json!({
        "entra_access_token": "upstream-at",
        "entra_refresh_token": "upstream-rt",
    });
    let session = db
        .create_session(p.id, scopes, Utc::now() + Duration::hours(1), state.clone())
        .await
        .unwrap();
    // The returned record carries the state we inserted.
    assert_eq!(session.state, state);
    // And it persists: a fresh lookup reads the same JSON back.
    let looked_up = db.lookup_session(&session.uuid).await.unwrap();
    assert_eq!(looked_up.state, state);
    assert_eq!(
        looked_up
            .state
            .pointer("/entra_refresh_token")
            .and_then(|v| v.as_str()),
        Some("upstream-rt")
    );
}

// A non-OIDC session stores `{}` (the column default), not null.
#[tokio::test]
async fn session_state_defaults_to_empty_object() {
    let (db, _dir) = fresh_db().await;
    let (p, _) = db.ensure_principal("dummy", "bob").await.unwrap();
    let session = db
        .create_session(
            p.id,
            ScopeSet::default(),
            Utc::now() + Duration::hours(1),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let looked_up = db.lookup_session(&session.uuid).await.unwrap();
    assert_eq!(looked_up.state, serde_json::json!({}));
}

#[tokio::test]
async fn dummy_authenticator_password_check() {
    let mut auth = DummyAuthenticator::new("dummy");
    auth.add_user("alice", "open-sesame").unwrap();
    let s = auth.authenticate("alice", "open-sesame").await.unwrap();
    assert_eq!(s.sub, "alice");
    auth.authenticate("alice", "wrong").await.unwrap_err();
}

// ---------------------------------------------------------------------------
// G4 — IdP-brokered device flow: pending_sessions store
// ---------------------------------------------------------------------------

use tiled_rs::auth::PendingSessionStatus;

// Full lifecycle: create → poll(pending) → lookup-by-user_code → bind →
// poll(fulfilled, single use). The device_code returned to the client polls
// successfully even though only its SHA-256 hash is persisted.
#[tokio::test]
async fn pending_session_full_lifecycle() {
    let (db, _dir) = fresh_db().await;

    let init = db
        .create_pending_session(Duration::minutes(15))
        .await
        .unwrap();
    assert_eq!(init.device_code.len(), 64, "32 bytes → 64 hex chars");
    assert_eq!(init.user_code.len(), 8, "4 bytes → 8 hex chars");
    assert_eq!(init.user_code, init.user_code.to_uppercase());

    // Before binding, the CLI poll sees authorization_pending.
    assert!(matches!(
        db.poll_pending_session(&init.device_code).await.unwrap(),
        PendingSessionStatus::AuthorizationPending
    ));

    // The submit route looks up the row by the (un-normalized) user_code.
    let rec = db
        .lookup_valid_pending_session_by_user_code(&init.user_code)
        .await
        .unwrap();
    assert!(rec.session_id.is_none(), "unbound until login completes");
    assert!(!rec.hashed_device_code.is_empty());

    // Bind a real session, then the poll yields it exactly once.
    let (p, _) = db.ensure_principal("entra", "oid-1").await.unwrap();
    let session = db
        .create_session(
            p.id,
            ScopeSet::from_iter([Scope::ReadMetadata]),
            Utc::now() + Duration::hours(1),
            serde_json::json!({"entra_access_token": "at"}),
        )
        .await
        .unwrap();
    db.bind_pending_session(&rec.hashed_device_code, session.id)
        .await
        .unwrap();

    match db.poll_pending_session(&init.device_code).await.unwrap() {
        PendingSessionStatus::Fulfilled(sid) => assert_eq!(sid, session.id),
        other => panic!("expected Fulfilled, got {other:?}"),
    }

    // Single use: the row is gone after a fulfilled poll.
    assert!(matches!(
        db.poll_pending_session(&init.device_code).await,
        Err(tiled_rs::auth::AuthError::NotFound(_))
    ));
}

// The user_code lookup normalizes input: the displayed dashed `XXXX-XXXX`
// form, in lowercase, still matches the canonical stored value.
#[tokio::test]
async fn pending_session_user_code_lookup_is_normalized() {
    let (db, _dir) = fresh_db().await;
    let init = db
        .create_pending_session(Duration::minutes(15))
        .await
        .unwrap();
    let dashed_lower = format!(
        "{}-{}",
        init.user_code[..4].to_lowercase(),
        init.user_code[4..].to_lowercase()
    );
    let rec = db
        .lookup_valid_pending_session_by_user_code(&dashed_lower)
        .await
        .unwrap();
    assert_eq!(rec.user_code, init.user_code);

    // A code that doesn't exist → NotFound.
    assert!(matches!(
        db.lookup_valid_pending_session_by_user_code("ZZZZZZZZ")
            .await,
        Err(tiled_rs::auth::AuthError::NotFound(_))
    ));
}

// An expired pending session is invisible to both lookups (boundary:
// expiration_time < now).
#[tokio::test]
async fn pending_session_expired_is_not_found() {
    let (db, _dir) = fresh_db().await;
    // ttl in the past → already expired.
    let init = db
        .create_pending_session(Duration::minutes(-1))
        .await
        .unwrap();
    assert!(matches!(
        db.lookup_valid_pending_session_by_user_code(&init.user_code)
            .await,
        Err(tiled_rs::auth::AuthError::NotFound(_))
    ));
    assert!(matches!(
        db.poll_pending_session(&init.device_code).await,
        Err(tiled_rs::auth::AuthError::NotFound(_))
    ));
}

// A malformed (non-hex) device_code is rejected distinctly (Python: 401
// "Invalid device code"), separate from the NotFound used for unknown codes.
#[tokio::test]
async fn pending_session_invalid_hex_device_code_is_unauthorized() {
    let (db, _dir) = fresh_db().await;
    for bad in ["not-hex!!", "abc", "0g"] {
        assert!(
            matches!(
                db.poll_pending_session(bad).await,
                Err(tiled_rs::auth::AuthError::Unauthorized(_))
            ),
            "{bad:?} must be Unauthorized, not NotFound"
        );
    }
    // Valid hex but never issued → NotFound (the stored value is a hash, so an
    // arbitrary same-length hex does not match any row).
    let other = "00".repeat(32);
    assert!(matches!(
        db.poll_pending_session(&other).await,
        Err(tiled_rs::auth::AuthError::NotFound(_))
    ));
}

// === OIDC authorization-code (PKCE browser) flow state — DB-backed (G6) ===

// Round-trip + single use: create persists the verifier/nonce/provider; take
// recovers them exactly once and atomically deletes the row, so a replayed
// callback (second take) finds nothing.
#[tokio::test]
async fn oidc_flow_create_take_single_use() {
    let (db, _dir) = fresh_db().await;
    db.create_oidc_flow_state(
        "state-xyz",
        "mock-idp",
        "the-code-verifier",
        "the-nonce",
        Duration::minutes(10),
    )
    .await
    .unwrap();

    let flow = db
        .take_oidc_flow_state("state-xyz")
        .await
        .unwrap()
        .expect("valid state must be recoverable");
    assert_eq!(flow.provider, "mock-idp");
    assert_eq!(flow.code_verifier, "the-code-verifier");
    assert_eq!(flow.nonce, "the-nonce");

    // Single use: the second take (a replay) finds the row already consumed.
    assert!(
        db.take_oidc_flow_state("state-xyz")
            .await
            .unwrap()
            .is_none(),
        "a consumed flow state must not be reusable"
    );
}

// An unknown state yields None, never an error.
#[tokio::test]
async fn oidc_flow_take_unknown_state_is_none() {
    let (db, _dir) = fresh_db().await;
    assert!(
        db.take_oidc_flow_state("no-such-state")
            .await
            .unwrap()
            .is_none()
    );
}

// Boundary: expiration_time < now. An expired state yields None AND is consumed
// by the take (delete-regardless), so it cannot be replayed once it lapses.
#[tokio::test]
async fn oidc_flow_expired_take_is_none() {
    let (db, _dir) = fresh_db().await;
    db.create_oidc_flow_state(
        "stale-state",
        "mock-idp",
        "v",
        "n",
        Duration::seconds(-1), // already expired
    )
    .await
    .unwrap();
    assert!(
        db.take_oidc_flow_state("stale-state")
            .await
            .unwrap()
            .is_none(),
        "expired state must not be honored"
    );
    // The row was removed by that take; a follow-up is still None.
    assert!(
        db.take_oidc_flow_state("stale-state")
            .await
            .unwrap()
            .is_none()
    );
}

// Distinct states are independent: consuming one leaves the other intact.
#[tokio::test]
async fn oidc_flow_states_are_keyed_independently() {
    let (db, _dir) = fresh_db().await;
    db.create_oidc_flow_state("state-a", "idp", "va", "na", Duration::minutes(10))
        .await
        .unwrap();
    db.create_oidc_flow_state("state-b", "idp", "vb", "nb", Duration::minutes(10))
        .await
        .unwrap();

    let a = db.take_oidc_flow_state("state-a").await.unwrap().unwrap();
    assert_eq!(a.code_verifier, "va");
    // Consuming state-a must not touch state-b.
    let b = db.take_oidc_flow_state("state-b").await.unwrap().unwrap();
    assert_eq!(b.code_verifier, "vb");
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
    // `initiate_device_code` returns the raw device_code (only its hash is
    // persisted). A fresh code is unapproved — the first poll below confirms
    // `Pending`, which is the observable "not yet approved" state.

    // First poll: pending.
    let st = db.poll_device_code(&dc.device_code).await.unwrap();
    assert!(matches!(
        st,
        tiled_rs::auth::device_code::DeviceStatus::Pending
    ));

    // Approve, then poll again: granted.
    db.approve_device_code(&dc.user_code, p.id).await.unwrap();
    let st = db.poll_device_code(&dc.device_code).await.unwrap();
    match st {
        tiled_rs::auth::device_code::DeviceStatus::Granted(pid) => assert_eq!(pid, p.id),
        _ => panic!("expected Granted"),
    }

    // After grant, the row is consumed; next poll fails to find it.
    let err = db.poll_device_code(&dc.device_code).await.unwrap_err();
    assert!(matches!(err, tiled_rs::auth::AuthError::NotFound(_)));
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
    let displayed = tiled_rs::auth::device_code::format_user_code(&dc.user_code);
    let typed = format!("  {}  ", displayed.to_lowercase());

    db.approve_device_code(&typed, p.id).await.unwrap();
    let st = db.poll_device_code(&dc.device_code).await.unwrap();
    assert!(
        matches!(st, tiled_rs::auth::device_code::DeviceStatus::Granted(pid) if pid == p.id),
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
        matches!(err, tiled_rs::auth::AuthError::Conflict(_)),
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
        matches!(err, tiled_rs::auth::AuthError::Conflict(_)),
        "expected Conflict for expired code, got {err:?}"
    );
}

#[tokio::test]
async fn make_admin_by_identity_creates_and_promotes() {
    let (db, _dir) = fresh_db().await;

    // First call: identity does not exist → creates principal + identity, sets admin.
    db.make_admin_by_identity("ldap", "alice").await.unwrap();
    let (principal, _) = db.ensure_principal("ldap", "alice").await.unwrap();
    assert_eq!(
        principal.role, "admin",
        "make_admin_by_identity must set role to 'admin'"
    );
}

#[tokio::test]
async fn make_admin_by_identity_is_idempotent() {
    let (db, _dir) = fresh_db().await;

    db.make_admin_by_identity("oidc", "bob").await.unwrap();
    // Second call must not error and must not demote.
    db.make_admin_by_identity("oidc", "bob").await.unwrap();
    let (principal, _) = db.ensure_principal("oidc", "bob").await.unwrap();
    assert_eq!(principal.role, "admin");
}

#[tokio::test]
async fn make_admin_by_identity_promotes_existing_user() {
    let (db, _dir) = fresh_db().await;

    // Create a user principal first (default role "user").
    let (user_principal, _) = db.ensure_principal("password", "carol").await.unwrap();
    assert_eq!(user_principal.role, "user", "default role must be 'user'");

    // Now bootstrap as admin.
    db.make_admin_by_identity("password", "carol")
        .await
        .unwrap();
    let (promoted, _) = db.ensure_principal("password", "carol").await.unwrap();
    assert_eq!(
        promoted.role, "admin",
        "existing user must be promoted to admin"
    );
}
