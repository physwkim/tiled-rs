//! Invariant-boundary tests for the tag-config compiler.

use std::sync::Arc;

use sqlx::SqlitePool;

use super::*;

// ---- helpers ---------------------------------------------------------------

fn parse_cfg(yaml: &str) -> TagConfig {
    serde_yaml::from_str(yaml).expect("parse tag config yaml")
}

/// Build a compiler over an in-memory DB, load the inline config. Does NOT
/// compile — the caller decides so error cases can be asserted.
async fn make(yaml: &str, group_parser: Option<GroupParser>) -> AccessTagsCompiler {
    let mut c = AccessTagsCompiler::connect(
        all_scope_names(),
        TagConfigSource::Inline(parse_cfg(yaml)),
        "sqlite::memory:",
        group_parser,
    )
    .await
    .expect("connect compiler");
    c.load_tag_config().expect("load config");
    c
}

fn gp_static(map: &'static [(&'static str, &'static [&'static str])]) -> GroupParser {
    Arc::new(move |name: &str| {
        map.iter()
            .find(|(g, _)| *g == name)
            .map(|(_, members)| members.iter().map(|s| s.to_string()).collect())
    })
}

async fn tag_has_user(pool: &SqlitePool, tag: &str, user: &str) -> bool {
    let n: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?)",
    )
    .bind(tag)
    .bind(user)
    .fetch_one(pool)
    .await
    .unwrap();
    n == 1
}

async fn tag_has_scope(pool: &SqlitePool, tag: &str, user: &str, scope: &str) -> bool {
    let n: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes \
         WHERE tag_name = ? AND user_name = ? AND scope_name = ?)",
    )
    .bind(tag)
    .bind(user)
    .bind(scope)
    .fetch_one(pool)
    .await
    .unwrap();
    n == 1
}

async fn is_public(pool: &SqlitePool, tag: &str) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM public_tags WHERE name = ?)")
        .bind(tag)
        .fetch_one(pool)
        .await
        .unwrap();
    n == 1
}

async fn is_owner(pool: &SqlitePool, tag: &str, user: &str) -> bool {
    let n: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM user_tag_owners WHERE tag_name = ? AND user_name = ?)",
    )
    .bind(tag)
    .bind(user)
    .fetch_one(pool)
    .await
    .unwrap();
    n == 1
}

async fn tag_exists(pool: &SqlitePool, tag: &str) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tags WHERE name = ?)")
        .bind(tag)
        .fetch_one(pool)
        .await
        .unwrap();
    n == 1
}

async fn count_acl_rows(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM tags_users_scopes")
        .fetch_one(pool)
        .await
        .unwrap()
}

// ---- example config end-to-end --------------------------------------------

const EXAMPLE: &str = r#"
roles:
  facility_user:
    scopes: ["read:data", "read:metadata"]
  facility_admin:
    scopes: ["read:data", "read:metadata", "write:data", "write:metadata",
             "delete:node", "delete:revision", "create:node", "register"]
tags:
  data_A:
    groups:
      - name: group_A
        role: facility_user
    auto_tags:
      - name: data_admin
  data_B:
    users:
      - name: alice
        scopes: ["read:data", "read:metadata"]
    auto_tags:
      - name: data_admin
  data_C:
    users:
      - name: bob
        role: facility_user
    auto_tags:
      - name: data_admin
  data_D:
    auto_tags:
      - name: data_admin
      - name: public
  data_admin:
    users:
      - name: cara
        role: facility_admin
tag_owners:
  data_admin:
    users:
      - name: cara
"#;

#[tokio::test]
async fn example_config_compiles_expected_acls() {
    let gp = gp_static(&[("group_A", &["alice", "bob"]), ("admins", &["cara"])]);
    let mut c = make(EXAMPLE, Some(gp)).await;
    c.compile().await.expect("compile example");
    let pool = c.pool();

    // data_admin: cara with the full facility_admin scope set.
    assert!(tag_has_scope(pool, "data_admin", "cara", "write:data").await);
    assert!(tag_has_scope(pool, "data_admin", "cara", "register").await);

    // data_A inherits data_admin (cara) and adds group_A (alice, bob) at
    // facility_user (read-only) scopes.
    assert!(tag_has_user(pool, "data_A", "cara").await);
    assert!(tag_has_scope(pool, "data_A", "alice", "read:data").await);
    assert!(tag_has_scope(pool, "data_A", "bob", "read:metadata").await);
    assert!(
        !tag_has_scope(pool, "data_A", "alice", "write:data").await,
        "group members get only their role scopes, not the inherited admin's"
    );

    // data_B: alice direct read + inherited cara.
    assert!(tag_has_user(pool, "data_B", "alice").await);
    assert!(tag_has_user(pool, "data_B", "cara").await);

    // public flag: data_D inherits `public`; data_A does not; `public` itself is public.
    assert!(is_public(pool, "data_D").await);
    assert!(!is_public(pool, "data_A").await);
    assert!(is_public(pool, "public").await);
    assert!(tag_exists(pool, "public").await);

    // ownership: cara owns data_admin; nobody owns data_A.
    assert!(is_owner(pool, "data_admin", "cara").await);
    assert!(!is_owner(pool, "data_A", "cara").await);
}

// ---- nesting boundary ------------------------------------------------------

/// Build a linear auto_tags chain t0 -> t1 -> ... -> t{n-1}; the deepest tag
/// carries a single user so an ACL exists to inherit up the chain.
fn chain_cfg(n: usize) -> String {
    let mut s = String::from("tags:\n");
    for i in 0..n {
        s.push_str(&format!("  t{i}:\n"));
        if i + 1 < n {
            s.push_str(&format!("    auto_tags:\n      - name: t{}\n", i + 1));
        } else {
            s.push_str("    users:\n      - name: leaf\n        scopes: [\"read:metadata\"]\n");
        }
    }
    s
}

#[tokio::test]
async fn nesting_at_limit_compiles() {
    // Chain t0..t5: the deepest tag (t5) is entered at nesting level 5, which
    // is == MAX_TAG_NESTING (5) and NOT over it. Must compile.
    let n = MAX_TAG_NESTING + 1; // 6 tags -> levels 0..=5
    let mut c = make(&chain_cfg(n), None).await;
    c.compile()
        .await
        .expect("chain at the nesting limit must compile");
    // The leaf user propagates all the way to the root tag.
    assert!(tag_has_user(c.pool(), "t0", "leaf").await);
}

#[tokio::test]
async fn nesting_over_limit_errors() {
    // Chain t0..t6: t6 is entered at nesting level 6 > MAX_TAG_NESTING -> error.
    let n = MAX_TAG_NESTING + 2; // 7 tags -> levels 0..=6
    let mut c = make(&chain_cfg(n), None).await;
    let err = c
        .compile()
        .await
        .expect_err("chain past the nesting limit must error");
    assert!(
        err.to_string().contains("maximum tag nesting"),
        "error must name the nesting limit, got: {err}"
    );
}

// ---- cyclic / duplicate references -----------------------------------------

const CYCLE: &str = r#"
tags:
  a:
    users:
      - name: ua
        scopes: ["read:metadata"]
    auto_tags:
      - name: b
  b:
    users:
      - name: ub
        scopes: ["read:data"]
    auto_tags:
      - name: a
"#;

#[tokio::test]
async fn cyclic_reference_is_safe_not_infinite() {
    let mut c = make(CYCLE, None).await;
    c.compile().await.expect("a cycle must compile, not hang");
    let pool = c.pool();
    // a is compiled first (BTreeMap order): a sees b's ub (b's recursion into a
    // hits the seen-guard and contributes nothing), then adds its own ua.
    assert!(tag_has_user(pool, "a", "ua").await);
    assert!(tag_has_user(pool, "a", "ub").await);
    // b, memoized after a, holds only its own ub (a was on the seen path).
    assert!(tag_has_user(pool, "b", "ub").await);
    assert!(
        !tag_has_user(pool, "b", "ua").await,
        "the cycle back-edge contributes nothing"
    );
}

#[tokio::test]
async fn self_loop_is_safe() {
    let cfg = r#"
tags:
  x:
    users:
      - name: ux
        scopes: ["read:metadata"]
    auto_tags:
      - name: x
"#;
    let mut c = make(cfg, None).await;
    c.compile().await.expect("a self-loop must compile");
    assert!(tag_has_user(c.pool(), "x", "ux").await);
}

#[tokio::test]
async fn undefined_auto_tag_errors() {
    let cfg = r#"
tags:
  x:
    auto_tags:
      - name: nope
"#;
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("undefined auto_tag must error");
    assert!(
        matches!(err, AccessTagsError::UndefinedAutoTag { .. }),
        "got: {err}"
    );
}

// ---- public tag rules ------------------------------------------------------

#[tokio::test]
async fn public_tag_redefinition_is_rejected() {
    // Any case-spelling of `public` as a top-level tag is a redefinition of the
    // built-in and must be rejected (casefolded comparison).
    for name in ["public", "Public", "PUBLIC"] {
        let cfg = format!(
            "tags:\n  {name}:\n    users:\n      - name: u\n        scopes: [\"read:metadata\"]\n"
        );
        let mut c = make(&cfg, None).await;
        let err = match c.compile().await {
            Ok(()) => panic!("redefining {name:?} must error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, AccessTagsError::PublicRedefined),
            "got: {err}"
        );
    }
}

// ---- group expansion: empty, missing, and no parser ------------------------

const ONE_GROUP: &str = r#"
tags:
  g:
    groups:
      - name: grp
        scopes: ["read:metadata"]
"#;

#[tokio::test]
async fn group_expansion_empty_group_adds_no_users() {
    // Parser knows grp but it has no members.
    let gp = gp_static(&[("grp", &[])]);
    let mut c = make(ONE_GROUP, Some(gp)).await;
    c.compile().await.expect("empty group compiles");
    assert!(tag_exists(c.pool(), "g").await);
    assert_eq!(
        count_acl_rows(c.pool()).await,
        0,
        "an empty group grants nobody"
    );
}

#[tokio::test]
async fn group_expansion_missing_group_is_skipped() {
    // Parser reports grp as missing (None) -> warn + skip, no error.
    let gp = gp_static(&[("other", &["x"])]);
    let mut c = make(ONE_GROUP, Some(gp)).await;
    c.compile()
        .await
        .expect("missing group is skipped, not fatal");
    assert_eq!(count_acl_rows(c.pool()).await, 0);
}

#[tokio::test]
async fn group_expansion_no_parser_is_skipped() {
    // No parser configured at all -> every group reference is skipped.
    let mut c = make(ONE_GROUP, None).await;
    c.compile()
        .await
        .expect("no parser: group refs are skipped");
    assert_eq!(count_acl_rows(c.pool()).await, 0);
}

#[tokio::test]
async fn group_bad_scopes_error_before_expansion() {
    // The scope validation runs before the parser is consulted: an invalid
    // group scope errors even when the group would have been skipped.
    let cfg = r#"
tags:
  g:
    groups:
      - name: grp
        scopes: ["not:a:scope"]
"#;
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("invalid group scope must error");
    assert!(
        err.to_string().contains("not in the valid set"),
        "got: {err}"
    );
}

// ---- role / member validation ----------------------------------------------

#[tokio::test]
async fn role_missing_scopes_errors() {
    let cfg = "roles:\n  r: {}\ntags:\n  t:\n    users:\n      - name: u\n        role: r\n";
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("role without scopes must error");
    assert!(
        matches!(err, AccessTagsError::RoleScopesMissing { .. }),
        "got: {err}"
    );
}

#[tokio::test]
async fn role_empty_scopes_errors() {
    let cfg =
        "roles:\n  r:\n    scopes: []\ntags:\n  t:\n    users:\n      - name: u\n        role: r\n";
    let mut c = make(cfg, None).await;
    let err = c.compile().await.expect_err("empty role scopes must error");
    assert!(
        matches!(err, AccessTagsError::RoleScopesEmpty { .. }),
        "got: {err}"
    );
}

#[tokio::test]
async fn role_invalid_scope_errors() {
    let cfg = "roles:\n  r:\n    scopes: [\"bogus\"]\ntags:\n  t:\n    users:\n      - name: u\n        role: r\n";
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("invalid role scope must error");
    assert!(
        matches!(err, AccessTagsError::RoleScopesInvalid { .. }),
        "got: {err}"
    );
}

#[tokio::test]
async fn user_both_role_and_scopes_errors() {
    let cfg = r#"
roles:
  r:
    scopes: ["read:metadata"]
tags:
  t:
    users:
      - name: u
        role: r
        scopes: ["read:data"]
"#;
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("both role and scopes must error");
    assert!(
        err.to_string().contains("both 'scopes' and 'role'"),
        "got: {err}"
    );
}

#[tokio::test]
async fn user_neither_role_nor_scopes_errors() {
    let cfg = "tags:\n  t:\n    users:\n      - name: u\n";
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("neither role nor scopes must error");
    assert!(
        err.to_string().contains("either 'scopes' or 'role'"),
        "got: {err}"
    );
}

#[tokio::test]
async fn user_empty_scopes_errors() {
    let cfg = "tags:\n  t:\n    users:\n      - name: u\n        scopes: []\n";
    let mut c = make(cfg, None).await;
    let err = c.compile().await.expect_err("empty user scopes must error");
    assert!(err.to_string().contains("must not be empty"), "got: {err}");
}

#[tokio::test]
async fn user_invalid_scope_errors() {
    let cfg = "tags:\n  t:\n    users:\n      - name: u\n        scopes: [\"nope\"]\n";
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("invalid user scope must error");
    assert!(
        err.to_string().contains("not in the valid set"),
        "got: {err}"
    );
}

#[tokio::test]
async fn unknown_role_resolves_empty_and_errors() {
    // A `role:` naming a role that isn't defined resolves to an empty scope set
    // (upstream falls through to `user.get("scopes", [])`), which then trips
    // the "scopes must not be empty" guard.
    let cfg = "tags:\n  t:\n    users:\n      - name: u\n        role: ghost\n";
    let mut c = make(cfg, None).await;
    let err = c
        .compile()
        .await
        .expect_err("unknown role -> empty -> error");
    assert!(err.to_string().contains("must not be empty"), "got: {err}");
}

// ---- missing config file ---------------------------------------------------

#[tokio::test]
async fn missing_config_file_errors() {
    let mut c = AccessTagsCompiler::connect(
        all_scope_names(),
        TagConfigSource::File("/no/such/tag_config.yml".into()),
        "sqlite::memory:",
        None,
    )
    .await
    .expect("connect");
    let err = c.load_tag_config().expect_err("missing file must error");
    assert!(
        matches!(err, AccessTagsError::ConfigFileMissing(_)),
        "got: {err}"
    );
}

// ---- raw config load / clear ----------------------------------------------

#[tokio::test]
async fn load_merges_roles_and_clear_resets_them() {
    // BASE defines a `reader` role. Roles are never persisted to the DB (upstream
    // keeps them in memory only), so they are observable via `roles()`.
    let mut c = make(BASE, None).await;
    assert!(c.roles().contains_key("reader"), "loaded role is visible");
    c.clear_raw_tags();
    assert!(c.roles().is_empty(), "clear_raw_tags drops loaded roles");
}

// ---- recompile idempotency + deltas ---------------------------------------

const BASE: &str = r#"
roles:
  reader:
    scopes: ["read:data"]
tags:
  t1:
    users:
      - name: u1
        scopes: ["read:metadata"]
  t2:
    users:
      - name: u2
        role: reader
"#;

// Same tag set as BASE, but: t1 removed; t3 added; t2's u2 switched from the
// `reader` role to explicit write scopes; t2 made public via auto_tags.
const CHANGED: &str = r#"
roles:
  reader:
    scopes: ["read:data"]
tags:
  t2:
    users:
      - name: u2
        scopes: ["write:data", "write:metadata"]
    auto_tags:
      - name: public
  t3:
    users:
      - name: u3
        scopes: ["read:data"]
"#;

#[tokio::test]
async fn recompile_same_config_is_idempotent() {
    let mut c = make(BASE, None).await;
    c.compile().await.expect("first compile");
    let before = count_acl_rows(c.pool()).await;
    c.recompile().await.expect("recompile same config");
    let after = count_acl_rows(c.pool()).await;
    assert_eq!(
        before, after,
        "recompiling identical config must not change row count"
    );
    // Still exactly the two expected grants.
    assert!(tag_has_scope(c.pool(), "t1", "u1", "read:metadata").await);
    assert!(tag_has_scope(c.pool(), "t2", "u2", "read:data").await);
}

#[tokio::test]
async fn recompile_applies_deltas() {
    let mut c = make(BASE, None).await;
    c.compile().await.expect("first compile");
    // Baseline present.
    assert!(tag_exists(c.pool(), "t1").await);
    assert!(tag_has_scope(c.pool(), "t2", "u2", "read:data").await);
    assert!(!is_public(c.pool(), "t2").await);

    // Swap in the changed config and recompile.
    c.clear_raw_tags();
    c.set_tag_config(TagConfigSource::Inline(parse_cfg(CHANGED)));
    c.load_tag_config().expect("load changed");
    c.recompile().await.expect("recompile changed");
    let pool = c.pool();

    // Removed tag and its grants are gone.
    assert!(!tag_exists(pool, "t1").await, "t1 removed");
    assert!(!tag_has_user(pool, "t1", "u1").await);

    // Added tag present.
    assert!(
        tag_has_scope(pool, "t3", "u3", "read:data").await,
        "t3 added"
    );

    // Scope change took effect: u2 lost read:data, gained write:data.
    assert!(tag_has_scope(pool, "t2", "u2", "write:data").await);
    assert!(
        !tag_has_scope(pool, "t2", "u2", "read:data").await,
        "old reader-role scope must be gone after the switch"
    );

    // Public toggled on.
    assert!(is_public(pool, "t2").await, "t2 became public");
}

#[tokio::test]
async fn recompile_reflects_group_membership_change() {
    let cfg = r#"
tags:
  team:
    groups:
      - name: grp
        scopes: ["read:metadata"]
tag_owners:
  team:
    groups:
      - name: grp
"#;
    // First compile: grp = [ann].
    let gp1 = gp_static(&[("grp", &["ann"])]);
    let mut c = make(cfg, Some(gp1)).await;
    c.compile().await.expect("compile v1");
    assert!(tag_has_user(c.pool(), "team", "ann").await);
    assert!(is_owner(c.pool(), "team", "ann").await);
    assert!(!tag_has_user(c.pool(), "team", "bea").await);

    // Membership change: grp = [ann, bea]. Recompile.
    let gp2 = gp_static(&[("grp", &["ann", "bea"])]);
    c.set_group_parser(Some(gp2));
    c.recompile().await.expect("compile v2");
    assert!(
        tag_has_user(c.pool(), "team", "bea").await,
        "new member added"
    );
    assert!(
        is_owner(c.pool(), "team", "bea").await,
        "new owner member added"
    );
}

// ---- ownership: public status is independent of ownership ------------------

#[tokio::test]
async fn public_and_owned_are_independent() {
    let cfg = r#"
tags:
  secret:
    users:
      - name: keeper
        scopes: ["read:metadata"]
  open:
    auto_tags:
      - name: public
tag_owners:
  secret:
    users:
      - name: keeper
"#;
    let mut c = make(cfg, None).await;
    c.compile().await.expect("compile");
    let pool = c.pool();
    // owned but not public
    assert!(is_owner(pool, "secret", "keeper").await);
    assert!(!is_public(pool, "secret").await);
    // public but not owned
    assert!(is_public(pool, "open").await);
    assert!(!is_owner(pool, "open", "keeper").await);
}
