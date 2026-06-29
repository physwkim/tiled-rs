//! `tiled admin {check-config, list-principals, show-principal}` (#1363).
//!
//! These exercise the CLI dispatch + argument resolution. The underlying DB
//! queries (list/detail) are covered by tiled-auth's sqlite_auth tests; here
//! we assert the command layer succeeds for valid input and surfaces an error
//! for invalid input (a malformed config, an unknown principal uuid).

use tiled_rs::auth::AuthDb;
use tiled_rs::cli::{AdminCommand, Command, run};

async fn temp_auth_db() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let db = AuthDb::connect(&uri).await.unwrap();
    db.migrate().await.unwrap();
    (uri, dir)
}

/// check-config: a well-formed config validates; a malformed one returns an
/// error naming the offending file (Python `tiled admin check-config` exits
/// non-zero on parse failure, _admin.py:158-162).
#[tokio::test]
async fn check_config_valid_passes_and_invalid_fails() {
    let dir = tempfile::tempdir().unwrap();

    let good = dir.path().join("good.yml");
    std::fs::write(&good, "trees: []\n").unwrap();
    run(Command::Admin {
        command: AdminCommand::CheckConfig {
            config_path: Some(good.display().to_string()),
        },
    })
    .await
    .expect("a well-formed config must pass check-config");

    // `trees` must be a list; a scalar is a deserialization error.
    let bad = dir.path().join("bad.yml");
    std::fs::write(&bad, "trees: not-a-list\n").unwrap();
    let err = run(Command::Admin {
        command: AdminCommand::CheckConfig {
            config_path: Some(bad.display().to_string()),
        },
    })
    .await
    .expect_err("a malformed config must fail check-config");
    let chain = format!("{err:#}");
    assert!(
        chain.contains("bad.yml"),
        "the error must name the offending config file: {chain}"
    );
}

/// list-principals + show-principal dispatch against a real auth DB. A created
/// principal is shown by its uuid; an unknown uuid is a not-found error.
#[tokio::test]
async fn list_and_show_principal_dispatch() {
    let (uri, _dir) = temp_auth_db().await;
    let db = AuthDb::connect(&uri).await.unwrap();
    let principal = db.create_service_principal("admin").await.unwrap();

    // list-principals succeeds (and the DB has at least the one we created).
    run(Command::Admin {
        command: AdminCommand::ListPrincipals {
            auth_db_uri: uri.clone(),
            offset: 0,
            limit: 100,
        },
    })
    .await
    .expect("list-principals must succeed");

    // show-principal with the real uuid succeeds.
    run(Command::Admin {
        command: AdminCommand::ShowPrincipal {
            auth_db_uri: uri.clone(),
            uuid: principal.uuid.clone(),
        },
    })
    .await
    .expect("show-principal must succeed for an existing uuid");

    // show-principal with an unknown uuid is a not-found error.
    let err = run(Command::Admin {
        command: AdminCommand::ShowPrincipal {
            auth_db_uri: uri,
            uuid: "00000000-0000-0000-0000-000000000000".into(),
        },
    })
    .await
    .expect_err("show-principal must fail for an unknown uuid");
    assert!(
        err.to_string().contains("No such Principal"),
        "error must report the missing principal: {err}"
    );
}
