//! End-to-end test for `tiled serve directory` (`tiled_rs::cli::serve_directory_start`).
//!
//! Drives the whole orchestration the CLI runs: create an ephemeral catalog,
//! start an HTTP server over a source directory, register the directory's files
//! through the client (over HTTP), then serve. The success criterion (sweep P2)
//! is that a directory containing a CSV is served — so the test starts the
//! server, reads the registered table back over HTTP with the single-user API
//! key, and asserts the concrete column values (not merely a 200).

#![cfg(feature = "csv-adapter")]

use arrow::array::{Float64Array, Int64Array};

use tiled_rs::cli::{ServeDirectoryArgs, serve_directory_start};
use tiled_rs::client::{ContextOptions, from_uri_with_options};

fn args_for(directory: std::path::PathBuf, api_key: &str) -> ServeDirectoryArgs {
    ServeDirectoryArgs {
        directory,
        // Ephemeral OS-assigned port so parallel test binaries never collide.
        host: Some("127.0.0.1".to_string()),
        port: Some(0),
        // Non-public: the read-back client must present the single-user key,
        // exercising the real single-user auth path (not anonymous access).
        public: false,
        api_key: Some(api_key.to_string()),
        keep_ext: false,
        include_ext: Vec::new(),
        ext: Vec::new(),
        watch: false,
        verbose: false,
    }
}

#[tokio::test]
async fn serve_directory_serves_a_csv_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("sales.csv");
    std::fs::write(&csv_path, "x,y\n1,1.5\n2,2.5\n3,3.5\n").unwrap();

    let api_key = "testkey12345";
    let server = serve_directory_start(args_for(dir.path().to_path_buf(), api_key))
        .await
        .expect("serve directory must build the catalog, serve, and register the CSV");

    // Read the registered node back over HTTP with the single-user key.
    let options = ContextOptions {
        api_key: Some(api_key.to_string()),
        ..Default::default()
    };
    let root = from_uri_with_options(&server.base_url, options, false)
        .await
        .expect("client must connect to the running serve-directory server")
        .into_container()
        .expect("root must be a container");

    let table = root
        .get("sales")
        .await
        .expect("the registered CSV must be listable as 'sales'")
        .into_table()
        .expect("the registered CSV node must be a table");
    assert_eq!(table.columns(), &["x", "y"]);

    let part = table.read_partition(0, None).await.unwrap();
    let rows: usize = part.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all 3 CSV rows must be served");

    let batch = &part.batches[0];
    let x = batch
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column x must decode as Int64");
    assert_eq!(x.values(), &[1, 2, 3]);
    let y = batch
        .column_by_name("y")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("column y must decode as Float64");
    assert_eq!(y.values(), &[1.5, 2.5, 3.5]);

    // Teardown: stop the server and remove the leaked temp catalog tree.
    let temp_catalog = server.temp_dir().to_path_buf();
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&temp_catalog);
}

#[tokio::test]
async fn serve_directory_read_requires_the_api_key_when_not_public() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("nums.csv"), "a\n1\n2\n").unwrap();

    let api_key = "secretkeyabcdef";
    let server = serve_directory_start(args_for(dir.path().to_path_buf(), api_key))
        .await
        .expect("serve directory must succeed");

    // No credential → the non-public server rejects the read (401), proving the
    // API-key requirement is in force for reads.
    let anon = from_uri_with_options(&server.base_url, ContextOptions::default(), false).await;
    assert!(
        anon.is_err(),
        "a non-public serve-directory server must reject an unauthenticated read"
    );

    let temp_catalog = server.temp_dir().to_path_buf();
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&temp_catalog);
}
