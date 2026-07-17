//! `TILED_EXPLAIN_SQL` debug aid (Finding 1) — boundary tests.
//!
//! Verifies the env-gated SQL plan emission ported from upstream
//! `tiled/catalog/explain.py`:
//!
//! * enabled → a search query emits a plan (captured off the
//!   `tiled.catalog.explain` tracing target);
//! * disabled → nothing is emitted and no EXPLAIN round-trip runs.
//!
//! The gate is exercised through the [`Catalog::with_explain_sql`] seam so both
//! states are covered deterministically in one test process, independent of the
//! ambient `TILED_EXPLAIN_SQL` environment value.

use std::sync::{Arc, Mutex};

use tiled_rs::catalog::Catalog;
use tiled_rs::catalog::node::RegisterRequest;
use tiled_rs::core::queries::{Eq, Query};

use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

/// A tracing layer that records the fields of every event emitted on the
/// `tiled.catalog.explain` target into a shared buffer.
#[derive(Clone, Default)]
struct PlanCapture(Arc<Mutex<Vec<String>>>);

impl PlanCapture {
    fn events(&self) -> Vec<String> {
        self.0.lock().expect("capture mutex").clone()
    }
}

struct FieldVisitor<'a>(&'a mut String);

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push_str(&format!("{}={} ", field.name(), value));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%sql` / `%plan` (Display) arrive here wrapped as Debug format-args.
        self.0.push_str(&format!("{}={:?} ", field.name(), value));
    }
}

impl<S: Subscriber> Layer<S> for PlanCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "tiled.catalog.explain" {
            return;
        }
        let mut buf = String::new();
        event.record(&mut FieldVisitor(&mut buf));
        self.0.lock().expect("capture mutex").push(buf);
    }
}

/// Fresh in-memory-ish SQLite catalog (temp file) with one node to search over.
async fn catalog_with_node(explain: bool) -> (Catalog, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri)
        .await
        .unwrap()
        .with_explain_sql(explain);
    catalog.migrate().await.unwrap();
    catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "node_a".into(),
                structure_family: "container".into(),
                metadata: serde_json::json!({"color": "red"}),
                specs: serde_json::json!([]),
                access_blob: serde_json::json!({}),
            },
        )
        .await
        .unwrap();
    (catalog, dir)
}

fn search_query() -> Vec<Query> {
    vec![Query::Eq(Eq {
        key: "color".into(),
        value: serde_json::json!("red"),
    })]
}

#[tokio::test]
async fn enabled_emits_plan_for_search_query() {
    let capture = PlanCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let (catalog, _dir) = catalog_with_node(true).await;
    let (rows, _total) = catalog
        .search_children(None, &search_query(), &[], 0, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "search should find the one matching node");

    let events = capture.events();
    assert!(
        !events.is_empty(),
        "TILED_EXPLAIN_SQL on: a search query must emit at least one plan event"
    );
    let joined = events.join("\n");
    assert!(
        joined.contains("nodes"),
        "captured plan must reference the `nodes` table, got: {joined}"
    );
    assert!(
        joined.contains("plan="),
        "captured event must carry a `plan` field, got: {joined}"
    );
}

#[tokio::test]
async fn disabled_emits_nothing_for_search_query() {
    let capture = PlanCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    // Default (explain off) — the disabled hot path must add no EXPLAIN
    // round-trip and emit no plan event.
    let (catalog, _dir) = catalog_with_node(false).await;
    let (rows, _total) = catalog
        .search_children(None, &search_query(), &[], 0, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "search should still find the matching node");

    assert!(
        capture.events().is_empty(),
        "TILED_EXPLAIN_SQL off: no plan event may be emitted, got: {:?}",
        capture.events()
    );
}

/// Also covers the cursor (keyset) and distinct query paths under the gate, so
/// every explained statement is exercised, not just the offset SELECT.
#[tokio::test]
async fn enabled_emits_plan_for_cursor_and_distinct() {
    let capture = PlanCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let (catalog, _dir) = catalog_with_node(true).await;

    let _ = catalog
        .search_children_cursor(None, &search_query(), &[], None, 10)
        .await
        .unwrap();
    let _ = catalog
        .get_distinct(None, &[], &["color".to_string()], false, false, false)
        .await
        .unwrap();

    let events = capture.events();
    assert!(
        events.len() >= 2,
        "cursor + distinct searches must each emit a plan, got {} events",
        events.len()
    );
}
