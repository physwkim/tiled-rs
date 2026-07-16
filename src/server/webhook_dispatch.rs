//! Webhook dispatcher (upstream tiled PR #1353).
//!
//! At server start we spawn one tokio task that subscribes to the
//! streaming bus's root channel. Every published event flows through
//! this task; for each event the dispatcher looks up the watching node
//! plus all of its ancestors, fetches webhooks subscribed to the event
//! type, and spawns one delivery task per match.
//!
//! Delivery: HMAC-SHA256 sign the JSON body with the webhook's
//! `secret` (if any), POST to the URL, retry on 5xx / connection
//! failure with exponential backoff. The full attempt history is
//! persisted in `webhook_deliveries`.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::catalog::db::Catalog;
use crate::catalog::orm::Webhook;
use crate::catalog::webhook::{OUTCOME_FAILED, OUTCOME_SUCCESS};

use crate::server::streaming::{StreamingBus, UpdateEnvelope, UpdateKind};

#[derive(Clone, Debug)]
pub struct WebhookConfig {
    /// Allow `http://` URLs. Default `false` — webhook targets must
    /// use HTTPS unless the operator opts in (e.g. for testing).
    pub allow_http: bool,
    /// Skip the SSRF check on URL hosts. Default `false`.
    pub allow_private_addresses: bool,
    /// Maximum attempts per delivery. Default 3.
    pub max_attempts: u32,
    /// Initial retry wait. Subsequent retries scale by `retry_factor`.
    pub initial_wait: Duration,
    pub retry_factor: u32,
    pub max_wait: Duration,
    /// Per-attempt request timeout.
    pub request_timeout: Duration,
    /// Upper bound on how long graceful shutdown waits for in-flight
    /// webhook deliveries to finish before abandoning them (see
    /// [`spawn`]'s drain finalizer). This is a hard ceiling on the extra
    /// shutdown latency the webhook subsystem can add: once it elapses,
    /// any delivery still in flight is aborted rather than awaited, so the
    /// process never hangs on an unresponsive third-party endpoint. The
    /// default (30 s) clears the per-attempt `request_timeout` (15 s), so a
    /// single in-flight attempt gets to complete, and equals `max_wait` so
    /// the ceiling reads consistently with the retry schedule. Not wired to
    /// a CLI flag — same as the other timing fields here; override it by
    /// constructing `WebhookConfig` directly.
    pub drain_timeout: Duration,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            allow_http: false,
            allow_private_addresses: false,
            max_attempts: 3,
            initial_wait: Duration::from_secs(1),
            retry_factor: 5,
            max_wait: Duration::from_secs(30),
            request_timeout: Duration::from_secs(15),
            drain_timeout: Duration::from_secs(30),
        }
    }
}

/// Register the dispatcher task with `background` (upstream tiled #1018:
/// the dispatcher must not be a detached `tokio::spawn` — it needs to be
/// findable and awaitable at shutdown). Returns immediately; the task runs
/// until `background`'s owner calls `shutdown()`, or the streaming bus is
/// dropped. If the streaming bus's broadcast lags, missed events are
/// surfaced as warnings (a webhook miss is far less catastrophic than a
/// corrupted state, so we log + continue).
///
/// Shutdown drain (mirrors upstream `WebhookDispatcher.shutdown`,
/// `tiled/server/webhooks.py:352`, which `asyncio.gather`s its
/// `_pending_tasks` set so no delivery is dropped on shutdown). This
/// dispatcher task is the single owner of an in-flight delivery
/// [`JoinSet`](tokio::task::JoinSet): every per-webhook delivery is spawned
/// into it (not detached), finished tasks are reaped each loop turn, and on
/// cancellation ONE finalizer runs — drain any events still buffered on the
/// bus into the set (so a *queued* event is not silently dropped by the
/// cancel branch), then await every *in-flight* delivery, bounded by
/// [`WebhookConfig::drain_timeout`]. Deliveries still running when that
/// bound elapses are abandoned by dropping the set (a clean, bounded abort
/// — the process never hangs on an unresponsive endpoint). Because the loop
/// does not return until this finalizer completes and `BackgroundTasks::
/// shutdown` awaits the loop, the drain composes into the CLI's
/// graceful-shutdown path with the timeout owned here (upstream's gather is
/// unbounded; tiled-rs bounds it per this task's requirement).
pub fn spawn(
    catalog: Catalog,
    bus: StreamingBus,
    config: WebhookConfig,
    background: &crate::server::state::BackgroundTasks,
) {
    let mut cancel = background.cancellation();
    background.spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(target: "tiled.webhooks", "build reqwest client: {e}");
                return;
            }
        };
        let client = Arc::new(client);
        let drain_timeout = config.drain_timeout;
        let config = Arc::new(config);
        // The bus's `publish` fans out to every prefix channel, so
        // subscribing at the empty path ("") yields every event in the
        // tree — exactly what the webhook dispatcher wants.
        let mut rx = bus.subscribe("");
        // Single owner of the in-flight per-webhook delivery tasks (upstream
        // `_pending_tasks`). Spawning into this set — never a detached
        // `tokio::spawn` — is what lets the drain finalizer await them.
        let mut deliveries = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = cancel.changed() => {
                    tracing::info!(target: "tiled.webhooks", "dispatcher stopping: shutdown signalled");
                    // Drain events already buffered on the bus into `deliveries`
                    // before leaving the loop; the biased cancel branch would
                    // otherwise drop a queued event that arrived just before
                    // shutdown. All HTTP write handlers have finished by now
                    // (graceful shutdown ran first), so `try_recv` sees the
                    // final, stable backlog.
                    drain_bus_backlog(&catalog, &client, &config, &mut rx, &mut deliveries).await;
                    break;
                }
                msg = rx.recv() => {
                    match msg {
                        Ok(env) => {
                            dispatch_envelope(&catalog, &client, &config, &env, &mut deliveries)
                                .await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                            tracing::warn!(
                                target: "tiled.webhooks",
                                "streaming bus lagged by {missed} events; webhook deliveries skipped"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
            // Reap finished deliveries so the set does not retain completed
            // handles for the process lifetime (tokio equivalent of upstream's
            // `task.add_done_callback(self._pending_tasks.discard)`).
            while deliveries.try_join_next().is_some() {}
        }
        // The one finalizer for the shutdown transition: await every in-flight
        // delivery, bounded by `drain_timeout`; abandon the remainder cleanly.
        drain_deliveries(&mut deliveries, drain_timeout).await;
    });
}

/// Resolve an envelope's event type and dispatch it, spawning any matched
/// deliveries into `deliveries`. Shared by the live-event branch and the
/// shutdown bus-backlog drain so both go through exactly one code path.
async fn dispatch_envelope(
    catalog: &Catalog,
    client: &Arc<reqwest::Client>,
    config: &Arc<WebhookConfig>,
    env: &UpdateEnvelope,
    deliveries: &mut tokio::task::JoinSet<()>,
) {
    let event_type = match envelope_event_type(env) {
        Some(t) => t,
        None => return, // unknown event kind
    };
    if let Err(e) = dispatch_event(catalog, client, config, env, event_type, deliveries).await {
        tracing::warn!(target: "tiled.webhooks", "dispatch failed: {e}");
    }
}

/// Drain events still buffered on the bus after cancellation, dispatching
/// each into `deliveries`. Non-blocking (`try_recv`): stops as soon as the
/// backlog is empty or the channel closes, so it cannot itself hang.
async fn drain_bus_backlog(
    catalog: &Catalog,
    client: &Arc<reqwest::Client>,
    config: &Arc<WebhookConfig>,
    rx: &mut tokio::sync::broadcast::Receiver<UpdateEnvelope>,
    deliveries: &mut tokio::task::JoinSet<()>,
) {
    use tokio::sync::broadcast::error::TryRecvError;
    loop {
        match rx.try_recv() {
            Ok(env) => dispatch_envelope(catalog, client, config, &env, deliveries).await,
            Err(TryRecvError::Lagged(missed)) => {
                tracing::warn!(
                    target: "tiled.webhooks",
                    "streaming bus lagged by {missed} events during shutdown drain; \
                     webhook deliveries skipped"
                );
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
        }
    }
}

/// Await every in-flight delivery, bounded by `drain_timeout`. On timeout
/// the caller drops the `JoinSet`, which aborts any task still running — a
/// bounded, clean abandon rather than an unbounded wait.
async fn drain_deliveries(deliveries: &mut tokio::task::JoinSet<()>, drain_timeout: Duration) {
    if deliveries.is_empty() {
        return;
    }
    let pending = deliveries.len();
    match tokio::time::timeout(drain_timeout, async {
        while deliveries.join_next().await.is_some() {}
    })
    .await
    {
        Ok(()) => tracing::info!(
            target: "tiled.webhooks",
            "drained {pending} in-flight webhook deliveries before shutdown"
        ),
        Err(_) => tracing::warn!(
            target: "tiled.webhooks",
            "webhook drain timed out after {drain_timeout:?}; abandoning {} in-flight deliveries",
            deliveries.len()
        ),
    }
}

fn envelope_event_type(env: &UpdateEnvelope) -> Option<&'static str> {
    Some(match &env.kind {
        UpdateKind::ChildCreated { .. } => "child-created",
        UpdateKind::MetadataUpdated { .. } => "metadata-updated",
        UpdateKind::NodeDeleted => "node-deleted",
        UpdateKind::DataAppended { .. } => "data-appended",
    })
}

async fn dispatch_event(
    catalog: &Catalog,
    client: &Arc<reqwest::Client>,
    config: &Arc<WebhookConfig>,
    env: &UpdateEnvelope,
    event_type: &'static str,
    deliveries: &mut tokio::task::JoinSet<()>,
) -> Result<(), String> {
    // Find every node along the published path — webhook on any
    // ancestor (or the leaf) should fire.
    let candidate_ids = collect_path_node_ids(catalog, &env.path).await?;
    if candidate_ids.is_empty() {
        return Ok(());
    }
    let webhooks = catalog
        .webhooks_matching(&candidate_ids, event_type)
        .await
        .map_err(|e| e.to_string())?;
    if webhooks.is_empty() {
        return Ok(());
    }
    let event_id = uuid_v4();
    let payload = serde_json::json!({
        "event_id": event_id,
        "event_type": event_type,
        "sequence": env.sequence,
        "timestamp": env.timestamp,
        "path": env.path,
        "data": &env.kind,
    });
    for wh in webhooks {
        let catalog = catalog.clone();
        let client = client.clone();
        let config = config.clone();
        let payload = payload.clone();
        let event_id = event_id.clone();
        let event_type = event_type.to_string();
        // Spawn into the dispatcher-owned `deliveries` set (not a detached
        // `tokio::spawn`) so the shutdown drain in `spawn` can await this
        // delivery. Each task is still individually bounded by
        // `config.max_attempts` retries capped at `config.max_wait`; the
        // drain adds `config.drain_timeout` as the overall ceiling on how
        // long shutdown waits for the whole set. A delivery abandoned at
        // that ceiling leaves its `webhook_deliveries` row `pending` (it
        // never reached `finalize_delivery`) — the bounded residual the
        // drain timeout trades for a guaranteed-terminating shutdown.
        deliveries.spawn(async move {
            if let Err(e) = deliver(
                &catalog,
                &client,
                &config,
                &wh,
                &event_id,
                &event_type,
                &payload,
            )
            .await
            {
                tracing::warn!(
                    target: "tiled.webhooks",
                    "deliver webhook {} -> {}: {e}",
                    wh.id,
                    wh.url,
                );
            }
        });
    }
    Ok(())
}

/// Resolve the published path back to the chain of node IDs, root → leaf.
/// We walk each prefix so a webhook on `/expt` fires for events under
/// `/expt/scan_1/x` too.
async fn collect_path_node_ids(catalog: &Catalog, path: &str) -> Result<Vec<i64>, String> {
    let segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    let mut ids = Vec::with_capacity(segments.len());
    for end in 1..=segments.len() {
        if let Ok(Some(node)) = catalog.lookup(&segments[..end]).await {
            ids.push(node.id);
        }
    }
    Ok(ids)
}

async fn deliver(
    catalog: &Catalog,
    client: &reqwest::Client,
    config: &WebhookConfig,
    wh: &Webhook,
    event_id: &str,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), String> {
    let row = catalog
        .insert_pending_delivery(wh.id, event_id, event_type, payload)
        .await
        .map_err(|e| e.to_string())?;

    let body_bytes = serde_json::to_vec(payload).map_err(|e| format!("encode body: {e}"))?;
    let signature = wh.secret.as_deref().map(|secret| {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
        mac.update(&body_bytes);
        let tag = mac.finalize().into_bytes();
        format!(
            "sha256={}",
            base64::engine::general_purpose::STANDARD.encode(tag)
        )
    });

    let mut wait = config.initial_wait;
    let mut last_status: Option<i32> = None;
    let mut last_error: Option<String> = None;
    let mut attempt = 0u32;

    while attempt < config.max_attempts {
        attempt += 1;
        let mut req = client
            .post(&wh.url)
            .header("Content-Type", "application/json")
            .header("X-Tiled-Event-Id", event_id)
            .header("X-Tiled-Event-Type", event_type)
            .header("X-Tiled-Delivery-Id", row.id.to_string())
            .header("X-Tiled-Delivery-Attempt", attempt.to_string())
            .body(body_bytes.clone());
        if let Some(sig) = signature.as_deref() {
            req = req.header("X-Tiled-Signature", sig);
        }
        match req.send().await {
            Ok(resp) => {
                let code = resp.status().as_u16() as i32;
                last_status = Some(code);
                if (200..300).contains(&code) {
                    catalog
                        .finalize_delivery(
                            row.id,
                            OUTCOME_SUCCESS,
                            last_status,
                            attempt as i32,
                            None,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    return Ok(());
                }
                last_error = Some(format!("HTTP {code}"));
                // 4xx is non-retryable — finalise immediately.
                if (400..500).contains(&code) {
                    catalog
                        .finalize_delivery(
                            row.id,
                            OUTCOME_FAILED,
                            last_status,
                            attempt as i32,
                            last_error.as_deref(),
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    return Err(last_error.unwrap());
                }
            }
            Err(e) => {
                last_error = Some(e.to_string());
            }
        }
        if attempt < config.max_attempts {
            tokio::time::sleep(wait).await;
            wait = std::cmp::min(wait * config.retry_factor, config.max_wait);
        }
    }
    catalog
        .finalize_delivery(
            row.id,
            OUTCOME_FAILED,
            last_status,
            attempt as i32,
            last_error.as_deref(),
        )
        .await
        .map_err(|e| e.to_string())?;
    Err(last_error.unwrap_or_else(|| "unknown delivery error".to_string()))
}

fn uuid_v4() -> String {
    // Lightweight UUIDv4 without pulling in the `uuid` crate — webhook
    // event IDs only need to be opaque + unique per event, not RFC4122-
    // perfect. 16 random bytes → hyphenated hex.
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // variant 1
    let h = |start: usize, end: usize| {
        bytes[start..end]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    let _ = Utc::now(); // touch to silence "unused import" if we later move it
    format!(
        "{}-{}-{}-{}-{}",
        h(0, 4),
        h(4, 6),
        h(6, 8),
        h(8, 10),
        h(10, 16)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::node::RegisterRequest;
    use crate::catalog::webhook::{OUTCOME_PENDING, WebhookCreate};
    use crate::server::state::BackgroundTasks;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// How the throwaway endpoint responds to a webhook POST.
    #[derive(Clone, Copy)]
    enum MockBehavior {
        /// Read the request, wait `delay`, then return `200 OK`.
        Ok200 { delay: Duration },
        /// Read the request, then never respond (hold the socket open).
        Hang,
    }

    /// Spin up a one-response-per-connection HTTP endpoint on 127.0.0.1.
    /// Returns its target URL and a counter bumped once per request read.
    async fn start_mock(behavior: MockBehavior) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/hook");
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_accept = hits.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let hits = hits_accept.clone();
                tokio::spawn(async move {
                    // Read the request head so the client finishes sending.
                    let mut buf = [0u8; 2048];
                    let _ = sock.read(&mut buf).await;
                    hits.fetch_add(1, Ordering::SeqCst);
                    match behavior {
                        MockBehavior::Ok200 { delay } => {
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            let _ = sock
                                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                                .await;
                            let _ = sock.flush().await;
                        }
                        MockBehavior::Hang => {
                            std::future::pending::<()>().await;
                        }
                    }
                });
            }
        });
        (url, hits)
    }

    /// A fresh, migrated catalog in a temp dir (kept alive by the returned dir).
    async fn temp_catalog() -> (tempfile::TempDir, Catalog) {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
        let catalog = Catalog::connect(&uri).await.unwrap();
        catalog.migrate().await.unwrap();
        (dir, catalog)
    }

    /// Create a top-level container node with `key`, returning its node id.
    async fn make_root_node(catalog: &Catalog, key: &str) -> i64 {
        catalog
            .create_node(
                None,
                vec![],
                RegisterRequest {
                    key: key.to_string(),
                    structure_family: "container".to_string(),
                    metadata: serde_json::json!({}),
                    specs: serde_json::json!([]),
                    access_blob: serde_json::json!({}),
                },
            )
            .await
            .unwrap()
            .id
    }

    fn child_created_event() -> UpdateKind {
        UpdateKind::ChildCreated {
            key: "child".to_string(),
            structure_family: "array".to_string(),
        }
    }

    /// Upstream tiled #1018 regression: the dispatcher must register with
    /// `BackgroundTasks` and select on its cancellation signal, not run in
    /// a bare detached `tokio::spawn`. If the dispatcher loop ever regresses
    /// to `rx.recv().await` with no cancellation branch, it never notices
    /// `shutdown()`'s signal and loops forever on the still-open streaming
    /// bus — this test's `timeout` then fails instead of the test hanging.
    #[tokio::test]
    async fn dispatcher_stops_when_shutdown_is_signalled() {
        let (_dir, catalog) = temp_catalog().await;
        let bus = StreamingBus::new();
        let background = BackgroundTasks::new();
        spawn(catalog, bus, WebhookConfig::default(), &background);

        // Let the task actually start (subscribe to the bus) before
        // signalling shutdown.
        tokio::time::sleep(Duration::from_millis(20)).await;

        tokio::time::timeout(Duration::from_secs(2), background.shutdown())
            .await
            .expect("dispatcher must stop once shutdown() is signalled, not hang");
    }

    /// Required case: a delivery that is *in flight* when shutdown starts must
    /// be flushed to completion before `shutdown()` returns. The endpoint
    /// stalls its `200` by 500 ms, so the delivery is provably mid-request at
    /// shutdown time — the only way its row can reach `success` is the drain
    /// finalizer awaiting it (upstream `WebhookDispatcher.shutdown` gathers
    /// `_pending_tasks`, webhooks.py:356).
    #[tokio::test]
    async fn pending_delivery_is_flushed_before_shutdown_completes() {
        let (_dir, catalog) = temp_catalog().await;
        let node_id = make_root_node(&catalog, "n").await;
        let (url, hits) = start_mock(MockBehavior::Ok200 {
            delay: Duration::from_millis(500),
        })
        .await;
        let wh = catalog
            .create_webhook(WebhookCreate {
                node_id,
                url,
                secret: None,
                events: None,
            })
            .await
            .unwrap();

        let bus = StreamingBus::new();
        let background = BackgroundTasks::new();
        let config = WebhookConfig {
            drain_timeout: Duration::from_secs(5),
            ..WebhookConfig::default()
        };
        spawn(catalog.clone(), bus.clone(), config, &background);

        // Subscribe, publish, then give the dispatcher a moment to consume the
        // event and put the delivery in flight before we signal shutdown.
        tokio::time::sleep(Duration::from_millis(50)).await;
        bus.publish("n", child_created_event());
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::time::timeout(Duration::from_secs(3), background.shutdown())
            .await
            .expect("shutdown must not hang");

        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "the endpoint should have received exactly one delivery"
        );
        let rows = catalog
            .list_deliveries_for_webhook(wh.id, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "one delivery row expected");
        assert_eq!(
            rows[0].outcome.as_str(),
            OUTCOME_SUCCESS,
            "the in-flight delivery must be drained to success before shutdown returns"
        );
    }

    /// A *queued* event — published just before shutdown and not yet consumed
    /// by the dispatcher's normal recv branch — must still be delivered, not
    /// dropped by the biased cancel branch. On a single-threaded runtime the
    /// dispatcher parks on `rx.recv()` during the initial sleep, then the
    /// synchronous `publish` + `shutdown()` run with no yield between them, so
    /// cancellation is observed before the event is ever recv'd: delivery here
    /// can only happen via the shutdown bus-backlog drain.
    #[tokio::test(flavor = "current_thread")]
    async fn queued_event_is_drained_not_dropped_on_shutdown() {
        let (_dir, catalog) = temp_catalog().await;
        let node_id = make_root_node(&catalog, "n").await;
        let (url, hits) = start_mock(MockBehavior::Ok200 {
            delay: Duration::from_millis(0),
        })
        .await;
        let wh = catalog
            .create_webhook(WebhookCreate {
                node_id,
                url,
                secret: None,
                events: None,
            })
            .await
            .unwrap();

        let bus = StreamingBus::new();
        let background = BackgroundTasks::new();
        let config = WebhookConfig {
            drain_timeout: Duration::from_secs(5),
            ..WebhookConfig::default()
        };
        spawn(catalog.clone(), bus.clone(), config, &background);

        // Yield once so the dispatcher subscribes and parks on rx.recv().
        tokio::time::sleep(Duration::from_millis(50)).await;
        // No await between publish and shutdown: the dispatcher cannot run its
        // recv branch for this event; cancellation fires first.
        bus.publish("n", child_created_event());
        tokio::time::timeout(Duration::from_secs(3), background.shutdown())
            .await
            .expect("shutdown must not hang");

        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "the queued event must still be delivered via the shutdown drain"
        );
        let rows = catalog
            .list_deliveries_for_webhook(wh.id, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "one delivery row expected");
        assert_eq!(rows[0].outcome.as_str(), OUTCOME_SUCCESS);
    }

    /// Required case: the drain is bounded. An endpoint that accepts but never
    /// responds keeps its delivery in flight past the drain window; shutdown
    /// must abandon it at `drain_timeout` and return, never blocking for the
    /// (much longer) per-request timeout.
    #[tokio::test]
    async fn drain_timeout_abandons_in_flight_delivery_without_hanging() {
        let (_dir, catalog) = temp_catalog().await;
        let node_id = make_root_node(&catalog, "n").await;
        let (url, hits) = start_mock(MockBehavior::Hang).await;
        let wh = catalog
            .create_webhook(WebhookCreate {
                node_id,
                url,
                secret: None,
                events: None,
            })
            .await
            .unwrap();

        let bus = StreamingBus::new();
        let background = BackgroundTasks::new();
        // Drain window (200 ms) far below the single attempt's request timeout
        // (10 s): the drain must give up and abandon, not wait it out.
        let config = WebhookConfig {
            drain_timeout: Duration::from_millis(200),
            request_timeout: Duration::from_secs(10),
            max_attempts: 1,
            ..WebhookConfig::default()
        };
        spawn(catalog.clone(), bus.clone(), config, &background);

        tokio::time::sleep(Duration::from_millis(50)).await;
        bus.publish("n", child_created_event());
        // Let the delivery reach the endpoint (and hang) before shutdown.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let start = tokio::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(2), background.shutdown())
            .await
            .expect("shutdown must abandon the hung delivery, not hang");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown should return near the 200 ms drain bound, took {elapsed:?}"
        );

        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "the endpoint should have received the request before it was abandoned"
        );
        let rows = catalog
            .list_deliveries_for_webhook(wh.id, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "one delivery row expected");
        assert_eq!(
            rows[0].outcome.as_str(),
            OUTCOME_PENDING,
            "an abandoned in-flight delivery stays pending — never finalized"
        );
    }

    /// Required case: a server with webhooks effectively disabled shuts down
    /// unchanged. Two facets: (a) `webhook_config = None` means `build_app`
    /// never spawns the dispatcher, so `BackgroundTasks` has nothing to await;
    /// (b) an enabled dispatcher with no registered webhooks spawns no
    /// deliveries, so the drain finalizer's set is empty. Both must return
    /// promptly, adding no shutdown latency.
    #[tokio::test]
    async fn shutdown_is_prompt_when_no_webhooks_are_active() {
        // (a) No dispatcher registered at all.
        let background = BackgroundTasks::new();
        tokio::time::timeout(Duration::from_secs(1), background.shutdown())
            .await
            .expect("empty shutdown must be immediate");

        // (b) Dispatcher running, events flowing, but zero webhooks registered.
        let (_dir, catalog) = temp_catalog().await;
        let _node_id = make_root_node(&catalog, "n").await;
        let bus = StreamingBus::new();
        let background = BackgroundTasks::new();
        spawn(
            catalog.clone(),
            bus.clone(),
            WebhookConfig::default(),
            &background,
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        bus.publish("n", child_created_event());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let start = tokio::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(2), background.shutdown())
            .await
            .expect("shutdown must not hang");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "with nothing to drain, shutdown should be prompt"
        );
    }
}
