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
        }
    }
}

/// Spawn the dispatcher task. Returns immediately. The task runs for
/// the lifetime of the server; if the streaming bus's broadcast lags,
/// missed events are surfaced as warnings (a webhook miss is far less
/// catastrophic than a corrupted state, so we log + continue).
pub fn spawn(
    catalog: Catalog,
    bus: StreamingBus,
    config: WebhookConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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
        let config = Arc::new(config);
        // The bus's `publish` fans out to every prefix channel, so
        // subscribing at the empty path ("") yields every event in the
        // tree — exactly what the webhook dispatcher wants.
        let mut rx = bus.subscribe("");
        loop {
            match rx.recv().await {
                Ok(env) => {
                    let event_type = match envelope_event_type(&env) {
                        Some(t) => t,
                        None => continue, // unknown event kind
                    };
                    if let Err(e) =
                        dispatch_event(&catalog, &client, &config, &env, event_type).await
                    {
                        tracing::warn!(target: "tiled.webhooks", "dispatch failed: {e}");
                    }
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
    })
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
        tokio::spawn(async move {
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
