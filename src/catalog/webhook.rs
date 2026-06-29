//! Webhook CRUD against the catalog DB.
//!
//! Mirrors upstream tiled PR #1353. Each [`Webhook`] watches one node
//! (and any descendants); when an event of a subscribed type fires,
//! the dispatcher inserts a [`WebhookDelivery`] row, POSTs to the URL,
//! and updates the row with the outcome.

use chrono::Utc;
use serde_json::Value;
use sqlx::Row;

use crate::catalog::db::{Catalog, DbPool};
use crate::catalog::error::{CatalogError, Result};
use crate::catalog::orm::{Webhook, WebhookDelivery};

/// What a caller passes to register a new webhook.
#[derive(Debug, Clone)]
pub struct WebhookCreate {
    pub node_id: i64,
    pub url: String,
    pub secret: Option<String>,
    pub events: Option<Vec<String>>,
}

/// Outcome of a delivery attempt — written to the `outcome` column.
pub const OUTCOME_PENDING: &str = "pending";
pub const OUTCOME_SUCCESS: &str = "success";
pub const OUTCOME_FAILED: &str = "failed";

impl Catalog {
    pub async fn create_webhook(&self, req: WebhookCreate) -> Result<Webhook> {
        let events_json = req
            .events
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| CatalogError::Validation(format!("encode events: {e}")))?;
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO webhooks (node_id, url, secret, events, active)
                     VALUES (?1, ?2, ?3, ?4, 1)
                     RETURNING id, node_id, url, secret, events, active,
                               time_created, time_updated",
                )
                .bind(req.node_id)
                .bind(&req.url)
                .bind(req.secret.as_deref())
                .bind(events_json.as_deref())
                .fetch_one(pool)
                .await?;
                Ok(decode_webhook_sqlite(&row)?)
            }
            DbPool::Postgres(pool) => {
                let events_value = req
                    .events
                    .as_ref()
                    .map(|v| serde_json::to_value(v).unwrap_or(Value::Null));
                let row = sqlx::query(
                    "INSERT INTO webhooks (node_id, url, secret, events)
                     VALUES ($1, $2, $3, $4)
                     RETURNING id, node_id, url, secret, events, active,
                               time_created, time_updated",
                )
                .bind(req.node_id)
                .bind(&req.url)
                .bind(req.secret.as_deref())
                .bind(events_value)
                .fetch_one(pool)
                .await?;
                Ok(decode_webhook_pg(&row)?)
            }
        }
    }

    pub async fn list_webhooks_for_node(&self, node_id: i64) -> Result<Vec<Webhook>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT id, node_id, url, secret, events, active,
                            time_created, time_updated
                       FROM webhooks
                      WHERE node_id = ?1
                      ORDER BY id",
                )
                .bind(node_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(decode_webhook_sqlite).collect()
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, node_id, url, secret, events, active,
                            time_created, time_updated
                       FROM webhooks
                      WHERE node_id = $1
                      ORDER BY id",
                )
                .bind(node_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(decode_webhook_pg).collect()
            }
        }
    }

    /// Webhooks watching a node's path or any of its ancestor paths,
    /// filtered to those subscribed to `event_type` (or with no event
    /// filter, which means "all events").
    pub async fn webhooks_matching(
        &self,
        candidate_node_ids: &[i64],
        event_type: &str,
    ) -> Result<Vec<Webhook>> {
        if candidate_node_ids.is_empty() {
            return Ok(Vec::new());
        }
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let placeholders: String = (1..=candidate_node_ids.len())
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "SELECT id, node_id, url, secret, events, active,
                            time_created, time_updated
                       FROM webhooks
                      WHERE active = 1 AND node_id IN ({placeholders})"
                );
                let mut q = sqlx::query(&sql);
                for id in candidate_node_ids {
                    q = q.bind(id);
                }
                let rows = q.fetch_all(pool).await?;
                let all = rows
                    .iter()
                    .map(decode_webhook_sqlite)
                    .collect::<Result<Vec<_>>>()?;
                Ok(all
                    .into_iter()
                    .filter(|w| webhook_matches_event(w, event_type))
                    .collect())
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, node_id, url, secret, events, active,
                            time_created, time_updated
                       FROM webhooks
                      WHERE active = TRUE AND node_id = ANY($1)",
                )
                .bind(candidate_node_ids)
                .fetch_all(pool)
                .await?;
                let all = rows
                    .iter()
                    .map(decode_webhook_pg)
                    .collect::<Result<Vec<_>>>()?;
                Ok(all
                    .into_iter()
                    .filter(|w| webhook_matches_event(w, event_type))
                    .collect())
            }
        }
    }

    pub async fn delete_webhook(&self, id: i64) -> Result<bool> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let res = sqlx::query("DELETE FROM webhooks WHERE id = ?1")
                    .bind(id)
                    .execute(pool)
                    .await?;
                Ok(res.rows_affected() > 0)
            }
            DbPool::Postgres(pool) => {
                let res = sqlx::query("DELETE FROM webhooks WHERE id = $1")
                    .bind(id)
                    .execute(pool)
                    .await?;
                Ok(res.rows_affected() > 0)
            }
        }
    }

    pub async fn insert_pending_delivery(
        &self,
        webhook_id: i64,
        event_id: &str,
        event_type: &str,
        payload: &Value,
    ) -> Result<WebhookDelivery> {
        let payload_json = serde_json::to_string(payload)
            .map_err(|e| CatalogError::Validation(format!("encode payload: {e}")))?;
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "INSERT INTO webhook_deliveries
                       (webhook_id, event_id, event_type, payload, outcome)
                     VALUES (?1, ?2, ?3, ?4, 'pending')
                     RETURNING id, webhook_id, event_id, event_type, payload,
                               status_code, attempts, delivered_at, outcome,
                               error_detail, time_created, time_updated",
                )
                .bind(webhook_id)
                .bind(event_id)
                .bind(event_type)
                .bind(&payload_json)
                .fetch_one(pool)
                .await?;
                Ok(decode_delivery_sqlite(&row)?)
            }
            DbPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO webhook_deliveries
                       (webhook_id, event_id, event_type, payload, outcome)
                     VALUES ($1, $2, $3, $4, 'pending')
                     RETURNING id, webhook_id, event_id, event_type, payload,
                               status_code, attempts, delivered_at, outcome,
                               error_detail, time_created, time_updated",
                )
                .bind(webhook_id)
                .bind(event_id)
                .bind(event_type)
                .bind(payload)
                .fetch_one(pool)
                .await?;
                Ok(decode_delivery_pg(&row)?)
            }
        }
    }

    pub async fn finalize_delivery(
        &self,
        delivery_id: i64,
        outcome: &str,
        status_code: Option<i32>,
        attempts: i32,
        error_detail: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now();
        match self.pool() {
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE webhook_deliveries
                        SET outcome = ?1,
                            status_code = ?2,
                            attempts = ?3,
                            error_detail = ?4,
                            delivered_at = ?5,
                            time_updated = ?5
                      WHERE id = ?6",
                )
                .bind(outcome)
                .bind(status_code)
                .bind(attempts)
                .bind(error_detail)
                .bind(now.to_rfc3339())
                .bind(delivery_id)
                .execute(pool)
                .await?;
            }
            DbPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE webhook_deliveries
                        SET outcome = $1,
                            status_code = $2,
                            attempts = $3,
                            error_detail = $4,
                            delivered_at = $5,
                            time_updated = $5
                      WHERE id = $6",
                )
                .bind(outcome)
                .bind(status_code)
                .bind(attempts)
                .bind(error_detail)
                .bind(now)
                .bind(delivery_id)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_deliveries_for_webhook(
        &self,
        webhook_id: i64,
        limit: i64,
    ) -> Result<Vec<WebhookDelivery>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT id, webhook_id, event_id, event_type, payload,
                            status_code, attempts, delivered_at, outcome,
                            error_detail, time_created, time_updated
                       FROM webhook_deliveries
                      WHERE webhook_id = ?1
                      ORDER BY id DESC
                      LIMIT ?2",
                )
                .bind(webhook_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.iter().map(decode_delivery_sqlite).collect()
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT id, webhook_id, event_id, event_type, payload,
                            status_code, attempts, delivered_at, outcome,
                            error_detail, time_created, time_updated
                       FROM webhook_deliveries
                      WHERE webhook_id = $1
                      ORDER BY id DESC
                      LIMIT $2",
                )
                .bind(webhook_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.iter().map(decode_delivery_pg).collect()
            }
        }
    }

    pub async fn get_webhook(&self, id: i64) -> Result<Option<Webhook>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT id, node_id, url, secret, events, active,
                            time_created, time_updated
                       FROM webhooks
                      WHERE id = ?1",
                )
                .bind(id)
                .fetch_optional(pool)
                .await?;
                row.as_ref().map(decode_webhook_sqlite).transpose()
            }
            DbPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT id, node_id, url, secret, events, active,
                            time_created, time_updated
                       FROM webhooks
                      WHERE id = $1",
                )
                .bind(id)
                .fetch_optional(pool)
                .await?;
                row.as_ref().map(decode_webhook_pg).transpose()
            }
        }
    }
}

fn webhook_matches_event(w: &Webhook, event_type: &str) -> bool {
    match &w.events {
        None => true,
        Some(v) if v.is_empty() => true,
        Some(v) => v.iter().any(|e| e == event_type),
    }
}

fn parse_iso(value: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| CatalogError::Validation(format!("parse timestamp '{value}': {e}")))
}

fn decode_events_text(text: Option<&str>) -> Result<Option<Vec<String>>> {
    match text {
        None => Ok(None),
        Some("") => Ok(None),
        Some(s) => serde_json::from_str(s)
            .map(Some)
            .map_err(|e| CatalogError::Validation(format!("decode events: {e}"))),
    }
}

fn decode_webhook_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<Webhook> {
    let events_text: Option<String> = row.try_get("events")?;
    let active_int: i64 = row.try_get("active")?;
    let time_created: String = row.try_get("time_created")?;
    let time_updated: String = row.try_get("time_updated")?;
    Ok(Webhook {
        id: row.try_get("id")?,
        node_id: row.try_get("node_id")?,
        url: row.try_get("url")?,
        secret: row.try_get("secret")?,
        events: decode_events_text(events_text.as_deref())?,
        active: active_int != 0,
        time_created: parse_iso(&time_created)?,
        time_updated: parse_iso(&time_updated)?,
    })
}

fn decode_webhook_pg(row: &sqlx::postgres::PgRow) -> Result<Webhook> {
    let events_value: Option<Value> = row.try_get("events")?;
    let events = match events_value {
        None => None,
        Some(v) => serde_json::from_value(v)
            .map_err(|e| CatalogError::Validation(format!("decode events: {e}")))?,
    };
    Ok(Webhook {
        id: row.try_get("id")?,
        node_id: row.try_get("node_id")?,
        url: row.try_get("url")?,
        secret: row.try_get("secret")?,
        events,
        active: row.try_get("active")?,
        time_created: row.try_get("time_created")?,
        time_updated: row.try_get("time_updated")?,
    })
}

fn decode_delivery_sqlite(row: &sqlx::sqlite::SqliteRow) -> Result<WebhookDelivery> {
    let payload_text: String = row.try_get("payload")?;
    let payload: Value = serde_json::from_str(&payload_text)
        .map_err(|e| CatalogError::Validation(format!("decode payload: {e}")))?;
    let delivered_at: Option<String> = row.try_get("delivered_at")?;
    let time_created: String = row.try_get("time_created")?;
    let time_updated: String = row.try_get("time_updated")?;
    Ok(WebhookDelivery {
        id: row.try_get("id")?,
        webhook_id: row.try_get("webhook_id")?,
        event_id: row.try_get("event_id")?,
        event_type: row.try_get("event_type")?,
        payload,
        status_code: row.try_get("status_code")?,
        attempts: row.try_get("attempts")?,
        delivered_at: delivered_at.as_deref().map(parse_iso).transpose()?,
        outcome: row.try_get("outcome")?,
        error_detail: row.try_get("error_detail")?,
        time_created: parse_iso(&time_created)?,
        time_updated: parse_iso(&time_updated)?,
    })
}

fn decode_delivery_pg(row: &sqlx::postgres::PgRow) -> Result<WebhookDelivery> {
    Ok(WebhookDelivery {
        id: row.try_get("id")?,
        webhook_id: row.try_get("webhook_id")?,
        event_id: row.try_get("event_id")?,
        event_type: row.try_get("event_type")?,
        payload: row.try_get("payload")?,
        status_code: row.try_get("status_code")?,
        attempts: row.try_get("attempts")?,
        delivered_at: row.try_get("delivered_at")?,
        outcome: row.try_get("outcome")?,
        error_detail: row.try_get("error_detail")?,
        time_created: row.try_get("time_created")?,
        time_updated: row.try_get("time_updated")?,
    })
}
