-- Webhooks (upstream tiled PR #1353).
--
-- A registered webhook fires an HTTP POST to a URL when an event occurs
-- on the watched node OR any of its descendants. Each Webhook produces
-- zero or more WebhookDelivery rows — one per delivery attempt — with
-- outcome=pending → success | failed.

CREATE TABLE IF NOT EXISTS webhooks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id      INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    url          TEXT    NOT NULL,
    -- HMAC-SHA256 secret. NULL = no signature header sent.
    secret       TEXT,
    -- JSON array of event-type strings, e.g. '["child-created"]'.
    -- NULL or '[]' = subscribe to every event.
    events       TEXT,
    active       INTEGER NOT NULL DEFAULT 1,   -- bool
    time_created TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    time_updated TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_webhooks_node_id ON webhooks(node_id);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    webhook_id    INTEGER NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    -- Stable identifier for the logical event; retries within one
    -- _deliver call share the same event_id and update in place.
    event_id      TEXT    NOT NULL,
    event_type    TEXT    NOT NULL,
    payload       TEXT    NOT NULL,                  -- JSON
    status_code   INTEGER,                           -- NULL while pending
    attempts      INTEGER NOT NULL DEFAULT 0,
    delivered_at  TEXT,                              -- ISO-8601 UTC
    -- pending | success | failed
    outcome       TEXT    NOT NULL DEFAULT 'pending',
    error_detail  TEXT,
    time_created  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    time_updated  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_webhook_id ON webhook_deliveries(webhook_id);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_event_id   ON webhook_deliveries(event_id);
