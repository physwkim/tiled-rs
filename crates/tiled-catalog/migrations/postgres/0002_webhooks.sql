-- Webhooks (upstream tiled PR #1353).
-- See sqlite/0002_webhooks.sql for the design notes.

CREATE TABLE IF NOT EXISTS webhooks (
    id           BIGSERIAL PRIMARY KEY,
    node_id      BIGINT  NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    url          TEXT    NOT NULL,
    secret       TEXT,
    events       JSONB,
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    time_created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    time_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhooks_node_id ON webhooks(node_id);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id            BIGSERIAL PRIMARY KEY,
    webhook_id    BIGINT  NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    event_id      TEXT    NOT NULL,
    event_type    TEXT    NOT NULL,
    payload       JSONB   NOT NULL,
    status_code   INTEGER,
    attempts      INTEGER NOT NULL DEFAULT 0,
    delivered_at  TIMESTAMPTZ,
    outcome       TEXT    NOT NULL DEFAULT 'pending',
    error_detail  TEXT,
    time_created  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    time_updated  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_webhook_id ON webhook_deliveries(webhook_id);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_event_id   ON webhook_deliveries(event_id);
