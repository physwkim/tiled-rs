-- tiled-rs catalog initial schema (SQLite)
--
-- Mirrors `tiled.catalog.orm` closely enough that the same data model
-- ports across SQLite and Postgres. Uses TEXT for JSON columns; sqlx
-- decodes them into serde_json::Value.

CREATE TABLE IF NOT EXISTS nodes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    key          TEXT    NOT NULL,
    parent_id    INTEGER          REFERENCES nodes(id) ON DELETE CASCADE,
    -- Materialised ancestor chain (without trailing key) so listing children
    -- by parent path stays O(1) rather than walking up via recursive CTEs.
    ancestors    TEXT    NOT NULL DEFAULT '[]',  -- JSON array of strings
    structure_family TEXT NOT NULL,
    metadata     TEXT    NOT NULL DEFAULT '{}',  -- JSON object
    specs        TEXT    NOT NULL DEFAULT '[]',  -- JSON array of {name, version}
    access_blob  TEXT    NOT NULL DEFAULT '{}',  -- JSON object (tags, owner, ...)
    time_created TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    time_updated TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- COALESCE(parent_id, -1) so the root (NULL parent) collides with itself.
-- Plain `UNIQUE (parent_id, key)` would let duplicates slip in at root
-- because standard SQL treats NULL != NULL.
CREATE UNIQUE INDEX IF NOT EXISTS nodes_uniq_parent_key
    ON nodes (COALESCE(parent_id, -1), key);
CREATE INDEX IF NOT EXISTS idx_nodes_parent_id ON nodes(parent_id);
CREATE INDEX IF NOT EXISTS idx_nodes_key       ON nodes(key);

CREATE TABLE IF NOT EXISTS data_sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id         INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    structure_family TEXT   NOT NULL,
    structure       TEXT    NOT NULL DEFAULT '{}', -- JSON
    mimetype        TEXT    NOT NULL,
    parameters      TEXT    NOT NULL DEFAULT '{}', -- JSON
    management      TEXT    NOT NULL DEFAULT 'external'  -- external | writable | immutable
);

CREATE INDEX IF NOT EXISTS idx_data_sources_node_id ON data_sources(node_id);

CREATE TABLE IF NOT EXISTS assets (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    data_source_id INTEGER NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    data_uri      TEXT    NOT NULL,
    is_directory  INTEGER NOT NULL DEFAULT 0, -- bool
    parameter     TEXT    NOT NULL DEFAULT 'data_uri',
    num           INTEGER          -- nullable: per-asset ordinal in a sequence
);

CREATE INDEX IF NOT EXISTS idx_assets_data_source_id ON assets(data_source_id);

CREATE TABLE IF NOT EXISTS revisions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id      INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    revision     INTEGER NOT NULL,                    -- sequential per-node
    metadata     TEXT    NOT NULL DEFAULT '{}',
    specs        TEXT    NOT NULL DEFAULT '[]',
    time_created TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (node_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_revisions_node_id ON revisions(node_id);
