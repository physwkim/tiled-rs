-- tiled-rs catalog initial schema (PostgreSQL)
--
-- JSON columns use jsonb for indexed search; tiled#588's btree_gin lesson
-- means we want jsonb so future search query builders can use GIN indexes
-- without another migration.

CREATE TABLE IF NOT EXISTS nodes (
    id           BIGSERIAL PRIMARY KEY,
    key          TEXT      NOT NULL,
    parent_id    BIGINT             REFERENCES nodes(id) ON DELETE CASCADE,
    ancestors    JSONB     NOT NULL DEFAULT '[]'::jsonb,
    structure_family TEXT  NOT NULL,
    metadata     JSONB     NOT NULL DEFAULT '{}'::jsonb,
    specs        JSONB     NOT NULL DEFAULT '[]'::jsonb,
    access_blob  JSONB     NOT NULL DEFAULT '{}'::jsonb,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now(),
    time_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- See sqlite/0001 for why we COALESCE: SQL treats NULL != NULL so a plain
-- (parent_id, key) UNIQUE allows duplicates at root.
CREATE UNIQUE INDEX IF NOT EXISTS nodes_uniq_parent_key
    ON nodes (COALESCE(parent_id, -1), key);
CREATE INDEX IF NOT EXISTS idx_nodes_parent_id ON nodes(parent_id);
CREATE INDEX IF NOT EXISTS idx_nodes_key       ON nodes(key);
CREATE INDEX IF NOT EXISTS idx_nodes_metadata_gin ON nodes USING gin (metadata);

CREATE TABLE IF NOT EXISTS data_sources (
    id              BIGSERIAL PRIMARY KEY,
    node_id         BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    structure_family TEXT  NOT NULL,
    structure       JSONB  NOT NULL DEFAULT '{}'::jsonb,
    mimetype        TEXT   NOT NULL,
    parameters      JSONB  NOT NULL DEFAULT '{}'::jsonb,
    management      TEXT   NOT NULL DEFAULT 'external'
);

CREATE INDEX IF NOT EXISTS idx_data_sources_node_id ON data_sources(node_id);

CREATE TABLE IF NOT EXISTS assets (
    id            BIGSERIAL PRIMARY KEY,
    data_source_id BIGINT NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    data_uri      TEXT   NOT NULL,
    is_directory  BOOLEAN NOT NULL DEFAULT FALSE,
    parameter     TEXT   NOT NULL DEFAULT 'data_uri',
    num           INTEGER
);

CREATE INDEX IF NOT EXISTS idx_assets_data_source_id ON assets(data_source_id);

CREATE TABLE IF NOT EXISTS revisions (
    id           BIGSERIAL PRIMARY KEY,
    node_id      BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    revision     INTEGER NOT NULL,
    metadata     JSONB  NOT NULL DEFAULT '{}'::jsonb,
    specs        JSONB  NOT NULL DEFAULT '[]'::jsonb,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (node_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_revisions_node_id ON revisions(node_id);
