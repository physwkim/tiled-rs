-- GIN index over the tsvector projection of metadata (upstream tiled
-- PR #640). Lets the FullText query use `@@ plainto_tsquery(...)` with
-- index support instead of falling back to `LIKE %term%` over JSON.

CREATE INDEX IF NOT EXISTS idx_nodes_metadata_fts
    ON nodes
    USING gin (to_tsvector('simple', metadata::text));
