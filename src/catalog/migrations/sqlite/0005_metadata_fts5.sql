-- Full-text search index over node metadata (catalog M2; upstream tiled #723).
--
-- `metadata_fts5` is an external-content FTS5 virtual table: it stores only the
-- inverted index, while `content='nodes'` / `content_rowid='id'` point each
-- posting back at the backing `nodes` row by its `id`. The three triggers keep
-- the index synchronized with `nodes` on insert/delete/update, and the one-time
-- 'rebuild' backfills any rows that already exist when this migration runs
-- (a no-op on a fresh database, where `nodes` is still empty).
--
-- Mirrors Python tiled `catalog/orm.py` (the metadata_fts5 table and the
-- nodes_metadata_fts5_sync_{ai,ad,au} triggers). The 'delete'/'rebuild' command
-- rows are the external-content maintenance API; see sqlite.org/fts5.html §4.4.3.
--
-- Cascade-deleted descendants do not fire the AFTER DELETE trigger (SQLite's
-- recursive_triggers pragma is off, matching Python), so their postings linger.
-- That is harmless: `nodes.id` is AUTOINCREMENT (rowids are never reused) and
-- the search filters to existing nodes via `id IN (SELECT rowid ...)`, so a
-- stale posting can never resurrect a deleted node or alias a new one.
CREATE VIRTUAL TABLE IF NOT EXISTS metadata_fts5
    USING fts5(metadata, content='nodes', content_rowid='id');

CREATE TRIGGER IF NOT EXISTS nodes_metadata_fts5_sync_ai AFTER INSERT ON nodes BEGIN
    INSERT INTO metadata_fts5(rowid, metadata) VALUES (new.id, new.metadata);
END;

CREATE TRIGGER IF NOT EXISTS nodes_metadata_fts5_sync_ad AFTER DELETE ON nodes BEGIN
    INSERT INTO metadata_fts5(metadata_fts5, rowid, metadata) VALUES ('delete', old.id, old.metadata);
END;

CREATE TRIGGER IF NOT EXISTS nodes_metadata_fts5_sync_au AFTER UPDATE ON nodes BEGIN
    INSERT INTO metadata_fts5(metadata_fts5, rowid, metadata) VALUES ('delete', old.id, old.metadata);
    INSERT INTO metadata_fts5(rowid, metadata) VALUES (new.id, new.metadata);
END;

INSERT INTO metadata_fts5(metadata_fts5) VALUES ('rebuild');
