-- Make `api_keys.access_tags` NULLABLE so one value means one thing:
--   NULL      => no restriction (principal's full tag set applies)
--   '[]'      => deny ALL tagged access (upstream `set([])`)
--   '[a,b]'   => narrow to the intersection with {a,b}
-- Upstream stores this column as JSONList(511) nullable and reads NULL as
-- "no restriction" vs `[]` as the empty set (tiled/authn_database/orm.py:192,
-- get_access_tags_from_api_key, authentication.py:261-263). The prior schema
-- used `TEXT NOT NULL DEFAULT '[]'`, collapsing `None` and `Some([])` onto the
-- same stored '[]', so a caller who sent `[]` intending deny-all silently got
-- an UNRESTRICTED key.
--
-- SQLite cannot drop a column's NOT NULL in place, so rebuild the table (the
-- sqlite.org "Making Other Kinds Of Table Schema Changes" pattern). Nothing
-- REFERENCES api_keys, so the drop/rename is safe with foreign keys enabled.
--
-- EXISTING ROWS: every pre-migration row holds '[]' either because the caller
-- omitted access_tags (intent: no restriction) or explicitly sent '[]' (intent:
-- deny-all) — the old collapse erased which. The stored value cannot tell them
-- apart, so migrate '[]' -> NULL (no restriction): this preserves access for
-- live keys rather than retroactively locking them out. Non-empty arrays carry
-- unambiguous intent and are kept verbatim.
CREATE TABLE api_keys_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id INTEGER NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    secret_hash TEXT NOT NULL,
    first_eight TEXT NOT NULL,
    note TEXT,
    scopes TEXT NOT NULL DEFAULT '[]',
    expiration_time TEXT,
    time_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    latest_activity TEXT,
    -- Nullable, no default: an omitted access_tags now persists as NULL
    -- (no restriction), matching upstream's nullable JSONList.
    access_tags TEXT
);

INSERT INTO api_keys_new (id, principal_id, secret_hash, first_eight, note,
                          scopes, expiration_time, time_created, latest_activity,
                          access_tags)
SELECT id, principal_id, secret_hash, first_eight, note,
       scopes, expiration_time, time_created, latest_activity,
       CASE WHEN access_tags = '[]' THEN NULL ELSE access_tags END
FROM api_keys;

DROP TABLE api_keys;

ALTER TABLE api_keys_new RENAME TO api_keys;

CREATE INDEX IF NOT EXISTS idx_api_keys_first_eight ON api_keys(first_eight);
CREATE INDEX IF NOT EXISTS idx_api_keys_principal_id ON api_keys(principal_id);
