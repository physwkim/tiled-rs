-- Make `api_keys.access_tags` NULLABLE so one value means one thing:
--   NULL      => no restriction (principal's full tag set applies)
--   '[]'      => deny ALL tagged access (upstream `set([])`)
--   '[a,b]'   => narrow to the intersection with {a,b}
-- See the sqlite/0010 companion for the full rationale. Upstream stores this
-- column as a nullable JSONList and reads NULL as "no restriction" vs `[]` as
-- the empty set (get_access_tags_from_api_key, authentication.py:261-263). The
-- prior schema (`JSONB NOT NULL DEFAULT '[]'`) collapsed `None` and `Some([])`
-- onto the same stored '[]', so a caller sending `[]` for deny-all silently got
-- an UNRESTRICTED key.
ALTER TABLE api_keys ALTER COLUMN access_tags DROP NOT NULL;
ALTER TABLE api_keys ALTER COLUMN access_tags DROP DEFAULT;

-- EXISTING ROWS: pre-migration '[]' rows were written under the old
-- None/[]-collapse and their intent is ambiguous. Relax them to NULL
-- (no restriction) so no live key is retroactively locked out; non-empty
-- arrays carry unambiguous intent and are kept.
UPDATE api_keys SET access_tags = NULL WHERE access_tags = '[]'::jsonb;
