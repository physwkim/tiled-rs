-- See sqlite/0003_revisions_access_blob.sql.

ALTER TABLE revisions
    ADD COLUMN IF NOT EXISTS access_blob JSONB NOT NULL DEFAULT '{}'::jsonb;
