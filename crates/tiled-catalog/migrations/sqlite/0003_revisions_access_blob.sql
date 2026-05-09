-- Add access_blob to revisions (upstream tiled PR #1084).
--
-- Lets revision history carry the per-revision access policy snapshot
-- so an undo doesn't lose the access metadata that was effective at
-- the time. Existing rows default to '{}' (the same default the nodes
-- table uses).

ALTER TABLE revisions
    ADD COLUMN access_blob TEXT NOT NULL DEFAULT '{}';
