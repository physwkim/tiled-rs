-- No-op on SQLite. SQLite FTS5 needs a virtual mirror table that's
-- substantially more involved (upstream tiled PR #723 covers it).
-- We keep the migration entry so the per-dialect lockstep doesn't drift.

SELECT 1;
