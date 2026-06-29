-- No-op on Postgres. Full-text search is served by the GIN tsvector index from
-- migration 0004 (`@@ plainto_tsquery(...)` in search.rs), so no FTS5 mirror
-- table is needed here. We keep the migration entry so the per-dialect lockstep
-- doesn't drift. Mirrors Python's no-op FTS5 compile on PostgreSQL
-- (catalog/orm.py `_compile_no_op_fts5_postgresql`).

SELECT 1;
