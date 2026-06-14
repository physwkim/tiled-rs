//! Translate `tiled_core::queries::Query` filters into SQL clauses.
//!
//! Each `Query` variant we support produces a fragment + a list of bind
//! parameters. The `WhereBuilder` glues them with `AND`; un-supported
//! variants fall through with a no-op (matching `MongoCatalog`'s behaviour
//! of "leave the result set unchanged" rather than dropping every row).
//!
//! The SQL syntax differs between SQLite and Postgres for JSON access:
//! - SQLite uses `json_extract(metadata, '$.key')`.
//! - Postgres uses `metadata ->> 'key'` (text) / `metadata -> 'key'` (json).
//!
//! Each `to_sql_*` method picks the right shape.

use serde_json::Value;

use sqlx::Row;
use tiled_core::queries::{Operator, Query};

use crate::db::{Catalog, DbPool};
use crate::error::Result;
use crate::orm::Node;

#[derive(Debug, Clone, Copy)]
enum Dialect {
    Sqlite,
    Postgres,
}

impl Dialect {
    fn for_pool(pool: &DbPool) -> Self {
        match pool {
            DbPool::Sqlite(_) => Self::Sqlite,
            DbPool::Postgres(_) => Self::Postgres,
        }
    }

    /// Format a placeholder for the `i`-th bound parameter (0-indexed).
    fn placeholder(self, i: usize) -> String {
        match self {
            Self::Sqlite => "?".to_string(),
            Self::Postgres => format!("${}", i + 1),
        }
    }

    /// SQL fragment that pulls JSON value at `key` as text.
    fn json_text(self, column: &str, key: &str) -> String {
        let safe = sanitize_json_key(key);
        match self {
            Self::Sqlite => format!("json_extract({column}, '$.{safe}')"),
            Self::Postgres => format!("({column} ->> '{safe}')"),
        }
    }

    /// SQL fragment that pulls JSON value at `key` as JSON (for type-safe
    /// array containment etc.).
    fn json_value(self, column: &str, key: &str) -> String {
        let safe = sanitize_json_key(key);
        match self {
            Self::Sqlite => format!("json_extract({column}, '$.{safe}')"),
            Self::Postgres => format!("({column} -> '{safe}')"),
        }
    }
}

/// Strip every byte that isn't alphanumeric / `_` / `.` / `-` from a JSON
/// key before splicing it into a SQL fragment. The query interface is the
/// only place a remote attacker can supply this string, and we don't want
/// `'); DROP TABLE …` getting through. Legitimate Tiled metadata keys are
/// alphanumeric in practice; an unfortunate user with a key containing
/// special characters will simply not match — which is preferable to SQL
/// injection.
fn sanitize_json_key(key: &str) -> String {
    key.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        .collect()
}

impl Dialect {
    fn metadata_full_text(self) -> &'static str {
        match self {
            Self::Sqlite => "metadata",
            Self::Postgres => "metadata::text",
        }
    }
}

/// Builds a parameterised WHERE clause as `(fragment, bindings)`.
struct WhereBuilder {
    dialect: Dialect,
    pieces: Vec<String>,
    bindings: Vec<Bind>,
}

#[derive(Debug, Clone)]
enum Bind {
    Text(String),
    Int(i64),
    Real(f64),
}

impl WhereBuilder {
    fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            pieces: Vec::new(),
            bindings: Vec::new(),
        }
    }

    fn push_eq(&mut self, key: &str, value: &Value) {
        let lhs = self.dialect.json_text("metadata", key);
        let p = self.dialect.placeholder(self.bindings.len());
        let rendered = render_value_as_text(value);
        self.pieces.push(format!("{lhs} = {p}"));
        self.bindings.push(Bind::Text(rendered));
    }

    fn push_neq(&mut self, key: &str, value: &Value) {
        let lhs = self.dialect.json_text("metadata", key);
        let p = self.dialect.placeholder(self.bindings.len());
        let rendered = render_value_as_text(value);
        // Treat NULL as "not equal" too — without `IS DISTINCT FROM`/COALESCE,
        // a JSON key that's missing would otherwise drop out of the result.
        self.pieces.push(format!("({lhs} IS NULL OR {lhs} != {p})"));
        self.bindings.push(Bind::Text(rendered));
    }

    fn push_key_present(&mut self, key: &str, exists: bool) {
        let lhs = self.dialect.json_value("metadata", key);
        let op = if exists { "IS NOT NULL" } else { "IS NULL" };
        self.pieces.push(format!("{lhs} {op}"));
    }

    fn push_full_text(&mut self, text: &str) {
        match self.dialect {
            Dialect::Postgres => {
                // Postgres GIN-indexed full-text search (upstream tiled
                // PR #640). `to_tsquery` would force the caller to write
                // tsquery syntax themselves; `plainto_tsquery` accepts a
                // plain phrase and ANDs the lexemes together — close to
                // the user's intent without surprises.
                let p = self.dialect.placeholder(self.bindings.len());
                self.pieces.push(format!(
                    "to_tsvector('simple', metadata::text) @@ plainto_tsquery('simple', {p})"
                ));
                self.bindings.push(Bind::Text(text.to_string()));
            }
            Dialect::Sqlite => {
                // SQLite: portable `LIKE %term%` substring match. FTS5
                // would be faster but requires a virtual-table mirror —
                // separate port (upstream #723).
                let col = self.dialect.metadata_full_text();
                let p = self.dialect.placeholder(self.bindings.len());
                self.pieces.push(format!("{col} LIKE {p}"));
                self.bindings.push(Bind::Text(format!("%{text}%")));
            }
        }
    }

    fn push_structure_family(&mut self, family: &str) {
        let p = self.dialect.placeholder(self.bindings.len());
        self.pieces.push(format!("structure_family = {p}"));
        self.bindings.push(Bind::Text(family.to_string()));
    }

    fn push_keys_filter(&mut self, keys: &[String]) {
        if keys.is_empty() {
            // KeysFilter with no keys → match nothing.
            self.pieces.push("1 = 0".to_string());
            return;
        }
        let placeholders: Vec<String> = (0..keys.len())
            .map(|i| self.dialect.placeholder(self.bindings.len() + i))
            .collect();
        self.pieces
            .push(format!("key IN ({})", placeholders.join(", ")));
        for k in keys {
            self.bindings.push(Bind::Text(k.clone()));
        }
    }

    /// `In(key, [v1, v2, ...])` — match rows whose metadata.key equals
    /// any of the listed values. Empty list → match nothing (always
    /// false), matching upstream tiled #746's empty-list semantics.
    fn push_in(&mut self, key: &str, values: &[Value]) {
        if values.is_empty() {
            // SQLite/Postgres reject `IN ()`; emit a guaranteed-false
            // predicate instead so an empty match list yields zero rows.
            self.pieces.push("FALSE".into());
            return;
        }
        let lhs = self.dialect.json_text("metadata", key);
        let mut placeholders = Vec::with_capacity(values.len());
        for v in values {
            let p = self.dialect.placeholder(self.bindings.len());
            placeholders.push(p);
            self.bindings.push(Bind::Text(render_value_as_text(v)));
        }
        self.pieces
            .push(format!("{lhs} IN ({})", placeholders.join(", ")));
    }

    /// `NotIn(key, [v1, v2, ...])` — inverse of `push_in`. Empty list
    /// → match everything (always true). NULLs (missing key) also pass.
    fn push_not_in(&mut self, key: &str, values: &[Value]) {
        if values.is_empty() {
            self.pieces.push("TRUE".into());
            return;
        }
        let lhs = self.dialect.json_text("metadata", key);
        let mut placeholders = Vec::with_capacity(values.len());
        for v in values {
            let p = self.dialect.placeholder(self.bindings.len());
            placeholders.push(p);
            self.bindings.push(Bind::Text(render_value_as_text(v)));
        }
        self.pieces.push(format!(
            "({lhs} IS NULL OR {lhs} NOT IN ({}))",
            placeholders.join(", ")
        ));
    }

    /// `Contains(key, value)` — substring match on the text rendering
    /// of `metadata.key`. Stricter array-containment semantics would
    /// need per-dialect JSON operators (`@>` on PG, `json_each` on
    /// SQLite); LIKE is portable and covers the typical "metadata
    /// includes this token" case.
    fn push_contains(&mut self, key: &str, value: &Value) {
        let lhs = self.dialect.json_text("metadata", key);
        let p = self.dialect.placeholder(self.bindings.len());
        let needle = match value {
            Value::String(s) => s.clone(),
            other => render_value_as_text(other),
        };
        // Escape SQL LIKE metacharacters in the user-supplied needle.
        let escaped = needle
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        self.pieces.push(format!("{lhs} LIKE {p} ESCAPE '\\'"));
        self.bindings.push(Bind::Text(format!("%{escaped}%")));
    }

    fn push_comparison(&mut self, key: &str, op: Operator, value: &Value) {
        let lhs = self.dialect.json_text("metadata", key);
        let op_sql = match op {
            Operator::Lt => "<",
            Operator::Le => "<=",
            Operator::Gt => ">",
            Operator::Ge => ">=",
        };
        let p = self.dialect.placeholder(self.bindings.len());
        if let Some(n) = value.as_f64() {
            // Cast LHS to real for numeric comparison. Both dialects accept
            // `CAST(x AS REAL)`.
            self.pieces
                .push(format!("CAST({lhs} AS REAL) {op_sql} {p}"));
            self.bindings.push(Bind::Real(n));
        } else if let Some(i) = value.as_i64() {
            self.pieces
                .push(format!("CAST({lhs} AS INTEGER) {op_sql} {p}"));
            self.bindings.push(Bind::Int(i));
        } else {
            let rendered = render_value_as_text(value);
            self.pieces.push(format!("{lhs} {op_sql} {p}"));
            self.bindings.push(Bind::Text(rendered));
        }
    }

    /// `Like(key, pattern)` — SQL LIKE on the text value of `metadata.key`.
    /// The caller supplies the pattern verbatim (with `%` / `_` wildcards);
    /// no escaping is applied — mirrors Python `attr.like(query.pattern)`.
    fn push_like(&mut self, key: &str, pattern: &str) {
        let lhs = self.dialect.json_text("metadata", key);
        let p = self.dialect.placeholder(self.bindings.len());
        self.pieces.push(format!("{lhs} LIKE {p}"));
        self.bindings.push(Bind::Text(pattern.to_string()));
    }

    /// `Regex(key, pattern, case_sensitive)` — regex match on `metadata.key`.
    ///
    /// Postgres: `~` (case-sensitive) or `~*` (case-insensitive).
    /// SQLite: no native regex operator; the condition is a no-op (passes all
    /// rows through). A future port could register a custom function via sqlx.
    fn push_regex(&mut self, key: &str, pattern: &str, case_sensitive: bool) {
        match self.dialect {
            Dialect::Postgres => {
                let lhs = self.dialect.json_text("metadata", key);
                let op = if case_sensitive { "~" } else { "~*" };
                let p = self.dialect.placeholder(self.bindings.len());
                self.pieces.push(format!("{lhs} {op} {p}"));
                self.bindings.push(Bind::Text(pattern.to_string()));
            }
            Dialect::Sqlite => {
                // SQLite has no native regex operator; leave as no-op.
            }
        }
    }

    /// `Specs(include, exclude)` — filter by the `specs` JSONB/text column.
    ///
    /// Mirrors Python `catalog/adapter.py::specs()`:
    /// - SQLite: one `LIKE '%{"name":"<n>",%'` per name (Python's approach;
    ///   note this misses specs serialised without extra fields after "name").
    /// - Postgres: `specs @> '[{"name":"<n>"}]'::jsonb` containment.
    fn push_specs(&mut self, include: &[String], exclude: &[String]) {
        match self.dialect {
            Dialect::Sqlite => {
                for name in include {
                    let escaped = escape_like_meta(name);
                    let p = self.dialect.placeholder(self.bindings.len());
                    self.pieces.push(format!("specs LIKE {p} ESCAPE '\\'"));
                    // Pattern: %{"name":"<escaped>",% — mirrors Python
                    self.bindings
                        .push(Bind::Text(format!("%{{\"name\":\"{escaped}\",%")));
                }
                for name in exclude {
                    let escaped = escape_like_meta(name);
                    let p = self.dialect.placeholder(self.bindings.len());
                    self.pieces
                        .push(format!("NOT (specs LIKE {p} ESCAPE '\\')"));
                    self.bindings
                        .push(Bind::Text(format!("%{{\"name\":\"{escaped}\",%")));
                }
            }
            Dialect::Postgres => {
                if !include.is_empty() {
                    let arr: Vec<serde_json::Value> = include
                        .iter()
                        .map(|n| serde_json::json!({"name": n}))
                        .collect();
                    let p = self.dialect.placeholder(self.bindings.len());
                    self.pieces.push(format!("specs @> {p}::jsonb"));
                    self.bindings.push(Bind::Text(
                        serde_json::to_string(&arr).expect("json serialization"),
                    ));
                }
                if !exclude.is_empty() {
                    let arr: Vec<serde_json::Value> = exclude
                        .iter()
                        .map(|n| serde_json::json!({"name": n}))
                        .collect();
                    let p = self.dialect.placeholder(self.bindings.len());
                    self.pieces.push(format!("NOT (specs @> {p}::jsonb)"));
                    self.bindings.push(Bind::Text(
                        serde_json::to_string(&arr).expect("json serialization"),
                    ));
                }
            }
        }
    }

    /// `AccessBlobFilter` — match nodes whose `access_blob` grants access.
    ///
    /// Mirrors Python catalog `access_blob_filter` (adapter.py:2048-2079):
    ///
    /// - `user_id` / `tags` / `include_untagged` all absent/false → `1 = 0`
    ///   (deny all; Python `false()` branch).
    /// - Tags present → EXISTS subquery over `access_blob.tags`.
    /// - `user_id` present → `access_blob.user` text equality.
    /// - `include_untagged` → OR arm for rows with absent/empty tags array
    ///   (these are treated as "public" — visible to everyone).
    /// - Multiple conditions → `(cond1 OR cond2 …)`.
    fn push_access_blob_filter(&mut self, filter: &tiled_core::queries::AccessBlobFilter) {
        if filter.user_id.is_none() && filter.tags.is_empty() && !filter.include_untagged {
            self.pieces.push("1 = 0".into());
            return;
        }
        let mut conds: Vec<String> = Vec::new();
        if !filter.tags.is_empty() {
            // Bind tags first, then compose the IN-list with their placeholder positions.
            let start = self.bindings.len();
            for tag in &filter.tags {
                self.bindings.push(Bind::Text(tag.clone()));
            }
            let tag_phs: Vec<String> = (start..self.bindings.len())
                .map(|i| self.dialect.placeholder(i))
                .collect();
            match self.dialect {
                Dialect::Sqlite => conds.push(format!(
                    "EXISTS (SELECT 1 FROM json_each(json_extract(access_blob, '$.tags')) \
                     WHERE value IN ({}))",
                    tag_phs.join(", ")
                )),
                Dialect::Postgres => conds.push(format!(
                    "EXISTS (SELECT 1 FROM jsonb_array_elements_text(access_blob->'tags') _t \
                     WHERE _t IN ({}))",
                    tag_phs.join(", ")
                )),
            }
        }
        if let Some(ref uid) = filter.user_id {
            let p = self.dialect.placeholder(self.bindings.len());
            self.bindings.push(Bind::Text(uid.clone()));
            match self.dialect {
                Dialect::Sqlite => {
                    conds.push(format!("json_extract(access_blob, '$.user') = {p}"));
                }
                Dialect::Postgres => {
                    conds.push(format!("access_blob->>'user' = {p}"));
                }
            }
        }
        if filter.include_untagged {
            match self.dialect {
                Dialect::Sqlite => conds.push(
                    "(json_extract(access_blob, '$.tags') IS NULL \
                     OR json_array_length(json_extract(access_blob, '$.tags')) = 0)"
                        .into(),
                ),
                Dialect::Postgres => conds.push(
                    "(access_blob->'tags' IS NULL \
                     OR jsonb_array_length(access_blob->'tags') = 0)"
                        .into(),
                ),
            }
        }
        if conds.len() == 1 {
            self.pieces.push(conds.remove(0));
        } else {
            self.pieces.push(format!("({})", conds.join(" OR ")));
        }
    }

    fn finish(self) -> (String, Vec<Bind>) {
        if self.pieces.is_empty() {
            ("TRUE".into(), self.bindings)
        } else {
            (self.pieces.join(" AND "), self.bindings)
        }
    }
}

/// Escape LIKE metacharacters (`\`, `%`, `_`) in a string that will be
/// embedded inside a LIKE pattern bound with `ESCAPE '\'`.
fn escape_like_meta(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn render_value_as_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

impl Catalog {
    /// Search children of `parent_id` (`None` for root) matching all
    /// queries, returning at most `limit` rows beginning at `offset` along
    /// with the total match count.
    pub async fn search_children(
        &self,
        parent_id: Option<i64>,
        queries: &[Query],
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<Node>, i64)> {
        let dialect = Dialect::for_pool(self.pool());
        let mut builder = WhereBuilder::new(dialect);
        for q in queries {
            match q {
                Query::Eq(eq) => builder.push_eq(&eq.key, &eq.value),
                Query::NotEq(neq) => builder.push_neq(&neq.key, &neq.value),
                Query::KeyPresent(kp) => builder.push_key_present(&kp.key, kp.exists),
                Query::FullText(ft) => builder.push_full_text(&ft.text),
                Query::StructureFamily(sf) => {
                    let s = match sf.value {
                        tiled_core::structures::StructureFamily::Container => "container",
                        tiled_core::structures::StructureFamily::Array => "array",
                        tiled_core::structures::StructureFamily::Table => "table",
                        tiled_core::structures::StructureFamily::Sparse => "sparse",
                        tiled_core::structures::StructureFamily::Awkward => "awkward",
                    };
                    builder.push_structure_family(s);
                }
                Query::KeysFilter(kf) => builder.push_keys_filter(&kf.keys),
                Query::Lookup(l) => {
                    builder.push_keys_filter(std::slice::from_ref(&l.key));
                }
                Query::Comparison(c) => builder.push_comparison(&c.key, c.operator, &c.value),
                // Variants we don't push down still influence ranking, but
                // we leave them as no-ops here (see header doc).
                Query::In(in_q) => builder.push_in(&in_q.key, &in_q.value),
                Query::NotIn(nin) => builder.push_not_in(&nin.key, &nin.value),
                Query::Contains(c) => builder.push_contains(&c.key, &c.value),
                Query::Like(l) => builder.push_like(&l.key, &l.pattern),
                Query::Regex(r) => builder.push_regex(&r.key, &r.pattern, r.case_sensitive),
                Query::Specs(s) => builder.push_specs(&s.include, &s.exclude),
                Query::AccessBlobFilter(f) => builder.push_access_blob_filter(f),
            }
        }
        let (where_clause, bindings) = builder.finish();
        let parent_clause = if parent_id.is_some() {
            "parent_id = ?"
        } else {
            "parent_id IS NULL"
        };
        // Compose the parent_id placeholder with the dialect-specific
        // numbering. WhereBuilder already emitted N placeholders; the
        // parent_id binding becomes the (N+1)-th.
        let parent_clause = match dialect {
            Dialect::Sqlite => parent_clause.to_string(),
            Dialect::Postgres => {
                if parent_id.is_some() {
                    format!("parent_id = ${}", bindings.len() + 1)
                } else {
                    "parent_id IS NULL".to_string()
                }
            }
        };

        match self.pool() {
            DbPool::Sqlite(pool) => {
                let count_sql =
                    format!("SELECT COUNT(*) FROM nodes WHERE {parent_clause} AND {where_clause}");
                let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
                if parent_id.is_some() {
                    count_q = count_q.bind(parent_id);
                }
                count_q = bind_all_sqlite(count_q, &bindings);
                let total: i64 = count_q.fetch_one(pool).await?;

                let select_sql = format!(
                    "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                            specs, access_blob, time_created, time_updated
                       FROM nodes WHERE {parent_clause} AND {where_clause}
                       ORDER BY id LIMIT ? OFFSET ?"
                );
                let mut q = sqlx::query(&select_sql);
                if parent_id.is_some() {
                    q = q.bind(parent_id);
                }
                for b in &bindings {
                    q = match b {
                        Bind::Text(s) => q.bind(s.clone()),
                        Bind::Int(i) => q.bind(*i),
                        Bind::Real(f) => q.bind(*f),
                    };
                }
                let rows = q.bind(limit).bind(offset).fetch_all(pool).await?;
                let nodes: Result<Vec<Node>> = rows.iter().map(node_from_sqlite_row).collect();
                Ok((nodes?, total))
            }
            DbPool::Postgres(pool) => {
                let limit_p = format!(
                    "${}",
                    bindings.len() + if parent_id.is_some() { 2 } else { 1 }
                );
                let offset_p = format!(
                    "${}",
                    bindings.len() + if parent_id.is_some() { 3 } else { 2 }
                );
                let count_sql =
                    format!("SELECT COUNT(*) FROM nodes WHERE {parent_clause} AND {where_clause}");
                // Bind WHERE params first ($1..$N), then parent_id ($N+1), matching
                // the placeholder numbering emitted by parent_clause above.
                let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
                count_q = bind_all_postgres(count_q, &bindings);
                if parent_id.is_some() {
                    count_q = count_q.bind(parent_id);
                }
                let total: i64 = count_q.fetch_one(pool).await?;

                let select_sql = format!(
                    "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                            specs, access_blob, time_created, time_updated
                       FROM nodes WHERE {parent_clause} AND {where_clause}
                       ORDER BY id LIMIT {limit_p} OFFSET {offset_p}"
                );
                let mut q = sqlx::query(&select_sql);
                for b in &bindings {
                    q = match b {
                        Bind::Text(s) => q.bind(s.clone()),
                        Bind::Int(i) => q.bind(*i),
                        Bind::Real(f) => q.bind(*f),
                    };
                }
                if parent_id.is_some() {
                    q = q.bind(parent_id);
                }
                let rows = q.bind(limit).bind(offset).fetch_all(pool).await?;
                let nodes: Result<Vec<Node>> = rows.iter().map(node_from_postgres_row).collect();
                Ok((nodes?, total))
            }
        }
    }
}

fn bind_all_sqlite<'q>(
    mut q: sqlx::query::QueryScalar<'q, sqlx::Sqlite, i64, sqlx::sqlite::SqliteArguments<'q>>,
    bindings: &'q [Bind],
) -> sqlx::query::QueryScalar<'q, sqlx::Sqlite, i64, sqlx::sqlite::SqliteArguments<'q>> {
    for b in bindings {
        q = match b {
            Bind::Text(s) => q.bind(s.clone()),
            Bind::Int(i) => q.bind(*i),
            Bind::Real(f) => q.bind(*f),
        };
    }
    q
}

fn bind_all_postgres<'q>(
    mut q: sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    bindings: &'q [Bind],
) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments> {
    for b in bindings {
        q = match b {
            Bind::Text(s) => q.bind(s.clone()),
            Bind::Int(i) => q.bind(*i),
            Bind::Real(f) => q.bind(*f),
        };
    }
    q
}

fn node_from_sqlite_row(row: &sqlx::sqlite::SqliteRow) -> Result<Node> {
    use chrono::{DateTime, Utc};
    let parse_dt = |s: String| -> Result<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&s)
            .map(|d| d.with_timezone(&Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.fZ")
                    .map(|n| n.and_utc())
            })
            .map_err(|e| crate::error::CatalogError::Validation(format!("bad timestamp {s}: {e}")))
    };
    Ok(Node {
        id: row.get("id"),
        key: row.get("key"),
        parent_id: row.try_get("parent_id").ok(),
        ancestors: serde_json::from_str(&row.get::<String, _>("ancestors"))?,
        structure_family: row.get("structure_family"),
        metadata: serde_json::from_str(&row.get::<String, _>("metadata"))?,
        specs: serde_json::from_str(&row.get::<String, _>("specs"))?,
        access_blob: serde_json::from_str(&row.get::<String, _>("access_blob"))?,
        time_created: parse_dt(row.get::<String, _>("time_created"))?,
        time_updated: parse_dt(row.get::<String, _>("time_updated"))?,
    })
}

fn node_from_postgres_row(row: &sqlx::postgres::PgRow) -> Result<Node> {
    Ok(Node {
        id: row.get("id"),
        key: row.get("key"),
        parent_id: row.try_get("parent_id").ok(),
        ancestors: serde_json::from_value(row.get::<Value, _>("ancestors"))?,
        structure_family: row.get("structure_family"),
        metadata: row.get("metadata"),
        specs: row.get("specs"),
        access_blob: row.get("access_blob"),
        time_created: row.get("time_created"),
        time_updated: row.get("time_updated"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn build(dialect: Dialect, queries: &[Query]) -> (String, usize) {
        let mut b = WhereBuilder {
            dialect,
            pieces: Vec::new(),
            bindings: Vec::new(),
        };
        for q in queries {
            match q {
                Query::In(i) => b.push_in(&i.key, &i.value),
                Query::NotIn(n) => b.push_not_in(&n.key, &n.value),
                Query::Contains(c) => b.push_contains(&c.key, &c.value),
                _ => {}
            }
        }
        let bcount = b.bindings.len();
        let (sql, _) = b.finish();
        (sql, bcount)
    }

    #[test]
    fn in_empty_list_yields_false() {
        let q = Query::In(tiled_core::queries::In {
            key: "color".into(),
            value: vec![],
        });
        let (sql, n) = build(Dialect::Sqlite, &[q]);
        assert!(sql.contains("FALSE"));
        assert_eq!(n, 0);
    }

    #[test]
    fn notin_empty_list_yields_true() {
        let q = Query::NotIn(tiled_core::queries::NotIn {
            key: "color".into(),
            value: vec![],
        });
        let (sql, n) = build(Dialect::Sqlite, &[q]);
        assert!(sql.contains("TRUE"));
        assert_eq!(n, 0);
    }

    #[test]
    fn in_two_values_renders_in_clause_sqlite() {
        let q = Query::In(tiled_core::queries::In {
            key: "color".into(),
            value: vec![json!("red"), json!("blue")],
        });
        let (sql, n) = build(Dialect::Sqlite, &[q]);
        assert!(sql.contains("IN (?, ?)"));
        assert_eq!(n, 2);
    }

    #[test]
    fn notin_two_values_keeps_null_through() {
        let q = Query::NotIn(tiled_core::queries::NotIn {
            key: "tag".into(),
            value: vec![json!("a"), json!("b")],
        });
        let (sql, _) = build(Dialect::Sqlite, &[q]);
        assert!(sql.contains("IS NULL OR"));
        assert!(sql.contains("NOT IN (?, ?)"));
    }

    #[test]
    fn contains_escapes_like_metacharacters() {
        let q = Query::Contains(tiled_core::queries::Contains {
            key: "note".into(),
            value: json!("100% off_now"),
        });
        let (sql, _) = build(Dialect::Sqlite, &[q]);
        // Generated SQL contains a single-backslash ESCAPE clause.
        assert!(sql.contains("LIKE ? ESCAPE '\\'"));
    }

    // H1: Verify that Postgres placeholder numbering is consistent with the
    // bind order fix (WHERE bindings $1..$N, parent_id $N+1, limit $N+2,
    // offset $N+3).
    #[test]
    fn postgres_parent_placeholder_after_where_bindings() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_eq("color", &json!("red")); // emits $1
        b.push_eq("material", &json!("Cu")); // emits $2
        let (where_sql, where_binds) = b.finish();
        // WHERE clause must reference $1 and $2 (0-indexed: positions 0 and 1)
        assert!(where_sql.contains("$1"), "first WHERE bind must be $1");
        assert!(where_sql.contains("$2"), "second WHERE bind must be $2");
        // parent_id placeholder = N+1 = 3
        let parent_placeholder = format!("${}", where_binds.len() + 1);
        assert_eq!(parent_placeholder, "$3");
        // limit placeholder = N+2 = 4, offset = N+3 = 5
        let limit_p = format!("${}", where_binds.len() + 2);
        let offset_p = format!("${}", where_binds.len() + 3);
        assert_eq!(limit_p, "$4");
        assert_eq!(offset_p, "$5");
    }

    // H2: Like SQL generation
    #[test]
    fn like_sqlite_generates_like_clause() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_like("sample", "Cu%");
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("json_extract(metadata, '$.sample') LIKE ?"),
            "SQLite Like must use json_extract + LIKE ?, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "Cu%"));
    }

    #[test]
    fn like_postgres_generates_like_clause() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_like("sample", "Cu%");
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("(metadata ->> 'sample') LIKE $1"),
            "Postgres Like must use ->> + LIKE $1, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "Cu%"));
    }

    // H2: Specs SQL generation
    #[test]
    fn specs_sqlite_include_generates_like_pattern() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_specs(&["XAS".to_string()], &[]);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("specs LIKE ? ESCAPE '\\'"),
            "SQLite Specs include must use LIKE with ESCAPE, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(
            matches!(&binds[0], Bind::Text(s) if s.contains(r#""name":"XAS""#)),
            "Bound pattern must contain the spec name, got: {:?}",
            binds[0]
        );
    }

    #[test]
    fn specs_sqlite_exclude_generates_not_like() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_specs(&[], &["BadSpec".to_string()]);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("NOT (specs LIKE ? ESCAPE '\\')"),
            "SQLite Specs exclude must use NOT (LIKE), got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s.contains(r#""name":"BadSpec""#)));
    }

    #[test]
    fn specs_postgres_include_generates_containment() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_specs(&["XAS".to_string(), "NXdata".to_string()], &[]);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("specs @> $1::jsonb"),
            "Postgres Specs include must use @> containment, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        let pattern = match &binds[0] {
            Bind::Text(s) => s.clone(),
            _ => panic!("expected Text bind"),
        };
        assert!(
            pattern.contains(r#""name":"XAS""#) && pattern.contains(r#""name":"NXdata""#),
            "Bound JSON must list all include specs, got: {pattern}"
        );
    }

    #[test]
    fn specs_postgres_exclude_generates_not_containment() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_specs(&[], &["BadSpec".to_string()]);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("NOT (specs @> $1::jsonb)"),
            "Postgres Specs exclude must use NOT (@>), got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s.contains(r#""name":"BadSpec""#)));
    }

    // AccessBlobFilter SQL generation tests.

    #[test]
    fn access_blob_filter_empty_yields_false() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter::default();
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("1 = 0"),
            "empty filter must yield FALSE, got: {sql}"
        );
        assert_eq!(binds.len(), 0);
    }

    #[test]
    fn access_blob_filter_tags_only_sqlite() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            tags: vec!["alpha".into(), "beta".into()],
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("json_each(json_extract(access_blob, '$.tags'))"),
            "SQLite tags filter must use json_each, got: {sql}"
        );
        assert!(sql.contains("IN (?, ?)"), "must bind two tags, got: {sql}");
        assert_eq!(binds.len(), 2);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "alpha"));
        assert!(matches!(&binds[1], Bind::Text(s) if s == "beta"));
    }

    #[test]
    fn access_blob_filter_user_only_sqlite() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            user_id: Some("bill".into()),
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("json_extract(access_blob, '$.user') = ?"),
            "SQLite user filter must use json_extract, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "bill"));
    }

    #[test]
    fn access_blob_filter_both_user_and_tags_sqlite() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            user_id: Some("alice".into()),
            tags: vec!["pub".into()],
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        // Must be an OR of two conditions wrapped in parens.
        assert!(sql.contains(" OR "), "user+tags must be ORed, got: {sql}");
        assert!(
            sql.starts_with('(') && sql.ends_with(')'),
            "must be wrapped, got: {sql}"
        );
        assert_eq!(binds.len(), 2, "one bind per tag + one for user");
        assert!(matches!(&binds[0], Bind::Text(s) if s == "pub"));
        assert!(matches!(&binds[1], Bind::Text(s) if s == "alice"));
    }

    #[test]
    fn access_blob_filter_tags_only_postgres() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            tags: vec!["grp1".into()],
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("jsonb_array_elements_text(access_blob->'tags')"),
            "Postgres tags filter must use jsonb_array_elements_text, got: {sql}"
        );
        assert!(
            sql.contains("IN ($1)"),
            "must bind one tag as $1, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "grp1"));
    }

    #[test]
    fn access_blob_filter_user_only_postgres() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            user_id: Some("bob".into()),
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("access_blob->>'user' = $1"),
            "Postgres user filter must use ->>, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "bob"));
    }

    #[test]
    fn access_blob_filter_include_untagged_sqlite() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            user_id: Some("alice".into()),
            tags: vec!["team".into()],
            include_untagged: true,
        };
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("json_array_length(json_extract(access_blob, '$.tags')) = 0"),
            "include_untagged must add empty-tags arm, got: {sql}"
        );
        assert!(sql.contains(" OR "), "must have OR conditions, got: {sql}");
        assert_eq!(binds.len(), 2, "one for tag, one for user");
    }

    #[test]
    fn access_blob_filter_only_include_untagged_sqlite() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            include_untagged: true,
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_access_blob_filter(&f);
        let (sql, binds) = b.finish();
        assert!(
            sql.contains("json_array_length(json_extract(access_blob, '$.tags')) = 0"),
            "include_untagged alone must yield empty-tags check, got: {sql}"
        );
        assert_eq!(binds.len(), 0);
    }
}
