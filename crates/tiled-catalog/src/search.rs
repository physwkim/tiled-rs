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
        match self {
            // Caller must double-quote escape `key`; we keep it simple by
            // assuming sane key names (Tiled metadata keys don't contain
            // `'` in practice).
            Self::Sqlite => format!("json_extract({column}, '$.{key}')"),
            Self::Postgres => format!("({column} ->> '{key}')"),
        }
    }

    /// SQL fragment that pulls JSON value at `key` as JSON (for type-safe
    /// array containment etc.).
    fn json_value(self, column: &str, key: &str) -> String {
        match self {
            Self::Sqlite => format!("json_extract({column}, '$.{key}')"),
            Self::Postgres => format!("({column} -> '{key}')"),
        }
    }

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
        self.pieces
            .push(format!("({lhs} IS NULL OR {lhs} != {p})"));
        self.bindings.push(Bind::Text(rendered));
    }

    fn push_key_present(&mut self, key: &str, exists: bool) {
        let lhs = self.dialect.json_value("metadata", key);
        let op = if exists { "IS NOT NULL" } else { "IS NULL" };
        self.pieces.push(format!("{lhs} {op}"));
    }

    fn push_full_text(&mut self, text: &str) {
        let col = self.dialect.metadata_full_text();
        let p = self.dialect.placeholder(self.bindings.len());
        // Use LIKE for portability — `metadata::text LIKE %term%`. Case-
        // insensitive matching would need lower(...) on both sides; left
        // case-sensitive for now (matches MongoCatalog).
        self.pieces.push(format!("{col} LIKE {p}"));
        self.bindings.push(Bind::Text(format!("%{text}%")));
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
            self.pieces.push(format!("CAST({lhs} AS INTEGER) {op_sql} {p}"));
            self.bindings.push(Bind::Int(i));
        } else {
            let rendered = render_value_as_text(value);
            self.pieces.push(format!("{lhs} {op_sql} {p}"));
            self.bindings.push(Bind::Text(rendered));
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
                Query::Contains(_)
                | Query::In(_)
                | Query::NotIn(_)
                | Query::Like(_)
                | Query::Regex(_)
                | Query::Specs(_)
                | Query::AccessBlobFilter(_) => {}
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
                let count_sql = format!(
                    "SELECT COUNT(*) FROM nodes WHERE {parent_clause} AND {where_clause}"
                );
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
                let limit_p = format!("${}", bindings.len() + if parent_id.is_some() { 2 } else { 1 });
                let offset_p = format!("${}", bindings.len() + if parent_id.is_some() { 3 } else { 2 });
                let count_sql = format!(
                    "SELECT COUNT(*) FROM nodes WHERE {parent_clause} AND {where_clause}"
                );
                let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
                if parent_id.is_some() {
                    count_q = count_q.bind(parent_id);
                }
                count_q = bind_all_postgres(count_q, &bindings);
                let total: i64 = count_q.fetch_one(pool).await?;

                let select_sql = format!(
                    "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                            specs, access_blob, time_created, time_updated
                       FROM nodes WHERE {parent_clause} AND {where_clause}
                       ORDER BY id LIMIT {limit_p} OFFSET {offset_p}"
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
            .map_err(|e| {
                crate::error::CatalogError::Validation(format!("bad timestamp {s}: {e}"))
            })
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
