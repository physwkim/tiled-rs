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
use tiled_core::schemas::SortDirection;

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
    ///
    /// `key` may be dotted (`a.b`) to address a nested path. SQLite uses a
    /// `$.a.b` JSON path; Postgres splits the key and uses the `#>>` path
    /// operator (`metadata #>> '{a,b}'`) so a dotted key is a genuine nested
    /// lookup, not a top-level key literally named `"a.b"`. Mirrors Python
    /// `orm.Node.metadata_[key.split(".")]` rendered as `#>>`/`.astext`
    /// (catalog/adapter.py:1971-1972, 1990-1991, 2006-2007).
    fn json_text(self, column: &str, key: &str) -> String {
        match self {
            Self::Sqlite => {
                let safe = sanitize_json_key(key);
                format!("json_extract({column}, '$.{safe}')")
            }
            Self::Postgres => format!("({column} #>> {})", pg_path_array(key)),
        }
    }

    /// SQL fragment that pulls JSON value at `key` as JSON (for type-safe
    /// array containment, presence checks, etc.).
    ///
    /// Same dotted-key path handling as [`Dialect::json_text`], but returns
    /// the JSON value: SQLite `json_extract`, Postgres `#>` path operator
    /// (`metadata #> '{a,b}'`) — mirrors Python `metadata_[keys]` rendered as
    /// `#>` (catalog/adapter.py:2147-2149).
    fn json_value(self, column: &str, key: &str) -> String {
        match self {
            Self::Sqlite => {
                let safe = sanitize_json_key(key);
                format!("json_extract({column}, '$.{safe}')")
            }
            Self::Postgres => format!("({column} #> {})", pg_path_array(key)),
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

/// Render a (possibly dotted) JSON key as a Postgres text-array path literal
/// for the `#>` / `#>>` operators: `"a.b"` → `'{a,b}'`. Each segment is
/// sanitized like [`sanitize_json_key`] before splicing into SQL. Mirrors
/// Python's `query.key.split(".")` path access (catalog/adapter.py).
fn pg_path_array(key: &str) -> String {
    let segments: Vec<String> = key.split('.').map(sanitize_json_key).collect();
    format!("'{{{}}}'", segments.join(","))
}

/// Build the nested JSON document Python's `key_array_to_json` produces for a
/// (possibly dotted) key: `"x.y" , 1` → `{"x": {"y": 1}}`. The result is bound
/// as a JSONB parameter for the `@>` containment operator, so the key segments
/// are not spliced into SQL and need no sanitizing (adapter.py:2349-2369).
fn key_path_to_json(key: &str, value: &Value) -> Value {
    let mut acc = value.clone();
    for segment in key.split('.').rev() {
        let mut obj = serde_json::Map::new();
        obj.insert(segment.to_string(), acc);
        acc = Value::Object(obj);
    }
    acc
}

impl Dialect {
    fn metadata_full_text(self) -> &'static str {
        match self {
            Self::Sqlite => "metadata",
            Self::Postgres => "metadata::text",
        }
    }

    /// Build the `ORDER BY` clause body (without the leading `ORDER BY`),
    /// mirroring Python `construct_order_by_clauses` (adapter.py:1891-1923):
    ///
    /// - each sort key becomes a column / JSON-path clause, descending keys
    ///   get a `DESC` suffix;
    /// - the empty key `""` is the default-direction sentinel — it applies only
    ///   to the trailing `id` tiebreaker, not a column of its own;
    /// - the strictly-monotonic `id` column is always appended last so the
    ///   ordering is deterministic (and a sufficient keyset-pagination cursor).
    ///
    /// No bind parameters are produced (keys are sanitized and spliced;
    /// direction is a literal), so this can be appended after the WHERE
    /// bindings without disturbing placeholder numbering.
    fn order_by(self, sorting: &[(String, SortDirection)]) -> String {
        let mut clauses: Vec<String> = Vec::new();
        let mut default_desc = false;
        for (key, dir) in sorting {
            if key.is_empty() {
                default_desc = matches!(dir, SortDirection::Descending);
                continue;
            }
            let col = if key == "id" {
                // _STANDARD_SORT_KEYS maps the logical "id" to the `key` column
                // (the node's name); the real `id` is reserved for the
                // tiebreaker below.
                "key".to_string()
            } else {
                // Bare ("color") or namespaced ("metadata.color"): strip the
                // optional "metadata." prefix, then address the JSON path.
                let k = key.strip_prefix("metadata.").unwrap_or(key);
                self.json_value("metadata", k)
            };
            if matches!(dir, SortDirection::Descending) {
                clauses.push(format!("{col} DESC"));
            } else {
                clauses.push(col);
            }
        }
        clauses.push(if default_desc {
            "id DESC".to_string()
        } else {
            "id".to_string()
        });
        clauses.join(", ")
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
        match self.dialect {
            Dialect::Sqlite => {
                // `json_extract` returns the native storage class, and
                // `value_to_bind` binds a matching type, so `lhs = ?` works
                // for any JSON scalar.
                let lhs = self.dialect.json_text("metadata", key);
                let p = self.dialect.placeholder(self.bindings.len());
                self.pieces.push(format!("{lhs} = {p}"));
                self.bindings.push(value_to_bind(value));
            }
            Dialect::Postgres => {
                // `(metadata ->> 'k') = $1::int8` has no operator (text vs
                // int8) and the query fails. Use JSONB containment instead —
                // type-safe and GIN-indexable, matching Python
                // (adapter.py:1976-1982).
                let pred = self.pg_containment(key, value);
                self.pieces.push(pred);
            }
        }
    }

    fn push_neq(&mut self, key: &str, value: &Value) {
        match self.dialect {
            Dialect::Sqlite => {
                let lhs = self.dialect.json_text("metadata", key);
                let p = self.dialect.placeholder(self.bindings.len());
                // Treat NULL as "not equal" too — without `IS DISTINCT
                // FROM`/COALESCE, a JSON key that's missing would otherwise
                // drop out of the result.
                self.pieces.push(format!("({lhs} IS NULL OR {lhs} != {p})"));
                self.bindings.push(value_to_bind(value));
            }
            Dialect::Postgres => {
                // Compare jsonb-to-jsonb (`#> ... != $1::jsonb`); a text `#>>`
                // against a typed int8/float8 bind has no operator. Mirrors
                // Python `ne(metadata_[keys], type_coerce(value, JSONB))`
                // (adapter.py:1983-1984). Missing key (NULL) stays "not equal"
                // per the SQLite arm's semantics above.
                let lhs = self.dialect.json_value("metadata", key);
                let p = self.dialect.placeholder(self.bindings.len());
                self.pieces
                    .push(format!("({lhs} IS NULL OR {lhs} != {p}::jsonb)"));
                self.bindings.push(Bind::Text(
                    serde_json::to_string(value).expect("json serialization"),
                ));
            }
        }
    }

    fn push_key_present(&mut self, key: &str, exists: bool) {
        let op = if exists { "IS NOT NULL" } else { "IS NULL" };
        let lhs = match self.dialect {
            // SQLite: the `->` operator returns JSON 'null' (text) for a
            // present-but-null key and SQL NULL only for an absent key, so a
            // present null reports present — matching Python
            // `metadata_.op("->")("$."+key) != None` (adapter.py:2144-2145).
            // `json_extract` would coerce a JSON null to SQL NULL and wrongly
            // report the key absent.
            Dialect::Sqlite => format!("(metadata -> '$.{}')", sanitize_json_key(key)),
            // Postgres: `#>` path access returns jsonb 'null' for a present
            // null and SQL NULL for absent — already correct
            // (adapter.py:2147-2149).
            Dialect::Postgres => self.dialect.json_value("metadata", key),
        };
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

    /// Bind one nested-key JSON document and return the Postgres containment
    /// predicate `metadata @> $N::jsonb` (no surrounding parens). Mirrors
    /// Python `metadata_.op("@>")(key_array_to_json(keys, value))`
    /// (adapter.py:1977-1982, 2109, 2120).
    fn pg_containment(&mut self, key: &str, value: &Value) -> String {
        let p = self.dialect.placeholder(self.bindings.len());
        let doc = key_path_to_json(key, value);
        self.bindings.push(Bind::Text(
            serde_json::to_string(&doc).expect("json serialization"),
        ));
        format!("metadata @> {p}::jsonb")
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
        match self.dialect {
            Dialect::Sqlite => {
                let lhs = self.dialect.json_text("metadata", key);
                let mut placeholders = Vec::with_capacity(values.len());
                for v in values {
                    let p = self.dialect.placeholder(self.bindings.len());
                    placeholders.push(p);
                    self.bindings.push(value_to_bind(v));
                }
                self.pieces
                    .push(format!("{lhs} IN ({})", placeholders.join(", ")));
            }
            Dialect::Postgres => {
                // `(metadata ->> 'k') IN ($1::int8, ...)` has no operator
                // (text vs int8). Use an OR of JSONB containments — type-safe,
                // GIN-indexable, matching Python (adapter.py:2107-2112).
                let mut preds = Vec::with_capacity(values.len());
                for v in values {
                    preds.push(self.pg_containment(key, v));
                }
                self.pieces.push(format!("({})", preds.join(" OR ")));
            }
        }
    }

    /// `NotIn(key, [v1, v2, ...])` — inverse of `push_in`. Empty list
    /// → match everything (always true).
    ///
    /// Missing-key handling is dialect-specific, matching Python: SQLite
    /// `attr.not_in(...)` yields `NULL NOT IN (...)` → NULL → the row is
    /// EXCLUDED (adapter.py:2096); Postgres `NOT (OR of @>)` is true for a
    /// missing key → the row is INCLUDED (adapter.py:2117-2124).
    fn push_not_in(&mut self, key: &str, values: &[Value]) {
        if values.is_empty() {
            self.pieces.push("TRUE".into());
            return;
        }
        match self.dialect {
            Dialect::Sqlite => {
                let lhs = self.dialect.json_text("metadata", key);
                let mut placeholders = Vec::with_capacity(values.len());
                for v in values {
                    let p = self.dialect.placeholder(self.bindings.len());
                    placeholders.push(p);
                    self.bindings.push(value_to_bind(v));
                }
                // No `IS NULL OR` arm: a missing key (json_extract → NULL)
                // makes `NULL NOT IN (...)` evaluate to NULL, excluding the
                // row — matching Python SQLite `attr.not_in`.
                self.pieces
                    .push(format!("{lhs} NOT IN ({})", placeholders.join(", ")));
            }
            Dialect::Postgres => {
                // `NOT (OR of JSONB containments)` — type-safe and includes
                // missing-key rows (containment is false → NOT is true),
                // matching Python (adapter.py:2117-2124).
                let mut preds = Vec::with_capacity(values.len());
                for v in values {
                    preds.push(self.pg_containment(key, v));
                }
                self.pieces.push(format!("NOT ({})", preds.join(" OR ")));
            }
        }
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
    ///   AND no `user` key — genuinely public, visible to everyone. A
    ///   user-owned blob `{"user": id}` carries no `tags` key but is NOT
    ///   public; it reaches a caller only through the `user_id` arm.
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
            // "Untagged-public" must exclude user-owned rows: a blob like
            // `{"user": "<uuid>"}` has no `tags` key, so the tags-absent test
            // alone would match — and leak — every user-owned node to every
            // caller. Require the row to carry no `user` key as well, so this
            // arm means strictly "no tags AND no owner" (genuinely public).
            // User-owned rows reach the caller only through the `user_id` arm.
            match self.dialect {
                Dialect::Sqlite => conds.push(
                    "((json_extract(access_blob, '$.tags') IS NULL \
                     OR json_array_length(json_extract(access_blob, '$.tags')) = 0) \
                     AND json_extract(access_blob, '$.user') IS NULL)"
                        .into(),
                ),
                Dialect::Postgres => conds.push(
                    "((access_blob->'tags' IS NULL \
                     OR jsonb_array_length(access_blob->'tags') = 0) \
                     AND access_blob->'user' IS NULL)"
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

/// Produce a type-aware `Bind` for a JSON value so SQLite compares
/// `json_extract` results by storage class rather than text coercion.
///
/// SQLite's `json_extract` returns the *native* storage class (INTEGER for
/// JSON integers and booleans, REAL for JSON floats, TEXT for strings).
/// A `Bind::Text` binding never matches an INTEGER/REAL value under SQLite's
/// no-affinity comparison rules — `json_extract('{"x":5}','$.x') = '5'` is
/// FALSE.  Postgres is unaffected because `->>` always returns TEXT.
fn value_to_bind(v: &Value) -> Bind {
    if let Some(i) = v.as_i64() {
        return Bind::Int(i);
    }
    if let Some(f) = v.as_f64() {
        return Bind::Real(f);
    }
    if let Some(b) = v.as_bool() {
        // SQLite stores JSON booleans as INTEGER 1/0 via json_extract.
        return Bind::Int(if b { 1 } else { 0 });
    }
    Bind::Text(render_value_as_text(v))
}

/// Translate every `Query` filter into `WhereBuilder` pieces. Shared by the
/// offset ([`Catalog::search_children`]) and keyset
/// ([`Catalog::search_children_cursor`]) paths so both apply an identical
/// WHERE clause.
fn apply_queries(builder: &mut WhereBuilder, queries: &[Query]) {
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
                    tiled_core::structures::StructureFamily::Ragged => "ragged",
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
}

/// Whether the trailing `id` tiebreaker (and therefore the keyset cursor
/// direction) is descending for this sort. Mirrors `Dialect::order_by`: the
/// empty-key sentinel carries the default direction.
fn default_sort_descending(sorting: &[(String, SortDirection)]) -> bool {
    sorting
        .iter()
        .rfind(|(k, _)| k.is_empty())
        .map(|(_, d)| matches!(d, SortDirection::Descending))
        .unwrap_or(false)
}

impl Catalog {
    /// Keyset (cursor) page of children of `parent_id`, valid for the default
    /// sort order only. Returns the page rows, the total match count, and the
    /// cursor (the last row's id) for the next page when more rows remain.
    ///
    /// Mirrors Python `CatalogContainerAdapter.keys_page`/`items_page` +
    /// `_apply_cursor_pagination` (catalog/adapter.py:1341-1412): order by the
    /// strictly-monotonic `id` (ASC or DESC), filter `id > cursor` (ASC) /
    /// `id < cursor` (DESC), and fetch one extra row to detect whether a
    /// following page exists. The count is the full filtered match count (for
    /// the response `meta`), independent of the page window.
    pub async fn search_children_cursor(
        &self,
        parent_id: Option<i64>,
        queries: &[Query],
        sorting: &[(String, SortDirection)],
        cursor: Option<i64>,
        limit: i64,
    ) -> Result<(Vec<Node>, i64, Option<i64>)> {
        let dialect = Dialect::for_pool(self.pool());
        let mut builder = WhereBuilder::new(dialect);
        apply_queries(&mut builder, queries);
        let (where_clause, bindings) = builder.finish();
        let order_by = dialect.order_by(sorting);
        let cursor_op = if default_sort_descending(sorting) {
            "<"
        } else {
            ">"
        };
        // Fetch one extra row to detect whether a following page exists
        // (Python `_apply_cursor_pagination` uses `limit + 1`). A non-positive
        // limit asks for an empty page; clamp the fetch to >= 0.
        let fetch = limit.max(0).saturating_add(1);

        let parent_present = parent_id.is_some();
        let cursor_present = cursor.is_some();
        let n = bindings.len();

        match self.pool() {
            DbPool::Sqlite(pool) => {
                let parent_clause = if parent_present {
                    "parent_id = ?"
                } else {
                    "parent_id IS NULL"
                };
                // Count of all matching rows (no cursor/limit) for `meta`.
                let count_sql =
                    format!("SELECT COUNT(*) FROM nodes WHERE {parent_clause} AND {where_clause}");
                let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
                if parent_present {
                    count_q = count_q.bind(parent_id);
                }
                count_q = bind_all_sqlite(count_q, &bindings);
                let total: i64 = count_q.fetch_one(pool).await?;

                // Keyset window: parent + filters + (optional) `id <op> cursor`.
                let cursor_clause = if cursor_present {
                    format!(" AND id {cursor_op} ?")
                } else {
                    String::new()
                };
                let select_sql = format!(
                    "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                            specs, access_blob, time_created, time_updated
                       FROM nodes WHERE {parent_clause} AND {where_clause}{cursor_clause}
                       ORDER BY {order_by} LIMIT ?"
                );
                let mut q = sqlx::query(&select_sql);
                if parent_present {
                    q = q.bind(parent_id);
                }
                for b in &bindings {
                    q = match b {
                        Bind::Text(s) => q.bind(s.clone()),
                        Bind::Int(i) => q.bind(*i),
                        Bind::Real(f) => q.bind(*f),
                    };
                }
                if cursor_present {
                    q = q.bind(cursor);
                }
                let rows = q.bind(fetch).fetch_all(pool).await?;
                let mut nodes: Vec<Node> = rows
                    .iter()
                    .map(node_from_sqlite_row)
                    .collect::<Result<_>>()?;
                let next_cursor = trim_to_page(&mut nodes, limit);
                Ok((nodes, total, next_cursor))
            }
            DbPool::Postgres(pool) => {
                // Placeholder numbering: WHERE binds $1..$N, then parent_id,
                // then cursor, then limit — matching the bind order below.
                let parent_clause = if parent_present {
                    format!("parent_id = ${}", n + 1)
                } else {
                    "parent_id IS NULL".to_string()
                };
                let count_sql =
                    format!("SELECT COUNT(*) FROM nodes WHERE {parent_clause} AND {where_clause}");
                let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
                count_q = bind_all_postgres(count_q, &bindings);
                if parent_present {
                    count_q = count_q.bind(parent_id);
                }
                let total: i64 = count_q.fetch_one(pool).await?;

                let np = n + if parent_present { 1 } else { 0 };
                let cursor_clause = if cursor_present {
                    format!(" AND id {cursor_op} ${}", np + 1)
                } else {
                    String::new()
                };
                let nc = np + if cursor_present { 1 } else { 0 };
                let limit_p = format!("${}", nc + 1);
                let select_sql = format!(
                    "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                            specs, access_blob, time_created, time_updated
                       FROM nodes WHERE {parent_clause} AND {where_clause}{cursor_clause}
                       ORDER BY {order_by} LIMIT {limit_p}"
                );
                let mut q = sqlx::query(&select_sql);
                for b in &bindings {
                    q = match b {
                        Bind::Text(s) => q.bind(s.clone()),
                        Bind::Int(i) => q.bind(*i),
                        Bind::Real(f) => q.bind(*f),
                    };
                }
                if parent_present {
                    q = q.bind(parent_id);
                }
                if cursor_present {
                    q = q.bind(cursor);
                }
                let rows = q.bind(fetch).fetch_all(pool).await?;
                let mut nodes: Vec<Node> = rows
                    .iter()
                    .map(node_from_postgres_row)
                    .collect::<Result<_>>()?;
                let next_cursor = trim_to_page(&mut nodes, limit);
                Ok((nodes, total, next_cursor))
            }
        }
    }

    /// Search children of `parent_id` (`None` for root) matching all
    /// queries, returning at most `limit` rows beginning at `offset` along
    /// with the total match count.
    pub async fn search_children(
        &self,
        parent_id: Option<i64>,
        queries: &[Query],
        sorting: &[(String, SortDirection)],
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<Node>, i64)> {
        let dialect = Dialect::for_pool(self.pool());
        let mut builder = WhereBuilder::new(dialect);
        apply_queries(&mut builder, queries);
        let (where_clause, bindings) = builder.finish();
        // ORDER BY is parameter-free (sanitized keys + literal direction), so
        // it does not affect the LIMIT/OFFSET placeholder numbering below.
        let order_by = dialect.order_by(sorting);
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
                       ORDER BY {order_by} LIMIT ? OFFSET ?"
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
                       ORDER BY {order_by} LIMIT {limit_p} OFFSET {offset_p}"
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

    /// Distinct metadata-key values, structure families, and/or specs among the
    /// direct children of `parent_id` (`None` = root), scoped by the same search
    /// `queries` as a listing. Mirrors Python `CatalogAdapter.get_distinct`
    /// (catalog/adapter.py:647-698) and `format_distinct_result` (2331-2338):
    /// each facet is a `GROUP BY` over the column with `COUNT(col)` — so the
    /// missing-key group reports count 0 — and `counts=false` omits the count.
    pub async fn get_distinct(
        &self,
        parent_id: Option<i64>,
        queries: &[Query],
        metadata_keys: &[String],
        structure_families: bool,
        specs: bool,
        counts: bool,
    ) -> Result<tiled_core::schemas::GetDistinctResponse> {
        let dialect = Dialect::for_pool(self.pool());
        let mut builder = WhereBuilder::new(dialect);
        apply_queries(&mut builder, queries);
        let (where_clause, bindings) = builder.finish();
        // parent_id placeholder is numbered after the WHERE bindings, exactly
        // as in `search_children`: SQLite binds it first (positional `?`),
        // Postgres last (`$N+1`).
        let parent_clause = match dialect {
            Dialect::Sqlite => {
                if parent_id.is_some() {
                    "parent_id = ?".to_string()
                } else {
                    "parent_id IS NULL".to_string()
                }
            }
            Dialect::Postgres => {
                if parent_id.is_some() {
                    format!("parent_id = ${}", bindings.len() + 1)
                } else {
                    "parent_id IS NULL".to_string()
                }
            }
        };

        let mut resp = tiled_core::schemas::GetDistinctResponse::default();

        if !metadata_keys.is_empty() {
            let mut map = std::collections::HashMap::new();
            for key in metadata_keys {
                // GROUP BY the JSON value (Python `metadata_[keys]` → `#>`),
                // SELECT it as JSON text so any storage class round-trips back
                // to its original JSON type: SQLite `json_quote`, Postgres
                // `::text`.
                let group_expr = dialect.json_value("metadata", key);
                let value_expr = match dialect {
                    Dialect::Sqlite => format!("json_quote({group_expr})"),
                    Dialect::Postgres => format!("({group_expr})::text"),
                };
                let rows = self
                    .run_distinct_group(
                        &parent_clause,
                        &where_clause,
                        &bindings,
                        parent_id,
                        &value_expr,
                        &group_expr,
                        counts,
                    )
                    .await?;
                map.insert(key.clone(), rows);
            }
            resp.metadata = Some(map);
        }

        if structure_families {
            let value_expr = match dialect {
                Dialect::Sqlite => "json_quote(structure_family)".to_string(),
                Dialect::Postgres => "to_jsonb(structure_family)::text".to_string(),
            };
            resp.structure_families = Some(
                self.run_distinct_group(
                    &parent_clause,
                    &where_clause,
                    &bindings,
                    parent_id,
                    &value_expr,
                    "structure_family",
                    counts,
                )
                .await?,
            );
        }

        if specs {
            // `specs` is already a JSON array column; for Postgres jsonb cast to
            // text, for SQLite it is stored as JSON text already.
            let value_expr = match dialect {
                Dialect::Sqlite => "specs".to_string(),
                Dialect::Postgres => "specs::text".to_string(),
            };
            resp.specs = Some(
                self.run_distinct_group(
                    &parent_clause,
                    &where_clause,
                    &bindings,
                    parent_id,
                    &value_expr,
                    "specs",
                    counts,
                )
                .await?,
            );
        }

        Ok(resp)
    }

    /// Run one facet's `GROUP BY` query and decode `(value, count)` rows.
    /// `value_expr` must yield JSON text (parsed back to a `serde_json::Value`);
    /// `group_expr` is the parameter-free column expression grouped + counted.
    #[allow(clippy::too_many_arguments)]
    async fn run_distinct_group(
        &self,
        parent_clause: &str,
        where_clause: &str,
        bindings: &[Bind],
        parent_id: Option<i64>,
        value_expr: &str,
        group_expr: &str,
        counts: bool,
    ) -> Result<Vec<tiled_core::schemas::DistinctValueInfo>> {
        let select_cols = if counts {
            format!("{value_expr} AS v, COUNT({group_expr}) AS c")
        } else {
            format!("{value_expr} AS v")
        };
        let sql = format!(
            "SELECT {select_cols} FROM nodes \
             WHERE {parent_clause} AND {where_clause} GROUP BY {group_expr}"
        );
        let mut out = Vec::new();
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let mut q = sqlx::query(&sql);
                if parent_id.is_some() {
                    q = q.bind(parent_id);
                }
                for b in bindings {
                    q = match b {
                        Bind::Text(s) => q.bind(s.clone()),
                        Bind::Int(i) => q.bind(*i),
                        Bind::Real(f) => q.bind(*f),
                    };
                }
                for row in &q.fetch_all(pool).await? {
                    let value = parse_distinct_value(row.try_get::<Option<String>, _>("v")?);
                    let count = if counts {
                        Some(row.try_get::<i64, _>("c")?)
                    } else {
                        None
                    };
                    out.push(tiled_core::schemas::DistinctValueInfo { value, count });
                }
            }
            DbPool::Postgres(pool) => {
                let mut q = sqlx::query(&sql);
                for b in bindings {
                    q = match b {
                        Bind::Text(s) => q.bind(s.clone()),
                        Bind::Int(i) => q.bind(*i),
                        Bind::Real(f) => q.bind(*f),
                    };
                }
                if parent_id.is_some() {
                    q = q.bind(parent_id);
                }
                for row in &q.fetch_all(pool).await? {
                    let value = parse_distinct_value(row.try_get::<Option<String>, _>("v")?);
                    let count = if counts {
                        Some(row.try_get::<i64, _>("c")?)
                    } else {
                        None
                    };
                    out.push(tiled_core::schemas::DistinctValueInfo { value, count });
                }
            }
        }
        Ok(out)
    }
}

/// Parse a distinct facet's JSON-text value back into a `serde_json::Value`.
/// A SQL NULL (or unparseable text) becomes JSON `null`, matching the
/// missing-key group Python returns as `{"value": null}`.
fn parse_distinct_value(text: Option<String>) -> serde_json::Value {
    match text {
        Some(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
        None => serde_json::Value::Null,
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

/// Trim a keyset fetch (which pulled `limit + 1` rows to peek ahead) down to
/// the page size, returning the next-page cursor — the last kept row's id —
/// when an extra row was present (mirrors Python popping the extra row and
/// taking `rows[-1].id`). A non-positive `limit` yields an empty page.
fn trim_to_page(nodes: &mut Vec<Node>, limit: i64) -> Option<i64> {
    if limit <= 0 {
        nodes.clear();
        return None;
    }
    let limit = limit as usize;
    if nodes.len() > limit {
        nodes.truncate(limit);
        nodes.last().map(|n| n.id)
    } else {
        None
    }
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

    // F-O: SQLite NotIn excludes missing-key rows (no `IS NULL OR` arm),
    // matching Python SQLite `attr.not_in` where `NULL NOT IN (...)` → NULL.
    #[test]
    fn notin_two_values_sqlite_excludes_missing_key() {
        let q = Query::NotIn(tiled_core::queries::NotIn {
            key: "tag".into(),
            value: vec![json!("a"), json!("b")],
        });
        let (sql, _) = build(Dialect::Sqlite, &[q]);
        assert!(
            !sql.contains("IS NULL OR"),
            "must not keep missing-key rows"
        );
        assert_eq!(sql, "json_extract(metadata, '$.tag') NOT IN (?, ?)");
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

    // H1: value_to_bind produces type-aware binds so SQLite json_extract
    // comparisons match by storage class, not text coercion.
    #[test]
    fn value_to_bind_integer_yields_int() {
        assert!(matches!(value_to_bind(&json!(42)), Bind::Int(42)));
    }

    #[test]
    fn value_to_bind_float_yields_real() {
        let b = value_to_bind(&json!(2.5));
        assert!(matches!(b, Bind::Real(f) if (f - 2.5).abs() < 1e-10));
    }

    #[test]
    fn value_to_bind_bool_yields_int() {
        assert!(matches!(value_to_bind(&json!(true)), Bind::Int(1)));
        assert!(matches!(value_to_bind(&json!(false)), Bind::Int(0)));
    }

    #[test]
    fn value_to_bind_string_yields_text() {
        assert!(matches!(value_to_bind(&json!("hello")), Bind::Text(s) if s == "hello"));
    }

    #[test]
    fn push_eq_numeric_binds_int_not_text() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_eq("count", &json!(5));
        let (_, binds) = b.finish();
        assert_eq!(binds.len(), 1);
        assert!(
            matches!(&binds[0], Bind::Int(5)),
            "push_eq with integer must bind Bind::Int, got: {:?}",
            binds[0]
        );
    }

    #[test]
    fn push_neq_numeric_binds_int_not_text() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_neq("count", &json!(5));
        let (_, binds) = b.finish();
        assert!(
            matches!(&binds[0], Bind::Int(5)),
            "push_neq with integer must bind Bind::Int, got: {:?}",
            binds[0]
        );
    }

    #[test]
    fn push_in_numeric_binds_int_not_text() {
        let q = Query::In(tiled_core::queries::In {
            key: "scan_id".into(),
            value: vec![json!(1), json!(2), json!(3)],
        });
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_in("scan_id", &[json!(1), json!(2), json!(3)]);
        let (_, binds) = b.finish();
        assert_eq!(binds.len(), 3);
        for bind in &binds {
            assert!(
                matches!(bind, Bind::Int(_)),
                "push_in integers must all bind as Bind::Int, got: {:?}",
                bind
            );
        }
        // suppress unused variable warning
        let _ = q;
    }

    #[test]
    fn push_not_in_numeric_binds_int_not_text() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_not_in("scan_id", &[json!(10), json!(20)]);
        let (_, binds) = b.finish();
        assert_eq!(binds.len(), 2);
        for bind in &binds {
            assert!(
                matches!(bind, Bind::Int(_)),
                "push_not_in integers must all bind as Bind::Int, got: {:?}",
                bind
            );
        }
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
            sql.contains("(metadata #>> '{sample}') LIKE $1"),
            "Postgres Like must use #>> path + LIKE $1, got: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "Cu%"));
    }

    // F-D: Postgres dotted/nested metadata keys must use the `#>>` / `#>`
    // path operators, not a single top-level key literally named "a.b".
    #[test]
    fn json_text_postgres_dotted_key_uses_path_operator() {
        assert_eq!(
            Dialect::Postgres.json_text("metadata", "instrument.detector.id"),
            "(metadata #>> '{instrument,detector,id}')"
        );
    }

    #[test]
    fn json_value_postgres_dotted_key_uses_path_operator() {
        assert_eq!(
            Dialect::Postgres.json_value("metadata", "a.b"),
            "(metadata #> '{a,b}')"
        );
    }

    #[test]
    fn json_text_postgres_single_key_uses_path_operator() {
        // Even a non-dotted key uses the uniform path form.
        assert_eq!(
            Dialect::Postgres.json_text("metadata", "color"),
            "(metadata #>> '{color}')"
        );
    }

    #[test]
    fn json_text_sqlite_dotted_key_uses_json_path() {
        // SQLite path access via `$.a.b` was already correct.
        assert_eq!(
            Dialect::Sqlite.json_text("metadata", "a.b"),
            "json_extract(metadata, '$.a.b')"
        );
    }

    // F-E: Postgres non-string Eq/In/NotIn/NotEq must avoid the
    // text-vs-typed-bind type error (`(metadata ->> 'k') = $1::int8` has no
    // operator) via JSONB containment / jsonb comparison.
    #[test]
    fn push_eq_postgres_uses_containment() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_eq("count", &json!(5));
        let (sql, binds) = b.finish();
        assert_eq!(sql, "metadata @> $1::jsonb");
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == r#"{"count":5}"#));
    }

    #[test]
    fn push_eq_postgres_string_uses_containment() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_eq("material", &json!("Cu"));
        let (sql, binds) = b.finish();
        assert_eq!(sql, "metadata @> $1::jsonb");
        assert!(matches!(&binds[0], Bind::Text(s) if s == r#"{"material":"Cu"}"#));
    }

    #[test]
    fn push_eq_postgres_dotted_key_nests_containment_json() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_eq("a.b", &json!("red"));
        let (_, binds) = b.finish();
        assert!(matches!(&binds[0], Bind::Text(s) if s == r#"{"a":{"b":"red"}}"#));
    }

    #[test]
    fn push_in_postgres_uses_containment_or() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_in("scan_id", &[json!(1), json!(2)]);
        let (sql, binds) = b.finish();
        assert_eq!(sql, "(metadata @> $1::jsonb OR metadata @> $2::jsonb)");
        assert_eq!(binds.len(), 2);
        assert!(matches!(&binds[0], Bind::Text(s) if s == r#"{"scan_id":1}"#));
        assert!(matches!(&binds[1], Bind::Text(s) if s == r#"{"scan_id":2}"#));
    }

    #[test]
    fn push_not_in_postgres_uses_not_containment_or() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_not_in("scan_id", &[json!(1), json!(2)]);
        let (sql, binds) = b.finish();
        assert_eq!(sql, "NOT (metadata @> $1::jsonb OR metadata @> $2::jsonb)");
        assert_eq!(binds.len(), 2);
    }

    #[test]
    fn push_neq_postgres_uses_jsonb_comparison() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_neq("count", &json!(5));
        let (sql, binds) = b.finish();
        assert_eq!(
            sql,
            "((metadata #> '{count}') IS NULL OR (metadata #> '{count}') != $1::jsonb)"
        );
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], Bind::Text(s) if s == "5"));
    }

    // SQLite arms keep type-aware native comparison (regression guard).
    #[test]
    fn push_eq_sqlite_still_uses_json_extract_equality() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_eq("count", &json!(5));
        let (sql, _) = b.finish();
        assert_eq!(sql, "json_extract(metadata, '$.count') = ?");
    }

    // F-Q: SQLite KeyPresent must use the `->` operator so a present-but-null
    // key reports present (json_extract would coerce JSON null → SQL NULL).
    #[test]
    fn push_key_present_sqlite_uses_arrow_operator() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_key_present("color", true);
        let (sql, _) = b.finish();
        assert_eq!(sql, "(metadata -> '$.color') IS NOT NULL");
    }

    #[test]
    fn push_key_present_sqlite_absent_uses_is_null() {
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_key_present("color", false);
        let (sql, _) = b.finish();
        assert_eq!(sql, "(metadata -> '$.color') IS NULL");
    }

    #[test]
    fn push_key_present_postgres_uses_hash_arrow_path() {
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_key_present("a.b", true);
        let (sql, _) = b.finish();
        assert_eq!(sql, "(metadata #> '{a,b}') IS NOT NULL");
    }

    // F-C: ORDER BY construction mirrors Python construct_order_by_clauses.
    #[test]
    fn order_by_empty_defaults_to_id_asc() {
        assert_eq!(Dialect::Sqlite.order_by(&[]), "id");
    }

    #[test]
    fn order_by_single_metadata_key_asc() {
        let s = [("color".to_string(), SortDirection::Ascending)];
        assert_eq!(
            Dialect::Sqlite.order_by(&s),
            "json_extract(metadata, '$.color'), id"
        );
    }

    #[test]
    fn order_by_metadata_key_desc() {
        let s = [("color".to_string(), SortDirection::Descending)];
        assert_eq!(
            Dialect::Sqlite.order_by(&s),
            "json_extract(metadata, '$.color') DESC, id"
        );
    }

    #[test]
    fn order_by_strips_metadata_prefix() {
        let s = [("metadata.color".to_string(), SortDirection::Ascending)];
        assert_eq!(
            Dialect::Sqlite.order_by(&s),
            "json_extract(metadata, '$.color'), id"
        );
    }

    #[test]
    fn order_by_standard_id_key_maps_to_key_column() {
        let s = [("id".to_string(), SortDirection::Ascending)];
        assert_eq!(Dialect::Sqlite.order_by(&s), "key, id");
    }

    #[test]
    fn order_by_default_direction_sentinel_applies_to_id_tiebreaker() {
        // A bare "-" from the client → empty key, descending → id DESC.
        let s = [(String::new(), SortDirection::Descending)];
        assert_eq!(Dialect::Sqlite.order_by(&s), "id DESC");
    }

    #[test]
    fn order_by_postgres_uses_hash_arrow_path() {
        let s = [("color".to_string(), SortDirection::Descending)];
        assert_eq!(
            Dialect::Postgres.order_by(&s),
            "(metadata #> '{color}') DESC, id"
        );
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

    /// Regression (fail-open leak): the untagged-public arm must also require
    /// the row to carry no `user` key, so a user-owned blob `{"user": id}` —
    /// which has no `tags` key — is NOT matched as "public" for everyone.
    #[test]
    fn access_blob_filter_untagged_excludes_user_owned_sqlite() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            include_untagged: true,
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Sqlite);
        b.push_access_blob_filter(&f);
        let (sql, _) = b.finish();
        assert!(
            sql.contains("json_extract(access_blob, '$.user') IS NULL"),
            "untagged arm must exclude user-owned rows, got: {sql}"
        );
    }

    #[test]
    fn access_blob_filter_untagged_excludes_user_owned_postgres() {
        use tiled_core::queries::AccessBlobFilter;
        let f = AccessBlobFilter {
            include_untagged: true,
            ..Default::default()
        };
        let mut b = WhereBuilder::new(Dialect::Postgres);
        b.push_access_blob_filter(&f);
        let (sql, _) = b.finish();
        assert!(
            sql.contains("access_blob->'user' IS NULL"),
            "untagged arm must exclude user-owned rows, got: {sql}"
        );
    }
}
