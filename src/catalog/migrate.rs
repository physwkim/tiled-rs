//! Schema migration runner.
//!
//! We don't use `sqlx::migrate!` because it bakes a single migration set into
//! the binary at compile time — and we ship two dialect-specific sets. At
//! runtime we read the matching directory and apply each `*.sql` file once,
//! recording applied versions in `_tiled_migrations`.

use sqlx::Row;

use crate::catalog::db::{Catalog, DbPool};
use crate::catalog::error::{CatalogError, Result};

const SQLITE_MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_initial",
        include_str!("migrations/sqlite/0001_initial.sql"),
    ),
    (
        "0002_webhooks",
        include_str!("migrations/sqlite/0002_webhooks.sql"),
    ),
    (
        "0003_revisions_access_blob",
        include_str!("migrations/sqlite/0003_revisions_access_blob.sql"),
    ),
    (
        "0004_metadata_fts",
        include_str!("migrations/sqlite/0004_metadata_fts.sql"),
    ),
    (
        "0005_metadata_fts5",
        include_str!("migrations/sqlite/0005_metadata_fts5.sql"),
    ),
];

const POSTGRES_MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_initial",
        include_str!("migrations/postgres/0001_initial.sql"),
    ),
    (
        "0002_webhooks",
        include_str!("migrations/postgres/0002_webhooks.sql"),
    ),
    (
        "0003_revisions_access_blob",
        include_str!("migrations/postgres/0003_revisions_access_blob.sql"),
    ),
    (
        "0004_metadata_fts",
        include_str!("migrations/postgres/0004_metadata_fts.sql"),
    ),
    (
        "0005_metadata_fts5",
        include_str!("migrations/postgres/0005_metadata_fts5.sql"),
    ),
];

impl Catalog {
    /// Apply any pending schema migrations.
    pub async fn migrate(&self) -> Result<()> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS _tiled_migrations (
                        name TEXT PRIMARY KEY,
                        applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                    )",
                )
                .execute(pool)
                .await?;
                for (name, sql) in SQLITE_MIGRATIONS {
                    let already: Option<String> =
                        sqlx::query_scalar("SELECT name FROM _tiled_migrations WHERE name = ?")
                            .bind(*name)
                            .fetch_optional(pool)
                            .await?;
                    if already.is_some() {
                        continue;
                    }
                    apply_multi_statement(pool, sql).await?;
                    sqlx::query("INSERT INTO _tiled_migrations (name) VALUES (?)")
                        .bind(*name)
                        .execute(pool)
                        .await?;
                    tracing::info!(target: "tiled.catalog", "applied sqlite migration {name}");
                }
            }
            DbPool::Postgres(pool) => {
                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS _tiled_migrations (
                        name TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )",
                )
                .execute(pool)
                .await?;
                for (name, sql) in POSTGRES_MIGRATIONS {
                    let already: Option<String> =
                        sqlx::query_scalar("SELECT name FROM _tiled_migrations WHERE name = $1")
                            .bind(*name)
                            .fetch_optional(pool)
                            .await?;
                    if already.is_some() {
                        continue;
                    }
                    apply_multi_statement_pg(pool, sql).await?;
                    sqlx::query("INSERT INTO _tiled_migrations (name) VALUES ($1)")
                        .bind(*name)
                        .execute(pool)
                        .await?;
                    tracing::info!(target: "tiled.catalog", "applied postgres migration {name}");
                }
            }
        }
        Ok(())
    }

    /// Returns the list of applied migration names, oldest-first. Used by
    /// the CLI's `catalog upgrade-database` to report state.
    pub async fn applied_migrations(&self) -> Result<Vec<String>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let rows = sqlx::query("SELECT name FROM _tiled_migrations ORDER BY applied_at")
                    .fetch_all(pool)
                    .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| r.get::<String, _>("name"))
                    .collect())
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query("SELECT name FROM _tiled_migrations ORDER BY applied_at")
                    .fetch_all(pool)
                    .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| r.get::<String, _>("name"))
                    .collect())
            }
        }
    }
}

/// Split a multi-statement SQL string and execute each piece. SQLx doesn't
/// run `CREATE TABLE; CREATE INDEX;` as one prepared statement, so we split
/// on `;` while ignoring `;` inside string literals.
async fn apply_multi_statement(pool: &sqlx::Pool<sqlx::Sqlite>, sql: &str) -> Result<()> {
    for stmt in split_statements(sql) {
        if stmt.trim().is_empty() {
            continue;
        }
        sqlx::query(&stmt).execute(pool).await.map_err(|e| {
            CatalogError::Migration(format!("statement failed: {e}\n--- sql ---\n{stmt}"))
        })?;
    }
    Ok(())
}

async fn apply_multi_statement_pg(pool: &sqlx::Pool<sqlx::Postgres>, sql: &str) -> Result<()> {
    for stmt in split_statements(sql) {
        if stmt.trim().is_empty() {
            continue;
        }
        sqlx::query(&stmt).execute(pool).await.map_err(|e| {
            CatalogError::Migration(format!("statement failed: {e}\n--- sql ---\n{stmt}"))
        })?;
    }
    Ok(())
}

/// Naive splitter — tracks `'` quoted string boundaries (with `''` escape) and
/// `BEGIN`/`END` blocks so a `CREATE TRIGGER ... BEGIN ...; ...; END;` body keeps
/// its inner statement terminators and stays a single statement. Sufficient for
/// our migration SQL, which has no `$$` blocks.
fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_quote = false;
    // Nesting depth of `BEGIN`...`END` blocks (trigger bodies). A `;` only ends a
    // statement at depth 0, so a trigger body's inner terminators don't split it.
    let mut depth: usize = 0;
    // Current run of identifier/keyword characters, classified at its boundary
    // so we can recognise the `BEGIN`/`END` keywords (and only them — never a
    // substring like `BEGINNING` or a quoted literal).
    let mut word = String::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        // Inside a string literal only the closing quote (with `''` escape)
        // matters; keywords and `;` are inert here.
        if in_quote {
            if c == '\'' {
                if chars.peek() == Some(&'\'') {
                    buf.push(c);
                    buf.push(chars.next().unwrap());
                    continue;
                }
                in_quote = false;
            }
            buf.push(c);
            continue;
        }
        // Accumulate an identifier/keyword run; classify it when it ends.
        if c.is_ascii_alphanumeric() || c == '_' {
            word.push(c);
            buf.push(c);
            continue;
        }
        if !word.is_empty() {
            match word.to_ascii_uppercase().as_str() {
                "BEGIN" => depth += 1,
                "END" => depth = depth.saturating_sub(1),
                _ => {}
            }
            word.clear();
        }
        if c == '-' && chars.peek() == Some(&'-') {
            // Comment to end of line.
            for c2 in chars.by_ref() {
                if c2 == '\n' {
                    buf.push('\n');
                    break;
                }
            }
            continue;
        }
        if c == '\'' {
            in_quote = true;
            buf.push(c);
            continue;
        }
        if c == ';' && depth == 0 {
            out.push(std::mem::take(&mut buf));
            continue;
        }
        buf.push(c);
    }
    if !buf.trim().is_empty() {
        out.push(buf);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::split_statements;

    /// Only non-empty, trimmed statements — matches what `apply_multi_statement`
    /// actually executes (it skips blank pieces).
    fn split_nonempty(sql: &str) -> Vec<String> {
        split_statements(sql)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    #[test]
    fn splits_plain_statements() {
        let stmts = split_nonempty("CREATE TABLE a (id INT); CREATE INDEX i ON a(id);");
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("CREATE TABLE a"));
        assert!(stmts[1].starts_with("CREATE INDEX i"));
    }

    #[test]
    fn does_not_split_on_semicolon_inside_quotes() {
        let stmts = split_nonempty("INSERT INTO a VALUES ('x; y'); SELECT 1;");
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'x; y'"));
    }

    #[test]
    fn keeps_trigger_body_as_single_statement() {
        // The FTS5 update trigger has two inner `;` terminators inside BEGIN..END.
        let sql = "CREATE TRIGGER t AFTER UPDATE ON nodes BEGIN \
                   INSERT INTO fts(metadata) VALUES (old.metadata); \
                   INSERT INTO fts(metadata) VALUES (new.metadata); \
                   END; \
                   INSERT INTO fts(fts) VALUES ('rebuild');";
        let stmts = split_nonempty(sql);
        assert_eq!(
            stmts.len(),
            2,
            "trigger + rebuild = 2 statements: {stmts:?}"
        );
        assert!(stmts[0].starts_with("CREATE TRIGGER t"));
        assert!(stmts[0].contains("END"));
        // Both inner terminators survive inside the trigger body.
        assert_eq!(stmts[0].matches(';').count(), 2);
        assert!(stmts[1].starts_with("INSERT INTO fts(fts)"));
    }

    #[test]
    fn begin_substring_is_not_a_block() {
        // A word that merely starts with BEGIN/END must not change nesting.
        let stmts = split_nonempty("SELECT beginning, ending FROM a; SELECT 2;");
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn strips_line_comments() {
        let stmts = split_nonempty("-- a comment; not a statement\nSELECT 1;");
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("SELECT 1"));
    }
}
