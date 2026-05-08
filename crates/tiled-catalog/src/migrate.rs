//! Schema migration runner.
//!
//! We don't use `sqlx::migrate!` because it bakes a single migration set into
//! the binary at compile time — and we ship two dialect-specific sets. At
//! runtime we read the matching directory and apply each `*.sql` file once,
//! recording applied versions in `_tiled_migrations`.

use sqlx::Row;

use crate::db::{Catalog, DbPool};
use crate::error::{CatalogError, Result};

const SQLITE_MIGRATIONS: &[(&str, &str)] = &[(
    "0001_initial",
    include_str!("../migrations/sqlite/0001_initial.sql"),
)];

const POSTGRES_MIGRATIONS: &[(&str, &str)] = &[(
    "0001_initial",
    include_str!("../migrations/postgres/0001_initial.sql"),
)];

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
                    let already: Option<String> = sqlx::query_scalar(
                        "SELECT name FROM _tiled_migrations WHERE name = ?",
                    )
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
                    let already: Option<String> = sqlx::query_scalar(
                        "SELECT name FROM _tiled_migrations WHERE name = $1",
                    )
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
                let rows = sqlx::query(
                    "SELECT name FROM _tiled_migrations ORDER BY applied_at",
                )
                .fetch_all(pool)
                .await?;
                Ok(rows.into_iter().map(|r| r.get::<String, _>("name")).collect())
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT name FROM _tiled_migrations ORDER BY applied_at",
                )
                .fetch_all(pool)
                .await?;
                Ok(rows.into_iter().map(|r| r.get::<String, _>("name")).collect())
            }
        }
    }
}

/// Split a multi-statement SQL string and execute each piece. SQLx doesn't
/// run `CREATE TABLE; CREATE INDEX;` as one prepared statement, so we split
/// on `;` while ignoring `;` inside string literals.
async fn apply_multi_statement(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    sql: &str,
) -> Result<()> {
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

async fn apply_multi_statement_pg(
    pool: &sqlx::Pool<sqlx::Postgres>,
    sql: &str,
) -> Result<()> {
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

/// Naive splitter — tracks `'` quoted string boundaries (with `''` escape).
/// Sufficient for our migration SQL which has no `$$` blocks.
fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_quote = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if in_quote && chars.peek() == Some(&'\'') {
                buf.push(c);
                buf.push(chars.next().unwrap());
                continue;
            }
            in_quote = !in_quote;
            buf.push(c);
            continue;
        }
        if c == '-' && chars.peek() == Some(&'-') && !in_quote {
            // Comment to end of line.
            for c2 in chars.by_ref() {
                if c2 == '\n' {
                    buf.push('\n');
                    break;
                }
            }
            continue;
        }
        if c == ';' && !in_quote {
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
