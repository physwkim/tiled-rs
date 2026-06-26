//! Migration runner — same pattern as tiled-catalog.

use sqlx::Row;

use crate::db::{AuthDb, AuthPool};
use crate::error::{AuthError, Result};

const SQLITE_MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_initial",
        include_str!("../migrations/sqlite/0001_initial.sql"),
    ),
    (
        "0002_add_principal_role",
        include_str!("../migrations/sqlite/0002_add_principal_role.sql"),
    ),
    (
        "0003_add_session_refresh_count",
        include_str!("../migrations/sqlite/0003_add_session_refresh_count.sql"),
    ),
    (
        "0004_add_access_tags",
        include_str!("../migrations/sqlite/0004_add_access_tags.sql"),
    ),
    (
        "0005_tag_registry",
        include_str!("../migrations/sqlite/0005_tag_registry.sql"),
    ),
    (
        "0006_add_session_state",
        include_str!("../migrations/sqlite/0006_add_session_state.sql"),
    ),
    (
        "0007_add_pending_sessions",
        include_str!("../migrations/sqlite/0007_add_pending_sessions.sql"),
    ),
];

const POSTGRES_MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_initial",
        include_str!("../migrations/postgres/0001_initial.sql"),
    ),
    (
        "0002_add_principal_role",
        include_str!("../migrations/postgres/0002_add_principal_role.sql"),
    ),
    (
        "0003_add_session_refresh_count",
        include_str!("../migrations/postgres/0003_add_session_refresh_count.sql"),
    ),
    (
        "0004_add_access_tags",
        include_str!("../migrations/postgres/0004_add_access_tags.sql"),
    ),
    (
        "0005_tag_registry",
        include_str!("../migrations/postgres/0005_tag_registry.sql"),
    ),
    (
        "0006_add_session_state",
        include_str!("../migrations/postgres/0006_add_session_state.sql"),
    ),
    (
        "0007_add_pending_sessions",
        include_str!("../migrations/postgres/0007_add_pending_sessions.sql"),
    ),
];

impl AuthDb {
    pub async fn migrate(&self) -> Result<()> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS _tiled_auth_migrations (
                        name TEXT PRIMARY KEY,
                        applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                    )",
                )
                .execute(pool)
                .await?;
                for (name, sql) in SQLITE_MIGRATIONS {
                    let already: Option<String> = sqlx::query_scalar(
                        "SELECT name FROM _tiled_auth_migrations WHERE name = ?",
                    )
                    .bind(*name)
                    .fetch_optional(pool)
                    .await?;
                    if already.is_some() {
                        continue;
                    }
                    apply_multi_sqlite(pool, sql).await?;
                    sqlx::query("INSERT INTO _tiled_auth_migrations (name) VALUES (?)")
                        .bind(*name)
                        .execute(pool)
                        .await?;
                    tracing::info!(target: "tiled.auth", "applied sqlite migration {name}");
                }
            }
            AuthPool::Postgres(pool) => {
                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS _tiled_auth_migrations (
                        name TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )",
                )
                .execute(pool)
                .await?;
                for (name, sql) in POSTGRES_MIGRATIONS {
                    let already: Option<String> = sqlx::query_scalar(
                        "SELECT name FROM _tiled_auth_migrations WHERE name = $1",
                    )
                    .bind(*name)
                    .fetch_optional(pool)
                    .await?;
                    if already.is_some() {
                        continue;
                    }
                    apply_multi_postgres(pool, sql).await?;
                    sqlx::query("INSERT INTO _tiled_auth_migrations (name) VALUES ($1)")
                        .bind(*name)
                        .execute(pool)
                        .await?;
                    tracing::info!(target: "tiled.auth", "applied postgres migration {name}");
                }
            }
        }
        Ok(())
    }

    pub async fn applied_migrations(&self) -> Result<Vec<String>> {
        match self.pool() {
            AuthPool::Sqlite(pool) => {
                let rows =
                    sqlx::query("SELECT name FROM _tiled_auth_migrations ORDER BY applied_at")
                        .fetch_all(pool)
                        .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| r.get::<String, _>("name"))
                    .collect())
            }
            AuthPool::Postgres(pool) => {
                let rows =
                    sqlx::query("SELECT name FROM _tiled_auth_migrations ORDER BY applied_at")
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

async fn apply_multi_sqlite(pool: &sqlx::Pool<sqlx::Sqlite>, sql: &str) -> Result<()> {
    for stmt in split_statements(sql) {
        if stmt.trim().is_empty() {
            continue;
        }
        sqlx::query(&stmt).execute(pool).await.map_err(|e| {
            AuthError::Migration(format!("statement failed: {e}\n--- sql ---\n{stmt}"))
        })?;
    }
    Ok(())
}

async fn apply_multi_postgres(pool: &sqlx::Pool<sqlx::Postgres>, sql: &str) -> Result<()> {
    for stmt in split_statements(sql) {
        if stmt.trim().is_empty() {
            continue;
        }
        sqlx::query(&stmt).execute(pool).await.map_err(|e| {
            AuthError::Migration(format!("statement failed: {e}\n--- sql ---\n{stmt}"))
        })?;
    }
    Ok(())
}

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
