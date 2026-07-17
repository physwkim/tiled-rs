//! Schema migration runner.
//!
//! We don't use `sqlx::migrate!` because it bakes a single migration set into
//! the binary at compile time — and we ship two dialect-specific sets. At
//! runtime we read the matching directory and apply each `*.sql` file once,
//! recording applied versions in `_tiled_migrations`.

use sqlx::Row;

use crate::catalog::db::{Catalog, DbPool};
use crate::catalog::error::{CatalogError, Result};

/// The schema state of a catalog DB relative to the migrations this binary
/// ships, mirroring the states upstream `check_catalog_database`
/// (`catalog/core.py`) distinguishes before serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaState {
    /// No migrations applied — the `_tiled_migrations` table is absent or
    /// empty. Upstream `UninitializedDatabase`.
    Uninitialized,
    /// Every shipped migration is applied and the DB carries no unknown ones.
    /// Safe to serve.
    Current,
    /// A subset of the shipped migrations is applied; the DB was written by an
    /// older tiled-rs and needs a forward migration. Upstream
    /// `DatabaseUpgradeNeeded`.
    Behind { applied: usize, required: usize },
    /// The DB carries migration name(s) this binary does not know about — it
    /// was written by a newer tiled-rs. Upstream `UnrecognizedDatabase`
    /// ("created by a newer version").
    Ahead { unknown: Vec<String> },
}

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

    /// The migration names this binary ships for the active dialect, oldest
    /// first. This is the authoritative "current schema" the serve guard
    /// compares a DB against.
    fn known_migration_names(&self) -> Vec<&'static str> {
        match self.pool() {
            DbPool::Sqlite(_) => SQLITE_MIGRATIONS.iter().map(|(n, _)| *n).collect(),
            DbPool::Postgres(_) => POSTGRES_MIGRATIONS.iter().map(|(n, _)| *n).collect(),
        }
    }

    /// Whether the `_tiled_migrations` bookkeeping table exists. A brand-new
    /// `--catalog-uri` connects (via `create_if_missing`) to an empty DB where
    /// it does not — that DB is uninitialized, distinct from a real query
    /// error.
    async fn migrations_table_exists(&self) -> Result<bool> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let found: Option<String> = sqlx::query_scalar(
                    "SELECT name FROM sqlite_master \
                     WHERE type = 'table' AND name = '_tiled_migrations'",
                )
                .fetch_optional(pool)
                .await?;
                Ok(found.is_some())
            }
            DbPool::Postgres(pool) => {
                // `to_regclass` yields NULL when the relation does not exist.
                let reg: Option<String> =
                    sqlx::query_scalar("SELECT to_regclass('_tiled_migrations')::text")
                        .fetch_one(pool)
                        .await?;
                Ok(reg.is_some())
            }
        }
    }

    /// Classify the DB's schema relative to the migrations this binary ships,
    /// without mutating it. Mirrors upstream `check_catalog_database`'s
    /// revision check (`catalog/core.py`): uninitialized / current / behind /
    /// ahead. Never auto-migrates.
    pub async fn schema_state(&self) -> Result<SchemaState> {
        if !self.migrations_table_exists().await? {
            return Ok(SchemaState::Uninitialized);
        }
        let applied = self.applied_migrations().await?;
        if applied.is_empty() {
            return Ok(SchemaState::Uninitialized);
        }
        let known = self.known_migration_names();
        let known_set: std::collections::HashSet<&str> = known.iter().copied().collect();
        // Any applied migration we do not ship was written by a newer tiled-rs.
        let unknown: Vec<String> = applied
            .iter()
            .filter(|n| !known_set.contains(n.as_str()))
            .cloned()
            .collect();
        if !unknown.is_empty() {
            return Ok(SchemaState::Ahead { unknown });
        }
        // All applied names are known; behind if any shipped migration is
        // still pending.
        let applied_set: std::collections::HashSet<&str> =
            applied.iter().map(String::as_str).collect();
        let pending = known.iter().filter(|n| !applied_set.contains(**n)).count();
        if pending > 0 {
            Ok(SchemaState::Behind {
                applied: applied.len(),
                required: known.len(),
            })
        } else {
            Ok(SchemaState::Current)
        }
    }

    /// Verify a catalog DB is safe to serve, mirroring upstream
    /// `check_catalog_database` (`catalog/core.py`). The single owner of the
    /// serve-time schema decision — there is deliberately **no** silent
    /// auto-migrate on serve:
    ///
    /// * `Current` → no-op (serve).
    /// * `Uninitialized` + `may_initialize` (serve `--init`/`--temp`) → apply
    ///   migrations to initialize in place.
    /// * `Uninitialized` without the flag → refuse, naming `tiled catalog init`.
    /// * `Behind` → refuse, naming `tiled catalog upgrade-database`.
    /// * `Ahead`/unknown → refuse with a version-mismatch error.
    ///
    /// `redacted_uri` is only used in messages; the caller passes an
    /// already password-redacted URI.
    pub async fn ensure_serveable(&self, redacted_uri: &str, may_initialize: bool) -> Result<()> {
        match self.schema_state().await? {
            SchemaState::Current => Ok(()),
            SchemaState::Uninitialized => {
                if may_initialize {
                    self.migrate().await
                } else {
                    Err(CatalogError::Validation(format!(
                        "Catalog database at {redacted_uri} is not initialized. \
                         Initialize it with `tiled catalog init {redacted_uri}` \
                         (or pass --init to create it now)."
                    )))
                }
            }
            SchemaState::Behind { applied, required } => Err(CatalogError::Migration(format!(
                "Catalog database at {redacted_uri} was created by an older tiled-rs \
                 ({applied} of {required} migrations applied). Back up the database, \
                 then upgrade it with `tiled catalog upgrade-database {redacted_uri}`."
            ))),
            SchemaState::Ahead { unknown } => Err(CatalogError::Migration(format!(
                "Catalog database at {redacted_uri} has migration(s) this tiled-rs \
                 does not recognize ({}); it was created by a newer version of tiled-rs. \
                 Upgrade tiled-rs to serve this catalog.",
                unknown.join(", ")
            ))),
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

    // --- w28-F2: serve-time schema-version guard boundaries ---

    use crate::catalog::db::{Catalog, DbPool};
    use crate::catalog::migrate::SchemaState;

    /// A fresh, un-migrated catalog in a temp dir (kept alive by the returned
    /// dir). File-backed so a multi-connection pool sees one consistent DB,
    /// unlike `sqlite::memory:`.
    async fn fresh_catalog() -> (tempfile::TempDir, Catalog) {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
        let cat = Catalog::connect(&uri).await.unwrap();
        (dir, cat)
    }

    /// Run a raw statement against the (sqlite) test pool.
    async fn exec_sqlite(cat: &Catalog, sql: &str) {
        match cat.pool() {
            DbPool::Sqlite(pool) => {
                sqlx::query(sql).execute(pool).await.unwrap();
            }
            DbPool::Postgres(_) => unreachable!("tests use sqlite"),
        }
    }

    // Boundary: an uninitialized catalog is refused when neither --init nor
    // --temp is set, with a message naming `tiled catalog init`.
    #[tokio::test]
    async fn uninitialized_without_flag_refuses() {
        let (_dir, cat) = fresh_catalog().await;
        assert_eq!(
            cat.schema_state().await.unwrap(),
            SchemaState::Uninitialized
        );
        let err = cat
            .ensure_serveable("sqlite://cat.db", false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not initialized"), "message: {msg}");
        assert!(msg.contains("tiled catalog init"), "message: {msg}");
    }

    // Boundary: an uninitialized catalog with --init/--temp is initialized in
    // place (migrations applied), leaving it Current.
    #[tokio::test]
    async fn uninitialized_with_init_initializes() {
        let (_dir, cat) = fresh_catalog().await;
        cat.ensure_serveable("sqlite://cat.db", true).await.unwrap();
        assert_eq!(cat.schema_state().await.unwrap(), SchemaState::Current);
    }

    // Boundary: a fully-migrated catalog is Current and serves without a
    // re-migrate.
    #[tokio::test]
    async fn current_serves() {
        let (_dir, cat) = fresh_catalog().await;
        cat.migrate().await.unwrap();
        assert_eq!(cat.schema_state().await.unwrap(), SchemaState::Current);
        // Serving neither errors nor requires the init flag.
        cat.ensure_serveable("sqlite://cat.db", false)
            .await
            .unwrap();
    }

    // Boundary: a catalog missing the newest shipped migration (written by an
    // older tiled-rs) is Behind and refused — even with --init — naming the
    // upgrade command. `--init` is not `--upgrade`.
    #[tokio::test]
    async fn behind_refuses_and_names_upgrade_command() {
        let (_dir, cat) = fresh_catalog().await;
        cat.migrate().await.unwrap();
        // Drop the newest migration row to simulate a behind-schema DB.
        exec_sqlite(
            &cat,
            "DELETE FROM _tiled_migrations WHERE name = '0005_metadata_fts5'",
        )
        .await;
        match cat.schema_state().await.unwrap() {
            SchemaState::Behind { applied, required } => {
                assert_eq!(applied, 4);
                assert_eq!(required, 5);
            }
            other => panic!("expected Behind, got {other:?}"),
        }
        // Without the flag: refused, naming upgrade-database.
        let err = cat
            .ensure_serveable("sqlite://cat.db", false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("tiled catalog upgrade-database"),
            "message: {err}"
        );
        // With --init: still refused (init initializes, it does not upgrade).
        assert!(
            cat.ensure_serveable("sqlite://cat.db", true).await.is_err(),
            "--init must not silently upgrade a behind DB"
        );
    }

    // Boundary: a catalog carrying a migration this binary does not know about
    // (written by a newer tiled-rs) is Ahead and refused with a version
    // mismatch error, regardless of the init flag.
    #[tokio::test]
    async fn ahead_unknown_revision_refuses() {
        let (_dir, cat) = fresh_catalog().await;
        cat.migrate().await.unwrap();
        // Stamp a future migration the current binary does not ship.
        exec_sqlite(
            &cat,
            "INSERT INTO _tiled_migrations (name) VALUES ('9999_from_future')",
        )
        .await;
        match cat.schema_state().await.unwrap() {
            SchemaState::Ahead { unknown } => {
                assert_eq!(unknown, vec!["9999_from_future".to_string()]);
            }
            other => panic!("expected Ahead, got {other:?}"),
        }
        // Refused whether or not --init is set — a newer-schema DB is never
        // servable by this binary.
        for may_init in [false, true] {
            let err = cat
                .ensure_serveable("sqlite://cat.db", may_init)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("newer version"), "message: {err}");
        }
    }
}
