mod config;

use std::sync::Arc;

use anyhow::Result;
use clap::Subcommand;
use indexmap::IndexMap;

use tiled_adapters::{ArrayAdapter, MapAdapter};
use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::queries::Query;
use tiled_server::state::CorsOriginPolicy;

#[derive(Subcommand)]
pub enum Command {
    /// Start the Tiled server
    Serve {
        /// Path to configuration file (YAML)
        #[arg(short, long)]
        config: Option<String>,

        /// Host to bind to
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Port to bind to
        #[arg(short, long, default_value_t = 8000)]
        port: u16,

        /// Start with a demo dataset
        #[arg(long)]
        demo: bool,

        /// Public base URL for generated links (default: derived from request Host header)
        #[arg(long)]
        public_url: Option<String>,

        /// Allowed CORS origins (repeatable). Use '*' for permissive.
        /// Default: same-origin only.
        #[arg(long = "allow-origin")]
        allow_origins: Vec<String>,

        /// Trust X-Forwarded-Host/Proto headers from reverse proxies.
        /// Only enable behind a trusted proxy.
        #[arg(long)]
        trust_proxy: bool,

        /// Single-user API key. Also reads TILED_SINGLE_USER_API_KEY env var.
        #[arg(long, env = "TILED_SINGLE_USER_API_KEY")]
        api_key: Option<String>,

        /// MongoDB URI for Bluesky data (e.g. mongodb://localhost:27017/my_database)
        #[arg(long)]
        mongo_uri: Option<String>,

        /// SQLite or Postgres URI for the persistent catalog (e.g.
        /// `sqlite:///var/lib/tiled.db` or `postgres://user@host/tiled`).
        /// When set, write endpoints (`POST /register`, `PATCH /metadata`,
        /// `PUT /data_source`, `DELETE /metadata`) operate against this DB.
        #[arg(long)]
        catalog_uri: Option<String>,

        /// Auth DB URI. When set, the server runs in multi-user mode:
        /// `/auth/{provider}/login` and friends use this DB; API keys
        /// are looked up here too. Required when --user is supplied.
        #[arg(long, env = "TILED_AUTH_DB_URI")]
        auth_db_uri: Option<String>,

        /// HMAC secret used to sign JWT access/refresh tokens. Required
        /// when --auth-db-uri is set. Must be at least 16 bytes; 32 is
        /// recommended.
        #[arg(long, env = "TILED_JWT_SECRET")]
        jwt_secret: Option<String>,

        /// Add a username/password pair to the dummy authenticator.
        /// Repeatable. Format: `name:password`. Without --auth-db-uri
        /// these are silently ignored — the dummy authenticator only
        /// makes sense in multi-user mode.
        #[arg(long = "user")]
        users: Vec<String>,

        /// Provider name to expose as `/auth/{provider}/login`. Defaults
        /// to "dummy" — change it when fronting an external IdP.
        #[arg(long, default_value = "dummy")]
        auth_provider_name: String,

        /// Trust the `X-Forwarded-User` header from a reverse proxy that
        /// has already authenticated the user. Implies `--trust-proxy`.
        #[arg(long)]
        proxied_auth_header: bool,
    },

    /// Database management commands (not yet implemented)
    #[command(hide = true)]
    Catalog {
        #[command(subcommand)]
        command: CatalogCommand,
    },

    /// API key management (not yet implemented)
    #[command(hide = true)]
    ApiKey {
        #[command(subcommand)]
        command: ApiKeyCommand,
    },
}

#[derive(Subcommand)]
pub enum CatalogCommand {
    /// Initialize a new catalog database
    Init {
        /// Database URI (e.g. sqlite:///path/to/catalog.db)
        uri: String,
    },
    /// Upgrade an existing catalog database
    UpgradeDatabase {
        /// Database URI
        uri: String,
    },
}

#[derive(Subcommand)]
pub enum ApiKeyCommand {
    /// Create a new API key. Prints the plaintext secret exactly once;
    /// the server only stores its Argon2id hash.
    Create {
        /// Auth DB URI (e.g. sqlite:///var/lib/tiled-auth.db).
        #[arg(long, env = "TILED_AUTH_DB_URI")]
        auth_db_uri: String,
        /// Principal UUID this key belongs to. If absent, a fresh
        /// service principal is created so the key can stand alone.
        #[arg(long)]
        principal: Option<String>,
        /// Optional note describing the key (visible in `api-key list`).
        #[arg(long)]
        note: Option<String>,
        /// Repeat to grant a scope. Default: full scope set.
        #[arg(long = "scope")]
        scopes: Vec<String>,
        /// Expiration in seconds from now. None = never expires.
        #[arg(long)]
        expires_in: Option<i64>,
    },
    /// List API keys.
    List {
        #[arg(long, env = "TILED_AUTH_DB_URI")]
        auth_db_uri: String,
    },
    /// Revoke an API key by its first-eight prefix.
    Revoke {
        #[arg(long, env = "TILED_AUTH_DB_URI")]
        auth_db_uri: String,
        first_eight: String,
    },
}

/// Replace the password segment of a MongoDB URI with `***` so it's safe to
/// log. Format: `scheme://[user[:password]@]host[/db][?opts]`. Leaves the
/// rest of the URI intact so operators can still match logs against the
/// configured host. If parsing fails (no `://`, no userinfo), returns the
/// input unchanged — there's no password to leak.
fn redact_mongo_uri(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return uri.to_string();
    };
    let Some(at_idx) = rest.find('@') else {
        return uri.to_string();
    };
    let userinfo = &rest[..at_idx];
    let host_and_rest = &rest[at_idx + 1..];
    let user = userinfo.split_once(':').map(|(u, _)| u).unwrap_or(userinfo);
    if user.is_empty() && !userinfo.contains(':') {
        return uri.to_string();
    }
    format!("{scheme}://{user}:***@{host_and_rest}")
}

/// Build a demo MapAdapter with sample arrays for testing.
fn build_demo_tree() -> MapAdapter {
    let mut mapping = IndexMap::new();

    // 1D array of floats
    let data_1d: Vec<f64> = (0..100).map(|i| (i as f64) * 0.1).collect();
    let arr_1d = ArrayAdapter::from_f64_1d(
        &data_1d,
        serde_json::json!({"description": "A 1D array of 100 floats"}),
    );
    mapping.insert("small_1d".to_string(), AnyAdapter::Array(Box::new(arr_1d)));

    // 2D array of floats
    let data_2d: Vec<f64> = (0..200).map(|i| (i as f64) * 0.5).collect();
    let arr_2d = ArrayAdapter::from_f64_2d(
        &data_2d,
        10,
        20,
        serde_json::json!({"description": "A 10x20 array of floats"}),
    );
    mapping.insert("medium_2d".to_string(), AnyAdapter::Array(Box::new(arr_2d)));

    // Nested container
    let mut inner_mapping = IndexMap::new();
    let inner_data: Vec<f64> = (0..50).map(|i| i as f64).collect();
    let inner_arr = ArrayAdapter::from_f64_1d(
        &inner_data,
        serde_json::json!({"element": "Cu", "edge": "K"}),
    );
    inner_mapping.insert(
        "spectrum".to_string(),
        AnyAdapter::Array(Box::new(inner_arr)),
    );

    let inner_container = MapAdapter::new(
        inner_mapping,
        serde_json::json!({"sample": "copper_foil"}),
        vec![],
    );
    mapping.insert(
        "sample_data".to_string(),
        AnyAdapter::Container(Box::new(inner_container)),
    );

    // Larger arrays for benchmarking
    let large_1d: Vec<f64> = (0..100_000).map(|i| (i as f64) * 0.001).collect();
    let arr_large_1d = ArrayAdapter::from_f64_1d(
        &large_1d,
        serde_json::json!({"description": "100k element array"}),
    );
    mapping.insert(
        "large_1d".to_string(),
        AnyAdapter::Array(Box::new(arr_large_1d)),
    );

    let large_2d: Vec<f64> = (0..1_000_000).map(|i| (i as f64) * 0.001).collect();
    let arr_large_2d = ArrayAdapter::from_f64_2d(
        &large_2d,
        1000,
        1000,
        serde_json::json!({"description": "1000x1000 array"}),
    );
    mapping.insert(
        "large_2d".to_string(),
        AnyAdapter::Array(Box::new(arr_large_2d)),
    );

    MapAdapter::new(
        mapping,
        serde_json::json!({"description": "Tiled demo catalog"}),
        vec![],
    )
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                _ = ctrl_c => tracing::info!("Received SIGINT, shutting down gracefully"),
                _ = sigterm.recv() => tracing::info!("Received SIGTERM, shutting down gracefully"),
            }
        }
        Err(e) => {
            tracing::warn!("Could not install SIGTERM handler: {e}, using SIGINT only");
            let _ = ctrl_c.await;
            tracing::info!("Received SIGINT, shutting down gracefully");
        }
    }
}

pub async fn run(command: Command) -> Result<()> {
    match command {
        Command::Serve {
            config,
            host,
            port,
            demo,
            public_url,
            allow_origins,
            trust_proxy,
            api_key,
            mongo_uri,
            catalog_uri,
            auth_db_uri,
            jwt_secret,
            users,
            auth_provider_name,
            proxied_auth_header,
        } => {
            // Load config file if provided.
            let file_config = config
                .as_deref()
                .map(config::TiledConfig::from_file)
                .transpose()?;

            // Resolve MongoDB URI: CLI flag > config file.
            let resolved_mongo_uri = mongo_uri.or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|c| c.mongo_uri().map(String::from))
            });

            // Resolve API key: CLI flag > config file > env var.
            let api_key = api_key.or_else(|| file_config.as_ref().and_then(|c| c.api_key()));
            // Empty string means "auth enabled but expected key is empty" — a
            // request with `?api_key=` or `Authorization: Apikey ` would then
            // pass the constant-time compare and silently grant access while
            // the startup log still claims auth is on. Refuse to start.
            if api_key.as_deref() == Some("") {
                anyhow::bail!(
                    "--api-key (or config single_user_api_key) is empty; \
                     either omit it for anonymous access or supply a non-empty key"
                );
            }

            // Open the persistent catalog up-front (before the read tree) so
            // a misconfigured DB fails the start-up rather than the first
            // write request.
            let catalog_handle: Option<tiled_catalog::Catalog> = match catalog_uri.as_deref() {
                None => None,
                Some(uri) => {
                    tracing::info!("Opening catalog: {}", redact_mongo_uri(uri));
                    let cat = tiled_catalog::Catalog::connect(uri)
                        .await
                        .map_err(|e| anyhow::anyhow!("catalog connect: {e}"))?;
                    cat.migrate()
                        .await
                        .map_err(|e| anyhow::anyhow!("catalog migrate: {e}"))?;
                    Some(cat)
                }
            };

            let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> =
                if let Some(ref uri) = resolved_mongo_uri {
                    tracing::info!("Connecting to MongoDB: {}", redact_mongo_uri(uri));
                    let catalog = tiled_mongo::MongoCatalog::from_uri(uri)
                        .map_err(|e| anyhow::anyhow!("MongoDB connection failed: {e}"))?;
                    tracing::info!("MongoDB catalog loaded ({} runs)", catalog.len());
                    Arc::new(catalog)
                } else if let Some(ref cat) = catalog_handle {
                    // Wire the file-format adapters so leaves backed by
                    // CSV / NPY / TIFF / HDF5 / PNG / JPEG / Parquet
                    // resolve to the right adapter.
                    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
                        Arc::new(tiled_server::file_resolver::FileLeafResolver);
                    Arc::new(tiled_catalog::CatalogAdapter::root(cat.clone(), resolver))
                } else if demo {
                    tracing::info!("Starting with demo dataset");
                    Arc::new(build_demo_tree())
                } else {
                    anyhow::bail!(
                        "Specify --demo, --mongo-uri, --catalog-uri, or --config to start the server"
                    );
                };

            let registry = Arc::new(tiled_serialization::default_registry());

            // CORS: explicit '*' = permissive, explicit origins = allow-list,
            // default (nothing specified) = same-origin only.
            let cors_policy = if allow_origins.iter().any(|o| o == "*") {
                CorsOriginPolicy::Permissive
            } else if !allow_origins.is_empty() {
                CorsOriginPolicy::AllowList(allow_origins)
            } else {
                CorsOriginPolicy::AllowList(Vec::new())
            };

            if api_key.is_some() {
                tracing::info!("API key authentication enabled");
            } else {
                tracing::info!("Anonymous access (no API key)");
            }

            // Multi-user auth wiring.
            let (auth_db_handle, issuer_handle, authenticators_built, proxied_auth) =
                build_auth_state(
                    auth_db_uri.as_deref(),
                    jwt_secret.as_deref(),
                    &users,
                    &auth_provider_name,
                    proxied_auth_header,
                )
                .await?;

            let trust_forwarded_headers = trust_proxy || proxied_auth_header;

            let state = tiled_server::AppState {
                root_tree,
                serialization_registry: registry,
                query_names: Query::all_query_names()
                    .into_iter()
                    .map(String::from)
                    .collect(),
                base_url: public_url,
                cors_policy,
                trust_forwarded_headers,
                api_key,
                catalog: catalog_handle,
                auth_db: auth_db_handle,
                issuer: issuer_handle,
                authenticators: authenticators_built,
                proxied_header_auth: proxied_auth,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
            };

            let app = tiled_server::build_app(state);

            let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
            tracing::info!("Tiled server listening on {host}:{port}");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await?;
            Ok(())
        }
        Command::Catalog { command } => match command {
            CatalogCommand::Init { uri } => {
                tracing::info!("Initialising catalog at {}", redact_mongo_uri(&uri));
                let cat = tiled_catalog::Catalog::connect(&uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
                cat.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("migrate: {e}"))?;
                let applied = cat
                    .applied_migrations()
                    .await
                    .map_err(|e| anyhow::anyhow!("query migrations: {e}"))?;
                println!("catalog initialised; applied migrations: {applied:?}");
                Ok(())
            }
            CatalogCommand::UpgradeDatabase { uri } => {
                let cat = tiled_catalog::Catalog::connect(&uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
                cat.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("migrate: {e}"))?;
                let applied = cat
                    .applied_migrations()
                    .await
                    .map_err(|e| anyhow::anyhow!("query migrations: {e}"))?;
                println!("up-to-date; applied migrations: {applied:?}");
                Ok(())
            }
        },
        Command::ApiKey { command } => match command {
            ApiKeyCommand::Create {
                auth_db_uri,
                principal,
                note,
                scopes,
                expires_in,
            } => {
                let db = tiled_auth::AuthDb::connect(&auth_db_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("auth db: {e}"))?;
                db.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;
                let principal_id = match principal {
                    Some(uuid) => {
                        // Look up the existing principal by uuid; refuse
                        // to mint a key for a stranger.
                        find_principal_by_uuid(&db, &uuid)
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("principal {uuid} not found"))?
                    }
                    None => {
                        let p = db
                            .create_principal("service")
                            .await
                            .map_err(|e| anyhow::anyhow!("principal: {e}"))?;
                        eprintln!("created service principal {} for new key", p.uuid);
                        p.id
                    }
                };
                let scope_set = if scopes.is_empty() {
                    tiled_auth::ScopeSet::full()
                } else {
                    let mut set = tiled_auth::ScopeSet::default();
                    for s in &scopes {
                        let scope = tiled_auth::Scope::parse(s)
                            .ok_or_else(|| anyhow::anyhow!("unknown scope: {s}"))?;
                        set.insert(scope);
                    }
                    set
                };
                let exp = expires_in.map(|s| {
                    chrono::Utc::now() + chrono::Duration::seconds(s)
                });
                let material = db
                    .create_api_key(tiled_auth::ApiKeyCreate {
                        principal_id,
                        note,
                        scopes: scope_set,
                        expiration_time: exp,
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("create api key: {e}"))?;
                println!("secret: {}", material.secret);
                println!("first_eight: {}", material.record.first_eight);
                println!(
                    "scopes: {}",
                    material
                        .record
                        .scopes
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if let Some(t) = material.record.expiration_time {
                    println!("expires_at: {t}");
                }
                eprintln!(
                    "\n\
                    NOTE: Save the secret above — the server only kept its hash, so this is \n\
                    the only chance to copy it.",
                );
                Ok(())
            }
            ApiKeyCommand::List { auth_db_uri } => {
                let db = tiled_auth::AuthDb::connect(&auth_db_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("auth db: {e}"))?;
                db.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;
                let keys = db
                    .list_api_keys(None)
                    .await
                    .map_err(|e| anyhow::anyhow!("list: {e}"))?;
                if keys.is_empty() {
                    println!("(no keys)");
                } else {
                    println!(
                        "{:<8} {:<6} {:<40} {:<24} expires",
                        "PREFIX", "ID", "SCOPES", "NOTE"
                    );
                    for k in keys {
                        let scopes: String = k
                            .scopes
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(",");
                        let note = k.note.unwrap_or_default();
                        let exp = k
                            .expiration_time
                            .map(|t| t.to_rfc3339())
                            .unwrap_or_else(|| "never".into());
                        println!(
                            "{:<8} {:<6} {:<40} {:<24} {}",
                            k.first_eight, k.id, scopes, note, exp
                        );
                    }
                }
                Ok(())
            }
            ApiKeyCommand::Revoke {
                auth_db_uri,
                first_eight,
            } => {
                let db = tiled_auth::AuthDb::connect(&auth_db_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("auth db: {e}"))?;
                db.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;
                let removed = db
                    .revoke_api_key(&first_eight)
                    .await
                    .map_err(|e| anyhow::anyhow!("revoke: {e}"))?;
                println!("revoked api key {} (id={})", removed.first_eight, removed.id);
                Ok(())
            }
        },
    }
}

/// Wire up the multi-user auth pieces from the supplied flags. Returns
/// the AppState fields the caller needs.
async fn build_auth_state(
    auth_db_uri: Option<&str>,
    jwt_secret: Option<&str>,
    users: &[String],
    provider_name: &str,
    proxied_auth_header: bool,
) -> Result<(
    Option<tiled_auth::AuthDb>,
    Option<tiled_auth::Issuer>,
    Vec<Arc<dyn tiled_auth::Authenticator>>,
    Option<Arc<tiled_auth::ProxiedHeaderAuthenticator>>,
)> {
    if auth_db_uri.is_none() && users.is_empty() && !proxied_auth_header {
        return Ok((None, None, vec![], None));
    }
    let auth_uri = auth_db_uri.ok_or_else(|| {
        anyhow::anyhow!(
            "--user / --proxied-auth-header require --auth-db-uri so the server can persist sessions"
        )
    })?;
    let secret_str = jwt_secret.ok_or_else(|| {
        anyhow::anyhow!("--auth-db-uri requires --jwt-secret (>= 16 bytes)")
    })?;
    let issuer = tiled_auth::Issuer::new(secret_str.as_bytes())
        .map_err(|e| anyhow::anyhow!("jwt secret: {e}"))?;
    let db = tiled_auth::AuthDb::connect(auth_uri)
        .await
        .map_err(|e| anyhow::anyhow!("auth db connect: {e}"))?;
    db.migrate()
        .await
        .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;

    let mut dummy = tiled_auth::DummyAuthenticator::new(provider_name);
    let mut total = 0;
    for entry in users {
        let (name, secret) = entry.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("--user expects 'name:password', got '{entry}'")
        })?;
        dummy
            .add_user(name, secret)
            .map_err(|e| anyhow::anyhow!("add user {name}: {e}"))?;
        total += 1;
    }
    let mut authenticators: Vec<Arc<dyn tiled_auth::Authenticator>> = Vec::new();
    if total > 0 {
        tracing::info!(
            "Auth: {} dummy user(s) configured under provider '{}'",
            total,
            provider_name
        );
        authenticators.push(Arc::new(dummy));
    }
    let proxied = if proxied_auth_header {
        tracing::info!("Auth: trusting X-Forwarded-User from upstream proxy");
        Some(Arc::new(tiled_auth::ProxiedHeaderAuthenticator::default()))
    } else {
        None
    };
    Ok((Some(db), Some(issuer), authenticators, proxied))
}

async fn find_principal_by_uuid(
    db: &tiled_auth::AuthDb,
    uuid: &str,
) -> anyhow::Result<Option<i64>> {
    use sqlx::Row;
    use tiled_auth::db::AuthPool;
    match db.pool() {
        AuthPool::Sqlite(pool) => Ok(sqlx::query("SELECT id FROM principals WHERE uuid = ?")
            .bind(uuid)
            .fetch_optional(pool)
            .await
            .map_err(|e| anyhow::anyhow!("lookup: {e}"))?
            .map(|r| r.get::<i64, _>("id"))),
        AuthPool::Postgres(pool) => Ok(sqlx::query("SELECT id FROM principals WHERE uuid = $1")
            .bind(uuid)
            .fetch_optional(pool)
            .await
            .map_err(|e| anyhow::anyhow!("lookup: {e}"))?
            .map(|r| r.get::<i64, _>("id"))),
    }
}
