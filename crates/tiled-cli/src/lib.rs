pub mod config;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Subcommand;
use indexmap::IndexMap;

use tiled_adapters::{ArrayAdapter, MapAdapter};
use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::queries::Query;
use tiled_server::state::CorsOriginPolicy;

// The `Serve` variant carries the full server configuration (~20 CLI flags),
// so it is far larger than the tiny subcommand variants. That asymmetry is
// harmless here: `Command` is parsed exactly once at startup and exactly one
// instance ever exists, so the per-variant size has no runtime cost. Boxing
// the payload would add indirection purely to satisfy a layout lint that does
// not model a parse-once singleton.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
pub enum Command {
    /// Start the Tiled server
    Serve {
        /// Path to configuration file (YAML)
        #[arg(short, long)]
        config: Option<String>,

        /// Host to bind to. Defaults to loopback; use 0.0.0.0 to expose on all
        /// interfaces (explicit opt-in required).
        #[arg(long, default_value = "127.0.0.1")]
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

        /// Restrict file-backed reads to data files under these directories
        /// (repeatable). Reads are deny-by-default: without this flag (and
        /// without `--allow-unrestricted-reads`) the server refuses to serve
        /// any local file referenced by a registered data_uri.
        #[arg(long = "allowed-data-dir")]
        allowed_data_dirs: Vec<std::path::PathBuf>,

        /// Disable path containment entirely: serve any local file a
        /// registered data_uri points at. The explicit opt-out for trusted
        /// single-user deployments — unsafe once untrusted writers can
        /// register nodes. Overrides `--allowed-data-dir`.
        #[arg(long)]
        allow_unrestricted_reads: bool,

        /// Directories under which the server may *create* internally-managed
        /// data when a client `POST`s to `/metadata` (repeatable). Without
        /// this flag, managed creation is disabled and such requests fall back
        /// to metadata-only nodes. Each directory is also folded into the read
        /// allow-list so a freshly-created file is immediately readable
        /// (writable ⊆ readable). Distinct from `--allowed-data-dir`, which
        /// only governs which *existing* files may be read/registered.
        #[arg(long = "writable-storage")]
        writable_storage: Vec<std::path::PathBuf>,

        /// Disable the bundled WebUI shell. The API still works; only
        /// the `/`, `/static/*`, and `/admin/*` browser surface goes
        /// away. Useful for headless deployments.
        #[arg(long)]
        no_web: bool,

        /// Override the embedded SPA bundle with a directory on disk —
        /// the typical way to swap in the prebuilt bluesky/tiled WebUI
        /// without recompiling tiled-rs.
        #[arg(long)]
        web_assets_dir: Option<std::path::PathBuf>,

        /// Allow `http://` URLs as webhook targets. Default off — the
        /// server enforces HTTPS for outbound webhook posts. Useful only
        /// for local testing; never set in production.
        #[arg(long)]
        webhooks_allow_http: bool,

        /// Allow webhook URLs that resolve to private / loopback / link-
        /// local / RFC1918 / metadata-IP ranges. Default off; SSRF
        /// protection. Useful only for local testing.
        #[arg(long)]
        webhooks_allow_private_addresses: bool,
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

    /// Auth database administration
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
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

#[derive(Subcommand)]
pub enum AdminCommand {
    /// Initialize the auth database schema. DB-direct; mirrors Python
    /// `tiled admin initialize-database` (_admin.py:16).
    InitializeDatabase {
        /// Auth DB URI (e.g. sqlite:///var/lib/tiled-auth.db).
        #[arg(env = "TILED_AUTH_DB_URI")]
        uri: String,
    },
    /// Create a service principal. DB-direct (no server required); mirrors
    /// Python `tiled admin create-service-principal` (_admin.py:201).
    /// For REST-based creation (requires a running server with admin
    /// credentials), use the server's POST /api/v1/auth/principal endpoint.
    CreateServicePrincipal {
        /// Auth DB URI (e.g. sqlite:///var/lib/tiled-auth.db).
        #[arg(long, env = "TILED_AUTH_DB_URI")]
        auth_db_uri: String,
        /// Role to assign. Defaults to "user"; use "admin" for an admin
        /// service account.
        #[arg(long, default_value = "user")]
        role: String,
    },
    /// Validate a configuration file (or directory of config files) for
    /// syntax and validation errors without starting the server. Mirrors
    /// Python `tiled admin check-config` (_admin.py:141): exits non-zero with
    /// the parse error on failure, prints a success line otherwise.
    CheckConfig {
        /// Path to a config file or directory. If omitted, uses $TILED_CONFIG,
        /// then `./config.yml` (Python parity).
        config_path: Option<String>,
    },
    /// List principals (users and services) with their linked identities.
    /// DB-direct (no running server required); the equivalent of Python
    /// `tiled admin list-principals` (_admin.py:166), which reaches a server's
    /// admin API — tiled-rs queries the auth DB directly, consistent with the
    /// other `tiled admin` subcommands.
    ListPrincipals {
        /// Auth DB URI (e.g. sqlite:///var/lib/tiled-auth.db).
        #[arg(long, env = "TILED_AUTH_DB_URI")]
        auth_db_uri: String,
        /// Page offset (Python `page_offset`).
        #[arg(long, default_value_t = 0)]
        offset: i64,
        /// Max items to show (Python `page_limit`).
        #[arg(long, default_value_t = 100)]
        limit: i64,
    },
    /// Show one principal (user or service) by UUID, with its linked
    /// identities. DB-direct; the equivalent of Python
    /// `tiled admin show-principal` (_admin.py:184).
    ShowPrincipal {
        /// Auth DB URI (e.g. sqlite:///var/lib/tiled-auth.db).
        #[arg(long, env = "TILED_AUTH_DB_URI")]
        auth_db_uri: String,
        /// UUID identifying the principal of interest.
        uuid: String,
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

/// Generate a 64-character hex string from 32 cryptographically-random bytes.
/// Mirrors Python's `secrets.token_hex(32)` used in `_serve.py`.
fn generate_single_user_key() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Validate an operator-supplied single-user API key at startup, mirroring
/// Python `build_app` (server/app.py:549-561) and the config schema
/// `[a-zA-Z0-9]+` (config.py:149). An empty key would pass the server's
/// constant-time compare against `?api_key=` / `Authorization: Apikey ` and
/// silently grant access; a non-alphanumeric key carries reserved URL/header
/// bytes (`&`, `=`, space, control chars) that cannot round-trip cleanly. The
/// auto-generated key is hex and never reaches this gate.
fn validate_single_user_api_key(key: &str) -> anyhow::Result<()> {
    if key.is_empty() {
        anyhow::bail!(
            "--api-key (or config single_user_api_key) is empty; \
             either omit it for anonymous access or supply a non-empty key"
        );
    }
    if !key.chars().all(|c| c.is_ascii_alphanumeric()) {
        anyhow::bail!(
            "--api-key (or config single_user_api_key) must be alphanumeric \
             ([a-zA-Z0-9]); generate one with `openssl rand -hex 32`"
        );
    }
    Ok(())
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
    mapping.insert("small_1d".to_string(), AnyAdapter::Array(Arc::new(arr_1d)));

    // 2D array of floats
    let data_2d: Vec<f64> = (0..200).map(|i| (i as f64) * 0.5).collect();
    let arr_2d = ArrayAdapter::from_f64_2d(
        &data_2d,
        10,
        20,
        serde_json::json!({"description": "A 10x20 array of floats"}),
    );
    mapping.insert("medium_2d".to_string(), AnyAdapter::Array(Arc::new(arr_2d)));

    // Nested container
    let mut inner_mapping = IndexMap::new();
    let inner_data: Vec<f64> = (0..50).map(|i| i as f64).collect();
    let inner_arr = ArrayAdapter::from_f64_1d(
        &inner_data,
        serde_json::json!({"element": "Cu", "edge": "K"}),
    );
    inner_mapping.insert(
        "spectrum".to_string(),
        AnyAdapter::Array(Arc::new(inner_arr)),
    );

    let inner_container = MapAdapter::new(
        inner_mapping,
        serde_json::json!({"sample": "copper_foil"}),
        vec![],
    );
    mapping.insert(
        "sample_data".to_string(),
        AnyAdapter::Container(Arc::new(inner_container)),
    );

    // Larger arrays for benchmarking
    let large_1d: Vec<f64> = (0..100_000).map(|i| (i as f64) * 0.001).collect();
    let arr_large_1d = ArrayAdapter::from_f64_1d(
        &large_1d,
        serde_json::json!({"description": "100k element array"}),
    );
    mapping.insert(
        "large_1d".to_string(),
        AnyAdapter::Array(Arc::new(arr_large_1d)),
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
        AnyAdapter::Array(Arc::new(arr_large_2d)),
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
            allowed_data_dirs,
            allow_unrestricted_reads,
            writable_storage,
            no_web,
            web_assets_dir,
            webhooks_allow_http,
            webhooks_allow_private_addresses,
        } => {
            // cli-M6: with no --config flag, fall back to the TILED_CONFIG env
            // var (container/k8s pattern). Unlike Python we do not additionally
            // default to ./config.yml — the Rust `serve` is multi-modal (demo /
            // mongo / catalog / config), so an implicit ./config.yml would break
            // flag-only starts like `serve --demo`. The value may name a file or
            // a directory of config.d files (see TiledConfig::from_file).
            let config = config.or_else(|| std::env::var("TILED_CONFIG").ok());

            // Load config file (or directory) if provided.
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

            // Resolve catalog URI: CLI flag > config file. The config source is
            // the recommended `catalog:` block or a `trees:` entry with a
            // catalog adapter (cli-M4). Symmetric with mongo_uri above; without
            // this a valid Python `catalog: {uri: ...}` config never reaches the
            // catalog and the server bails as if no source were given.
            let resolved_catalog_uri = catalog_uri.or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|c| c.catalog_uri().map(String::from))
            });

            // Resolve API key: CLI flag > config file > env var.
            let api_key = api_key.or_else(|| file_config.as_ref().and_then(|c| c.api_key()));
            // Validate an operator-supplied single-user key once, at startup.
            // The auto-generated key below is hex, so only an operator-provided
            // key reaches this gate.
            if let Some(key) = api_key.as_deref() {
                validate_single_user_api_key(key)?;
            }

            // Warn early: explicit 0.0.0.0 bind with no operator-configured auth.
            // auth_db_uri.is_some() means multi-user JWT mode; api_key.is_some()
            // means single-user key mode. Neither set → server would be fully open
            // to any network peer if an interface-wide bind is allowed.
            if host == "0.0.0.0" && api_key.is_none() && auth_db_uri.is_none() {
                tracing::warn!(
                    "Binding 0.0.0.0 with no authentication configured. \
                     The server is reachable on all network interfaces without \
                     credentials. A single-user API key will be generated for \
                     this session. Pass --api-key or restrict to --host 127.0.0.1."
                );
            }

            // Single-user mode with no explicit key: mirror Python _serve.py
            // (secrets.token_hex(32)). Generate once per process; the key is not
            // persisted — restart produces a new key unless the operator exports it.
            let api_key = if auth_db_uri.is_none() && api_key.is_none() {
                let key = generate_single_user_key();
                eprintln!("Auto-generated single-user API key: {key}");
                eprintln!("Set TILED_SINGLE_USER_API_KEY={key} to reuse across restarts.\n");
                Some(key)
            } else {
                api_key
            };

            // Canonicalise the writable-storage roots to absolute paths,
            // creating them if missing, so `init_storage` can build valid
            // `file://` URIs and the read/delete/write containment checks
            // compare against real paths. Empty without `--writable-storage`.
            let mut writable_abs: Vec<std::path::PathBuf> =
                Vec::with_capacity(writable_storage.len());
            for dir in &writable_storage {
                std::fs::create_dir_all(dir).map_err(|e| {
                    anyhow::anyhow!("create writable-storage dir {}: {e}", dir.display())
                })?;
                let abs = dir.canonicalize().map_err(|e| {
                    anyhow::anyhow!("canonicalize writable-storage dir {}: {e}", dir.display())
                })?;
                writable_abs.push(abs);
            }
            // Read + delete containment must also cover the writable-storage
            // roots: a freshly-created managed file must be readable and
            // (force-)deletable. writable ⊆ readable.
            let read_dirs = {
                let mut d = allowed_data_dirs.clone();
                d.extend(writable_abs.iter().cloned());
                d
            };

            // Open the persistent catalog up-front (before the read tree) so
            // a misconfigured DB fails the start-up rather than the first
            // write request.
            let catalog_handle: Option<tiled_catalog::Catalog> =
                match resolved_catalog_uri.as_deref() {
                    None => None,
                    Some(uri) => {
                        tracing::info!("Opening catalog: {}", redact_mongo_uri(uri));
                        let cat = tiled_catalog::Catalog::connect(uri)
                            .await
                            .map_err(|e| anyhow::anyhow!("catalog connect: {e}"))?;
                        // Contain managed-asset physical deletion to the same
                        // directories the read-side resolver allows. A client can
                        // register an internally-managed data source with an
                        // arbitrary file:// data_uri, so a forced DELETE must
                        // refuse to remove a file outside storage rather than
                        // delete whatever path the client chose.
                        // --allow-unrestricted-reads opts out of both checks.
                        let cat = if allow_unrestricted_reads {
                            cat
                        } else {
                            cat.with_managed_delete_dirs(read_dirs.clone())
                        };
                        // Where the server may create internally-managed
                        // storage (independent of the read-containment opt-out).
                        let cat = cat.with_writable_storage(writable_abs.clone());
                        cat.migrate()
                            .await
                            .map_err(|e| anyhow::anyhow!("catalog migrate: {e}"))?;
                        Some(cat)
                    }
                };

            let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = if let Some(ref uri) =
                resolved_mongo_uri
            {
                tracing::info!("Connecting to MongoDB: {}", redact_mongo_uri(uri));
                let catalog = tiled_mongo::MongoCatalog::from_uri(uri)
                    .map_err(|e| anyhow::anyhow!("MongoDB connection failed: {e}"))?;
                match catalog.len().await {
                    Ok(n) => tracing::info!("MongoDB catalog loaded ({n} runs)"),
                    Err(e) => {
                        tracing::warn!("MongoDB catalog loaded (run count unavailable: {e})")
                    }
                }
                Arc::new(catalog)
            } else if let Some(ref cat) = catalog_handle {
                // Wire the file-format adapters so leaves backed by
                // CSV / NPY / TIFF / HDF5 / PNG / JPEG / Parquet
                // resolve to the right adapter. Reads are deny-by-default:
                // set --allowed-data-dir to allow specific directories, or
                // --allow-unrestricted-reads to disable containment.
                use tiled_server::file_resolver::FileLeafResolver;
                let file_resolver = if allow_unrestricted_reads {
                    FileLeafResolver::unrestricted()
                } else {
                    FileLeafResolver::new(read_dirs.clone())
                };
                let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
                    Arc::new(file_resolver);
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

            // CORS allowed origins: CLI `--allow-origin` takes precedence; when
            // the flag is absent, fall back to the config file's `allow_origins:`
            // (Python tiled config.py:281, previously dropped silently from YAML).
            let allow_origins = if allow_origins.is_empty() {
                file_config
                    .as_ref()
                    .map(|c| c.allow_origins().to_vec())
                    .unwrap_or_default()
            } else {
                allow_origins
            };

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

            // Decide BEFORE the struct literal moves auth_db_handle / catalog_handle.
            // An explicit `access_control:` config selects and constructs a real
            // policy (e.g. TagBasedPolicy); absent it, fall back to the previous
            // default — PassthroughPolicy when an auth DB is configured, no
            // policy otherwise (backward compatible).
            let access_policy_value: Option<Arc<dyn tiled_access::AccessPolicy>> = match file_config
                .as_ref()
                .and_then(|c| c.access_control.as_ref())
            {
                Some(ac) => Some(ac.build()?),
                None if auth_db_handle.is_some() => Some(Arc::new(tiled_access::PassthroughPolicy)),
                None => None,
            };
            let webhook_config_value = if catalog_handle.is_some() {
                Some(tiled_server::webhook_dispatch::WebhookConfig {
                    allow_http: webhooks_allow_http,
                    allow_private_addresses: webhooks_allow_private_addresses,
                    ..Default::default()
                })
            } else {
                None
            };

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
                external_oidc: None,
                forwarded_allow_ips: None,
                max_request_body_bytes: 10 * 1024 * 1024,
                response_bytesize_limit: file_config
                    .as_ref()
                    .map(|c| c.response_bytesize_limit)
                    .unwrap_or_else(config::default_response_bytesize_limit),
                streaming_bus: tiled_server::streaming::StreamingBus::new(),
                access_policy: access_policy_value,
                default_login_scopes: tiled_auth::ScopeSet::read_only(),
                enable_web: !no_web,
                web_assets_dir,
                spec_views: file_config
                    .as_ref()
                    .and_then(|c| c.web.as_ref())
                    .map(|w| {
                        w.spec_views
                            .iter()
                            .map(|s| tiled_server::state::SpecViewEntry {
                                spec: s.spec.clone(),
                                url: s.url.clone(),
                                label: s.label.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                // Enable the webhook subsystem when a catalog is configured —
                // webhooks need a DB to persist registrations and deliveries.
                // Move catalog_handle's presence check before AppState consumes
                // it; otherwise the borrow-after-move check fires.
                webhook_config: webhook_config_value,
                request_timeout_secs: file_config
                    .as_ref()
                    .map(|c| c.request_timeout_secs)
                    .unwrap_or(30),
                expose_raw_assets: file_config
                    .as_ref()
                    .map(|c| c.expose_raw_assets)
                    .unwrap_or(true),
            };

            let app = tiled_server::build_app(state);

            // Bind via the (host, port) tuple rather than format!("{host}:{port}").
            // A bare IPv6 literal (e.g. `--host ::1`) string-concatenated with the
            // port produces "::1:8000", which is not a valid SocketAddr — IPv6
            // authorities must be bracketed ("[::1]:8000"). The tuple form goes
            // through ToSocketAddrs, which resolves bare IPv6 literals, IPv4
            // literals, and hostnames correctly without any manual bracketing.
            let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
            // Log the address actually bound (correctly bracketed for IPv6, and
            // reflecting the real port when 0 was requested) rather than the raw
            // host/port concatenation.
            match listener.local_addr() {
                Ok(addr) => tracing::info!("Tiled server listening on {addr}"),
                Err(e) => {
                    tracing::info!(host = %host, port, "Tiled server listening (local_addr unavailable: {e})")
                }
            }
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
                let exp = expires_in.map(|s| chrono::Utc::now() + chrono::Duration::seconds(s));
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
                    .revoke_api_key(&first_eight, None)
                    .await
                    .map_err(|e| anyhow::anyhow!("revoke: {e}"))?;
                println!(
                    "revoked api key {} (id={})",
                    removed.first_eight, removed.id
                );
                Ok(())
            }
        },
        Command::Admin { command } => match command {
            AdminCommand::InitializeDatabase { uri } => {
                let db = tiled_auth::AuthDb::connect(&uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("auth db connect: {e}"))?;
                db.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;
                eprintln!("Database initialized.");
                Ok(())
            }
            AdminCommand::CreateServicePrincipal { auth_db_uri, role } => {
                let db = tiled_auth::AuthDb::connect(&auth_db_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("auth db: {e}"))?;
                db.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;
                let principal = db
                    .create_service_principal(&role)
                    .await
                    .map_err(|e| anyhow::anyhow!("create service principal: {e}"))?;
                println!("uuid: {}", principal.uuid);
                println!("type: {}", principal.r#type);
                println!("role: {}", principal.role);
                Ok(())
            }
            AdminCommand::CheckConfig { config_path } => {
                let path = config_path
                    .or_else(|| std::env::var("TILED_CONFIG").ok())
                    .unwrap_or_else(|| "config.yml".to_string());
                // Parsing is the validation: `from_file` surfaces I/O, YAML,
                // and reconciliation errors. Propagate on failure (the binary
                // prints it and exits non-zero, mirroring Python's
                // `typer.Exit(1)`); confirm on success.
                config::TiledConfig::from_file(&path)
                    .with_context(|| format!("configuration check failed for {path}"))?;
                println!("No errors found in configuration.");
                Ok(())
            }
            AdminCommand::ListPrincipals {
                auth_db_uri,
                offset,
                limit,
            } => {
                let db = tiled_auth::AuthDb::connect(&auth_db_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("auth db: {e}"))?;
                db.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;
                let principals = db
                    .list_principals(offset, limit)
                    .await
                    .map_err(|e| anyhow::anyhow!("list principals: {e}"))?;
                println!("{}", serde_json::to_string_pretty(&principals)?);
                Ok(())
            }
            AdminCommand::ShowPrincipal { auth_db_uri, uuid } => {
                let db = tiled_auth::AuthDb::connect(&auth_db_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("auth db: {e}"))?;
                db.migrate()
                    .await
                    .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;
                let detail = db
                    .get_principal_detail(&uuid)
                    .await
                    .map_err(|e| anyhow::anyhow!("show principal: {e}"))?
                    .ok_or_else(|| anyhow::anyhow!("No such Principal {uuid}"))?;
                println!("{}", serde_json::to_string_pretty(&detail)?);
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
    // JWT signing secret(s). Python tiled supports key rotation via a list of
    // secrets (`secret_keys` / `TILED_SECRET_KEYS`, a JSON list of strings): the
    // first signs new tokens, all are tried when verifying. Honor that env when
    // present; otherwise fall back to the single `--jwt-secret` / TILED_JWT_SECRET.
    let issuer = match std::env::var("TILED_SECRET_KEYS") {
        Ok(json) => {
            let keys: Vec<String> = serde_json::from_str(&json).map_err(|e| {
                anyhow::anyhow!("TILED_SECRET_KEYS must be a JSON list of strings: {e}")
            })?;
            let refs: Vec<&[u8]> = keys.iter().map(|s| s.as_bytes()).collect();
            tiled_auth::Issuer::with_secrets(&refs)
                .map_err(|e| anyhow::anyhow!("jwt secret: {e}"))?
        }
        Err(_) => {
            let secret_str = jwt_secret.ok_or_else(|| {
                anyhow::anyhow!(
                    "--auth-db-uri requires --jwt-secret (or TILED_SECRET_KEYS for key rotation)"
                )
            })?;
            tiled_auth::Issuer::new(secret_str.as_bytes())
                .map_err(|e| anyhow::anyhow!("jwt secret: {e}"))?
        }
    };
    let db = tiled_auth::AuthDb::connect(auth_uri)
        .await
        .map_err(|e| anyhow::anyhow!("auth db connect: {e}"))?;
    db.migrate()
        .await
        .map_err(|e| anyhow::anyhow!("auth migrate: {e}"))?;

    let mut dummy = tiled_auth::DummyAuthenticator::new(provider_name);
    let mut total = 0;
    for entry in users {
        let (name, secret) = entry
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("--user expects 'name:password', got '{entry}'"))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: Command,
    }

    #[test]
    fn serve_default_host_is_loopback() {
        let cli = TestCli::parse_from(["tiled", "serve", "--demo"]);
        let Command::Serve { host, .. } = cli.command else {
            panic!("expected Serve variant");
        };
        assert_eq!(host, "127.0.0.1");
    }

    #[test]
    fn serve_explicit_host_0000_overrides_default() {
        let cli = TestCli::parse_from(["tiled", "serve", "--host", "0.0.0.0", "--demo"]);
        let Command::Serve { host, .. } = cli.command else {
            panic!("expected Serve variant");
        };
        assert_eq!(host, "0.0.0.0");
    }

    // cli-L1: an IPv6 `--host` must not be string-concatenated with the port.
    #[test]
    fn ipv6_host_must_not_be_string_concatenated_with_port() {
        use std::net::{SocketAddr, ToSocketAddrs};
        // The bug: format!("{host}:{port}") for host="::1" yields "::1:8000",
        // which is NOT a valid SocketAddr — IPv6 literals require bracketing.
        assert!(
            "::1:8000".parse::<SocketAddr>().is_err(),
            "bare IPv6 host concatenated with port is not a valid SocketAddr"
        );
        // The fix: the (host, port) tuple goes through ToSocketAddrs (the same
        // path tokio's TcpListener::bind((&str, u16)) takes), which resolves the
        // bare IPv6 literal to a bracketed loopback addr with no manual work.
        let resolved: Vec<SocketAddr> = ("::1", 8000u16)
            .to_socket_addrs()
            .expect("(\"::1\", port) must resolve")
            .collect();
        assert!(
            resolved.iter().any(|a| a.is_ipv6() && a.port() == 8000),
            "tuple form must resolve ::1 to an IPv6 socket addr; got {resolved:?}"
        );
    }

    #[test]
    fn generated_key_is_64_hex_chars() {
        let key = generate_single_user_key();
        assert_eq!(key.len(), 64, "32 bytes → 64 hex chars");
        assert!(
            key.chars().all(|c| c.is_ascii_hexdigit()),
            "must be lowercase hex; got: {key}"
        );
    }

    #[test]
    fn generated_keys_are_unique() {
        let k1 = generate_single_user_key();
        let k2 = generate_single_user_key();
        assert_ne!(k1, k2, "consecutive generated keys must differ");
    }

    #[test]
    fn api_key_accepts_alphanumeric() {
        // A hex key (the canonical `openssl rand -hex 32` / token_hex output)
        // and a mixed alphanumeric key are both accepted.
        assert!(validate_single_user_api_key(&generate_single_user_key()).is_ok());
        assert!(validate_single_user_api_key("abcDEF0123").is_ok());
    }

    #[test]
    fn api_key_rejects_empty() {
        let err = validate_single_user_api_key("").unwrap_err().to_string();
        assert!(err.contains("empty"), "empty key must be rejected: {err}");
    }

    #[test]
    fn api_key_rejects_non_alphanumeric_chars() {
        // Reserved URL/header bytes and whitespace cannot round-trip through
        // `?api_key=` / `Authorization: Apikey `; Python rejects them via
        // `single_user_api_key.isalnum()`.
        for bad in [
            "key with space",
            "key&evil=1",
            "key\ninject",
            "kéy",
            "key-dash",
        ] {
            let err = validate_single_user_api_key(bad).unwrap_err().to_string();
            assert!(
                err.contains("alphanumeric"),
                "non-alphanumeric key {bad:?} must be rejected: {err}"
            );
        }
    }
}
