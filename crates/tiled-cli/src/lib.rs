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
    /// Create a new API key
    Create {
        /// Optional note for the key
        #[arg(long)]
        note: Option<String>,
    },
    /// List API keys
    List,
    /// Revoke an API key
    Revoke {
        /// First eight characters of the key
        first_eight: String,
    },
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
    mapping.insert(
        "medium_2d".to_string(),
        AnyAdapter::Array(Box::new(arr_2d)),
    );

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
        } => {
            // Load config file if provided.
            let file_config = config
                .as_deref()
                .map(config::TiledConfig::from_file)
                .transpose()?;

            // Resolve MongoDB URI: CLI flag > config file.
            let resolved_mongo_uri = mongo_uri
                .or_else(|| file_config.as_ref().and_then(|c| c.mongo_uri().map(String::from)));

            // Resolve API key: CLI flag > config file > env var.
            let api_key = api_key.or_else(|| file_config.as_ref().and_then(|c| c.api_key()));

            let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> =
                if let Some(ref uri) = resolved_mongo_uri {
                    tracing::info!("Connecting to MongoDB: {uri}");
                    let catalog = tiled_mongo::MongoCatalog::from_uri(uri)
                        .map_err(|e| anyhow::anyhow!("MongoDB connection failed: {e}"))?;
                    tracing::info!("MongoDB catalog loaded ({} runs)", catalog.len());
                    Arc::new(catalog)
                } else if demo {
                    tracing::info!("Starting with demo dataset");
                    Arc::new(build_demo_tree())
                } else {
                    anyhow::bail!(
                        "Specify --demo, --mongo-uri, or --config to start the server"
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

            let state = tiled_server::AppState {
                root_tree,
                serialization_registry: registry,
                query_names: Query::all_query_names()
                    .into_iter()
                    .map(String::from)
                    .collect(),
                base_url: public_url,
                cors_policy,
                trust_forwarded_headers: trust_proxy,
                api_key,
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
                anyhow::bail!("'catalog init' is not yet implemented (uri: {uri})")
            }
            CatalogCommand::UpgradeDatabase { uri } => {
                anyhow::bail!("'catalog upgrade-database' is not yet implemented (uri: {uri})")
            }
        },
        Command::ApiKey { command } => match command {
            ApiKeyCommand::Create { note: _ } => {
                anyhow::bail!("'api-key create' is not yet implemented")
            }
            ApiKeyCommand::List => {
                anyhow::bail!("'api-key list' is not yet implemented")
            }
            ApiKeyCommand::Revoke { first_eight: _ } => {
                anyhow::bail!("'api-key revoke' is not yet implemented")
            }
        },
    }
}
