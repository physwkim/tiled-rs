use std::sync::Arc;

use anyhow::Result;
use clap::Subcommand;
use indexmap::IndexMap;

use tiled_adapters::{ArrayAdapter, MapAdapter};
use tiled_core::adapters::AnyAdapter;
use tiled_core::queries::Query;

#[derive(Subcommand)]
pub enum Command {
    /// Start the Tiled server
    Serve {
        /// Path to configuration file
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
    },

    /// Database management commands
    Catalog {
        #[command(subcommand)]
        command: CatalogCommand,
    },

    /// API key management
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
    let arr_1d = ArrayAdapter::from_f64_1d(&data_1d, serde_json::json!({"description": "A 1D array of 100 floats"}));
    mapping.insert("small_1d".to_string(), AnyAdapter::Array(Box::new(arr_1d)));

    // 2D array of floats
    let data_2d: Vec<f64> = (0..200).map(|i| (i as f64) * 0.5).collect();
    let arr_2d = ArrayAdapter::from_f64_2d(&data_2d, 10, 20, serde_json::json!({"description": "A 10x20 array of floats"}));
    mapping.insert("medium_2d".to_string(), AnyAdapter::Array(Box::new(arr_2d)));

    // Nested container
    let mut inner_mapping = IndexMap::new();
    let inner_data: Vec<f64> = (0..50).map(|i| i as f64).collect();
    let inner_arr = ArrayAdapter::from_f64_1d(&inner_data, serde_json::json!({"element": "Cu", "edge": "K"}));
    inner_mapping.insert("spectrum".to_string(), AnyAdapter::Array(Box::new(inner_arr)));

    let inner_container = MapAdapter::new(
        inner_mapping,
        serde_json::json!({"sample": "copper_foil"}),
        vec![],
    );
    mapping.insert(
        "sample_data".to_string(),
        AnyAdapter::Container(Box::new(inner_container)),
    );

    MapAdapter::new(
        mapping,
        serde_json::json!({"description": "Tiled demo catalog"}),
        vec![],
    )
}

pub async fn run(command: Command) -> Result<()> {
    match command {
        Command::Serve {
            config,
            host,
            port,
            demo,
        } => {
            if let Some(ref config) = config {
                tracing::info!("Using config: {config}");
            }

            let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = if demo {
                tracing::info!("Starting with demo dataset");
                Arc::new(build_demo_tree())
            } else if config.is_some() {
                anyhow::bail!("Config-based serving not yet implemented. Use --demo for a demo server.");
            } else {
                anyhow::bail!("Either --config or --demo must be specified");
            };

            let registry = Arc::new(tiled_serialization::default_registry());
            let base_url = format!("http://{host}:{port}");

            let state = tiled_server::AppState {
                root_tree,
                serialization_registry: registry,
                query_names: Query::all_query_names()
                    .into_iter()
                    .map(String::from)
                    .collect(),
                base_url,
            };

            let app = tiled_server::build_app(state);

            let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
            tracing::info!("Tiled server listening on {host}:{port}");
            axum::serve(listener, app).await?;
            Ok(())
        }
        Command::Catalog { command } => match command {
            CatalogCommand::Init { uri } => {
                tracing::info!("Initializing catalog database: {uri}");
                todo!("Catalog init not yet implemented")
            }
            CatalogCommand::UpgradeDatabase { uri } => {
                tracing::info!("Upgrading catalog database: {uri}");
                todo!("Catalog upgrade not yet implemented")
            }
        },
        Command::ApiKey { command } => match command {
            ApiKeyCommand::Create { note } => {
                tracing::info!("Creating API key (note: {:?})", note);
                todo!("API key create not yet implemented")
            }
            ApiKeyCommand::List => {
                todo!("API key list not yet implemented")
            }
            ApiKeyCommand::Revoke { first_eight } => {
                tracing::info!("Revoking API key: {first_eight}");
                todo!("API key revoke not yet implemented")
            }
        },
    }
}
