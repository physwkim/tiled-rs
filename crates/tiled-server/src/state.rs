//! Application state shared across all request handlers.

use std::sync::Arc;

use tiled_core::adapters::ContainerAdapter;
use tiled_serialization::SerializationRegistry;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub root_tree: Arc<dyn ContainerAdapter>,
    pub serialization_registry: Arc<SerializationRegistry>,
    pub query_names: Vec<String>,
    pub base_url: String,
}
