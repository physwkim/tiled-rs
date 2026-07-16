//! `tiled-client` — Rust client for the Tiled HTTP catalog server.
//!
//! Port of the Python `tiled.client` package (`tiled/client/`). The Python
//! library multiplexes httpx + dask + numpy + pandas; we ship a focused async
//! reader: pull metadata, walk containers, fetch raw bytes for arrays, and
//! decode tabular data via Apache Arrow IPC.
//!
//! ## Quickstart
//!
//! ```no_run
//! use tiled_rs::client::from_uri;
//!
//! # async fn run() -> tiled_rs::client::Result<()> {
//! let client = from_uri("http://localhost:8000").await?;
//! let root = client.into_container()?;
//! for key in root.keys().await? {
//!     println!("{key}");
//! }
//! # Ok(()) }
//! ```

pub mod any_client;
pub mod array;
pub mod auth;
pub mod awkward;
pub mod base;
pub mod blosc2;
pub mod cache;
pub mod composite;
pub mod constructors;
pub mod container;
pub mod context;
pub mod dataframe;
pub mod dataset;
pub mod error;
pub mod profiles;
pub mod queries;
pub mod ragged;
pub mod register;
pub mod sparse;
pub mod stream;
pub mod utils;
pub mod xarray_client;

pub use any_client::AnyClient;
pub use array::{ArrayBlock, ArrayClient};
pub use auth::{
    AuthProvider, ProviderMode, TiledAuth, TokenStore, Tokens, WhoAmI, default_token_cache_dir,
    device_code_grant, password_grant, prompt_for_credentials, token_directory_for_server,
};
pub use awkward::{AwkwardBuffers, AwkwardClient};
pub use base::{
    BaseClient, Item, JSON_PATCH_MIME, MERGE_PATCH_MIME, MetadataRevisions, ParsedStructure,
    PatchContentType, Revision,
};
pub use cache::{CacheControl, CacheEntry, HttpCache};
pub use composite::{CompositeClient, CompositePart};
pub use constructors::{from_context, from_uri, from_uri_with_options};
pub use container::{ContainerClient, SortDirection};
pub use context::{ApiKeyCreated, ApiKeyInfo, Context, ContextOptions};
pub use dataframe::{TableClient, TablePartition};
pub use dataset::{Dataset, Variable};
pub use error::{ClientError, Result};
pub use profiles::{
    Profile, ProfileSet, create_profile, delete_profile, from_profile, get_default_profile_name,
    list_profiles, load_profiles, paths as profile_paths, set_default_profile_name,
};
pub use register::{
    AssetSpec, CsvAdapter, DataSourceSpec, JsonAdapter, ParquetAdapter, PassthroughAdapter,
    RegistrationAdapter, Settings as RegisterSettings, WatchHandle, default_filter,
    default_mimetypes, list_files, register, resolve_mimetype, strip_suffixes, watch,
};
pub use sparse::{SparseBlock, SparseClient};
pub use stream::{
    ArrayData, ArrayPatch, ArrayRef, ChildCreated, ChildMetadataUpdated, Schema, Subscription,
    SubscriptionStream, TableData, Update,
};
pub use xarray_client::DatasetClient;
