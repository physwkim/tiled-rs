//! Filesystem walker — register files/directories as Tiled nodes.
//!
//! Mirrors `tiled/client/register.py` (`register`, `_walk`, `register_single_item`,
//! `group_image_sequences`, `watch`) plus
//! `tiled/mimetypes.py::DEFAULT_MIMETYPES_BY_FILE_EXT`.
//!
//! ## Workflow
//!
//! 1. Walk a directory tree (`register(node, path)`).
//! 2. For each file: detect mimetype by extension (Python's
//!    `DEFAULT_MIMETYPES_BY_FILE_EXT` ported as `default_mimetypes`).
//! 3. Look up a [`RegistrationAdapter`] implementation for that mimetype.
//! 4. The adapter inspects the file enough to fill in `structure`,
//!    `metadata`, `specs`.
//! 5. POST `/api/v1/register/<container_path>` with the resulting payload.
//!
//! ## Adapters
//!
//! `RegistrationAdapter` is small: implement `inspect(&self, uri)` to return
//! a `DataSourceSpec`. Built-ins: CSV, Parquet, JSON.
//!
//! HDF5 / TIFF adapters are intentionally kept out of this crate — they pull
//! large native dependencies. Use the trait and provide your own.
//!
//! ## Watch mode
//!
//! `watch(node, path, ...)` uses the `notify` crate to listen for filesystem
//! changes after an initial walk; new/changed files are re-registered.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use url::Url;
use walkdir::WalkDir;

use crate::core::structures::{Spec, StructureFamily};

use crate::client::container::ContainerClient;
use crate::client::error::{ClientError, Result};
use crate::client::utils::retry;

// ---------------------------------------------------------------------------
// Default mimetypes by file extension
// ---------------------------------------------------------------------------

/// Mirror of `tiled/mimetypes.py::DEFAULT_MIMETYPES_BY_FILE_EXT`.
///
/// Built once and reused across calls.
pub fn default_mimetypes() -> &'static HashMap<&'static str, &'static str> {
    static MAP: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();
    MAP.get_or_init(|| {
        [
            (".csv", "text/csv"),
            (".tsv", "text/tab-separated-values"),
            (".txt", "text/plain"),
            (".json", "application/json"),
            (".yaml", "application/yaml"),
            (".yml", "application/yaml"),
            (".parquet", "application/x-parquet"),
            (".pq", "application/x-parquet"),
            (".h5", "application/x-hdf5"),
            (".hdf5", "application/x-hdf5"),
            (".nx", "application/x-hdf5"),
            (".tif", "image/tiff"),
            (".tiff", "image/tiff"),
            (".jpg", "image/jpeg"),
            (".jpeg", "image/jpeg"),
            (".png", "image/png"),
            (".npy", "application/x-npy"),
            (".zarr", "application/x-zarr"),
            (".n5", "application/x-n5"),
            (".pdf", "application/pdf"),
        ]
        .into_iter()
        .collect()
    })
}

/// Resolve a path's mimetype from extension (and an optional override map).
pub fn resolve_mimetype(path: &Path, overrides: &HashMap<String, String>) -> Option<String> {
    let suffixes: Vec<String> = path
        .file_name()?
        .to_string_lossy()
        .split('.')
        .skip(1)
        .map(|s| format!(".{s}"))
        .collect();
    let defaults = default_mimetypes();
    // Try compound suffixes first, then falling back to last suffix.
    for i in 0..suffixes.len() {
        let ext: String = suffixes[i..].concat();
        if let Some(v) = overrides.get(&ext) {
            return Some(v.clone());
        }
        if let Some(v) = defaults.get(ext.as_str()) {
            return Some((*v).into());
        }
    }
    mime_guess::from_path(path).first_raw().map(String::from)
}

/// Filter that ignores hidden files. Mirrors `default_filter`.
///
/// Non-UTF-8 filenames are accepted (only files whose name *starts* with `.`
/// after lossy conversion are dropped).
pub fn default_filter(path: &Path) -> bool {
    !path
        .file_name()
        .map(|n| n.to_string_lossy().starts_with('.'))
        .unwrap_or(false)
}

/// Strip suffixes from a filename to produce a node key. Mirrors
/// `strip_suffixes`.
pub fn strip_suffixes(filename: &str) -> String {
    match filename.find('.') {
        Some(i) => filename[..i].to_string(),
        None => filename.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Registration adapter trait
// ---------------------------------------------------------------------------

/// Output of inspecting a single file/directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceSpec {
    pub structure_family: StructureFamily,
    pub mimetype: String,
    pub structure: Option<serde_json::Value>,
    #[serde(default)]
    pub parameters: serde_json::Value,
    pub assets: Vec<AssetSpec>,
    #[serde(default)]
    pub specs: Vec<Spec>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetSpec {
    pub data_uri: String,
    pub is_directory: bool,
    pub parameter: String,
    #[serde(default)]
    pub num: Option<u32>,
}

/// Per-mimetype client-side adapter that infers structure.
#[async_trait]
pub trait RegistrationAdapter: Send + Sync {
    fn structure_family(&self) -> StructureFamily;
    fn mimetype(&self) -> &str;
    async fn inspect(&self, uri: &Url, is_directory: bool) -> Result<DataSourceSpec>;
}

/// Thin adapter that doesn't peek at the file at all — just records the URI.
/// Useful when the server-side adapter will figure out the structure.
pub struct PassthroughAdapter {
    pub mimetype: String,
    pub family: StructureFamily,
}

#[async_trait]
impl RegistrationAdapter for PassthroughAdapter {
    fn structure_family(&self) -> StructureFamily {
        self.family
    }
    fn mimetype(&self) -> &str {
        &self.mimetype
    }
    async fn inspect(&self, uri: &Url, is_directory: bool) -> Result<DataSourceSpec> {
        Ok(DataSourceSpec {
            structure_family: self.family,
            mimetype: self.mimetype.clone(),
            structure: None,
            parameters: serde_json::json!({}),
            assets: vec![AssetSpec {
                data_uri: uri.to_string(),
                is_directory,
                parameter: "data_uri".into(),
                num: None,
            }],
            specs: vec![],
            metadata: serde_json::json!({}),
        })
    }
}

/// Build the table `structure` JSON — carrying a real base64-encoded Arrow
/// schema — for a table file being registered.
///
/// The registered structure is produced by the **same** server-side adapter
/// (`CsvAdapter` / `ParquetAdapter`) that will later serve the node, so it is
/// byte-identical to what a read re-derives. This mirrors upstream
/// `register_single_item`, which serializes `adapter.structure()` from the very
/// adapter class the server uses (`tiled/client/register.py:334-336`); the
/// adapter's `structure()` carries the Arrow schema the family-authoritative
/// `TableStructure` parse requires (`tiled/structures/table.py`).
///
/// This is the single owner of register-time table-structure inference: every
/// table-family adapter in the engine routes here rather than hand-rolling a
/// second bespoke schema inference. `from_path` reads the file (CSV: whole file
/// for schema inference + null-column promotion; Parquet: footer only), so the
/// call is dispatched onto a blocking thread.
async fn table_structure_from_file(mimetype: &str, path: PathBuf) -> Result<serde_json::Value> {
    let mimetype = mimetype.to_string();
    tokio::task::spawn_blocking(move || match mimetype.as_str() {
        "text/csv" => csv_table_structure_json(path),
        "application/x-parquet" => parquet_table_structure_json(path),
        other => Err(ClientError::Invalid(format!(
            "no register-time table-structure builder for mimetype '{other}'"
        ))),
    })
    .await
    .map_err(|e| ClientError::Invalid(format!("register inspect join: {e}")))?
}

#[cfg(feature = "csv-adapter")]
fn csv_table_structure_json(path: PathBuf) -> Result<serde_json::Value> {
    use crate::core::adapters::TableAdapterRead;
    let adapter = crate::adapters::CsvAdapter::from_path(path, serde_json::json!({}))
        .map_err(|e| ClientError::Invalid(format!("csv inspect: {e}")))?;
    serde_json::to_value(adapter.structure())
        .map_err(|e| ClientError::Invalid(format!("serialize csv table structure: {e}")))
}

#[cfg(not(feature = "csv-adapter"))]
fn csv_table_structure_json(_path: PathBuf) -> Result<serde_json::Value> {
    Err(ClientError::Invalid(
        "CSV registration requires the `csv-adapter` feature".into(),
    ))
}

#[cfg(feature = "parquet-adapter")]
fn parquet_table_structure_json(path: PathBuf) -> Result<serde_json::Value> {
    use crate::core::adapters::TableAdapterRead;
    let adapter = crate::adapters::ParquetAdapter::from_path(path, serde_json::json!({}))
        .map_err(|e| ClientError::Invalid(format!("parquet inspect: {e}")))?;
    serde_json::to_value(adapter.structure())
        .map_err(|e| ClientError::Invalid(format!("serialize parquet table structure: {e}")))
}

#[cfg(not(feature = "parquet-adapter"))]
fn parquet_table_structure_json(_path: PathBuf) -> Result<serde_json::Value> {
    Err(ClientError::Invalid(
        "Parquet registration requires the `parquet-adapter` feature".into(),
    ))
}

/// CSV adapter — delegates to the server-side `CsvAdapter` to infer a full
/// `table` structure (columns + partition count + base64 Arrow schema).
pub struct CsvAdapter;

#[async_trait]
impl RegistrationAdapter for CsvAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Table
    }
    fn mimetype(&self) -> &str {
        "text/csv"
    }
    async fn inspect(&self, uri: &Url, _is_directory: bool) -> Result<DataSourceSpec> {
        let path = uri
            .to_file_path()
            .map_err(|_| ClientError::Invalid("CSV adapter expects a file:// URI".into()))?;
        let structure = table_structure_from_file("text/csv", path).await?;
        Ok(DataSourceSpec {
            structure_family: StructureFamily::Table,
            mimetype: "text/csv".into(),
            structure: Some(structure),
            parameters: serde_json::json!({}),
            assets: vec![AssetSpec {
                data_uri: uri.to_string(),
                is_directory: false,
                parameter: "data_uri".into(),
                num: None,
            }],
            specs: vec![],
            metadata: serde_json::json!({}),
        })
    }
}

/// Parquet adapter — delegates to the server-side `ParquetAdapter` to infer a
/// full `table` structure (columns + row-group partition count + base64 Arrow
/// schema) from the file's own footer metadata.
pub struct ParquetAdapter;

#[async_trait]
impl RegistrationAdapter for ParquetAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Table
    }
    fn mimetype(&self) -> &str {
        "application/x-parquet"
    }
    async fn inspect(&self, uri: &Url, _is_directory: bool) -> Result<DataSourceSpec> {
        let path = uri
            .to_file_path()
            .map_err(|_| ClientError::Invalid("Parquet adapter expects file:// URI".into()))?;
        let structure = table_structure_from_file("application/x-parquet", path).await?;
        Ok(DataSourceSpec {
            structure_family: StructureFamily::Table,
            mimetype: "application/x-parquet".into(),
            structure: Some(structure),
            parameters: serde_json::json!({}),
            assets: vec![AssetSpec {
                data_uri: uri.to_string(),
                is_directory: false,
                parameter: "data_uri".into(),
                num: None,
            }],
            specs: vec![],
            metadata: serde_json::json!({}),
        })
    }
}

/// JSON adapter — wraps the file as `application/json` with no structure
/// inspection. The server interprets it.
pub struct JsonAdapter;

#[async_trait]
impl RegistrationAdapter for JsonAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Container
    }
    fn mimetype(&self) -> &str {
        "application/json"
    }
    async fn inspect(&self, uri: &Url, _is_directory: bool) -> Result<DataSourceSpec> {
        Ok(DataSourceSpec {
            structure_family: StructureFamily::Container,
            mimetype: "application/json".into(),
            structure: None,
            parameters: serde_json::json!({}),
            assets: vec![AssetSpec {
                data_uri: uri.to_string(),
                is_directory: false,
                parameter: "data_uri".into(),
                num: None,
            }],
            specs: vec![],
            metadata: serde_json::json!({}),
        })
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

pub struct Settings {
    pub adapters: HashMap<String, Arc<dyn RegistrationAdapter>>,
    pub mimetypes_by_ext: HashMap<String, String>,
    pub key_from_filename: Box<dyn Fn(&str) -> String + Send + Sync>,
    pub filter: Box<dyn Fn(&Path) -> bool + Send + Sync>,
}

impl Default for Settings {
    fn default() -> Self {
        let mut adapters: HashMap<String, Arc<dyn RegistrationAdapter>> = HashMap::new();
        adapters.insert("text/csv".into(), Arc::new(CsvAdapter));
        adapters.insert("application/x-parquet".into(), Arc::new(ParquetAdapter));
        adapters.insert("application/json".into(), Arc::new(JsonAdapter));
        Self {
            adapters,
            mimetypes_by_ext: HashMap::new(),
            key_from_filename: Box::new(strip_suffixes),
            filter: Box::new(default_filter),
        }
    }
}

// ---------------------------------------------------------------------------
// Register entry point
// ---------------------------------------------------------------------------

/// Register a file or directory under `node`. Walks recursively.
///
/// `prefix` is split on `/`; intermediate containers are auto-created.
pub async fn register(
    node: &ContainerClient,
    path: &Path,
    prefix: &str,
    settings: &Settings,
    overwrite: bool,
) -> Result<()> {
    let target = navigate_or_create(node, prefix, settings).await?;
    let meta = tokio::fs::metadata(path)
        .await
        .map_err(|e| ClientError::Invalid(format!("stat {}: {e}", path.display())))?;
    if meta.is_dir() {
        if try_register_single(&target, path, true, settings)
            .await?
            .is_some()
        {
            return Ok(());
        }
        if overwrite {
            tracing::info!(target: "tiled.register", "overwriting children of '{prefix}'");
        }
        walk_and_register(&target, path, settings).await?;
    } else {
        try_register_single(&target, path, false, settings).await?;
    }
    Ok(())
}

async fn navigate_or_create(
    node: &ContainerClient,
    prefix: &str,
    settings: &Settings,
) -> Result<ContainerClient> {
    let mut current = node.clone();
    for segment in prefix.split('/').filter(|s| !s.is_empty()) {
        match current.get(segment).await {
            Ok(child) => current = child.into_container()?,
            // Only treat genuine "not found" (or HTTP 404) as "needs create".
            // Network/auth/parse errors are propagated so we don't spuriously
            // create children on transient failures.
            Err(ClientError::KeyNotFound(_)) | Err(ClientError::Server { status: 404, .. }) => {
                let key = (settings.key_from_filename)(segment);
                create_container(&current, &key).await?;
                let child = current.get(&key).await?;
                current = child.into_container()?;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(current)
}

async fn create_container(parent: &ContainerClient, key: &str) -> Result<()> {
    // Wire field is `id`, matching Python tiled's `PostMetadataRequest.id`
    // (tiled/server/schemas.py:462) — a real Python tiled server ignores a
    // top-level `key` field.
    let body = serde_json::json!({
        "structure_family": "container",
        "metadata": {},
        "specs": [],
        "data_sources": [],
        "id": key,
    });
    let url = build_register_url(parent)?;
    // Routed through the drop-collision helper (register.py:168, 240 →
    // create_node_or_drop_collision:623-648): a 409 removes the offending node
    // and returns `Ok`; a non-409 error propagates.
    create_node_or_drop_collision(parent, key, &url, &body).await
}

/// A create POST returned HTTP 409 — the target key is already occupied. The
/// server surfaces a duplicate `(parent, key)` create as 409 (PR #106).
fn is_conflict(err: &ClientError) -> bool {
    matches!(err, ClientError::Server { status: 409, .. })
}

/// Create a node, or drop the collision. Mirrors upstream
/// `create_node_or_drop_collision` (register.py:623-648): POST the create; on a
/// `409 Conflict` remove the pre-existing node occupying the key
/// (`node.get(key)` → `delete(recursive = true)`) and log a `COLLISION`
/// warning, then return `Ok` so the walk continues — neither the original nor
/// the new node remains, avoiding the ambiguity of two source items mapping to
/// one node. Any non-409 error propagates, matching upstream's `else: raise`.
///
/// The create itself stays wrapped in `retry` (transient 5xx/429/connect), so a
/// genuine transient blip is retried before a real collision is ever observed.
async fn create_node_or_drop_collision(
    node: &ContainerClient,
    key: &str,
    url: &Url,
    body: &serde_json::Value,
) -> Result<()> {
    match retry(|| async { node.base().context().post_json(url, body).await }).await {
        Ok(_) => Ok(()),
        Err(e) if is_conflict(&e) => {
            // The offender exists (it just caused the 409); fetch and remove it.
            let offender = node.get(key).await?;
            if let Some(base) = offender.base() {
                base.delete(true, true).await?;
            }
            tracing::warn!(
                target: "tiled.register",
                key,
                "COLLISION: multiple items would result in this node. Skipping all."
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn build_register_url(parent: &ContainerClient) -> Result<Url> {
    let self_link = parent
        .base()
        .uri()
        .ok_or_else(|| ClientError::MissingLink("self".into()))?;
    let mut url = Url::parse(self_link)?;
    // The register endpoint mirrors the metadata path:
    //   /api/v1/register/<...>  ↔  /api/v1/metadata/<...>
    // Replace only the *last* `/metadata/` so a sub-path host whose prefix
    // happens to contain that string isn't mangled.
    let path = url.path().to_string();
    let idx = path
        .rfind("/metadata/")
        .ok_or_else(|| ClientError::Invalid(format!("self link missing /metadata/: {path}")))?;
    let new_path = format!(
        "{}{}{}",
        &path[..idx],
        "/register/",
        &path[idx + "/metadata/".len()..]
    );
    url.set_path(&new_path);
    Ok(url)
}

async fn walk_and_register(node: &ContainerClient, path: &Path, settings: &Settings) -> Result<()> {
    let mut files: Vec<PathBuf> = Vec::new();
    let mut directories: Vec<PathBuf> = Vec::new();
    let mut rd = tokio::fs::read_dir(path)
        .await
        .map_err(|e| ClientError::Invalid(format!("readdir {}: {e}", path.display())))?;
    while let Some(entry) = rd
        .next_entry()
        .await
        .map_err(|e| ClientError::Invalid(format!("readdir entry: {e}")))?
    {
        let p = entry.path();
        let ft = entry
            .file_type()
            .await
            .map_err(|e| ClientError::Invalid(format!("file_type {}: {e}", p.display())))?;
        if ft.is_dir() {
            // Always descend into directories that pass the hidden-name
            // check. A user-supplied `filter` typically targets file
            // extensions (e.g. ".csv"), and applying it to directories
            // would short-circuit the walk before ever reaching the
            // matching files inside. Mirror Python tiled's fix
            // (bluesky/tiled#1370).
            if default_filter(&p) {
                directories.push(p);
            }
        } else {
            if !(settings.filter)(&p) {
                continue;
            }
            files.push(p);
        }
    }

    // Group image sequences first.
    let (sequences, files) = group_image_sequences(files);
    for (name, seq) in sequences {
        // Propagate a real server/transport write failure (401/409/5xx/network)
        // so the walk aborts like Python's `_walk` instead of reporting false
        // success; adapter/mimetype skips are handled inside the helper.
        register_image_sequence(node, &name, &seq, settings).await?;
    }

    for file in files {
        // Same: `try_register_single` returns `Ok(None)` for legitimate skips
        // (no mimetype / no adapter / inspect failure) and only `Err`s on a POST
        // write failure, which must abort the walk rather than be swallowed.
        try_register_single(node, &file, false, settings).await?;
    }
    for dir in directories {
        let key = (settings.key_from_filename)(
            dir.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unnamed"),
        );
        // `create_container` drops a 409 collision (deleting the offender) and
        // propagates any non-409 error, mirroring upstream `_walk`'s
        // `create_node_or_drop_collision` (register.py:240). After a collision
        // the offender is gone, so the following `get` 404s and the walk aborts
        // — exactly upstream's shape (it likewise `get`s the key, then recurses).
        create_container(node, &key).await?;
        let child = node.get(&key).await?.into_container()?;
        Box::pin(walk_and_register(&child, &dir, settings)).await?;
    }
    Ok(())
}

async fn try_register_single(
    node: &ContainerClient,
    path: &Path,
    is_directory: bool,
    settings: &Settings,
) -> Result<Option<()>> {
    let mimetype = resolve_mimetype(path, &settings.mimetypes_by_ext);
    let Some(mimetype) = mimetype else {
        return Ok(None);
    };
    let Some(adapter) = settings.adapters.get(&mimetype).cloned() else {
        tracing::debug!(target: "tiled.register", "no adapter for {mimetype} ({})", path.display());
        return Ok(None);
    };

    let uri =
        Url::from_file_path(path).map_err(|_| ClientError::Invalid("path to file URI".into()))?;
    // Mirror Python's `register_single_item`: a failure constructing/inspecting
    // the adapter for this file is logged and SKIPPED (return), not propagated,
    // so one unreadable file does not abort a bulk directory walk
    // (tiled/client/register.py:316-321 `except Exception: ... return`). Only the
    // server write below (`post_json`) is allowed to propagate an `Err`.
    let spec = match adapter.inspect(&uri, is_directory).await {
        Ok(spec) => spec,
        Err(e) => {
            tracing::warn!(
                target: "tiled.register",
                "SKIPPED: error constructing adapter for {}: {e}",
                path.display()
            );
            return Ok(None);
        }
    };

    let key = (settings.key_from_filename)(
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unnamed"),
    );
    let url = build_register_url(node)?;
    let body = serde_json::json!({
        "structure_family": spec.structure_family,
        "metadata": spec.metadata,
        "specs": spec.specs,
        "data_sources": [{
            "structure_family": spec.structure_family,
            "mimetype": spec.mimetype,
            "structure": spec.structure,
            "parameters": spec.parameters,
            "management": "external",
            "assets": spec.assets,
        }],
        "id": key,
    });
    // Routed through the drop-collision helper (register.py:349 →
    // create_node_or_drop_collision:623-648): a 409 (e.g. a sibling file with
    // the same stem, or a re-run over this node) removes the offender and skips;
    // a non-409 error aborts the walk.
    create_node_or_drop_collision(node, &key, &url, &body).await?;
    Ok(Some(()))
}

// ---------------------------------------------------------------------------
// Image sequence grouping
// ---------------------------------------------------------------------------

fn img_sequence_regex(ext: &str) -> Option<&'static Regex> {
    static TIF: OnceLock<Regex> = OnceLock::new();
    static JPG: OnceLock<Regex> = OnceLock::new();
    static NPY: OnceLock<Regex> = OnceLock::new();
    static PNG: OnceLock<Regex> = OnceLock::new();
    match ext {
        ".tif" | ".tiff" => {
            Some(TIF.get_or_init(|| Regex::new(r"^(.*?)(\d+)\.(?:tif|tiff)$").unwrap()))
        }
        ".jpg" | ".jpeg" => {
            Some(JPG.get_or_init(|| Regex::new(r"^(.*?)(\d+)\.(?:jpg|jpeg)$").unwrap()))
        }
        ".npy" => Some(NPY.get_or_init(|| Regex::new(r"^(.*?)(\d+)\.npy$").unwrap())),
        ".png" => Some(PNG.get_or_init(|| Regex::new(r"^(.*?)(\d+)\.png$").unwrap())),
        _ => None,
    }
}

fn img_sequence_mimetype(ext: &str) -> Option<&'static str> {
    match ext {
        ".tif" | ".tiff" => Some("multipart/related;type=image/tiff"),
        ".jpg" | ".jpeg" => Some("multipart/related;type=image/jpeg"),
        ".npy" => Some("multipart/related;type=application/x-npy"),
        ".png" => Some("multipart/related;type=image/png"),
        _ => None,
    }
}

fn group_image_sequences(files: Vec<PathBuf>) -> (HashMap<String, Vec<PathBuf>>, Vec<PathBuf>) {
    let mut sequences: HashMap<String, Vec<PathBuf>> = HashMap::new();
    let mut unhandled = Vec::new();
    for file in files {
        let Some(name) = file.file_name().and_then(|n| n.to_str()) else {
            unhandled.push(file);
            continue;
        };
        let ext = file
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e.to_ascii_lowercase()));
        let Some(ext) = ext else {
            unhandled.push(file);
            continue;
        };
        let Some(re) = img_sequence_regex(&ext) else {
            unhandled.push(file);
            continue;
        };
        match re.captures(name) {
            Some(caps) => {
                let stem = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                let key = if stem.is_empty() {
                    "_unnamed".to_string()
                } else {
                    stem.to_string()
                };
                sequences.entry(key).or_default().push(file);
            }
            None => unhandled.push(file),
        }
    }
    for v in sequences.values_mut() {
        v.sort();
    }
    (sequences, unhandled)
}

async fn register_image_sequence(
    node: &ContainerClient,
    name: &str,
    sequence: &[PathBuf],
    settings: &Settings,
) -> Result<()> {
    let first = sequence
        .first()
        .ok_or_else(|| ClientError::Invalid("empty image sequence".into()))?;
    let ext = first
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| format!(".{}", e.to_ascii_lowercase()))
        .ok_or_else(|| ClientError::Invalid("no extension on image".into()))?;
    let mimetype = img_sequence_mimetype(&ext)
        .ok_or_else(|| ClientError::Invalid(format!("no mimetype for {ext}")))?;

    let assets: Vec<AssetSpec> = sequence
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let uri = Url::from_file_path(p)
                .ok()
                .map(|u| u.to_string())
                .unwrap_or_default();
            AssetSpec {
                data_uri: uri,
                is_directory: false,
                parameter: "data_uris".into(),
                num: Some(i as u32),
            }
        })
        .collect();

    let key = (settings.key_from_filename)(name);
    let url = build_register_url(node)?;
    let body = serde_json::json!({
        "structure_family": "array",
        "metadata": {},
        "specs": [],
        "data_sources": [{
            "structure_family": "array",
            "mimetype": mimetype,
            "structure": null,
            "parameters": {},
            "management": "external",
            "assets": assets,
        }],
        "id": key,
    });
    // Routed through the drop-collision helper (register.py:437 →
    // create_node_or_drop_collision:623-648): a 409 removes the offender and
    // skips; a non-409 error aborts the walk.
    create_node_or_drop_collision(node, &key, &url, &body).await
}

// ---------------------------------------------------------------------------
// Watch mode
// ---------------------------------------------------------------------------

/// Watch a directory: do an initial walk, then re-walk on changes.
///
/// This blocks the caller until the returned `WatchHandle::stop()` is called.
pub async fn watch(
    node: ContainerClient,
    path: PathBuf,
    prefix: String,
    settings: Arc<Settings>,
) -> Result<WatchHandle> {
    use notify::{RecursiveMode, Watcher};

    // Initial walk.
    register(&node, &path, &prefix, &settings, true).await?;

    let (tx, mut rx) = mpsc::channel::<notify::Result<notify::Event>>(64);
    let mut watcher = notify::recommended_watcher(move |res| {
        // Drop events when the channel is full — the debounce re-walks
        // the tree on the next window so no event is load-bearing.
        let _ = tx.try_send(res);
    })
    .map_err(|e| ClientError::Invalid(format!("create watcher: {e}")))?;
    watcher
        .watch(&path, RecursiveMode::Recursive)
        .map_err(|e| ClientError::Invalid(format!("watch: {e}")))?;

    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop_rx.recv() => break,
                event = rx.recv() => {
                    if event.is_none() { break; }
                    // Debounce: keep waiting up to debounce_window for more
                    // events; coalesce them all into a single re-register.
                    let debounce_window = Duration::from_millis(500);
                    let deadline = tokio::time::Instant::now() + debounce_window;
                    loop {
                        let remaining = deadline.saturating_duration_since(
                            tokio::time::Instant::now()
                        );
                        if remaining.is_zero() {
                            break;
                        }
                        match tokio::time::timeout(remaining, rx.recv()).await {
                            Ok(Some(_)) => continue,
                            Ok(None) | Err(_) => break,
                        }
                    }
                    if let Err(e) = register(&node, &path, &prefix, &settings, false).await {
                        tracing::warn!(target: "tiled.register", "watch re-register failed: {e}");
                    }
                }
            }
        }
        drop(watcher);
    });

    Ok(WatchHandle {
        stop: stop_tx,
        task: Some(task),
    })
}

pub struct WatchHandle {
    stop: mpsc::Sender<()>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl WatchHandle {
    pub async fn stop(mut self) {
        let _ = self.stop.send(()).await;
        if let Some(t) = self.task.take() {
            let _ = t.await;
        }
    }
}

// ---------------------------------------------------------------------------
// walkdir helper for callers that just want a flat file list
// ---------------------------------------------------------------------------

/// Recursively list files under `path` that pass the filter.
pub fn list_files(path: &Path, filter: &dyn Fn(&Path) -> bool) -> Vec<PathBuf> {
    WalkDir::new(path)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && filter(e.path()))
        .map(|e| e.path().to_path_buf())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_csv_mimetype() {
        let p = Path::new("/tmp/foo.csv");
        let mt = resolve_mimetype(p, &HashMap::new());
        assert_eq!(mt.as_deref(), Some("text/csv"));
    }

    #[test]
    fn resolve_compound_mimetype() {
        let mut overrides = HashMap::new();
        overrides.insert(".tar.gz".into(), "application/x-tar+gzip".into());
        let p = Path::new("/tmp/x.tar.gz");
        let mt = resolve_mimetype(p, &overrides);
        assert_eq!(mt.as_deref(), Some("application/x-tar+gzip"));
    }

    #[test]
    fn strip_suffixes_examples() {
        assert_eq!(strip_suffixes("a.tif"), "a");
        assert_eq!(strip_suffixes("thing.tar.gz"), "thing");
        assert_eq!(strip_suffixes("noext"), "noext");
    }

    #[test]
    fn group_sequences_by_stem() {
        let files = vec![
            PathBuf::from("/tmp/scan_001.tif"),
            PathBuf::from("/tmp/scan_002.tif"),
            PathBuf::from("/tmp/other.csv"),
        ];
        let (seqs, others) = group_image_sequences(files);
        assert_eq!(seqs.len(), 1);
        assert!(seqs.contains_key("scan_"));
        assert_eq!(others.len(), 1);
    }

    #[test]
    fn default_filter_drops_hidden() {
        assert!(default_filter(Path::new("/tmp/visible.txt")));
        assert!(!default_filter(Path::new("/tmp/.hidden")));
    }

    // -----------------------------------------------------------------------
    // F3 — the register walk must propagate a real server/transport write
    // failure (not swallow it and report false success), while still skipping
    // files whose adapter cannot be constructed/inspected.
    // -----------------------------------------------------------------------

    use crate::client::base::Item;
    use crate::client::context::Context;

    /// Spawn an axum app on an ephemeral port and return its base URL.
    async fn spawn(app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        format!("http://{addr}")
    }

    /// Build a root container client whose `self` link points at `base`.
    fn container_at(base: &str) -> ContainerClient {
        let (ctx, _) = Context::from_uri(base).unwrap();
        let item: Item = serde_json::from_value(serde_json::json!({
            "id": "mydir",
            "attributes": { "ancestors": [], "structure_family": "container" },
            "links": { "self": format!("{base}/api/v1/metadata/mydir") },
        }))
        .unwrap();
        ContainerClient::from_item(ctx, item, false).unwrap()
    }

    /// Settings that map `.stub` to the given adapter and nothing else.
    fn stub_settings(adapter: Arc<dyn RegistrationAdapter>) -> Settings {
        let mut settings = Settings::default();
        settings.adapters.clear();
        settings
            .adapters
            .insert("application/x-stub".into(), adapter);
        settings
            .mimetypes_by_ext
            .insert(".stub".into(), "application/x-stub".into());
        settings
    }

    #[tokio::test]
    async fn walk_propagates_post_write_failure() {
        async fn always_500() -> axum::http::StatusCode {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
        let app =
            axum::Router::new().route("/api/v1/register/{*path}", axum::routing::post(always_500));
        let base = spawn(app).await;
        let node = container_at(&base);

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.stub"), b"x").unwrap();

        // PassthroughAdapter inspects without touching the file, so the only
        // failure left is the server POST — which must abort the walk.
        let settings = stub_settings(Arc::new(PassthroughAdapter {
            mimetype: "application/x-stub".into(),
            family: StructureFamily::Container,
        }));

        let err = walk_and_register(&node, dir.path(), &settings)
            .await
            .expect_err("a 500 on the register POST must abort the walk");
        match err {
            ClientError::Server { status, .. } => assert_eq!(status, 500),
            other => panic!("expected Server 500, got {other:?}"),
        }
    }

    /// Adapter whose `inspect` always fails — stands in for Python's
    /// "Error constructing adapter" case, which is skipped, not propagated.
    struct FailingInspectAdapter;

    #[async_trait]
    impl RegistrationAdapter for FailingInspectAdapter {
        fn structure_family(&self) -> StructureFamily {
            StructureFamily::Container
        }
        fn mimetype(&self) -> &str {
            "application/x-stub"
        }
        async fn inspect(&self, _uri: &Url, _is_directory: bool) -> Result<DataSourceSpec> {
            Err(ClientError::Invalid("boom: corrupt file".into()))
        }
    }

    #[tokio::test]
    async fn walk_skips_inspect_failure_without_aborting() {
        // The POST endpoint 500s if ever reached; a skipped file must never
        // reach it, so the walk should still return Ok.
        async fn always_500() -> axum::http::StatusCode {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
        let app =
            axum::Router::new().route("/api/v1/register/{*path}", axum::routing::post(always_500));
        let base = spawn(app).await;
        let node = container_at(&base);

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.stub"), b"x").unwrap();

        let settings = stub_settings(Arc::new(FailingInspectAdapter));

        walk_and_register(&node, dir.path(), &settings)
            .await
            .expect("an inspect failure is a logged skip, not a walk abort");
    }

    // -----------------------------------------------------------------------
    // Finding 1 (wave-30): two source files that map to the same node key must
    // NOT abort the walk. Upstream `create_node_or_drop_collision`
    // (register.py:623-648) removes the offending node on a 409 and continues,
    // logging a COLLISION warning; neither the original nor the new node
    // remains. Previously the Rust client propagated the 409 via `?`, aborting
    // the entire registration on the second same-key file.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn walk_drops_collision_and_continues() {
        use std::collections::HashSet;
        use std::sync::Mutex;

        #[derive(Clone)]
        struct St {
            base: String,
            created: Arc<Mutex<HashSet<String>>>,
            deletes: Arc<Mutex<Vec<String>>>,
        }

        // POST create: first create of a key succeeds; a repeat create of the
        // same key is a 409 collision (the server's duplicate-(parent,key) shape).
        async fn post_register(
            axum::extract::State(st): axum::extract::State<St>,
            axum::extract::Path(_path): axum::extract::Path<String>,
            axum::Json(body): axum::Json<serde_json::Value>,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            let id = body
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let mut created = st.created.lock().unwrap();
            if created.contains(&id) {
                return (axum::http::StatusCode::CONFLICT, "already exists").into_response();
            }
            created.insert(id.clone());
            axum::Json(serde_json::json!({ "id": id })).into_response()
        }

        // GET the offender: return a minimal container item whose `self` link
        // points back here so the follow-up DELETE lands on this server.
        async fn get_meta(
            axum::extract::State(st): axum::extract::State<St>,
            axum::extract::Path(path): axum::extract::Path<String>,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            let key = path.rsplit('/').next().unwrap_or_default().to_string();
            axum::Json(serde_json::json!({
                "data": {
                    "id": key,
                    "attributes": { "ancestors": ["mydir"], "structure_family": "container" },
                    "links": { "self": format!("{}/api/v1/metadata/{}", st.base, path) },
                }
            }))
            .into_response()
        }

        // DELETE the offender: record it and drop it from the created set.
        async fn delete_meta(
            axum::extract::State(st): axum::extract::State<St>,
            axum::extract::Path(path): axum::extract::Path<String>,
        ) -> axum::http::StatusCode {
            let key = path.rsplit('/').next().unwrap_or_default().to_string();
            st.deletes.lock().unwrap().push(key.clone());
            st.created.lock().unwrap().remove(&key);
            axum::http::StatusCode::OK
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        let st = St {
            base: base.clone(),
            created: Arc::new(Mutex::new(HashSet::new())),
            deletes: Arc::new(Mutex::new(Vec::new())),
        };
        let app = axum::Router::new()
            .route(
                "/api/v1/register/{*path}",
                axum::routing::post(post_register),
            )
            .route(
                "/api/v1/metadata/{*path}",
                axum::routing::get(get_meta).delete(delete_meta),
            )
            .with_state(st.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let node = container_at(&base);

        // Two files, same stem, different extensions → both strip to key `data`.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("data.aaa"), b"x").unwrap();
        std::fs::write(dir.path().join("data.bbb"), b"y").unwrap();

        // Both extensions resolve to the passthrough adapter, so both files are
        // registered (and thus collide on `data`).
        let adapter: Arc<dyn RegistrationAdapter> = Arc::new(PassthroughAdapter {
            mimetype: "application/x-stub".into(),
            family: StructureFamily::Array,
        });
        let mut settings = Settings::default();
        settings.adapters.clear();
        settings
            .adapters
            .insert("application/x-stub".into(), adapter);
        settings
            .mimetypes_by_ext
            .insert(".aaa".into(), "application/x-stub".into());
        settings
            .mimetypes_by_ext
            .insert(".bbb".into(), "application/x-stub".into());

        walk_and_register(&node, dir.path(), &settings)
            .await
            .expect("a same-key collision must be dropped, not abort the walk");

        // The second create collided → the offender `data` was deleted.
        assert_eq!(
            *st.deletes.lock().unwrap(),
            vec!["data".to_string()],
            "the offending node must be deleted exactly once on collision"
        );
        // Neither the original nor the new node survives (deleted-on-collision).
        assert!(
            !st.created.lock().unwrap().contains("data"),
            "no `data` node may remain after the collision is dropped"
        );
    }

    // -----------------------------------------------------------------------
    // Wire-format regression: the create-node body must carry the requested
    // name under `id` (Python tiled's `PostMetadataRequest.id`,
    // server/schemas.py:462), not a top-level `key` — a real Python tiled
    // server ignores `key` and silently auto-generates a name instead.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn create_container_body_carries_id_not_key() {
        use std::sync::Mutex;

        #[derive(Clone)]
        struct Captured(Arc<Mutex<Option<serde_json::Value>>>);

        async fn capture(
            axum::extract::State(captured): axum::extract::State<Captured>,
            axum::Json(body): axum::Json<serde_json::Value>,
        ) -> axum::http::StatusCode {
            *captured.0.lock().unwrap() = Some(body);
            axum::http::StatusCode::CREATED
        }

        let captured = Captured(Arc::new(Mutex::new(None)));
        let app = axum::Router::new()
            .route("/api/v1/register/{*path}", axum::routing::post(capture))
            .with_state(captured.clone());
        let base = spawn(app).await;
        let node = container_at(&base);

        create_container(&node, "sub")
            .await
            .expect("create_container POST must succeed");

        let body = captured
            .0
            .lock()
            .unwrap()
            .clone()
            .expect("register handler must have captured a body");
        assert_eq!(
            body["id"], "sub",
            "wire body must carry `id` (Python tiled PostMetadataRequest.id, schemas.py:462)"
        );
        assert!(
            body.get("key").is_none(),
            "client must not emit a top-level `key` field; a real Python tiled server ignores it"
        );
    }
}
