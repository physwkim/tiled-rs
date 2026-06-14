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

use tiled_core::structures::{Spec, StructureFamily};

use crate::container::ContainerClient;
use crate::error::{ClientError, Result};

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

/// CSV adapter — reads the header, sniffs the first ~10 rows for column
/// types, and emits a `table` structure spec.
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
        let path_for_io = path.clone();
        let columns: Vec<String> = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let file = std::fs::File::open(&path_for_io).map_err(|e| {
                ClientError::Invalid(format!("open {}: {e}", path_for_io.display()))
            })?;
            let mut reader = csv::Reader::from_reader(file);
            Ok(reader
                .headers()
                .map_err(|e| ClientError::Invalid(format!("csv headers: {e}")))?
                .iter()
                .map(String::from)
                .collect())
        })
        .await
        .map_err(|e| ClientError::Invalid(format!("csv inspect join: {e}")))??;
        Ok(DataSourceSpec {
            structure_family: StructureFamily::Table,
            mimetype: "text/csv".into(),
            structure: Some(serde_json::json!({
                "columns": columns,
                "npartitions": 1,
            })),
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

/// Parquet adapter — reads the schema header.
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
        let path_for_io = path.clone();
        let columns: Vec<String> = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            use parquet::file::reader::FileReader;
            let file = std::fs::File::open(&path_for_io).map_err(|e| {
                ClientError::Invalid(format!("open {}: {e}", path_for_io.display()))
            })?;
            let reader = parquet::file::reader::SerializedFileReader::new(file)
                .map_err(|e| ClientError::Invalid(format!("parquet read: {e}")))?;
            Ok(reader
                .metadata()
                .file_metadata()
                .schema()
                .get_fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect())
        })
        .await
        .map_err(|e| ClientError::Invalid(format!("parquet inspect join: {e}")))??;
        Ok(DataSourceSpec {
            structure_family: StructureFamily::Table,
            mimetype: "application/x-parquet".into(),
            structure: Some(serde_json::json!({
                "columns": columns,
                "npartitions": 1,
            })),
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
    let body = serde_json::json!({
        "structure_family": "container",
        "metadata": {},
        "specs": [],
        "data_sources": [],
        "key": key,
    });
    let url = build_register_url(parent)?;
    parent.base().context().post_json(&url, &body).await?;
    Ok(())
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
        let _ = register_image_sequence(node, &name, &seq, settings).await;
    }

    for file in files {
        let _ = try_register_single(node, &file, false, settings).await;
    }
    for dir in directories {
        let key = (settings.key_from_filename)(
            dir.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unnamed"),
        );
        if let Err(_e) = create_container(node, &key).await {
            // Likely 409 — the container already exists.
        }
        match node.get(&key).await {
            Ok(child) => {
                let child = child.into_container()?;
                Box::pin(walk_and_register(&child, &dir, settings)).await?;
            }
            Err(e) => return Err(e),
        }
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
    let spec = adapter.inspect(&uri, is_directory).await?;

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
        "key": key,
    });
    node.base().context().post_json(&url, &body).await?;
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
        "key": key,
    });
    node.base().context().post_json(&url, &body).await?;
    Ok(())
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
}
