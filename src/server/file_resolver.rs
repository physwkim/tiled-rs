//! File-format leaf resolver.
//!
//! Wires the catalog's `LeafResolver` trait to the file-system adapters in
//! `tiled-adapters`. Picks the right adapter based on the data source's
//! mimetype and decodes the first asset's `data_uri`. Multi-asset
//! sequences (NPY_SEQ etc.) and the more exotic formats are intentionally
//! not handled here — operators with those workloads can wrap or replace
//! this resolver.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::catalog::adapter::LeafResolver;
use crate::catalog::error::CatalogError;
use crate::catalog::orm::Node;
use crate::core::adapters::{AnyAdapter, BoxFuture};
use crate::server::resource_cache::{AdapterCache, CacheKey};

/// Resolver that decodes `file://` URIs and dispatches to the right
/// `tiled-adapters` implementation.
///
/// Reads are **deny-by-default**, matching Python tiled
/// (`catalog/adapter.py` refuses to serve an asset outside the readable
/// storage area): a `Restricted` resolver serves a
/// `file:` path only when its canonicalised location lives under one of the
/// configured directories, and an empty directory list serves nothing — so
/// a registered `file:///etc/passwd` is refused unless an operator has
/// explicitly allowed that location. Serving every path requires the
/// explicit [`unrestricted`](Self::unrestricted) opt-out.
pub struct FileLeafResolver {
    scope: ReadScope,
    /// TLRU cache of already-built read-only leaf adapters (upstream tiled's
    /// `adapters/resource_cache.py`). Shared as a single owner: the server
    /// wraps one `FileLeafResolver` in an `Arc`, so this cache is
    /// process-wide. Writable adapters are never inserted (see
    /// [`crate::server::resource_cache`]).
    cache: Arc<AdapterCache>,
}

/// What a [`FileLeafResolver`] may read off disk.
///
/// Modelling the scope as a sum type removes the old dual meaning of an
/// empty `Vec<PathBuf>` (was it "no restriction" or "deny everything"?):
/// [`Unrestricted`](ReadScope::Unrestricted) is the explicit opt-out, and
/// `Restricted([])` is the safe deny-all default.
#[derive(Clone)]
enum ReadScope {
    /// Serve any `file:` path with no containment check. The explicit,
    /// audited opt-out — never a default.
    Unrestricted,
    /// Serve only `file:` paths under one of these directories. An empty
    /// list serves nothing (deny-by-default).
    Restricted(Vec<PathBuf>),
}

impl FileLeafResolver {
    /// Construct a resolver restricted to `allowed_data_dirs`
    /// (deny-by-default).
    ///
    /// An **empty** list serves no files at all — every file-backed read is
    /// refused until at least one directory is configured — so it is logged
    /// as a `warn!`: the misconfiguration is not silent. For a deliberately
    /// unrestricted server use [`FileLeafResolver::unrestricted`].
    pub fn new(allowed_data_dirs: Vec<PathBuf>) -> Self {
        if allowed_data_dirs.is_empty() {
            tracing::warn!(
                "FileLeafResolver has no allowed-data-dir: file-backed reads will be \
                 refused. Pass --allowed-data-dir (repeatable) to allow specific \
                 directories, or --allow-unrestricted-reads to serve any path."
            );
        }
        Self {
            scope: ReadScope::Restricted(allowed_data_dirs),
            cache: Arc::new(AdapterCache::from_env()),
        }
    }

    /// Explicit opt-out: serve any `file:` path with no containment check.
    ///
    /// Reachable only by deliberate choice (the CLI's
    /// `--allow-unrestricted-reads`, or tests). Logged as a `warn!` because
    /// an unrestricted server will serve any local file a registered
    /// `data_uri` points at.
    pub fn unrestricted() -> Self {
        tracing::warn!(
            "FileLeafResolver is unrestricted: the server will serve any local file \
             referenced by a registered data_uri, with no path containment."
        );
        Self {
            scope: ReadScope::Unrestricted,
            cache: Arc::new(AdapterCache::from_env()),
        }
    }

    /// Replace the resource cache — the injection seam mirroring upstream's
    /// `set_resource_cache`. Used by tests to drive a deterministic
    /// (manual-clock or capacity-bounded) cache; production always uses the
    /// env-configured cache from the constructors above.
    #[cfg(test)]
    fn with_resource_cache(mut self, cache: Arc<AdapterCache>) -> Self {
        self.cache = cache;
        self
    }
}

/// Containment check, run on the blocking pool (`canonicalize` is a
/// filesystem stat). Deny-by-default: an
/// [`Unrestricted`](ReadScope::Unrestricted) scope permits everything, a
/// [`Restricted`](ReadScope::Restricted) scope permits only paths under one
/// of its directories, and an empty `Restricted` list permits nothing.
fn check_allowed(scope: &ReadScope, path: &Path) -> std::result::Result<(), CatalogError> {
    let allowed_data_dirs = match scope {
        ReadScope::Unrestricted => return Ok(()),
        ReadScope::Restricted(dirs) => dirs,
    };
    if allowed_data_dirs.is_empty() {
        return Err(CatalogError::Validation(format!(
            "refusing to read {}: no allowed-data-dir configured (pass --allowed-data-dir, \
             or --allow-unrestricted-reads to disable the check)",
            path.display()
        )));
    }
    // Resolve symlinks before comparing so a malicious symlink inside
    // an allowed dir can't escape.
    let canonical = path.canonicalize().map_err(|e| {
        CatalogError::Validation(format!("data path {} not accessible: {e}", path.display()))
    })?;
    for allowed in allowed_data_dirs {
        let allowed_canon = allowed.canonicalize().unwrap_or_else(|_| allowed.clone());
        if canonical.starts_with(&allowed_canon) {
            return Ok(());
        }
    }
    Err(CatalogError::Validation(format!(
        "data path {} is outside the allowed_data_dirs allow-list",
        canonical.display()
    )))
}

/// Whether `path` lives under one of the configured writable-storage
/// directories. Empty list (the default) means nothing is writable, so a
/// server with no `--writable-storage` serves every adapter read-only. Run on
/// the blocking pool alongside [`check_allowed`] (it `canonicalize`s, a stat).
fn is_writable_path(writable_storage: &[PathBuf], path: &Path) -> bool {
    if writable_storage.is_empty() {
        return false;
    }
    let Ok(canonical) = path.canonicalize() else {
        return false;
    };
    writable_storage.iter().any(|dir| {
        let dir_canon = dir.canonicalize().unwrap_or_else(|_| dir.clone());
        canonical.starts_with(&dir_canon)
    })
}

impl Default for FileLeafResolver {
    /// The default is the safe deny-all: an empty
    /// `Restricted` scope that serves no files until
    /// directories are configured. Routes through
    /// [`new`](FileLeafResolver::new) so the empty-list warning fires.
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl LeafResolver for FileLeafResolver {
    fn resolve<'a>(
        &'a self,
        catalog: &'a Catalog,
        node: &'a Node,
    ) -> BoxFuture<'a, std::result::Result<AnyAdapter, CatalogError>> {
        Box::pin(async move {
            // Data-source / asset lookups are async sqlx — await on the executor.
            let data_sources = catalog.list_data_sources(node.id).await?;
            let ds = data_sources.first().ok_or_else(|| {
                CatalogError::Validation(format!(
                    "node {} has no data_sources to resolve",
                    node.key
                ))
            })?;
            let assets = catalog.list_assets(ds.id).await?;
            let asset = assets.first().ok_or_else(|| {
                CatalogError::Validation(format!("data_source {} has no assets", ds.id))
            })?;

            // Ragged-SQL nodes resolve from a `sqlite://` data_uri plus the SQL
            // coordinates persisted as data-source parameters, not from a
            // file:// header read — so they branch out before `uri_to_path`
            // (which only accepts file://) and before the file-adapter path.
            #[cfg(feature = "sql-adapter")]
            if ds.mimetype == crate::core::media_type::mime::RAGGED_SQL {
                let scope = self.scope.clone();
                let writable_storage = catalog.writable_storage().to_vec();
                let data_uri = asset.data_uri.clone();
                let structure_json = ds.structure.clone();
                let parameters = ds.parameters.clone();
                let metadata = node.metadata.clone();
                // The only blocking work is the two `canonicalize` containment
                // checks; adapter construction itself opens no connection.
                return tokio::task::spawn_blocking(move || {
                    build_ragged_sql_adapter(
                        &scope,
                        &writable_storage,
                        &data_uri,
                        &structure_json,
                        &parameters,
                        metadata,
                    )
                })
                .await
                .map_err(|e| {
                    CatalogError::Validation(format!("ragged-SQL resolve task failed: {e}"))
                })?;
            }

            // Sparse (COO) nodes resolve from *all* block assets — one parquet
            // file per block — not just the first, so they branch out before the
            // single-asset `uri_to_path` below. The block files may not exist yet
            // on a created-but-unwritten node (init_storage lays out the assets
            // but leaves each file to its first write), so containment is checked
            // on the block directory rather than each file.
            #[cfg(feature = "parquet")]
            if ds.mimetype == crate::core::media_type::mime::SPARSE_BLOCKS_PARQUET {
                // `list_assets` already orders by `num`; sort again so the
                // block-grid order the adapter reconstructs is local and explicit
                // rather than implied by the query.
                let mut ordered: Vec<_> = assets.iter().collect();
                ordered.sort_by_key(|a| a.num.unwrap_or(0));
                let mut paths = Vec::with_capacity(ordered.len());
                for a in &ordered {
                    paths.push(uri_to_path(&a.data_uri)?);
                }
                let scope = self.scope.clone();
                let writable_storage = catalog.writable_storage().to_vec();
                let structure_json = ds.structure.clone();
                let metadata = node.metadata.clone();
                return tokio::task::spawn_blocking(move || {
                    build_sparse_blocks_adapter(
                        &scope,
                        &writable_storage,
                        paths,
                        &structure_json,
                        metadata,
                    )
                })
                .await
                .map_err(|e| {
                    CatalogError::Validation(format!("sparse resolve task failed: {e}"))
                })?;
            }

            let path = uri_to_path(&asset.data_uri)?;
            let scope = self.scope.clone();
            // Write-containment mirror of `scope`: an adapter is built writable
            // only when its backing file lives under the catalog's configured
            // writable storage. This is the single gate that decides
            // writability, so `as_writable().is_some()` ⟹ the file is under
            // writable storage (no per-endpoint path re-check needed).
            let writable_storage = catalog.writable_storage().to_vec();
            let mimetype = ds.mimetype.clone();
            let parameters = ds.parameters.clone();
            let metadata = node.metadata.clone();
            // Awkward is a `file://` directory asset like zarr, so it resolves
            // through the same path — but its structure (form + length) is not
            // stored on disk, so it travels from the catalog data_source here.
            let structure = ds.structure.clone();

            // Resource-cache fast path (upstream `with_resource_cache`): a
            // previously built *read-only* adapter for this exact
            // (mimetype, path, recipe) is returned without touching the
            // filesystem — no re-open, no re-parse, and not even the
            // containment `canonicalize`, since a cached entry only exists
            // because `check_allowed` already passed when it was built and the
            // scope is fixed for this resolver's lifetime.
            let cache_key = CacheKey::new(&mimetype, &path, &parameters, &structure, &metadata);
            if let Some(cached) = self.cache.get(&cache_key) {
                return Ok(cached);
            }
            let cache = self.cache.clone();

            // The allow-list check (`canonicalize`) and adapter construction
            // (`from_path` reads the data file's header) are blocking
            // filesystem work — offload to the blocking pool so the async
            // `get` that awaits this never parks the executor.
            tokio::task::spawn_blocking(move || {
                check_allowed(&scope, &path)?;
                let writable = is_writable_path(&writable_storage, &path);
                let adapter = build_leaf_adapter(
                    &mimetype,
                    path,
                    &parameters,
                    structure,
                    metadata,
                    writable,
                )?;
                // Cache only read-only adapters. A writable adapter's in-memory
                // snapshot goes stale after a write (it writes the file but
                // cannot refresh its own `&self`), so writable paths must
                // rebuild fresh every request — matching upstream, which caches
                // only its read-only npy/jpeg/tiff adapters. The lock is not
                // held across the build above, so two concurrent misses may
                // both build and both insert (benign last-write-wins race,
                // exactly as upstream's `with_resource_cache`).
                if !writable {
                    cache.insert(cache_key, adapter.clone());
                }
                Ok(adapter)
            })
            .await
            .map_err(|e| CatalogError::Validation(format!("leaf resolve task failed: {e}")))?
        })
    }
}

/// Build a leaf adapter from a decoded file path + mimetype. Pure blocking
/// work (each `from_path` reads the file's header), so it is only ever called
/// inside `spawn_blocking`.
fn build_leaf_adapter(
    mimetype: &str,
    path: PathBuf,
    // Only the (feature-gated) hdf5 and zarr arms read `parameters`; without
    // either feature the binding is genuinely unused.
    #[cfg_attr(
        not(any(feature = "hdf5-adapter", feature = "zarr-adapter")),
        allow(unused_variables)
    )]
    parameters: &serde_json::Value,
    // Only the awkward arm reads this: its form + length are not stored on disk
    // (the buffer directory holds only raw buffers), so the structure travels
    // from the catalog data_source as raw JSON. Every other arm derives its
    // structure from the file it opens.
    structure: serde_json::Value,
    metadata: serde_json::Value,
    // Whether the resolver decided this backing file is under writable storage.
    // Only adapters that implement a writer act on it; the rest are read-only
    // regardless.
    writable: bool,
) -> std::result::Result<AnyAdapter, CatalogError> {
    let any_adapter = match mimetype {
        "application/x-npy" | "application/x-numpy" | "npy" => {
            let mut adapter = crate::adapters::NpyAdapter::from_path(path, metadata)
                .map_err(|e| CatalogError::Validation(e.to_string()))?;
            if writable {
                adapter = adapter.into_writable();
            }
            AnyAdapter::Array(Arc::new(adapter))
        }
        "image/tiff" | "image/x-tiff" | "tiff" => {
            #[cfg(feature = "tiff-adapter")]
            {
                let adapter = crate::adapters::TiffAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Array(Arc::new(adapter))
            }
            #[cfg(not(feature = "tiff-adapter"))]
            {
                return Err(CatalogError::Validation("tiff support not built in".into()));
            }
        }
        "image/png" | "image/jpeg" => {
            #[cfg(feature = "tiff-adapter")]
            {
                let adapter = crate::adapters::ImageAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Array(Arc::new(adapter))
            }
            #[cfg(not(feature = "tiff-adapter"))]
            {
                return Err(CatalogError::Validation(
                    "image (png/jpeg) support not built in".into(),
                ));
            }
        }
        "application/x-hdf5" | "application/x-nexus" => {
            #[cfg(feature = "hdf5-adapter")]
            {
                let dataset = parameters
                    .get("dataset")
                    .and_then(|v| v.as_str())
                    .unwrap_or("entry/data/data");
                let adapter = crate::adapters::Hdf5Adapter::from_path(path, dataset, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Array(Arc::new(adapter))
            }
            #[cfg(not(feature = "hdf5-adapter"))]
            {
                return Err(CatalogError::Validation("hdf5 support not built in".into()));
            }
        }
        "application/vnd.apache.arrow.file" => {
            #[cfg(feature = "arrow-ipc")]
            {
                let adapter = crate::adapters::ArrowIpcAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Table(Arc::new(adapter))
            }
            #[cfg(not(feature = "arrow-ipc"))]
            {
                return Err(CatalogError::Validation(
                    "arrow IPC support not built in".into(),
                ));
            }
        }
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => {
            #[cfg(feature = "excel-adapter")]
            {
                // A workbook resolves to a container of per-sheet tables (keyed
                // by sheet name), matching upstream `ExcelAdapter(MapAdapter[
                // TableAdapter])` (adapters/excel.py:14) — the former single-Table
                // resolution dropped every sheet but the first. Each sheet is a
                // `TableAdapterRead` child served through the standard container
                // `get()` / `walk_tree` path.
                let container = crate::adapters::ExcelAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Container(Arc::new(container))
            }
            #[cfg(not(feature = "excel-adapter"))]
            {
                return Err(CatalogError::Validation(
                    "excel (.xlsx) support not built in".into(),
                ));
            }
        }
        "text/csv" => {
            #[cfg(feature = "csv-adapter")]
            {
                let mut adapter = crate::adapters::CsvAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                if writable {
                    adapter = adapter.into_writable();
                }
                AnyAdapter::Table(Arc::new(adapter))
            }
            #[cfg(not(feature = "csv-adapter"))]
            {
                return Err(CatalogError::Validation("csv support not built in".into()));
            }
        }
        "application/x-parquet" => {
            #[cfg(feature = "parquet-adapter")]
            {
                let mut adapter = crate::adapters::ParquetAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                if writable {
                    adapter = adapter.into_writable();
                }
                AnyAdapter::Table(Arc::new(adapter))
            }
            #[cfg(not(feature = "parquet-adapter"))]
            {
                return Err(CatalogError::Validation(
                    "parquet support not built in".into(),
                ));
            }
        }
        "application/x-zarr" => {
            #[cfg(feature = "zarr-adapter")]
            {
                // The store is a directory; the array lives at `array_path`
                // (default `MANAGED_ARRAY_PATH`, what `init_storage_zarr`
                // creates). An externally-registered store can override it.
                let array_path = parameters
                    .get("array_path")
                    .and_then(|v| v.as_str())
                    .unwrap_or(crate::adapters::MANAGED_ARRAY_PATH);
                let mut adapter =
                    crate::adapters::ZarrAdapter::from_path(path, array_path, metadata)
                        .map_err(|e| CatalogError::Validation(e.to_string()))?;
                if writable {
                    adapter = adapter.into_writable();
                }
                AnyAdapter::Array(Arc::new(adapter))
            }
            #[cfg(not(feature = "zarr-adapter"))]
            {
                return Err(CatalogError::Validation("zarr support not built in".into()));
            }
        }
        "application/x-netcdf" | "application/netcdf" => {
            #[cfg(feature = "netcdf-adapter")]
            {
                let adapter = crate::adapters::netcdf_from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Container(Arc::new(adapter))
            }
            #[cfg(not(feature = "netcdf-adapter"))]
            {
                return Err(CatalogError::Validation(
                    "netcdf support not built in".into(),
                ));
            }
        }
        "application/x-awkward-buffers" => {
            // A directory of raw buffer files (one per form key). The form +
            // length are not on disk, so they come from the catalog structure
            // JSON (persisted verbatim at create time).
            let awk =
                crate::core::structures::AwkwardStructure::from_json(&structure).map_err(|e| {
                    CatalogError::Validation(format!(
                        "awkward node data_source has no valid awkward structure: {e}"
                    ))
                })?;
            let mut adapter =
                crate::adapters::AwkwardBuffersAdapter::from_path(path, awk, metadata);
            if writable {
                adapter = adapter.into_writable();
            }
            AnyAdapter::Awkward(Arc::new(adapter))
        }
        other => {
            return Err(CatalogError::Validation(format!(
                "no built-in adapter for mimetype: {other}"
            )));
        }
    };
    Ok(any_adapter)
}

/// Build a ragged-SQL leaf adapter from a `sqlite://` data_uri and the SQL
/// coordinates the create path persisted as data-source parameters. Pure
/// blocking work (two `canonicalize` containment checks); construction opens no
/// connection, so it is only ever called inside `spawn_blocking`.
///
/// Read containment (`check_allowed`) applies exactly as for file adapters: a
/// registered `sqlite://` path is served only when it lives under the read
/// allow-list. Writability is gated on writable-storage containment, the same
/// single gate the file adapters use — so `as_writable().is_some()` ⟹ the
/// database file is under writable storage.
#[cfg(feature = "sql-adapter")]
fn build_ragged_sql_adapter(
    scope: &ReadScope,
    writable_storage: &[PathBuf],
    data_uri: &str,
    structure_json: &serde_json::Value,
    parameters: &serde_json::Value,
    metadata: serde_json::Value,
) -> std::result::Result<AnyAdapter, CatalogError> {
    let path = sqlite_uri_to_path(data_uri)?;
    check_allowed(scope, &path)?;
    let writable = is_writable_path(writable_storage, &path);

    let structure = crate::core::structures::RaggedStructure::from_json(structure_json)
        .map_err(|e| CatalogError::Validation(e.to_string()))?;
    let table_name = parameters
        .get("table_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            CatalogError::Validation(
                "ragged-SQL data source is missing the `table_name` parameter".into(),
            )
        })?;
    let dataset_id = parameters
        .get("dataset_id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            CatalogError::Validation(
                "ragged-SQL data source is missing the `dataset_id` parameter".into(),
            )
        })?;

    let mut adapter = crate::adapters::RaggedSQLAdapter::new(
        data_uri.to_string(),
        table_name,
        dataset_id,
        structure,
        metadata,
        Vec::new(),
    )
    .map_err(|e| CatalogError::Validation(e.to_string()))?;
    if writable {
        adapter = adapter.into_writable();
    }
    Ok(AnyAdapter::Ragged(Arc::new(adapter)))
}

/// Build a sparse-blocks-parquet leaf adapter from its ordered block file paths
/// and the COO structure the create path persisted. Pure blocking work (two
/// `canonicalize` containment checks on the block directory), so it is only ever
/// called inside `spawn_blocking`.
///
/// Containment (`check_allowed`) and writability (`is_writable_path`) are checked
/// on the block *directory* — the location `init_storage_sparse_parquet` created
/// — not on each block file: a created-but-unwritten node has no block files yet,
/// and `canonicalize` would fail on a missing path. The block files still live
/// inside that directory, so the directory is the correct containment unit.
///
/// The COO structure (shape + chunks + data dtype + dims) is not on disk — the
/// block parquet files hold only coords + values — so it travels from the
/// catalog data_source JSON, exactly as `SparseBlocksParquetAdapter::from_paths`
/// needs it. Writable only when the directory is under writable storage, the
/// same single write-containment gate the other managed adapters use.
#[cfg(feature = "parquet")]
fn build_sparse_blocks_adapter(
    scope: &ReadScope,
    writable_storage: &[PathBuf],
    paths: Vec<PathBuf>,
    structure_json: &serde_json::Value,
    metadata: serde_json::Value,
) -> std::result::Result<AnyAdapter, CatalogError> {
    let directory = paths
        .first()
        .and_then(|p| p.parent())
        .ok_or_else(|| {
            CatalogError::Validation("sparse node has no block assets to resolve".into())
        })?
        .to_path_buf();
    check_allowed(scope, &directory)?;
    let writable = is_writable_path(writable_storage, &directory);

    let structure = crate::core::structures::SparseStructure::from_json(structure_json)
        .map_err(|e| CatalogError::Validation(e.to_string()))?;
    let data_dtype = match &structure.data_type {
        Some(crate::core::dtype::DType::Builtin(b)) => b.clone(),
        _ => {
            return Err(CatalogError::Validation(
                "sparse node data_source has no builtin value dtype to resolve".into(),
            ));
        }
    };
    let mut adapter = crate::adapters::SparseBlocksParquetAdapter::from_paths(
        paths,
        structure.shape.clone(),
        structure.chunks.clone(),
        data_dtype,
        structure.dims.clone(),
        metadata,
    )
    .map_err(|e| CatalogError::Validation(e.to_string()))?;
    if writable {
        adapter = adapter.into_writable();
    }
    Ok(AnyAdapter::Sparse(Arc::new(adapter)))
}

/// Decode a `sqlite://` data_uri into its backing database file path, for the
/// read allow-list and writable-containment checks. The create path emits
/// `sqlite://{absolute_path}`: `sqlite:///abs/...` on Unix, `sqlite://C:\...` on
/// Windows. Strip the scheme and any `?query` suffix sqlx would accept and
/// require the result to be absolute, so a `sqlite://relative` URI cannot slip
/// past the containment checks.
#[cfg(feature = "sql-adapter")]
fn sqlite_uri_to_path(uri: &str) -> std::result::Result<PathBuf, CatalogError> {
    let rest = uri.strip_prefix("sqlite://").ok_or_else(|| {
        CatalogError::Validation(format!(
            "ragged data_uri {uri} must use the sqlite:// scheme"
        ))
    })?;
    let path_part = rest.split('?').next().unwrap_or(rest);
    let path = PathBuf::from(path_part);
    // Use the platform's own notion of absoluteness rather than a leading-`/`
    // test: on Windows the create path emits a drive-letter path (`C:\...`) that
    // is absolute but does not start with `/`, so the old check rejected every
    // managed ragged write there (HTTP 500). `is_absolute()` accepts both
    // `/abs/...` and `C:\...` while still rejecting `sqlite://relative`.
    if !path.is_absolute() {
        return Err(CatalogError::Validation(format!(
            "ragged data_uri {uri} has no absolute sqlite path"
        )));
    }
    Ok(path)
}

/// Decode a `data_uri` into a local filesystem path.
///
/// Mirrors Python tiled's `path_from_uri` (`tiled/utils.py`): only the
/// `file:` scheme maps to a local path. A bare absolute path with no scheme
/// (`/etc/passwd`) or any other scheme (`s3://`, `http://`, …) is rejected,
/// so a registered `data_uri` cannot read an arbitrary file off disk by
/// skipping the scheme. (Previously a `starts_with('/')` fallback accepted
/// scheme-less absolute paths — that bypass is N1 and is removed here.)
///
/// Both authority forms decode to the same absolute path: `file:///a/b/c`
/// (empty authority) and `file://localhost/a/b/c` (host authority) both yield
/// `/a/b/c`. Delegates to the shared cross-platform [`crate::core::file_uri`]
/// parser, so the form a write path produced (`file:///C:/...` on Windows)
/// round-trips here.
fn uri_to_path(uri: &str) -> std::result::Result<PathBuf, CatalogError> {
    crate::core::file_uri::file_uri_to_path(uri).ok_or_else(|| {
        CatalogError::Validation(format!("data_uri {uri} must be a valid file:// path"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // The decoded path string is platform-specific: a drive-less `/data/...`
    // is absolute on Unix but not on Windows (`to_file_path` needs a drive),
    // so each platform asserts its own absolute form.
    #[cfg(unix)]
    #[test]
    fn uri_to_path_accepts_empty_authority() {
        assert_eq!(
            uri_to_path("file:///data/scan001.h5").unwrap(),
            PathBuf::from("/data/scan001.h5")
        );
    }

    #[cfg(windows)]
    #[test]
    fn uri_to_path_accepts_empty_authority() {
        assert_eq!(
            uri_to_path("file:///C:/data/scan001.h5").unwrap(),
            PathBuf::from(r"C:\data\scan001.h5")
        );
    }

    // file://localhost/... -> same path as file:///... (url strips localhost),
    // matching Python's urlparse; the decoded path is platform-specific.
    #[cfg(unix)]
    #[test]
    fn uri_to_path_strips_host_authority() {
        assert_eq!(
            uri_to_path("file://localhost/a/b").unwrap(),
            PathBuf::from("/a/b")
        );
    }

    #[cfg(windows)]
    #[test]
    fn uri_to_path_strips_host_authority() {
        assert_eq!(
            uri_to_path("file://localhost/C:/a/b").unwrap(),
            PathBuf::from(r"C:\a\b")
        );
    }

    #[test]
    fn uri_to_path_rejects_bare_absolute_path() {
        // N1: a scheme-less absolute path must not bypass the file:// check.
        assert!(uri_to_path("/etc/passwd").is_err());
    }

    #[test]
    fn uri_to_path_rejects_other_schemes() {
        assert!(uri_to_path("s3://bucket/key").is_err());
        assert!(uri_to_path("http://host/p").is_err());
        assert!(uri_to_path("sqlite:///db.sqlite").is_err());
    }

    #[test]
    fn uri_to_path_rejects_malformed_file_uri() {
        assert!(uri_to_path("file://").is_err());
        assert!(uri_to_path("file://relative-no-slash").is_err());
    }

    #[test]
    fn empty_restricted_scope_denies_all() {
        // N2 deny-by-default: an empty allow-list serves nothing. The gate
        // refuses before touching the filesystem, so even a non-existent
        // path is rejected rather than permitted.
        assert!(
            check_allowed(&ReadScope::Restricted(Vec::new()), Path::new("/etc/passwd")).is_err()
        );
    }

    #[test]
    fn unrestricted_scope_permits_any_path() {
        // The explicit opt-out serves everything, no filesystem check.
        assert!(check_allowed(&ReadScope::Unrestricted, Path::new("/etc/passwd")).is_ok());
    }

    #[test]
    fn writable_path_requires_containment_in_writable_storage() {
        // Write-containment mirror of the read allow-list: a file is writable
        // only when it lives under a configured writable-storage dir. Empty
        // list = nothing writable (read-only server).
        let writable = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let inside = writable.path().join("a.npy");
        let outside = other.path().join("b.npy");
        std::fs::write(&inside, b"x").unwrap();
        std::fs::write(&outside, b"x").unwrap();

        let dirs = vec![writable.path().to_path_buf()];
        assert!(is_writable_path(&dirs, &inside));
        assert!(!is_writable_path(&dirs, &outside));
        // No writable storage configured → nothing is writable.
        assert!(!is_writable_path(&[], &inside));
    }

    #[test]
    fn restricted_scope_contains_reads() {
        let inside = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let inside_file = inside.path().join("data.csv");
        let outside_file = outside.path().join("secret.csv");
        std::fs::write(&inside_file, b"x").unwrap();
        std::fs::write(&outside_file, b"x").unwrap();

        let scope = ReadScope::Restricted(vec![inside.path().to_path_buf()]);
        assert!(check_allowed(&scope, &inside_file).is_ok());
        assert!(check_allowed(&scope, &outside_file).is_err());
    }

    #[test]
    fn constructors_map_to_expected_scope() {
        // new([]) and default() are deny-all (Restricted empty); only the
        // explicit unrestricted() opts out of containment. new([dir])
        // restricts to the dir.
        assert!(matches!(
            FileLeafResolver::new(Vec::new()).scope,
            ReadScope::Restricted(ref d) if d.is_empty()
        ));
        assert!(matches!(
            FileLeafResolver::default().scope,
            ReadScope::Restricted(ref d) if d.is_empty()
        ));
        assert!(matches!(
            FileLeafResolver::unrestricted().scope,
            ReadScope::Unrestricted
        ));
        assert!(matches!(
            FileLeafResolver::new(vec![PathBuf::from("/data")]).scope,
            ReadScope::Restricted(ref d) if d == &[PathBuf::from("/data")]
        ));
    }
}

/// Resolve-level integration of the resource cache: proves a read-only leaf is
/// served from cache (built once), a writable leaf bypasses it (rebuilt every
/// request so writes are visible), and a disabled cache rebuilds like today.
#[cfg(test)]
mod cache_tests {
    use super::*;
    use crate::adapters::npy_bytes;
    use crate::catalog::data_source::{AssetSpec, DataSourceSpec};
    use crate::catalog::{Catalog, RegisterRequest};
    use crate::core::dtype::{BuiltinDType, DynNDArray, Endianness, Kind};
    use crate::server::resource_cache::AdapterCache;
    use serde_json::json;

    /// A SQLite-backed catalog with the given writable-storage roots. A path
    /// under one of these resolves to a *writable* adapter (cache-bypassed);
    /// an empty list makes every adapter read-only (cacheable).
    async fn catalog_with(dbdir: &Path, writable: Vec<PathBuf>) -> Catalog {
        let uri = format!("sqlite://{}", dbdir.join("cat.db").display());
        let cat = Catalog::connect(&uri).await.unwrap();
        cat.migrate().await.unwrap();
        cat.with_writable_storage(writable)
    }

    /// Write a real `n`-element float64 `.npy` file (zeros) at `path`.
    fn write_npy(path: &Path, n: usize) {
        let dt = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let arr = DynNDArray::new(bytes::Bytes::from(vec![0u8; 8 * n]), dt, vec![n]);
        std::fs::write(path, npy_bytes(&arr)).unwrap();
    }

    /// Register a root array node keyed `key` backed by the npy file at `path`.
    async fn register_npy(cat: &Catalog, key: &str, path: &Path) -> Node {
        let node = cat
            .create_node(
                None,
                vec![],
                RegisterRequest {
                    key: key.into(),
                    structure_family: "array".into(),
                    metadata: json!({}),
                    specs: json!([]),
                    access_blob: json!({}),
                },
            )
            .await
            .unwrap();
        let data_uri = crate::core::file_uri::path_to_file_uri(path).unwrap();
        cat.create_data_source(
            node.id,
            DataSourceSpec {
                structure_family: "array".into(),
                structure: json!({}),
                mimetype: "application/x-npy".into(),
                parameters: json!({}),
                management: "external".into(),
                assets: vec![AssetSpec {
                    data_uri,
                    is_directory: false,
                    parameter: "data_uri".into(),
                    num: None,
                }],
            },
        )
        .await
        .unwrap();
        node
    }

    /// True iff the two adapters share the same underlying `Arc` (a cache hit
    /// hands back a clone of the exact same built adapter).
    fn same_arc(a: &AnyAdapter, b: &AnyAdapter) -> bool {
        match (a, b) {
            (AnyAdapter::Array(x), AnyAdapter::Array(y)) => Arc::ptr_eq(x, y),
            _ => false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolve_caches_read_only_adapter() {
        let data = tempfile::tempdir().unwrap();
        let dbdir = tempfile::tempdir().unwrap();
        let npy = data.path().join("a.npy");
        write_npy(&npy, 3);
        // No writable storage -> the adapter is read-only -> cacheable.
        let cat = catalog_with(dbdir.path(), vec![]).await;
        let node = register_npy(&cat, "a", &npy).await;

        let resolver = FileLeafResolver::unrestricted()
            .with_resource_cache(Arc::new(AdapterCache::new(1024, 60.0)));

        let a1 = resolver.resolve(&cat, &node).await.unwrap();
        let a2 = resolver.resolve(&cat, &node).await.unwrap();
        assert!(
            same_arc(&a1, &a2),
            "read-only adapter must be served from cache (same Arc, built once)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolve_bypasses_cache_for_writable_adapter() {
        let data = tempfile::tempdir().unwrap();
        let dbdir = tempfile::tempdir().unwrap();
        let npy = data.path().join("w.npy");
        write_npy(&npy, 3);
        // The npy's dir is writable storage -> the adapter is writable ->
        // never cached, so a write is always visible to the next read.
        let cat = catalog_with(dbdir.path(), vec![data.path().to_path_buf()]).await;
        let node = register_npy(&cat, "w", &npy).await;

        let resolver = FileLeafResolver::unrestricted()
            .with_resource_cache(Arc::new(AdapterCache::new(1024, 60.0)));

        let a1 = resolver.resolve(&cat, &node).await.unwrap();
        let a2 = resolver.resolve(&cat, &node).await.unwrap();
        assert!(
            !same_arc(&a1, &a2),
            "writable adapter must rebuild every request (not cached)"
        );
        // Sanity: it really is the writable variant.
        match &a1 {
            AnyAdapter::Array(a) => assert!(a.as_writable().is_some(), "expected writable adapter"),
            _ => panic!("expected an array adapter"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolve_with_disabled_cache_rebuilds() {
        let data = tempfile::tempdir().unwrap();
        let dbdir = tempfile::tempdir().unwrap();
        let npy = data.path().join("d.npy");
        write_npy(&npy, 3);
        let cat = catalog_with(dbdir.path(), vec![]).await;
        let node = register_npy(&cat, "d", &npy).await;

        // max_size 0 == the disable env setting == today's behaviour.
        let resolver = FileLeafResolver::unrestricted()
            .with_resource_cache(Arc::new(AdapterCache::new(0, 60.0)));

        let a1 = resolver.resolve(&cat, &node).await.unwrap();
        let a2 = resolver.resolve(&cat, &node).await.unwrap();
        assert!(
            !same_arc(&a1, &a2),
            "disabled cache must rebuild every request (like today)"
        );
    }
}
