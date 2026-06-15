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

use tiled_catalog::Catalog;
use tiled_catalog::adapter::LeafResolver;
use tiled_catalog::error::CatalogError;
use tiled_catalog::orm::Node;
use tiled_core::adapters::{AnyAdapter, BoxFuture};

/// Resolver that decodes file:// URIs and dispatches to the right
/// `tiled-adapters` implementation. When `allowed_data_dirs` is non-empty
/// only paths whose canonicalised location lives under one of those
/// directories are accepted; everything else fails with a
/// `Validation` error so a registered `file:///etc/passwd` can't be
/// served back as a CSV. An empty list means "no restriction" (for tests /
/// trusted single-user deployments); constructing one that way through
/// [`new`](Self::new) or [`Default`] logs a `warn!` so the unrestricted
/// state is never silent — see [`unrestricted`](Self::unrestricted) for the
/// explicit, un-warned opt-in.
pub struct FileLeafResolver {
    allowed_data_dirs: Vec<PathBuf>,
}

impl FileLeafResolver {
    /// Construct a resolver restricted to `allowed_data_dirs`.
    ///
    /// An **empty** list means "no restriction" — every `file:` path a
    /// registered `data_uri` points at is served. That is convenient for a
    /// trusted single-user deployment but unsafe once untrusted writers can
    /// register nodes (N2), so an empty list is logged as a `warn!` at
    /// construction: the dangerous state is no longer silent. Callers that
    /// *intend* no restriction should use [`FileLeafResolver::unrestricted`],
    /// which is the explicit, un-warned opt-in.
    pub fn new(allowed_data_dirs: Vec<PathBuf>) -> Self {
        if allowed_data_dirs.is_empty() {
            tracing::warn!(
                "FileLeafResolver has no allowed-data-dir restriction: the server will \
                 serve any local file referenced by a registered data_uri. Pass \
                 --allowed-data-dir (repeatable) to confine reads to specific directories."
            );
        }
        Self { allowed_data_dirs }
    }

    /// Explicit, un-warned constructor for a deliberately unrestricted
    /// resolver (tests, trusted single-user deployments). Unlike
    /// [`FileLeafResolver::new`] with an empty list, this does not log a
    /// warning, because the lack of restriction is the caller's stated
    /// intent rather than a forgotten `--allowed-data-dir`.
    pub fn unrestricted() -> Self {
        Self {
            allowed_data_dirs: Vec::new(),
        }
    }
}

/// Allow-list check, run on the blocking pool (it calls `canonicalize`, a
/// filesystem stat). An empty allow-list means "no restriction".
fn check_allowed(
    allowed_data_dirs: &[PathBuf],
    path: &Path,
) -> std::result::Result<(), CatalogError> {
    if allowed_data_dirs.is_empty() {
        return Ok(());
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

impl Default for FileLeafResolver {
    /// The implicit default is unrestricted, but — unlike
    /// [`FileLeafResolver::unrestricted`] — routes through
    /// [`new`](FileLeafResolver::new) so a defaulted (i.e. unconsidered)
    /// resolver still warns that reads are not path-restricted.
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
            let path = uri_to_path(&asset.data_uri)?;
            let allowed = self.allowed_data_dirs.clone();
            let mimetype = ds.mimetype.clone();
            let parameters = ds.parameters.clone();
            let metadata = node.metadata.clone();
            // The allow-list check (`canonicalize`) and adapter construction
            // (`from_path` reads the data file's header) are blocking
            // filesystem work — offload to the blocking pool so the async
            // `get` that awaits this never parks the executor.
            tokio::task::spawn_blocking(move || {
                check_allowed(&allowed, &path)?;
                build_leaf_adapter(&mimetype, path, &parameters, metadata)
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
    // Only the (feature-gated) hdf5 arm reads `parameters`; without that
    // feature the binding is genuinely unused.
    #[cfg_attr(not(feature = "hdf5-adapter"), allow(unused_variables))]
    parameters: &serde_json::Value,
    metadata: serde_json::Value,
) -> std::result::Result<AnyAdapter, CatalogError> {
    let any_adapter = match mimetype {
        "application/x-npy" | "application/x-numpy" | "npy" => {
            let adapter = tiled_adapters::NpyAdapter::from_path(path, metadata)
                .map_err(|e| CatalogError::Validation(e.to_string()))?;
            AnyAdapter::Array(Arc::new(adapter))
        }
        "image/tiff" | "image/x-tiff" | "tiff" => {
            #[cfg(feature = "tiff-adapter")]
            {
                let adapter = tiled_adapters::TiffAdapter::from_path(path, metadata)
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
                let adapter = tiled_adapters::ImageAdapter::from_path(path, metadata)
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
                let adapter = tiled_adapters::Hdf5Adapter::from_path(path, dataset, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Array(Arc::new(adapter))
            }
            #[cfg(not(feature = "hdf5-adapter"))]
            {
                return Err(CatalogError::Validation("hdf5 support not built in".into()));
            }
        }
        "text/csv" => {
            #[cfg(feature = "csv-adapter")]
            {
                let adapter = tiled_adapters::CsvAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
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
                let adapter = tiled_adapters::ParquetAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Table(Arc::new(adapter))
            }
            #[cfg(not(feature = "parquet-adapter"))]
            {
                return Err(CatalogError::Validation(
                    "parquet support not built in".into(),
                ));
            }
        }
        other => {
            return Err(CatalogError::Validation(format!(
                "no built-in adapter for mimetype: {other}"
            )));
        }
    };
    Ok(any_adapter)
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
/// Both authority forms decode to the absolute path that begins at the first
/// `/` after the scheme: `file:///a/b/c` (empty authority) and
/// `file://localhost/a/b/c` (host authority) both yield `/a/b/c`, matching
/// `urlparse`.
fn uri_to_path(uri: &str) -> std::result::Result<PathBuf, CatalogError> {
    let rest = uri.strip_prefix("file://").ok_or_else(|| {
        CatalogError::Validation(format!("data_uri {uri} must use the file:// scheme"))
    })?;
    // `rest` is `<authority><path>`; the path is everything from the first
    // `/`. No `/` means a malformed `file://` with no absolute path.
    match rest.find('/') {
        Some(i) => Ok(PathBuf::from(&rest[i..])),
        None => Err(CatalogError::Validation(format!(
            "data_uri {uri} has no absolute file:// path"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_to_path_accepts_empty_authority() {
        assert_eq!(
            uri_to_path("file:///data/scan001.h5").unwrap(),
            PathBuf::from("/data/scan001.h5")
        );
    }

    #[test]
    fn uri_to_path_strips_host_authority() {
        // file://localhost/a/b -> /a/b, matching Python's urlparse.
        assert_eq!(
            uri_to_path("file://localhost/a/b").unwrap(),
            PathBuf::from("/a/b")
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
    fn empty_allow_list_permits_any_path() {
        // N2: an empty allow-list is "no restriction" (now warned at
        // construction). The gate short-circuits without touching the
        // filesystem, so a non-existent path is still permitted.
        assert!(check_allowed(&[], Path::new("/etc/passwd")).is_ok());
    }

    #[test]
    fn non_empty_allow_list_contains_reads() {
        let inside = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let inside_file = inside.path().join("data.csv");
        let outside_file = outside.path().join("secret.csv");
        std::fs::write(&inside_file, b"x").unwrap();
        std::fs::write(&outside_file, b"x").unwrap();

        let allowed = vec![inside.path().to_path_buf()];
        assert!(check_allowed(&allowed, &inside_file).is_ok());
        assert!(check_allowed(&allowed, &outside_file).is_err());
    }

    #[test]
    fn permissive_constructors_leave_allow_list_empty() {
        // new([]), unrestricted(), and default() differ only in whether they
        // warn; all three leave reads unrestricted (the non-breaking N2
        // contract). new([dir]) stores the restriction.
        assert!(
            FileLeafResolver::new(Vec::new())
                .allowed_data_dirs
                .is_empty()
        );
        assert!(
            FileLeafResolver::unrestricted()
                .allowed_data_dirs
                .is_empty()
        );
        assert!(FileLeafResolver::default().allowed_data_dirs.is_empty());
        assert_eq!(
            FileLeafResolver::new(vec![PathBuf::from("/data")]).allowed_data_dirs,
            vec![PathBuf::from("/data")]
        );
    }
}
