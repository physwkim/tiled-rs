//! Directory-backed awkward buffers adapter.
//!
//! Corresponds to `tiled/adapters/awkward.py:AwkwardBuffersAdapter`
//! (`awkward.py:93-160`). Each awkward buffer is persisted to its own file in a
//! directory, filename == form key, content == raw buffer bytes — upstream's
//! `DirectoryContainer` on-disk convention (`tiled/storage.py:423-448`). Storing
//! the tree this way means a Python tiled server pointed at the same directory
//! reads it back verbatim.
//!
//! Deviation from upstream: upstream `read_buffers` enumerates the buffer keys
//! by asking the awkward form which buffers it expects
//! (`awkward_form.expected_from_buffers()`, `awkward.py:82`) and then opens each
//! by name; this port has no awkward runtime and the Rust `/awkward/full`
//! contract operates at the buffer-map level (the GET handler re-zips whatever
//! `read()` returns, `router.rs:3548`), so `read`/`read_buffers` instead list
//! the directory and return every buffer file found. The form still travels in
//! the structure for the client to reconstruct the array.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use bytes::Bytes;

use crate::core::adapters::{AwkwardAdapterRead, AwkwardAdapterWrite, BaseAdapter, BoxFuture};
use crate::core::data_source::Asset;
use crate::core::error::{Result, TiledError};
use crate::core::structures::{AwkwardStructure, Spec, StructureFamily};

/// Directory-backed awkward buffers adapter (managed / catalog storage).
pub struct AwkwardBuffersAdapter {
    directory: PathBuf,
    structure: AwkwardStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    // Writable only when the resolver opted this directory in (it lives under
    // writable storage). The single gate for write-containment, mirroring the
    // other managed adapters (`ZarrAdapter`, `CsvAdapter`, ...).
    writable: bool,
}

impl AwkwardBuffersAdapter {
    /// Open a directory of awkward buffer files. The `structure` (form + length)
    /// is *not* stored on disk — upstream keeps it in the catalog data_source
    /// and hands it to the adapter in `from_catalog` (`awkward.py:107-114`) — so
    /// the resolver passes it from the node's data_source. Read-only until
    /// [`into_writable`](Self::into_writable) opts it in.
    pub fn from_path(
        directory: PathBuf,
        structure: AwkwardStructure,
        metadata: serde_json::Value,
    ) -> Self {
        Self {
            directory,
            structure,
            metadata,
            specs: vec![],
            writable: false,
        }
    }

    /// Mark this adapter writable. The resolver calls this only when the backing
    /// directory is under the catalog's configured writable storage.
    pub fn into_writable(mut self) -> Self {
        self.writable = true;
        self
    }

    /// List the directory and read every buffer file into a `form_key → bytes`
    /// map. Blocking filesystem work — only ever called inside `spawn_blocking`.
    fn read_dir_buffers(directory: &Path) -> Result<HashMap<String, Bytes>> {
        let entries = std::fs::read_dir(directory).map_err(|e| {
            TiledError::Internal(format!("awkward read_dir {}: {e}", directory.display()))
        })?;
        let mut out = HashMap::new();
        for entry in entries {
            let entry =
                entry.map_err(|e| TiledError::Internal(format!("awkward dir entry: {e}")))?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            // A buffer file's name is its form key. A non-UTF-8 filename can
            // never be a form key, so it is not a buffer — skip it.
            let name = match entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            let bytes = std::fs::read(&path).map_err(|e| {
                TiledError::Internal(format!("awkward read {}: {e}", path.display()))
            })?;
            out.insert(name, Bytes::from(bytes));
        }
        Ok(out)
    }
}

/// Reject a buffer key that would escape the buffer directory when joined as a
/// filename. Keys reach `write` from a client-supplied ZIP whose entry names are
/// unpacked verbatim (`unpack_zip_to_buffers`, `router.rs:3428`), so — unlike
/// upstream, whose keys come from awkward internally — the write path must guard
/// against path traversal before it does `directory.join(key)`. Same
/// component-safety rule the `init_storage_*` helpers apply to node path parts.
fn validate_form_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key == "."
        || key == ".."
        || key.contains('/')
        || key.contains('\\')
        || key.contains('\0')
    {
        return Err(TiledError::Validation(format!(
            "unsafe awkward buffer key {key:?}"
        )));
    }
    Ok(())
}

impl BaseAdapter for AwkwardBuffersAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Awkward
    }

    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl AwkwardAdapterRead for AwkwardBuffersAdapter {
    fn structure(&self) -> &AwkwardStructure {
        &self.structure
    }

    fn read(&self) -> BoxFuture<'_, Result<HashMap<String, Bytes>>> {
        let directory = self.directory.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || Self::read_dir_buffers(&directory))
                .await
                .map_err(|e| TiledError::Internal(format!("awkward read task: {e}")))?
        })
    }

    fn read_buffers<'a>(
        &'a self,
        form_keys: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<HashMap<String, Bytes>>> {
        let directory = self.directory.clone();
        let form_keys = form_keys.map(|k| k.to_vec());
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let all = Self::read_dir_buffers(&directory)?;
                Ok(match form_keys {
                    None => all,
                    // Prefix filter, matching the in-memory `AwkwardAdapter` and
                    // upstream `read_buffers` (`awkward.py:140-149`).
                    Some(keys) => all
                        .into_iter()
                        .filter(|(k, _)| keys.iter().any(|fk| k.starts_with(fk.as_str())))
                        .collect(),
                })
            })
            .await
            .map_err(|e| TiledError::Internal(format!("awkward read_buffers task: {e}")))?
        })
    }

    fn as_writable(&self) -> Option<&dyn AwkwardAdapterWrite> {
        if self.writable { Some(self) } else { None }
    }
}

impl AwkwardAdapterWrite for AwkwardBuffersAdapter {
    fn write(
        &self,
        buffers: HashMap<String, Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let directory = self.directory.clone();
        Box::pin(async move {
            // Validate every key before writing any file, so a single unsafe key
            // aborts the whole write rather than leaving a partial one on disk.
            for key in buffers.keys() {
                validate_form_key(key)?;
            }
            tokio::task::spawn_blocking(move || {
                // Write one file per form key, overwriting in place — upstream's
                // `DirectoryContainer.__setitem__` (`storage.py:437-439`) opens
                // each `directory / key` with `"wb"`. The directory is *not*
                // cleared first, so a re-write with the same key set overwrites
                // it buffer-for-buffer (upstream behavior).
                for (key, value) in &buffers {
                    let path = directory.join(key);
                    std::fs::write(&path, value).map_err(|e| {
                        TiledError::Internal(format!("awkward write {}: {e}", path.display()))
                    })?;
                }
                Ok::<(), TiledError>(())
            })
            .await
            .map_err(|e| TiledError::Internal(format!("awkward write task: {e}")))?
        })
    }
}

/// Create on-disk storage for a managed awkward node: make the buffer directory
/// and register a single `is_directory=true` asset. Mirrors upstream
/// `AwkwardBuffersAdapter.init_storage` (`tiled/adapters/awkward.py:120-138`),
/// which mkdirs the directory, refuses a non-empty one, and appends exactly one
/// `Asset(is_directory=True, parameter="data_uri")`. The directory is named by
/// the node path parts (last part = key) with no suffix — upstream's
/// `storage.uri + "/".join(path_parts)` layout — so no `.zarr`-style rename.
pub fn init_storage_awkward(
    writable_root: &Path,
    path_parts: &[String],
) -> Result<(String, Vec<Asset>)> {
    if !writable_root.is_absolute() {
        return Err(TiledError::Internal(format!(
            "writable storage root {} is not absolute",
            writable_root.display()
        )));
    }
    if path_parts.is_empty() {
        return Err(TiledError::Validation(
            "init_storage: node path is empty".into(),
        ));
    }
    for part in path_parts {
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.contains('/')
            || part.contains('\\')
            || part.contains('\0')
        {
            return Err(TiledError::Validation(format!(
                "init_storage: unsafe path component {part:?}"
            )));
        }
    }

    let mut directory = writable_root.to_path_buf();
    for part in path_parts {
        directory.push(part);
    }
    std::fs::create_dir_all(&directory).map_err(|e| {
        TiledError::Internal(format!("init_storage mkdir {}: {e}", directory.display()))
    })?;
    // Refuse a non-empty directory — upstream raises `FileExistsError`
    // (`awkward.py:132-134`) so a create never silently writes over buffers that
    // an earlier node already put there.
    let mut existing = std::fs::read_dir(&directory).map_err(|e| {
        TiledError::Internal(format!(
            "init_storage read_dir {}: {e}",
            directory.display()
        ))
    })?;
    if existing.next().is_some() {
        return Err(TiledError::Validation(format!(
            "init_storage: directory not empty: {}",
            directory.display()
        )));
    }

    let data_uri = crate::core::file_uri::path_to_file_uri(&directory).ok_or_else(|| {
        TiledError::Internal(format!(
            "init_storage: buffer directory is not absolute: {}",
            directory.display()
        ))
    })?;
    let asset = Asset {
        data_uri: data_uri.clone(),
        is_directory: true,
        parameter: Some("data_uri".into()),
        num: None,
        id: None,
    };
    Ok((data_uri, vec![asset]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn structure() -> AwkwardStructure {
        AwkwardStructure {
            length: 3,
            form: serde_json::json!({
                "class": "NumpyArray",
                "primitive": "float64",
                "form_key": "node0"
            }),
        }
    }

    fn tmpdir(name: &str) -> PathBuf {
        // A unique per-test directory under the OS temp dir. `std::process::id`
        // plus the caller-supplied test name keeps concurrent tests from
        // colliding without needing `Date`/random (both unavailable in some
        // sandboxes anyway).
        let mut d = std::env::temp_dir();
        d.push(format!(
            "tiled_awkward_bufadapt_{}_{name}",
            std::process::id()
        ));
        d
    }

    #[test]
    fn init_storage_creates_directory_and_single_directory_asset() {
        let root = tmpdir("init_ok");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let parts = vec!["outer".to_string(), "buf".to_string()];
        let (data_uri, assets) = init_storage_awkward(&root, &parts).unwrap();
        assert_eq!(assets.len(), 1);
        assert!(assets[0].is_directory, "awkward asset must be a directory");
        assert_eq!(assets[0].parameter.as_deref(), Some("data_uri"));
        assert!(root.join("outer").join("buf").is_dir());
        let back = crate::core::file_uri::file_uri_to_path(&data_uri).unwrap();
        assert_eq!(back, root.join("outer").join("buf"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn init_storage_refuses_non_empty_directory() {
        let root = tmpdir("nonempty");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let parts = vec!["occupied".to_string()];
        std::fs::create_dir_all(root.join("occupied")).unwrap();
        std::fs::write(root.join("occupied").join("stale"), b"x").unwrap();
        let err = init_storage_awkward(&root, &parts).unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)), "got {err:?}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn write_then_read_roundtrips_buffers_on_disk() {
        let root = tmpdir("roundtrip");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let (uri, _assets) = init_storage_awkward(&root, &["arr".to_string()]).unwrap();
        let dir = crate::core::file_uri::file_uri_to_path(&uri).unwrap();

        let adapter =
            AwkwardBuffersAdapter::from_path(dir.clone(), structure(), serde_json::json!({}))
                .into_writable();
        let mut buffers = HashMap::new();
        buffers.insert("node0-data".to_string(), Bytes::from(vec![1u8, 2, 3]));
        buffers.insert("node1-offsets".to_string(), Bytes::from(vec![4u8, 5]));
        adapter.as_writable().unwrap().write(buffers).await.unwrap();

        // Files land on disk named by form key (Python-readable layout).
        assert!(dir.join("node0-data").is_file());
        assert!(dir.join("node1-offsets").is_file());

        let back = adapter.read().await.unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(&back["node0-data"][..], &[1u8, 2, 3]);
        assert_eq!(&back["node1-offsets"][..], &[4u8, 5]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn rewrite_overwrites_buffer_in_place() {
        let root = tmpdir("rewrite");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let (uri, _) = init_storage_awkward(&root, &["rw".to_string()]).unwrap();
        let dir = crate::core::file_uri::file_uri_to_path(&uri).unwrap();
        let adapter =
            AwkwardBuffersAdapter::from_path(dir.clone(), structure(), serde_json::json!({}))
                .into_writable();
        let writable = adapter.as_writable().unwrap();

        let mut first = HashMap::new();
        first.insert("node0-data".to_string(), Bytes::from(vec![9u8; 4]));
        writable.write(first).await.unwrap();

        let mut second = HashMap::new();
        second.insert("node0-data".to_string(), Bytes::from(vec![7u8; 2]));
        writable.write(second).await.unwrap();

        let back = adapter.read().await.unwrap();
        assert_eq!(&back["node0-data"][..], &[7u8, 7], "re-write overwrites");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn read_buffers_filters_by_prefix() {
        let root = tmpdir("filter");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let (uri, _) = init_storage_awkward(&root, &["filt".to_string()]).unwrap();
        let dir = crate::core::file_uri::file_uri_to_path(&uri).unwrap();
        let adapter = AwkwardBuffersAdapter::from_path(dir, structure(), serde_json::json!({}))
            .into_writable();
        let mut buffers = HashMap::new();
        buffers.insert("node0-data".to_string(), Bytes::from_static(b"aaa"));
        buffers.insert("node1-data".to_string(), Bytes::from_static(b"bbb"));
        adapter.as_writable().unwrap().write(buffers).await.unwrap();

        let keys = vec!["node0".to_string()];
        let filtered = adapter.read_buffers(Some(&keys)).await.unwrap();
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("node0-data"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn write_rejects_path_traversal_key() {
        let root = tmpdir("traversal");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let (uri, _) = init_storage_awkward(&root, &["sec".to_string()]).unwrap();
        let dir = crate::core::file_uri::file_uri_to_path(&uri).unwrap();
        let adapter = AwkwardBuffersAdapter::from_path(dir, structure(), serde_json::json!({}))
            .into_writable();
        let mut buffers = HashMap::new();
        buffers.insert("../escape".to_string(), Bytes::from_static(b"x"));
        let err = adapter
            .as_writable()
            .unwrap()
            .write(buffers)
            .await
            .unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)), "got {err:?}");
        // Nothing escaped the directory.
        assert!(!root.join("escape").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn read_only_when_not_opted_writable() {
        let adapter = AwkwardBuffersAdapter::from_path(
            tmpdir("readonly"),
            structure(),
            serde_json::json!({}),
        );
        assert!(adapter.as_writable().is_none());
    }
}
