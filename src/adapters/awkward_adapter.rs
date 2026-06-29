//! In-memory awkward array adapter.
//!
//! Corresponds to `tiled/adapters/awkward.py:AwkwardBuffersAdapter`.
//!
//! An `AwkwardAdapter` holds a buffer map (`form_key → bytes`) plus an
//! `AwkwardStructure` (form JSON + length).  `read_buffers` optionally
//! filters the map to keys whose name starts with one of the requested
//! form keys, matching Python `AwkwardBuffersAdapter.read_buffers`
//! (adapters/awkward.py:140-149).  `write` replaces the whole buffer map
//! atomically, matching Python `AwkwardBuffersAdapter.write` (:157-160).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;

use bytes::Bytes;

use crate::core::adapters::{AwkwardAdapterRead, AwkwardAdapterWrite, BaseAdapter, BoxFuture};
use crate::core::error::Result;
use crate::core::structures::{AwkwardStructure, Spec, StructureFamily};

/// In-memory awkward array adapter.
pub struct AwkwardAdapter {
    buffers: RwLock<HashMap<String, Bytes>>,
    structure: AwkwardStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl AwkwardAdapter {
    pub fn new(buffers: HashMap<String, Bytes>, structure: AwkwardStructure) -> Self {
        Self {
            buffers: RwLock::new(buffers),
            structure,
            metadata: serde_json::Value::Object(Default::default()),
            specs: vec![],
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_specs(mut self, specs: Vec<Spec>) -> Self {
        self.specs = specs;
        self
    }
}

impl BaseAdapter for AwkwardAdapter {
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

impl AwkwardAdapterRead for AwkwardAdapter {
    fn structure(&self) -> &AwkwardStructure {
        &self.structure
    }

    fn read(&self) -> BoxFuture<'_, Result<HashMap<String, Bytes>>> {
        Box::pin(async move {
            let guard = self.buffers.read().unwrap();
            Ok(guard.clone())
        })
    }

    fn read_buffers<'a>(
        &'a self,
        form_keys: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<HashMap<String, Bytes>>> {
        Box::pin(async move {
            let guard = self.buffers.read().unwrap();
            match form_keys {
                None => Ok(guard.clone()),
                Some(keys) => Ok(guard
                    .iter()
                    .filter(|(k, _)| keys.iter().any(|fk| k.starts_with(fk.as_str())))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()),
            }
        })
    }

    fn as_writable(&self) -> Option<&dyn AwkwardAdapterWrite> {
        Some(self)
    }
}

impl AwkwardAdapterWrite for AwkwardAdapter {
    fn write(
        &self,
        buffers: HashMap<String, Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut guard = self.buffers.write().unwrap();
            *guard = buffers;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::adapters::{AwkwardAdapterRead, BaseAdapter};
    use crate::core::structures::{AwkwardStructure, StructureFamily};

    fn simple_adapter() -> AwkwardAdapter {
        let structure = AwkwardStructure {
            length: 3,
            form: serde_json::json!({
                "class": "NumpyArray",
                "primitive": "float64",
                "form_key": "node0"
            }),
        };
        let mut buffers = HashMap::new();
        buffers.insert("node0-data".to_string(), Bytes::from(vec![0u8; 24]));
        AwkwardAdapter::new(buffers, structure)
    }

    #[test]
    fn structure_family_is_awkward() {
        assert_eq!(
            simple_adapter().structure_family(),
            StructureFamily::Awkward
        );
    }

    #[test]
    fn structure_length_roundtrips() {
        assert_eq!(simple_adapter().structure().length, 3);
    }

    #[tokio::test]
    async fn read_returns_all_buffers() {
        let adapter = simple_adapter();
        let buffers = adapter.read().await.unwrap();
        assert_eq!(buffers.len(), 1);
        assert!(buffers.contains_key("node0-data"));
    }

    #[tokio::test]
    async fn read_buffers_filtered_by_prefix() {
        let structure = AwkwardStructure {
            length: 2,
            form: serde_json::json!({"class": "NumpyArray", "form_key": "node0"}),
        };
        let mut bufs = HashMap::new();
        bufs.insert("node0-data".to_string(), Bytes::from(b"aaa".as_ref()));
        bufs.insert("node1-data".to_string(), Bytes::from(b"bbb".as_ref()));
        let adapter = AwkwardAdapter::new(bufs, structure);

        let keys = vec!["node0".to_string()];
        let filtered = adapter.read_buffers(Some(&keys)).await.unwrap();
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("node0-data"));
        assert!(!filtered.contains_key("node1-data"));
    }

    #[tokio::test]
    async fn write_replaces_buffers() {
        let adapter = simple_adapter();

        let writable = adapter
            .as_writable()
            .expect("in-memory adapter is writable");
        let mut new_buffers = HashMap::new();
        new_buffers.insert("node0-data".to_string(), Bytes::from(b"updated".as_ref()));
        writable.write(new_buffers).await.unwrap();

        let back = adapter.read().await.unwrap();
        assert_eq!(&back["node0-data"][..], b"updated");
    }

    #[tokio::test]
    async fn read_buffers_none_returns_all() {
        let adapter = simple_adapter();
        let all = adapter.read_buffers(None).await.unwrap();
        assert_eq!(all.len(), 1);
    }
}
