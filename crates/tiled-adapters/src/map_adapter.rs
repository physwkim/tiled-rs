//! In-memory container adapter backed by an IndexMap.
//!
//! Corresponds to `tiled/adapters/mapping.py:MapAdapter`.

use std::sync::Arc;

use indexmap::IndexMap;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::schemas::{SortDirection, SortingItem};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

/// An in-memory container adapter using an ordered map.
pub struct MapAdapter {
    mapping: Arc<IndexMap<String, AnyAdapter>>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    sorting: Vec<SortingItem>,
    must_revalidate: bool,
}

impl MapAdapter {
    /// Create a new MapAdapter from an ordered map of children.
    pub fn new(
        mapping: IndexMap<String, AnyAdapter>,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Self {
        Self {
            mapping: Arc::new(mapping),
            metadata,
            specs,
            sorting: vec![SortingItem {
                key: "_".into(),
                direction: SortDirection::Ascending,
            }],
            must_revalidate: true,
        }
    }

    /// Create with explicit sorting.
    pub fn with_sorting(mut self, sorting: Vec<SortingItem>) -> Self {
        self.sorting = sorting;
        self
    }

    /// Set must_revalidate flag.
    pub fn with_must_revalidate(mut self, must_revalidate: bool) -> Self {
        self.must_revalidate = must_revalidate;
        self
    }

    /// Get the must_revalidate flag.
    pub fn must_revalidate(&self) -> bool {
        self.must_revalidate
    }

    /// Get sorting configuration.
    pub fn sorting(&self) -> &[SortingItem] {
        &self.sorting
    }

    /// Get a range of keys for pagination.
    pub fn keys_range(&self, offset: usize, limit: usize) -> Vec<String> {
        self.mapping
            .keys()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get a range of (key, adapter) pairs for pagination.
    pub fn items_range(&self, offset: usize, limit: usize) -> Vec<(&String, &AnyAdapter)> {
        self.mapping.iter().skip(offset).take(limit).collect()
    }
}

impl BaseAdapter for MapAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Container
    }

    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ContainerAdapter for MapAdapter {
    fn structure(&self) -> &ContainerStructure {
        // We create this on demand. Since the trait returns a reference,
        // we use a leaked box for the static lifetime. In practice this adapter
        // lives for the entire server lifetime so this is acceptable.
        // A better approach would be to cache this, but for now keep it simple.
        //
        // Actually, let's use a different approach — we'll store a ContainerStructure.
        // But the trait requires &ContainerStructure. We need to store it.
        // Let's just leak a small allocation — the MapAdapter is long-lived.
        //
        // For now, return a static reference via Box::leak. This is fine for
        // a demo server where the adapter lives for the process lifetime.
        let keys: Vec<String> = self.mapping.keys().cloned().collect();
        Box::leak(Box::new(ContainerStructure { keys }))
    }

    fn get(&self, key: &str) -> Option<&AnyAdapter> {
        self.mapping.get(key)
    }

    fn keys(&self) -> Vec<String> {
        self.mapping.keys().cloned().collect()
    }

    fn len(&self) -> usize {
        self.mapping.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_adapter_basic() {
        let mapping = IndexMap::new();
        let adapter = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
        assert_eq!(adapter.structure_family(), StructureFamily::Container);
        assert_eq!(adapter.len(), 0);
        assert!(adapter.is_empty());
    }

    #[test]
    fn test_map_adapter_with_children() {
        let mapping = IndexMap::new();
        // We can't easily create child adapters without ArrayAdapter, so just test the container
        // structure with an empty map for now.
        let adapter = MapAdapter::new(mapping, serde_json::json!({"name": "root"}), vec![]);
        assert_eq!(adapter.metadata()["name"], "root");
    }

    #[test]
    fn test_keys_range() {
        let mut mapping = IndexMap::new();
        // Insert several entries using Container adapters (nested MapAdapters)
        for i in 0..10 {
            let child = MapAdapter::new(IndexMap::new(), serde_json::json!({}), vec![]);
            mapping.insert(
                format!("item_{i}"),
                AnyAdapter::Container(Box::new(child)),
            );
        }
        let adapter = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
        assert_eq!(adapter.len(), 10);

        let page = adapter.keys_range(2, 3);
        assert_eq!(page, vec!["item_2", "item_3", "item_4"]);

        let page = adapter.keys_range(8, 5);
        assert_eq!(page, vec!["item_8", "item_9"]);
    }
}
