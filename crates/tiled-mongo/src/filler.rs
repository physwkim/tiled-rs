//! Filler — resolves datum_id references to actual file data.
//!
//! Flow: datum_id → datum document → resource document → file handler → bytes

use std::collections::HashMap;
use std::sync::Arc;

use mongodb::bson::{Document, doc};
use mongodb::sync::Database;

use tiled_core::error::{Result, TiledError};

use crate::handler::{FileHandler, HandlerRegistry};

/// Resolves datum_id strings to actual data by chaining through
/// MongoDB datum/resource collections and file handlers.
pub struct Filler {
    db: Database,
    registry: Arc<HandlerRegistry>,
    /// Cache: resource_uid → handler instance.
    handler_cache: std::sync::Mutex<HashMap<String, Arc<dyn FileHandler>>>,
}

impl Filler {
    pub fn new(db: Database, registry: Arc<HandlerRegistry>) -> Self {
        Self {
            db,
            registry,
            handler_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Resolve a datum_id to raw data bytes + shape.
    pub fn fill(&self, datum_id: &str) -> Result<(Vec<u8>, Vec<usize>)> {
        // 1. Look up datum document.
        let datum_doc = self
            .db
            .collection::<Document>("datum")
            .find_one(doc! { "datum_id": datum_id })
            .run()
            .map_err(|e| TiledError::Internal(format!("Datum lookup failed: {e}")))?
            .ok_or_else(|| TiledError::NotFound(format!("Datum not found: {datum_id}")))?;

        let resource_uid = datum_doc
            .get_str("resource")
            .map_err(|_| TiledError::Validation("Datum missing 'resource' field".into()))?;

        let datum_kwargs: serde_json::Value = datum_doc
            .get_document("datum_kwargs")
            .ok()
            .and_then(|d| mongodb::bson::from_document(d.clone()).ok())
            .unwrap_or(serde_json::json!({}));

        // 2. Get or create handler for this resource.
        let handler = self.get_handler(resource_uid)?;

        // 3. Read data via handler.
        handler.read(&datum_kwargs)
    }

    /// Fill a column of datum_ids into raw byte arrays.
    ///
    /// `expected_shape` is the per-datum shape (i.e. the column's
    /// `inner_shape` excluding the leading row axis). Each handler's
    /// returned shape must equal this; otherwise the result would be a
    /// jagged/misaligned column. Errors out early instead of silently
    /// concatenating bytes of differing per-row sizes — including the
    /// scalar case where `expected_shape` is empty but the handler
    /// returned a non-empty shape.
    pub fn fill_column(&self, datum_ids: &[String], expected_shape: &[usize]) -> Result<Vec<u8>> {
        let mut all_bytes = Vec::new();
        for datum_id in datum_ids {
            let (data, shape) = self.fill(datum_id)?;
            if shape.as_slice() != expected_shape {
                return Err(TiledError::Validation(format!(
                    "datum {datum_id}: shape {shape:?} does not match expected {expected_shape:?}"
                )));
            }
            all_bytes.extend_from_slice(&data);
        }
        Ok(all_bytes)
    }

    fn get_handler(&self, resource_uid: &str) -> Result<Arc<dyn FileHandler>> {
        // Check cache first.
        {
            let cache = self.handler_cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(handler) = cache.get(resource_uid) {
                return Ok(handler.clone());
            }
        }

        // Look up resource document.
        let resource_doc = self
            .db
            .collection::<Document>("resource")
            .find_one(doc! { "uid": resource_uid })
            .run()
            .map_err(|e| TiledError::Internal(format!("Resource lookup failed: {e}")))?
            .ok_or_else(|| TiledError::NotFound(format!("Resource not found: {resource_uid}")))?;

        let spec = resource_doc
            .get_str("spec")
            .map_err(|_| TiledError::Validation("Resource missing 'spec' field".into()))?;

        let root = resource_doc.get_str("root").unwrap_or("");
        let resource_path = resource_doc
            .get_str("resource_path")
            .or_else(|_| resource_doc.get_str("path"))
            .unwrap_or("");

        let resource_kwargs: serde_json::Value = resource_doc
            .get_document("resource_kwargs")
            .ok()
            .and_then(|d| mongodb::bson::from_document(d.clone()).ok())
            .unwrap_or(serde_json::json!({}));

        let handler = self
            .registry
            .create_handler(spec, root, resource_path, &resource_kwargs)?;

        let handler: Arc<dyn FileHandler> = Arc::from(handler);

        // Cache it.
        {
            let mut cache = self.handler_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.insert(resource_uid.to_string(), handler.clone());
        }

        Ok(handler)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A panic while holding the handler_cache lock poisons the mutex.
    /// `.unwrap_or_else(|e| e.into_inner())` must recover and return the inner
    /// HashMap rather than propagating a panic on every subsequent fill call.
    #[test]
    fn poisoned_mutex_is_recovered_not_panicked() {
        let m: Mutex<HashMap<String, i32>> = Mutex::new(HashMap::new());
        let _ = std::panic::catch_unwind(|| {
            let _guard = m.lock().unwrap();
            panic!("poison!");
        });
        assert!(m.is_poisoned());
        // Must not panic.
        let inner = m.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            inner.is_empty(),
            "recovered map must be the original empty map"
        );
    }
}
