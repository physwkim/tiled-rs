//! `CompositeClient` — flat-keyed view that merges all child tables/arrays
//! into one logical container.
//!
//! Mirrors `tiled/client/composite.py::CompositeClient`. The composite shape
//! is: a parent container whose children are tables and arrays; the table
//! columns are exposed at the *parent* level so users can read them with a
//! single key.

use std::collections::HashMap;

use bytes::Bytes;
use tiled_core::structures::StructureFamily;

use crate::any_client::AnyClient;
use crate::base::Item;
use crate::container::ContainerClient;
use crate::dataset::{Dataset, Variable};
use crate::error::{ClientError, Result};

#[derive(Debug, Clone)]
pub struct CompositeClient {
    inner: ContainerClient,
}

impl CompositeClient {
    pub fn new(container: ContainerClient) -> Self {
        Self { inner: container }
    }

    pub fn container(&self) -> &ContainerClient {
        &self.inner
    }

    /// Direct child items as in [`ContainerClient::list_entries`], but cached
    /// as a `key → item` map.
    pub async fn get_contents(&self) -> Result<HashMap<String, Item>> {
        let entries = self.inner.list_entries(None).await?;
        Ok(entries.into_iter().map(|i| (i.id.clone(), i)).collect())
    }

    /// Flat key → real path mapping. Table children expose their columns at
    /// the top level (`<table_id>/<col>`).
    ///
    /// Conflicts (e.g. a top-level array named `x` and a table column also
    /// called `x`) are reported as an error rather than silently picking
    /// one — composite is supposed to be flat-keyed and ambiguous lookups
    /// would surprise callers.
    pub async fn flat_keys_mapping(&self) -> Result<HashMap<String, String>> {
        let mut out: HashMap<String, String> = HashMap::new();
        for (key, item) in self.get_contents().await? {
            if item.attributes.structure_family == Some(StructureFamily::Table) {
                if let Some(structure) = &item.attributes.structure {
                    if let Some(cols) = structure.get("columns").and_then(|v| v.as_array()) {
                        for col in cols {
                            if let Some(name) = col.as_str() {
                                if let Some(prev) =
                                    out.insert(name.to_string(), format!("{key}/{name}"))
                                {
                                    return Err(ClientError::Invalid(format!(
                                        "composite key collision on '{name}': existing='{prev}', new='{key}/{name}'"
                                    )));
                                }
                            }
                        }
                        continue;
                    }
                }
            }
            if let Some(prev) = out.insert(key.clone(), key.clone()) {
                return Err(ClientError::Invalid(format!(
                    "composite key collision on '{key}': existing='{prev}'"
                )));
            }
        }
        Ok(out)
    }

    /// Resolve a flat key.
    ///
    /// - For real children of the underlying container: returns
    ///   [`CompositePart::Node`] wrapping an [`AnyClient`].
    /// - For table-column shortcuts (e.g. `composite["temperature"]` where
    ///   `temperature` is a column of a child table): returns
    ///   [`CompositePart::Column`] holding an in-memory [`Variable`].
    ///
    /// **Performance warning for column shortcuts:** the column path fetches
    /// every partition of the underlying table (column-projected, but full
    /// row count) and materialises it into a `Vec<u8>`. For large tables
    /// (millions of rows), prefer `composite.container().get(table_id)` to
    /// get a `TableClient` and stream partitions instead.
    pub async fn get(&self, key: &str) -> Result<CompositePart> {
        let map = self.flat_keys_mapping().await?;
        let real = map.get(key).cloned().ok_or_else(|| {
            ClientError::KeyNotFound(format!(
                "'{key}' not found in composite. For real children, use the .container() accessor."
            ))
        })?;
        if let Some((table_id, col)) = real.split_once('/') {
            let any = self.inner.get(table_id).await?;
            let table = any.into_table()?;
            let parts = table.read(Some(&[col])).await?;
            // Take the dtype from the FIRST non-empty partition's schema —
            // even if it has zero rows, the schema field carries the type.
            // This avoids a silent float64 fallback when all partitions are
            // empty.
            let dtype: Option<tiled_core::dtype::DType> = parts.iter().find_map(|p| {
                if p.schema.fields().is_empty() {
                    None
                } else {
                    arrow_to_tiled_dtype(p.schema.field(0).data_type()).ok()
                }
            });
            let dtype = dtype.ok_or_else(|| {
                ClientError::Invalid(format!(
                    "composite column '{col}': no partitions in table '{table_id}', cannot infer dtype"
                ))
            })?;
            let mut data: Vec<u8> = Vec::new();
            let mut total = 0usize;
            for partition in &parts {
                if partition.batches.is_empty() {
                    continue;
                }
                let field = partition.schema.field(0);
                for batch in &partition.batches {
                    let array = batch.column(0);
                    let bytes = array_to_le_bytes(array, field.data_type())?;
                    total += array.len();
                    data.extend_from_slice(&bytes);
                }
            }
            return Ok(CompositePart::Column(Variable {
                dims: vec![format!("{col}_dim0")],
                shape: vec![total],
                dtype,
                data: Bytes::from(data),
                attrs: serde_json::Value::Null,
            }));
        }
        Ok(CompositePart::Node(self.inner.get(&real).await?))
    }

    /// Read the composite as a [`Dataset`].
    ///
    /// Errors out on name collision (a top-level array and a table column
    /// sharing a name), matching the contract of `flat_keys_mapping`.
    pub async fn read(&self, variables: Option<&[&str]>) -> Result<Dataset> {
        fn insert_unique(ds: &mut Dataset, name: impl Into<String>, var: Variable) -> Result<()> {
            let name = name.into();
            if ds.data_vars.contains_key(&name) || ds.coords.contains_key(&name) {
                return Err(ClientError::Invalid(format!(
                    "composite name collision on '{name}'"
                )));
            }
            ds.insert_data_var(name, var);
            Ok(())
        }

        let mut ds = Dataset::new();
        ds.attrs = self
            .inner
            .base()
            .metadata()
            .get("attrs")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        for (id, item) in self.get_contents().await? {
            let want = variables
                .map(|vs| vs.iter().any(|v| *v == id))
                .unwrap_or(true);
            let any =
                AnyClient::from_item(self.inner.base().context().clone(), item.clone(), false)?;
            match item.attributes.structure_family {
                Some(StructureFamily::Array) => {
                    if !want {
                        continue;
                    }
                    let arr = match &any {
                        AnyClient::Array(a) => a,
                        _ => {
                            tracing::warn!(
                                target: "tiled.client.composite",
                                "skipping '{id}': structure_family=array but client variant was not Array"
                            );
                            continue;
                        }
                    };
                    let blocks = arr.read().await?;
                    let mut data = Vec::new();
                    for b in &blocks {
                        data.extend_from_slice(&b.data);
                    }
                    let dims = arr.structure().dims.clone().unwrap_or_else(|| {
                        (0..arr.ndim()).map(|i| format!("{id}_dim{i}")).collect()
                    });
                    insert_unique(
                        &mut ds,
                        id,
                        Variable {
                            dims,
                            shape: arr.shape().to_vec(),
                            dtype: arr.structure().data_type.clone(),
                            data: Bytes::from(data),
                            attrs: serde_json::Value::Null,
                        },
                    )?;
                }
                Some(StructureFamily::Sparse) => {
                    // Sparse arrays need COO assembly (indices + data) which
                    // the flat `Variable` model can't represent. Skip with a
                    // warning so callers can use `composite.container()` to
                    // get the underlying SparseClient directly.
                    tracing::warn!(
                        target: "tiled.client.composite",
                        "skipping sparse child '{id}' — composite::read produces only dense Variables"
                    );
                    continue;
                }
                Some(StructureFamily::Table) => {
                    let tbl = any
                        .as_table()
                        .ok_or_else(|| ClientError::Invalid("expected table".into()))?;
                    let columns: Vec<String> = if let Some(vs) = variables {
                        tbl.columns()
                            .iter()
                            .filter(|c| vs.iter().any(|v| *v == c.as_str()))
                            .cloned()
                            .collect()
                    } else {
                        tbl.columns().to_vec()
                    };
                    if columns.is_empty() {
                        continue;
                    }
                    let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    let parts = tbl.read(Some(&col_refs)).await?;
                    let n = parts.first().map(|p| p.schema.fields().len()).unwrap_or(0);
                    for col_i in 0..n {
                        let name = parts[0].schema.field(col_i).name().clone();
                        let dt = arrow_to_tiled_dtype(parts[0].schema.field(col_i).data_type())?;
                        let mut data = Vec::new();
                        let mut total = 0usize;
                        for p in &parts {
                            for batch in &p.batches {
                                let array = batch.column(col_i);
                                let bytes =
                                    array_to_le_bytes(array, p.schema.field(col_i).data_type())?;
                                data.extend_from_slice(&bytes);
                                total += array.len();
                            }
                        }
                        insert_unique(
                            &mut ds,
                            name.clone(),
                            Variable {
                                dims: vec![format!("{name}_dim0")],
                                shape: vec![total],
                                dtype: dt,
                                data: Bytes::from(data),
                                attrs: serde_json::Value::Null,
                            },
                        )?;
                    }
                }
                Some(StructureFamily::Awkward) => {
                    // Awkward arrays are jagged + need a `form` for typed
                    // reconstruction. Skip with a warning — the flat
                    // `Variable` model would type-lie. Callers should use
                    // `composite.container()` to reach the AwkwardClient.
                    tracing::warn!(
                        target: "tiled.client.composite",
                        "skipping awkward child '{id}' — composite::read produces only flat Variables"
                    );
                    continue;
                }
                _ => {}
            }
        }
        Ok(ds)
    }
}

// ---------------------------------------------------------------------------
// Helpers — duplicated here to avoid a cyclic import with xarray_client; small
// enough to keep both copies maintainable.
// ---------------------------------------------------------------------------

use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};

fn arrow_to_tiled_dtype(dt: &ArrowDataType) -> Result<DType> {
    let b = match dt {
        ArrowDataType::Float64 => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        ArrowDataType::Float32 => BuiltinDType::new(Endianness::Little, Kind::Float, 4),
        ArrowDataType::Int64 => BuiltinDType::new(Endianness::Little, Kind::Integer, 8),
        ArrowDataType::Int32 => BuiltinDType::new(Endianness::Little, Kind::Integer, 4),
        ArrowDataType::UInt64 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 8),
        ArrowDataType::UInt32 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 4),
        other => {
            return Err(ClientError::Invalid(format!(
                "unsupported arrow dtype: {other:?}"
            )));
        }
    };
    Ok(DType::Builtin(b))
}

fn array_to_le_bytes(array: &dyn Array, dt: &ArrowDataType) -> Result<Vec<u8>> {
    let out = match dt {
        ArrowDataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| ClientError::Invalid("downcast Float64".into()))?
            .values()
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect(),
        ArrowDataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| ClientError::Invalid("downcast Float32".into()))?
            .values()
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect(),
        ArrowDataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ClientError::Invalid("downcast Int64".into()))?
            .values()
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect(),
        ArrowDataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ClientError::Invalid("downcast Int32".into()))?
            .values()
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect(),
        ArrowDataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| ClientError::Invalid("downcast UInt64".into()))?
            .values()
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect(),
        ArrowDataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ClientError::Invalid("downcast UInt32".into()))?
            .values()
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect(),
        other => {
            return Err(ClientError::Invalid(format!(
                "unsupported arrow dtype: {other:?}"
            )));
        }
    };
    Ok(out)
}

/// Result of [`CompositeClient::get`]: either a real child client or a
/// synthesized in-memory column projection.
///
/// `Node` is what you get when the key matches a direct child of the
/// underlying container (`composite["temperature_sensor"]`). `Column` is what
/// you get when the key matches a column inside one of the container's child
/// tables (`composite["temperature"]` where `temperature` is a column of
/// `temperature_sensor` table).
#[derive(Debug, Clone)]
pub enum CompositePart {
    /// A real child of the underlying container.
    Node(AnyClient),
    /// A column projected out of a child table, materialised in memory.
    Column(Variable),
}

impl CompositePart {
    /// Move out the inner [`AnyClient`]. Returns `Err` when this is a column.
    pub fn into_node(self) -> Result<AnyClient> {
        match self {
            Self::Node(n) => Ok(n),
            Self::Column(_) => Err(ClientError::Invalid(
                "expected a node child, got a table-column projection".into(),
            )),
        }
    }

    /// Move out the inner [`Variable`]. Returns `Err` when this is a node.
    pub fn into_column(self) -> Result<Variable> {
        match self {
            Self::Column(v) => Ok(v),
            Self::Node(_) => Err(ClientError::Invalid(
                "expected a table-column projection, got a node child".into(),
            )),
        }
    }

    /// Borrow the inner [`AnyClient`] without consuming `self`.
    pub fn as_node(&self) -> Option<&AnyClient> {
        match self {
            Self::Node(n) => Some(n),
            _ => None,
        }
    }

    /// Borrow the inner [`Variable`] without consuming `self`.
    pub fn as_column(&self) -> Option<&Variable> {
        match self {
            Self::Column(v) => Some(v),
            _ => None,
        }
    }
}
