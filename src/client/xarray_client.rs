//! `DatasetClient` — read an xarray-style container as a `Dataset`.
//!
//! Mirrors `tiled/client/xarray.py::DaskDatasetClient` / `DatasetClient` /
//! `_WideTableFetcher`. Children of the container must carry `xarray_coord`
//! or `xarray_data_var` specs; their `metadata.attrs` and `dims` are
//! preserved on the resulting `Variable`.
//!
//! Wide-table optimisation (`/full?format=arrow`): if all children share the
//! same first-dim length and look scalar-ish (1-D and short), we issue a
//! single Arrow IPC call instead of one HTTP request per column. Mirrors the
//! Python `_WideTableFetcher`.

use std::collections::HashMap;

use crate::core::structures::Spec;
use bytes::Bytes;
use url::Url;

use crate::client::any_client::AnyClient;
use crate::client::base::ParsedStructure;
use crate::client::container::ContainerClient;
use crate::client::dataset::{Dataset, Variable};
use crate::client::error::{ClientError, Result};
use crate::client::utils::ARROW_FILE_MIME_TYPE;

const URL_CHARACTER_LIMIT: usize = 2_000;
const LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION: usize = 1_000_000;

#[derive(Debug, Clone)]
pub struct DatasetClient {
    inner: ContainerClient,
}

impl DatasetClient {
    pub fn new(container: ContainerClient) -> Self {
        Self { inner: container }
    }

    pub fn container(&self) -> &ContainerClient {
        &self.inner
    }

    fn dataset_attrs(&self) -> serde_json::Value {
        self.inner
            .base()
            .metadata()
            .get("attrs")
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    }

    /// Read the full dataset.
    ///
    /// `variables`: optional inclusion list (matches Python `read(variables=...)`).
    /// `optimize_wide_table`: when true and all variables share the same first
    /// dimension, fetch them as a single Arrow IPC payload via `links.full`.
    pub async fn read(
        &self,
        variables: Option<&[&str]>,
        optimize_wide_table: bool,
    ) -> Result<Dataset> {
        let mut ds = Dataset::new();
        ds.attrs = self.dataset_attrs();

        let entries = self.inner.list_entries(None).await?;
        let mut wide_candidates: Vec<(String, AnyClient, Vec<Spec>)> = Vec::new();
        let mut narrow_candidates: Vec<(String, AnyClient, Vec<Spec>)> = Vec::new();
        let mut first_dims: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();

        for item in entries {
            let id = item.id.clone();
            if let Some(vars) = variables
                && !vars.iter().any(|v| *v == id)
            {
                continue;
            }
            let specs = item.attributes.specs.clone().unwrap_or_default();
            let any = AnyClient::from_item(self.inner.base().context().clone(), item, false)?;
            let array = match &any {
                AnyClient::Array(a) => Some(a),
                _ => None,
            };
            if let Some(arr) = array {
                let shape = arr.shape();
                if !shape.is_empty() {
                    first_dims.insert(shape[0]);
                }
                if optimize_wide_table
                    && (shape.is_empty()
                        || (shape.len() == 1
                            && shape[0] < LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION))
                {
                    wide_candidates.push((id, any, specs));
                    continue;
                }
            }
            narrow_candidates.push((id, any, specs));
        }

        // Wide-table optimisation only applies when every child shares the
        // same first dim.
        let wide_ok = first_dims.len() <= 1;
        if wide_ok && !wide_candidates.is_empty() {
            let names: Vec<String> = wide_candidates.iter().map(|(n, _, _)| n.clone()).collect();
            let columns = self
                .fetch_wide_arrow(&names)
                .await
                .map_err(|e| {
                    tracing::warn!(target: "tiled.client.xarray", "wide fetch failed: {e}; falling back to per-array reads");
                    e
                });
            match columns {
                Ok(decoded) => {
                    // Place every variable that came back; route the rest to
                    // the narrow path so they don't get silently dropped.
                    for (name, any, specs) in wide_candidates {
                        match decoded.get(&name) {
                            Some(var) => {
                                let role = classify_role(&specs)?;
                                place_variable(&mut ds, role, name, var.clone());
                            }
                            None => {
                                tracing::warn!(
                                    target: "tiled.client.xarray",
                                    "wide-arrow response missing '{name}'; falling back to per-array read"
                                );
                                narrow_candidates.push((name, any, specs));
                            }
                        }
                    }
                }
                Err(_) => {
                    narrow_candidates.extend(wide_candidates);
                }
            }
        } else {
            narrow_candidates.extend(wide_candidates);
        }

        for (name, any, specs) in narrow_candidates {
            let Some(arr) = any.as_array() else {
                tracing::warn!(
                    target: "tiled.client.xarray",
                    "child '{name}' has structure_family={:?} but xarray dataset only handles arrays — skipping",
                    any.structure_family()
                );
                continue;
            };
            let blocks = arr.read().await?;
            // Concatenate row-major. For 1-D this is just append.
            let mut data = Vec::new();
            for b in &blocks {
                data.extend_from_slice(&b.data);
            }
            let var =
                Variable {
                    dims: arr.structure().dims.clone().unwrap_or_else(|| {
                        (0..arr.ndim()).map(|i| format!("{name}_dim{i}")).collect()
                    }),
                    shape: arr.shape().to_vec(),
                    dtype: arr.structure().data_type.clone(),
                    data: Bytes::from(data),
                    attrs: arr
                        .base()
                        .metadata()
                        .get("attrs")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                };
            let role = classify_role(&specs)?;
            place_variable(&mut ds, role, name, var);
        }

        Ok(ds)
    }

    async fn fetch_wide_arrow(&self, variables: &[String]) -> Result<HashMap<String, Variable>> {
        let link = self.inner.base().require_link("full")?;

        // Estimate the GET URL length the way upstream does (`xarray.py:176-182`):
        // the base link plus one `&field=<name>` per variable. Past
        // `URL_CHARACTER_LIMIT` the field list moves into a POST JSON body
        // (upstream `_fetch_variables__post`, `xarray.py:206`); otherwise it stays
        // as repeated `field=` query params (`_fetch_variables__get`,
        // `xarray.py:193`). Mirrors the table wide-column fallback in
        // `dataframe.rs::read_partition`. Both forms hit `/container/full/{path}`
        // (GET vs POST) and come back as Arrow IPC.
        const EXTRA_CHARS_PER_ITEM: usize = "&field=".len();
        let projected_len = link.len()
            + variables
                .iter()
                .map(|v| EXTRA_CHARS_PER_ITEM + v.len())
                .sum::<usize>();

        let bytes = if projected_len > URL_CHARACTER_LIMIT {
            // POST: `format` stays a query param; the field list becomes the JSON
            // body (parity with the server's `post_container_full`).
            let mut url = Url::parse(link)?;
            url.query_pairs_mut()
                .append_pair("format", ARROW_FILE_MIME_TYPE);
            let body = serde_json::Value::Array(
                variables
                    .iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            );
            self.inner
                .base()
                .context()
                .post_bytes(&url, ARROW_FILE_MIME_TYPE, &body)
                .await?
        } else {
            let mut url = Url::parse(link)?;
            url.query_pairs_mut()
                .append_pair("format", ARROW_FILE_MIME_TYPE);
            for v in variables {
                url.query_pairs_mut().append_pair("field", v);
            }
            self.inner
                .base()
                .context()
                .get_bytes(&url, ARROW_FILE_MIME_TYPE)
                .await?
        };

        // Decode arrow file into per-column Variables.
        use arrow::ipc::reader::FileReader;
        let cursor = std::io::Cursor::new(bytes.to_vec());
        let reader = FileReader::try_new(cursor, None)?;
        let schema = reader.schema();
        let mut all: HashMap<String, Vec<u8>> = HashMap::new();
        let mut len_per_col: HashMap<String, usize> = HashMap::new();
        let mut dtype_per_col: HashMap<String, DType> = HashMap::new();
        for batch in reader {
            let batch = batch?;
            for (i, field) in schema.fields().iter().enumerate() {
                let name = field.name().clone();
                let array = batch.column(i);
                let dtype = arrow_dtype_to_tiled_dtype(field.data_type())?;
                let buf = array_to_le_bytes(array, field.data_type())?;
                all.entry(name.clone()).or_default().extend_from_slice(&buf);
                *len_per_col.entry(name.clone()).or_default() += array.len();
                dtype_per_col.entry(name).or_insert(dtype);
            }
        }

        let mut out = HashMap::new();
        for (name, data) in all {
            let len = *len_per_col.get(&name).unwrap_or(&0);
            let dtype = dtype_per_col
                .remove(&name)
                .ok_or_else(|| ClientError::Invalid(format!("dtype missing for {name}")))?;
            out.insert(
                name.clone(),
                Variable {
                    dims: vec![format!("{name}_dim0")],
                    shape: vec![len],
                    dtype,
                    data: Bytes::from(data),
                    attrs: serde_json::Value::Null,
                },
            );
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Coord,
    DataVar,
}

fn classify_role(specs: &[Spec]) -> Result<Role> {
    for s in specs {
        if s.name == "xarray_coord" {
            return Ok(Role::Coord);
        }
        if s.name == "xarray_data_var" {
            return Ok(Role::DataVar);
        }
    }
    Err(ClientError::Invalid(
        "child must carry 'xarray_coord' or 'xarray_data_var' spec".into(),
    ))
}

fn place_variable(ds: &mut Dataset, role: Role, name: String, var: Variable) {
    match role {
        Role::Coord => ds.insert_coord(name, var),
        Role::DataVar => ds.insert_data_var(name, var),
    }
}

// ---------------------------------------------------------------------------
// Arrow → tiled DType / bytes conversions (minimal coverage)
// ---------------------------------------------------------------------------

use crate::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType as ArrowDataType;

fn arrow_dtype_to_tiled_dtype(dt: &ArrowDataType) -> Result<DType> {
    let b = match dt {
        ArrowDataType::Float64 => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        ArrowDataType::Float32 => BuiltinDType::new(Endianness::Little, Kind::Float, 4),
        ArrowDataType::Int64 => BuiltinDType::new(Endianness::Little, Kind::Integer, 8),
        ArrowDataType::Int32 => BuiltinDType::new(Endianness::Little, Kind::Integer, 4),
        ArrowDataType::UInt64 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 8),
        ArrowDataType::UInt32 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 4),
        other => {
            return Err(ClientError::Invalid(format!(
                "unsupported arrow dtype in wide-table fetch: {other:?}"
            )));
        }
    };
    Ok(DType::Builtin(b))
}

fn array_to_le_bytes(array: &dyn Array, dt: &ArrowDataType) -> Result<Vec<u8>> {
    let out = match dt {
        ArrowDataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ClientError::Invalid("downcast Float64".into()))?;
            a.values().iter().flat_map(|v| v.to_le_bytes()).collect()
        }
        ArrowDataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| ClientError::Invalid("downcast Float32".into()))?;
            a.values().iter().flat_map(|v| v.to_le_bytes()).collect()
        }
        ArrowDataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ClientError::Invalid("downcast Int64".into()))?;
            a.values().iter().flat_map(|v| v.to_le_bytes()).collect()
        }
        ArrowDataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ClientError::Invalid("downcast Int32".into()))?;
            a.values().iter().flat_map(|v| v.to_le_bytes()).collect()
        }
        ArrowDataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ClientError::Invalid("downcast UInt64".into()))?;
            a.values().iter().flat_map(|v| v.to_le_bytes()).collect()
        }
        ArrowDataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| ClientError::Invalid("downcast UInt32".into()))?;
            a.values().iter().flat_map(|v| v.to_le_bytes()).collect()
        }
        other => {
            return Err(ClientError::Invalid(format!(
                "unsupported arrow dtype: {other:?}"
            )));
        }
    };
    Ok(out)
}

#[allow(dead_code)]
const _: fn() = || {
    // Keep ParsedStructure visible for future use.
    let _ = std::mem::size_of::<ParsedStructure>();
};
