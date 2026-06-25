//! NetCDF container adapter: NetCDF-4/HDF5 via `rust-hdf5`, NetCDF-3 classic via `netcdf3`.
//!
//! The public entry point is `netcdf_from_path`, which reads the first 8 bytes
//! of the file to determine the format:
//!
//! - `\x89HDF\r\n\x1a\n` → NetCDF-4 (HDF5 container), read via `rust-hdf5`
//! - `CDF\x01` / `CDF\x02` / `CDF\x05` → NetCDF-3 classic, read via `netcdf3`
//!
//! Both paths produce the same container structure, mirroring Python tiled's
//! `DatasetAdapter.from_dataset(xarray.open_dataset(path, decode_times=False))`:
//!
//! - Container root spec: `xarray_dataset`
//! - Coordinate variable spec: `xarray_coord`
//! - Data variable spec: `xarray_data_var`
//!
//! **Coordinate detection:**
//! - HDF5: the dataset carries a `CLASS=DIMENSION_SCALE` HDF5 attribute.
//! - Classic: a 1-D variable whose name matches the name of its single
//!   dimension (the standard CF/NetCDF-3 convention).

#![cfg(feature = "netcdf-adapter")]

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use indexmap::IndexMap;

use tiled_core::adapters::{
    AnyAdapter, ArrayAdapterRead, ArrayAdapterWrite, BaseAdapter, BoxFuture,
};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

use crate::map_adapter::MapAdapter;

// ─── format detection ─────────────────────────────────────────────────────────

enum NetCdfFormat {
    Hdf5,
    Classic,
}

fn detect_format(path: &Path) -> Result<NetCdfFormat> {
    let mut buf = [0u8; 8];
    let mut f = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("netcdf probe {}: {e}", path.display())))?;
    f.read_exact(&mut buf)
        .map_err(|e| TiledError::Internal(format!("netcdf probe {}: {e}", path.display())))?;
    if buf.starts_with(b"\x89HDF\r\n\x1a\n") {
        Ok(NetCdfFormat::Hdf5)
    } else if buf.starts_with(b"CDF\x01")
        || buf.starts_with(b"CDF\x02")
        || buf.starts_with(b"CDF\x05")
    {
        Ok(NetCdfFormat::Classic)
    } else {
        Err(TiledError::Validation(format!(
            "{}: unrecognised NetCDF magic; expected HDF5 (\\x89HDF...) or CDF-1/2/5",
            path.display()
        )))
    }
}

// ─── backend ──────────────────────────────────────────────────────────────────

enum NetCdfBackend {
    /// Lazy HDF5 read: the file is reopened inside `spawn_blocking` per call.
    Hdf5 {
        path: PathBuf,
        variable: String,
        scalar_promoted: bool,
    },
    /// Eager in-memory read: data is read at open time and shared via `Arc`.
    Classic { array: Arc<DynNDArray> },
}

// ─── per-variable adapter ─────────────────────────────────────────────────────

struct NetCdfVariableAdapter {
    backend: NetCdfBackend,
    dtype: BuiltinDType,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl BaseAdapter for NetCdfVariableAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Array
    }
    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }
    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ArrayAdapterRead for NetCdfVariableAdapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        match &self.backend {
            NetCdfBackend::Hdf5 {
                path,
                variable,
                scalar_promoted,
            } => {
                let path = path.clone();
                let variable = variable.clone();
                let dtype = self.dtype.clone();
                let shape = self.structure.shape.clone();
                let scalar_promoted = *scalar_promoted;
                let slice = slice.clone();
                Box::pin(async move {
                    let arr = tokio::task::spawn_blocking(move || {
                        read_netcdf_var(path, variable, dtype, shape, scalar_promoted)
                    })
                    .await
                    .map_err(|e| TiledError::Internal(format!("netcdf spawn: {e}")))??;
                    arr.apply_slice(&slice)
                })
            }
            NetCdfBackend::Classic { array } => {
                let array = Arc::clone(array);
                let slice = slice.clone();
                Box::pin(async move { array.apply_slice(&slice) })
            }
        }
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        let block = block.to_vec();
        match &self.backend {
            NetCdfBackend::Hdf5 {
                path,
                variable,
                scalar_promoted,
            } => {
                let path = path.clone();
                let variable = variable.clone();
                let dtype = self.dtype.clone();
                let shape = self.structure.shape.clone();
                let scalar_promoted = *scalar_promoted;
                let slice = slice.clone();
                Box::pin(async move {
                    for (axis, &b) in block.iter().enumerate() {
                        if b != 0 {
                            return Err(TiledError::Validation(format!(
                                "netcdf adapter is single-chunk per axis; block[{axis}] = {b}"
                            )));
                        }
                    }
                    let arr = tokio::task::spawn_blocking(move || {
                        read_netcdf_var(path, variable, dtype, shape, scalar_promoted)
                    })
                    .await
                    .map_err(|e| TiledError::Internal(format!("netcdf spawn: {e}")))??;
                    arr.apply_slice(&slice)
                })
            }
            NetCdfBackend::Classic { array } => {
                let array = Arc::clone(array);
                let slice = slice.clone();
                Box::pin(async move {
                    for (axis, &b) in block.iter().enumerate() {
                        if b != 0 {
                            return Err(TiledError::Validation(format!(
                                "netcdf adapter is single-chunk per axis; block[{axis}] = {b}"
                            )));
                        }
                    }
                    array.apply_slice(&slice)
                })
            }
        }
    }

    fn as_writable(&self) -> Option<&dyn ArrayAdapterWrite> {
        None
    }
}

// ─── container assembly ───────────────────────────────────────────────────────

struct VarInfo {
    name: String,
    backend: NetCdfBackend,
    dtype: BuiltinDType,
    shape: Vec<usize>,
    is_coord: bool,
    attrs: serde_json::Map<String, serde_json::Value>,
}

fn build_map_adapter(
    vars: Vec<VarInfo>,
    global_attrs: serde_json::Map<String, serde_json::Value>,
    user_metadata: serde_json::Value,
) -> MapAdapter {
    let container_metadata = match user_metadata {
        serde_json::Value::Object(mut m) => {
            m.insert("attrs".into(), serde_json::Value::Object(global_attrs));
            serde_json::Value::Object(m)
        }
        _ => serde_json::json!({ "attrs": global_attrs }),
    };

    let mut children: IndexMap<String, AnyAdapter> = IndexMap::new();
    for info in vars {
        let var_metadata = serde_json::json!({ "attrs": info.attrs });
        let spec = if info.is_coord {
            Spec::new("xarray_coord")
        } else {
            Spec::new("xarray_data_var")
        };
        let chunks: Vec<Vec<usize>> = info.shape.iter().map(|d| vec![*d]).collect();
        let structure = ArrayStructure {
            data_type: DType::Builtin(info.dtype.clone()),
            chunks,
            shape: info.shape,
            dims: None,
            resizable: Default::default(),
        };
        let adapter = NetCdfVariableAdapter {
            backend: info.backend,
            dtype: info.dtype,
            structure,
            metadata: var_metadata,
            specs: vec![spec],
        };
        children.insert(info.name, AnyAdapter::Array(Arc::new(adapter)));
    }

    MapAdapter::new(
        children,
        container_metadata,
        vec![Spec::new("xarray_dataset")],
    )
}

// ─── public entry point ───────────────────────────────────────────────────────

/// Open a NetCDF file and return a `MapAdapter` container whose children are
/// the file's root-level variables (data_vars + coords).
///
/// Dispatches on file magic: HDF5 bytes → NetCDF-4 via `rust-hdf5`;
/// CDF-1/2/5 bytes → NetCDF-3 classic via `netcdf3`.
///
/// Called from `file_resolver.rs`.
pub fn netcdf_from_path(path: PathBuf, metadata: serde_json::Value) -> Result<MapAdapter> {
    let fmt = detect_format(&path)?;
    let (vars, global_attrs) = match fmt {
        NetCdfFormat::Hdf5 => open_hdf5(&path)?,
        NetCdfFormat::Classic => open_classic(&path)?,
    };
    Ok(build_map_adapter(vars, global_attrs, metadata))
}

// ─── HDF5 backend ─────────────────────────────────────────────────────────────

fn open_hdf5(path: &Path) -> Result<(Vec<VarInfo>, serde_json::Map<String, serde_json::Value>)> {
    let file = rust_hdf5::H5File::open(path)
        .map_err(|e| TiledError::Internal(format!("netcdf open {}: {e}", path.display())))?;

    let global_attrs = read_file_attrs(&file);
    // H5File::dataset_names() is infallible and returns Vec<String>.
    let var_names = file.dataset_names();

    let mut vars: Vec<VarInfo> = Vec::new();
    for var_name in var_names {
        let ds = match file.dataset(&var_name) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("netcdf: skipping {var_name}: {e}");
                continue;
            }
        };

        let dtype = match dtype_from_hdf5(&ds) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("netcdf: skipping {var_name}: unsupported dtype: {e}");
                continue;
            }
        };

        let raw_shape = ds.shape();
        let scalar_promoted = raw_shape.is_empty();
        let shape = if scalar_promoted {
            vec![1usize]
        } else {
            raw_shape
        };

        let is_coord = is_dimension_scale(&ds);
        let attrs = read_dataset_attrs(&ds);

        vars.push(VarInfo {
            name: var_name.clone(),
            backend: NetCdfBackend::Hdf5 {
                path: path.to_owned(),
                variable: var_name,
                scalar_promoted,
            },
            dtype,
            shape,
            is_coord,
            attrs,
        });
    }

    Ok((vars, global_attrs))
}

/// Read the full contents of an HDF5 dataset into a little-endian byte buffer.
fn read_netcdf_var(
    path: PathBuf,
    variable: String,
    dtype: BuiltinDType,
    shape: Vec<usize>,
    scalar_promoted: bool,
) -> Result<DynNDArray> {
    if shape.contains(&0) {
        return Ok(DynNDArray::new(Bytes::new(), dtype, shape));
    }
    let file = rust_hdf5::H5File::open(&path)
        .map_err(|e| TiledError::Internal(format!("netcdf reopen {}: {e}", path.display())))?;
    let ds = file
        .dataset(&variable)
        .map_err(|e| TiledError::Internal(format!("netcdf dataset {variable}: {e}")))?;

    let raw = read_full_as_le(&ds, &dtype)?;
    let final_shape = if scalar_promoted { vec![1usize] } else { shape };
    Ok(DynNDArray::new(Bytes::from(raw), dtype, final_shape))
}

fn read_full_as_le(ds: &rust_hdf5::H5Dataset, dtype: &BuiltinDType) -> Result<Vec<u8>> {
    macro_rules! read_as {
        ($t:ty) => {{
            let values = ds
                .read_raw::<$t>()
                .map_err(|e| TiledError::Internal(format!("netcdf read_raw: {e}")))?;
            let mut buf = Vec::with_capacity(values.len() * dtype.element_size());
            for v in &values {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Ok(buf)
        }};
    }
    match (dtype.kind, dtype.element_size()) {
        (Kind::Float, 8) => read_as!(f64),
        (Kind::Float, 4) => read_as!(f32),
        (Kind::Integer, 8) => read_as!(i64),
        (Kind::Integer, 4) => read_as!(i32),
        (Kind::Integer, 2) => read_as!(i16),
        (Kind::Integer, 1) => read_as!(i8),
        (Kind::UnsignedInteger, 8) => read_as!(u64),
        (Kind::UnsignedInteger, 4) => read_as!(u32),
        (Kind::UnsignedInteger, 2) => read_as!(u16),
        (Kind::UnsignedInteger, 1) => read_as!(u8),
        (kind, size) => Err(TiledError::Internal(format!(
            "netcdf dtype {kind:?}/{size}B not supported"
        ))),
    }
}

fn dtype_from_hdf5(ds: &rust_hdf5::H5Dataset) -> Result<BuiltinDType> {
    use rust_hdf5::DatatypeMessage;
    let datatype = ds
        .datatype()
        .map_err(|e| TiledError::Internal(format!("netcdf datatype: {e}")))?;
    let element_size = ds.element_size();
    let kind = match datatype {
        DatatypeMessage::FixedPoint { signed: true, .. } => Kind::Integer,
        DatatypeMessage::FixedPoint { signed: false, .. } => Kind::UnsignedInteger,
        DatatypeMessage::FloatingPoint { .. } => Kind::Float,
        other => {
            return Err(TiledError::Internal(format!(
                "netcdf variable type {other:?} not supported by tiled-rs adapter"
            )));
        }
    };
    let endianness = if element_size == 1 {
        Endianness::NotApplicable
    } else {
        Endianness::Little
    };
    Ok(BuiltinDType::new(endianness, kind, element_size))
}

fn is_dimension_scale(ds: &rust_hdf5::H5Dataset) -> bool {
    if let Ok(attr) = ds.attr("CLASS")
        && let Ok(val) = attr.read_string()
    {
        return val == "DIMENSION_SCALE";
    }
    false
}

fn read_dataset_attrs(ds: &rust_hdf5::H5Dataset) -> serde_json::Map<String, serde_json::Value> {
    use rust_hdf5::DatatypeMessage;
    let mut out = serde_json::Map::new();
    let Ok(names) = ds.attr_names() else {
        return out;
    };
    for name in names {
        if matches!(
            name.as_str(),
            "CLASS" | "NAME" | "DIMENSION_LIST" | "REFERENCE_LIST" | "_Netcdf4Dimid"
        ) {
            continue;
        }
        let Ok(attr) = ds.attr(&name) else { continue };
        let Ok(datatype) = attr.datatype() else {
            continue;
        };
        let value: Option<serde_json::Value> = match datatype {
            DatatypeMessage::FixedPoint {
                signed: true, size, ..
            } => match size {
                1 => attr.read_numeric::<i8>().ok().map(Into::into),
                2 => attr.read_numeric::<i16>().ok().map(Into::into),
                4 => attr.read_numeric::<i32>().ok().map(Into::into),
                8 => attr.read_numeric::<i64>().ok().map(Into::into),
                _ => None,
            },
            DatatypeMessage::FixedPoint {
                signed: false,
                size,
                ..
            } => match size {
                1 => attr.read_numeric::<u8>().ok().map(Into::into),
                2 => attr.read_numeric::<u16>().ok().map(Into::into),
                4 => attr.read_numeric::<u32>().ok().map(Into::into),
                8 => attr.read_numeric::<u64>().ok().map(Into::into),
                _ => None,
            },
            DatatypeMessage::FloatingPoint { size, .. } => match size {
                4 => attr
                    .read_numeric::<f32>()
                    .ok()
                    .and_then(|v| serde_json::Number::from_f64(v as f64))
                    .map(serde_json::Value::Number),
                8 => attr
                    .read_numeric::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(serde_json::Value::Number),
                _ => None,
            },
            DatatypeMessage::FixedString { .. } | DatatypeMessage::VarLenString { .. } => {
                attr.read_string().ok().map(serde_json::Value::String)
            }
            _ => None,
        };
        if let Some(v) = value {
            out.insert(name, v);
        }
    }
    out
}

fn read_file_attrs(file: &rust_hdf5::H5File) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    let Ok(names) = file.attr_names() else {
        return out;
    };
    for name in names {
        if let Ok(val) = file.attr_string(&name) {
            out.insert(name, serde_json::Value::String(val));
        }
    }
    out
}

// ─── NetCDF-3 classic backend ─────────────────────────────────────────────────

fn open_classic(path: &Path) -> Result<(Vec<VarInfo>, serde_json::Map<String, serde_json::Value>)> {
    let mut reader = netcdf3::FileReader::open(path)
        .map_err(|e| TiledError::Internal(format!("netcdf3 open {}: {e}", path.display())))?;

    // ── phase 1: collect all metadata while `data_set()` borrow is live ─────
    let global_attrs = nc3_read_global_attrs(reader.data_set());

    struct Nc3VarMeta {
        name: String,
        dtype: BuiltinDType,
        shape: Vec<usize>,
        is_coord: bool,
        attrs: serde_json::Map<String, serde_json::Value>,
    }

    let dim_names: std::collections::HashSet<String> =
        reader.data_set().dim_names().into_iter().collect();

    let var_metas: Vec<Nc3VarMeta> = reader
        .data_set()
        .get_var_names()
        .into_iter()
        .filter_map(|name| {
            let var = reader.data_set().get_var(&name)?;
            let dtype = nc3_dtype_to_builtin(var.data_type());
            // Rc<Dimension> is not Send; extract sizes immediately.
            let shape: Vec<usize> = var.get_dims().iter().map(|d| d.size()).collect();
            // Classic coordinate: 1-D variable whose name matches its dimension.
            let is_coord = var.num_dims() == 1 && (var.dim_names().first() == Some(&name));
            let _ = &dim_names; // ensure the HashSet is not dropped yet
            let attrs = nc3_read_var_attrs(var);
            Some(Nc3VarMeta {
                name,
                dtype,
                shape,
                is_coord,
                attrs,
            })
        })
        .collect();

    // ── phase 2: read all variable data ─────────────────────────────────────
    // `data_set()` borrow ends here; we can call mutable reader methods.
    let mut all_data = reader
        .read_all_vars()
        .map_err(|e| TiledError::Internal(format!("netcdf3 read {}: {e}", path.display())))?;

    // ── phase 3: build VarInfo ───────────────────────────────────────────────
    let mut vars: Vec<VarInfo> = Vec::with_capacity(var_metas.len());
    for meta in var_metas {
        let data_vec = match all_data.remove(&meta.name) {
            Some(dv) => dv,
            None => {
                tracing::warn!("netcdf3: no data for {}; skipping", meta.name);
                continue;
            }
        };

        let raw_shape = meta.shape;
        let scalar_promoted = raw_shape.is_empty();
        let shape = if scalar_promoted {
            vec![1usize]
        } else {
            raw_shape
        };

        let bytes = nc3_data_vector_to_bytes(data_vec);
        let array = Arc::new(DynNDArray::new(
            Bytes::from(bytes),
            meta.dtype.clone(),
            shape.clone(),
        ));

        vars.push(VarInfo {
            name: meta.name,
            backend: NetCdfBackend::Classic { array },
            dtype: meta.dtype,
            shape,
            is_coord: meta.is_coord,
            attrs: meta.attrs,
        });
    }

    Ok((vars, global_attrs))
}

fn nc3_dtype_to_builtin(dt: netcdf3::DataType) -> BuiltinDType {
    match dt {
        netcdf3::DataType::F64 => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        netcdf3::DataType::F32 => BuiltinDType::new(Endianness::Little, Kind::Float, 4),
        netcdf3::DataType::I32 => BuiltinDType::new(Endianness::Little, Kind::Integer, 4),
        netcdf3::DataType::I16 => BuiltinDType::new(Endianness::Little, Kind::Integer, 2),
        netcdf3::DataType::I8 => BuiltinDType::new(Endianness::NotApplicable, Kind::Integer, 1),
        netcdf3::DataType::U8 => {
            BuiltinDType::new(Endianness::NotApplicable, Kind::UnsignedInteger, 1)
        }
    }
}

fn nc3_data_vector_to_bytes(dv: netcdf3::DataVector) -> Vec<u8> {
    macro_rules! le_bytes {
        ($v:expr) => {{
            let v = $v;
            let mut buf = Vec::with_capacity(v.len() * std::mem::size_of_val(&v[0]));
            for x in v {
                buf.extend_from_slice(&x.to_le_bytes());
            }
            buf
        }};
    }
    match dv {
        netcdf3::DataVector::F64(v) => le_bytes!(v),
        netcdf3::DataVector::F32(v) => le_bytes!(v),
        netcdf3::DataVector::I32(v) => le_bytes!(v),
        netcdf3::DataVector::I16(v) => le_bytes!(v),
        // i8: to_le_bytes() on a 1-byte integer is just the raw byte value.
        netcdf3::DataVector::I8(v) => v.into_iter().map(|b| b.to_le_bytes()[0]).collect(),
        netcdf3::DataVector::U8(v) => v,
    }
}

fn nc3_attr_to_json(attr: &netcdf3::Attribute) -> Option<serde_json::Value> {
    match attr.data_type() {
        netcdf3::DataType::U8 => {
            // NC_CHAR is used for string data in NetCDF-3.
            attr.get_as_string().map(serde_json::Value::String)
        }
        netcdf3::DataType::I8 => {
            let s = attr.get_i8()?;
            Some(if s.len() == 1 {
                serde_json::json!(s[0] as i64)
            } else {
                serde_json::json!(s.iter().map(|&v| v as i64).collect::<Vec<_>>())
            })
        }
        netcdf3::DataType::I16 => {
            let s = attr.get_i16()?;
            Some(if s.len() == 1 {
                serde_json::json!(s[0] as i64)
            } else {
                serde_json::json!(s.iter().map(|&v| v as i64).collect::<Vec<_>>())
            })
        }
        netcdf3::DataType::I32 => {
            let s = attr.get_i32()?;
            Some(if s.len() == 1 {
                serde_json::json!(s[0] as i64)
            } else {
                serde_json::json!(s.iter().map(|&v| v as i64).collect::<Vec<_>>())
            })
        }
        netcdf3::DataType::F32 => {
            let s = attr.get_f32()?;
            if s.len() == 1 {
                serde_json::Number::from_f64(s[0] as f64).map(serde_json::Value::Number)
            } else {
                let arr: Option<Vec<serde_json::Value>> = s
                    .iter()
                    .map(|&v| serde_json::Number::from_f64(v as f64).map(serde_json::Value::Number))
                    .collect();
                arr.map(serde_json::Value::Array)
            }
        }
        netcdf3::DataType::F64 => {
            let s = attr.get_f64()?;
            if s.len() == 1 {
                serde_json::Number::from_f64(s[0]).map(serde_json::Value::Number)
            } else {
                let arr: Option<Vec<serde_json::Value>> = s
                    .iter()
                    .map(|&v| serde_json::Number::from_f64(v).map(serde_json::Value::Number))
                    .collect();
                arr.map(serde_json::Value::Array)
            }
        }
    }
}

fn nc3_read_global_attrs(ds: &netcdf3::DataSet) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for attr in ds.get_global_attrs() {
        if let Some(v) = nc3_attr_to_json(attr) {
            out.insert(attr.name().to_owned(), v);
        }
    }
    out
}

fn nc3_read_var_attrs(var: &netcdf3::Variable) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for attr in var.get_attrs() {
        if let Some(v) = nc3_attr_to_json(attr) {
            out.insert(attr.name().to_owned(), v);
        }
    }
    out
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tiled_core::adapters::{BaseAdapter, ContainerAdapter};
    use tiled_core::dtype::Kind;
    use tiled_core::ndslice::NDSlice;
    use tiled_core::structures::StructureFamily;

    use super::*;

    // ── NetCDF-4 fixture ─────────────────────────────────────────────────────

    /// Minimal NetCDF-4 HDF5 fixture:
    /// - `time`        (f64, [3]): dimension scale → `xarray_coord`
    /// - `temperature` (f32, [3]): data variable  → `xarray_data_var`
    /// - `pressure`    (i32, [3]): data variable  → `xarray_data_var`
    /// - global attr: `title = "test dataset"`
    fn write_test_fixture(path: &std::path::Path) {
        let file = rust_hdf5::H5File::create(path).unwrap();
        file.set_attr_string("title", "test dataset").unwrap();

        let time = file.new_dataset::<f64>().shape([3]).create("time").unwrap();
        time.write_raw(&[0.0f64, 1.0, 2.0]).unwrap();
        time.new_attr::<rust_hdf5::types::VarLenUnicode>()
            .shape(())
            .create("CLASS")
            .unwrap()
            .write_string("DIMENSION_SCALE")
            .unwrap();
        time.new_attr::<rust_hdf5::types::VarLenUnicode>()
            .shape(())
            .create("units")
            .unwrap()
            .write_string("seconds")
            .unwrap();
        drop(time);

        let temp = file
            .new_dataset::<f32>()
            .shape([3])
            .create("temperature")
            .unwrap();
        temp.write_raw(&[20.0f32, 21.0, 22.0]).unwrap();
        temp.new_attr::<rust_hdf5::types::VarLenUnicode>()
            .shape(())
            .create("units")
            .unwrap()
            .write_string("degC")
            .unwrap();
        drop(temp);

        let pres = file
            .new_dataset::<i32>()
            .shape([3])
            .create("pressure")
            .unwrap();
        pres.write_raw(&[1013i32, 1012, 1011]).unwrap();
        drop(pres);

        drop(file);
    }

    #[tokio::test]
    async fn container_structure_lists_all_variables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        assert_eq!(adapter.structure_family(), StructureFamily::Container);

        let structure = adapter.structure().await.unwrap();
        let mut keys = structure.keys.clone();
        keys.sort();
        assert_eq!(keys, vec!["pressure", "temperature", "time"]);
    }

    #[tokio::test]
    async fn xarray_dataset_spec_on_container() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let spec_names: Vec<&str> = adapter.specs().iter().map(|s| s.name.as_str()).collect();
        assert!(
            spec_names.contains(&"xarray_dataset"),
            "container must have xarray_dataset spec; got {spec_names:?}"
        );
    }

    #[tokio::test]
    async fn coordinate_gets_xarray_coord_spec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let time = adapter.get("time").await.unwrap().unwrap();
        let time_arr = time.as_array().expect("time must be an array");
        let spec_names: Vec<&str> = time_arr.specs().iter().map(|s| s.name.as_str()).collect();
        assert!(
            spec_names.contains(&"xarray_coord"),
            "time must have xarray_coord spec; got {spec_names:?}"
        );
    }

    #[tokio::test]
    async fn data_variable_gets_xarray_data_var_spec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().expect("temperature must be an array");
        let spec_names: Vec<&str> = temp_arr.specs().iter().map(|s| s.name.as_str()).collect();
        assert!(
            spec_names.contains(&"xarray_data_var"),
            "temperature must have xarray_data_var spec; got {spec_names:?}"
        );
    }

    #[tokio::test]
    async fn variable_shapes_and_dtypes_are_correct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();

        let time = adapter.get("time").await.unwrap().unwrap();
        let time_arr = time.as_array().unwrap();
        assert_eq!(time_arr.structure().shape, vec![3]);
        match &time_arr.structure().data_type {
            DType::Builtin(b) => assert_eq!(b.kind, Kind::Float),
            other => panic!("expected builtin dtype, got {other:?}"),
        }

        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().unwrap();
        assert_eq!(temp_arr.structure().shape, vec![3]);
        match &temp_arr.structure().data_type {
            DType::Builtin(b) => {
                assert_eq!(b.kind, Kind::Float);
                assert_eq!(b.element_size(), 4);
            }
            other => panic!("expected builtin dtype, got {other:?}"),
        }

        let pres = adapter.get("pressure").await.unwrap().unwrap();
        let pres_arr = pres.as_array().unwrap();
        assert_eq!(pres_arr.structure().shape, vec![3]);
        match &pres_arr.structure().data_type {
            DType::Builtin(b) => assert_eq!(b.kind, Kind::Integer),
            other => panic!("expected builtin dtype, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_returns_correct_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();

        let time = adapter.get("time").await.unwrap().unwrap();
        let time_arr = time.as_array().unwrap();
        let data = time_arr.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(data.shape, vec![3]);
        let v: Vec<f64> = data
            .data
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert!((v[0] - 0.0_f64).abs() < 1e-12);
        assert!((v[1] - 1.0_f64).abs() < 1e-12);
        assert!((v[2] - 2.0_f64).abs() < 1e-12);

        let pres = adapter.get("pressure").await.unwrap().unwrap();
        let pres_arr = pres.as_array().unwrap();
        let data = pres_arr.read(&NDSlice::empty()).await.unwrap();
        let vals: Vec<i32> = data
            .data
            .chunks_exact(4)
            .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(vals, vec![1013, 1012, 1011]);
    }

    #[tokio::test]
    async fn read_block_zero_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().unwrap();
        let data = temp_arr.read_block(&[0], &NDSlice::empty()).await.unwrap();
        assert_eq!(data.shape, vec![3]);
    }

    #[tokio::test]
    async fn read_block_nonzero_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().unwrap();
        let err = temp_arr.read_block(&[1], &NDSlice::empty()).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn global_attrs_appear_in_container_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let meta = adapter.metadata();
        assert_eq!(meta["attrs"]["title"], serde_json::json!("test dataset"));
    }

    #[tokio::test]
    async fn variable_attrs_appear_in_child_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().unwrap();
        assert_eq!(
            temp_arr.metadata()["attrs"]["units"],
            serde_json::json!("degC")
        );
    }

    #[tokio::test]
    async fn variables_are_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.nc");
        write_test_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().unwrap();
        assert!(temp_arr.as_writable().is_none());
    }

    // ── NetCDF-3 classic fixture ──────────────────────────────────────────────

    /// Minimal NetCDF-3 classic fixture:
    /// - `time`        (f64, [3]): dim "time" → `xarray_coord` (name == dim name)
    /// - `temperature` (f32, [3]): dim "time" → `xarray_data_var`
    /// - `pressure`    (i32, [3]): dim "time" → `xarray_data_var`
    /// - global attr: `title = "test nc3 dataset"` (NC_CHAR)
    /// - `time` attr: `units = "seconds"`
    /// - `temperature` attr: `units = "degC"`
    fn write_nc3_fixture(path: &std::path::Path) {
        let mut ds = netcdf3::DataSet::new();
        ds.add_fixed_dim("time", 3).unwrap();
        ds.add_var_f64("time", &["time"]).unwrap();
        ds.add_var_f32("temperature", &["time"]).unwrap();
        ds.add_var_i32("pressure", &["time"]).unwrap();

        ds.add_global_attr_string("title", "test nc3 dataset")
            .unwrap();

        ds.get_var_mut("time")
            .unwrap()
            .add_attr_string("units", "seconds")
            .unwrap();
        ds.get_var_mut("temperature")
            .unwrap()
            .add_attr_string("units", "degC")
            .unwrap();

        let mut writer = netcdf3::FileWriter::create_new(path).unwrap();
        writer.set_def(&ds, netcdf3::Version::Classic, 0).unwrap();
        writer.write_var_f64("time", &[0.0f64, 1.0, 2.0]).unwrap();
        writer
            .write_var_f32("temperature", &[20.0f32, 21.0, 22.0])
            .unwrap();
        writer
            .write_var_i32("pressure", &[1013i32, 1012, 1011])
            .unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn nc3_container_structure_lists_all_variables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        assert_eq!(adapter.structure_family(), StructureFamily::Container);

        let structure = adapter.structure().await.unwrap();
        let mut keys = structure.keys.clone();
        keys.sort();
        assert_eq!(keys, vec!["pressure", "temperature", "time"]);
    }

    #[tokio::test]
    async fn nc3_xarray_dataset_spec_on_container() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let spec_names: Vec<&str> = adapter.specs().iter().map(|s| s.name.as_str()).collect();
        assert!(
            spec_names.contains(&"xarray_dataset"),
            "nc3 container must have xarray_dataset spec; got {spec_names:?}"
        );
    }

    #[tokio::test]
    async fn nc3_coordinate_gets_xarray_coord_spec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let time = adapter.get("time").await.unwrap().unwrap();
        let time_arr = time.as_array().expect("time must be an array");
        let spec_names: Vec<&str> = time_arr.specs().iter().map(|s| s.name.as_str()).collect();
        assert!(
            spec_names.contains(&"xarray_coord"),
            "nc3 time must have xarray_coord spec; got {spec_names:?}"
        );
    }

    #[tokio::test]
    async fn nc3_data_variable_gets_xarray_data_var_spec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().expect("temperature must be an array");
        let spec_names: Vec<&str> = temp_arr.specs().iter().map(|s| s.name.as_str()).collect();
        assert!(
            spec_names.contains(&"xarray_data_var"),
            "nc3 temperature must have xarray_data_var spec; got {spec_names:?}"
        );
    }

    #[tokio::test]
    async fn nc3_variable_shapes_and_dtypes_are_correct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();

        let time = adapter.get("time").await.unwrap().unwrap();
        let time_arr = time.as_array().unwrap();
        assert_eq!(time_arr.structure().shape, vec![3]);
        match &time_arr.structure().data_type {
            DType::Builtin(b) => {
                assert_eq!(b.kind, Kind::Float);
                assert_eq!(b.element_size(), 8);
            }
            other => panic!("expected builtin dtype, got {other:?}"),
        }

        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().unwrap();
        assert_eq!(temp_arr.structure().shape, vec![3]);
        match &temp_arr.structure().data_type {
            DType::Builtin(b) => {
                assert_eq!(b.kind, Kind::Float);
                assert_eq!(b.element_size(), 4);
            }
            other => panic!("expected builtin dtype, got {other:?}"),
        }

        let pres = adapter.get("pressure").await.unwrap().unwrap();
        let pres_arr = pres.as_array().unwrap();
        assert_eq!(pres_arr.structure().shape, vec![3]);
        match &pres_arr.structure().data_type {
            DType::Builtin(b) => {
                assert_eq!(b.kind, Kind::Integer);
                assert_eq!(b.element_size(), 4);
            }
            other => panic!("expected builtin dtype, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nc3_read_returns_correct_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();

        let time = adapter.get("time").await.unwrap().unwrap();
        let time_arr = time.as_array().unwrap();
        let data = time_arr.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(data.shape, vec![3]);
        let v: Vec<f64> = data
            .data
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert!((v[0] - 0.0_f64).abs() < 1e-12);
        assert!((v[1] - 1.0_f64).abs() < 1e-12);
        assert!((v[2] - 2.0_f64).abs() < 1e-12);

        let pres = adapter.get("pressure").await.unwrap().unwrap();
        let pres_arr = pres.as_array().unwrap();
        let data = pres_arr.read(&NDSlice::empty()).await.unwrap();
        let vals: Vec<i32> = data
            .data
            .chunks_exact(4)
            .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(vals, vec![1013, 1012, 1011]);
    }

    #[tokio::test]
    async fn nc3_global_attrs_appear_in_container_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let meta = adapter.metadata();
        assert_eq!(
            meta["attrs"]["title"],
            serde_json::json!("test nc3 dataset")
        );
    }

    #[tokio::test]
    async fn nc3_variable_attrs_appear_in_child_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test3.nc");
        write_nc3_fixture(&path);

        let adapter = netcdf_from_path(path, serde_json::Value::Null).unwrap();
        let temp = adapter.get("temperature").await.unwrap().unwrap();
        let temp_arr = temp.as_array().unwrap();
        assert_eq!(
            temp_arr.metadata()["attrs"]["units"],
            serde_json::json!("degC")
        );
    }
}
