//! NetCDF-4 container adapter (via `rust-hdf5`).
//!
//! NetCDF-4 files are HDF5 containers with NetCDF conventions. This adapter
//! opens a `.nc` file as a **container** whose children are the file's
//! variables, mirroring Python tiled's
//! `DatasetAdapter.from_dataset(xarray.open_dataset(path, decode_times=False))`.
//!
//! Specs assigned:
//! - Container root: `xarray_dataset`
//! - Child whose HDF5 dataset carries `CLASS=DIMENSION_SCALE`: `xarray_coord`
//! - All other child datasets: `xarray_data_var`
//!
//! **Limitation**: NetCDF-3 classic format (not an HDF5 container) is not
//! supported — open such files with a separate adapter backed by `libnetcdf`.
//! NetCDF-4 (the default since netcdf-c 4.x) is fully supported.

#![cfg(feature = "netcdf-adapter")]

use std::path::PathBuf;
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

// ─── per-variable adapter ────────────────────────────────────────────────────

/// Lazy array adapter for a single NetCDF-4/HDF5 variable.
///
/// Holds the file path + variable name; reopens the file on each read so the
/// `H5File` handle (which is `!Send`) never crosses thread boundaries.
struct NetCdfVariableAdapter {
    path: PathBuf,
    variable: String,
    dtype: BuiltinDType,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    scalar_promoted: bool,
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
        let path = self.path.clone();
        let variable = self.variable.clone();
        let dtype = self.dtype.clone();
        let shape = self.structure.shape.clone();
        let scalar_promoted = self.scalar_promoted;
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

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        let path = self.path.clone();
        let variable = self.variable.clone();
        let dtype = self.dtype.clone();
        let shape = self.structure.shape.clone();
        let scalar_promoted = self.scalar_promoted;
        let slice = slice.clone();
        let block = block.to_vec();
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

    fn as_writable(&self) -> Option<&dyn ArrayAdapterWrite> {
        None
    }
}

// ─── public entry point ───────────────────────────────────────────────────────

/// Open a NetCDF-4 file and return a `MapAdapter` container whose children are
/// the file's root-level variables (data_vars + coords).
///
/// Called from `file_resolver.rs` inside a `spawn_blocking` context.
pub fn netcdf_from_path(path: PathBuf, metadata: serde_json::Value) -> Result<MapAdapter> {
    let file = rust_hdf5::H5File::open(&path)
        .map_err(|e| TiledError::Internal(format!("netcdf open {}: {e}", path.display())))?;

    // Global file attributes → container metadata["attrs"].
    let global_attrs = read_file_attrs(&file);
    let container_metadata = match metadata {
        serde_json::Value::Object(mut m) => {
            m.insert("attrs".into(), serde_json::Value::Object(global_attrs));
            serde_json::Value::Object(m)
        }
        _ => serde_json::json!({ "attrs": global_attrs }),
    };

    // Enumerate datasets at root level (NetCDF variables).
    // H5File::dataset_names() returns Vec<String> directly (infallible).
    let var_names = file.dataset_names();

    let mut children: IndexMap<String, AnyAdapter> = IndexMap::new();
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
        let chunks: Vec<Vec<usize>> = shape.iter().map(|d| vec![*d]).collect();

        // A dimension-scale dataset is a NetCDF coordinate variable.
        let is_coord = is_dimension_scale(&ds);
        let spec = if is_coord {
            Spec::new("xarray_coord")
        } else {
            Spec::new("xarray_data_var")
        };

        // Variable attributes → child metadata["attrs"].
        let var_attrs = read_dataset_attrs(&ds);
        let var_metadata = serde_json::json!({ "attrs": var_attrs });

        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape,
            dims: None,
            resizable: Default::default(),
        };

        let var_adapter = NetCdfVariableAdapter {
            path: path.clone(),
            variable: var_name.clone(),
            dtype,
            structure,
            metadata: var_metadata,
            specs: vec![spec],
            scalar_promoted,
        };

        children.insert(var_name, AnyAdapter::Array(Arc::new(var_adapter)));
    }

    Ok(MapAdapter::new(
        children,
        container_metadata,
        vec![Spec::new("xarray_dataset")],
    ))
}

// ─── HDF5 helpers ────────────────────────────────────────────────────────────

/// Read the full contents of an HDF5 dataset into a little-endian byte buffer,
/// then wrap it in a `DynNDArray`.
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

/// Read a dataset's entire contents as little-endian bytes, dispatching on the
/// stored element type (same kind/size dispatch as `Hdf5Adapter::read_native`).
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

/// Map an HDF5 dataset's type class to a `BuiltinDType`.
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

/// Return `true` when the dataset carries the NetCDF-4 dimension-scale marker
/// (`CLASS` attribute = `"DIMENSION_SCALE"`).
fn is_dimension_scale(ds: &rust_hdf5::H5Dataset) -> bool {
    if let Ok(attr) = ds.attr("CLASS")
        && let Ok(val) = attr.read_string()
    {
        return val == "DIMENSION_SCALE";
    }
    false
}

/// Read all scalar attributes of an HDF5 dataset into a JSON object, mirroring
/// Python `dict(data_array.attrs)`. Unsupported types (compound, enum, array)
/// are silently skipped — omitting is safe, misreading is not.
fn read_dataset_attrs(ds: &rust_hdf5::H5Dataset) -> serde_json::Map<String, serde_json::Value> {
    use rust_hdf5::DatatypeMessage;
    let mut out = serde_json::Map::new();
    let Ok(names) = ds.attr_names() else {
        return out;
    };
    for name in names {
        // Skip HDF5 internal dimension-scale bookkeeping attributes.
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

/// Read global (root-group) string attributes from an `H5File`.
///
/// `H5File` exposes `attr_string(name)` for string attributes. Non-string
/// global attributes are not accessible without the group's `attr()` handle,
/// so they are omitted. This covers the common case of NetCDF history /
/// Conventions / title / institution attributes.
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

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tiled_core::adapters::{BaseAdapter, ContainerAdapter};
    use tiled_core::dtype::Kind;
    use tiled_core::ndslice::NDSlice;
    use tiled_core::structures::StructureFamily;

    use super::*;

    /// Create a minimal NetCDF-4 compatible HDF5 fixture:
    ///
    /// - `time`    (f64, shape=[3]): dimension scale → `xarray_coord`
    /// - `temperature` (f32, shape=[3]): data variable → `xarray_data_var`
    /// - `pressure`    (i32, shape=[3]): data variable → `xarray_data_var`
    ///
    /// Global attrs: `title = "test dataset"`
    fn write_test_fixture(path: &std::path::Path) {
        let file = rust_hdf5::H5File::create(path).unwrap();
        file.set_attr_string("title", "test dataset").unwrap();

        // Coordinate: time (dimension scale).
        let time = file.new_dataset::<f64>().shape([3]).create("time").unwrap();
        time.write_raw(&[0.0f64, 1.0, 2.0]).unwrap();
        // Mark as dimension scale.
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

        // Data variable: temperature (f32).
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

        // Data variable: pressure (i32).
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

        // time: f64, shape=[3]
        let time = adapter.get("time").await.unwrap().unwrap();
        let time_arr = time.as_array().unwrap();
        assert_eq!(time_arr.structure().shape, vec![3]);
        match &time_arr.structure().data_type {
            DType::Builtin(b) => assert_eq!(b.kind, Kind::Float),
            other => panic!("expected builtin dtype, got {other:?}"),
        }

        // temperature: f32, shape=[3]
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

        // pressure: i32, shape=[3]
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

        // time: [0.0, 1.0, 2.0]
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

        // pressure: [1013, 1012, 1011]
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
}
