//! HDF5 export serializers for the array and sparse families.
//!
//! `rust-hdf5`'s API only writes through a file path, so both serializers
//! round-trip through a temp file ([`h5_to_bytes`]): serialize → write → read
//! bytes → return.
//!
//! - **array** → a self-contained `.h5` with one dataset at `/data` storing the
//!   dense array. Python registers no `(array, application/x-hdf5)` serializer,
//!   so this is a Rust-only export; the `meta` it receives is the array
//!   *structure* (kind/itemsize/shape/byteorder), not the node's user metadata,
//!   so no dataset attributes are written (node-metadata→attrs lives in the
//!   container deep-export, which can reach `adapter.metadata()`).
//! - **sparse** → `data` (the nonzero values, 1-D) + `coords` (the COO
//!   coordinates, shape `(ndim, nnz)`, int64), matching Python
//!   `serialization/sparse.py:11-27` `serialize_hdf5`. The sparse route
//!   (`build_sparse_response`) hands the serializer a COO Arrow-IPC table
//!   (columns `dim0..dim{ndim-1}`, `data`), NOT a dense buffer, so the sparse
//!   family CANNOT reuse the array serializer — it would reinterpret the IPC
//!   bytes as a dense array and 500/corrupt. It decodes the IPC and writes the
//!   two upstream datasets instead. (Node-metadata→attrs is not written: the
//!   sparse route passes `meta = null` to every sparse serializer, so there is
//!   no user metadata to attach here.)

#![cfg(feature = "hdf5")]

use std::io::Read;

use bytes::Bytes;

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializeError, SerializerFn};

pub fn register_hdf5_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Array, mime::HDF5, hdf5_serializer());
    // Sparse gets its OWN serializer: the sparse route hands over a COO Arrow-IPC
    // table, not a dense buffer, so the array serializer would mis-decode it.
    reg.register(
        StructureFamily::Sparse,
        mime::HDF5,
        sparse_hdf5_serializer(),
    );
    reg.register_alias(".h5", mime::HDF5);
    reg.register_alias(".hdf5", mime::HDF5);
    reg.register_alias(".nx", mime::HDF5);
}

/// Write an `.h5` file via `rust-hdf5` (which only writes through a filesystem
/// path) and return its bytes. `write` populates datasets under the file's root
/// group. Shared by the array and sparse serializers.
fn h5_to_bytes<F>(write: F) -> Result<Bytes, SerializeError>
where
    F: FnOnce(&rust_hdf5::H5Group) -> Result<(), crate::serialization::hdf5_common::DynError>,
{
    // rust-hdf5 writes via filesystem; use a process-unique temp path.
    let tmp = tempfile::Builder::new()
        .prefix("tiled-h5-")
        .suffix(".h5")
        .tempfile()
        .map_err(|e| format!("temp file: {e}"))?;
    // Drop the NamedTempFile so HDF5 can open the path itself. `into_temp_path`
    // keeps a TempPath that auto-deletes on drop.
    let path = tmp.into_temp_path();
    let path_str = path.to_path_buf();

    let file = rust_hdf5::H5File::create(&path_str).map_err(|e| format!("hdf5 create: {e}"))?;
    write(&file.root_group()).map_err(|e| format!("hdf5 write: {e}"))?;
    // Close the file so all buffered bytes hit the path before we read them back.
    drop(file);

    let mut buf = Vec::new();
    std::fs::File::open(&path_str)
        .map_err(|e| format!("read back: {e}"))?
        .read_to_end(&mut buf)
        .map_err(|e| format!("read back: {e}"))?;
    Ok(Bytes::from(buf))
}

fn hdf5_serializer() -> SerializerFn {
    Box::new(|data, meta| -> Result<Bytes, SerializeError> {
        let itemsize = meta.get("itemsize").and_then(|v| v.as_u64()).unwrap_or(8) as usize;
        let kind = meta.get("kind").and_then(|v| v.as_str()).unwrap_or("f");
        let shape: Vec<usize> = meta
            .get("shape")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_u64().map(|n| n as usize))
                    .collect()
            })
            .unwrap_or_default();
        // The array adapter may emit big-endian (`>`) buffers (e.g. zarr/numpy
        // `>f8` dtypes). The host is little-endian (x86/arm), so a BE buffer
        // must be byte-swapped to native before reinterpreting as `T` — same
        // rule as the CSV serializer (array.rs `if big_endian { b.reverse() }`).
        let big_endian = meta
            .get("byteorder")
            .and_then(|v| v.as_str())
            .unwrap_or("<")
            == ">";

        h5_to_bytes(|root| {
            crate::serialization::hdf5_common::write_array_dataset(
                root,
                "data",
                data,
                kind.chars().next().unwrap_or('f'),
                itemsize,
                big_endian,
                &shape,
                // `meta` here is the array structure, not user metadata; no
                // node-metadata attrs for the Rust-only array export.
                &serde_json::Value::Null,
            )
        })
    })
}

/// HDF5 serializer for the sparse family (Python `serialization/sparse.py:11-27`
/// `serialize_hdf5`: `data` + `coords` datasets).
///
/// The sparse route (`build_sparse_response`) encodes a `SparseData` as a COO
/// Arrow-IPC table with columns `dim0..dim{ndim-1}` (one per coordinate axis)
/// and `data` (the nonzero values). This serializer inverts that: `data` →
/// a 1-D dataset with the values' dtype, and the stacked axes → a `(ndim, nnz)`
/// int64 `coords` dataset, matching h5py `create_dataset("coords",
/// data=sparse_arr.coords)` (upstream `sparse_arr.coords` has shape
/// `(ndim, nnz)`).
fn sparse_hdf5_serializer() -> SerializerFn {
    Box::new(|data, _meta| -> Result<Bytes, SerializeError> {
        use arrow::array::{Array, ArrayRef, Int64Array};
        use arrow::datatypes::DataType;
        use arrow::ipc::reader::FileReader;
        use std::io::Cursor;

        let reader = FileReader::try_new(Cursor::new(data.to_vec()), None)
            .map_err(|e| format!("sparse hdf5: ipc reader: {e}"))?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| format!("sparse hdf5: ipc batch: {e}"))?);
        }
        // build_sparse_response emits a single batch; concat is a no-op guard for
        // the general (0- or multi-batch) case.
        let batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| format!("sparse hdf5: concat batches: {e}"))?;

        // COO layout: dim0..dim{ndim-1}, data. Locate `data` by name; every other
        // column is a coordinate axis, kept in schema order (dim0, dim1, ...).
        let data_idx = schema
            .index_of("data")
            .map_err(|_| "sparse hdf5: COO table has no 'data' column")?;
        let data_col = batch.column(data_idx);
        let nnz = data_col.len();

        let dim_cols: Vec<&ArrayRef> = (0..batch.num_columns())
            .filter(|&i| i != data_idx)
            .map(|i| batch.column(i))
            .collect();
        let ndim = dim_cols.len();

        // Stack the axes into a row-major (ndim, nnz) int64 buffer — upstream
        // `sparse_arr.coords` is shape (ndim, nnz). Coords are int64 at runtime
        // (the resolver normalizes them); cast so a narrower int width still
        // writes as int64.
        let mut coords: Vec<i64> = Vec::with_capacity(ndim * nnz);
        for col in dim_cols {
            let casted = arrow::compute::cast(col, &DataType::Int64)
                .map_err(|e| format!("sparse hdf5: coord cast to int64: {e}"))?;
            let a = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("sparse hdf5: coord column not int64 after cast")?;
            coords.extend_from_slice(a.values());
        }
        let coords_bytes: Vec<u8> = coords.iter().flat_map(|v| v.to_le_bytes()).collect();

        h5_to_bytes(|root| {
            // `data`: the nonzero values (1-D), dtype preserved.
            crate::serialization::hdf5_common::write_table_column(root, "data", data_col.as_ref())?;
            // `coords`: the COO coordinates, shape (ndim, nnz), int64.
            crate::serialization::hdf5_common::write_array_dataset(
                root,
                "coords",
                &coords_bytes,
                'i',
                8,
                false,
                &[ndim, nnz],
                &serde_json::Value::Null,
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write the serializer's `.h5` blob to disk and read `/data` back.
    fn roundtrip_f64(bytes: &[u8], byteorder: &str, n: usize) -> Vec<f64> {
        let reg = SerializationRegistry::new();
        register_hdf5_serializer(&reg);
        let ser = reg.dispatch(StructureFamily::Array, mime::HDF5).unwrap();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "f", "byteorder": byteorder, "shape": [n],
        });
        let h5 = ser(bytes, &meta).expect("serialize");
        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &h5).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();
        file.dataset("data")
            .unwrap()
            .read_slice::<f64>(&[0], &[n])
            .unwrap()
            .to_vec()
    }

    /// Finding 2: a big-endian (`>`) source array must store its TRUE values,
    /// not byte-swapped. Before the fix, `write_typed` copied raw BE bytes into
    /// native (LE) `f64` and wrote a native datatype → double corruption.
    #[test]
    fn hdf5_serializer_honors_big_endian_byteorder() {
        let values = [1.5f64, -2.0, 3.25];
        let be: Vec<u8> = values.iter().flat_map(|v| v.to_be_bytes()).collect();
        let got = roundtrip_f64(&be, ">", values.len());
        assert_eq!(
            got,
            values.to_vec(),
            "big-endian source must store true values, not byte-swapped"
        );
    }

    /// LE and BE encodings of the same logical array read back identically.
    #[test]
    fn hdf5_serializer_le_and_be_agree() {
        let values = [10.0f64, -20.5, 30.0, 40.25];
        let le: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let be: Vec<u8> = values.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(roundtrip_f64(&le, "<", values.len()), values.to_vec());
        assert_eq!(roundtrip_f64(&be, ">", values.len()), values.to_vec());
    }

    /// Encode a COO Arrow-IPC table the way `build_sparse_response` does:
    /// one `dim{i}` column per coordinate axis, then `data`.
    fn coo_ipc_bytes(dims: &[Vec<i64>], data: &[f64]) -> Vec<u8> {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::FileWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
        for (i, axis) in dims.iter().enumerate() {
            fields.push(Field::new(format!("dim{i}"), DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(axis.clone())));
        }
        fields.push(Field::new("data", DataType::Float64, false));
        columns.push(Arc::new(Float64Array::from(data.to_vec())));

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut buf = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    /// Finding F1: the sparse family shares the array HDF5 serializer, which
    /// reinterprets the COO Arrow-IPC bytes as a dense `f8` array → 500/corrupt.
    /// The dedicated serializer must write upstream's two datasets: `data` (the
    /// nonzero values, 1-D) and `coords` (shape `(ndim, nnz)`, int64), matching
    /// Python `serialization/sparse.py` `serialize_hdf5`. Before the fix this
    /// panics/errors on serialize; after, both datasets read back correctly.
    #[test]
    fn sparse_hdf5_writes_data_and_coords() {
        // 2-D COO, 3 nonzeros: coords rows dim0=[0,1,2], dim1=[3,4,5].
        let ipc = coo_ipc_bytes(&[vec![0, 1, 2], vec![3, 4, 5]], &[10.0, 20.0, 30.0]);

        let reg = SerializationRegistry::new();
        register_hdf5_serializer(&reg);
        let ser = reg.dispatch(StructureFamily::Sparse, mime::HDF5).unwrap();
        // The sparse route always hands the serializer null meta.
        let h5 = ser(&ipc, &serde_json::Value::Null).expect("sparse hdf5 serialize");

        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &h5).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();

        // `data`: 1-D, the nonzero values, dtype preserved.
        let data_ds = file.dataset("data").unwrap();
        assert_eq!(data_ds.shape(), vec![3], "data is 1-D, length nnz");
        assert_eq!(
            data_ds.read_slice::<f64>(&[0], &[3]).unwrap(),
            vec![10.0, 20.0, 30.0],
            "data dataset values"
        );

        // `coords`: shape (ndim, nnz) = (2, 3), row-major [dim0.. ; dim1..].
        let coords_ds = file.dataset("coords").unwrap();
        assert_eq!(coords_ds.shape(), vec![2, 3], "coords is (ndim, nnz)");
        assert_eq!(
            coords_ds.read_raw::<i64>().unwrap(),
            vec![0, 1, 2, 3, 4, 5],
            "coords row-major: row0=dim0 coords, row1=dim1 coords"
        );
    }

    /// A 1-D sparse array (ndim=1) still writes `coords` as `(1, nnz)`, matching
    /// upstream's always-2-D `sparse_arr.coords`.
    #[test]
    fn sparse_hdf5_one_dimensional_coords_shape() {
        let ipc = coo_ipc_bytes(&[vec![2, 5]], &[7.0, 9.0]);

        let reg = SerializationRegistry::new();
        register_hdf5_serializer(&reg);
        let ser = reg.dispatch(StructureFamily::Sparse, mime::HDF5).unwrap();
        let h5 = ser(&ipc, &serde_json::Value::Null).expect("sparse hdf5 serialize");

        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &h5).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();

        assert_eq!(file.dataset("coords").unwrap().shape(), vec![1, 2]);
        assert_eq!(
            file.dataset("coords").unwrap().read_raw::<i64>().unwrap(),
            vec![2, 5]
        );
        assert_eq!(
            file.dataset("data")
                .unwrap()
                .read_slice::<f64>(&[0], &[2])
                .unwrap(),
            vec![7.0, 9.0]
        );
    }
}
