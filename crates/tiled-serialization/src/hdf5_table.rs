//! HDF5 export serializer for the `table` family.
//!
//! Python registers `container.serialize_hdf5` for `StructureFamily.table`
//! (`tiled/serialization/table.py:176-181`). That walk yields one entry per
//! column but hands the whole DataFrame to `create_dataset` per column, which
//! is ill-defined for mixed-dtype tables (h5py forces a 2-D array or raises).
//! tiled-rs instead writes **each column as its own 1-D dataset** named after
//! the column — the HDF5-idiomatic shape, and the same rule the array-leaf and
//! sparse exporters use, so a container HDF5 export is uniform across families.
//!
//! Input is the Arrow IPC table the server hands every table serializer; output
//! is a self-contained `.h5` file. Like [`crate::hdf5_array`], `rust-hdf5` only
//! writes through a file path, so we round-trip through a temp file, and table
//! `metadata` is not written as HDF5 attributes (matching the array exporter;
//! JSON→HDF5 attribute mapping is intentionally out of scope).
//!
//! Limitation: `rust-hdf5` 0.2.20 implements `H5Type` only for numeric/boolean
//! scalars — it has no string *dataset* support (its `VarLenUnicode` works only
//! for attributes). So numeric and boolean columns export; a string or temporal
//! column makes the whole table a hard error rather than emitting a non-standard
//! byte-packed surrogate. h5py (Python) does support string datasets, so this is
//! a known parity gap bounded by the Rust HDF5 library, not a design choice.

#![cfg(feature = "hdf5")]

use std::io::Read;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializeError, SerializerFn};

pub fn register_hdf5_table_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Table, mime::HDF5, hdf5_table_serializer());
    // `.h5`/`.hdf5`/`.nx` aliases are registered by hdf5_array; they resolve for
    // every family once any serializer exists for that family.
}

fn hdf5_table_serializer() -> SerializerFn {
    Box::new(|data, _meta| -> Result<Bytes, SerializeError> {
        // Decode the Arrow IPC table into a single RecordBatch (the server hands
        // one IPC stream; concat multi-batch streams into one frame so each
        // column becomes a single contiguous dataset).
        let cursor = std::io::Cursor::new(data.to_vec());
        let reader = FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("ipc batch: {e}"))?;
        let batch = match batches.len() {
            0 => RecordBatch::new_empty(schema.clone()),
            1 => batches.into_iter().next().unwrap(),
            _ => arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| format!("ipc concat: {e}"))?,
        };

        // rust-hdf5 writes via the filesystem; use a process-unique temp path
        // that auto-deletes on drop (same pattern as hdf5_array).
        let tmp = tempfile::Builder::new()
            .prefix("tiled-h5tbl-")
            .suffix(".h5")
            .tempfile()
            .map_err(|e| format!("temp file: {e}"))?;
        let path = tmp.into_temp_path();

        write_table(&path, &batch).map_err(|e| format!("hdf5 write: {e}"))?;

        let mut buf = Vec::new();
        std::fs::File::open(&path)
            .map_err(|e| format!("read back: {e}"))?
            .read_to_end(&mut buf)
            .map_err(|e| format!("read back: {e}"))?;
        Ok(Bytes::from(buf))
    })
}

type DynError = Box<dyn std::error::Error + Send + Sync>;

fn write_table(path: &std::path::Path, batch: &RecordBatch) -> Result<(), DynError> {
    let file = rust_hdf5::H5File::create(path)?;
    for (i, field) in batch.schema().fields().iter().enumerate() {
        write_column(&file, field.name(), batch.column(i).as_ref())?;
    }
    Ok(())
}

/// Write one Arrow column as a 1-D HDF5 dataset named `name`.
///
/// HDF5 has no native null: float nulls become NaN (matching the JSON
/// serializer's NaN↔null rule and h5py's float behavior), and integer/boolean
/// nulls carry their underlying buffer value (pandas promotes nullable integer
/// columns to float, so a genuinely null-bearing integer column does not reach
/// the integer arms). String/temporal columns are unsupported — see the module
/// docs (rust-hdf5 has no string dataset type).
fn write_column(file: &rust_hdf5::H5File, name: &str, array: &dyn Array) -> Result<(), DynError> {
    let n = array.len();

    // Integer/unsigned: copy the underlying values buffer verbatim.
    macro_rules! write_copy {
        ($arr_ty:ty, $native:ty) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| -> DynError {
                    format!("downcast to {} failed", stringify!($arr_ty)).into()
                })?;
            let values: Vec<$native> = a.values().to_vec();
            file.new_dataset::<$native>()
                .shape([n])
                .create(name)?
                .write_raw(&values)?;
        }};
    }

    // Float: map each slot, turning Arrow nulls into NaN.
    macro_rules! write_float {
        ($arr_ty:ty, $native:ty) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| -> DynError {
                    format!("downcast to {} failed", stringify!($arr_ty)).into()
                })?;
            let values: Vec<$native> = (0..n)
                .map(|i| {
                    if a.is_null(i) {
                        <$native>::NAN
                    } else {
                        a.value(i)
                    }
                })
                .collect();
            file.new_dataset::<$native>()
                .shape([n])
                .create(name)?
                .write_raw(&values)?;
        }};
    }

    match array.data_type() {
        DataType::Int8 => write_copy!(Int8Array, i8),
        DataType::Int16 => write_copy!(Int16Array, i16),
        DataType::Int32 => write_copy!(Int32Array, i32),
        DataType::Int64 => write_copy!(Int64Array, i64),
        DataType::UInt8 => write_copy!(UInt8Array, u8),
        DataType::UInt16 => write_copy!(UInt16Array, u16),
        DataType::UInt32 => write_copy!(UInt32Array, u32),
        DataType::UInt64 => write_copy!(UInt64Array, u64),
        DataType::Float32 => write_float!(Float32Array, f32),
        DataType::Float64 => write_float!(Float64Array, f64),
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| -> DynError { "downcast to BooleanArray failed".into() })?;
            // No native bool dataset path; store as u8 (0/1), null → 0.
            let values: Vec<u8> = (0..n)
                .map(|i| u8::from(!a.is_null(i) && a.value(i)))
                .collect();
            file.new_dataset::<u8>()
                .shape([n])
                .create(name)?
                .write_raw(&values)?;
        }
        other => {
            return Err(format!(
                "hdf5 table serializer does not support column '{name}' of type {other:?} \
                 (rust-hdf5 has no string/temporal dataset support)"
            )
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut buf, &batch.schema()).unwrap();
            w.write(batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    fn registry() -> SerializationRegistry {
        let reg = SerializationRegistry::new();
        register_hdf5_table_serializer(&reg);
        reg
    }

    #[test]
    fn table_hdf5_registered() {
        assert!(
            registry()
                .dispatch(StructureFamily::Table, mime::HDF5)
                .is_some(),
            "table application/x-hdf5 must be registered"
        );
    }

    /// Each column lands as its own 1-D dataset named after the column, read
    /// back with the correct values (the column-per-dataset rule chosen over
    /// Python's whole-frame-per-column behavior). Covers float, int, and bool.
    #[test]
    fn table_hdf5_writes_one_dataset_per_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("temperature", DataType::Float64, false),
            Field::new("count", DataType::Int64, false),
            Field::new("ok", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap();

        let ser = registry()
            .dispatch(StructureFamily::Table, mime::HDF5)
            .unwrap();
        let h5 = ser(&ipc_bytes(&batch), &serde_json::Value::Null).expect("serialize");

        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &h5).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();

        let temp = file
            .dataset("temperature")
            .unwrap()
            .read_slice::<f64>(&[0], &[3])
            .unwrap()
            .to_vec();
        assert_eq!(temp, vec![1.5, 2.5, 3.5]);

        let count = file
            .dataset("count")
            .unwrap()
            .read_slice::<i64>(&[0], &[3])
            .unwrap()
            .to_vec();
        assert_eq!(count, vec![10, 20, 30]);

        let ok = file
            .dataset("ok")
            .unwrap()
            .read_slice::<u8>(&[0], &[3])
            .unwrap()
            .to_vec();
        assert_eq!(ok, vec![1, 0, 1]);
    }

    /// A float null becomes NaN in the stored dataset (HDF5 has no native null).
    #[test]
    fn table_hdf5_float_null_becomes_nan() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(3.0),
            ]))],
        )
        .unwrap();
        let ser = registry()
            .dispatch(StructureFamily::Table, mime::HDF5)
            .unwrap();
        let h5 = ser(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &h5).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();
        let x = file.dataset("x").unwrap().read_raw::<f64>().unwrap();
        assert_eq!(x[0], 1.0);
        assert!(x[1].is_nan(), "null float slot must be NaN");
        assert_eq!(x[2], 3.0);
    }

    /// A string column is a hard error — rust-hdf5 has no string dataset type,
    /// so the table fails to serialize rather than emitting a surrogate. The
    /// error names the offending column and type.
    #[test]
    fn table_hdf5_string_column_is_unsupported() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "label",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
            .unwrap();
        let ser = registry()
            .dispatch(StructureFamily::Table, mime::HDF5)
            .unwrap();
        let err = ser(&ipc_bytes(&batch), &serde_json::Value::Null)
            .expect_err("string column must be unsupported")
            .to_string();
        assert!(
            err.contains("label") && err.contains("Utf8"),
            "error must name the unsupported column and type: {err}"
        );
    }
}
