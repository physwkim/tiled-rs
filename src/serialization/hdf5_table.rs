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
//! Input is the Arrow IPC table the server hands every table serializer; the
//! `meta` argument is the table node's user metadata, written as HDF5 file (root
//! group) attributes — Python `serialize_hdf5`'s `file.attrs.update(metadata)`.
//! (Python also copies it onto each per-column dataset; with one dataset per
//! column the file is the single natural carrier, and the columns stay
//! attribute-free.) Scalar JSON values map to scalar attributes and homogeneous
//! JSON arrays to array attributes (nested rectangular arrays → N-D); a value
//! h5py cannot store (nested object, null, mixed-kind or `null`-bearing array, or
//! a ragged nested array) fails the export, the same fail-fast as Python's
//! `except TypeError: raise SerializationError` (see
//! [`crate::serialization::hdf5_common::write_file_attrs`]). Output is a
//! self-contained `.h5` file; like [`crate::serialization::hdf5_array`], `rust-hdf5` only writes
//! through a file path, so we round-trip through a temp file.
//!
//! Limitation: `rust-hdf5` 0.2.20 implements `H5Type` only for numeric/boolean
//! scalars — it has no string *dataset* support (its `VarLenUnicode` works only
//! for attributes). So numeric and boolean columns export; a string or temporal
//! column makes the whole table a hard error rather than emitting a non-standard
//! byte-packed surrogate. h5py (Python) does support string datasets, so this is
//! a known parity gap bounded by the Rust HDF5 library, not a design choice.

#![cfg(feature = "hdf5")]

use std::io::Read;

use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializeError, SerializerFn};

pub fn register_hdf5_table_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Table, mime::HDF5, hdf5_table_serializer());
    // `.h5`/`.hdf5`/`.nx` aliases are registered by hdf5_array; they resolve for
    // every family once any serializer exists for that family.
}

fn hdf5_table_serializer() -> SerializerFn {
    Box::new(|data, meta| -> Result<Bytes, SerializeError> {
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

        write_table(&path, &batch, meta).map_err(|e| format!("hdf5 write: {e}"))?;

        let mut buf = Vec::new();
        std::fs::File::open(&path)
            .map_err(|e| format!("read back: {e}"))?
            .read_to_end(&mut buf)
            .map_err(|e| format!("read back: {e}"))?;
        Ok(Bytes::from(buf))
    })
}

type DynError = Box<dyn std::error::Error + Send + Sync>;

fn write_table(
    path: &std::path::Path,
    batch: &RecordBatch,
    meta: &serde_json::Value,
) -> Result<(), DynError> {
    let file = rust_hdf5::H5File::create(path)?;
    let group = file.root_group();
    for (i, field) in batch.schema().fields().iter().enumerate() {
        crate::serialization::hdf5_common::write_table_column(
            &group,
            field.name(),
            batch.column(i).as_ref(),
        )?;
    }
    // Table node metadata → file (root group) attrs (Python `file.attrs.update`).
    crate::serialization::hdf5_common::write_file_attrs(&file, meta)?;
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

    /// The table node's metadata is written as HDF5 file (root group) attributes
    /// — Python `file.attrs.update(metadata)` — across the four scalar kinds.
    /// rust-hdf5 reads only dataset attr *values*, so file attrs are checked by
    /// name (the builder unit test covers per-kind values).
    #[test]
    fn table_hdf5_writes_metadata_as_file_attrs() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let meta = serde_json::json!({
            "sample": "NaCl", "temperature": 300, "scale": 1.5, "ok": true, "tags": [1, 2, 3],
        });
        let ser = registry()
            .dispatch(StructureFamily::Table, mime::HDF5)
            .unwrap();
        let h5 = ser(&ipc_bytes(&batch), &meta).expect("serialize");
        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &h5).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();
        let attrs = file.attr_names().unwrap();
        // Four scalar kinds plus a homogeneous array attr ("tags").
        for key in ["sample", "temperature", "scale", "ok", "tags"] {
            assert!(
                attrs.contains(&key.to_string()),
                "file attr '{key}' must be present; got {attrs:?}"
            );
        }
    }

    /// Python parity: a metadata value h5py cannot store fails the export,
    /// mirroring h5py's `TypeError → SerializationError`. A homogeneous array is
    /// now storable, so the unstorable case here is a nested object.
    #[test]
    fn table_hdf5_unstorable_metadata_fails() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let ser = registry()
            .dispatch(StructureFamily::Table, mime::HDF5)
            .unwrap();
        let err = ser(
            &ipc_bytes(&batch),
            &serde_json::json!({"calibration": {"slope": 1.0}}),
        )
        .expect_err("unstorable (nested-object) metadata must fail the export")
        .to_string();
        assert!(
            err.contains("calibration"),
            "error must name the offending key: {err}"
        );
    }
}
