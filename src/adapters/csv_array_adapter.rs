//! CSV array adapter.
//!
//! Reads a headerless CSV file as a 2-D numeric array (rows × columns).
//! Corresponds to `tiled/adapters/csv.py:CSVArrayAdapter`.
//!
//! Schema inference: if every column is integer-typed (after Arrow's CSV
//! inference on the first 64 rows) and no cell is empty, the array dtype is
//! `<i8` (int64 little-endian); if any column is floating-point, the array dtype
//! is `<f8` (float64 little-endian). Mixed numeric CSVs are promoted to float64.
//! A missing value (empty cell) also promotes to float64 with NaN at the gap,
//! matching pandas' `read_csv` (which has no typed-int-with-null).
//!
//! Read-only. Single chunk covering the whole array.

#![cfg(feature = "csv-adapter")]

use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::compute::cast;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, SchemaRef};
use bytes::Bytes;

use crate::core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use crate::core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use crate::core::error::{Result, TiledError};
use crate::core::ndslice::NDSlice;
use crate::core::structures::{ArrayStructure, Spec, StructureFamily};

#[derive(Debug)]
pub struct CsvArrayAdapter {
    array: DynNDArray,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl CsvArrayAdapter {
    /// Open a headerless CSV file and load its numeric contents as a 2-D array.
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        let (array, shape, dtype) = read_csv_array(&path)?;
        let chunks: Vec<Vec<usize>> = shape.iter().map(|d| vec![*d]).collect();
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: None,
            resizable: Default::default(),
        };
        Ok(Self {
            array,
            structure,
            metadata,
            specs: vec![Spec::new("csv-array")],
        })
    }
}

impl BaseAdapter for CsvArrayAdapter {
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

impl ArrayAdapterRead for CsvArrayAdapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { self.array.apply_slice(slice) })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            for (axis, &b) in block.iter().enumerate() {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "csv-array adapter has one chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            self.array.apply_slice(slice)
        })
    }

    fn as_writable(&self) -> Option<&dyn crate::core::adapters::ArrayAdapterWrite> {
        None
    }
}

/// Read a headerless CSV and produce a C-order (row-major) byte buffer,
/// the shape `[nrows, ncols]`, and the chosen Arrow BuiltinDType.
///
/// Dtype promotion: int64 if all inferred columns are integer types; float64
/// if any column is a float type, if any cell is empty (see the missing-value
/// handling below), or if any column is all-empty (Arrow infers `Null`, which
/// pandas reads as an all-NaN float64 column). Anything else (strings, booleans)
/// causes a Validation error — this adapter is for numeric-only CSVs.
fn read_csv_array(path: &std::path::Path) -> Result<(DynNDArray, Vec<usize>, BuiltinDType)> {
    // Infer schema from the first 64 rows.
    let f = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let mut buf = BufReader::new(f);
    let format = arrow::csv::reader::Format::default().with_header(false);
    let (raw_schema, _) = format
        .infer_schema(&mut buf, Some(64))
        .map_err(|e| TiledError::Internal(format!("infer schema: {e}")))?;

    // Validate column types and take the schema-level float decision (any float
    // column ⇒ float64). Missing-value promotion is applied below, once the data
    // is read and nulls are visible.
    let schema_float = decide_dtype(raw_schema.fields().iter().map(|f| f.data_type()))?;
    let schema = Arc::new(raw_schema) as SchemaRef;

    // Read all batches.
    let f2 = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let reader = ReaderBuilder::new(schema.clone())
        .with_header(false)
        .build(f2)
        .map_err(|e| TiledError::Internal(format!("csv build: {e}")))?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| TiledError::Internal(format!("csv read: {e}")))?);
    }

    let ncols = schema.fields().len();
    let nrows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Parity with pandas' `read_csv` promotion (upstream reads via
    // dask/pandas, `csv.py:290`): an empty cell in an int-looking column makes
    // that column float64 with NaN at the gap — pandas has no typed-int-with-null
    // from a CSV, and dask's `assume_missing` promotes int columns that might be
    // missing to float64. Arrow's CSV reader instead keeps the column Int64 with
    // a null bitmap, so we reproduce the promotion here: any null anywhere forces
    // float64 output, and the float decode writes NaN at the null slots. This
    // guarantees the int decode path below never sees a null.
    let has_null = batches
        .iter()
        .any(|b| b.columns().iter().any(|c| c.null_count() > 0));
    let use_float = schema_float || has_null;

    // Handle empty file.
    if nrows == 0 || ncols == 0 {
        let dtype = if use_float {
            BuiltinDType::new(Endianness::Little, Kind::Float, 8)
        } else {
            BuiltinDType::new(Endianness::Little, Kind::Integer, 8)
        };
        let shape = vec![nrows, ncols];
        let array = DynNDArray::new(Bytes::new(), dtype.clone(), shape.clone());
        return Ok((array, shape, dtype));
    }

    // Build C-order output buffer.
    const ELEM: usize = 8;
    let mut out = vec![0u8; nrows * ncols * ELEM];
    let mut row_offset = 0;

    for batch in &batches {
        let nr = batch.num_rows();
        for (c, col) in batch.columns().iter().enumerate() {
            if use_float {
                let casted = cast(col, &DataType::Float64)
                    .map_err(|e| TiledError::Internal(format!("cast col {c} to f64: {e}")))?;
                let arr = casted
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| TiledError::Internal("downcast Float64Array".into()))?;
                for r in 0..nr {
                    let dest = ((row_offset + r) * ncols + c) * ELEM;
                    // A null (empty cell) decodes to NaN, matching pandas'
                    // missing-value semantics. Arrow leaves the value buffer of a
                    // null slot unspecified, so `arr.value(r)` there would be
                    // garbage; the `is_null` check is what makes the read faithful.
                    let v = if arr.is_null(r) {
                        f64::NAN
                    } else {
                        arr.value(r)
                    };
                    out[dest..dest + ELEM].copy_from_slice(&v.to_le_bytes());
                }
            } else {
                let casted = cast(col, &DataType::Int64)
                    .map_err(|e| TiledError::Internal(format!("cast col {c} to i64: {e}")))?;
                let arr = casted
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| TiledError::Internal("downcast Int64Array".into()))?;
                for r in 0..nr {
                    let dest = ((row_offset + r) * ncols + c) * ELEM;
                    // `use_float` is forced true whenever any cell is null (see the
                    // promotion above), so an integer column here is null-free and
                    // `arr.value(r)` is always a real value.
                    out[dest..dest + ELEM].copy_from_slice(&arr.value(r).to_le_bytes());
                }
            }
        }
        row_offset += nr;
    }

    let dtype = if use_float {
        BuiltinDType::new(Endianness::Little, Kind::Float, 8)
    } else {
        BuiltinDType::new(Endianness::Little, Kind::Integer, 8)
    };
    let shape = vec![nrows, ncols];
    let array = DynNDArray::new(Bytes::from(out), dtype.clone(), shape.clone());
    Ok((array, shape, dtype))
}

/// Returns `true` (float64) if the array must be float64, `false` (int64) if
/// every column is integer-typed. Errors on non-numeric column types.
///
/// A `DataType::Null` column is Arrow's inference for a column whose cells are
/// all empty. pandas/dask `read_csv` reads such a column as an all-NaN float64
/// column (a missing value has no integer representation), so it promotes the
/// array to float64 here — the same promotion the partial-null path applies via
/// `has_null`, and consistent with float decoding an all-null column to NaN.
fn decide_dtype<'a>(fields: impl Iterator<Item = &'a DataType>) -> Result<bool> {
    let mut has_float = false;
    for dt in fields {
        match dt {
            DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Null => {
                has_float = true
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {}
            other => {
                return Err(TiledError::Validation(format!(
                    "csv-array adapter requires a numeric CSV; got column type {other:?}"
                )));
            }
        }
    }
    Ok(has_float)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::core::adapters::{ArrayAdapterRead, BaseAdapter};
    use crate::core::dtype::Kind;
    use crate::core::ndslice::NDSlice;
    use crate::core::structures::StructureFamily;

    use super::CsvArrayAdapter;

    fn write_csv(path: &std::path::Path, content: &str) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
    }

    #[tokio::test]
    async fn int_csv_gives_int64_array() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.csv");
        write_csv(&p, "1,2,3\n4,5,6\n7,8,9\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        assert_eq!(adapter.structure().shape, vec![3, 3]);
        assert_eq!(adapter.structure_family(), StructureFamily::Array);
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(arr.shape, vec![3, 3]);
        assert_eq!(arr.dtype.kind, Kind::Integer);
        // Row-major: first 8 bytes = value 1 as little-endian i64
        let first = i64::from_le_bytes(arr.data[..8].try_into().unwrap());
        assert_eq!(first, 1i64);
    }

    #[tokio::test]
    async fn float_csv_gives_float64_array() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("b.csv");
        write_csv(&p, "1.0,2.0\n3.5,4.5\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        assert_eq!(adapter.structure().shape, vec![2, 2]);
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(arr.dtype.kind, Kind::Float);
        let v = f64::from_le_bytes(arr.data[8..16].try_into().unwrap());
        assert!((v - 2.0_f64).abs() < 1e-12, "expected 2.0, got {v}");
    }

    #[tokio::test]
    async fn mixed_int_float_promotes_to_float64() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.csv");
        write_csv(&p, "1,2.5\n3,4.0\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        assert_eq!(adapter.structure().shape, vec![2, 2]);
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(arr.dtype.kind, Kind::Float);
    }

    #[tokio::test]
    async fn empty_csv_gives_zero_row_array() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("d.csv");
        write_csv(&p, "");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        assert_eq!(adapter.structure().shape, vec![0, 0]);
    }

    #[tokio::test]
    async fn read_block_zero_is_identity() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("e.csv");
        write_csv(&p, "10,20\n30,40\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let arr = adapter
            .read_block(&[0, 0], &NDSlice::empty())
            .await
            .unwrap();
        assert_eq!(arr.shape, vec![2, 2]);
    }

    #[tokio::test]
    async fn read_block_nonzero_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f.csv");
        write_csv(&p, "1,2\n3,4\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let err = adapter.read_block(&[1, 0], &NDSlice::empty()).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn is_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("g.csv");
        write_csv(&p, "1,2\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        assert!(adapter.as_writable().is_none());
    }

    #[tokio::test]
    async fn row_major_order_is_correct() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("h.csv");
        // row 0: [10, 20], row 1: [30, 40]
        write_csv(&p, "10,20\n30,40\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        let vals: Vec<i64> = arr
            .data
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(vals, vec![10, 20, 30, 40]);
    }

    #[tokio::test]
    async fn spec_name_is_csv_array() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("i.csv");
        write_csv(&p, "1\n2\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        assert_eq!(adapter.specs()[0].name, "csv-array");
    }

    #[tokio::test]
    async fn string_column_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("j.csv");
        write_csv(&p, "foo,bar\nbaz,qux\n");
        let err = CsvArrayAdapter::from_path(p, serde_json::Value::Null);
        assert!(err.is_err());
    }

    // ---- missing-value (empty cell) parity (Wave-18 follow-up) ---------

    /// Collect a DynNDArray's f64 cells in row-major order.
    fn f64_cells(arr: &crate::core::dtype::DynNDArray) -> Vec<f64> {
        arr.data
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    // Invariant boundary — a float column with an empty cell. pandas reads the
    // gap as NaN; Arrow keeps the slot null with an unspecified value buffer, so
    // the decode must write NaN, not the garbage `arr.value(r)` would return.
    #[tokio::test]
    async fn float_column_empty_cell_becomes_nan() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fnull.csv");
        // col1 is float-typed (has 2.5); row 1 col1 is empty.
        write_csv(&p, "1.0,2.5\n3.0,\n5.0,6.5\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(arr.dtype.kind, Kind::Float);
        assert_eq!(arr.shape, vec![3, 2]);
        let cells = f64_cells(&arr);
        // row-major: [1.0, 2.5, 3.0, NaN, 5.0, 6.5]
        assert_eq!(cells[0], 1.0);
        assert_eq!(cells[1], 2.5);
        assert_eq!(cells[2], 3.0);
        assert!(
            cells[3].is_nan(),
            "empty float cell must be NaN, got {}",
            cells[3]
        );
        assert_eq!(cells[4], 5.0);
        assert_eq!(cells[5], 6.5);
    }

    // Invariant boundary — an int-looking column with an empty cell. Arrow infers
    // it Int64 with a null bitmap, but pandas promotes such a column to
    // float64+NaN (no typed-int-with-null exists from read_csv). The array must
    // therefore come back float64 with NaN at the gap, not int64 with a garbage
    // substitute.
    #[tokio::test]
    async fn int_column_empty_cell_promotes_to_float_nan() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("inull.csv");
        // Every cell is integer-looking; row 1 col1 is empty.
        write_csv(&p, "1,2\n3,\n5,6\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(
            arr.dtype.kind,
            Kind::Float,
            "int-looking CSV with a missing value must promote to float64, as pandas does"
        );
        assert_eq!(arr.shape, vec![3, 2]);
        let cells = f64_cells(&arr);
        // row-major: [1, 2, 3, NaN, 5, 6]
        assert_eq!(cells[0], 1.0);
        assert_eq!(cells[1], 2.0);
        assert_eq!(cells[2], 3.0);
        assert!(
            cells[3].is_nan(),
            "empty int cell must decode to NaN, got {}",
            cells[3]
        );
        assert_eq!(cells[4], 5.0);
        assert_eq!(cells[5], 6.0);
    }

    // Invariant boundary — a null confined to one column still promotes the whole
    // homogeneous array to float64 (numpy cannot hold mixed column dtypes in a
    // 2-D builtin array; pandas' `to_dask_array` upcasts to float64). The
    // all-integer, no-empty column must come back as exact float values.
    #[tokio::test]
    async fn null_in_one_column_promotes_whole_array() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("onenull.csv");
        // col0 all-int, no empties; col1 int-looking with one empty at row 1.
        write_csv(&p, "10,20\n30,\n50,60\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(arr.dtype.kind, Kind::Float);
        let cells = f64_cells(&arr);
        assert_eq!(cells[0], 10.0);
        assert_eq!(cells[2], 30.0);
        assert!(
            cells[3].is_nan(),
            "empty cell in col1 must be NaN, got {}",
            cells[3]
        );
        assert_eq!(cells[4], 50.0);
    }

    // Invariant boundary — a column whose cells are ALL empty. Arrow infers it as
    // `DataType::Null` (not a numeric type), which `decide_dtype` used to reject
    // outright. pandas/dask `read_csv` reads an all-empty column as an all-NaN
    // float64 column, so the whole array must promote to float64 with that column
    // entirely NaN, matching the partial-empty promotion.
    #[tokio::test]
    async fn all_empty_column_among_ints_promotes_to_float_nan() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("emptycol.csv");
        // col0 = [1,2,3] int; col1 entirely empty (trailing comma each row).
        write_csv(&p, "1,\n2,\n3,\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(
            arr.dtype.kind,
            Kind::Float,
            "an all-empty column must promote the array to float64, as pandas does"
        );
        assert_eq!(arr.shape, vec![3, 2]);
        let cells = f64_cells(&arr);
        // row-major: [1, NaN, 2, NaN, 3, NaN]
        assert_eq!(cells[0], 1.0);
        assert!(
            cells[1].is_nan(),
            "empty col1 row0 must be NaN, got {}",
            cells[1]
        );
        assert_eq!(cells[2], 2.0);
        assert!(
            cells[3].is_nan(),
            "empty col1 row1 must be NaN, got {}",
            cells[3]
        );
        assert_eq!(cells[4], 3.0);
        assert!(
            cells[5].is_nan(),
            "empty col1 row2 must be NaN, got {}",
            cells[5]
        );
    }

    // Invariant boundary — every column all-empty (a file that is all empty cells
    // but has detectable column structure, e.g. `",\n,\n"`). Every column infers
    // as `Null`; the array is float64 with every cell NaN, matching pandas.
    #[tokio::test]
    async fn all_empty_columns_file_is_all_nan_float() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("allempty.csv");
        // 3 rows × 2 cols, every cell empty.
        write_csv(&p, ",\n,\n,\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let arr = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(arr.dtype.kind, Kind::Float);
        assert_eq!(arr.shape, vec![3, 2]);
        let cells = f64_cells(&arr);
        assert_eq!(cells.len(), 6);
        assert!(
            cells.iter().all(|v| v.is_nan()),
            "every cell of an all-empty file must be NaN, got {cells:?}"
        );
    }

    // Boundary — a file of only blank lines has no parseable columns. Arrow infers
    // a 0-field schema and errors at read; pandas raises EmptyDataError ("No
    // columns to parse from file") for the same input, so an error is the
    // parity-correct outcome. This is pre-existing behavior; the all-empty-column
    // promotion above does not change it (the error is raised at CSV read, before
    // dtype promotion). Documented here to pin it.
    #[tokio::test]
    async fn blank_lines_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("blank.csv");
        write_csv(&p, "\n\n\n");
        let r = CsvArrayAdapter::from_path(p, serde_json::Value::Null);
        assert!(
            r.is_err(),
            "a file of only blank lines has no columns; pandas raises EmptyDataError"
        );
    }

    #[tokio::test]
    async fn slice_reduces_rows() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("k.csv");
        // 4 rows × 2 cols
        write_csv(&p, "1,2\n3,4\n5,6\n7,8\n");
        let adapter = CsvArrayAdapter::from_path(p, serde_json::Value::Null).unwrap();
        let slice = NDSlice::from_numpy_str("1:3,:").unwrap();
        let arr = adapter.read(&slice).await.unwrap();
        assert_eq!(arr.shape, vec![2, 2]);
        let vals: Vec<i64> = arr
            .data
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(vals, vec![3, 4, 5, 6]);
    }
}
