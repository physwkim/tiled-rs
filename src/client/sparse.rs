//! `SparseClient` — sparse-array node client.
//!
//! Mirrors `tiled/client/sparse.py`. The Python client decodes COO data into
//! `scipy.sparse` / `sparse.COO`; we decode the same Arrow IPC COO table into
//! a [`SparseBlock`].

use std::io::Cursor;

use crate::core::structures::SparseStructure;
use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use url::Url;

use crate::client::base::{BaseClient, Item, ParsedStructure};
use crate::client::context::Context;
use crate::client::error::{ClientError, Result};
use crate::client::utils::{ARROW_FILE_MIME_TYPE, resolve_export_format, retry};

/// Decoded COO (coordinate-format) sparse array block.
///
/// Mirrors `sparse.COO` from Python's `sparse` library. `coords[i]` holds the
/// non-zero indices along dimension `i`; `data[j]` holds the corresponding
/// value. All `coords[i]` vecs have the same length as `data`.
#[derive(Debug, Clone, PartialEq)]
pub struct SparseBlock {
    /// Index vectors, one per dimension (`dim0`…`dim{ndim-1}`), all the same length.
    pub coords: Vec<Vec<i64>>,
    /// Non-zero values, parallel to each `coords[i]`.
    pub data: Vec<f64>,
    /// Dense shape of the block (from the node structure, not from Arrow data).
    pub shape: Vec<usize>,
}

impl SparseBlock {
    /// Densify this COO block into a row-major (C-order) `f64` buffer of length
    /// `shape.iter().product()`: every non-zero is scattered to its coordinate
    /// and all other entries stay `0.0`.
    ///
    /// Mirrors `SparseClient.todense` (`tiled/client/sparse.py:44`), which is
    /// `self.read().todense()` — i.e. `sparse.COO(coords, data, shape).todense()`.
    /// The caller owns any reshaping into an N-D array; the flat buffer here is
    /// the C-order layout numpy produces. Coordinates are assumed in-range for
    /// `shape` (the sparse-node contract the server upholds), matching
    /// `sparse.COO`, which validates coords against shape on construction.
    pub fn to_dense(&self) -> Vec<f64> {
        let size: usize = self.shape.iter().product();
        let mut out = vec![0.0f64; size];
        if size == 0 {
            return out;
        }
        // Row-major (C-order) strides for `shape`: the last axis is contiguous.
        let ndim = self.shape.len();
        let mut strides = vec![1usize; ndim];
        for axis in (0..ndim.saturating_sub(1)).rev() {
            strides[axis] = strides[axis + 1] * self.shape[axis + 1];
        }
        for (j, &value) in self.data.iter().enumerate() {
            let mut flat = 0usize;
            for (axis, stride) in strides.iter().enumerate() {
                flat += (self.coords[axis][j] as usize) * stride;
            }
            out[flat] = value;
        }
        out
    }
}

/// Client over a `sparse` node.
#[derive(Debug, Clone)]
pub struct SparseClient {
    base: BaseClient,
}

impl SparseClient {
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let base = BaseClient::new(context, item, include_data_sources)?;
        if !matches!(base.structure(), ParsedStructure::Sparse(_)) {
            return Err(ClientError::StructureMismatch {
                expected: "sparse".into(),
                got: base
                    .structure_family()
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "unknown".into()),
            });
        }
        Ok(Self { base })
    }

    pub fn base(&self) -> &BaseClient {
        &self.base
    }

    pub fn structure(&self) -> &SparseStructure {
        match self.base.structure() {
            ParsedStructure::Sparse(s) => s,
            _ => unreachable!("SparseClient guards on construction"),
        }
    }

    /// Read one block as a decoded COO sparse array.
    ///
    /// Requests `application/vnd.apache.arrow.file` and decodes the response
    /// table — columns `dim0`…`dim{ndim-1}` (integer indices) and `data`
    /// (float values) — into a [`SparseBlock`].
    ///
    /// Mirrors `client/sparse.py::read_block`. No throttle semaphore is held:
    /// Python throttles only array and dataframe fetches, not sparse.
    pub async fn read_block(&self, block: &[usize]) -> Result<SparseBlock> {
        let link = self.base.require_link("block")?;
        let mut url = Url::parse(link)?;
        let block_str: String = block
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        url.query_pairs_mut().append_pair("block", &block_str);

        let bytes = retry(|| async {
            self.base
                .context
                .get_bytes(&url, ARROW_FILE_MIME_TYPE)
                .await
        })
        .await?;

        let shape = self.structure().shape.clone();
        decode_coo_arrow(bytes, shape)
    }

    /// Read the **whole** sparse array as a decoded COO sparse array.
    ///
    /// Requests `links["full"]` (`GET /api/v1/array/full/{path}`) with
    /// `application/vnd.apache.arrow.file`; the server assembles every block
    /// into one global COO frame before encoding. Decodes the same
    /// `dim0`…`dim{ndim-1}` + `data` table as [`read_block`](Self::read_block).
    ///
    /// Mirrors `client/sparse.py::read`, the full-read sibling of `read_block`.
    /// Unlike `read_block` — which fetches a single chunk — this returns the
    /// non-zeros from across all blocks, so it is the only way to see blocks
    /// other than `[0, 0, …]` of a multi-block sparse array.
    pub async fn read(&self) -> Result<SparseBlock> {
        let link = self.base.require_link("full")?;
        let url = Url::parse(link)?;

        let bytes = retry(|| async {
            self.base
                .context
                .get_bytes(&url, ARROW_FILE_MIME_TYPE)
                .await
        })
        .await?;

        let shape = self.structure().shape.clone();
        decode_coo_arrow(bytes, shape)
    }

    /// Write the **whole** sparse array as one COO block.
    ///
    /// Builds the `dim0`…`dim{ndim-1}` + `data` table Python's
    /// `client/sparse.py::write` (client/sparse.py:107) serializes —
    /// `DataFrame({f"dim{i}": coords[i], "data": data})` → `serialize_arrow` —
    /// encodes it as Arrow IPC, and PUTs it to `links["full"]`
    /// (`PUT /api/v1/array/full`). `coords[i]` holds the non-zero indices along
    /// axis `i`; every `coords[i]` and `data` must be the same length.
    ///
    /// Like [`read`](Self::read)/[`read_block`](Self::read_block), no throttle
    /// semaphore is held: the sparse family does not participate in the data
    /// fetch throttle (Python throttles only array and dataframe transfers).
    pub async fn write(&self, coords: &[Vec<i64>], data: &[f64]) -> Result<()> {
        let ndim = self.structure().shape.len();
        if coords.len() != ndim {
            return Err(ClientError::Invalid(format!(
                "sparse write: got {} coordinate column(s) but the array is {ndim}-dimensional",
                coords.len()
            )));
        }
        let body = encode_coo_arrow(coords, data)?;
        let url = Url::parse(self.base.require_link("full")?)?;
        retry(|| async {
            self.base
                .context
                .put_bytes_typed(&url, body.clone(), ARROW_FILE_MIME_TYPE)
                .await
                .map(|_| ())
        })
        .await
    }

    /// Write one block's **block-local** COO. `block` is the per-axis chunk
    /// index; `coords[i]` are the indices within that chunk (the server shifts
    /// them by the chunk origin on read). Mirrors `client/sparse.py::write_block`
    /// — same DataFrame → Arrow IPC body — to `links["block"]`
    /// (`PUT /api/v1/array/block?block=…`).
    pub async fn write_block(
        &self,
        block: &[usize],
        coords: &[Vec<i64>],
        data: &[f64],
    ) -> Result<()> {
        let ndim = self.structure().shape.len();
        if coords.len() != ndim {
            return Err(ClientError::Invalid(format!(
                "sparse write_block: got {} coordinate column(s) but the array is \
                 {ndim}-dimensional",
                coords.len()
            )));
        }
        let body = encode_coo_arrow(coords, data)?;
        let mut url = Url::parse(self.base.require_link("block")?)?;
        let block_str = block
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        url.query_pairs_mut().append_pair("block", &block_str);
        retry(|| async {
            self.base
                .context
                .put_bytes_typed(&url, body.clone(), ARROW_FILE_MIME_TYPE)
                .await
                .map(|_| ())
        })
        .await
    }

    /// Export the whole sparse array to a file at `dest`, mirroring Python
    /// `SparseClient.export` (`client/sparse.py:146`): sends
    /// `GET /api/v1/array/full/{path}?format=<fmt>` and streams the response bytes
    /// to `dest`.
    ///
    /// `format` is resolved by the shared `resolve_export_format` helper:
    /// `Some(fmt)` is used as given with a single leading `.` stripped, while
    /// `None` infers the format from `dest`'s file extension. The sparse `full`
    /// route serializes the COO frame to the requested representation
    /// (`json`/`csv`/`arrow`/…); an unsupported format surfaces as a mapped server
    /// error (`406`). Unlike Python this does not accept a `slice` filter — the
    /// Rust client exports the whole array, the same shape as
    /// [`ArrayClient::export`](crate::client::array::ArrayClient::export).
    pub async fn export(&self, dest: &std::path::Path, format: Option<&str>) -> Result<()> {
        let resolved = resolve_export_format(dest, format)?;
        let link = self.base.require_link("full")?;
        let mut url = Url::parse(link)?;
        url.query_pairs_mut().append_pair("format", &resolved);
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes = retry(|| async { self.base.context.get_bytes(&url, "*/*").await }).await?;
        std::fs::write(dest, &bytes)
            .map_err(|e| ClientError::Invalid(format!("write {}: {e}", dest.display())))
    }
}

/// Encode a COO table — columns `dim0`…`dim{ndim-1}` (Int64) plus `data`
/// (Float64) — as an Arrow IPC file. The inverse of [`decode_coo_arrow`], and
/// the wire body the sparse write routes deserialize; matches Python
/// `client/sparse.py`'s `serialize_arrow(DataFrame({...}))`.
fn encode_coo_arrow(coords: &[Vec<i64>], data: &[f64]) -> Result<bytes::Bytes> {
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let mut fields: Vec<Field> = Vec::with_capacity(coords.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(coords.len() + 1);
    for (i, c) in coords.iter().enumerate() {
        fields.push(Field::new(format!("dim{i}"), DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(c.clone())) as ArrayRef);
    }
    fields.push(Field::new("data", DataType::Float64, false));
    columns.push(Arc::new(Float64Array::from(data.to_vec())) as ArrayRef);

    let schema = Arc::new(Schema::new(fields));
    // `try_new` enforces equal column lengths, surfacing a ragged
    // coords/data caller error as a clean client error.
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|e| ClientError::Invalid(format!("sparse write: {e}")))?;

    let mut buf = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buf, &schema)
            .map_err(|e| ClientError::Invalid(format!("sparse write: Arrow IPC init: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| ClientError::Invalid(format!("sparse write: Arrow IPC write: {e}")))?;
        writer
            .finish()
            .map_err(|e| ClientError::Invalid(format!("sparse write: Arrow IPC finish: {e}")))?;
    }
    Ok(bytes::Bytes::from(buf))
}

/// Decode an Arrow IPC file containing a COO sparse table.
///
/// The table must have columns `dim0`…`dim{ndim-1}` (any integer type,
/// yielded as `i64`) and `data` (any float type, yielded as `f64`), where
/// `ndim = shape.len()`. Matches the format written by Python's
/// `client/sparse.py::write_block` and the server's sparse serializer.
pub fn decode_coo_arrow(bytes: bytes::Bytes, shape: Vec<usize>) -> Result<SparseBlock> {
    let ndim = shape.len();
    let cursor = Cursor::new(bytes.to_vec());
    let reader = FileReader::try_new(cursor, None)?;

    let mut coords: Vec<Vec<i64>> = (0..ndim).map(|_| Vec::new()).collect();
    let mut data: Vec<f64> = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        for (i, coord_buf) in coords.iter_mut().enumerate() {
            let col_name = format!("dim{i}");
            let col = batch
                .column_by_name(&col_name)
                .ok_or_else(|| ClientError::Invalid(format!("missing column '{col_name}'")))?;
            coord_buf.extend(col_to_i64(col.as_ref(), &col_name)?);
        }
        let data_col = batch
            .column_by_name("data")
            .ok_or_else(|| ClientError::Invalid("missing column 'data'".into()))?;
        data.extend(col_to_f64(data_col.as_ref())?);
    }

    Ok(SparseBlock {
        coords,
        data,
        shape,
    })
}

fn col_to_i64(col: &dyn Array, col_name: &str) -> Result<Vec<i64>> {
    match col.data_type() {
        DataType::Int64 => Ok(col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()),
        DataType::UInt64 => Ok(col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as i64)
            .collect()),
        DataType::Int32 => Ok(col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as i64)
            .collect()),
        DataType::UInt32 => Ok(col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as i64)
            .collect()),
        other => Err(ClientError::Invalid(format!(
            "column '{col_name}' has unsupported type {other:?} for COO coordinates"
        ))),
    }
}

fn col_to_f64(col: &dyn Array) -> Result<Vec<f64>> {
    match col.data_type() {
        DataType::Float64 => Ok(col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec()),
        DataType::Float32 => Ok(col
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as f64)
            .collect()),
        // Integer `data` columns: a sparse adapter storing integer non-zeros
        // (e.g. a count matrix) preserves its native dtype through the server's
        // COO serializer (`dyn_ndarray_to_arrow`). Python passes the column
        // straight to `sparse.COO`, which promotes integers to float; mirror
        // that by casting to f64 (matches how `col_to_i64` accepts every
        // integer width for the coordinate columns).
        DataType::Int64 => Ok(col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as f64)
            .collect()),
        DataType::UInt64 => Ok(col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as f64)
            .collect()),
        DataType::Int32 => Ok(col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as f64)
            .collect()),
        DataType::UInt32 => Ok(col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&v| v as f64)
            .collect()),
        other => Err(ClientError::Invalid(format!(
            "column 'data' has unsupported type {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn make_coo_arrow(ndim: usize, dim_cols: &[Vec<i64>], data_vals: &[f64]) -> bytes::Bytes {
        let mut fields: Vec<Field> = (0..ndim)
            .map(|i| Field::new(format!("dim{i}"), DataType::Int64, false))
            .collect();
        fields.push(Field::new("data", DataType::Float64, false));
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<Arc<dyn Array>> = dim_cols
            .iter()
            .map(|v| Arc::new(Int64Array::from(v.clone())) as Arc<dyn Array>)
            .collect();
        columns.push(Arc::new(Float64Array::from(data_vals.to_vec())) as Arc<dyn Array>);

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut buf = Vec::new();
        let mut writer = FileWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        bytes::Bytes::from(buf)
    }

    #[test]
    fn decode_2d_coo() {
        let dim0 = vec![1i64, 0];
        let dim1 = vec![2i64, 3];
        let data = vec![5.0f64, 9.0];
        let bytes = make_coo_arrow(2, &[dim0.clone(), dim1.clone()], &data);
        let shape = vec![3usize, 4];
        let block = decode_coo_arrow(bytes, shape.clone()).unwrap();
        assert_eq!(block.shape, shape);
        assert_eq!(block.coords[0], dim0);
        assert_eq!(block.coords[1], dim1);
        assert_eq!(block.data, data);
    }

    #[test]
    fn decode_1d_coo() {
        let dim0 = vec![0i64, 4, 7];
        let data = vec![1.0f64, 2.0, 3.0];
        let bytes = make_coo_arrow(1, std::slice::from_ref(&dim0), &data);
        let shape = vec![10usize];
        let block = decode_coo_arrow(bytes, shape.clone()).unwrap();
        assert_eq!(block.shape, shape);
        assert_eq!(block.coords[0], dim0);
        assert_eq!(block.data, data);
    }

    /// client-M3: an integer `data` column (Int64/UInt64/Int32/UInt32) must
    /// decode (cast to f64), not error — Python passes integer sparse data
    /// straight to `sparse.COO`. Each integer width yields the same f64 values.
    #[test]
    fn col_to_f64_accepts_integer_data_columns() {
        assert_eq!(
            col_to_f64(&Int64Array::from(vec![1i64, 2, 3])).unwrap(),
            vec![1.0, 2.0, 3.0]
        );
        assert_eq!(
            col_to_f64(&UInt64Array::from(vec![4u64, 5])).unwrap(),
            vec![4.0, 5.0]
        );
        assert_eq!(
            col_to_f64(&Int32Array::from(vec![-6i32, 7])).unwrap(),
            vec![-6.0, 7.0]
        );
        assert_eq!(
            col_to_f64(&UInt32Array::from(vec![8u32, 9])).unwrap(),
            vec![8.0, 9.0]
        );
    }

    #[test]
    fn decode_empty_coo() {
        let bytes = make_coo_arrow(2, &[vec![], vec![]], &[]);
        let shape = vec![5usize, 5];
        let block = decode_coo_arrow(bytes, shape.clone()).unwrap();
        assert_eq!(block.shape, shape);
        assert_eq!(block.coords.len(), 2);
        assert!(block.coords[0].is_empty());
        assert!(block.coords[1].is_empty());
        assert!(block.data.is_empty());
    }

    /// `encode_coo_arrow` is the inverse of `decode_coo_arrow`: an encoded COO
    /// frame decodes back to the same coords and data.
    #[test]
    fn encode_then_decode_coo_roundtrips() {
        let coords = vec![vec![0i64, 2], vec![1i64, 0]];
        let data = vec![1.5f64, 3.7];
        let body = encode_coo_arrow(&coords, &data).unwrap();
        let block = decode_coo_arrow(body, vec![3, 3]).unwrap();
        assert_eq!(block.coords, coords);
        assert_eq!(block.data, data);
        assert_eq!(block.shape, vec![3, 3]);
    }

    /// Ragged coords/data lengths surface as a client-side `Invalid` error from
    /// the Arrow builder rather than an opaque panic.
    #[test]
    fn encode_rejects_length_mismatch() {
        // dim0 has 2 entries but data has 1.
        let err = encode_coo_arrow(&[vec![0i64, 1]], &[5.0]);
        assert!(matches!(err, Err(ClientError::Invalid(_))), "got {err:?}");
    }

    /// `to_dense` scatters each non-zero into a row-major buffer; a 2-D COO
    /// densifies to the same C-order layout numpy's `sparse.COO.todense()`
    /// yields. `(1,2)=5` sits at flat index `1*4 + 2 = 6`; `(0,3)=9` at `3`.
    #[test]
    fn to_dense_2d() {
        let block = SparseBlock {
            coords: vec![vec![1i64, 0], vec![2i64, 3]],
            data: vec![5.0f64, 9.0],
            shape: vec![3, 4],
        };
        let mut expected = vec![0.0f64; 12];
        expected[6] = 5.0;
        expected[3] = 9.0;
        assert_eq!(block.to_dense(), expected);
    }

    /// A 1-D COO densifies by placing each value at its coordinate.
    #[test]
    fn to_dense_1d() {
        let block = SparseBlock {
            coords: vec![vec![0i64, 4, 7]],
            data: vec![1.0f64, 2.0, 3.0],
            shape: vec![10],
        };
        assert_eq!(
            block.to_dense(),
            vec![1.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 3.0, 0.0, 0.0]
        );
    }

    /// No non-zeros → an all-zero dense buffer of the full size.
    #[test]
    fn to_dense_empty_is_all_zeros() {
        let block = SparseBlock {
            coords: vec![vec![], vec![]],
            data: vec![],
            shape: vec![2, 3],
        };
        assert_eq!(block.to_dense(), vec![0.0f64; 6]);
    }

    /// A zero-sized dimension yields an empty dense buffer without indexing.
    #[test]
    fn to_dense_zero_dim() {
        let block = SparseBlock {
            coords: vec![vec![], vec![]],
            data: vec![],
            shape: vec![0, 4],
        };
        assert!(block.to_dense().is_empty());
    }
}
