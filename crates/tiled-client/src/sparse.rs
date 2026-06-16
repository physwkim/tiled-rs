//! `SparseClient` — sparse-array node client.
//!
//! Mirrors `tiled/client/sparse.py`. The Python client decodes COO data into
//! `scipy.sparse` / `sparse.COO`; we decode the same Arrow IPC COO table into
//! a [`SparseBlock`].

use std::io::Cursor;

use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use tiled_core::structures::SparseStructure;
use url::Url;

use crate::base::{BaseClient, Item, ParsedStructure};
use crate::context::Context;
use crate::error::{ClientError, Result};
use crate::utils::{ARROW_FILE_MIME_TYPE, retry};

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
        other => Err(ClientError::Invalid(format!(
            "column 'data' has unsupported type {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int64Array};
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
}
