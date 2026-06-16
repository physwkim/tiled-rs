//! `ArrayClient` — read array data from `/api/v1/array/block/<path>`.
//!
//! Mirrors `tiled/client/array.py::ArrayClient`. The Python client juggles
//! dask + numpy + slicing; here we expose the wire-level moves: `read_block`
//! to fetch one chunk, and `read` to concatenate everything as raw bytes.
//! Higher-level numpy/ndarray reshaping is left to the caller — same shape
//! the Python `_get_block` returns before NumPy assembly.

use tiled_core::structures::ArrayStructure;
use url::Url;

use crate::base::{BaseClient, Item, ParsedStructure};
use crate::context::Context;
use crate::error::{ClientError, Result};
use crate::utils::{OCTET_STREAM_MIME_TYPE, retry};

/// A single block of array bytes plus the dtype/shape needed to interpret it.
#[derive(Debug, Clone)]
pub struct ArrayBlock {
    pub data: bytes::Bytes,
    pub shape: Vec<usize>,
    pub dtype: tiled_core::dtype::DType,
}

/// Client over an `array` node.
#[derive(Debug, Clone)]
pub struct ArrayClient {
    base: BaseClient,
}

impl ArrayClient {
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let base = BaseClient::new(context, item, include_data_sources)?;
        if !matches!(base.structure(), ParsedStructure::Array(_)) {
            return Err(ClientError::StructureMismatch {
                expected: "array".into(),
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

    pub fn structure(&self) -> &ArrayStructure {
        match self.base.structure() {
            ParsedStructure::Array(s) => s,
            _ => unreachable!("ArrayClient guards on construction"),
        }
    }

    /// Overall shape `[d0, d1, ...]`.
    pub fn shape(&self) -> &[usize] {
        &self.structure().shape
    }

    /// Total element count.
    pub fn size(&self) -> usize {
        self.shape().iter().product()
    }

    /// Per-dim chunk layout (e.g. `[[100, 100], [50, 50, 50]]`).
    pub fn chunks(&self) -> &[Vec<usize>] {
        &self.structure().chunks
    }

    /// Number of dimensions.
    pub fn ndim(&self) -> usize {
        self.shape().len()
    }

    /// Fetch one block. `block` is a per-dim chunk index, e.g. `[0, 1]` for
    /// the second column of a 2-D array partitioned `(N, M)`.
    pub async fn read_block(&self, block: &[usize]) -> Result<ArrayBlock> {
        let link = self.base.require_link("block")?;
        let mut url = Url::parse(link)?;
        let block_str: String = block
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        url.query_pairs_mut().append_pair("block", &block_str);

        // Cap concurrent bulk-data fetches across the whole context, mirroring
        // Python's `with self.context.throttle()` around `_get_block`
        // (`array.py:133`). Held across retries, released on drop.
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes = retry(|| async {
            self.base
                .context
                .get_bytes(&url, OCTET_STREAM_MIME_TYPE)
                .await
        })
        .await?;

        let shape = self.block_shape(block)?;
        Ok(ArrayBlock {
            data: bytes,
            shape,
            dtype: self.structure().data_type.clone(),
        })
    }

    /// Read every block, concatenated in the natural row-major chunk order.
    /// The caller owns reshaping into an ndarray; we just stream the bytes.
    pub async fn read(&self) -> Result<Vec<ArrayBlock>> {
        let chunks = self.chunks().to_vec();
        let mut block = vec![0usize; chunks.len()];
        let mut out = Vec::new();
        loop {
            out.push(self.read_block(&block).await?);
            // Increment the multi-index, row-major (last axis first).
            let mut axis = chunks.len();
            loop {
                if axis == 0 {
                    return Ok(out);
                }
                axis -= 1;
                block[axis] += 1;
                if block[axis] < chunks[axis].len() {
                    break;
                }
                block[axis] = 0;
            }
        }
    }

    fn block_shape(&self, block: &[usize]) -> Result<Vec<usize>> {
        let chunks = self.chunks();
        if block.len() != chunks.len() {
            return Err(ClientError::Invalid(format!(
                "block index has {} dims, structure has {}",
                block.len(),
                chunks.len()
            )));
        }
        let mut shape = Vec::with_capacity(block.len());
        for (axis, &b) in block.iter().enumerate() {
            shape.push(*chunks[axis].get(b).ok_or_else(|| {
                ClientError::Invalid(format!(
                    "block index {b} out of range on axis {axis} (have {} chunks)",
                    chunks[axis].len()
                ))
            })?);
        }
        Ok(shape)
    }
}
