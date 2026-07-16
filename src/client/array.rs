//! `ArrayClient` — read and write array data via `/api/v1/array/`.
//!
//! Mirrors `tiled/client/array.py::ArrayClient`. The Python client juggles
//! dask + numpy + slicing; here we expose the wire-level moves: `read_block`
//! to fetch one chunk, `read` to concatenate everything as raw bytes, `write`
//! to overwrite the whole array, `write_block` to overwrite one chunk, and
//! `patch` to write a data block into a slice (optionally extending the shape).
//! Higher-level reshaping is left to the caller.

use crate::core::ndslice::NDSlice;
use crate::core::structures::ArrayStructure;
use url::Url;

use crate::client::base::{BaseClient, Item, ParsedStructure};
use crate::client::context::Context;
use crate::client::error::{ClientError, Result};
use crate::client::utils::{OCTET_STREAM_MIME_TYPE, decode_response, retry};

/// A single block of array bytes plus the dtype/shape needed to interpret it.
#[derive(Debug, Clone)]
pub struct ArrayBlock {
    pub data: bytes::Bytes,
    pub shape: Vec<usize>,
    pub dtype: crate::core::dtype::DType,
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

    /// Fetch an arbitrary N-D slice of the array in a single request via
    /// `GET /api/v1/array/full?slice=...`. Mirrors Python
    /// `_DaskArrayClient._get_slice` (`array.py:147-193`) minus the dask
    /// scheduling — the caller gets the raw bytes plus the shape needed to
    /// interpret them.
    ///
    /// Unlike [`read_block`](Self::read_block) (one chunk by chunk-index) and
    /// [`read`](Self::read) (every chunk, concatenated), this lets a caller
    /// fetch an arbitrary sub-region — e.g. `NDSlice::from_numpy_str("2:5")` —
    /// that may span multiple chunks, in one round trip. Pass
    /// [`NDSlice::empty`] to read the whole array as a single request.
    pub async fn read_slice(&self, slice: &NDSlice) -> Result<ArrayBlock> {
        let exp_shape = slice
            .shape_after_slice(self.shape())
            .map_err(|e| ClientError::Invalid(format!("invalid slice: {e}")))?;

        // A zero in the resulting shape means the slice selects no data;
        // short-circuit rather than issuing a request the server would
        // answer with a zero-length body anyway.
        if exp_shape.contains(&0) {
            return Ok(ArrayBlock {
                data: bytes::Bytes::new(),
                shape: exp_shape,
                dtype: self.structure().data_type.clone(),
            });
        }

        let link = self.base.require_link("full")?;
        let mut url = Url::parse(link)?;
        {
            let mut q = url.query_pairs_mut();
            let exp_shape_str = if exp_shape.is_empty() {
                "scalar".to_string()
            } else {
                exp_shape
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            q.append_pair("expected_shape", &exp_shape_str);
            if !slice.is_empty() {
                q.append_pair("slice", &slice.to_numpy_str());
            }
        }

        // Cap concurrent bulk-data fetches across the whole context, mirroring
        // Python's `with self.context.throttle()` around `_get_slice`
        // (`array.py:181`).
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes = retry(|| async {
            self.base
                .context
                .get_bytes(&url, OCTET_STREAM_MIME_TYPE)
                .await
        })
        .await?;

        Ok(ArrayBlock {
            data: bytes,
            shape: exp_shape,
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

    /// Overwrite the whole array. `data` is the C-order element buffer —
    /// `nelem * dtype.element_size()` bytes, matching `PUT /api/v1/array/full`.
    pub async fn write(&self, data: bytes::Bytes) -> Result<()> {
        let link = self.base.require_link("full")?;
        let url = Url::parse(link)?;
        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .put_bytes(&url, data.clone())
                .await
                .map(|_| ())
        })
        .await
    }

    /// Overwrite one chunk. `block` is the per-axis chunk index; `data` is
    /// the C-order buffer for that chunk — matches `PUT /api/v1/array/block`.
    pub async fn write_block(&self, block: &[usize], data: bytes::Bytes) -> Result<()> {
        let link = self.base.require_link("block")?;
        let mut url = Url::parse(link)?;
        let block_str = block
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        url.query_pairs_mut().append_pair("block", &block_str);
        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .put_bytes(&url, data.clone())
                .await
                .map(|_| ())
        })
        .await
    }

    /// Write a `data` block into a slice of the array at `offset`, optionally
    /// extending the shape when the slice overflows. `data` is the C-order
    /// element buffer of the block whose dimensions are `shape`. Mirrors Python
    /// `ArrayClient.patch` (client/array.py:341-444):
    /// `PATCH /api/v1/array/full?offset=<csv>&shape=<csv>&extend=<bool>[&persist=false]`,
    /// body = `array.tobytes()`.
    ///
    /// Returns the updated [`ArrayStructure`] the server reports. The Python
    /// client mutates its cached `self._structure`; here `&self` is immutable, so
    /// the caller refreshes from the returned structure (mirrors
    /// [`RaggedClient::patch`](crate::client::ragged::RaggedClient::patch)).
    ///
    /// `persist = false` streams the update to subscribers without writing to
    /// storage and returns the unchanged structure; the server rejects
    /// `extend = true` with `persist = false` (400). A slice that overflows the
    /// shape without `extend` returns 409, surfaced as a [`ClientError`].
    pub async fn patch(
        &self,
        data: bytes::Bytes,
        shape: &[usize],
        offset: &[usize],
        extend: bool,
        persist: bool,
    ) -> Result<ArrayStructure> {
        let csv = |v: &[usize]| {
            v.iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(",")
        };
        let mut url = Url::parse(self.base.require_link("full")?)?;
        url.query_pairs_mut()
            .append_pair("offset", &csv(offset))
            .append_pair("shape", &csv(shape))
            .append_pair("extend", if extend { "true" } else { "false" });
        // Python adds `persist` only for the non-default `false` (array.py:417).
        if !persist {
            url.query_pairs_mut().append_pair("persist", "false");
        }
        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            let resp = self
                .base
                .context
                .patch_bytes_typed(&url, data.clone(), OCTET_STREAM_MIME_TYPE)
                .await?;
            decode_response::<ArrayStructure>(resp).await
        })
        .await
    }

    /// Export the whole array to a file at `dest` in the requested `format`
    /// (e.g. `"npy"`, `"csv"`, `"json"`). Mirrors Python `ArrayClient.export`:
    /// sends `GET /api/v1/array/full/{path}?format=<ext>` and streams the
    /// response bytes to `dest`. The server resolves `format` as an alias or
    /// media-type; passing an unsupported format returns a server error.
    pub async fn export(&self, dest: &std::path::Path, format: &str) -> Result<()> {
        let link = self.base.require_link("full")?;
        let mut url = Url::parse(link)?;
        url.query_pairs_mut().append_pair("format", format);
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes = retry(|| async { self.base.context.get_bytes(&url, "*/*").await }).await?;
        std::fs::write(dest, &bytes)
            .map_err(|e| ClientError::Invalid(format!("write {}: {e}", dest.display())))
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
