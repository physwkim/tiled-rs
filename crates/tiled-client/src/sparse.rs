//! `SparseClient` — sparse-array node client.
//!
//! Mirrors `tiled/client/sparse.py`. The Python client decodes COO data into
//! `scipy.sparse` / `sparse.COO`; we expose the structure and the raw block
//! bytes and let the caller assemble.

use tiled_core::structures::SparseStructure;
use url::Url;

use crate::base::{BaseClient, Item, ParsedStructure};
use crate::context::Context;
use crate::error::{ClientError, Result};
use crate::utils::{OCTET_STREAM_MIME_TYPE, retry};

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

    /// Raw bytes for one block; layout interpretation is up to the caller.
    ///
    /// DIVERGENCE FROM PYTHON (documented, intentionally left as-is): Python's
    /// `client/sparse.py::read_block` requests `APACHE_ARROW_FILE_MIME_TYPE` and
    /// `deserialize_arrow`s a COO table (columns `dim0..dimN`, `data`); we
    /// request `application/octet-stream` and hand back the raw bytes. This is a
    /// known parity gap, NOT fixed here because the Rust server has no sparse
    /// data path to validate a change against: `links_for_node`
    /// (tiled-core/src/links.rs) points a sparse node's `block` link at
    /// `/api/v1/array/block`, whose handler (`router::array_block`) calls
    /// `as_array_arc()` — which is `None` for `AnyAdapter::Sparse`
    /// (tiled-core/src/adapters.rs) — so the request 422s ("is not an array")
    /// before `build_array_response` ever serializes. No `SparseAdapterRead`
    /// impl is constructed anywhere in the workspace. Switching the `Accept`
    /// header to Arrow would change the bytes returned by this `raw block bytes`
    /// contract without a Rust server that emits Arrow COO to verify against;
    /// the full fix (request Arrow + decode COO, mirroring `dataframe.rs`) is
    /// deferred until the server grows a real sparse serialization path.
    pub async fn read_block(&self, block: &[usize]) -> Result<bytes::Bytes> {
        let link = self.base.require_link("block")?;
        let mut url = Url::parse(link)?;
        let block_str: String = block
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        url.query_pairs_mut().append_pair("block", &block_str);

        retry(|| async {
            self.base
                .context
                .get_bytes(&url, OCTET_STREAM_MIME_TYPE)
                .await
        })
        .await
    }
}
