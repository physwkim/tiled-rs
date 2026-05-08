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
