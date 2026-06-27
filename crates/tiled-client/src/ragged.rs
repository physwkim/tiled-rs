//! `RaggedClient` — ragged-array node client.
//!
//! Mirrors `tiled/client/ragged.py::RaggedClient`. Read is served by
//! [`crate::any_client`]'s family dispatch; the write moves here —
//! [`write`](RaggedClient::write), [`write_block`](RaggedClient::write_block),
//! and [`patch`](RaggedClient::patch) — send the Awkward zipped-buffers body
//! (`application/zip`) the Python client sends, so the wire is identical against
//! both the Rust and Python servers.

use tiled_core::structures::RaggedStructure;
use url::Url;

use crate::base::{BaseClient, Item, ParsedStructure};
use crate::context::Context;
use crate::error::{ClientError, Result};
use crate::utils::{decode_response, retry};

/// The ragged write content-type — the Awkward zipped-buffers form
/// (`tiled/serialization/ragged.py:90-111`).
const RAGGED_ZIP_MIME: &str = "application/zip";

/// The ragged read Accept type — the JSON list-of-lists (the server's default
/// ragged media type).
const RAGGED_JSON_MIME: &str = "application/json";

#[derive(Debug, Clone)]
pub struct RaggedClient {
    base: BaseClient,
}

impl RaggedClient {
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let base = BaseClient::new(context, item, include_data_sources)?;
        if !matches!(base.structure(), ParsedStructure::Ragged(_)) {
            return Err(ClientError::StructureMismatch {
                expected: "ragged".into(),
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

    pub fn structure(&self) -> &RaggedStructure {
        match self.base.structure() {
            ParsedStructure::Ragged(s) => s,
            _ => unreachable!("RaggedClient guards on construction"),
        }
    }

    /// Read the whole ragged array as a JSON list-of-lists. Requests
    /// `links["full"]` (`GET /ragged/full`) with `Accept: application/json`, the
    /// server's default ragged media type. The read counterpart of
    /// [`write`](RaggedClient::write); higher-level conversion to a typed array
    /// is left to the caller.
    pub async fn read(&self) -> Result<serde_json::Value> {
        let url = Url::parse(self.base.require_link("full")?)?;
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes =
            retry(|| async { self.base.context.get_bytes(&url, RAGGED_JSON_MIME).await }).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| ClientError::Invalid(format!("ragged read: response is not JSON: {e}")))
    }

    /// Encode a JSON list-of-lists into the `application/zip` write body using
    /// this node's structure (the form is fixed by the structure, matching the
    /// adapter).
    fn zip_body(&self, data: &serde_json::Value) -> Result<bytes::Bytes> {
        tiled_serialization::ragged::to_zipped_buffers_from_json(self.structure(), data)
            .map_err(|e| ClientError::Invalid(format!("ragged write: zip encode failed: {e}")))
    }

    /// Write the whole ragged array (as chunk 0). `data` is a JSON
    /// list-of-lists. Mirrors `RaggedClient.write` (`PUT /ragged/full`).
    pub async fn write(&self, data: &serde_json::Value, persist: bool) -> Result<()> {
        let body = self.zip_body(data)?;
        let mut url = Url::parse(self.base.require_link("full")?)?;
        if !persist {
            url.query_pairs_mut().append_pair("persist", "false");
        }
        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .put_bytes_typed(&url, body.clone(), RAGGED_ZIP_MIME)
                .await
                .map(|_| ())
        })
        .await
    }

    /// Write one chunk. `block` is the per-fixed-dimension chunk index (only the
    /// leftmost entry, the chunk index, is meaningful for ragged). Mirrors
    /// `RaggedClient.write_block` (`PUT /ragged/block?block=…`).
    pub async fn write_block(
        &self,
        data: &serde_json::Value,
        block: &[usize],
        persist: bool,
    ) -> Result<()> {
        let body = self.zip_body(data)?;
        let mut url = Url::parse(self.base.require_link("block")?)?;
        let block_str = block
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        url.query_pairs_mut().append_pair("block", &block_str);
        if !persist {
            url.query_pairs_mut().append_pair("persist", "false");
        }
        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .put_bytes_typed(&url, body.clone(), RAGGED_ZIP_MIME)
                .await
                .map(|_| ())
        })
        .await
    }

    /// Append a chunk along the leftmost dimension, growing the array, and
    /// return the resulting structure. `data` is a JSON list-of-lists; `offset`
    /// must place the data at the current end of the leftmost dimension. Mirrors
    /// `RaggedClient.patch` (`PATCH /ragged/full?shape=…&offset=…&extend=…`).
    pub async fn patch(
        &self,
        data: &serde_json::Value,
        offset: &[usize],
        extend: bool,
        persist: bool,
    ) -> Result<RaggedStructure> {
        let body = self.zip_body(data)?;
        // `shape` is the row count of the appended chunk (Python sends
        // `array.shape[0]`); the server uses it for streaming metadata only.
        let shape0 = data.as_array().map(Vec::len).ok_or_else(|| {
            ClientError::Invalid("ragged patch: data must be a JSON list-of-lists".into())
        })?;
        let offset_str = offset
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut url = Url::parse(self.base.require_link("full")?)?;
        url.query_pairs_mut()
            .append_pair("shape", &shape0.to_string())
            .append_pair("offset", &offset_str)
            .append_pair("extend", if extend { "true" } else { "false" });
        if !persist {
            url.query_pairs_mut().append_pair("persist", "false");
        }
        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            let resp = self
                .base
                .context
                .patch_bytes_typed(&url, body.clone(), RAGGED_ZIP_MIME)
                .await?;
            let v = decode_response::<serde_json::Value>(resp).await?;
            RaggedStructure::from_json(&v).map_err(|e| {
                ClientError::Invalid(format!("ragged patch: bad structure response: {e}"))
            })
        })
        .await
    }
}
