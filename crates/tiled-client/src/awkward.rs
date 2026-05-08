//! `AwkwardClient` — awkward-array node client.
//!
//! Mirrors `tiled/client/awkward.py`. The Python client builds an Awkward
//! Array; we expose the structure (form + length) and `read_buffers` to
//! fetch the raw binary buffers — the caller is responsible for stitching
//! them back into an Awkward array via `ak.from_buffers` (or equivalent).

use tiled_core::structures::AwkwardStructure;
use url::Url;

use crate::base::{BaseClient, Item, ParsedStructure};
use crate::context::Context;
use crate::error::{ClientError, Result};
use crate::utils::{OCTET_STREAM_MIME_TYPE, retry};

/// Buffers fetched via `links.buffers`.
///
/// The Tiled server packages awkward buffers as a ZIP archive in which each
/// entry is a named buffer (the same `node{N}-data` / `node{N}-offsets`
/// layout that `awkward.to_buffers` produces). [`AwkwardBuffers::buffers`]
/// holds the named extraction (which is what `awkward.from_buffers`
/// expects); [`AwkwardBuffers::raw_zip`] keeps the original archive for
/// callers that want to re-stream it.
#[derive(Debug, Clone)]
pub struct AwkwardBuffers {
    pub form: serde_json::Value,
    pub length: u64,
    /// Named buffers, keyed by entry name within the zip archive.
    pub buffers: std::collections::HashMap<String, bytes::Bytes>,
    /// Original zip-archive bytes (for re-streaming).
    pub raw_zip: bytes::Bytes,
}

#[derive(Debug, Clone)]
pub struct AwkwardClient {
    base: BaseClient,
}

impl AwkwardClient {
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let base = BaseClient::new(context, item, include_data_sources)?;
        if !matches!(base.structure(), ParsedStructure::Awkward(_)) {
            return Err(ClientError::StructureMismatch {
                expected: "awkward".into(),
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

    pub fn structure(&self) -> &AwkwardStructure {
        match self.base.structure() {
            ParsedStructure::Awkward(s) => s,
            _ => unreachable!("AwkwardClient guards on construction"),
        }
    }

    /// Fetch the raw zipped buffers blob for the array. Most callers want
    /// [`AwkwardClient::read`] which also un-zips into a name→bytes map.
    pub async fn read_buffers(&self) -> Result<bytes::Bytes> {
        let link = self.base.require_link("buffers")?;
        let url = Url::parse(link)?;
        retry(|| async {
            self.base
                .context
                .get_bytes(&url, "application/zip")
                .await
        })
        .await
    }

    /// Fetch buffers + form/length and unpack the zip into a named map ready
    /// for `awkward.from_buffers` (or equivalent).
    pub async fn read(&self) -> Result<AwkwardBuffers> {
        let zipped = self.read_buffers().await?;
        let s = self.structure();
        let buffers = unzip_named_buffers(&zipped)?;
        Ok(AwkwardBuffers {
            form: s.form.clone(),
            length: s.length as u64,
            buffers,
            raw_zip: zipped,
        })
    }
}

fn unzip_named_buffers(
    zipped: &bytes::Bytes,
) -> Result<std::collections::HashMap<String, bytes::Bytes>> {
    use std::io::{Cursor, Read};
    let mut out = std::collections::HashMap::new();
    let cursor = Cursor::new(zipped.to_vec());
    let mut zip = zip::ZipArchive::new(cursor)
        .map_err(|e| ClientError::Invalid(format!("awkward zip open: {e}")))?;
    for i in 0..zip.len() {
        let mut entry = zip
            .by_index(i)
            .map_err(|e| ClientError::Invalid(format!("awkward zip entry {i}: {e}")))?;
        let name = entry.name().to_string();
        let mut buf = Vec::with_capacity(entry.size() as usize);
        entry
            .read_to_end(&mut buf)
            .map_err(|e| ClientError::Invalid(format!("awkward zip read {name}: {e}")))?;
        out.insert(name, bytes::Bytes::from(buf));
    }
    Ok(out)
}

#[allow(unused_imports)]
use OCTET_STREAM_MIME_TYPE as _OCTET_STREAM_KEEP;
