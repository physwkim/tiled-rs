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
    ///
    /// Mirrors Python `AwkwardClient.read` (tiled/client/awkward.py:53-72):
    /// the server's `/awkward/buffers` endpoint filters the returned buffers
    /// to those whose name starts with one of the requested form keys
    /// (tiled/adapters/awkward.py:79-87), so the request must carry the set of
    /// form keys. Python derives the *touched* keys from an awkward typetracer
    /// over the requested slice; this client reads the whole array (no slice
    /// projection), which is the typetracer's full set, so it sends every form
    /// key declared in the structure form. The keys travel in a JSON-array
    /// **POST** body (not `?form_key=...` GET params): Python switched to POST
    /// to avoid URL-length limits on arrays with many form keys
    /// (router.py:1611-1639), and the GET route is unsuitable here because the
    /// full key set can be large.
    pub async fn read_buffers(&self) -> Result<bytes::Bytes> {
        let link = self.base.require_link("buffers")?;
        let url = Url::parse(link)?;
        let form_keys = collect_form_keys(&self.structure().form);
        let body = serde_json::Value::Array(
            form_keys
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
        );
        retry(|| async {
            self.base
                .context
                .post_bytes(&url, "application/zip", &body)
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

    /// Write the whole awkward array by replacing its buffer map. `buffers`
    /// maps each form key to its raw buffer bytes (the same `node{N}-data` /
    /// `node{N}-offsets` layout `awkward.to_buffers` produces). The map is
    /// packed into an uncompressed (`ZIP_STORED`) archive and sent to
    /// `PUT /api/v1/awkward/full` with `Content-Type: application/zip`.
    ///
    /// Mirrors Python `AwkwardClient.write` (awkward.py:38-51): the form and
    /// length already live in the node's structure, so only the buffer map
    /// travels on the wire — `to_zipped_buffers` writes only `container.items()`
    /// (serialization/awkward.py:22-24), which the server's
    /// `unpack_zip_to_buffers` reads back verbatim.
    pub async fn write(
        &self,
        buffers: std::collections::HashMap<String, bytes::Bytes>,
    ) -> Result<()> {
        let body = pack_named_buffers(&buffers)?;
        let url = Url::parse(self.base.require_link("full")?)?;
        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .put_bytes_typed(&url, body.clone(), "application/zip")
                .await
                .map(|_| ())
        })
        .await
    }
}

/// Pack a form-key→bytes buffer map into an uncompressed (`ZIP_STORED`) ZIP
/// archive — the inverse of [`unzip_named_buffers`]. Matches the server's
/// `pack_buffers_to_zip` and Python's `to_zipped_buffers`
/// (serialization/awkward.py:14-25), which stores each buffer uncompressed so
/// the archive keeps random access and Tiled's own compression layer applies
/// over the whole payload.
fn pack_named_buffers(
    buffers: &std::collections::HashMap<String, bytes::Bytes>,
) -> Result<bytes::Bytes> {
    use std::io::{Cursor, Write};
    use zip::write::SimpleFileOptions;

    let cursor = Cursor::new(Vec::<u8>::new());
    let mut zip = zip::ZipWriter::new(cursor);
    let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, data) in buffers {
        zip.start_file(name, opts)
            .map_err(|e| ClientError::Invalid(format!("awkward zip start_file {name}: {e}")))?;
        zip.write_all(data)
            .map_err(|e| ClientError::Invalid(format!("awkward zip write {name}: {e}")))?;
    }
    let cursor = zip
        .finish()
        .map_err(|e| ClientError::Invalid(format!("awkward zip finish: {e}")))?;
    Ok(bytes::Bytes::from(cursor.into_inner()))
}

/// Collect every `form_key` declared in an awkward form, sorted and
/// deduplicated. An awkward form is a JSON tree of nested nodes (records,
/// lists, numpy leaves); each node that backs a buffer carries a
/// `"form_key"` string. We walk the whole tree generically rather than
/// modelling each node type, so any node carrying a `form_key` is collected
/// regardless of the surrounding shape. The `BTreeSet` yields the same
/// sorted, deduplicated order Python produces with `sorted(form_keys)`
/// (tiled/client/awkward.py:62), keeping the request deterministic.
fn collect_form_keys(form: &serde_json::Value) -> Vec<String> {
    let mut keys = std::collections::BTreeSet::new();
    collect_form_keys_into(form, &mut keys);
    keys.into_iter().collect()
}

fn collect_form_keys_into(value: &serde_json::Value, out: &mut std::collections::BTreeSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(fk)) = map.get("form_key") {
                out.insert(fk.clone());
            }
            for v in map.values() {
                collect_form_keys_into(v, out);
            }
        }
        serde_json::Value::Array(items) => {
            for v in items {
                collect_form_keys_into(v, out);
            }
        }
        _ => {}
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
    // Cap declared per-entry size so a malicious response can't drive the
    // process to abort via `Vec::with_capacity`. 1 GiB per entry is well
    // beyond any legitimate awkward payload chunk; tighten further if a
    // real workload disagrees.
    const MAX_ENTRY_SIZE: u64 = 1 << 30;
    for i in 0..zip.len() {
        let mut entry = zip
            .by_index(i)
            .map_err(|e| ClientError::Invalid(format!("awkward zip entry {i}: {e}")))?;
        let name = entry.name().to_string();
        let declared = entry.size();
        if declared > MAX_ENTRY_SIZE {
            return Err(ClientError::Invalid(format!(
                "awkward zip entry {name}: declared size {declared} exceeds {MAX_ENTRY_SIZE}",
            )));
        }
        let mut buf: Vec<u8> = Vec::new();
        // try_reserve so an inflated size can't trigger an allocation
        // abort even within the cap.
        buf.try_reserve(declared as usize).map_err(|e| {
            ClientError::Invalid(format!("awkward zip alloc {name} ({declared}B): {e}"))
        })?;
        entry
            .read_to_end(&mut buf)
            .map_err(|e| ClientError::Invalid(format!("awkward zip read {name}: {e}")))?;
        out.insert(name, bytes::Bytes::from(buf));
    }
    Ok(out)
}

#[allow(unused_imports)]
use OCTET_STREAM_MIME_TYPE as _OCTET_STREAM_KEEP;

#[cfg(test)]
mod tests {
    use super::collect_form_keys;

    #[test]
    fn collect_form_keys_walks_nested_form_sorted_and_deduped() {
        // form_keys live on nodes nested under both `contents` (array) and
        // `content` (object); `node1` appears twice and must collapse to one.
        let form = serde_json::json!({
            "class": "RecordArray",
            "contents": [
                {"class": "NumpyArray", "primitive": "float64", "form_key": "node2"},
                {"class": "ListOffsetArray", "offsets": "i64", "form_key": "node1",
                 "content": {"class": "NumpyArray", "primitive": "int64", "form_key": "node1"}}
            ],
            "form_key": "node0"
        });
        assert_eq!(
            collect_form_keys(&form),
            vec![
                "node0".to_string(),
                "node1".to_string(),
                "node2".to_string()
            ]
        );
    }

    #[test]
    fn collect_form_keys_handles_form_without_keys() {
        let form = serde_json::json!({"class": "EmptyArray"});
        assert!(collect_form_keys(&form).is_empty());
    }
}
