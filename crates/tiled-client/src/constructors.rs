//! Top-level entry points: `from_uri`, `from_context`.
//!
//! Mirrors `tiled/client/constructors.py`.

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Deserialize;
use url::Url;

use crate::any_client::AnyClient;
use crate::base::Item;
use crate::context::{Context, ContextOptions};
use crate::error::{ClientError, Result};
use crate::utils::{decode_response, retry};

const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'#')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b'%');

/// Connect to a Tiled server and return the client at the requested path.
///
/// Mirrors `tiled.client.from_uri`. The URI may include `?api_key=...` (which
/// is moved into a header) and a path past `/api/v1/` (which is treated as the
/// node to navigate to).
pub async fn from_uri(uri: &str) -> Result<AnyClient> {
    from_uri_with_options(uri, ContextOptions::default(), false).await
}

/// Like [`from_uri`] but with explicit options (api key, http client, ...).
pub async fn from_uri_with_options(
    uri: &str,
    options: ContextOptions,
    include_data_sources: bool,
) -> Result<AnyClient> {
    let (ctx, parts) = Context::from_uri_with_options(uri, options)?;
    from_context(ctx, &parts, include_data_sources).await
}

/// Connect using a pre-built `Context`. The `node_path_parts` walk past
/// `/api/v1/metadata/` to land at the requested node (empty = root).
pub async fn from_context(
    context: Context,
    node_path_parts: &[String],
    include_data_sources: bool,
) -> Result<AnyClient> {
    let mut url = context.api_uri().clone();
    let encoded: Vec<String> = node_path_parts
        .iter()
        .map(|p| utf8_percent_encode(p, PATH_SEGMENT).to_string())
        .collect();
    let path_segment = format!("{}metadata/{}", url.path(), encoded.join("/"));
    url.set_path(&path_segment);
    if include_data_sources {
        url.query_pairs_mut()
            .append_pair("include_data_sources", "true");
    }
    let item = retry(|| async {
        let resp = context.get(&url).await?;
        let envelope: ResourceEnvelope = decode_response(resp).await?;
        envelope
            .data
            .ok_or_else(|| ClientError::Invalid("metadata response missing data".into()))
    })
    .await?;
    AnyClient::from_item(context, item, include_data_sources)
}

/// Build the URL for a metadata fetch — used by tests + advanced callers.
#[doc(hidden)]
pub fn metadata_url(api_uri: &Url, node_path_parts: &[String]) -> Result<Url> {
    let mut url = api_uri.clone();
    let path = format!("{}metadata/{}", url.path(), node_path_parts.join("/"));
    url.set_path(&path);
    Ok(url)
}

#[derive(Debug, Deserialize)]
struct ResourceEnvelope {
    data: Option<Item>,
}
