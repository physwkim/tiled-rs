//! `ContainerClient` — navigate a tree of nodes by key, list, search.
//!
//! Mirrors `tiled/client/container.py::Container`. The Python class implements
//! `collections.abc.Mapping` (so you can do `c["foo"]["bar"]`); we expose the
//! equivalent async getters: `get`, `keys`, `len`, `iter`, plus `search` for
//! filtered listings.

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Deserialize;
use tiled_core::schemas::{ContainerLinks, NodeAttributes, NodeLinks, PaginationLinks, Resource};
use url::Url;

/// Characters to percent-encode inside a path segment (per RFC 3986
/// `pchar` minus `unreserved` and `sub-delims`).
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

use crate::any_client::AnyClient;
use crate::base::{BaseClient, Item};
use crate::context::Context;
use crate::error::{ClientError, Result};
use crate::utils::{decode_response, retry};

/// Sort direction for container child ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// A client over a container node.
#[derive(Debug, Clone)]
pub struct ContainerClient {
    base: BaseClient,
    sort: Vec<(String, SortDirection)>,
    queries: Vec<(String, String)>,
}

impl ContainerClient {
    /// Wrap an item that the caller has already parsed into a container.
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let base = BaseClient::new(context, item, include_data_sources)?;
        Ok(Self {
            base,
            sort: Vec::new(),
            queries: Vec::new(),
        })
    }

    pub fn base(&self) -> &BaseClient {
        &self.base
    }

    /// Iterate the *names* of children, page by page, until the server stops
    /// returning a `next` link. Default page size is 100.
    pub async fn keys(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in self.list_entries(None).await? {
            out.push(entry.id);
        }
        Ok(out)
    }

    /// Number of children. Honors the inline `structure.contents` shortcut
    /// when the server has provided it.
    pub async fn len(&self) -> Result<usize> {
        if let Some(structure) = self.base.item().attributes.structure.as_ref()
            && let Some(count) = structure.get("count").and_then(|v| v.as_u64())
        {
            return Ok(count as usize);
        }
        // Fall back to a search with limit=0; meta.count is the total.
        let url = self.search_url(0, 0)?;
        let resp: SearchResponse = retry(|| async {
            let r = self.base.context.get(&url).await?;
            decode_response::<SearchResponse>(r).await
        })
        .await?;
        Ok(resp.meta.count)
    }

    /// Look up a child by exact key.
    ///
    /// Honors the parent's `include_data_sources` flag — when true, the GET
    /// adds `?include_data_sources=true` so the server returns the child's
    /// `data_sources` payload (consistent with what `from_uri(...,
    /// include_data_sources=true)` requested at construction time).
    pub async fn get(&self, key: &str) -> Result<AnyClient> {
        let mut url = Url::parse(self.base.require_link("self")?).map_err(ClientError::from)?;
        // self link points to /metadata/.../<this>; appending `/<key>` walks one step.
        // Percent-encode the key so `?`, `#`, `/`, etc. don't reshape the URL.
        let encoded = utf8_percent_encode(key, PATH_SEGMENT).to_string();
        let new_path = if url.path().ends_with('/') {
            format!("{}{}", url.path(), encoded)
        } else {
            format!("{}/{}", url.path(), encoded)
        };
        url.set_path(&new_path);
        if self.base.include_data_sources {
            url.query_pairs_mut()
                .append_pair("include_data_sources", "true");
        }

        let resp: ResourceEnvelope = retry(|| async {
            let r = self.base.context.get(&url).await?;
            decode_response::<ResourceEnvelope>(r).await
        })
        .await?;
        let item = resp
            .data
            .ok_or_else(|| ClientError::KeyNotFound(format!("no child '{key}'")))?;
        AnyClient::from_item(
            self.base.context.clone(),
            item,
            self.base.include_data_sources,
        )
    }

    /// List the children of this container, optionally limited to `limit`
    /// items. Returns each as a parsed `Item`.
    pub async fn list_entries(&self, limit: Option<usize>) -> Result<Vec<Item>> {
        let mut all = Vec::new();
        let mut offset = 0usize;
        let page = limit.unwrap_or(100).min(100);
        loop {
            let url = self.search_url(offset, page)?;
            let resp: SearchResponse = retry(|| async {
                let r = self.base.context.get(&url).await?;
                decode_response::<SearchResponse>(r).await
            })
            .await?;
            let count = resp.data.len();
            all.extend(resp.data);
            if let Some(want) = limit
                && all.len() >= want
            {
                all.truncate(want);
                break;
            }
            // Stop when the server indicates there is no next page or we got
            // less than a full page back.
            let has_next = resp
                .links
                .as_ref()
                .and_then(|l| l.next.as_deref())
                .is_some();
            if !has_next || count == 0 || count < page {
                break;
            }
            offset += page;
        }
        Ok(all)
    }

    /// Apply a typed query filter, returning a new client that returns only
    /// matching entries.
    ///
    /// Mirrors Python `Container.search(query)`. Chain multiple calls for AND
    /// semantics (each additional query narrows the result set). Use the types
    /// in [`crate::queries`] to build queries; the [`crate::queries::Key`]
    /// builder covers equality, inequality, and numeric comparisons.
    ///
    /// ```no_run
    /// use tiled_client::queries::{FullText, Key};
    /// # use tiled_client::ContainerClient;
    /// # async fn run(c: ContainerClient) -> tiled_client::Result<()> {
    /// let items = c.search(Key::new("color").eq("red")).keys().await?;
    /// # Ok(()) }
    /// ```
    pub fn search(mut self, query: tiled_core::queries::Query) -> Self {
        self.queries.extend(query.encode());
        self
    }

    /// Add a raw `key=value` filter pair to subsequent searches.
    pub fn with_filter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.queries.push((key.into(), value.into()));
        self
    }

    /// Add a sort key. The Tiled API encodes direction with a `-` prefix.
    pub fn sort_by(mut self, key: impl Into<String>, direction: SortDirection) -> Self {
        self.sort.push((key.into(), direction));
        self
    }

    fn search_url(&self, offset: usize, limit: usize) -> Result<Url> {
        let link = self.base.require_link("search")?;
        let mut url = Url::parse(link).map_err(ClientError::from)?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("page[offset]", &offset.to_string());
            q.append_pair("page[limit]", &limit.to_string());
            for (k, v) in &self.queries {
                q.append_pair(k, v);
            }
            if !self.sort.is_empty() {
                let formatted: Vec<String> = self
                    .sort
                    .iter()
                    .map(|(k, d)| match d {
                        SortDirection::Ascending => k.clone(),
                        SortDirection::Descending => format!("-{k}"),
                    })
                    .collect();
                q.append_pair("sort", &formatted.join(","));
            }
        }
        Ok(url)
    }
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    data: Vec<Item>,
    #[serde(default)]
    meta: SearchMeta,
    #[serde(default)]
    links: Option<PaginationLinks>,
}

#[derive(Debug, Default, Deserialize)]
struct SearchMeta {
    #[serde(default)]
    count: usize,
}

/// Single-resource envelope (`/metadata/...` returns this shape).
#[derive(Debug, Deserialize)]
struct ResourceEnvelope {
    data: Option<Item>,
}

// -- explicit imports from tiled-core just to keep doc links functional --
#[allow(dead_code)]
const _: fn() = || {
    let _ = std::mem::size_of::<Resource<NodeAttributes>>();
    let _ = std::mem::size_of::<NodeLinks>();
    let _ = std::mem::size_of::<ContainerLinks>();
};
