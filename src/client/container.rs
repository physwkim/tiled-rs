//! `ContainerClient` — navigate a tree of nodes by key, list, search.
//!
//! Mirrors `tiled/client/container.py::Container`. The Python class implements
//! `collections.abc.Mapping` (so you can do `c["foo"]["bar"]`); we expose the
//! equivalent async getters: `get`, `keys`, `len`, `iter`, plus `search` for
//! filtered listings.

use crate::core::schemas::{ContainerLinks, NodeAttributes, NodeLinks, PaginationLinks, Resource};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Deserialize;
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

use crate::core::data_source::{DataSource, Management};
use crate::core::schemas::{GetDistinctResponse, PostMetadataResponse};
use crate::core::structures::{
    AnyStructure, ArrayStructure, RaggedStructure, StructureFamily, TableStructure,
};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::client::any_client::AnyClient;
use crate::client::array::ArrayClient;
use crate::client::base::{BaseClient, Item};
use crate::client::context::Context;
use crate::client::dataframe::TableClient;
use crate::client::error::{ClientError, Result};
use crate::client::ragged::RaggedClient;
use crate::client::utils::{decode_response, resolve_export_format, retry};

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
    ///
    /// Requests `fields=""` so the server returns only ids, not full attribute
    /// payloads (Python `Container.__iter__`, container.py:243). Against a
    /// Python server this is 5–10× fewer bytes; the Rust server ignores the
    /// hint and returns full items, from which the id is still read.
    pub async fn keys(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in self.list_entries_with_fields(None, Some("")).await? {
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
        // Fall back to a search with limit=0; meta.count is the total. The
        // `fields=count` hint tells a (Python) server to skip materializing any
        // item rows and return only the count (core.py:264 → `items = []`); the
        // Rust server ignores the hint but still returns `meta.count`, so this
        // is a pure-perf projection that is correct against both. Mirrors Python
        // `Container.__len__` (container.py:206).
        let url =
            self.search_url_with(0, 0, &[("fields".to_string(), "count".to_string())], false)?;
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
    ///
    /// When this client carries active search filters (from [`search`] /
    /// [`with_filter`]), the lookup is performed *within* the filtered result
    /// set: a `KeyLookup` filter plus the active queries are routed through the
    /// `search` link, and a key that is not in the filtered view yields
    /// [`ClientError::KeyNotFound`]. This mirrors Python
    /// `Container.__getitem__` (`container.py:280-310`), where
    /// `node.search(...)["key"]` raises `KeyError` if `"key"` is absent from the
    /// search results rather than fetching it unconditionally.
    ///
    /// [`search`]: Self::search
    /// [`with_filter`]: Self::with_filter
    pub async fn get(&self, key: &str) -> Result<AnyClient> {
        if !self.queries.is_empty() {
            return self.get_within_search(key).await;
        }
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

    /// Look up `key` *within* the active search results: route a `KeyLookup`
    /// filter plus the active queries through the `search` link with
    /// `page[limit]=1`. An empty result means the key is not in the filtered
    /// view → [`ClientError::KeyNotFound`] (Python's `KeyError`).
    async fn get_within_search(&self, key: &str) -> Result<AnyClient> {
        let lookup = crate::core::queries::Query::Lookup(crate::core::queries::KeyLookup {
            key: key.into(),
        });
        let url = self.search_url_with(0, 1, &lookup.encode(), self.base.include_data_sources)?;
        let resp: SearchResponse = retry(|| async {
            let r = self.base.context.get(&url).await?;
            decode_response::<SearchResponse>(r).await
        })
        .await?;
        let item = resp.data.into_iter().next().ok_or_else(|| {
            ClientError::KeyNotFound(format!("no child '{key}' in filtered results"))
        })?;
        AnyClient::from_item(
            self.base.context.clone(),
            item,
            self.base.include_data_sources,
        )
    }

    /// List the children of this container, optionally limited to `limit`
    /// items. Returns each as a parsed `Item` with its full attribute payload.
    pub async fn list_entries(&self, limit: Option<usize>) -> Result<Vec<Item>> {
        // `None` fields → the server returns full items (structure_family,
        // structure, metadata, links). Callers such as `xarray`/`composite`
        // depend on these, so the full payload is requested here.
        self.list_entries_with_fields(limit, None).await
    }

    /// Paginate the children, optionally projecting which fields the server
    /// returns. `fields = Some("")` requests id-only entries (Python
    /// `fields=""`, core.py:248 → `attributes={"ancestors": ...}`, self-link
    /// only); `fields = None` requests full items. Used by [`keys`] to avoid
    /// pulling full attribute payloads it discards.
    ///
    /// [`keys`]: Self::keys
    async fn list_entries_with_fields(
        &self,
        limit: Option<usize>,
        fields: Option<&str>,
    ) -> Result<Vec<Item>> {
        let mut all = Vec::new();
        let mut offset = 0usize;
        let page = limit.unwrap_or(100).min(100);
        let extra: Vec<(String, String)> = match fields {
            Some(f) => vec![("fields".to_string(), f.to_string())],
            None => Vec::new(),
        };
        loop {
            let url = self.search_url_with(offset, page, &extra, false)?;
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
    /// in [`crate::client::queries`] to build queries; the [`crate::client::queries::Key`]
    /// builder covers equality, inequality, and numeric comparisons.
    ///
    /// ```no_run
    /// use tiled_rs::client::queries::{FullText, Key};
    /// # use tiled_rs::client::ContainerClient;
    /// # async fn run(c: ContainerClient) -> tiled_rs::client::Result<()> {
    /// let items = c.search(Key::new("color").eq("red")).keys().await?;
    /// # Ok(()) }
    /// ```
    pub fn search(mut self, query: crate::core::queries::Query) -> Self {
        self.queries.extend(query.encode());
        self
    }

    /// Get the unique values (and optionally counts) of `metadata_keys`,
    /// structure families, and/or specs among this container's entries, via
    /// `GET /api/v1/distinct/{path}`.
    ///
    /// Mirrors Python `Container.distinct(*metadata_keys, structure_families,
    /// specs, counts)` (`container.py:570-606`). Honors any active search
    /// filters ([`search`](Self::search) / [`with_filter`](Self::with_filter)),
    /// same as Python's `**self._queries_as_params`. Errors with
    /// [`ClientError::Server`] (HTTP
    /// 405) when the server has no catalog backing this facility.
    ///
    /// ```no_run
    /// # use tiled_rs::client::ContainerClient;
    /// # async fn run(c: ContainerClient) -> tiled_rs::client::Result<()> {
    /// let distinct = c.distinct(&["color"], false, false, true).await?;
    /// # Ok(()) }
    /// ```
    pub async fn distinct(
        &self,
        metadata_keys: &[&str],
        structure_families: bool,
        specs: bool,
        counts: bool,
    ) -> Result<GetDistinctResponse> {
        let self_link = self.base.require_link("self")?;
        let distinct_link = self_link.replacen("/metadata", "/distinct", 1);
        let mut url = Url::parse(&distinct_link).map_err(ClientError::from)?;
        {
            let mut q = url.query_pairs_mut();
            for key in metadata_keys {
                q.append_pair("metadata", key);
            }
            q.append_pair("structure_families", &structure_families.to_string());
            q.append_pair("specs", &specs.to_string());
            q.append_pair("counts", &counts.to_string());
            for (k, v) in &self.queries {
                q.append_pair(k, v);
            }
        }
        retry(|| async {
            let r = self.base.context.get(&url).await?;
            decode_response::<GetDistinctResponse>(r).await
        })
        .await
    }

    /// Create a new child node via `POST /api/v1/metadata/{parent-path}`.
    ///
    /// Returns the key (id) assigned by the server. `data_sources` may be
    /// empty for a pure-metadata or server-managed node. `key` is
    /// `Some(name)` to request a specific name, or `None` to let the server
    /// generate a unique one (Python parity: `Container.new(key=None)`,
    /// `container.py:680-729`).
    pub async fn create_node(
        &self,
        key: Option<&str>,
        structure_family: StructureFamily,
        metadata: serde_json::Value,
        specs: Vec<serde_json::Value>,
        data_sources: Vec<DataSource>,
    ) -> Result<String> {
        self.post_new_node(key, structure_family, metadata, specs, data_sources, None)
            .await
    }

    /// The single `POST /api/v1/metadata/{parent}` builder behind
    /// [`create_node`](Self::create_node) and the `write_*` helpers, mirroring
    /// Python `Container.new` (`container.py:680`).
    ///
    /// `access_tags` maps to the wire `access_blob` field: `Some(tags)` sends
    /// `access_blob: {"tags": [...]}` (Python `new`: `access_blob = {"tags":
    /// access_tags}`, `container.py:704`), which the Rust server accepts on
    /// create (`PostMetadataRequest.access_blob`, `schemas.rs:309`; threaded
    /// through `init_node` at `router.rs:4586`). `None` omits the field
    /// entirely so the server assigns the default creator-owned blob
    /// (`creator_access_blob`, `router.rs:4588`) — deviating from Python, which
    /// always sends `access_blob` (`{}` when no tags); against the Rust server
    /// omission is what preserves the existing creator-ownership default.
    async fn post_new_node(
        &self,
        key: Option<&str>,
        structure_family: StructureFamily,
        metadata: serde_json::Value,
        specs: Vec<serde_json::Value>,
        data_sources: Vec<DataSource>,
        access_tags: Option<&[String]>,
    ) -> Result<String> {
        let self_link = self.base.require_link("self")?;
        let url = Url::parse(self_link)?;
        // Wire field is `id`, matching Python tiled's `PostMetadataRequest.id`
        // (tiled/server/schemas.py:462) — a real Python tiled server ignores
        // a top-level `key` field.
        let mut body = serde_json::json!({
            "id": key,
            "structure_family": structure_family,
            "metadata": metadata,
            "specs": specs,
            "data_sources": data_sources,
        });
        if let Some(tags) = access_tags {
            body["access_blob"] = serde_json::json!({ "tags": tags });
        }
        let resp = self.base.context.post_json(&url, &body).await?;
        let created = decode_response::<PostMetadataResponse>(resp).await?;
        Ok(created.id)
    }

    /// Convenience: create an empty container child. `key` is `Some(name)`
    /// to request a specific name, or `None` to let the server generate a
    /// unique one.
    pub async fn create_container(
        &self,
        key: Option<&str>,
        metadata: serde_json::Value,
    ) -> Result<ContainerClient> {
        let created_key = self
            .create_node(key, StructureFamily::Container, metadata, vec![], vec![])
            .await?;
        // Fetch the newly-created child (by the server-assigned key, which
        // echoes the caller's key when one was given) and return it as a
        // ContainerClient.
        self.get(&created_key).await?.into_container()
    }

    /// Create a writable array child from an explicit `structure`, then upload
    /// its C-order element buffer. Mirrors Python `Container.write_array`
    /// (`container.py:842`): `new(array, [DataSource(structure)], ...)` then
    /// `client.write(array)`.
    ///
    /// Deviation from Python: Python derives shape/dtype/chunks from a numpy /
    /// dask array; the Rust client has no array runtime, so the caller supplies
    /// the [`ArrayStructure`] and the raw `data` buffer directly (the same
    /// split as [`ArrayClient::write`], which takes `nelem * element_size`
    /// C-order bytes). Single-chunk upload only — chunked `write_block` fan-out
    /// (Python `container.py:919-937`) is left to the caller.
    ///
    /// The data source is `management: writable` with no pinned `mimetype`, so
    /// the server picks its managed-write backend for arrays (zarr, else npy —
    /// `default_creation_mimetype`, `router.rs`). `access_tags` is threaded
    /// through [`post_new_node`](Self::post_new_node).
    ///
    /// [`ArrayClient::write`]: crate::client::array::ArrayClient::write
    pub async fn write_array(
        &self,
        key: Option<&str>,
        structure: ArrayStructure,
        data: bytes::Bytes,
        metadata: serde_json::Value,
        specs: Vec<serde_json::Value>,
        access_tags: Option<&[String]>,
    ) -> Result<ArrayClient> {
        let ds = DataSource {
            structure_family: StructureFamily::Array,
            structure: Some(AnyStructure::Array(structure)),
            id: None,
            // Let the server choose the managed-write backend and its mimetype
            // (Python `write_array` likewise omits it).
            mimetype: None,
            parameters: serde_json::json!({}),
            properties: serde_json::json!({}),
            assets: vec![],
            management: Management::Writable,
        };
        let created_key = self
            .post_new_node(
                key,
                StructureFamily::Array,
                metadata,
                specs,
                vec![ds],
                access_tags,
            )
            .await?;
        let client = self.get(&created_key).await?.into_array()?;
        client.write(data).await?;
        Ok(client)
    }

    /// Create a writable table child whose columns come from `schema`, then
    /// upload `batches`. Mirrors Python `Container.write_table`
    /// (`container.py:1212`): `new(table, [DataSource(structure)], ...)` then
    /// `client.write(data)`.
    ///
    /// The [`TableStructure`] is derived from `schema` — one partition, columns
    /// taken from the field names — matching Python `TableStructure.from_schema`
    /// with `npartitions=1`. `arrow_schema` is left empty: the Rust server
    /// validates a write against the structure's `columns` and the written IPC
    /// stream (see [`TableClient::write`]), so the encoded schema is not needed
    /// at create time (existing round-trip tests create tables the same way).
    ///
    /// The data source is `management: writable` with no pinned `mimetype`, so
    /// the server picks its managed-write backend for tables (parquet, else
    /// csv). `access_tags` is threaded through
    /// [`post_new_node`](Self::post_new_node).
    ///
    /// Python's deprecated `write_dataframe` alias (`container.py:1286`) is not
    /// ported; use this method.
    ///
    /// [`TableClient::write`]: crate::client::dataframe::TableClient::write
    pub async fn write_table(
        &self,
        key: Option<&str>,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        metadata: serde_json::Value,
        specs: Vec<serde_json::Value>,
        access_tags: Option<&[String]>,
    ) -> Result<TableClient> {
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let structure = TableStructure {
            arrow_schema: String::new(),
            npartitions: 1,
            columns,
            resizable: Default::default(),
        };
        let ds = DataSource {
            structure_family: StructureFamily::Table,
            structure: Some(AnyStructure::Table(structure)),
            id: None,
            mimetype: None,
            parameters: serde_json::json!({}),
            properties: serde_json::json!({}),
            assets: vec![],
            management: Management::Writable,
        };
        let created_key = self
            .post_new_node(
                key,
                StructureFamily::Table,
                metadata,
                specs,
                vec![ds],
                access_tags,
            )
            .await?;
        let client = self.get(&created_key).await?.into_table()?;
        client.write(schema, batches).await?;
        Ok(client)
    }

    /// Create a writable ragged (variable-length) array child from an explicit
    /// `structure`, then upload its rows as a JSON list-of-lists. Mirrors Python
    /// `Container.write_ragged` (`container.py:996`): `new(ragged,
    /// [DataSource(structure, mimetype="application/x-ragged+sql")], ...)` then
    /// `client.write(array)`.
    ///
    /// The data source pins `application/x-ragged+sql` — the server's only
    /// ragged managed-write backend (Python `write_ragged` pins the same;
    /// `default_creation_mimetype(Ragged)` also resolves to it) — with
    /// `management: writable`. `access_tags` is threaded through
    /// [`post_new_node`](Self::post_new_node).
    ///
    /// Deviations from Python: Python coerces the input via `make_ragged_array`,
    /// derives partitioning with `make_ragged_chunks`, and builds the structure
    /// with `RaggedStructure.from_array`; the Rust client has no ragged runtime,
    /// so the caller supplies the [`RaggedStructure`] and the row data as a JSON
    /// list-of-lists (the shape [`RaggedClient::write`] takes). This performs the
    /// single-partition (chunk 0 / `PUT /ragged/full`) upload — Python's
    /// non-chunked branch (`container.py:1060`); a multi-partition structure must
    /// be filled per chunk via [`RaggedClient::write_block`].
    ///
    /// [`RaggedClient::write`]: crate::client::ragged::RaggedClient::write
    /// [`RaggedClient::write_block`]: crate::client::ragged::RaggedClient::write_block
    pub async fn write_ragged(
        &self,
        key: Option<&str>,
        structure: RaggedStructure,
        data: &serde_json::Value,
        metadata: serde_json::Value,
        specs: Vec<serde_json::Value>,
        access_tags: Option<&[String]>,
    ) -> Result<RaggedClient> {
        let ds = DataSource {
            structure_family: StructureFamily::Ragged,
            structure: Some(AnyStructure::Ragged(structure)),
            id: None,
            mimetype: Some(crate::core::media_type::mime::RAGGED_SQL.to_string()),
            parameters: serde_json::json!({}),
            properties: serde_json::json!({}),
            assets: vec![],
            management: Management::Writable,
        };
        let created_key = self
            .post_new_node(
                key,
                StructureFamily::Ragged,
                metadata,
                specs,
                vec![ds],
                access_tags,
            )
            .await?;
        let client = self.get(&created_key).await?.into_ragged()?;
        client.write(data, true).await?;
        Ok(client)
    }

    /// Delete every immediate child of this container (to empty it before
    /// deleting the container itself). Children that are themselves containers
    /// must already be empty or have their own children deleted first, since
    /// the server refuses to delete non-empty containers.
    pub async fn delete_contents(&self, external_only: bool) -> Result<()> {
        let keys = self.keys().await?;
        for key in keys {
            let child = self.get(&key).await?;
            if let Some(b) = child.base() {
                b.delete(external_only).await?;
            }
        }
        Ok(())
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

    /// Build a `search` URL with `extra_filters` prepended before this client's
    /// active queries (matching Python's param order: `KeyLookup` first, then
    /// the standing queries, then sort). `include_data_sources` appends
    /// `include_data_sources=true` when set.
    fn search_url_with(
        &self,
        offset: usize,
        limit: usize,
        extra_filters: &[(String, String)],
        include_data_sources: bool,
    ) -> Result<Url> {
        let link = self.base.require_link("search")?;
        let mut url = Url::parse(link).map_err(ClientError::from)?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("page[offset]", &offset.to_string());
            q.append_pair("page[limit]", &limit.to_string());
            for (k, v) in extra_filters {
                q.append_pair(k, v);
            }
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
            if include_data_sources {
                q.append_pair("include_data_sources", "true");
            }
        }
        Ok(url)
    }

    /// Export this whole subtree to a file at `dest`, mirroring Python
    /// `Container.export` (container.py:625). Downloads the container's full
    /// contents via `GET /api/v1/container/full/{path}?format=<fmt>` and streams
    /// the response body to `dest` — the same idiom as
    /// [`ArrayClient::export`](crate::client::array::ArrayClient::export) and
    /// [`TableClient::export`](crate::client::dataframe::TableClient::export).
    ///
    /// `format` selects the output serialization and is resolved like Python
    /// `tiled.client.utils.export_util`:
    /// - `Some(fmt)` — used as given, with a single leading `.` stripped
    ///   (`Some(".zip")` and `Some("zip")` are equivalent). For a container the
    ///   only content-bundling format the server produces is `zip`, which packs
    ///   each leaf array/table as a zip entry; `json`/`html` yield the metadata
    ///   tree / browseable index.
    /// - `None` — the format is inferred from `dest`'s filename suffixes joined
    ///   without dots (`export.zip` → `zip`, matching Python
    ///   `pathlib.Path.suffixes`). A `dest` with no extension to infer from
    ///   yields [`ClientError::Invalid`] rather than sending an empty format the
    ///   server would reject.
    ///
    /// The server resolves the resulting format as an alias or media type; an
    /// unsupported format surfaces as a mapped server error. Unlike Python this
    /// does not accept a `fields` filter — the Rust `/container/full` route does
    /// not read a `field` param.
    pub async fn export(&self, dest: &std::path::Path, format: Option<&str>) -> Result<()> {
        let resolved = resolve_export_format(dest, format)?;
        let link = self.base.require_link("full")?;
        let mut url = Url::parse(link).map_err(ClientError::from)?;
        url.query_pairs_mut().append_pair("format", &resolved);
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes = retry(|| async { self.base.context.get_bytes(&url, "*/*").await }).await?;
        std::fs::write(dest, &bytes)
            .map_err(|e| ClientError::Invalid(format!("write {}: {e}", dest.display())))
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
