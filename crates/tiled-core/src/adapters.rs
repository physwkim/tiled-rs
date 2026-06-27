//! Adapter trait hierarchy for the five data structure families.
//!
//! Corresponds to `tiled/adapters/protocols.py`.
//!
//! Traits used as `dyn` trait objects use explicit `Pin<Box<dyn Future>>` returns
//! instead of `#[async_trait]` to eliminate the proc-macro dependency.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dtype::{ArrowTable, DynNDArray};
use crate::error::{Result, TiledError};
use crate::ndslice::NDSlice;
use crate::schemas::SortDirection;
use crate::structures::{
    ArrayStructure, AwkwardStructure, ContainerStructure, RaggedStructure, SparseStructure, Spec,
    StructureFamily, TableStructure,
};

/// Boxed future type alias for async trait methods (dyn-safe).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Base trait that all adapters must implement.
pub trait BaseAdapter: Send + Sync {
    fn structure_family(&self) -> StructureFamily;
    fn metadata(&self) -> &serde_json::Value;
    fn specs(&self) -> &[Spec];
}

// ---------------------------------------------------------------------------
// Array (dyn-used → explicit Pin<Box<dyn Future>>)
// ---------------------------------------------------------------------------

pub trait ArrayAdapterRead: BaseAdapter {
    fn structure(&self) -> &ArrayStructure;

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>>;

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>>;

    /// Optional downcast to a write-capable view. Adapters whose
    /// underlying store supports `write_block` / `append` override this
    /// to return `Some(self)`; the rest leave it `None`. Lets the
    /// router pick a write path at request time without giving up the
    /// existing `Arc<dyn ArrayAdapterRead>` storage in
    /// `AnyAdapter::Array`. Mirrors the spirit of upstream tiled
    /// PR #802 (extendable arrays) on the trait side.
    fn as_writable(&self) -> Option<&dyn ArrayAdapterWrite> {
        None
    }
}

pub trait ArrayAdapterWrite: ArrayAdapterRead {
    /// Overwrite the whole array. `data.shape` must equal the array's shape
    /// (`structure().shape`). `PUT /array/full` routes here.
    fn write<'a>(&'a self, data: DynNDArray) -> BoxFuture<'a, Result<()>>;

    /// Overwrite a single chunk addressed by `block` (one index per axis).
    /// `data.shape` must equal that chunk's shape — the per-axis lengths
    /// `structure().chunks[axis][block[axis]]`. `PUT /array/block` routes here.
    /// Distinct from [`ArrayAdapterWrite::write`]: this targets one chunk, not
    /// the whole array, so block and full writes no longer share a sentinel.
    fn write_block<'a>(&'a self, data: DynNDArray, block: &'a [usize])
    -> BoxFuture<'a, Result<()>>;

    /// Extend the array along `axis` by appending `data`. Returns the
    /// new size of `axis` after the append. Mirrors upstream tiled
    /// PR #802's appendable-zarr work — only adapters whose underlying
    /// store supports growth (zarr, ND-streaming) implement it; default
    /// errors out with a clear "not supported" so the route stays the
    /// same shape regardless of backend support.
    fn append<'a>(&'a self, _data: DynNDArray, _axis: usize) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            Err(crate::error::TiledError::Validation(
                "append is not supported by this adapter".into(),
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Table (dyn-used)
// ---------------------------------------------------------------------------

pub trait TableAdapterRead: BaseAdapter {
    fn structure(&self) -> &TableStructure;

    fn read<'a>(&'a self, fields: Option<&'a [String]>) -> BoxFuture<'a, Result<ArrowTable>>;

    fn read_partition<'a>(
        &'a self,
        partition: usize,
        fields: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<ArrowTable>>;

    /// Optional downcast to a write-capable view, mirroring
    /// [`ArrayAdapterRead::as_writable`]. Adapters whose backing store can be
    /// written (managed tables under the server's writable storage) override
    /// this to return `Some(self)`; the rest leave it `None`, so a read-only
    /// table answers 405 rather than the write route silently not existing.
    fn as_table_writable(&self) -> Option<&dyn TableAdapterWrite> {
        None
    }
}

pub trait TableAdapterWrite: TableAdapterRead {
    /// Overwrite the whole table with `data`. `PUT /table/full` routes here.
    /// Kept distinct from `write_partition` so "full" never silently means
    /// "partition 0" (the dual meaning removed on the array side).
    fn write<'a>(&'a self, data: ArrowTable) -> BoxFuture<'a, Result<()>>;

    /// Overwrite a single partition. `PUT /table/partition` routes here.
    fn write_partition<'a>(
        &'a self,
        data: ArrowTable,
        partition: usize,
    ) -> BoxFuture<'a, Result<()>>;

    /// Append rows to a single partition. `PATCH /table/partition` routes here.
    /// Adapters that do not support row-level appends return a Validation error
    /// by default; override to enable.
    fn append_partition<'a>(
        &'a self,
        _data: ArrowTable,
        _partition: usize,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            Err(TiledError::Validation(
                "append_partition is not supported by this adapter".into(),
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Sparse (dyn-used)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SparseData {
    pub coords: Vec<DynNDArray>,
    pub data: DynNDArray,
}

pub trait SparseAdapterRead: BaseAdapter {
    fn structure(&self) -> &SparseStructure;

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<SparseData>>;

    fn read_block<'a>(&'a self, block: &'a [usize]) -> BoxFuture<'a, Result<SparseData>>;
}

// ---------------------------------------------------------------------------
// Awkward (dyn-used)
// ---------------------------------------------------------------------------

pub trait AwkwardAdapterRead: BaseAdapter {
    fn structure(&self) -> &AwkwardStructure;

    fn read(&self) -> BoxFuture<'_, Result<HashMap<String, bytes::Bytes>>>;

    fn read_buffers<'a>(
        &'a self,
        form_keys: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<HashMap<String, bytes::Bytes>>>;

    fn as_writable(&self) -> Option<&dyn AwkwardAdapterWrite> {
        None
    }
}

pub trait AwkwardAdapterWrite: AwkwardAdapterRead {
    fn write(
        &self,
        buffers: HashMap<String, bytes::Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

// ---------------------------------------------------------------------------
// Ragged (dyn-used) — the Rust analog of Python's awkward family.
// ---------------------------------------------------------------------------

/// Data returned by [`RaggedAdapterRead::read`].
///
/// `json_value` is a JSON-encoded list-of-lists, matching Python's
/// `array.tolist()` (`tiled/adapters/ragged.py:73`). `structure` is included
/// so serializers that need buffer-level detail (the ZIP serializer) can
/// compute the Awkward form without re-parsing the shape.
///
/// Lives in tiled-core (not tiled-adapters) so [`AnyAdapter::Ragged`] can name
/// the trait that returns it; the concrete `RaggedAdapter` in tiled-adapters
/// implements [`RaggedAdapterRead`] over this type.
#[derive(Debug, Clone)]
pub struct RaggedData {
    /// JSON list-of-lists, e.g. `[[1.0, 2.0], [3.0]]`.
    pub json_value: serde_json::Value,
    /// Structural description: shape, dtype, chunks.
    pub structure: RaggedStructure,
}

impl RaggedData {
    /// Serialize `json_value` to raw bytes (UTF-8 JSON) — what the JSON and
    /// ZIP serializers in `tiled-serialization` consume as their `&[u8]` data
    /// argument.
    pub fn to_json_bytes(&self) -> std::result::Result<bytes::Bytes, serde_json::Error> {
        serde_json::to_vec(&self.json_value).map(bytes::Bytes::from)
    }

    /// Serialize `structure` to a `serde_json::Value` for use as the metadata
    /// argument to the ragged serializers.
    pub fn structure_as_metadata(
        &self,
    ) -> std::result::Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(&self.structure)
    }
}

/// Trait for adapters that serve ragged (variable-length row) arrays.
///
/// Mirrors the per-family adapter traits above (`ArrayAdapterRead`, etc.).
pub trait RaggedAdapterRead: BaseAdapter {
    fn structure(&self) -> &RaggedStructure;

    /// Read the whole array, or a slice of it, as [`RaggedData`].
    ///
    /// A non-full `slice` is applied with numpy/awkward *basic* indexing
    /// semantics, matching Python `RaggedAdapter.read`
    /// (`tiled/adapters/ragged.py:73-75`).
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<RaggedData>>;

    /// The write face of this adapter, or `None` if it is read-only.
    ///
    /// Python's in-memory `RaggedAdapter` has no write methods; only
    /// `RaggedSQLAdapter` does (`tiled/adapters/ragged.py`). Mirrors
    /// [`AwkwardAdapterRead::as_writable`].
    fn as_writable(&self) -> Option<&dyn RaggedAdapterWrite> {
        None
    }
}

/// Write face for ragged adapters backed by writable storage.
///
/// Mirrors Python `RaggedSQLAdapter`'s write/write_block/patch
/// (`tiled/adapters/ragged.py:249-352`). The data currency is a JSON
/// list-of-lists (`serde_json::Value`) — the same shape `RaggedData.json_value`
/// carries — so the adapter owns the JSON↔Awkward-buffer encoding internally
/// (it holds the structure that fixes the form/dtype), keeping this trait free
/// of any dependency on the buffer codec.
pub trait RaggedAdapterWrite: RaggedAdapterRead {
    /// Write the whole array as a single chunk at block 0. `PUT /ragged/full`
    /// routes here. Python `RaggedSQLAdapter.write` (ragged.py:249-250).
    fn write<'a>(&'a self, data: &'a serde_json::Value) -> BoxFuture<'a, Result<()>> {
        self.write_block(data, 0)
    }

    /// Write one chunk at `block` along axis 0. `PUT /ragged/block` routes here.
    /// A duplicate `block` is a [`TiledError::Conflict`]. Python
    /// `RaggedSQLAdapter.write_block` (ragged.py:252-275).
    fn write_block<'a>(
        &'a self,
        data: &'a serde_json::Value,
        block: usize,
    ) -> BoxFuture<'a, Result<()>>;

    /// Append a chunk, extending axis 0 (`extend=True`), and return the grown
    /// structure for the caller to persist. `PATCH /ragged/full` routes here.
    /// Only appending along the leftmost dimension is supported, and
    /// `extend=false` (overwrite) is rejected — Python
    /// `RaggedSQLAdapter.patch` (ragged.py:277-352).
    fn patch<'a>(
        &'a self,
        data: &'a serde_json::Value,
        offset: &'a [usize],
        extend: bool,
    ) -> BoxFuture<'a, Result<RaggedStructure>>;
}

// ---------------------------------------------------------------------------
// Container (async — IO-backed: SQL catalog, MongoDB, etc.)
// ---------------------------------------------------------------------------

/// One row of a paginated container listing/search result.
///
/// A neutral data carrier (NOT the server `Resource` wire type) so the
/// `tiled-server` schema never leaks into the adapter layer. It holds exactly
/// the per-child attributes a listing needs, sourced WITHOUT resolving a
/// (possibly file-backed) leaf adapter: the SQL catalog fills these from its
/// `Node` row + `data_source.structure`, while in-memory adapters read them
/// off the child adapter itself. `ancestors`/`links`/`sorting` are derived by
/// the server from the path, so they are intentionally absent here.
#[derive(Debug, Clone)]
pub struct SearchEntry {
    pub key: String,
    pub structure_family: StructureFamily,
    pub metadata: serde_json::Value,
    pub specs: Vec<Spec>,
    /// The node's structure JSON (array/table shape, or a container's child
    /// count). `None` when the structure is unknown/not applicable.
    pub structure: Option<serde_json::Value>,
    /// Access-control blob carried through from the backing store. `None` for
    /// adapters that do not track per-node access (in-memory trees).
    pub access_blob: Option<serde_json::Value>,
}

/// One page of search results plus the totals a listing response needs.
///
/// `next_cursor` is the keyset cursor (a catalog node id) for the page that
/// follows this one. It is set **only** by adapters that do keyset pagination
/// (the SQL catalog, default sort) and only when more rows remain; `None`
/// means "no cursor available", so the server falls back to an offset `next`
/// link. In-memory / Mongo trees page by offset and always leave it `None`.
/// Mirrors the `(items, next_cursor)` pair Python's `keys_page`/`items_page`
/// return (tiled/catalog/adapter.py).
#[derive(Debug, Clone)]
pub struct SearchPage {
    pub entries: Vec<SearchEntry>,
    pub total: usize,
    pub next_cursor: Option<i64>,
}

/// A container node: a directory-like level whose children are looked up by
/// key. Every method is async because the only non-trivial implementors are
/// IO-backed (the SQL catalog awaits sqlx; the Mongo adapters offload the
/// sync driver to `spawn_blocking`). This mirrors the leaf adapter traits
/// above, which already return [`BoxFuture`].
///
/// `get` returns an **owned** `Option<AnyAdapter>` rather than a borrow:
/// `AnyAdapter` is `Arc`-backed (see below), so an owned value is a cheap
/// refcount bump for in-memory adapters and a freshly-resolved node for
/// DB-backed ones — which is what lets a DB adapter look a single key up
/// lazily instead of materialising every child to hand back a reference.
pub trait ContainerAdapter: BaseAdapter {
    fn structure(&self) -> BoxFuture<'_, Result<ContainerStructure>>;

    /// Look up one child by key. `Ok(None)` ⇒ no such key; `Err` ⇒ the
    /// lookup itself failed (DB error, etc.) — never collapse a failure into
    /// "absent".
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<AnyAdapter>>>;

    fn keys(&self) -> BoxFuture<'_, Result<Vec<String>>>;
    fn len(&self) -> BoxFuture<'_, Result<usize>>;

    fn is_empty(&self) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move { Ok(self.len().await? == 0) })
    }

    /// Search/filter children. Default: return all keys (no filtering).
    ///
    /// Returns `Err(TiledError::UnsupportedQuery)` for a query variant this
    /// adapter's search path cannot evaluate, so the server can answer HTTP
    /// 400 instead of silently returning a filtered subset (parity with
    /// Python tiled's `UnsupportedQueryType`); any other `Err` is a real IO
    /// failure (→ 500). The default impl supports no filtering, so it simply
    /// returns every key.
    fn search<'a>(
        &'a self,
        _queries: &'a [crate::queries::Query],
    ) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move { self.keys().await })
    }

    /// Paginated search: return a page of children matching `queries` (sorted
    /// by `sorting`) as [`SearchEntry`] rows, plus the **total** match count
    /// and the keyset cursor for the next page. This is the single listing
    /// primitive the server's search endpoint drives — both the catalog and
    /// in-memory trees flow through it, so there is no separate "direct SQL"
    /// path.
    ///
    /// Pagination is selected by the caller: a `cursor` of `Some(_)` requests
    /// the keyset page *after* that cursor; `None` requests the
    /// `[offset, offset+limit)` window. The returned
    /// [`SearchPage::next_cursor`] is `Some` only when the adapter did keyset
    /// pagination (SQL catalog, default sort) and more rows remain; the server
    /// uses it to emit a `page[cursor]` next link, falling back to a
    /// `page[offset]` link when it is `None`. Mirrors Python
    /// `keys_page`/`items_page`/`keys_range` (tiled/catalog/adapter.py).
    ///
    /// The default impl is the in-memory one: it runs [`search`](Self::search)
    /// (which enforces the adapter's query-support matrix → HTTP 400 on an
    /// unevaluable variant), pages the matched keys by offset, and builds each
    /// row from the child adapter. `sorting` and `cursor` are ignored — an
    /// in-memory tree preserves insertion order and cannot keyset-page, so it
    /// always returns `next_cursor: None` (matching Python's offset slice for
    /// trees without `keys_page`). The SQL catalog overrides this to push
    /// filter + sort + keyset/OFFSET down to the database and to carry each
    /// node's `access_blob` and `data_source` structure without resolving the
    /// leaf adapter.
    fn search_page<'a>(
        &'a self,
        queries: &'a [crate::queries::Query],
        _sorting: &'a [(String, SortDirection)],
        _cursor: Option<i64>,
        offset: usize,
        limit: usize,
    ) -> BoxFuture<'a, Result<SearchPage>> {
        Box::pin(async move {
            let matched = self.search(queries).await?;
            let total = matched.len();
            let mut entries = Vec::new();
            for key in matched.into_iter().skip(offset).take(limit) {
                // A key that vanished between `search` and `get` (concurrent
                // delete) is skipped; a `get` that errors propagates rather
                // than silently dropping the entry.
                let Some(adapter) = self.get(&key).await? else {
                    continue;
                };
                let structure = adapter.structure_json().await?;
                entries.push(SearchEntry {
                    key,
                    structure_family: adapter.structure_family(),
                    metadata: adapter.metadata().clone(),
                    specs: adapter.specs().to_vec(),
                    structure,
                    access_blob: None,
                });
            }
            Ok(SearchPage {
                entries,
                total,
                next_cursor: None,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// AnyAdapter — type-erased adapter enum
// ---------------------------------------------------------------------------

/// Leaf adapters are held behind `Arc`, not `Box`, so a tree walk can hand
/// back an owned, `'static` clone of the leaf (a cheap refcount bump) and the
/// async `read*` future can then be awaited on the executor instead of being
/// driven via `Handle::block_on` on a blocking-pool thread. File/DB-backed
/// adapter reads offload their own blocking internally, so driving them with
/// `block_on` parked a second pool thread per read — at high concurrency that
/// exhausts the blocking pool and deadlocks. Arc storage lets the read run on
/// the executor with exactly one pool thread per read (the adapter's own
/// inner offload).
///
/// `Clone` is a per-variant `Arc` refcount bump (every variant wraps an
/// `Arc<dyn …>`), which is what lets `ContainerAdapter::get` hand back an
/// owned, `'static` child without materialising anything.
#[derive(Clone)]
pub enum AnyAdapter {
    Array(Arc<dyn ArrayAdapterRead>),
    Table(Arc<dyn TableAdapterRead>),
    Sparse(Arc<dyn SparseAdapterRead>),
    Awkward(Arc<dyn AwkwardAdapterRead>),
    Ragged(Arc<dyn RaggedAdapterRead>),
    Container(Arc<dyn ContainerAdapter>),
}

impl AnyAdapter {
    #[inline]
    pub fn structure_family(&self) -> StructureFamily {
        match self {
            Self::Array(a) => a.structure_family(),
            Self::Table(a) => a.structure_family(),
            Self::Sparse(a) => a.structure_family(),
            Self::Awkward(a) => a.structure_family(),
            Self::Ragged(a) => a.structure_family(),
            Self::Container(a) => a.structure_family(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &serde_json::Value {
        match self {
            Self::Array(a) => a.metadata(),
            Self::Table(a) => a.metadata(),
            Self::Sparse(a) => a.metadata(),
            Self::Awkward(a) => a.metadata(),
            Self::Ragged(a) => a.metadata(),
            Self::Container(a) => a.metadata(),
        }
    }

    #[inline]
    pub fn specs(&self) -> &[Spec] {
        match self {
            Self::Array(a) => a.specs(),
            Self::Table(a) => a.specs(),
            Self::Sparse(a) => a.specs(),
            Self::Awkward(a) => a.specs(),
            Self::Ragged(a) => a.specs(),
            Self::Container(a) => a.specs(),
        }
    }

    /// Async because the container arm needs `len()`, which is now an async
    /// (DB-backed) call. Returns `Result` so a count failure propagates
    /// instead of being swallowed. Leaf arms are infallible.
    pub async fn structure_json(&self) -> Result<Option<serde_json::Value>> {
        match self {
            Self::Array(a) => Ok(serde_json::to_value(a.structure()).ok()),
            Self::Table(t) => Ok(serde_json::to_value(t.structure()).ok()),
            Self::Sparse(s) => Ok(serde_json::to_value(s.structure()).ok()),
            Self::Awkward(a) => Ok(serde_json::to_value(a.structure()).ok()),
            Self::Ragged(r) => Ok(serde_json::to_value(r.structure()).ok()),
            Self::Container(c) => {
                let count = c.len().await?;
                Ok(Some(serde_json::json!({
                    "contents": null,
                    "count": count,
                })))
            }
        }
    }

    #[inline]
    pub fn as_container(&self) -> Option<&dyn ContainerAdapter> {
        match self {
            Self::Container(c) => Some(c.as_ref()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_array(&self) -> Option<&dyn ArrayAdapterRead> {
        match self {
            Self::Array(a) => Some(a.as_ref()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_table(&self) -> Option<&dyn TableAdapterRead> {
        match self {
            Self::Table(t) => Some(t.as_ref()),
            _ => None,
        }
    }

    /// Owned, `'static` clone of the array leaf (a refcount bump). Returned
    /// from a `spawn_blocking` tree walk so the caller can `await` the
    /// adapter's `read*` future on the executor rather than driving it via
    /// `Handle::block_on` on the blocking pool. See the [`AnyAdapter`] doc
    /// comment for why this avoids the nested blocking-pool deadlock.
    #[inline]
    pub fn as_array_arc(&self) -> Option<Arc<dyn ArrayAdapterRead>> {
        match self {
            Self::Array(a) => Some(Arc::clone(a)),
            _ => None,
        }
    }

    /// Owned, `'static` clone of the table leaf. See [`AnyAdapter::as_array_arc`].
    #[inline]
    pub fn as_table_arc(&self) -> Option<Arc<dyn TableAdapterRead>> {
        match self {
            Self::Table(t) => Some(Arc::clone(t)),
            _ => None,
        }
    }

    /// Owned, `'static` clone of the sparse leaf. See [`AnyAdapter::as_array_arc`].
    #[inline]
    pub fn as_sparse_arc(&self) -> Option<Arc<dyn SparseAdapterRead>> {
        match self {
            Self::Sparse(s) => Some(Arc::clone(s)),
            _ => None,
        }
    }

    /// Owned, `'static` clone of the ragged leaf. See [`AnyAdapter::as_array_arc`].
    #[inline]
    pub fn as_ragged_arc(&self) -> Option<Arc<dyn RaggedAdapterRead>> {
        match self {
            Self::Ragged(r) => Some(Arc::clone(r)),
            _ => None,
        }
    }

    /// Owned, `'static` clone of the awkward leaf. See [`AnyAdapter::as_array_arc`].
    #[inline]
    pub fn as_awkward_arc(&self) -> Option<Arc<dyn AwkwardAdapterRead>> {
        match self {
            Self::Awkward(a) => Some(Arc::clone(a)),
            _ => None,
        }
    }
}
