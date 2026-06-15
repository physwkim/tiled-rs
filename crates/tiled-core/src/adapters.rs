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
use crate::error::Result;
use crate::ndslice::NDSlice;
use crate::schemas::SortDirection;
use crate::structures::{
    ArrayStructure, AwkwardStructure, ContainerStructure, SparseStructure, Spec, StructureFamily,
    TableStructure,
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
}

pub trait TableAdapterWrite: TableAdapterRead {
    fn write_partition<'a>(
        &'a self,
        data: ArrowTable,
        partition: usize,
    ) -> BoxFuture<'a, Result<()>>;
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
}

pub trait AwkwardAdapterWrite: AwkwardAdapterRead {
    fn write(
        &self,
        buffers: HashMap<String, bytes::Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
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

    /// Paginated search: return the `[offset, offset+limit)` window of
    /// children matching `queries` (sorted by `sorting`) as [`SearchEntry`]
    /// rows, plus the **total** match count (not the page size). This is the
    /// single listing primitive the server's search endpoint drives — both
    /// the catalog and in-memory trees flow through it, so there is no
    /// separate "direct SQL" path.
    ///
    /// The default impl is the in-memory one: it runs [`search`](Self::search)
    /// (which enforces the adapter's query-support matrix → HTTP 400 on an
    /// unevaluable variant), pages the matched keys, and builds each row from
    /// the child adapter. `sorting` is ignored here — an in-memory tree
    /// preserves insertion order. The SQL catalog overrides this to push
    /// filter + sort + LIMIT/OFFSET down to the database and to carry each
    /// node's `access_blob` and `data_source` structure without resolving the
    /// leaf adapter.
    fn search_page<'a>(
        &'a self,
        queries: &'a [crate::queries::Query],
        _sorting: &'a [(String, SortDirection)],
        offset: usize,
        limit: usize,
    ) -> BoxFuture<'a, Result<(Vec<SearchEntry>, usize)>> {
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
            Ok((entries, total))
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
}
