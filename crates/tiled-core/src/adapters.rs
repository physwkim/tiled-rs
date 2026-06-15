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
// Container (sync — no async methods)
// ---------------------------------------------------------------------------

pub trait ContainerAdapter: BaseAdapter {
    fn structure(&self) -> &ContainerStructure;
    fn get(&self, key: &str) -> Option<&AnyAdapter>;
    fn keys(&self) -> Vec<String>;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Search/filter children. Default: return all keys (no filtering).
    ///
    /// Returns `Err(UnsupportedQuery)` for a query variant this adapter's
    /// search path cannot evaluate, so the server can answer HTTP 400 instead
    /// of silently returning a filtered subset (parity with Python tiled's
    /// `UnsupportedQueryType`). The default impl supports no filtering, so it
    /// simply returns every key.
    // Note: `Result` here is `std::result::Result`, not the crate's
    // `error::Result<T>` alias (which fixes the error type to `TiledError`).
    fn search(
        &self,
        _queries: &[crate::queries::Query],
    ) -> std::result::Result<Vec<String>, crate::queries::UnsupportedQuery> {
        Ok(self.keys())
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

    pub fn structure_json(&self) -> Option<serde_json::Value> {
        match self {
            Self::Array(a) => serde_json::to_value(a.structure()).ok(),
            Self::Table(t) => serde_json::to_value(t.structure()).ok(),
            Self::Sparse(s) => serde_json::to_value(s.structure()).ok(),
            Self::Awkward(a) => serde_json::to_value(a.structure()).ok(),
            Self::Container(c) => Some(serde_json::json!({
                "contents": null,
                "count": c.len(),
            })),
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
