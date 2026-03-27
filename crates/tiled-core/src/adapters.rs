//! Adapter trait hierarchy for the five data structure families.
//!
//! Corresponds to `tiled/adapters/protocols.py`.
//!
//! Traits used as `dyn` trait objects use explicit `Pin<Box<dyn Future>>` returns
//! instead of `#[async_trait]` to eliminate the proc-macro dependency.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

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

    fn read<'a>(
        &'a self,
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>>;

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>>;
}

pub trait ArrayAdapterWrite: ArrayAdapterRead {
    fn write_block<'a>(
        &'a self,
        data: DynNDArray,
        block: &'a [usize],
    ) -> BoxFuture<'a, Result<()>>;
}

// ---------------------------------------------------------------------------
// Table (dyn-used)
// ---------------------------------------------------------------------------

pub trait TableAdapterRead: BaseAdapter {
    fn structure(&self) -> &TableStructure;

    fn read<'a>(
        &'a self,
        fields: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<ArrowTable>>;

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

    fn read<'a>(
        &'a self,
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<SparseData>>;

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
    ) -> BoxFuture<'a, Result<SparseData>>;
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
    fn search(&self, _queries: &[crate::queries::Query]) -> Vec<String> {
        self.keys()
    }
}

// ---------------------------------------------------------------------------
// AnyAdapter — type-erased adapter enum
// ---------------------------------------------------------------------------

pub enum AnyAdapter {
    Array(Box<dyn ArrayAdapterRead>),
    Table(Box<dyn TableAdapterRead>),
    Sparse(Box<dyn SparseAdapterRead>),
    Awkward(Box<dyn AwkwardAdapterRead>),
    Container(Box<dyn ContainerAdapter>),
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
}
