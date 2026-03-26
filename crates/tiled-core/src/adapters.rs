//! Adapter trait hierarchy for the five data structure families.
//!
//! Corresponds to `tiled/adapters/protocols.py`.
//!
//! These traits define the interface that data format adapters (HDF5, Zarr, CSV, etc.)
//! must implement. The server calls these methods to read data on behalf of clients.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::dtype::{ArrowTable, DynNDArray};
use crate::error::Result;
use crate::ndslice::NDSlice;
use crate::structures::{
    ArrayStructure, AwkwardStructure, ContainerStructure, SparseStructure, Spec, StructureFamily,
    TableStructure,
};

/// Base trait that all adapters must implement.
///
/// Provides metadata and spec information common to all data families.
pub trait BaseAdapter: Send + Sync {
    /// Which structure family this adapter serves.
    fn structure_family(&self) -> StructureFamily;

    /// User-supplied metadata (arbitrary JSON).
    fn metadata(&self) -> &serde_json::Value;

    /// List of specs this node conforms to.
    fn specs(&self) -> &[Spec];
}

// ---------------------------------------------------------------------------
// Array
// ---------------------------------------------------------------------------

/// Read interface for N-dimensional array data.
#[async_trait]
pub trait ArrayAdapterRead: BaseAdapter {
    /// Structural description (dtype, shape, chunks, dims).
    fn structure(&self) -> &ArrayStructure;

    /// Read a slice of the full array.
    async fn read(&self, slice: &NDSlice) -> Result<DynNDArray>;

    /// Read a single chunk identified by block indices.
    async fn read_block(&self, block: &[usize], slice: &NDSlice) -> Result<DynNDArray>;
}

/// Write interface for N-dimensional array data.
#[async_trait]
pub trait ArrayAdapterWrite: ArrayAdapterRead {
    /// Write a block of data.
    async fn write_block(&self, data: DynNDArray, block: &[usize]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

/// Read interface for tabular data.
#[async_trait]
pub trait TableAdapterRead: BaseAdapter {
    /// Structural description (Arrow schema, npartitions, columns).
    fn structure(&self) -> &TableStructure;

    /// Read the full table (optionally selecting a subset of fields).
    async fn read(&self, fields: Option<&[String]>) -> Result<ArrowTable>;

    /// Read a single partition (optionally selecting a subset of fields).
    async fn read_partition(
        &self,
        partition: usize,
        fields: Option<&[String]>,
    ) -> Result<ArrowTable>;
}

/// Write interface for tabular data.
#[async_trait]
pub trait TableAdapterWrite: TableAdapterRead {
    /// Write (append) a partition of data.
    async fn write_partition(&self, data: ArrowTable, partition: usize) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Sparse
// ---------------------------------------------------------------------------

/// COO sparse array data returned by sparse adapters.
#[derive(Debug, Clone)]
pub struct SparseData {
    /// Coordinate arrays — one `DynNDArray` per dimension.
    pub coords: Vec<DynNDArray>,
    /// Value array.
    pub data: DynNDArray,
}

/// Read interface for sparse array data.
#[async_trait]
pub trait SparseAdapterRead: BaseAdapter {
    /// Structural description (shape, chunks, layout).
    fn structure(&self) -> &SparseStructure;

    /// Read a slice of the sparse array.
    async fn read(&self, slice: &NDSlice) -> Result<SparseData>;

    /// Read a single chunk.
    async fn read_block(&self, block: &[usize]) -> Result<SparseData>;
}

// ---------------------------------------------------------------------------
// Awkward
// ---------------------------------------------------------------------------

/// Read interface for Awkward Array data.
#[async_trait]
pub trait AwkwardAdapterRead: BaseAdapter {
    /// Structural description (length, form).
    fn structure(&self) -> &AwkwardStructure;

    /// Read the full awkward array (as raw buffers).
    async fn read(&self) -> Result<HashMap<String, bytes::Bytes>>;

    /// Read only the specified form key buffers.
    async fn read_buffers(
        &self,
        form_keys: Option<&[String]>,
    ) -> Result<HashMap<String, bytes::Bytes>>;
}

/// Write interface for Awkward Array data.
#[async_trait]
pub trait AwkwardAdapterWrite: AwkwardAdapterRead {
    /// Write awkward array buffers.
    async fn write(&self, buffers: HashMap<String, bytes::Bytes>) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Container
// ---------------------------------------------------------------------------

/// Interface for container (group/directory) adapters.
///
/// A container holds named children that may themselves be any adapter type.
pub trait ContainerAdapter: BaseAdapter {
    /// Structural description (list of child keys).
    fn structure(&self) -> &ContainerStructure;

    /// Look up a child adapter by key.
    fn get(&self, key: &str) -> Option<&AnyAdapter>;

    /// Iterate over all child keys.
    fn keys(&self) -> Vec<String>;

    /// Number of children.
    fn len(&self) -> usize;

    /// Whether the container is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// AnyAdapter — type-erased adapter enum
// ---------------------------------------------------------------------------

/// Type-erased wrapper for any adapter variant.
///
/// Maps to Python `AnyAdapter = Union[ArrayAdapter, AwkwardAdapter, ...]`.
pub enum AnyAdapter {
    Array(Box<dyn ArrayAdapterRead>),
    Table(Box<dyn TableAdapterRead>),
    Sparse(Box<dyn SparseAdapterRead>),
    Awkward(Box<dyn AwkwardAdapterRead>),
    Container(Box<dyn ContainerAdapter>),
}

impl AnyAdapter {
    pub fn structure_family(&self) -> StructureFamily {
        match self {
            Self::Array(a) => a.structure_family(),
            Self::Table(a) => a.structure_family(),
            Self::Sparse(a) => a.structure_family(),
            Self::Awkward(a) => a.structure_family(),
            Self::Container(a) => a.structure_family(),
        }
    }

    pub fn metadata(&self) -> &serde_json::Value {
        match self {
            Self::Array(a) => a.metadata(),
            Self::Table(a) => a.metadata(),
            Self::Sparse(a) => a.metadata(),
            Self::Awkward(a) => a.metadata(),
            Self::Container(a) => a.metadata(),
        }
    }

    pub fn specs(&self) -> &[Spec] {
        match self {
            Self::Array(a) => a.specs(),
            Self::Table(a) => a.specs(),
            Self::Sparse(a) => a.specs(),
            Self::Awkward(a) => a.specs(),
            Self::Container(a) => a.specs(),
        }
    }

    /// Get the structure as a JSON value (type depends on family).
    pub fn structure_json(&self) -> Option<serde_json::Value> {
        match self {
            Self::Array(a) => serde_json::to_value(a.structure()).ok(),
            Self::Table(t) => serde_json::to_value(t.structure()).ok(),
            Self::Sparse(s) => serde_json::to_value(s.structure()).ok(),
            Self::Awkward(a) => serde_json::to_value(a.structure()).ok(),
            Self::Container(c) => {
                let count = c.len();
                serde_json::to_value(serde_json::json!({"contents": null, "count": count})).ok()
            }
        }
    }

    /// Get the number of children (for containers only).
    pub fn container_len(&self) -> Option<usize> {
        match self {
            Self::Container(c) => Some(c.len()),
            _ => None,
        }
    }
}
