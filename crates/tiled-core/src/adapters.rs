//! Adapter trait hierarchy for the five data structure families.
//!
//! Corresponds to `tiled/adapters/protocols.py`.

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
pub trait BaseAdapter: Send + Sync {
    fn structure_family(&self) -> StructureFamily;
    fn metadata(&self) -> &serde_json::Value;
    fn specs(&self) -> &[Spec];
}

// ---------------------------------------------------------------------------
// Array
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ArrayAdapterRead: BaseAdapter {
    fn structure(&self) -> &ArrayStructure;
    async fn read(&self, slice: &NDSlice) -> Result<DynNDArray>;
    async fn read_block(&self, block: &[usize], slice: &NDSlice) -> Result<DynNDArray>;
}

#[async_trait]
pub trait ArrayAdapterWrite: ArrayAdapterRead {
    async fn write_block(&self, data: DynNDArray, block: &[usize]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

#[async_trait]
pub trait TableAdapterRead: BaseAdapter {
    fn structure(&self) -> &TableStructure;
    async fn read(&self, fields: Option<&[String]>) -> Result<ArrowTable>;
    async fn read_partition(
        &self,
        partition: usize,
        fields: Option<&[String]>,
    ) -> Result<ArrowTable>;
}

#[async_trait]
pub trait TableAdapterWrite: TableAdapterRead {
    async fn write_partition(&self, data: ArrowTable, partition: usize) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Sparse
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SparseData {
    pub coords: Vec<DynNDArray>,
    pub data: DynNDArray,
}

#[async_trait]
pub trait SparseAdapterRead: BaseAdapter {
    fn structure(&self) -> &SparseStructure;
    async fn read(&self, slice: &NDSlice) -> Result<SparseData>;
    async fn read_block(&self, block: &[usize]) -> Result<SparseData>;
}

// ---------------------------------------------------------------------------
// Awkward
// ---------------------------------------------------------------------------

#[async_trait]
pub trait AwkwardAdapterRead: BaseAdapter {
    fn structure(&self) -> &AwkwardStructure;
    async fn read(&self) -> Result<HashMap<String, bytes::Bytes>>;
    async fn read_buffers(
        &self,
        form_keys: Option<&[String]>,
    ) -> Result<HashMap<String, bytes::Bytes>>;
}

#[async_trait]
pub trait AwkwardAdapterWrite: AwkwardAdapterRead {
    async fn write(&self, buffers: HashMap<String, bytes::Bytes>) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Container
// ---------------------------------------------------------------------------

pub trait ContainerAdapter: BaseAdapter {
    fn structure(&self) -> &ContainerStructure;
    fn get(&self, key: &str) -> Option<&AnyAdapter>;
    fn keys(&self) -> Vec<String>;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
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

    /// Get the structure as a JSON value (type depends on family).
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
