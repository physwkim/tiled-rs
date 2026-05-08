//! `BaseClient` — fields and methods common to every node client.
//!
//! Mirrors `tiled/client/base.py::BaseClient`. The Python class is the parent
//! of `Container`, `ArrayClient`, `DataFrameClient`, etc., and stores:
//! - the `Context`,
//! - the `item` (the `data` payload from `/api/v1/metadata/.../{path}`),
//! - the parsed structure (`ArrayStructure`, `TableStructure`, ...),
//! - per-family helpers like `metadata`, `specs`, `uri`.

use tiled_core::schemas::{NodeAttributes, Resource};
use tiled_core::structures::{
    ArrayStructure, AwkwardStructure, ContainerStructure, SparseStructure, Spec,
    StructureFamily, TableStructure,
};

use crate::context::Context;
use crate::error::{ClientError, Result};

/// The `data` field of a `/metadata/.../<path>` response. We carry it whole so
/// the family-specific clients can reach for whatever attribute they need.
pub type Item = Resource<NodeAttributes>;

/// Parsed structure variant — one per family.
#[derive(Debug, Clone)]
pub enum ParsedStructure {
    Container(Option<ContainerStructure>),
    Array(ArrayStructure),
    Table(TableStructure),
    Sparse(SparseStructure),
    Awkward(AwkwardStructure),
}

impl ParsedStructure {
    pub fn from_item(item: &Item) -> Result<Self> {
        let attrs = &item.attributes;
        let family = attrs
            .structure_family
            .ok_or_else(|| ClientError::Invalid("item missing structure_family".into()))?;
        match family {
            StructureFamily::Container => {
                // Container structure is `{contents, count}` — we don't parse contents
                // upfront because it is server-set and may be lazy.
                Ok(Self::Container(None))
            }
            StructureFamily::Array => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("array missing structure".into()))?;
                Ok(Self::Array(ArrayStructure::from_json(s).map_err(
                    |e| ClientError::Invalid(format!("invalid array structure: {e}")),
                )?))
            }
            StructureFamily::Table => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("table missing structure".into()))?;
                Ok(Self::Table(TableStructure::from_json(s).map_err(
                    |e| ClientError::Invalid(format!("invalid table structure: {e}")),
                )?))
            }
            StructureFamily::Sparse => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("sparse missing structure".into()))?;
                Ok(Self::Sparse(SparseStructure::from_json(s).map_err(
                    |e| ClientError::Invalid(format!("invalid sparse structure: {e}")),
                )?))
            }
            StructureFamily::Awkward => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("awkward missing structure".into()))?;
                Ok(Self::Awkward(AwkwardStructure::from_json(s).map_err(
                    |e| ClientError::Invalid(format!("invalid awkward structure: {e}")),
                )?))
            }
        }
    }
}

/// Common state for any client — held by family-specific clients.
#[derive(Debug, Clone)]
pub struct BaseClient {
    pub(crate) context: Context,
    pub(crate) item: Item,
    pub(crate) structure: ParsedStructure,
    pub(crate) include_data_sources: bool,
}

impl BaseClient {
    pub fn new(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let structure = ParsedStructure::from_item(&item)?;
        Ok(Self {
            context,
            item,
            structure,
            include_data_sources,
        })
    }

    /// The HTTP context.
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// The full item payload (id + attributes + links).
    pub fn item(&self) -> &Item {
        &self.item
    }

    /// The node's id (last path segment).
    pub fn id(&self) -> &str {
        &self.item.id
    }

    /// User metadata. Returns `&Value::Null` if the server omitted it.
    pub fn metadata(&self) -> &serde_json::Value {
        const NULL: serde_json::Value = serde_json::Value::Null;
        self.item.attributes.metadata.as_ref().unwrap_or(&NULL)
    }

    /// Specs the node conforms to.
    pub fn specs(&self) -> &[Spec] {
        self.item
            .attributes
            .specs
            .as_deref()
            .unwrap_or(&[])
    }

    /// Path ancestors (e.g. `["root", "subgroup"]`).
    pub fn ancestors(&self) -> &[String] {
        &self.item.attributes.ancestors
    }

    /// Family of the underlying structure.
    pub fn structure_family(&self) -> Option<StructureFamily> {
        self.item.attributes.structure_family
    }

    /// Parsed structure dataclass.
    pub fn structure(&self) -> &ParsedStructure {
        &self.structure
    }

    /// `self` link as a fully-qualified URL string.
    pub fn uri(&self) -> Option<&str> {
        self.item.links.self_link.as_deref()
    }

    /// Look up a link by name (`self`, `search`, `full`, `block`, `partition`,
    /// `buffers`).
    pub fn link(&self, name: &str) -> Option<&str> {
        match name {
            "self" => self.item.links.self_link.as_deref(),
            "search" => self.item.links.search.as_deref(),
            "full" => self.item.links.full.as_deref(),
            other => self.item.links.extra.get(other).map(String::as_str),
        }
    }

    /// Helper: required link, or `MissingLink` error.
    pub(crate) fn require_link(&self, name: &str) -> Result<&str> {
        self.link(name)
            .ok_or_else(|| ClientError::MissingLink(name.to_string()))
    }
}
