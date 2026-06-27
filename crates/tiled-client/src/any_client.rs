//! `AnyClient` — sum type that holds whichever family-specific client the
//! server returned.
//!
//! Mirrors `tiled.client.utils.client_for_item`: given a fresh `Item`, dispatch
//! to the matching family. Callers `match` on it to drill in.

use std::any::Any;
use std::sync::Arc;

use tiled_core::structures::StructureFamily;

use crate::array::ArrayClient;
use crate::awkward::AwkwardClient;
use crate::base::{BaseClient, Item};
use crate::container::ContainerClient;
use crate::context::Context;
use crate::dataframe::TableClient;
use crate::error::{ClientError, Result};
use crate::ragged::RaggedClient;
use crate::sparse::SparseClient;

/// User hook for substituting custom client types based on `Spec`s the node
/// carries.
///
/// Mirrors the Python `structure_clients` dispatch table. The resolver is
/// consulted *before* the built-in family dispatch, so callers can intercept
/// a `Spec("xarray_dataset")` node and return a `DatasetClient` instead of
/// the plain `ContainerClient` the family would otherwise yield.
///
/// Implementations return `None` to defer to the built-in dispatch.
pub trait ClientResolver: Send + Sync + std::fmt::Debug {
    fn resolve(
        &self,
        ctx: &Context,
        item: &Item,
        include_data_sources: bool,
    ) -> Option<Result<Arc<dyn Any + Send + Sync>>>;
}

/// One client per node, regardless of family. The `Custom` variant lets a
/// [`ClientResolver`] return arbitrary types (e.g. a `DatasetClient`); use
/// [`AnyClient::downcast`] / [`AnyClient::as_custom`] to retrieve the
/// concrete type.
///
/// `Clone` is supported on every variant — `Custom` clones the inner `Arc`.
/// `#[non_exhaustive]` so adding new variants is not a breaking change for
/// downstream `match` callers.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AnyClient {
    Container(ContainerClient),
    Array(ArrayClient),
    Table(TableClient),
    Sparse(SparseClient),
    Awkward(AwkwardClient),
    Ragged(RaggedClient),
    /// Custom client emitted by a [`ClientResolver`].
    Custom(Arc<dyn Any + Send + Sync>),
}

impl AnyClient {
    /// Dispatch on `structure_family` to build the right client. If the
    /// context has a [`ClientResolver`] configured, it is consulted first.
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        if let Some(resolver) = context.resolver()
            && let Some(custom) = resolver.resolve(&context, &item, include_data_sources)
        {
            return Ok(Self::Custom(custom?));
        }
        let family = item
            .attributes
            .structure_family
            .ok_or_else(|| ClientError::Invalid("item missing structure_family".into()))?;
        match family {
            StructureFamily::Container => Ok(Self::Container(ContainerClient::from_item(
                context,
                item,
                include_data_sources,
            )?)),
            StructureFamily::Array => Ok(Self::Array(ArrayClient::from_item(
                context,
                item,
                include_data_sources,
            )?)),
            StructureFamily::Table => Ok(Self::Table(TableClient::from_item(
                context,
                item,
                include_data_sources,
            )?)),
            StructureFamily::Sparse => Ok(Self::Sparse(SparseClient::from_item(
                context,
                item,
                include_data_sources,
            )?)),
            StructureFamily::Awkward => Ok(Self::Awkward(AwkwardClient::from_item(
                context,
                item,
                include_data_sources,
            )?)),
            StructureFamily::Ragged => Ok(Self::Ragged(RaggedClient::from_item(
                context,
                item,
                include_data_sources,
            )?)),
        }
    }

    pub fn structure_family(&self) -> StructureFamily {
        match self {
            Self::Container(_) => StructureFamily::Container,
            Self::Array(_) => StructureFamily::Array,
            Self::Table(_) => StructureFamily::Table,
            Self::Sparse(_) => StructureFamily::Sparse,
            Self::Awkward(_) => StructureFamily::Awkward,
            Self::Ragged(_) => StructureFamily::Ragged,
            // A custom client has no canonical family on the wire — a resolver
            // chose its own type. We default to Container for repr purposes.
            Self::Custom(_) => StructureFamily::Container,
        }
    }

    /// Borrow the inner custom client (no-op for the built-in variants).
    pub fn as_custom<T: Any>(&self) -> Option<&T> {
        match self {
            Self::Custom(a) => (&**a as &(dyn Any + 'static)).downcast_ref::<T>(),
            _ => None,
        }
    }

    /// Move out the inner custom client. Returns `Err` with the original
    /// `AnyClient` if the variant or downcast doesn't match.
    // Mirrors `Arc::downcast`, which hands the original value back on failure;
    // boxing the error would force every caller through a `*` deref for no gain.
    #[allow(clippy::result_large_err)]
    pub fn downcast<T: Any + Send + Sync>(self) -> std::result::Result<Arc<T>, Self> {
        match self {
            Self::Custom(a) => match Arc::downcast::<T>(a) {
                Ok(t) => Ok(t),
                Err(a) => Err(Self::Custom(a)),
            },
            other => Err(other),
        }
    }

    /// Borrow the `BaseClient` underlying this node, for metadata access and
    /// write operations (`delete`, `patch_metadata`) common to all families.
    /// Returns `None` only for the `Custom` variant.
    pub fn base(&self) -> Option<&BaseClient> {
        match self {
            Self::Container(c) => Some(c.base()),
            Self::Array(a) => Some(a.base()),
            Self::Table(t) => Some(t.base()),
            Self::Sparse(s) => Some(s.base()),
            Self::Awkward(a) => Some(a.base()),
            Self::Ragged(r) => Some(r.base()),
            Self::Custom(_) => None,
        }
    }

    pub fn as_container(&self) -> Option<&ContainerClient> {
        match self {
            Self::Container(c) => Some(c),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&ArrayClient> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_table(&self) -> Option<&TableClient> {
        match self {
            Self::Table(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_sparse(&self) -> Option<&SparseClient> {
        match self {
            Self::Sparse(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_awkward(&self) -> Option<&AwkwardClient> {
        match self {
            Self::Awkward(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_ragged(&self) -> Option<&RaggedClient> {
        match self {
            Self::Ragged(r) => Some(r),
            _ => None,
        }
    }

    pub fn into_container(self) -> Result<ContainerClient> {
        match self {
            Self::Container(c) => Ok(c),
            other => Err(ClientError::StructureMismatch {
                expected: "container".into(),
                got: other.structure_family().to_string(),
            }),
        }
    }

    pub fn into_array(self) -> Result<ArrayClient> {
        match self {
            Self::Array(a) => Ok(a),
            other => Err(ClientError::StructureMismatch {
                expected: "array".into(),
                got: other.structure_family().to_string(),
            }),
        }
    }

    pub fn into_table(self) -> Result<TableClient> {
        match self {
            Self::Table(t) => Ok(t),
            other => Err(ClientError::StructureMismatch {
                expected: "table".into(),
                got: other.structure_family().to_string(),
            }),
        }
    }

    pub fn into_ragged(self) -> Result<RaggedClient> {
        match self {
            Self::Ragged(r) => Ok(r),
            other => Err(ClientError::StructureMismatch {
                expected: "ragged".into(),
                got: other.structure_family().to_string(),
            }),
        }
    }
}
