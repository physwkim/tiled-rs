//! `RaggedClient` — ragged-array node client (structure only).
//!
//! Mirrors the thin construction path of the array/awkward clients. The full
//! ragged read/write adapter (upstream feature #1104) is intentionally not
//! ported here; this client exists so a `ragged` node is representable
//! end-to-end (container listings, structure access) without panicking on the
//! `AnyClient` family dispatch.

use tiled_core::structures::RaggedStructure;

use crate::base::{BaseClient, Item, ParsedStructure};
use crate::context::Context;
use crate::error::{ClientError, Result};

#[derive(Debug, Clone)]
pub struct RaggedClient {
    base: BaseClient,
}

impl RaggedClient {
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let base = BaseClient::new(context, item, include_data_sources)?;
        if !matches!(base.structure(), ParsedStructure::Ragged(_)) {
            return Err(ClientError::StructureMismatch {
                expected: "ragged".into(),
                got: base
                    .structure_family()
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "unknown".into()),
            });
        }
        Ok(Self { base })
    }

    pub fn base(&self) -> &BaseClient {
        &self.base
    }

    pub fn structure(&self) -> &RaggedStructure {
        match self.base.structure() {
            ParsedStructure::Ragged(s) => s,
            _ => unreachable!("RaggedClient guards on construction"),
        }
    }
}
