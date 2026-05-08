//! `Dataset` — Rust analogue of `xarray.Dataset`, used by `xarray_client` and
//! `composite_client`.
//!
//! Python builds an `xarray.Dataset` directly. Rust has no `xarray` so we
//! materialise the same shape: a map of named coordinates + a map of named
//! data variables + free-form attrs. Each variable is an Arrow `RecordBatch`
//! column or a raw array buffer keyed on a `DType`.

use std::collections::HashMap;

use bytes::Bytes;
use tiled_core::dtype::DType;

/// One labelled array in a `Dataset`.
#[derive(Debug, Clone)]
pub struct Variable {
    pub dims: Vec<String>,
    pub shape: Vec<usize>,
    pub dtype: DType,
    pub data: Bytes,
    pub attrs: serde_json::Value,
}

impl Variable {
    fn ensure_dtype(&self, kind: tiled_core::dtype::Kind, itemsize: usize) -> Option<&[u8]> {
        // Tiled servers consistently emit little-endian array buffers.
        match &self.dtype {
            DType::Builtin(b)
                if b.kind == kind
                    && b.itemsize == itemsize
                    && b.endianness == tiled_core::dtype::Endianness::Little =>
            {
                let chunks = self.data.chunks_exact(itemsize);
                if !chunks.remainder().is_empty() {
                    None
                } else {
                    Some(self.data.as_ref())
                }
            }
            _ => None,
        }
    }

    /// Decode as `Vec<f64>` if the dtype is little-endian f64.
    pub fn as_f64_vec(&self) -> Option<Vec<f64>> {
        let bytes = self.ensure_dtype(tiled_core::dtype::Kind::Float, 8)?;
        Some(
            bytes
                .chunks_exact(8)
                .map(|c| f64::from_le_bytes(c.try_into().expect("chunks_exact")))
                .collect(),
        )
    }

    /// Decode as `Vec<f32>` if the dtype is little-endian f32.
    pub fn as_f32_vec(&self) -> Option<Vec<f32>> {
        let bytes = self.ensure_dtype(tiled_core::dtype::Kind::Float, 4)?;
        Some(
            bytes
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes(c.try_into().expect("chunks_exact")))
                .collect(),
        )
    }

    pub fn as_i64_vec(&self) -> Option<Vec<i64>> {
        let bytes = self.ensure_dtype(tiled_core::dtype::Kind::Integer, 8)?;
        Some(
            bytes
                .chunks_exact(8)
                .map(|c| i64::from_le_bytes(c.try_into().expect("chunks_exact")))
                .collect(),
        )
    }

    pub fn as_i32_vec(&self) -> Option<Vec<i32>> {
        let bytes = self.ensure_dtype(tiled_core::dtype::Kind::Integer, 4)?;
        Some(
            bytes
                .chunks_exact(4)
                .map(|c| i32::from_le_bytes(c.try_into().expect("chunks_exact")))
                .collect(),
        )
    }

    pub fn as_u64_vec(&self) -> Option<Vec<u64>> {
        let bytes = self.ensure_dtype(tiled_core::dtype::Kind::UnsignedInteger, 8)?;
        Some(
            bytes
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact")))
                .collect(),
        )
    }

    pub fn as_u32_vec(&self) -> Option<Vec<u32>> {
        let bytes = self.ensure_dtype(tiled_core::dtype::Kind::UnsignedInteger, 4)?;
        Some(
            bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().expect("chunks_exact")))
                .collect(),
        )
    }
}

/// Lightweight `xarray.Dataset` analogue.
#[derive(Debug, Clone, Default)]
pub struct Dataset {
    pub coords: HashMap<String, Variable>,
    pub data_vars: HashMap<String, Variable>,
    pub attrs: serde_json::Value,
}

impl Dataset {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_coord(&mut self, name: impl Into<String>, var: Variable) {
        self.coords.insert(name.into(), var);
    }

    pub fn insert_data_var(&mut self, name: impl Into<String>, var: Variable) {
        self.data_vars.insert(name.into(), var);
    }
}
