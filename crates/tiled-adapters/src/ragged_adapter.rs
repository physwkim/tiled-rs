//! In-memory ragged (variable-length row) array adapter.
//!
//! Corresponds to Python `RaggedAdapter` (`tiled/adapters/ragged.py:39-78`).
//! The full adapter trait (`RaggedAdapterRead`) is defined here because
//! tiled-core does not yet expose one; a future tiled-core edit will hoist it
//! there alongside `AnyAdapter::Ragged`.

use bytes::Bytes;

use tiled_core::adapters::{BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{RaggedStructure, Resizable, Spec, StructureFamily};

// ---------------------------------------------------------------------------
// RaggedData — data returned by read()
// ---------------------------------------------------------------------------

/// Data returned by [`RaggedAdapterRead::read`].
///
/// `json_value` is a JSON-encoded list-of-lists, matching Python's
/// `array.tolist()` (`tiled/adapters/ragged.py:73`).  `structure` is
/// included so serializers that need buffer-level detail (e.g. the ZIP
/// serializer) can compute the Awkward form without re-parsing the shape.
#[derive(Debug, Clone)]
pub struct RaggedData {
    /// JSON list-of-lists, e.g. `[[1.0, 2.0], [3.0]]`.
    pub json_value: serde_json::Value,
    /// Structural description: shape, dtype, chunks.
    pub structure: RaggedStructure,
}

impl RaggedData {
    /// Serialize `json_value` to raw bytes (UTF-8 JSON).
    ///
    /// The bytes are what the JSON and ZIP serializers in
    /// `tiled-serialization` consume as their `&[u8]` data argument.
    pub fn to_json_bytes(&self) -> std::result::Result<Bytes, serde_json::Error> {
        serde_json::to_vec(&self.json_value).map(Bytes::from)
    }

    /// Serialize `structure` to a `serde_json::Value` for use as the
    /// metadata argument to the ragged serializers.
    pub fn structure_as_metadata(
        &self,
    ) -> std::result::Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(&self.structure)
    }
}

// ---------------------------------------------------------------------------
// RaggedAdapterRead trait
// ---------------------------------------------------------------------------

/// Trait for adapters that serve ragged (variable-length row) arrays.
///
/// Mirrors the existing per-family adapter traits in `tiled-core::adapters`
/// (`ArrayAdapterRead`, `AwkwardAdapterRead`, etc.).  Defined here until
/// tiled-core exposes a `RaggedAdapterRead` and `AnyAdapter::Ragged`.
pub trait RaggedAdapterRead: BaseAdapter {
    fn structure(&self) -> &RaggedStructure;

    /// Read the whole array (or a slice of it) as [`RaggedData`].
    ///
    /// Slice support is deferred: the in-memory adapter ignores `slice`
    /// and always returns the full array, matching Python's simple
    /// `RaggedAdapter.read` which calls `make_ragged_array(self._array,
    /// slice=slice)` — the slice logic lives in `make_ragged_array`, not in
    /// the adapter itself.
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<RaggedData>>;
}

// ---------------------------------------------------------------------------
// RaggedAdapter — in-memory implementation
// ---------------------------------------------------------------------------

/// In-memory ragged adapter.
///
/// Stores rows as a `serde_json::Value` (JSON array of arrays).  Float64 is
/// the primary concrete constructor; arbitrary JSON data and an explicit
/// [`RaggedStructure`] can also be supplied via [`RaggedAdapter::new`].
///
/// Corresponds to Python `RaggedAdapter` (`tiled/adapters/ragged.py:39`).
pub struct RaggedAdapter {
    data: serde_json::Value,
    structure: RaggedStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl RaggedAdapter {
    /// Build an adapter from `float64` rows (produces shape `[N, None]`).
    ///
    /// Each inner `Vec<f64>` becomes one variable-length row.  The resulting
    /// [`RaggedStructure`] has:
    /// * `shape = [n_rows, None]`
    /// * `size = total element count`
    /// * `chunks = [[n_rows], None]` (single chunk along axis 0)
    /// * `data_type = little-endian float64`
    ///
    /// Corresponds to Python `RaggedAdapter.from_array` for a list-of-lists
    /// of floats (`tiled/adapters/ragged.py:57-71`).
    pub fn from_rows_f64(
        rows: Vec<Vec<f64>>,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Self {
        let n = rows.len();
        let size: usize = rows.iter().map(|r| r.len()).sum();
        let structure = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![Some(n), None],
            size,
            chunks: vec![Some(vec![n]), None],
            dims: None,
            resizable: Resizable::default(),
        };
        let json_rows: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|r| {
                serde_json::Value::Array(
                    r.into_iter()
                        .map(|v| {
                            serde_json::Number::from_f64(v)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        })
                        .collect(),
                )
            })
            .collect();
        Self {
            data: serde_json::Value::Array(json_rows),
            structure,
            metadata,
            specs,
        }
    }

    /// Build an adapter from an explicit JSON list-of-lists and structure.
    ///
    /// Use this when the dtype is not float64 or the structure is pre-computed.
    pub fn new(
        data: serde_json::Value,
        structure: RaggedStructure,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Self {
        Self {
            data,
            structure,
            metadata,
            specs,
        }
    }
}

impl BaseAdapter for RaggedAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Ragged
    }

    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl RaggedAdapterRead for RaggedAdapter {
    fn structure(&self) -> &RaggedStructure {
        &self.structure
    }

    fn read<'a>(&'a self, _slice: &'a NDSlice) -> BoxFuture<'a, Result<RaggedData>> {
        let json_value = self.data.clone();
        let structure = self.structure.clone();
        Box::pin(async move {
            Ok(RaggedData {
                json_value,
                structure,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Validation helper
// ---------------------------------------------------------------------------

/// Validate a `RaggedStructure` against its declared constraints.
///
/// Returns `Err` if any invariant from `RaggedStructure.__post_init__` is
/// violated (corresponds to `tiled/structures/ragged.py:79-98`).
pub fn validate_ragged_structure(s: &RaggedStructure) -> Result<()> {
    if s.shape.is_empty() {
        return Err(TiledError::Validation(
            "ragged array must have at least one dimension".into(),
        ));
    }
    if s.chunks.is_empty() {
        return Err(TiledError::Validation(
            "ragged array must have at least one chunk dimension".into(),
        ));
    }
    if s.shape[0].is_none() {
        return Err(TiledError::Validation(
            "first dimension of a ragged array must be a known integer".into(),
        ));
    }
    if s.chunks[0].is_none() {
        return Err(TiledError::Validation(
            "first chunks dimension must be a known integer partitioning".into(),
        ));
    }
    if s.shape.len() != s.chunks.len() {
        return Err(TiledError::Validation(
            "shape and chunks must have the same number of dimensions".into(),
        ));
    }
    for v in s.chunks.iter().skip(1).flatten() {
        if v.len() != 1 {
            return Err(TiledError::Validation(
                "only the first dimension can be partitioned into chunks".into(),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tiled_core::ndslice::NDSlice;

    // ------------------------------------------------------------------
    // ragged_adapter_structure_from_rows_f64
    // Boundary: N=0 (empty) and N>0 (non-empty)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn ragged_adapter_structure_from_rows_f64_nonempty() {
        let rows = vec![vec![1.0_f64, 2.0, 3.0], vec![4.0], vec![5.0, 6.0]];
        let adapter = RaggedAdapter::from_rows_f64(rows, serde_json::json!({}), vec![]);

        assert_eq!(adapter.structure_family(), StructureFamily::Ragged);
        assert_eq!(adapter.structure().shape, vec![Some(3), None]);
        assert_eq!(adapter.structure().size, 6);
        assert_eq!(adapter.structure().chunks, vec![Some(vec![3]), None]);
        assert!(adapter.metadata().is_object());

        // Verify dtype: little-endian float64
        match &adapter.structure().data_type {
            DType::Builtin(b) => {
                assert_eq!(b.endianness, Endianness::Little);
                assert_eq!(b.kind, Kind::Float);
                assert_eq!(b.itemsize, 8);
            }
            DType::Struct(_) => panic!("expected builtin dtype"),
        }
    }

    #[tokio::test]
    async fn ragged_adapter_structure_from_rows_f64_empty() {
        let adapter = RaggedAdapter::from_rows_f64(vec![], serde_json::json!({}), vec![]);
        assert_eq!(adapter.structure().shape, vec![Some(0), None]);
        assert_eq!(adapter.structure().size, 0);
        assert_eq!(adapter.structure().chunks, vec![Some(vec![0]), None]);
    }

    // ------------------------------------------------------------------
    // ragged_adapter_read_returns_json
    // Boundary: correct list-of-lists JSON output
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn ragged_adapter_read_returns_json_list_of_lists() {
        let rows = vec![vec![1.0_f64, 2.0, 3.0], vec![4.0], vec![5.0, 6.0]];
        let adapter = RaggedAdapter::from_rows_f64(rows, serde_json::json!({}), vec![]);

        let slice = NDSlice::empty();
        let data = adapter.read(&slice).await.unwrap();

        let arr = data
            .json_value
            .as_array()
            .expect("json_value must be array");
        assert_eq!(arr.len(), 3);

        // Row 0: [1.0, 2.0, 3.0]
        let r0: Vec<f64> = arr[0]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(r0, vec![1.0, 2.0, 3.0]);

        // Row 1: [4.0]
        let r1: Vec<f64> = arr[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(r1, vec![4.0]);

        // Row 2: [5.0, 6.0]
        let r2: Vec<f64> = arr[2]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(r2, vec![5.0, 6.0]);
    }

    #[tokio::test]
    async fn ragged_adapter_read_json_bytes_round_trips() {
        let rows = vec![vec![1.0_f64, 2.0], vec![3.0, 4.0, 5.0]];
        let adapter = RaggedAdapter::from_rows_f64(rows, serde_json::json!({}), vec![]);

        let slice = NDSlice::empty();
        let data = adapter.read(&slice).await.unwrap();

        let bytes = data.to_json_bytes().expect("serialize to JSON bytes");
        let back: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, data.json_value);
    }

    // ------------------------------------------------------------------
    // validate_ragged_structure boundary cases
    // ------------------------------------------------------------------

    #[test]
    fn validate_ragged_structure_valid_passes() {
        let s = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![Some(3), None],
            size: 6,
            chunks: vec![Some(vec![3]), None],
            dims: None,
            resizable: Resizable::default(),
        };
        assert!(validate_ragged_structure(&s).is_ok());
    }

    #[test]
    fn validate_ragged_structure_empty_shape_fails() {
        let s = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![],
            size: 0,
            chunks: vec![],
            dims: None,
            resizable: Resizable::default(),
        };
        assert!(validate_ragged_structure(&s).is_err());
    }

    #[test]
    fn validate_ragged_structure_first_dim_null_fails() {
        let s = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![None],
            size: 0,
            chunks: vec![None],
            dims: None,
            resizable: Resizable::default(),
        };
        assert!(validate_ragged_structure(&s).is_err());
    }
}
