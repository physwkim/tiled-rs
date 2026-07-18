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

use crate::core::dtype::{ArrowTable, DynNDArray, Endianness, Kind};
use crate::core::error::{Result, TiledError};
use crate::core::ndslice::NDSlice;
use crate::core::schemas::SortDirection;
use crate::core::structures::{
    ArrayStructure, AwkwardStructure, ContainerStructure, RaggedStructure, SparseStructure, Spec,
    StructureFamily, TableStructure,
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

/// `(shape, chunks)` of an array after an [`ArrayAdapterWrite::patch`]: the
/// per-axis extents and the regular chunk grid — exactly the two fields
/// Python's catalog adapter writes back to the structure row
/// (catalog/adapter.py:1664).
pub type ArrayShapeChunks = (Vec<usize>, Vec<Vec<usize>>);

pub trait ArrayAdapterWrite: ArrayAdapterRead {
    /// Overwrite the whole array. `data.shape` must equal the array's shape
    /// (`structure().shape`). `PUT /array/full` routes here.
    fn write<'a>(&'a self, data: DynNDArray) -> BoxFuture<'a, Result<()>>;

    /// Overwrite a single chunk addressed by `block` (one index per axis).
    /// `data.shape` must equal that chunk's shape — the per-axis lengths
    /// `structure().chunks[axis][block[axis]]`. `PUT /array/block` routes here.
    /// Distinct from [`ArrayAdapterWrite::write`]: this targets one chunk, not
    /// the whole array, so block and full writes no longer share a sentinel.
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
            Err(crate::core::error::TiledError::Validation(
                "append is not supported by this adapter".into(),
            ))
        })
    }

    /// Write `data` into the slice that starts at `offset`, optionally extending
    /// the array to fit, and return the array's `(shape, chunks)` afterwards.
    /// `PATCH /array/full` routes here. Mirrors Python `ZarrArrayAdapter.patch`
    /// (adapters/zarr.py:128-186): per axis the new extent is
    /// `max(current, data_len + offset)`; when that grows the array it is allowed
    /// only if `extend` is true, otherwise a [`TiledError::Conflict`] (HTTP 409)
    /// is returned. Only growth-capable stores (zarr) implement it; the default
    /// errors so the route shape is backend-independent.
    fn patch<'a>(
        &'a self,
        _data: DynNDArray,
        _offset: &'a [usize],
        _extend: bool,
    ) -> BoxFuture<'a, Result<ArrayShapeChunks>> {
        Box::pin(async move {
            Err(crate::core::error::TiledError::Validation(
                "patch is not supported by this adapter".into(),
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

    /// Optional downcast to a write-capable view, mirroring
    /// [`ArrayAdapterRead::as_writable`]. Adapters whose backing store can be
    /// written (managed tables under the server's writable storage) override
    /// this to return `Some(self)`; the rest leave it `None`, so a read-only
    /// table answers 405 rather than the write route silently not existing.
    fn as_table_writable(&self) -> Option<&dyn TableAdapterWrite> {
        None
    }
}

pub trait TableAdapterWrite: TableAdapterRead {
    /// Overwrite the whole table with `data`. `PUT /table/full` routes here.
    /// Kept distinct from `write_partition` so "full" never silently means
    /// "partition 0" (the dual meaning removed on the array side).
    fn write<'a>(&'a self, data: ArrowTable) -> BoxFuture<'a, Result<()>>;

    /// Overwrite a single partition. `PUT /table/partition` routes here.
    fn write_partition<'a>(
        &'a self,
        data: ArrowTable,
        partition: usize,
    ) -> BoxFuture<'a, Result<()>>;

    /// Append rows to a single partition. `PATCH /table/partition` routes here.
    /// Adapters that do not support row-level appends return a Validation error
    /// by default; override to enable.
    fn append_partition<'a>(
        &'a self,
        _data: ArrowTable,
        _partition: usize,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            Err(TiledError::Validation(
                "append_partition is not supported by this adapter".into(),
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Sparse (dyn-used)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SparseData {
    pub coords: Vec<DynNDArray>,
    pub data: DynNDArray,
}

impl SparseData {
    /// Materialize a rectangular region of this COO data as a dense,
    /// zero-filled, C-order buffer — the Rust equivalent of upstream's
    /// `sparse.COO.todense()` (`tiled/server/zarr.py`).
    ///
    /// `starts[d]` is the region's lower bound along dimension `d`, in the
    /// same coordinate frame as `self.coords`, and `shape[d]` is the region's
    /// extent. A non-zero is placed in the output only when its coordinate
    /// falls within `[starts[d], starts[d] + shape[d])` on every axis
    /// (remapped to `coord[d] - starts[d]`); non-zeros outside the region are
    /// dropped. Passing all-zero `starts` with `shape` equal to the full array
    /// shape densifies the whole array.
    ///
    /// Callers wanting a sub-region (e.g. one zarr chunk) should read the
    /// *whole* array first (`SparseAdapterRead::read(&NDSlice::empty())`,
    /// which every adapter assembles correctly into one global coordinate
    /// frame) and pass that region's `starts`/`shape` here, rather than
    /// relying on `SparseAdapterRead::read` with a non-trivial slice — not
    /// every adapter implements slice filtering (see
    /// `sparse_blocks_parquet_adapter.rs`'s `apply_sparse_slice`).
    ///
    /// Duplicate coordinates (multiple non-zeros at the same position) are
    /// last-write-wins, not summed; well-formed COO data has no duplicates.
    pub fn densify(&self, starts: &[usize], shape: &[usize]) -> DynNDArray {
        let dtype = self.data.dtype.clone();
        let esz = dtype.element_size();
        let ndim = shape.len();
        let total: usize = shape.iter().product();
        let mut out = vec![0u8; total * esz];

        // C-order element strides over the output region.
        let mut strides = vec![1usize; ndim];
        for d in (0..ndim.saturating_sub(1)).rev() {
            strides[d] = strides[d + 1] * shape[d + 1];
        }

        let nnz = self.data.len();
        'nz: for i in 0..nnz {
            let mut flat = 0usize;
            for d in 0..ndim {
                let local = read_coord_i64(&self.coords[d], i) - starts[d] as i64;
                if local < 0 || local as usize >= shape[d] {
                    continue 'nz;
                }
                flat += local as usize * strides[d];
            }
            let dst = flat * esz;
            let src = i * esz;
            out[dst..dst + esz].copy_from_slice(&self.data.data[src..src + esz]);
        }
        DynNDArray::new(bytes::Bytes::from(out), dtype, shape.to_vec())
    }
}

/// Decode element `i` of an integer-typed [`DynNDArray`] (a COO coordinate
/// column) as `i64`. Coordinate columns are always an integer kind — signed
/// or unsigned, 1/2/4/8 bytes, either endianness.
fn read_coord_i64(arr: &DynNDArray, i: usize) -> i64 {
    let esz = arr.dtype.element_size();
    let off = i * esz;
    let b = &arr.data[off..off + esz];
    let le = arr.dtype.endianness != Endianness::Big;
    macro_rules! rd {
        ($ty:ty) => {{
            let a: [u8; std::mem::size_of::<$ty>()] = b.try_into().unwrap();
            (if le {
                <$ty>::from_le_bytes(a)
            } else {
                <$ty>::from_be_bytes(a)
            }) as i64
        }};
    }
    match (arr.dtype.kind, esz) {
        (Kind::Integer, 1) => rd!(i8),
        (Kind::Integer, 2) => rd!(i16),
        (Kind::Integer, 4) => rd!(i32),
        (Kind::Integer, 8) => rd!(i64),
        (Kind::UnsignedInteger, 1) => rd!(u8),
        (Kind::UnsignedInteger, 2) => rd!(u16),
        (Kind::UnsignedInteger, 4) => rd!(u32),
        (Kind::UnsignedInteger, 8) => rd!(u64),
        other => unreachable!("sparse coordinate dtype must be an integer kind, got {other:?}"),
    }
}

pub trait SparseAdapterRead: BaseAdapter {
    fn structure(&self) -> &SparseStructure;

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<SparseData>>;

    fn read_block<'a>(&'a self, block: &'a [usize]) -> BoxFuture<'a, Result<SparseData>>;

    /// The write face of this adapter, or `None` if it is read-only.
    ///
    /// The in-memory `CooAdapter` and any externally-registered sparse node are
    /// read-only; only a managed parquet-backed sparse node under the server's
    /// writable storage returns `Some`. Mirrors [`AwkwardAdapterRead::as_writable`]
    /// and [`ArrayAdapterRead::as_writable`], so a read-only sparse node answers
    /// 405 rather than the write route silently not existing.
    fn as_writable(&self) -> Option<&dyn SparseAdapterWrite> {
        None
    }
}

/// Write face for sparse (COO) adapters backed by writable storage.
///
/// The Rust analog of Python's `SparseBlocksParquetAdapter.write` /
/// `write_block` (`tiled/adapters/sparse_blocks_parquet.py:91-109`), which
/// persist a COO DataFrame to per-block parquet files. `write` and `write_block`
/// are kept distinct — as on the array/table side — so "full" never silently
/// means "block 0": `write` overwrites the whole (single-block) array,
/// `write_block` targets one addressed block of a chunked array.
pub trait SparseAdapterWrite: SparseAdapterRead {
    /// Overwrite the whole sparse array with `data`. `PUT /array/full` routes
    /// here. Upstream `SparseBlocksParquetAdapter.write` supports only the
    /// single-block case (`NotImplementedError` for >1 block,
    /// `sparse_blocks_parquet.py:106-107`); adapters mirror that guard.
    ///
    /// `data.coords` are in the array's global coordinate frame; for a
    /// single-block array that frame equals the block-local one.
    fn write<'a>(&'a self, data: SparseData) -> BoxFuture<'a, Result<()>>;

    /// Overwrite a single block. `PUT /array/block` routes here. `data.coords`
    /// are in the *block-local* frame (the reference frame of that chunk),
    /// matching upstream `write_block` (`sparse_blocks_parquet.py:91-103`).
    fn write_block<'a>(&'a self, data: SparseData, block: &'a [usize])
    -> BoxFuture<'a, Result<()>>;
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

    fn as_writable(&self) -> Option<&dyn AwkwardAdapterWrite> {
        None
    }
}

pub trait AwkwardAdapterWrite: AwkwardAdapterRead {
    fn write(
        &self,
        buffers: HashMap<String, bytes::Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

// ---------------------------------------------------------------------------
// Ragged (dyn-used) — the Rust analog of Python's awkward family.
// ---------------------------------------------------------------------------

/// Data returned by [`RaggedAdapterRead::read`].
///
/// `json_value` is a JSON-encoded list-of-lists, matching Python's
/// `array.tolist()` (`tiled/adapters/ragged.py:73`). `structure` is included
/// so serializers that need buffer-level detail (the ZIP serializer) can
/// compute the Awkward form without re-parsing the shape.
///
/// Lives in tiled-core (not tiled-adapters) so [`AnyAdapter::Ragged`] can name
/// the trait that returns it; the concrete `RaggedAdapter` in tiled-adapters
/// implements [`RaggedAdapterRead`] over this type.
#[derive(Debug, Clone)]
pub struct RaggedData {
    /// JSON list-of-lists, e.g. `[[1.0, 2.0], [3.0]]`.
    pub json_value: serde_json::Value,
    /// Structural description: shape, dtype, chunks.
    pub structure: RaggedStructure,
}

impl RaggedData {
    /// Serialize `json_value` to raw bytes (UTF-8 JSON) — what the JSON and
    /// ZIP serializers in `tiled-serialization` consume as their `&[u8]` data
    /// argument.
    pub fn to_json_bytes(&self) -> std::result::Result<bytes::Bytes, serde_json::Error> {
        serde_json::to_vec(&self.json_value).map(bytes::Bytes::from)
    }

    /// Serialize `structure` to a `serde_json::Value` for use as the metadata
    /// argument to the ragged serializers.
    pub fn structure_as_metadata(
        &self,
    ) -> std::result::Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(&self.structure)
    }
}

/// Trait for adapters that serve ragged (variable-length row) arrays.
///
/// Mirrors the per-family adapter traits above (`ArrayAdapterRead`, etc.).
pub trait RaggedAdapterRead: BaseAdapter {
    fn structure(&self) -> &RaggedStructure;

    /// Read the whole array, or a slice of it, as [`RaggedData`].
    ///
    /// A non-full `slice` is applied with numpy/awkward *basic* indexing
    /// semantics, matching Python `RaggedAdapter.read`
    /// (`tiled/adapters/ragged.py:73-75`).
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<RaggedData>>;

    /// The write face of this adapter, or `None` if it is read-only.
    ///
    /// Python's in-memory `RaggedAdapter` has no write methods; only
    /// `RaggedSQLAdapter` does (`tiled/adapters/ragged.py`). Mirrors
    /// [`AwkwardAdapterRead::as_writable`].
    fn as_writable(&self) -> Option<&dyn RaggedAdapterWrite> {
        None
    }
}

/// Write face for ragged adapters backed by writable storage.
///
/// Mirrors Python `RaggedSQLAdapter`'s write/write_block/patch
/// (`tiled/adapters/ragged.py:249-352`). The data currency is a JSON
/// list-of-lists (`serde_json::Value`) — the same shape `RaggedData.json_value`
/// carries — so the adapter owns the JSON↔Awkward-buffer encoding internally
/// (it holds the structure that fixes the form/dtype), keeping this trait free
/// of any dependency on the buffer codec.
pub trait RaggedAdapterWrite: RaggedAdapterRead {
    /// Write the whole array as a single chunk at block 0. `PUT /ragged/full`
    /// routes here. Python `RaggedSQLAdapter.write` (ragged.py:249-250).
    fn write<'a>(&'a self, data: &'a serde_json::Value) -> BoxFuture<'a, Result<()>> {
        self.write_block(data, 0)
    }

    /// Write one chunk at `block` along axis 0. `PUT /ragged/block` routes here.
    /// A duplicate `block` is a [`TiledError::Conflict`]. Python
    /// `RaggedSQLAdapter.write_block` (ragged.py:252-275).
    fn write_block<'a>(
        &'a self,
        data: &'a serde_json::Value,
        block: usize,
    ) -> BoxFuture<'a, Result<()>>;

    /// Append a chunk, extending axis 0 (`extend=True`), and return the grown
    /// structure for the caller to persist. `PATCH /ragged/full` routes here.
    /// Only appending along the leftmost dimension is supported, and
    /// `extend=false` (overwrite) is rejected — Python
    /// `RaggedSQLAdapter.patch` (ragged.py:277-352).
    fn patch<'a>(
        &'a self,
        data: &'a serde_json::Value,
        offset: &'a [usize],
        extend: bool,
    ) -> BoxFuture<'a, Result<RaggedStructure>>;
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
    /// The node's data sources (with assets), populated **only** when the
    /// caller requested `include_data_sources` and the backing store can
    /// supply them. `None` means "not requested / not available" (omitted on
    /// the wire); `Some(vec![])` means "requested, but this node has none"
    /// (e.g. a container) — matching Python's `entry.data_sources` returning an
    /// empty list for such nodes (catalog/adapter.py:409-410). In-memory / Mongo
    /// trees leave this `None`.
    pub data_sources: Option<Vec<crate::core::data_source::DataSource>>,
}

/// One page of search results plus the totals a listing response needs.
///
/// `next_cursor` is the keyset cursor (a catalog node id) for the page that
/// follows this one. It is set **only** by adapters that do keyset pagination
/// (the SQL catalog, default sort) and only when more rows remain; `None`
/// means "no cursor available", so the server falls back to an offset `next`
/// link. In-memory / Mongo trees page by offset and always leave it `None`.
/// Mirrors the `(items, next_cursor)` pair Python's `keys_page`/`items_page`
/// return (tiled/catalog/adapter.py).
#[derive(Debug, Clone)]
pub struct SearchPage {
    pub entries: Vec<SearchEntry>,
    pub total: usize,
    pub next_cursor: Option<i64>,
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
        _queries: &'a [crate::core::queries::Query],
    ) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move { self.keys().await })
    }

    /// Paginated search: return a page of children matching `queries` (sorted
    /// by `sorting`) as [`SearchEntry`] rows, plus the **total** match count
    /// and the keyset cursor for the next page. This is the single listing
    /// primitive the server's search endpoint drives — both the catalog and
    /// in-memory trees flow through it, so there is no separate "direct SQL"
    /// path.
    ///
    /// Pagination is selected by the caller: a `cursor` of `Some(_)` requests
    /// the keyset page *after* that cursor; `None` requests the
    /// `[offset, offset+limit)` window. The returned
    /// [`SearchPage::next_cursor`] is `Some` only when the adapter did keyset
    /// pagination (SQL catalog, default sort) and more rows remain; the server
    /// uses it to emit a `page[cursor]` next link, falling back to a
    /// `page[offset]` link when it is `None`. Mirrors Python
    /// `keys_page`/`items_page`/`keys_range` (tiled/catalog/adapter.py).
    ///
    /// The default impl is the in-memory one: it runs [`search`](Self::search)
    /// (which enforces the adapter's query-support matrix → HTTP 400 on an
    /// unevaluable variant), orders the matched keys via
    /// [`sort_matched_keys`](Self::sort_matched_keys) (a no-op unless the adapter
    /// supports sorting), pages them by offset, and builds each row from the
    /// child adapter. `cursor` is ignored — an in-memory tree cannot keyset-page,
    /// so it always returns `next_cursor: None` (matching Python's offset slice
    /// for a sorted, non-`keys_page` tree). The SQL catalog overrides this to
    /// push filter + sort + keyset/OFFSET down to the database and to carry each
    /// node's `access_blob` and `data_source` structure without resolving the
    /// leaf adapter.
    ///
    /// `include_data_sources` asks each row to carry its `data_sources` list
    /// (with assets), for the `?include_data_sources=true` query param. Only a
    /// backend that tracks data sources (the SQL catalog) can honor it; the
    /// default in-memory impl has no such notion, so it ignores the flag and
    /// leaves every entry's `data_sources` at `None` — matching Python, where a
    /// tree adapter without `data_sources` simply omits the field
    /// (core.py:483, `hasattr(entry, "data_sources")`).
    /// Order the keys returned by [`search`](Self::search) according to
    /// `sorting`, before the `search_page` window is applied. The default is a
    /// no-op (matched order preserved) — the Rust analog of Python's
    /// `hasattr(tree, "sort")` gate (server/core.py:235): a tree that does not
    /// override this simply ignores `?sort=`. [`MapAdapter`](crate::adapters::MapAdapter)
    /// overrides it to order by child metadata. The SQL catalog does not reach
    /// this path — it overrides [`search_page`](Self::search_page) to push
    /// `ORDER BY` into the query.
    fn sort_matched_keys(
        &self,
        keys: Vec<String>,
        _sorting: &[(String, SortDirection)],
    ) -> Vec<String> {
        keys
    }

    fn search_page<'a>(
        &'a self,
        queries: &'a [crate::core::queries::Query],
        sorting: &'a [(String, SortDirection)],
        _cursor: Option<i64>,
        offset: usize,
        limit: usize,
        _include_data_sources: bool,
    ) -> BoxFuture<'a, Result<SearchPage>> {
        Box::pin(async move {
            let matched = self.sort_matched_keys(self.search(queries).await?, sorting);
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
                    // In-memory trees do not track data sources; the flag is
                    // ignored and the field stays omitted on the wire.
                    data_sources: None,
                });
            }
            Ok(SearchPage {
                entries,
                total,
                next_cursor: None,
            })
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
    Ragged(Arc<dyn RaggedAdapterRead>),
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
            Self::Ragged(a) => a.structure_family(),
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
            Self::Ragged(a) => a.metadata(),
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
            Self::Ragged(a) => a.specs(),
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
            Self::Ragged(r) => Ok(serde_json::to_value(r.structure()).ok()),
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

    /// Owned, `'static` clone of the sparse leaf. See [`AnyAdapter::as_array_arc`].
    #[inline]
    pub fn as_sparse_arc(&self) -> Option<Arc<dyn SparseAdapterRead>> {
        match self {
            Self::Sparse(s) => Some(Arc::clone(s)),
            _ => None,
        }
    }

    /// Owned, `'static` clone of the ragged leaf. See [`AnyAdapter::as_array_arc`].
    #[inline]
    pub fn as_ragged_arc(&self) -> Option<Arc<dyn RaggedAdapterRead>> {
        match self {
            Self::Ragged(r) => Some(Arc::clone(r)),
            _ => None,
        }
    }

    /// Owned, `'static` clone of the awkward leaf. See [`AnyAdapter::as_array_arc`].
    #[inline]
    pub fn as_awkward_arc(&self) -> Option<Arc<dyn AwkwardAdapterRead>> {
        match self {
            Self::Awkward(a) => Some(Arc::clone(a)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod sparse_densify_tests {
    use super::SparseData;
    use crate::core::dtype::{BuiltinDType, DynNDArray, Endianness, Kind};
    use bytes::Bytes;

    fn i64_coords(vals: &[i64]) -> DynNDArray {
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);
        let bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        DynNDArray::new(Bytes::from(bytes), dtype, vec![vals.len()])
    }

    fn f64_data(vals: &[f64]) -> DynNDArray {
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        DynNDArray::new(Bytes::from(bytes), dtype, vec![vals.len()])
    }

    fn read_f64s(arr: &DynNDArray) -> Vec<f64> {
        arr.data
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    #[test]
    fn densify_whole_array_scatters_at_global_coords() {
        // 3x3 grid, non-zeros at (0,1)=1.5 and (2,0)=3.7.
        let sd = SparseData {
            coords: vec![i64_coords(&[0, 2]), i64_coords(&[1, 0])],
            data: f64_data(&[1.5, 3.7]),
        };
        let dense = sd.densify(&[0, 0], &[3, 3]);
        assert_eq!(dense.shape, vec![3, 3]);
        assert_eq!(
            read_f64s(&dense),
            vec![0.0, 1.5, 0.0, 0.0, 0.0, 0.0, 3.7, 0.0, 0.0]
        );
    }

    #[test]
    fn densify_region_remaps_to_local_and_drops_outside() {
        // Global non-zeros at (1,1)=10.0 and (2,2)=20.0. Region starting at
        // (2,2) with shape (2,2) keeps only (2,2), remapped to local (0,0);
        // (1,1) falls outside the region and is dropped.
        let sd = SparseData {
            coords: vec![i64_coords(&[1, 2]), i64_coords(&[1, 2])],
            data: f64_data(&[10.0, 20.0]),
        };
        let dense = sd.densify(&[2, 2], &[2, 2]);
        assert_eq!(dense.shape, vec![2, 2]);
        assert_eq!(read_f64s(&dense), vec![20.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn densify_empty_sparse_data_is_all_zeros() {
        let sd = SparseData {
            coords: vec![i64_coords(&[]), i64_coords(&[])],
            data: f64_data(&[]),
        };
        let dense = sd.densify(&[0, 0], &[2, 2]);
        assert_eq!(read_f64s(&dense), vec![0.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn densify_boundary_region_zero_pads_past_array_edge() {
        // Array shape (3,), one non-zero at global index 2. A boundary chunk
        // covering [2, 4) (padded past the array edge at 3) keeps index 2 at
        // local 0, and zero-fills local index 1 (which has no backing data).
        let sd = SparseData {
            coords: vec![i64_coords(&[2])],
            data: f64_data(&[9.0]),
        };
        let dense = sd.densify(&[2], &[2]);
        assert_eq!(read_f64s(&dense), vec![9.0, 0.0]);
    }
}
