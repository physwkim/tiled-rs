//! Read-only Zarr protocol routers (`/zarr/v2`, `/zarr/v3`).
//!
//! Ports upstream tiled `tiled/server/zarr.py` (`get_zarr_router_v2` /
//! `get_zarr_router_v3`, added in PR #774) which are mounted in `app.py:419-420`.
//! These expose every catalog node as a READ-ONLY zarr store over HTTP so
//! generic zarr clients (zarr-python + fsspec HTTP store) can read tiled data
//! without speaking the tiled API:
//!
//! * **v2** — `.zgroup`/`.zattrs` for containers, `.zarray`/`.zattrs` for
//!   arrays, and chunk key paths like `0.0.1` returning raw chunk bytes.
//! * **v3** — `zarr.json` metadata documents and chunk key paths like
//!   `c/0/0/1`.
//!
//! ## Deliberate divergence from upstream: uncompressed chunks
//!
//! Upstream declares a Blosc(lz4) `compressor`/`codecs` entry and blosc-encodes
//! every chunk (`zarr_codec.encode(array)`). This port declares **no
//! compression** — v2 `"compressor": null`, v3 codecs = just the `bytes` codec
//! — and returns the raw C-order chunk buffer. This is a fully valid zarr v2/v3
//! store that any zarr client can read; it trades byte-identical parity with
//! upstream's blosc frames for a dependency-free, provably-correct path (the
//! in-repo blosc tooling targets the blosc2 frame format with a fixed
//! typesize/no-shuffle, which does not reproduce numcodecs' classic-blosc
//! frames, so a "faithful" blosc port could not be verified against a real
//! zarr client). See the task report for details.
//!
//! ## Scope
//!
//! Arrays, sparse arrays, and containers are fully supported. Tables are
//! surfaced as zarr *groups* whose listing enumerates the column names
//! (mirroring upstream), and each per-column URL resolves to a zarr array:
//! `walk_tree` synthesizes an array-adapter view over the column (see
//! [`crate::server::core`]), so a table column's `.zarray`/`zarr.json` and chunk
//! reads work like any other array. A column whose Arrow dtype is non-numeric
//! (string, temporal, nested) cannot be served as an array yet — reported as
//! UNFIXED in the task report.
//!
//! ## Sparse arrays: densify via a full read, not a partial slice
//!
//! Upstream densifies a sparse chunk with `array.todense()` after reading the
//! block's slice (`entry.read(slice=block_slices)`). This port reads the
//! *whole* sparse array once per chunk request
//! (`SparseAdapterRead::read(&NDSlice::empty())`) and densifies just the
//! requested chunk region from that via [`crate::core::adapters::SparseData::densify`],
//! rather than requesting the chunk's slice directly from the adapter: not
//! every `SparseAdapterRead` implementation applies a non-trivial slice (see
//! `sparse_blocks_parquet_adapter.rs`'s `apply_sparse_slice`), while every
//! adapter's *empty*-slice read correctly assembles the full array into one
//! global coordinate frame. Sparse data is small by definition, so re-reading
//! the whole array per chunk is an acceptable trade for not depending on a
//! partially-implemented slice path.

use axum::extract::{OriginalUri, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::core::adapters::{AnyAdapter, ArrayAdapterRead, ContainerAdapter, SparseAdapterRead};
use crate::core::dtype::{BuiltinDType, DType, DynNDArray, Kind};
use crate::core::ndslice::NDSlice;
use crate::core::structures::{SparseStructure, StructureFamily};
use crate::server::AuthContext;
use crate::server::core;
use crate::server::error::ServerError;
use crate::server::extractors::{BaseUrl, PathSegments};
use crate::server::router::resolve_entry;
use crate::server::state::AppState;

/// Zarr's per-dimension chunk-size cap (mirrors upstream `ZARR_BLOCK_SIZE`).
const ZARR_BLOCK_SIZE: usize = 10000;

// ---------------------------------------------------------------------------
// Chunk-spec helpers
// ---------------------------------------------------------------------------

/// Convert the full tiled/dask chunk specification into zarr form.
///
/// Zarr requires a single constant chunk size along each dimension; tiled
/// permits variable-sized chunks, so — matching upstream `convert_chunks_for_zarr`
/// — each dimension collapses to `min(ZARR_BLOCK_SIZE, max(sizes, 1))`. A
/// zero-dimensional array yields an empty spec.
fn convert_chunks_for_zarr(chunks: &[Vec<usize>]) -> Vec<usize> {
    chunks
        .iter()
        .map(|tc| {
            tc.iter()
                .copied()
                .max()
                .unwrap_or(1)
                .clamp(1, ZARR_BLOCK_SIZE)
        })
        .collect()
}

/// True when `seg` is a v2 chunk key such as `0`, `0.1`, `0.1.2` — one or more
/// dot-separated runs of digits (upstream regex `^(?:\d+\.)*\d+$`).
fn parse_v2_block(seg: &str) -> Option<Vec<usize>> {
    if seg.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for part in seg.split('.') {
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        out.push(part.parse::<usize>().ok()?);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Dtype rendering
// ---------------------------------------------------------------------------

/// Render a tiled dtype as a numpy descr for a v2 `.zarray` `dtype` field,
/// mirroring upstream `DataType.to_numpy_descr()`: a builtin dtype becomes its
/// numpy string (e.g. `"<f8"`); a structured dtype becomes the list of field
/// descrs (`[name, dtype, shape?]`).
fn dtype_to_numpy_descr(dt: &DType) -> serde_json::Value {
    match dt {
        DType::Builtin(b) => serde_json::Value::String(b.to_numpy_str()),
        DType::Struct(s) => serde_json::Value::Array(
            s.fields
                .iter()
                .map(|f| {
                    let mut entry = vec![
                        serde_json::Value::String(f.name.clone()),
                        dtype_to_numpy_descr(&f.dtype),
                    ];
                    if let Some(shape) = &f.shape {
                        entry.push(serde_json::json!(shape));
                    }
                    serde_json::Value::Array(entry)
                })
                .collect(),
        ),
    }
}

/// Map a builtin numeric dtype to its zarr v3 core `data_type` name (e.g.
/// `float64`, `int32`, `uint8`, `bool`, `complex128`).
///
/// Only the zarr v3 core numeric/bool types are supported; string, unicode,
/// datetime, void and structured dtypes have no v3 core name and yield a 422
/// (they are reported UNFIXED for the v3 router).
fn dtype_v3_name(b: &BuiltinDType) -> Result<String, ServerError> {
    let bits = b.itemsize * 8;
    let name = match b.kind {
        Kind::Boolean => "bool".to_string(),
        Kind::Integer => format!("int{bits}"),
        Kind::UnsignedInteger => format!("uint{bits}"),
        Kind::Float => format!("float{bits}"),
        Kind::ComplexFloat => format!("complex{bits}"),
        other => {
            return Err(ServerError::Validation(format!(
                "zarr v3 export does not support numpy dtype kind '{}'",
                other.to_numpy_char()
            )));
        }
    };
    Ok(name)
}

/// The zarr v3 JSON scalar for a builtin dtype's default (zero) fill value,
/// mirroring `zarr_dtype.default_scalar()` → `to_json_scalar`.
fn v3_default_scalar(b: &BuiltinDType) -> serde_json::Value {
    match b.kind {
        Kind::Boolean => serde_json::Value::Bool(false),
        Kind::Integer | Kind::UnsignedInteger => serde_json::json!(0),
        Kind::Float => serde_json::json!(0.0),
        Kind::ComplexFloat => serde_json::json!([0.0, 0.0]),
        _ => serde_json::Value::Null,
    }
}

/// Decode a single scalar element into a zarr v3 JSON scalar — used for a
/// 0-dimensional array whose fill value is the value itself (upstream
/// `entry.read()[()]`).
fn decode_v3_scalar(b: &BuiltinDType, bytes: &[u8]) -> serde_json::Value {
    let le = b.endianness != crate::core::dtype::Endianness::Big;
    macro_rules! rd {
        ($ty:ty, $n:expr) => {{
            if bytes.len() < $n {
                return v3_default_scalar(b);
            }
            let arr: [u8; $n] = bytes[..$n].try_into().unwrap();
            if le {
                <$ty>::from_le_bytes(arr)
            } else {
                <$ty>::from_be_bytes(arr)
            }
        }};
    }
    match (b.kind, b.itemsize) {
        (Kind::Boolean, _) => serde_json::Value::Bool(bytes.first().is_some_and(|&x| x != 0)),
        (Kind::Integer, 1) => serde_json::json!(rd!(i8, 1)),
        (Kind::Integer, 2) => serde_json::json!(rd!(i16, 2)),
        (Kind::Integer, 4) => serde_json::json!(rd!(i32, 4)),
        (Kind::Integer, 8) => serde_json::json!(rd!(i64, 8)),
        (Kind::UnsignedInteger, 1) => serde_json::json!(rd!(u8, 1)),
        (Kind::UnsignedInteger, 2) => serde_json::json!(rd!(u16, 2)),
        (Kind::UnsignedInteger, 4) => serde_json::json!(rd!(u32, 4)),
        (Kind::UnsignedInteger, 8) => serde_json::json!(rd!(u64, 8)),
        (Kind::Float, 4) => serde_json::json!(rd!(f32, 4)),
        (Kind::Float, 8) => serde_json::json!(rd!(f64, 8)),
        // float16: widen to f32 (lossless) so a 0-dimensional float16 array
        // reports its real scalar fill value instead of falling through to
        // v3_default_scalar's 0.0. dtype_v3_name already emits "float16".
        (Kind::Float, 2) => serde_json::json!(half::f16::from_bits(rd!(u16, 2)).to_f32()),
        _ => v3_default_scalar(b),
    }
}

// ---------------------------------------------------------------------------
// Chunk extraction
// ---------------------------------------------------------------------------

/// Read one zarr chunk from an array adapter and return its raw C-order bytes,
/// zero-padded so every chunk has the full zarr chunk shape.
///
/// Mirrors upstream: the chunk maps to the array slice
/// `[i*c : (i+1)*c]` per dimension (`c` = zarr chunk size), read via the
/// adapter's `read(slice)` (NOT `read_block`, so variable tiled chunking is
/// transparent), then each axis is right-padded with zeros up to `c` when the
/// slice ran past the array's edge.
async fn read_zarr_chunk(
    adapter: &dyn ArrayAdapterRead,
    block: &[usize],
) -> Result<bytes::Bytes, ServerError> {
    let structure = adapter.structure();
    let zc = convert_chunks_for_zarr(&structure.chunks);

    // Scalar (0-d) array: the only valid chunk key is `0`. Otherwise the block
    // index length must match the array rank. (Upstream raises 400.)
    let is_scalar = zc.is_empty() && block == [0];
    if !is_scalar && zc.len() != block.len() {
        return Err(ServerError::BadRequest(format!(
            "Requested zarr block index {block:?} is inconsistent with the shape of array, {:?}.",
            structure.shape
        )));
    }

    // Build the per-axis slice `i*c : (i+1)*c`; a scalar reads the whole (empty)
    // slice.
    let slice = if is_scalar {
        NDSlice::empty()
    } else {
        let spec = block
            .iter()
            .zip(&zc)
            .map(|(&i, &c)| format!("{}:{}", i * c, (i + 1) * c))
            .collect::<Vec<_>>()
            .join(",");
        NDSlice::from_numpy_str(&spec).map_err(|e| {
            ServerError::Validation(format!("Invalid zarr block slice '{spec}': {e}"))
        })?
    };

    let array = adapter.read(&slice).await.map_err(ServerError::from)?;

    // Pad each axis up to the full zarr chunk size when the boundary chunk was
    // clipped by the array edge (upstream `np.pad(..., mode="constant")`).
    if is_scalar {
        return Ok(array.data);
    }
    let target: Vec<usize> = (0..array.shape.len())
        .map(|d| {
            let over = ((block[d] + 1) * zc[d]).saturating_sub(structure.shape[d]);
            array.shape[d] + over
        })
        .collect();
    Ok(pad_c_order(&array, &target).data)
}

/// Right-pad a C-contiguous array with zeros so its shape becomes `target`
/// (each `target[d] >= arr.shape[d]`), copying the source into the leading
/// corner. Used to bring a boundary chunk up to the full zarr chunk shape.
fn pad_c_order(arr: &DynNDArray, target: &[usize]) -> DynNDArray {
    if arr.shape == target {
        return arr.clone();
    }
    let esz = arr.dtype.element_size();
    let ndim = arr.shape.len();
    let total: usize = target.iter().product();
    let mut out = vec![0u8; total * esz];

    // Nothing to copy when the source is empty.
    if esz == 0 || arr.shape.contains(&0) {
        return DynNDArray::new(bytes::Bytes::from(out), arr.dtype.clone(), target.to_vec());
    }

    // C-order element strides over the target shape.
    let mut tstride = vec![1usize; ndim];
    for i in (0..ndim.saturating_sub(1)).rev() {
        tstride[i] = tstride[i + 1] * target[i + 1];
    }

    let last = ndim - 1;
    let run = arr.shape[last]; // contiguous elements per source row
    let run_bytes = run * esz;
    let outer: usize = arr.shape[..last].iter().product();

    let mut idx = vec![0usize; last];
    for src_row in 0..outer {
        let src_off = src_row * run_bytes;
        let mut dst_elem = 0usize;
        for d in 0..last {
            dst_elem += idx[d] * tstride[d];
        }
        let dst_off = dst_elem * esz;
        out[dst_off..dst_off + run_bytes].copy_from_slice(&arr.data[src_off..src_off + run_bytes]);
        // Odometer over the outer (non-contiguous) axes.
        for d in (0..last).rev() {
            idx[d] += 1;
            if idx[d] < arr.shape[d] {
                break;
            }
            idx[d] = 0;
        }
    }
    DynNDArray::new(bytes::Bytes::from(out), arr.dtype.clone(), target.to_vec())
}

/// Read one zarr chunk from a sparse adapter and return its densified raw
/// C-order bytes — the sparse-family counterpart of [`read_zarr_chunk`].
///
/// Mirrors upstream: the chunk maps to the region `[i*c : (i+1)*c]` per
/// dimension (`c` = zarr chunk size). Unlike the dense-array path, this reads
/// the *whole* array via `SparseAdapterRead::read(&NDSlice::empty())` (see the
/// module docs for why) and densifies just the requested region with
/// [`crate::core::adapters::SparseData::densify`], which — because it only
/// places non-zeros whose coordinates fall inside the region — zero-pads a
/// boundary chunk automatically; no separate padding step is needed here.
async fn read_zarr_chunk_sparse(
    adapter: &dyn SparseAdapterRead,
    block: &[usize],
) -> Result<bytes::Bytes, ServerError> {
    let structure = adapter.structure();
    let zc = convert_chunks_for_zarr(&structure.chunks);

    // Scalar (0-d) array: the only valid chunk key is `0`. Otherwise the block
    // index length must match the array rank. (Upstream raises 400.)
    let is_scalar = zc.is_empty() && block == [0];
    if !is_scalar && zc.len() != block.len() {
        return Err(ServerError::BadRequest(format!(
            "Requested zarr block index {block:?} is inconsistent with the shape of array, {:?}.",
            structure.shape
        )));
    }

    let (starts, shape): (Vec<usize>, Vec<usize>) = if is_scalar {
        (vec![], vec![])
    } else {
        (block.iter().zip(&zc).map(|(&i, &c)| i * c).collect(), zc)
    };

    let sparse_data = adapter
        .read(&NDSlice::empty())
        .await
        .map_err(ServerError::from)?;
    Ok(sparse_data.densify(&starts, &shape).data)
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

fn json_response(value: &serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string()),
    )
        .into_response()
}

fn chunk_response(body: bytes::Bytes) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        body,
    )
        .into_response()
}

/// Resolve `segments` to a node adapter, enforcing auth exactly as the other
/// data routes do: `read:data` is required, per-node access policy is applied,
/// and a missing path (or one narrowed to no read:metadata) is a 404.
///
/// Returns `None` for the root (empty `segments`) — the caller treats the root
/// as a container without a leaf adapter.
async fn resolve_zarr_node(
    state: &AppState,
    auth: &AuthContext,
    segments: &[String],
) -> Result<Option<AnyAdapter>, ServerError> {
    // Every zarr endpoint requires read:data (upstream passes at least
    // ["read:data"] to get_entry); resolve_entry additionally 404s if the
    // per-node policy strips read:metadata at any level.
    resolve_entry(state, auth.clone(), segments, crate::auth::Scope::ReadData).await?;
    if segments.is_empty() {
        return Ok(None);
    }
    let adapter = core::walk_tree(state.root_tree.as_ref(), segments).await?;
    Ok(Some(adapter))
}

/// The metadata JSON for a resolved node — the root or a leaf adapter.
fn node_metadata(state: &AppState, node: &Option<AnyAdapter>) -> serde_json::Value {
    match node {
        None => state.root_tree.metadata().clone(),
        Some(a) => a.metadata().clone(),
    }
}

/// The structure family of a resolved node (root ⇒ container).
fn node_family(node: &Option<AnyAdapter>) -> StructureFamily {
    match node {
        None => StructureFamily::Container,
        Some(a) => a.structure_family(),
    }
}

/// Enumerate a container's child keys — the single owner for a caller-facing
/// zarr group listing. When an access policy is configured its `list_filter`
/// result is injected as an `AccessBlobFilter` query so a principal never
/// receives the names of children they may not read. The listing scope is
/// `read:metadata`, matching upstream `filter_for_access(..., ["read:metadata"],
/// ...)` (dependencies.py:78) and the `/search` path (router.rs). With no policy
/// (`access_policy: None`) the raw key set is returned unchanged.
async fn filtered_child_keys(
    state: &AppState,
    auth: &AuthContext,
    container: &dyn ContainerAdapter,
) -> Result<Vec<String>, ServerError> {
    if let Some(ref policy) = state.access_policy {
        let requested = crate::auth::ScopeSet::from_iter([crate::auth::Scope::ReadMetadata]);
        if let Some(f) = policy
            .list_filter(
                auth.principal.as_deref(),
                &auth.scopes,
                &requested,
                auth.authn_access_tags.as_deref(),
            )
            .await
        {
            return container
                .search(&[crate::core::queries::Query::AccessBlobFilter(f)])
                .await
                .map_err(ServerError::from);
        }
    }
    container.keys().await.map_err(ServerError::from)
}

/// Build the JSON array of child URLs for a group listing: container children
/// (keys, access-filtered via [`filtered_child_keys`]) or, for a table, its
/// column names. `base` is the full request URL with any trailing slash removed.
async fn group_listing(
    state: &AppState,
    auth: &AuthContext,
    node: &Option<AnyAdapter>,
    base: &str,
) -> Result<Vec<String>, ServerError> {
    let keys: Vec<String> = match node {
        None => filtered_child_keys(state, auth, state.root_tree.as_ref()).await?,
        Some(AnyAdapter::Container(c)) => filtered_child_keys(state, auth, c.as_ref()).await?,
        Some(AnyAdapter::Table(t)) => t.structure().columns.clone(),
        _ => {
            return Err(ServerError::WrongType(
                "node is not a zarr group".to_string(),
            ));
        }
    };
    Ok(keys.into_iter().map(|k| format!("{base}/{k}")).collect())
}

/// The full request URL (scheme://authority + path), trailing slash trimmed —
/// the base against which child URLs are formed in a group listing.
fn request_base(base_url: &str, uri: &axum::http::Uri) -> String {
    let mut s = format!("{base_url}{}", uri.path());
    while s.ends_with('/') {
        s.pop();
    }
    s
}

// ---------------------------------------------------------------------------
// v2 router
// ---------------------------------------------------------------------------

/// Single dispatch handler for the `/zarr/v2/{*path}` surface. Distinguishes
/// the metadata documents (`.zattrs`, `.zgroup`, `.zarray`) from a group
/// listing / chunk read by inspecting the trailing path segment.
pub async fn zarr_v2(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    auth: AuthContext,
) -> Result<Response, ServerError> {
    let segments = PathSegments::from_raw_path(uri.path(), "/zarr/v2/").0;
    let last = segments.last().map(String::as_str).unwrap_or("");

    match last {
        ".zattrs" => {
            let entry = &segments[..segments.len() - 1];
            let node = resolve_zarr_node(&state, &auth, entry).await?;
            let attrs = node_metadata(&state, &node)
                .get("attributes")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            Ok(json_response(&attrs))
        }
        ".zgroup" => {
            let entry = &segments[..segments.len() - 1];
            let node = resolve_zarr_node(&state, &auth, entry).await?;
            match node_family(&node) {
                StructureFamily::Container | StructureFamily::Table => {
                    Ok(json_response(&serde_json::json!({"zarr_format": 2})))
                }
                _ => Err(ServerError::NotFound(format!(
                    "'{}' is not a zarr group",
                    entry.join("/")
                ))),
            }
        }
        ".zarray" => {
            let entry = &segments[..segments.len() - 1];
            let node = resolve_zarr_node(&state, &auth, entry).await?;
            let doc = match &node {
                Some(AnyAdapter::Array(a)) => zarray_metadata(a.as_ref()),
                Some(AnyAdapter::Sparse(s)) => sparse_zarray_metadata(s.structure())?,
                _ => {
                    return Err(ServerError::NotFound(format!(
                        "'{}' is not a zarr array",
                        entry.join("/")
                    )));
                }
            };
            Ok(json_response(&doc))
        }
        _ => {
            // Group listing or a chunk read. A trailing dotted-digits segment is
            // a chunk key; strip it to locate the array.
            let block = parse_v2_block(last);
            let entry: &[String] = if block.is_some() {
                &segments[..segments.len() - 1]
            } else {
                &segments
            };
            let node = resolve_zarr_node(&state, &auth, entry).await?;
            match &node {
                Some(AnyAdapter::Container(_)) | None => {
                    let base = request_base(&base_url, &uri);
                    let urls = group_listing(&state, &auth, &node, &base).await?;
                    Ok(json_response(&serde_json::json!(urls)))
                }
                Some(AnyAdapter::Table(_)) => {
                    let base = request_base(&base_url, &uri);
                    let urls = group_listing(&state, &auth, &node, &base).await?;
                    Ok(json_response(&serde_json::json!(urls)))
                }
                Some(AnyAdapter::Array(a)) => match block {
                    Some(idx) => {
                        let body = read_zarr_chunk(a.as_ref(), &idx).await?;
                        Ok(chunk_response(body))
                    }
                    // Whole-array URL with no chunk key: upstream returns `{}`.
                    None => Ok(json_response(&serde_json::json!({}))),
                },
                Some(AnyAdapter::Sparse(s)) => match block {
                    Some(idx) => {
                        let body = read_zarr_chunk_sparse(s.as_ref(), &idx).await?;
                        Ok(chunk_response(body))
                    }
                    // Whole-array URL with no chunk key: upstream returns `{}`.
                    None => Ok(json_response(&serde_json::json!({}))),
                },
                Some(_) => Err(ServerError::NotFound(format!(
                    "'{}' cannot be read as a zarr array",
                    entry.join("/")
                ))),
            }
        }
    }
}

/// Build a v2 `.zarray` document (uncompressed — see module docs).
fn zarray_metadata(adapter: &dyn ArrayAdapterRead) -> serde_json::Value {
    let s = adapter.structure();
    serde_json::json!({
        "chunks": convert_chunks_for_zarr(&s.chunks),
        "compressor": serde_json::Value::Null,
        "dtype": dtype_to_numpy_descr(&s.data_type),
        "fill_value": serde_json::Value::Null,
        "filters": serde_json::Value::Null,
        "order": "C",
        "shape": s.shape,
        "zarr_format": 2,
    })
}

/// A sparse array's declared value dtype, or a 500 if unset. Shared by the v2
/// and v3 sparse metadata builders below — [`SparseStructure::data_type`] is
/// optional on the wire (unlike [`crate::core::structures::ArrayStructure::data_type`]),
/// but every adapter that actually serves data sets it.
fn sparse_data_type(s: &SparseStructure) -> Result<&DType, ServerError> {
    s.data_type
        .as_ref()
        .ok_or_else(|| ServerError::Internal("sparse array has no data_type set".to_string()))
}

/// Build a v2 `.zarray` document for a sparse array (uncompressed — see module
/// docs). Same shape as [`zarray_metadata`], mirroring upstream's single
/// `get_zarr_array_metadata` handler, which serves both `StructureFamily.array`
/// and `StructureFamily.sparse` from `entry.structure()`.
fn sparse_zarray_metadata(s: &SparseStructure) -> Result<serde_json::Value, ServerError> {
    let dt = sparse_data_type(s)?;
    Ok(serde_json::json!({
        "chunks": convert_chunks_for_zarr(&s.chunks),
        "compressor": serde_json::Value::Null,
        "dtype": dtype_to_numpy_descr(dt),
        "fill_value": serde_json::Value::Null,
        "filters": serde_json::Value::Null,
        "order": "C",
        "shape": s.shape,
        "zarr_format": 2,
    }))
}

// ---------------------------------------------------------------------------
// v3 router
// ---------------------------------------------------------------------------

/// Single dispatch handler for the `/zarr/v3/{*path}` surface. Distinguishes
/// the `zarr.json` metadata document, a `c/<i>/<j>/…` chunk read, and a group
/// listing by inspecting the trailing path segments.
pub async fn zarr_v3(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    auth: AuthContext,
) -> Result<Response, ServerError> {
    let segments = PathSegments::from_raw_path(uri.path(), "/zarr/v3/").0;

    // `zarr.json` metadata document.
    if segments.last().map(String::as_str) == Some("zarr.json") {
        let entry = &segments[..segments.len() - 1];
        let node = resolve_zarr_node(&state, &auth, entry).await?;
        return zarr_v3_metadata(&state, &node, entry).await;
    }

    // Chunk read: `.../c/<i>/<j>/…` where every segment after the last `c` is a
    // non-negative integer. Greedy match on the last such `c` (mirrors the
    // upstream `{path:path}/c/{block:path}` route).
    if let Some((entry, block)) = split_v3_chunk(&segments) {
        let node = resolve_zarr_node(&state, &auth, entry).await?;
        let body = match &node {
            Some(AnyAdapter::Array(a)) => read_zarr_chunk(a.as_ref(), &block).await?,
            Some(AnyAdapter::Sparse(s)) => read_zarr_chunk_sparse(s.as_ref(), &block).await?,
            _ => {
                return Err(ServerError::NotFound(format!(
                    "'{}' is not a zarr array",
                    entry.join("/")
                )));
            }
        };
        return Ok(chunk_response(body));
    }

    // Otherwise a group URL. Containers/tables list their children; an array
    // path resolves to its `zarr.json` metadata (upstream delegates here).
    let node = resolve_zarr_node(&state, &auth, &segments).await?;
    match &node {
        Some(AnyAdapter::Container(_)) | None | Some(AnyAdapter::Table(_)) => {
            let base = request_base(&base_url, &uri);
            let urls = group_listing(&state, &auth, &node, &base).await?;
            Ok(json_response(&serde_json::json!(urls)))
        }
        Some(_) => zarr_v3_metadata(&state, &node, &segments).await,
    }
}

/// Split `segments` into `(entry, block)` when they encode a v3 chunk key
/// (`<entry…>/c/<int>/<int>/…`). Chooses the last `c` whose trailing segments
/// are all integers, matching the greedy upstream route.
fn split_v3_chunk(segments: &[String]) -> Option<(&[String], Vec<usize>)> {
    for i in (0..segments.len()).rev() {
        if segments[i] != "c" || i + 1 >= segments.len() {
            continue;
        }
        let tail = &segments[i + 1..];
        let mut block = Vec::with_capacity(tail.len());
        let mut ok = true;
        for seg in tail {
            match seg.parse::<usize>() {
                Ok(n) if seg.bytes().all(|b| b.is_ascii_digit()) => block.push(n),
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return Some((&segments[..i], block));
        }
    }
    None
}

/// Build a v3 `zarr.json` metadata document for a resolved node — an `array`
/// document for arrays, a `group` document for containers/tables.
async fn zarr_v3_metadata(
    state: &AppState,
    node: &Option<AnyAdapter>,
    entry: &[String],
) -> Result<Response, ServerError> {
    match node {
        Some(AnyAdapter::Array(a)) => {
            let doc = array_v3_metadata(a.as_ref()).await?;
            Ok(json_response(&doc))
        }
        Some(AnyAdapter::Sparse(s)) => {
            let doc = sparse_array_v3_metadata(s.structure(), s.metadata())?;
            Ok(json_response(&doc))
        }
        // Container / table / root → group document.
        None | Some(AnyAdapter::Container(_)) | Some(AnyAdapter::Table(_)) => {
            Ok(json_response(&serde_json::json!({
                "zarr_format": 3,
                "node_type": "group",
                "attributes": node_metadata(state, node),
            })))
        }
        Some(_) => Err(ServerError::NotFound(format!(
            "'{}' cannot be exported as zarr v3",
            entry.join("/")
        ))),
    }
}

/// Build the v3 `array` metadata document (uncompressed — codecs = `bytes`
/// only; see module docs).
async fn array_v3_metadata(
    adapter: &dyn ArrayAdapterRead,
) -> Result<serde_json::Value, ServerError> {
    let s = adapter.structure();
    let builtin = match &s.data_type {
        DType::Builtin(b) => b.clone(),
        DType::Struct(_) => {
            return Err(ServerError::Validation(
                "zarr v3 export does not support structured dtypes".to_string(),
            ));
        }
    };
    let data_type = dtype_v3_name(&builtin)?;

    // Fill value: the default zero scalar for a shaped array, or the sole value
    // for a 0-dimensional array (upstream `entry.read()[()]`).
    let fill_value = if s.shape.is_empty() {
        let scalar = adapter
            .read(&NDSlice::empty())
            .await
            .map_err(ServerError::from)?;
        decode_v3_scalar(&builtin, &scalar.data)
    } else {
        v3_default_scalar(&builtin)
    };

    let dimension_names = match &s.dims {
        Some(d) => serde_json::json!(d),
        None => serde_json::Value::Null,
    };

    Ok(serde_json::json!({
        "zarr_format": 3,
        "node_type": "array",
        "shape": s.shape,
        "data_type": data_type,
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": convert_chunks_for_zarr(&s.chunks)},
        },
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": fill_value,
        "codecs": [
            {"name": "bytes", "configuration": {"endian": "little"}},
        ],
        "dimension_names": dimension_names,
        "attributes": adapter.metadata().clone(),
    }))
}

/// Build the v3 `array` metadata document for a sparse array (uncompressed —
/// see module docs). Same shape as [`array_v3_metadata`], mirroring
/// upstream's single `get_zarr_metadata` handler which serves both
/// `StructureFamily.array` and `StructureFamily.sparse` alike.
///
/// Unlike the dense-array builder, the 0-dimensional fill-value case does not
/// read the array back: a sparse array's implicit fill value is its zero
/// element by definition, so the default zero scalar always applies, even
/// for a 0-d shape.
fn sparse_array_v3_metadata(
    s: &SparseStructure,
    metadata: &serde_json::Value,
) -> Result<serde_json::Value, ServerError> {
    let dt = sparse_data_type(s)?;
    let builtin = match dt {
        DType::Builtin(b) => b.clone(),
        DType::Struct(_) => {
            return Err(ServerError::Validation(
                "zarr v3 export does not support structured dtypes".to_string(),
            ));
        }
    };
    let data_type = dtype_v3_name(&builtin)?;
    let fill_value = v3_default_scalar(&builtin);

    let dimension_names = match &s.dims {
        Some(d) => serde_json::json!(d),
        None => serde_json::Value::Null,
    };

    Ok(serde_json::json!({
        "zarr_format": 3,
        "node_type": "array",
        "shape": s.shape,
        "data_type": data_type,
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": convert_chunks_for_zarr(&s.chunks)},
        },
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": fill_value,
        "codecs": [
            {"name": "bytes", "configuration": {"endian": "little"}},
        ],
        "dimension_names": dimension_names,
        "attributes": metadata,
    }))
}

/// Build the `/zarr/v2` + `/zarr/v3` sub-router. Merged into the auth-guarded
/// group in [`crate::server::app::build_app`] so every request passes through
/// the same auth middleware as the tiled API. A root route is provided for the
/// bare `/zarr/vN/` group listing; `{*path}` covers everything else.
pub fn zarr_router() -> axum::Router<AppState> {
    use axum::routing::get;
    axum::Router::new()
        .route("/zarr/v2/", get(zarr_v2))
        .route("/zarr/v2/{*path}", get(zarr_v2))
        .route("/zarr/v3/", get(zarr_v3))
        .route("/zarr/v3/{*path}", get(zarr_v3))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dtype::Endianness;

    fn seg(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn convert_chunks_collapses_to_constant_max_capped() {
        // Variable tiled chunks collapse to their max per dimension.
        assert_eq!(convert_chunks_for_zarr(&[vec![2, 2]]), vec![2]);
        assert_eq!(convert_chunks_for_zarr(&[vec![3, 2]]), vec![3]);
        // Multi-dim.
        assert_eq!(convert_chunks_for_zarr(&[vec![4], vec![5, 1]]), vec![4, 5]);
        // Capped at ZARR_BLOCK_SIZE; empty dim floors at 1.
        assert_eq!(
            convert_chunks_for_zarr(&[vec![50_000]]),
            vec![ZARR_BLOCK_SIZE]
        );
        assert_eq!(convert_chunks_for_zarr(&[vec![]]), vec![1]);
        // 0-d array → empty spec.
        assert!(convert_chunks_for_zarr(&[]).is_empty());
    }

    #[test]
    fn parse_v2_block_matches_dotted_digits_only() {
        assert_eq!(parse_v2_block("0"), Some(vec![0]));
        assert_eq!(parse_v2_block("0.1.2"), Some(vec![0, 1, 2]));
        assert_eq!(parse_v2_block("12.34"), Some(vec![12, 34]));
        // Non-chunk trailing segments are not blocks.
        assert_eq!(parse_v2_block(".zarray"), None);
        assert_eq!(parse_v2_block("nested_arr"), None);
        assert_eq!(parse_v2_block("0."), None);
        assert_eq!(parse_v2_block(".0"), None);
        assert_eq!(parse_v2_block(""), None);
        assert_eq!(parse_v2_block("1.a"), None);
    }

    #[test]
    fn split_v3_chunk_greedy_on_last_c() {
        // Simple case.
        let s = seg(&["arr", "c", "0", "1"]);
        let (entry, block) = split_v3_chunk(&s).unwrap();
        assert_eq!(entry, seg(&["arr"]).as_slice());
        assert_eq!(block, vec![0, 1]);

        // A container named "c" with no numeric tail is NOT a chunk.
        assert!(split_v3_chunk(&seg(&["grp", "c"])).is_none());
        // No "c" at all.
        assert!(split_v3_chunk(&seg(&["arr", "zarr.json"])).is_none());

        // Greedy: the last "c" whose tail is all-numeric wins.
        let s = seg(&["a", "c", "b", "c", "0"]);
        let (entry, block) = split_v3_chunk(&s).unwrap();
        assert_eq!(entry, seg(&["a", "c", "b"]).as_slice());
        assert_eq!(block, vec![0]);
    }

    #[test]
    fn pad_c_order_1d_appends_trailing_zeros() {
        let dt = BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 1);
        let arr = DynNDArray::new(bytes::Bytes::from(vec![7u8, 8]), dt, vec![2]);
        let out = pad_c_order(&arr, &[4]);
        assert_eq!(out.shape, vec![4]);
        assert_eq!(out.data.as_ref(), &[7, 8, 0, 0]);
    }

    #[test]
    fn pad_c_order_2d_pads_each_axis_into_corner() {
        // 2x2 source [[1,2],[3,4]] padded into a 3x3 chunk → source lands in the
        // top-left corner, everything else zero.
        let dt = BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 1);
        let arr = DynNDArray::new(bytes::Bytes::from(vec![1u8, 2, 3, 4]), dt, vec![2, 2]);
        let out = pad_c_order(&arr, &[3, 3]);
        assert_eq!(out.shape, vec![3, 3]);
        assert_eq!(
            out.data.as_ref(),
            &[
                1, 2, 0, /* row0 */ 3, 4, 0, /* row1 */ 0, 0, 0 /* row2 */
            ]
        );
    }

    #[test]
    fn pad_c_order_identity_when_shape_matches() {
        let dt = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let arr = DynNDArray::new(bytes::Bytes::from(vec![0u8; 16]), dt, vec![2]);
        let out = pad_c_order(&arr, &[2]);
        assert_eq!(out.shape, vec![2]);
        assert_eq!(out.data.len(), 16);
    }

    #[test]
    fn dtype_v3_names_cover_numeric_core() {
        let f = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        assert_eq!(dtype_v3_name(&f).unwrap(), "float64");
        let i = BuiltinDType::new(Endianness::Little, Kind::Integer, 4);
        assert_eq!(dtype_v3_name(&i).unwrap(), "int32");
        let u = BuiltinDType::new(Endianness::NotApplicable, Kind::UnsignedInteger, 1);
        assert_eq!(dtype_v3_name(&u).unwrap(), "uint8");
        let b = BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1);
        assert_eq!(dtype_v3_name(&b).unwrap(), "bool");
        // Unsupported kinds (e.g. unicode) error out.
        let s = BuiltinDType::new(Endianness::Little, Kind::Unicode, 40);
        assert!(dtype_v3_name(&s).is_err());
    }

    #[test]
    fn numpy_descr_renders_builtin_string() {
        let dt = DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8));
        assert_eq!(dtype_to_numpy_descr(&dt), serde_json::json!("<f8"));
    }

    /// A 0-dimensional float16 array reports its real scalar fill value: the
    /// decoder widens f16 -> f32 rather than falling through to the default
    /// 0.0, and dtype_v3_name labels it "float16".
    #[test]
    fn decode_v3_scalar_handles_float16() {
        let f16 = BuiltinDType::new(Endianness::Little, Kind::Float, 2);
        assert_eq!(dtype_v3_name(&f16).unwrap(), "float16");

        // 1.5 and -2.5 are exact in float16.
        let bytes = half::f16::from_f32(1.5).to_bits().to_le_bytes();
        assert_eq!(decode_v3_scalar(&f16, &bytes), serde_json::json!(1.5));

        // A nonzero value must NOT collapse to the default 0.0.
        let bytes = half::f16::from_f32(-2.5).to_bits().to_le_bytes();
        assert_eq!(decode_v3_scalar(&f16, &bytes), serde_json::json!(-2.5));
        assert_ne!(decode_v3_scalar(&f16, &bytes), serde_json::json!(0.0));
    }
}
