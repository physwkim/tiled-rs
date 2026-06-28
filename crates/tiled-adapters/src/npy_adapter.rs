//! NPY (NumPy `.npy`) array adapter.
//!
//! Reads a `.npy` file off disk into memory once (resource-cached by the
//! adapter instance) and exposes it as an [`ArrayAdapterRead`]. Header
//! parsing follows the NPY 1.0/2.0/3.0 spec — version 2+ files use a
//! 4-byte header length so we accept both.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use tiled_core::adapters::{ArrayAdapterRead, ArrayAdapterWrite, BaseAdapter, BoxFuture};
use tiled_core::data_source::Asset;
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

pub struct NpyAdapter {
    array: DynNDArray,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Backing file path, set when the adapter was opened from a file (not
    /// raw bytes). `None` for `from_bytes` adapters, which are read-only.
    path: Option<PathBuf>,
    /// Whether this adapter may be written through. The resolver sets it (via
    /// [`NpyAdapter::into_writable`]) only when the backing file lives under
    /// the server's configured writable storage; a bytes-backed adapter (no
    /// `path`) can never be writable.
    writable: bool,
}

impl NpyAdapter {
    /// Open a `.npy` file at `path` and parse the header.
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        let raw = std::fs::read(&path)
            .map_err(|e| TiledError::Internal(format!("read {}: {e}", path.display())))?;
        let mut adapter = Self::from_bytes(&raw, metadata)?;
        adapter.path = Some(path);
        Ok(adapter)
    }

    /// Mark this file-backed adapter as writable. The leaf resolver calls
    /// this only when the backing path is contained in the server's writable
    /// storage; a bytes-backed adapter (no path) is left read-only so the
    /// write invariant holds by construction: `as_writable().is_some()` ⟹ the
    /// file is under writable storage.
    pub fn into_writable(mut self) -> Self {
        if self.path.is_some() {
            self.writable = true;
        }
        self
    }

    /// Decode raw NPY bytes — useful for tests and in-memory pipelines.
    pub fn from_bytes(raw: &[u8], metadata: serde_json::Value) -> Result<Self> {
        if raw.len() < 10 || &raw[..6] != b"\x93NUMPY" {
            return Err(TiledError::Validation("not a .npy file".into()));
        }
        let major = raw[6];
        let (header_len, body_start) = if major >= 2 {
            if raw.len() < 12 {
                return Err(TiledError::Validation("truncated v2+ header".into()));
            }
            let len = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;
            (len, 12)
        } else {
            let len = u16::from_le_bytes([raw[8], raw[9]]) as usize;
            (len, 10)
        };
        let header_end = body_start + header_len;
        if raw.len() < header_end {
            return Err(TiledError::Validation("truncated header body".into()));
        }
        let header = std::str::from_utf8(&raw[body_start..header_end])
            .map_err(|e| TiledError::Validation(format!("non-utf8 header: {e}")))?;
        let (dtype, shape, fortran_order) = parse_header(header)?;
        if fortran_order {
            return Err(TiledError::Validation(
                "fortran-order .npy files not supported".into(),
            ));
        }
        let body = Bytes::copy_from_slice(&raw[header_end..]);
        let expected_bytes = shape.iter().product::<usize>() * dtype.element_size();
        if body.len() != expected_bytes {
            return Err(TiledError::Validation(format!(
                "npy body is {} bytes; header implies {expected_bytes}",
                body.len()
            )));
        }
        let chunks: Vec<Vec<usize>> = shape.iter().map(|d| vec![*d]).collect();
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: None,
            resizable: Default::default(),
        };
        let array = DynNDArray::new(body, dtype, shape);
        Ok(Self {
            array,
            structure,
            metadata,
            specs: vec![Spec::new("npy")],
            path: None,
            writable: false,
        })
    }
}

impl BaseAdapter for NpyAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Array
    }
    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }
    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ArrayAdapterRead for NpyAdapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }
    fn as_writable(&self) -> Option<&dyn ArrayAdapterWrite> {
        // Writable only when the resolver opted this file-backed adapter in
        // (path under writable storage). Bytes-backed adapters never qualify.
        if self.writable && self.path.is_some() {
            Some(self)
        } else {
            None
        }
    }
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { self.array.apply_slice(slice) })
    }
    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            // NPY adapter exposes a single chunk per dim, so any non-zero
            // block index is out of range.
            for (axis, &b) in block.iter().enumerate() {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "npy adapter has a single chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            self.array.apply_slice(slice)
        })
    }
}

impl ArrayAdapterWrite for NpyAdapter {
    fn write<'a>(&'a self, data: DynNDArray) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let path = self.path.clone().ok_or_else(|| {
                TiledError::Internal("npy adapter has no backing path to write".into())
            })?;
            // The `.npy` file holds the whole array, so a write must supply
            // exactly the registered shape — no partial / mismatched writes.
            if data.shape != self.structure.shape {
                return Err(TiledError::Validation(format!(
                    "npy write shape {:?} does not match the array shape {:?}",
                    data.shape, self.structure.shape
                )));
            }
            let bytes = npy_bytes(&data);
            tokio::task::spawn_blocking(move || write_atomic(&path, &bytes))
                .await
                .map_err(|e| TiledError::Internal(format!("npy write spawn: {e}")))?
        })
    }

    fn write_block<'a>(
        &'a self,
        data: DynNDArray,
        block: &'a [usize],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // npy exposes a single chunk per axis, so the only valid block is
            // the all-zero one — which is exactly the whole array. A non-zero
            // index has no corresponding chunk.
            for (axis, &b) in block.iter().enumerate() {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "npy adapter has a single chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            self.write(data).await
        })
    }
}

/// Serialise a [`DynNDArray`] into NPY v1.0 file bytes (header + C-order
/// body). Inverse of [`NpyAdapter::from_bytes`]; the data buffer is written
/// verbatim, so the caller must pass a C-contiguous array (what
/// [`DynNDArray::new`] and `apply_slice` produce).
pub fn npy_bytes(array: &DynNDArray) -> Vec<u8> {
    let descr = array.dtype.to_numpy_str();
    let shape_str = format_npy_shape(&array.shape);
    let dict = format!("{{'descr': '{descr}', 'fortran_order': False, 'shape': {shape_str}, }}");
    // NPY spec: the total of (10-byte preamble + dict + padding + '\n') must
    // be a multiple of 64. Pad the dict with spaces, terminate with '\n'.
    const PREAMBLE: usize = 10; // magic(6) + version(2) + header-len field(2)
    let unpadded = dict.len() + 1; // trailing '\n'
    let pad = (64 - ((PREAMBLE + unpadded) % 64)) % 64;
    let header_len = unpadded + pad;
    let mut out = Vec::with_capacity(PREAMBLE + header_len + array.data.len());
    out.extend_from_slice(b"\x93NUMPY");
    out.push(1); // major version
    out.push(0); // minor version
    out.extend_from_slice(&(header_len as u16).to_le_bytes());
    out.extend_from_slice(dict.as_bytes());
    out.resize(out.len() + pad, b' ');
    out.push(b'\n');
    out.extend_from_slice(&array.data);
    out
}

/// numpy `repr` of a shape tuple: `()` for 0-d, `(N,)` for 1-d (trailing
/// comma), `(a, b, c)` otherwise.
fn format_npy_shape(shape: &[usize]) -> String {
    match shape {
        [] => "()".to_string(),
        [n] => format!("({n},)"),
        _ => format!(
            "({})",
            shape
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Generate a `data_uri` + create a zero-filled skeleton `.npy` file under
/// `writable_root` for a new internally-managed array node.
///
/// This is the NPY analogue of Python tiled's per-adapter `init_storage`
/// (`tiled/adapters/zarr.py` etc.): the server, not the client, decides where
/// a managed asset lives, so a managed register cannot point physical storage
/// at an arbitrary path. `path_parts` is the node's full path (ancestor
/// segments followed by the node key); each part becomes exactly one on-disk
/// path component, so every part must be a safe single component — empty,
/// `.`/`..`, or any part containing a path separator is rejected, which keeps
/// the result under `writable_root` by construction (no traversal). A
/// zero-filled file with the declared shape/dtype is created so reads before
/// the first write succeed. Returns the generated `data_uri` and the single
/// asset describing it.
pub fn init_storage_npy(
    writable_root: &Path,
    path_parts: &[String],
    structure: &ArrayStructure,
) -> Result<(String, Vec<Asset>)> {
    if !writable_root.is_absolute() {
        return Err(TiledError::Internal(format!(
            "writable storage root {} is not absolute",
            writable_root.display()
        )));
    }
    let dtype = match &structure.data_type {
        DType::Builtin(b) => b.clone(),
        _ => {
            return Err(TiledError::Validation(
                "npy storage supports only builtin (non-struct) dtypes".into(),
            ));
        }
    };
    if path_parts.is_empty() {
        return Err(TiledError::Validation(
            "init_storage: node path is empty".into(),
        ));
    }
    for part in path_parts {
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.contains('/')
            || part.contains('\\')
            || part.contains('\0')
        {
            return Err(TiledError::Validation(format!(
                "init_storage: unsafe path component {part:?}"
            )));
        }
    }
    let (key, ancestors) = path_parts.split_last().expect("non-empty checked above");
    let mut dir = writable_root.to_path_buf();
    for a in ancestors {
        dir.push(a);
    }
    let file = dir.join(format!("{key}.npy"));
    std::fs::create_dir_all(&dir)
        .map_err(|e| TiledError::Internal(format!("init_storage mkdir {}: {e}", dir.display())))?;
    let nbytes = structure.shape.iter().product::<usize>() * dtype.element_size();
    let zeros = DynNDArray::new(
        Bytes::from(vec![0u8; nbytes]),
        dtype,
        structure.shape.clone(),
    );
    write_atomic(&file, &npy_bytes(&zeros))?;
    // Cross-platform `file://` URI for the absolute file path (forward slashes,
    // `file:///C:/...` on Windows). See `tiled_core::file_uri`.
    let data_uri = tiled_core::file_uri::path_to_file_uri(&file).ok_or_else(|| {
        TiledError::Internal(format!(
            "init_storage: storage path is not absolute: {}",
            file.display()
        ))
    })?;
    let asset = Asset {
        data_uri: data_uri.clone(),
        is_directory: false,
        parameter: Some("data_uri".into()),
        num: None,
        id: None,
    };
    Ok((data_uri, vec![asset]))
}

/// Per-process counter that makes temp filenames unique within a single
/// writer process (paired with the PID for cross-process uniqueness).
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write `bytes` to `path` atomically: write a uniquely-named sibling temp
/// file, then rename it over `path` (same-directory rename is atomic on POSIX
/// and Windows). A crash mid-write leaves the previous file intact rather
/// than a truncated one.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        TiledError::Internal(format!("npy path {} has no parent dir", path.display()))
    })?;
    let stem = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("array.npy");
    let pid = std::process::id();
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{stem}.{pid}.{n}.npytmp"));
    std::fs::write(&tmp, bytes)
        .map_err(|e| TiledError::Internal(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        TiledError::Internal(format!(
            "rename {} -> {}: {e}",
            tmp.display(),
            path.display()
        ))
    })?;
    Ok(())
}

/// Parse the NPY header (a Python literal dict) into (dtype, shape,
/// fortran_order). Doesn't import a Python parser — just lifts the three
/// fields we care about by string scanning, which matches what Python tiled
/// itself does for cheap deserialisation.
fn parse_header(header: &str) -> Result<(BuiltinDType, Vec<usize>, bool)> {
    let descr = pick(header, "'descr':")
        .ok_or_else(|| TiledError::Validation("npy header missing descr".into()))?;
    let descr = descr.trim_matches(|c: char| c == '\'' || c.is_whitespace());
    let dtype = parse_descr(descr)?;

    let shape_str = pick(header, "'shape':")
        .ok_or_else(|| TiledError::Validation("npy header missing shape".into()))?;
    let inside = shape_str
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')');
    let mut shape = Vec::new();
    for token in inside.split(',') {
        let t = token.trim();
        if t.is_empty() {
            continue;
        }
        shape.push(
            t.parse::<usize>()
                .map_err(|_| TiledError::Validation(format!("bad shape element: {t}")))?,
        );
    }

    let fortran = pick(header, "'fortran_order':")
        .map(|v| v.trim().starts_with("True"))
        .unwrap_or(false);
    Ok((dtype, shape, fortran))
}

fn pick<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let idx = s.find(key)?;
    let rest = &s[idx + key.len()..];
    // Read until the next ',' that isn't inside (...) or '...'.
    let mut depth = 0;
    let mut in_str = false;
    let mut last = rest.len();
    for (i, c) in rest.char_indices() {
        match c {
            '(' if !in_str => depth += 1,
            ')' if !in_str => depth -= 1,
            '\'' => in_str = !in_str,
            ',' if depth == 0 && !in_str => {
                last = i;
                break;
            }
            _ => {}
        }
    }
    Some(&rest[..last])
}

fn parse_descr(descr: &str) -> Result<BuiltinDType> {
    // NumPy descrs we cope with: '<f8', '<f4', '<i8', '<i4', '<u8', '<u4',
    // '|i1', '|u1', '|b1'. Two-char form `<f8` is endianness + kind +
    // itemsize.
    let bytes = descr.as_bytes();
    if bytes.len() < 3 {
        return Err(TiledError::Validation(format!("bad descr: {descr}")));
    }
    let endian = match bytes[0] {
        b'<' => Endianness::Little,
        b'>' => Endianness::Big,
        b'=' => {
            if cfg!(target_endian = "big") {
                Endianness::Big
            } else {
                Endianness::Little
            }
        }
        b'|' => Endianness::NotApplicable,
        other => {
            return Err(TiledError::Validation(format!(
                "bad endian byte in descr: {}",
                other as char
            )));
        }
    };
    let kind = match bytes[1] {
        b'f' => Kind::Float,
        b'i' => Kind::Integer,
        b'u' => Kind::UnsignedInteger,
        b'b' => Kind::Boolean,
        b'c' => Kind::ComplexFloat,
        b'S' => Kind::String,
        b'U' => Kind::Unicode,
        other => {
            return Err(TiledError::Validation(format!(
                "unsupported descr kind: {}",
                other as char
            )));
        }
    };
    let size: usize = std::str::from_utf8(&bytes[2..])
        .map_err(|_| TiledError::Validation("descr size not utf8".into()))?
        .parse()
        .map_err(|_| TiledError::Validation(format!("descr size not int: {descr}")))?;
    Ok(BuiltinDType::new(endian, kind, size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiled_core::ndslice::NDSlice;

    /// Build a v1 `.npy` file for a 3×4 little-endian f64 array.
    /// Values: arr[i][j] = (i * 4 + j) as f64  →  0.0 … 11.0.
    fn make_npy_3x4_f64() -> Vec<u8> {
        let header = b"{'descr': '<f8', 'fortran_order': False, 'shape': (3, 4), }\n";
        let mut raw = Vec::new();
        raw.extend_from_slice(b"\x93NUMPY");
        raw.push(1); // major
        raw.push(0); // minor
        raw.extend_from_slice(&(header.len() as u16).to_le_bytes());
        raw.extend_from_slice(header);
        for v in 0..12u64 {
            raw.extend_from_slice(&(v as f64).to_le_bytes());
        }
        raw
    }

    #[tokio::test]
    async fn read_with_slice_returns_subarray() {
        let adapter = NpyAdapter::from_bytes(&make_npy_3x4_f64(), serde_json::json!({})).unwrap();
        // arr[1:3, 1:3] → rows 1-2, cols 1-2 → [[5,6],[9,10]]
        let slice = NDSlice::from_numpy_str("1:3,1:3").unwrap();
        let result = adapter.read(&slice).await.unwrap();
        assert_eq!(result.shape, vec![2, 2]);
        let floats: Vec<f64> = result
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(floats, vec![5.0, 6.0, 9.0, 10.0]);
    }

    #[tokio::test]
    async fn read_block_with_slice_returns_subarray() {
        let adapter = NpyAdapter::from_bytes(&make_npy_3x4_f64(), serde_json::json!({})).unwrap();
        // Block [0, 0] is the only block; slice arr[0, :] → row 0 = [0,1,2,3]
        let slice = NDSlice::from_numpy_str("0,:").unwrap();
        let result = adapter.read_block(&[0, 0], &slice).await.unwrap();
        assert_eq!(result.shape, vec![4]);
        let floats: Vec<f64> = result
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(floats, vec![0.0, 1.0, 2.0, 3.0]);
    }

    #[test]
    fn parse_simple_2d() {
        let (dtype, shape, fortran) =
            parse_header("{'descr': '<f8', 'fortran_order': False, 'shape': (3, 4), }").unwrap();
        assert_eq!(shape, vec![3, 4]);
        assert!(!fortran);
        assert_eq!(dtype.element_size(), 8);
    }

    #[tokio::test]
    async fn npy_bytes_roundtrips_through_reader() {
        // npy_bytes is the inverse of from_bytes: serialise an array, parse it
        // back, and recover the same shape/dtype/values.
        let body: Vec<u8> = (0..12u64).flat_map(|v| (v as f64).to_le_bytes()).collect();
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let array = DynNDArray::new(Bytes::from(body), dtype, vec![3, 4]);
        let raw = npy_bytes(&array);
        // Header (after the 10-byte preamble) must be 64-byte aligned.
        assert_eq!((raw.len() - array.data.len()) % 64, 0);
        let adapter = NpyAdapter::from_bytes(&raw, serde_json::json!({})).unwrap();
        assert_eq!(adapter.structure().shape, vec![3, 4]);
        let slice = NDSlice::from_numpy_str("").unwrap();
        let values: Vec<f64> = adapter
            .read(&slice)
            .await
            .unwrap()
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(values, (0..12).map(|v| v as f64).collect::<Vec<_>>());
    }

    #[test]
    fn npy_shape_formatting_matches_numpy() {
        assert_eq!(format_npy_shape(&[]), "()");
        assert_eq!(format_npy_shape(&[5]), "(5,)");
        assert_eq!(format_npy_shape(&[3, 4]), "(3, 4)");
    }

    #[tokio::test]
    async fn init_storage_creates_skeleton_under_root_and_resolves() {
        let root = tempfile::tempdir().unwrap();
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype),
            chunks: vec![vec![2], vec![3]],
            shape: vec![2, 3],
            dims: None,
            resizable: Default::default(),
        };
        let parts = vec!["outer".to_string(), "leaf".to_string()];
        let (data_uri, assets) = init_storage_npy(root.path(), &parts, &structure).unwrap();
        // URI is the cross-platform file:// form of <root>/outer/leaf.npy and
        // the skeleton exists.
        let expected = root.path().join("outer").join("leaf.npy");
        assert_eq!(
            data_uri,
            tiled_core::file_uri::path_to_file_uri(&expected).unwrap()
        );
        assert!(expected.exists());
        assert_eq!(assets.len(), 1);
        assert!(!assets[0].is_directory);
        // The skeleton is a valid zero-filled npy with the declared shape.
        let adapter = NpyAdapter::from_path(expected, serde_json::json!({})).unwrap();
        assert_eq!(adapter.structure().shape, vec![2, 3]);
        let slice = NDSlice::from_numpy_str("").unwrap();
        let zeros = adapter.read(&slice).await.unwrap();
        assert!(zeros.data.iter().all(|&b| b == 0));
    }

    #[test]
    fn init_storage_rejects_traversal_components() {
        let root = tempfile::tempdir().unwrap();
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype),
            chunks: vec![vec![1]],
            shape: vec![1],
            dims: None,
            resizable: Default::default(),
        };
        for bad in [
            vec!["..".to_string()],
            vec!["a/b".to_string()],
            vec!["".to_string()],
        ] {
            assert!(
                init_storage_npy(root.path(), &bad, &structure).is_err(),
                "expected rejection for {bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn write_block_persists_full_array_and_is_gated_by_into_writable() {
        let root = tempfile::tempdir().unwrap();
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks: vec![vec![4]],
            shape: vec![4],
            dims: None,
            resizable: Default::default(),
        };
        let (_uri, _assets) =
            init_storage_npy(root.path(), &["arr".to_string()], &structure).unwrap();
        let file = root.path().join("arr.npy");

        // Without into_writable(), the resolved adapter is read-only.
        let ro = NpyAdapter::from_path(file.clone(), serde_json::json!({})).unwrap();
        assert!(ro.as_writable().is_none());

        // With into_writable(), write_block overwrites the file with new data.
        let rw = NpyAdapter::from_path(file.clone(), serde_json::json!({})).unwrap();
        let rw = rw.into_writable();
        let writer = rw.as_writable().expect("writable");
        let body: Vec<u8> = [1.5f64, 2.5, 3.5, 4.5]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let payload = DynNDArray::new(Bytes::from(body), dtype, vec![4]);
        writer.write_block(payload, &[0]).await.unwrap();

        // Re-open and confirm the written values round-trip.
        let reopened = NpyAdapter::from_path(file, serde_json::json!({})).unwrap();
        let slice = NDSlice::from_numpy_str("").unwrap();
        let values: Vec<f64> = reopened
            .read(&slice)
            .await
            .unwrap()
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(values, vec![1.5, 2.5, 3.5, 4.5]);
    }

    #[tokio::test]
    async fn write_block_rejects_shape_mismatch_and_nonzero_block() {
        let root = tempfile::tempdir().unwrap();
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks: vec![vec![2]],
            shape: vec![2],
            dims: None,
            resizable: Default::default(),
        };
        init_storage_npy(root.path(), &["a".to_string()], &structure).unwrap();
        let rw = NpyAdapter::from_path(root.path().join("a.npy"), serde_json::json!({}))
            .unwrap()
            .into_writable();
        let writer = rw.as_writable().unwrap();
        // Wrong shape (3 != 2).
        let three = DynNDArray::new(Bytes::from(vec![0u8; 24]), dtype.clone(), vec![3]);
        assert!(writer.write_block(three, &[0]).await.is_err());
        // Non-zero block index.
        let two = DynNDArray::new(Bytes::from(vec![0u8; 16]), dtype, vec![2]);
        assert!(writer.write_block(two, &[1]).await.is_err());
    }

    #[test]
    fn rejects_fortran_order() {
        // synthetic v1 header bytes — magic + ver + 2-byte len + body.
        let header = b"{'descr': '<f8', 'fortran_order': True, 'shape': (1,), }    \n";
        let mut raw = vec![];
        raw.extend_from_slice(b"\x93NUMPY");
        raw.push(1);
        raw.push(0);
        raw.extend_from_slice(&(header.len() as u16).to_le_bytes());
        raw.extend_from_slice(header);
        raw.extend_from_slice(&0_f64.to_le_bytes());
        let result = NpyAdapter::from_bytes(&raw, serde_json::json!({}));
        assert!(matches!(result, Err(TiledError::Validation(_))));
    }
}
