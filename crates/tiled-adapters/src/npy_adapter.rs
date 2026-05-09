//! NPY (NumPy `.npy`) array adapter.
//!
//! Reads a `.npy` file off disk into memory once (resource-cached by the
//! adapter instance) and exposes it as an [`ArrayAdapterRead`]. Header
//! parsing follows the NPY 1.0/2.0/3.0 spec — version 2+ files use a
//! 4-byte header length so we accept both.

use std::path::PathBuf;

use bytes::Bytes;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

pub struct NpyAdapter {
    array: DynNDArray,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl NpyAdapter {
    /// Open a `.npy` file at `path` and parse the header.
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        let raw = std::fs::read(&path).map_err(|e| {
            TiledError::Internal(format!("read {}: {e}", path.display()))
        })?;
        Self::from_bytes(&raw, metadata)
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
    fn read<'a>(&'a self, _slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { Ok(self.array.clone()) })
    }
    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        _slice: &'a NDSlice,
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
            Ok(self.array.clone())
        })
    }
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
        shape.push(t.parse::<usize>().map_err(|_| {
            TiledError::Validation(format!("bad shape element: {t}"))
        })?);
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

    #[test]
    fn parse_simple_2d() {
        let (dtype, shape, fortran) =
            parse_header("{'descr': '<f8', 'fortran_order': False, 'shape': (3, 4), }")
                .unwrap();
        assert_eq!(shape, vec![3, 4]);
        assert!(!fortran);
        assert_eq!(dtype.element_size(), 8);
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
