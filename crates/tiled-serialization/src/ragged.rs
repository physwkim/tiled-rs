//! Ragged array serializers.
//!
//! Corresponds to `tiled/serialization/ragged.py`.
//!
//! Registered media types (matches Python `default_serialization_registry`):
//! * `application/json`  — JSON list-of-lists (`to_json`, ragged.py:70-76)
//! * `application/zip`   — Awkward zip-of-buffers (`to_zipped_buffers`, ragged.py:90-111)
//!
//! The `&[u8]` data argument to every serializer is the **UTF-8 JSON bytes**
//! of the list-of-lists produced by `RaggedData::to_json_bytes()`.
//! The `&serde_json::Value` metadata argument is the serialized
//! `RaggedStructure` (`RaggedData::structure_as_metadata()`), from which the
//! ZIP serializer reads `shape` and `data_type`.

use std::collections::HashMap;
use std::io::{Read, Write};

use tiled_core::dtype::DType;
use tiled_core::structures::{RaggedStructure, StructureFamily};

use crate::registry::{SerializationRegistry, SerializeError};

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register ragged serializers into `registry`.
///
/// Corresponds to the `@default_serialization_registry.register` decorators
/// in `tiled/serialization/ragged.py:70,90`.
pub fn register_ragged_serializers(registry: &SerializationRegistry) {
    registry.register(
        StructureFamily::Ragged,
        tiled_core::media_type::mime::JSON,
        Box::new(to_json),
    );
    registry.register(
        StructureFamily::Ragged,
        tiled_core::media_type::mime::ZIP,
        Box::new(to_zipped_buffers),
    );
}

// ---------------------------------------------------------------------------
// JSON serializer
// ---------------------------------------------------------------------------

/// Pass-through: the data bytes are already the JSON encoding of the
/// list-of-lists.
///
/// Corresponds to `tiled/serialization/ragged.py:70-76`:
/// ```python
/// def to_json(mimetype, array, metadata):
///     return safe_json_dump(array.tolist())
/// ```
fn to_json(data: &[u8], _metadata: &serde_json::Value) -> Result<bytes::Bytes, SerializeError> {
    Ok(bytes::Bytes::copy_from_slice(data))
}

// ---------------------------------------------------------------------------
// ZIP serializer
// ---------------------------------------------------------------------------

/// Produces an uncompressed ZIP file whose entries mirror what Python's
/// `to_zipped_buffers` writes (`tiled/serialization/ragged.py:90-111`):
///
/// * One file per Awkward buffer (e.g. `node0-offsets`, `node1-data`) —
///   raw little-endian numeric bytes.
/// * `"length"` — 8-byte big-endian encoding of the top-level array length
///   (`ragged.py:108`).
/// * `"form"` — JSON bytes of the Awkward form dict (`ragged.py:109`),
///   as produced by `awkward.forms.Form.to_dict()`.  The form matches
///   `RaggedStructure.awkward_form` (`tiled/structures/ragged.py:164-210`).
///
/// The `data` argument is UTF-8 JSON bytes of the list-of-lists.
/// The `metadata` argument must be the `RaggedStructure` serialized to JSON.
fn to_zipped_buffers(
    data: &[u8],
    metadata: &serde_json::Value,
) -> Result<bytes::Bytes, SerializeError> {
    let value: serde_json::Value = serde_json::from_slice(data)
        .map_err(|e| format!("ragged/zip: cannot parse JSON list-of-lists: {e}"))?;

    let structure = RaggedStructure::from_json(metadata)
        .map_err(|e| format!("ragged/zip: cannot parse RaggedStructure from metadata: {e}"))?;

    let primitive = dtype_to_primitive(&structure.data_type)
        .ok_or_else(|| format!("ragged/zip: unsupported dtype {:?}", structure.data_type))?;

    let form_json = build_awkward_form_json(&structure.shape, &primitive);

    let rows = value
        .as_array()
        .ok_or("ragged/zip: top-level JSON must be an array")?;
    let length = rows.len();

    let mut buffers: HashMap<String, Vec<u8>> = HashMap::new();
    collect_buffers(&form_json, &value, &primitive, &mut buffers)?;

    // Pack into ZIP (uncompressed, matching Python's ZIP_STORED).
    let mut out = Vec::<u8>::new();
    {
        let cursor = std::io::Cursor::new(&mut out);
        let mut zip = zip::ZipWriter::new(cursor);
        let opts = zip::write::FileOptions::<()>::default()
            .compression_method(zip::CompressionMethod::Stored);

        // Buffer files (order follows Python dict iteration — for tests we
        // sort for determinism; Python does not guarantee order either).
        let mut keys: Vec<&String> = buffers.keys().collect();
        keys.sort();
        for key in keys {
            zip.start_file(key, opts)?;
            zip.write_all(&buffers[key])?;
        }

        // "length": 8-byte big-endian (ragged.py:108)
        zip.start_file("length", opts)?;
        zip.write_all(&(length as u64).to_be_bytes())?;

        // "form": JSON bytes of the form dict (ragged.py:109)
        zip.start_file("form", opts)?;
        zip.write_all(serde_json::to_string(&form_json)?.as_bytes())?;

        zip.finish()?;
    }

    Ok(bytes::Bytes::from(out))
}

// ---------------------------------------------------------------------------
// ZIP deserializer (for round-trip testing; not registered)
// ---------------------------------------------------------------------------

/// Reconstruct a JSON list-of-lists from ZIP bytes produced by
/// [`to_zipped_buffers`].
///
/// Mirrors `tiled/serialization/ragged.py:114-128` (`from_zipped_buffers`).
pub fn from_zipped_buffers(data: &[u8]) -> Result<serde_json::Value, SerializeError> {
    let cursor = std::io::Cursor::new(data);
    let mut zip = zip::ZipArchive::new(cursor)?;

    // Read "form" entry.
    let form_json: serde_json::Value = {
        let mut f = zip.by_name("form")?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        serde_json::from_slice(&buf)?
    };

    // Read "length" entry (8-byte big-endian).
    let length: usize = {
        let mut f = zip.by_name("length")?;
        let mut buf = [0u8; 8];
        f.read_exact(&mut buf)?;
        u64::from_be_bytes(buf) as usize
    };

    // Read all buffer entries (everything that isn't "form" or "length").
    let names: Vec<String> = (0..zip.len())
        .filter_map(|i| {
            zip.by_index(i)
                .ok()
                .map(|f| f.name().to_string())
                .filter(|n| n != "form" && n != "length")
        })
        .collect();

    let mut buffers: HashMap<String, Vec<u8>> = HashMap::new();
    for name in &names {
        let mut f = zip.by_name(name)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        buffers.insert(name.clone(), buf);
    }

    reconstruct_lists(&form_json, length, &buffers)
}

// ---------------------------------------------------------------------------
// Awkward form JSON builder
// ---------------------------------------------------------------------------

/// Build an Awkward form JSON value from `shape` and `primitive`.
///
/// Matches Python's `RaggedStructure.awkward_form` property
/// (`tiled/structures/ragged.py:164-210`).
///
/// Rule (iterating dimensions in *reverse* from the innermost to axis 1):
/// * `None` dimension → ListOffsetForm (offsets i64)
/// * Fixed dimension after a variable one → RegularForm
/// * Trailing fixed dimensions → accumulate into `inner_shape` of the leaf
///   NumpyForm
///
/// Node numbering: the leaf NumpyForm gets `form_key = "node{dim}"` where
/// `dim` is the first variable dimension encountered in the reverse sweep.
/// Each wrapping form gets `form_key = "node{dim-1}"`.
fn build_awkward_form_json(shape: &[Option<usize>], primitive: &str) -> serde_json::Value {
    let ndims = shape.len();

    // Accumulate trailing fixed dimensions into inner_shape (prepend to keep
    // order; we traverse reversed so the first prepend goes outermost).
    let mut inner_shape: Vec<usize> = Vec::new();
    let mut form: Option<serde_json::Value> = None;

    for dim in (1..ndims).rev() {
        match shape[dim] {
            None => {
                // Variable-length dimension.
                if form.is_none() {
                    // Leaf NumpyForm with the accumulated inner_shape.
                    form = Some(serde_json::json!({
                        "class": "NumpyArray",
                        "primitive": primitive,
                        "inner_shape": inner_shape,
                        "parameters": {},
                        "form_key": format!("node{dim}")
                    }));
                    inner_shape = Vec::new(); // consumed
                }
                let inner = form.take().unwrap();
                form = Some(serde_json::json!({
                    "class": "ListOffsetArray",
                    "offsets": "i64",
                    "content": inner,
                    "parameters": {},
                    "form_key": format!("node{}", dim - 1)
                }));
            }
            Some(size) => {
                if form.is_some() {
                    // Fixed dimension after a variable one → RegularForm.
                    let inner = form.take().unwrap();
                    form = Some(serde_json::json!({
                        "class": "RegularArray",
                        "size": size,
                        "content": inner,
                        "parameters": {},
                        "form_key": format!("node{}", dim - 1)
                    }));
                } else {
                    // Trailing all-fixed dimension; push front so order is
                    // innermost-last (inner_shape[0] is axis 1 of the array).
                    inner_shape.insert(0, size);
                }
            }
        }
    }

    // No form yet: 1-D array or entirely fixed → a single NumpyForm.
    form.unwrap_or_else(|| {
        serde_json::json!({
            "class": "NumpyArray",
            "primitive": primitive,
            "inner_shape": inner_shape,
            "parameters": {},
            "form_key": "node0"
        })
    })
}

// ---------------------------------------------------------------------------
// Buffer builder (serialization direction)
// ---------------------------------------------------------------------------

/// Recursively populate `buffers` from a form + data JSON value.
///
/// Mirrors Python's `_buffers_from_data` inner `recurse` function
/// (`tiled/serialization/ragged.py:35-65`).
fn collect_buffers(
    form: &serde_json::Value,
    data: &serde_json::Value,
    primitive: &str,
    buffers: &mut HashMap<String, Vec<u8>>,
) -> Result<(), SerializeError> {
    let class = form["class"].as_str().unwrap_or("");
    let form_key = form["form_key"].as_str().unwrap_or("");

    match class {
        "NumpyArray" => {
            let arr = data
                .as_array()
                .ok_or_else(|| format!("NumpyArray: expected JSON array, got {data}"))?;
            let mut data_bytes: Vec<u8> =
                Vec::with_capacity(arr.len() * primitive_itemsize(primitive));
            for elem in arr {
                encode_element(elem, primitive, &mut data_bytes)?;
            }
            buffers.insert(format!("{form_key}-data"), data_bytes);
        }
        "ListOffsetArray" => {
            let arr = data
                .as_array()
                .ok_or_else(|| format!("ListOffsetArray: expected JSON array, got {data}"))?;

            // Build offsets (int64 LE prefix-sum) and flatten content.
            let mut offsets_bytes: Vec<u8> = Vec::with_capacity((arr.len() + 1) * 8);
            let mut offset: i64 = 0;
            offsets_bytes.extend_from_slice(&offset.to_le_bytes());

            let mut flat_content: Vec<serde_json::Value> = Vec::new();
            for row in arr {
                let row_arr = row
                    .as_array()
                    .ok_or("ListOffsetArray: each row must be a JSON array")?;
                offset += row_arr.len() as i64;
                offsets_bytes.extend_from_slice(&offset.to_le_bytes());
                flat_content.extend(row_arr.iter().cloned());
            }
            buffers.insert(format!("{form_key}-offsets"), offsets_bytes);

            let content_form = &form["content"];
            let flat = serde_json::Value::Array(flat_content);
            collect_buffers(content_form, &flat, primitive, buffers)?;
        }
        "RegularArray" => {
            let arr = data
                .as_array()
                .ok_or_else(|| format!("RegularArray: expected JSON array, got {data}"))?;
            let size = form["size"].as_u64().unwrap_or(0) as usize;
            let mut flat: Vec<serde_json::Value> = Vec::new();
            for row in arr {
                let row_arr = row
                    .as_array()
                    .ok_or("RegularArray: each row must be a JSON array")?;
                if row_arr.len() != size {
                    return Err(format!(
                        "RegularArray row width mismatch: expected {size}, got {}",
                        row_arr.len()
                    )
                    .into());
                }
                flat.extend(row_arr.iter().cloned());
            }
            let content_form = &form["content"];
            let flat = serde_json::Value::Array(flat);
            collect_buffers(content_form, &flat, primitive, buffers)?;
        }
        other => return Err(format!("unsupported Awkward class: {other}").into()),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Buffer reconstructor (deserialization direction)
// ---------------------------------------------------------------------------

/// Reconstruct JSON list-of-lists from a form + length + buffers map.
fn reconstruct_lists(
    form: &serde_json::Value,
    length: usize,
    buffers: &HashMap<String, Vec<u8>>,
) -> Result<serde_json::Value, SerializeError> {
    let class = form["class"].as_str().unwrap_or("");
    let form_key = form["form_key"].as_str().unwrap_or("");

    match class {
        "NumpyArray" => {
            let primitive = form["primitive"].as_str().unwrap_or("float64");
            let key = format!("{form_key}-data");
            let buf = buffers
                .get(&key)
                .ok_or_else(|| format!("missing buffer entry '{key}'"))?;
            decode_numpy_array(primitive, buf, length)
        }
        "ListOffsetArray" => {
            let key = format!("{form_key}-offsets");
            let offsets_buf = buffers
                .get(&key)
                .ok_or_else(|| format!("missing buffer entry '{key}'"))?;
            if offsets_buf.len() < (length + 1) * 8 {
                return Err(format!(
                    "offsets buffer too short: need {} bytes, got {}",
                    (length + 1) * 8,
                    offsets_buf.len()
                )
                .into());
            }
            let offsets: Vec<i64> = offsets_buf
                .chunks_exact(8)
                .take(length + 1)
                .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                .collect();

            let content_len = *offsets.last().unwrap_or(&0) as usize;
            let content_form = &form["content"];
            let flat = reconstruct_lists(content_form, content_len, buffers)?;
            let flat_arr = flat.as_array().ok_or("content must be a JSON array")?;

            let mut rows: Vec<serde_json::Value> = Vec::with_capacity(length);
            for i in 0..length {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                let row = flat_arr[start..end].to_vec();
                rows.push(serde_json::Value::Array(row));
            }
            Ok(serde_json::Value::Array(rows))
        }
        "RegularArray" => {
            let size = form["size"].as_u64().unwrap_or(0) as usize;
            let content_form = &form["content"];
            let flat = reconstruct_lists(content_form, length * size, buffers)?;
            let flat_arr = flat
                .as_array()
                .ok_or("RegularArray content must be a JSON array")?;

            let mut rows: Vec<serde_json::Value> = Vec::with_capacity(length);
            for i in 0..length {
                let row = flat_arr[i * size..(i + 1) * size].to_vec();
                rows.push(serde_json::Value::Array(row));
            }
            Ok(serde_json::Value::Array(rows))
        }
        other => Err(format!("unsupported Awkward class in reconstruction: {other}").into()),
    }
}

// ---------------------------------------------------------------------------
// Dtype helpers
// ---------------------------------------------------------------------------

/// Map `DType` → Awkward primitive name.
///
/// Corresponds to Python `awkward.types.numpytype.dtype_to_primitive()`
/// which maps numpy dtypes to awkward primitive strings.
fn dtype_to_primitive(dtype: &DType) -> Option<String> {
    match dtype {
        DType::Builtin(b) => builtin_dtype_to_primitive(b),
        DType::Struct(_) => None,
    }
}

fn builtin_dtype_to_primitive(b: &tiled_core::dtype::BuiltinDType) -> Option<String> {
    use tiled_core::dtype::Kind;
    // Boolean has no itemsize requirement (always 1 byte).
    if b.kind == Kind::Boolean {
        return Some("bool".to_string());
    }
    Some(
        match (b.kind, b.itemsize) {
            (Kind::Integer, 1) => "int8",
            (Kind::Integer, 2) => "int16",
            (Kind::Integer, 4) => "int32",
            (Kind::Integer, 8) => "int64",
            (Kind::UnsignedInteger, 1) => "uint8",
            (Kind::UnsignedInteger, 2) => "uint16",
            (Kind::UnsignedInteger, 4) => "uint32",
            (Kind::UnsignedInteger, 8) => "uint64",
            (Kind::Float, 2) => "float16",
            (Kind::Float, 4) => "float32",
            (Kind::Float, 8) => "float64",
            (Kind::ComplexFloat, 8) => "complex64",
            (Kind::ComplexFloat, 16) => "complex128",
            _ => return None,
        }
        .to_string(),
    )
}

fn primitive_itemsize(primitive: &str) -> usize {
    match primitive {
        "bool" | "int8" | "uint8" => 1,
        "int16" | "uint16" | "float16" => 2,
        "int32" | "uint32" | "float32" => 4,
        "int64" | "uint64" | "float64" | "complex64" => 8,
        "complex128" => 16,
        _ => 8, // fallback
    }
}

/// Encode a single JSON scalar into `out` as `primitive` little-endian bytes.
fn encode_element(
    elem: &serde_json::Value,
    primitive: &str,
    out: &mut Vec<u8>,
) -> Result<(), SerializeError> {
    match primitive {
        "float64" => {
            let v = elem
                .as_f64()
                .ok_or_else(|| format!("expected float64, got {elem}"))?;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "float32" => {
            let v = elem
                .as_f64()
                .ok_or_else(|| format!("expected float, got {elem}"))? as f32;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "int8" => {
            let v = elem
                .as_i64()
                .ok_or_else(|| format!("expected int8, got {elem}"))? as i8;
            out.push(v as u8);
        }
        "int16" => {
            let v = elem
                .as_i64()
                .ok_or_else(|| format!("expected int16, got {elem}"))? as i16;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "int32" => {
            let v = elem
                .as_i64()
                .ok_or_else(|| format!("expected int32, got {elem}"))? as i32;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "int64" => {
            let v = elem
                .as_i64()
                .ok_or_else(|| format!("expected int64, got {elem}"))?;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "uint8" => {
            let v = elem
                .as_u64()
                .ok_or_else(|| format!("expected uint8, got {elem}"))? as u8;
            out.push(v);
        }
        "uint16" => {
            let v = elem
                .as_u64()
                .ok_or_else(|| format!("expected uint16, got {elem}"))? as u16;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "uint32" => {
            let v = elem
                .as_u64()
                .ok_or_else(|| format!("expected uint32, got {elem}"))? as u32;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "uint64" => {
            let v = elem
                .as_u64()
                .ok_or_else(|| format!("expected uint64, got {elem}"))?;
            out.extend_from_slice(&v.to_le_bytes());
        }
        "bool" => {
            let v = elem
                .as_bool()
                .ok_or_else(|| format!("expected bool, got {elem}"))?;
            out.push(v as u8);
        }
        other => return Err(format!("unsupported primitive type '{other}'").into()),
    }
    Ok(())
}

/// Decode raw buffer bytes as a flat JSON array.
fn decode_numpy_array(
    primitive: &str,
    buf: &[u8],
    length: usize,
) -> Result<serde_json::Value, SerializeError> {
    let itemsize = primitive_itemsize(primitive);
    if length * itemsize > buf.len() {
        return Err(format!(
            "buffer too short: need {}, got {} bytes (primitive={primitive}, length={length})",
            length * itemsize,
            buf.len()
        )
        .into());
    }
    let mut elems: Vec<serde_json::Value> = Vec::with_capacity(length);
    for i in 0..length {
        let slice = &buf[i * itemsize..(i + 1) * itemsize];
        let v = decode_one(slice, primitive)?;
        elems.push(v);
    }
    Ok(serde_json::Value::Array(elems))
}

fn decode_one(bytes: &[u8], primitive: &str) -> Result<serde_json::Value, SerializeError> {
    Ok(match primitive {
        "float64" => {
            let v = f64::from_le_bytes(bytes.try_into().map_err(|_| "float64 slice wrong size")?);
            serde_json::json!(v)
        }
        "float32" => {
            let v = f32::from_le_bytes(bytes.try_into().map_err(|_| "float32 slice wrong size")?);
            serde_json::json!(v as f64)
        }
        "int8" => serde_json::json!(bytes[0] as i8),
        "int16" => serde_json::json!(i16::from_le_bytes(bytes.try_into().unwrap())),
        "int32" => serde_json::json!(i32::from_le_bytes(bytes.try_into().unwrap())),
        "int64" => serde_json::json!(i64::from_le_bytes(bytes.try_into().unwrap())),
        "uint8" => serde_json::json!(bytes[0]),
        "uint16" => serde_json::json!(u16::from_le_bytes(bytes.try_into().unwrap())),
        "uint32" => serde_json::json!(u32::from_le_bytes(bytes.try_into().unwrap())),
        "uint64" => serde_json::json!(u64::from_le_bytes(bytes.try_into().unwrap())),
        "bool" => serde_json::json!(bytes[0] != 0),
        other => return Err(format!("unsupported primitive in decode: {other}").into()),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_core::structures::{RaggedStructure, Resizable};

    fn f64_structure(n_rows: usize) -> RaggedStructure {
        RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![Some(n_rows), None],
            size: 0, // not checked by serializers
            chunks: vec![Some(vec![n_rows]), None],
            dims: None,
            resizable: Resizable::default(),
        }
    }

    fn sample_json() -> serde_json::Value {
        serde_json::json!([[1.0, 2.0, 3.0], [4.0], [5.0, 6.0]])
    }

    // ------------------------------------------------------------------
    // ragged_form_json_matches_python_for_1d_ragged_float64
    // Verifies build_awkward_form_json matches the exact dict Python's
    // `RaggedStructure.awkward_form.to_dict()` produces for shape=[3, None].
    // Python reference:
    //   {"class": "ListOffsetArray", "offsets": "i64",
    //    "content": {"class": "NumpyArray", "primitive": "float64",
    //                "inner_shape": [], "parameters": {}, "form_key": "node1"},
    //    "parameters": {}, "form_key": "node0"}
    // ------------------------------------------------------------------

    #[test]
    fn ragged_form_json_matches_python_for_1d_ragged_float64() {
        let shape = vec![Some(3usize), None];
        let form = build_awkward_form_json(&shape, "float64");

        assert_eq!(form["class"], "ListOffsetArray");
        assert_eq!(form["offsets"], "i64");
        assert_eq!(form["form_key"], "node0");
        assert_eq!(form["parameters"], serde_json::json!({}));

        let content = &form["content"];
        assert_eq!(content["class"], "NumpyArray");
        assert_eq!(content["primitive"], "float64");
        assert_eq!(content["inner_shape"], serde_json::json!([]));
        assert_eq!(content["parameters"], serde_json::json!({}));
        assert_eq!(content["form_key"], "node1");
    }

    // ------------------------------------------------------------------
    // ragged_form_json_for_fixed_1d
    // shape=[N] → pure NumpyForm ("node0")
    // ------------------------------------------------------------------

    #[test]
    fn ragged_form_json_for_fixed_1d() {
        let form = build_awkward_form_json(&[Some(5)], "int64");
        assert_eq!(form["class"], "NumpyArray");
        assert_eq!(form["primitive"], "int64");
        assert_eq!(form["form_key"], "node0");
        assert_eq!(form["inner_shape"], serde_json::json!([]));
    }

    // ------------------------------------------------------------------
    // ragged_to_json_pass_through
    // JSON serializer returns the input bytes unchanged.
    // ------------------------------------------------------------------

    #[test]
    fn ragged_to_json_pass_through() {
        let input = serde_json::to_vec(&sample_json()).unwrap();
        let meta = serde_json::json!({});
        let out = to_json(&input, &meta).expect("to_json must not fail");
        let back: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(back, sample_json());
    }

    // ------------------------------------------------------------------
    // ragged_to_zip_contains_expected_files
    // ZIP must contain "form", "length", "node0-offsets", "node1-data".
    // ------------------------------------------------------------------

    #[test]
    fn ragged_to_zip_contains_expected_files() {
        let json_bytes = serde_json::to_vec(&sample_json()).unwrap();
        let meta = serde_json::to_value(f64_structure(3)).unwrap();

        let zip_bytes =
            to_zipped_buffers(&json_bytes, &meta).expect("to_zipped_buffers must succeed");

        let cursor = std::io::Cursor::new(zip_bytes.as_ref());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        let mut names: Vec<String> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().to_string())
            .collect();
        names.sort();
        assert!(names.contains(&"form".to_string()), "missing 'form' entry");
        assert!(
            names.contains(&"length".to_string()),
            "missing 'length' entry"
        );
        assert!(
            names.contains(&"node0-offsets".to_string()),
            "missing 'node0-offsets'"
        );
        assert!(
            names.contains(&"node1-data".to_string()),
            "missing 'node1-data'"
        );
    }

    // ------------------------------------------------------------------
    // ragged_to_zip_length_bytes_big_endian
    // "length" entry is the top-level count as 8-byte big-endian integer
    // (ragged.py:108).
    // ------------------------------------------------------------------

    #[test]
    fn ragged_to_zip_length_bytes_big_endian() {
        use std::io::Read;

        let json_bytes = serde_json::to_vec(&sample_json()).unwrap();
        let meta = serde_json::to_value(f64_structure(3)).unwrap();
        let zip_bytes = to_zipped_buffers(&json_bytes, &meta).unwrap();

        let cursor = std::io::Cursor::new(zip_bytes.as_ref());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        let mut f = zip.by_name("length").unwrap();
        let mut buf = [0u8; 8];
        f.read_exact(&mut buf).unwrap();
        let length = u64::from_be_bytes(buf);
        assert_eq!(length, 3, "length must be the top-level row count");
    }

    // ------------------------------------------------------------------
    // ragged_to_zip_offsets_match_python
    // Offsets for [[1,2,3],[4],[5,6]] must be [0,3,4,6] as LE int64.
    // Python: node0-offsets = [0, 3, 4, 6] as int64 LE.
    // ------------------------------------------------------------------

    #[test]
    fn ragged_to_zip_offsets_match_python() {
        use std::io::Read;

        let json_bytes = serde_json::to_vec(&sample_json()).unwrap();
        let meta = serde_json::to_value(f64_structure(3)).unwrap();
        let zip_bytes = to_zipped_buffers(&json_bytes, &meta).unwrap();

        let cursor = std::io::Cursor::new(zip_bytes.as_ref());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        let mut f = zip.by_name("node0-offsets").unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();

        let offsets: Vec<i64> = buf
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        // Python reference: [0, 3, 4, 6]
        assert_eq!(offsets, vec![0i64, 3, 4, 6]);
    }

    // ------------------------------------------------------------------
    // ragged_to_zip_data_matches_python
    // Data for [[1.0,2.0,3.0],[4.0],[5.0,6.0]] must be [1,2,3,4,5,6] LE f64.
    // Python: node1-data dtype=float64 values=[1.0,2.0,3.0,4.0,5.0,6.0].
    // ------------------------------------------------------------------

    #[test]
    fn ragged_to_zip_data_matches_python() {
        use std::io::Read;

        let json_bytes = serde_json::to_vec(&sample_json()).unwrap();
        let meta = serde_json::to_value(f64_structure(3)).unwrap();
        let zip_bytes = to_zipped_buffers(&json_bytes, &meta).unwrap();

        let cursor = std::io::Cursor::new(zip_bytes.as_ref());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        let mut f = zip.by_name("node1-data").unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();

        let data: Vec<f64> = buf
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        // Python reference: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        assert_eq!(data, vec![1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0]);
    }

    // ------------------------------------------------------------------
    // ragged_zip_round_trip
    // serialize(data) → deserialize → same list-of-lists
    // Boundary: non-empty 1D ragged float64 array
    // ------------------------------------------------------------------

    #[test]
    fn ragged_zip_round_trip() {
        let original = serde_json::json!([[1.0, 2.0, 3.0], [4.0], [5.0, 6.0]]);
        let json_bytes = serde_json::to_vec(&original).unwrap();
        let meta = serde_json::to_value(f64_structure(3)).unwrap();

        let zip_bytes = to_zipped_buffers(&json_bytes, &meta).unwrap();
        let reconstructed = from_zipped_buffers(&zip_bytes).unwrap();

        assert_eq!(reconstructed, original);
    }

    // ------------------------------------------------------------------
    // ragged_zip_round_trip_single_row
    // Boundary: single-row ragged array
    // ------------------------------------------------------------------

    #[test]
    fn ragged_zip_round_trip_single_row() {
        let original = serde_json::json!([[42.0, 43.0]]);
        let json_bytes = serde_json::to_vec(&original).unwrap();
        let meta = serde_json::to_value(f64_structure(1)).unwrap();

        let zip_bytes = to_zipped_buffers(&json_bytes, &meta).unwrap();
        let reconstructed = from_zipped_buffers(&zip_bytes).unwrap();
        assert_eq!(reconstructed, original);
    }

    // ------------------------------------------------------------------
    // ragged_zip_round_trip_empty_rows
    // Boundary: some rows have zero elements
    // ------------------------------------------------------------------

    #[test]
    fn ragged_zip_round_trip_empty_rows() {
        let original = serde_json::json!([[], [1.0, 2.0], []]);
        let json_bytes = serde_json::to_vec(&original).unwrap();
        let meta = serde_json::to_value(f64_structure(3)).unwrap();

        let zip_bytes = to_zipped_buffers(&json_bytes, &meta).unwrap();
        let reconstructed = from_zipped_buffers(&zip_bytes).unwrap();
        assert_eq!(reconstructed, original);
    }

    // ------------------------------------------------------------------
    // ragged_zip_round_trip_int64
    // Boundary: non-float64 primitive (int64)
    // ------------------------------------------------------------------

    #[test]
    fn ragged_zip_round_trip_int64() {
        let structure = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Integer, 8)),
            shape: vec![Some(2), None],
            size: 5,
            chunks: vec![Some(vec![2]), None],
            dims: None,
            resizable: Resizable::default(),
        };
        let original = serde_json::json!([[1, 2, 3], [4, 5]]);
        let json_bytes = serde_json::to_vec(&original).unwrap();
        let meta = serde_json::to_value(structure).unwrap();

        let zip_bytes = to_zipped_buffers(&json_bytes, &meta).unwrap();
        let reconstructed = from_zipped_buffers(&zip_bytes).unwrap();
        assert_eq!(reconstructed, original);
    }

    // ------------------------------------------------------------------
    // ragged_registry_dispatch_json_and_zip
    // Both media types must be registered and dispatchable.
    // ------------------------------------------------------------------

    #[test]
    fn ragged_registry_dispatch_json_and_zip() {
        let reg = crate::registry::SerializationRegistry::new();
        register_ragged_serializers(&reg);

        assert!(
            reg.dispatch(StructureFamily::Ragged, tiled_core::media_type::mime::JSON)
                .is_some(),
            "application/json must be registered for ragged"
        );
        assert!(
            reg.dispatch(StructureFamily::Ragged, tiled_core::media_type::mime::ZIP)
                .is_some(),
            "application/zip must be registered for ragged"
        );
    }
}
