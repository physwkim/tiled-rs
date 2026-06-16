//! Array serializers.
//!
//! Corresponds to `tiled/serialization/array.py`.

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::SerializationRegistry;

/// Register built-in array serializers.
pub fn register_array_serializers(registry: &SerializationRegistry) {
    // application/octet-stream → raw bytes (zero-copy)
    registry.register(
        StructureFamily::Array,
        mime::OCTET_STREAM,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );

    // CSV for 1-D/2-D arrays. Python registers the SAME serialize_csv under four
    // media types (array.py:49-56), so `Accept: text/plain` / `?format=txt` and
    // the Excel / x-comma-separated aliases all yield CSV rather than falling to
    // the octet-stream default.
    for media_type in [
        mime::CSV,
        "text/x-comma-separated-values",
        mime::PLAIN,
        mime::EXCEL,
    ] {
        registry.register(
            StructureFamily::Array,
            media_type,
            Box::new(serialize_array_csv),
        );
    }

    // text/html: try PNG embed, fall back to CSV (mirrors Python serialize_html, array.py:143-163).
    registry.register(
        StructureFamily::Array,
        "text/html",
        Box::new(serialize_array_html),
    );

    // application/json: nested row-major JSON array (Python array.py:33-38,
    // `safe_json_dump(array)`). Registered under the orjson guard in Python,
    // which is effectively always present → register unconditionally.
    registry.register(
        StructureFamily::Array,
        mime::JSON,
        Box::new(serialize_array_json),
    );

    // Sparse arrays also use octet-stream
    registry.register(
        StructureFamily::Sparse,
        mime::OCTET_STREAM,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );
}

/// Format a float as a string, preserving a trailing ".0" for whole-number
/// values so the output matches `numpy.savetxt(fmt="%s")`.
///
/// `f64::to_string()` (Rust Display/Ryu) emits "1" for 1.0; Python's
/// `str(numpy.float64(1.0))` emits "1.0".  Append ".0" when the Ryu
/// output contains no '.', 'e'/'E', or any letter (NaN/inf already have
/// letters and must NOT be modified).
fn ensure_decimal(s: String) -> String {
    if s.bytes().all(|b| b == b'-' || b.is_ascii_digit()) {
        format!("{s}.0")
    } else {
        s
    }
}

/// CSV serializer for 1-D/2-D arrays (Python `serialize_csv`, array.py:41-46).
fn serialize_array_csv(
    data: &[u8],
    metadata: &serde_json::Value,
) -> Result<bytes::Bytes, crate::registry::SerializeError> {
    // metadata: {"itemsize": N, "kind": "f"|"i"|"u"|..., "byteorder": "<"|">"|"|", "shape": [...]}
    // 1-D → one value per line
    // 2-D → rows of comma-separated values (matches Python tiled CSV)
    let itemsize = metadata
        .get("itemsize")
        .and_then(|v| v.as_u64())
        .unwrap_or(8) as usize;
    if itemsize == 0 {
        return Err("itemsize must be > 0".into());
    }
    let kind = metadata.get("kind").and_then(|v| v.as_str()).unwrap_or("f");
    let shape: Vec<usize> = metadata
        .get("shape")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();
    // Python `serialize_csv` raises `UnsupportedShape` for ndim > 2
    // (array.py:42-43) — it has NO flatten fallback. Match it: a >2-D
    // array must error (→ HTTP 406), not silently reshape to one column.
    if shape.len() > 2 {
        return Err(crate::registry::UnsupportedShape {
            shape: shape.clone(),
        }
        .into());
    }
    let big_endian = metadata
        .get("byteorder")
        .and_then(|v| v.as_str())
        .unwrap_or("<")
        == ">";

    let format_value = |bytes: &[u8]| -> String {
        match (kind, itemsize) {
            ("f", 8) => {
                let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                if big_endian {
                    b.reverse();
                }
                ensure_decimal(f64::from_le_bytes(b).to_string())
            }
            ("f", 4) => {
                let mut b: [u8; 4] = bytes.try_into().unwrap_or([0u8; 4]);
                if big_endian {
                    b.reverse();
                }
                ensure_decimal(f32::from_le_bytes(b).to_string())
            }
            ("i", 8) => {
                let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                if big_endian {
                    b.reverse();
                }
                i64::from_le_bytes(b).to_string()
            }
            ("i", 4) => {
                let mut b: [u8; 4] = bytes.try_into().unwrap_or([0u8; 4]);
                if big_endian {
                    b.reverse();
                }
                i32::from_le_bytes(b).to_string()
            }
            ("i", 2) => {
                let mut b: [u8; 2] = bytes.try_into().unwrap_or([0u8; 2]);
                if big_endian {
                    b.reverse();
                }
                i16::from_le_bytes(b).to_string()
            }
            ("i", 1) => i8::from_le_bytes(bytes.try_into().unwrap_or([0u8; 1])).to_string(),
            ("u", 8) => {
                let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                if big_endian {
                    b.reverse();
                }
                u64::from_le_bytes(b).to_string()
            }
            ("u", 4) => {
                let mut b: [u8; 4] = bytes.try_into().unwrap_or([0u8; 4]);
                if big_endian {
                    b.reverse();
                }
                u32::from_le_bytes(b).to_string()
            }
            ("u", 2) => {
                let mut b: [u8; 2] = bytes.try_into().unwrap_or([0u8; 2]);
                if big_endian {
                    b.reverse();
                }
                u16::from_le_bytes(b).to_string()
            }
            ("u", 1) => u8::from_le_bytes(bytes.try_into().unwrap_or([0u8; 1])).to_string(),
            ("b", _) => (bytes.iter().any(|&b| b != 0)).to_string(),
            ("c", _) if itemsize >= 2 => {
                // Complex float: real then imaginary, each `itemsize/2` bytes.
                let half = itemsize / 2;
                if bytes.len() < itemsize {
                    return format!("unsupported-complex{itemsize}");
                }
                match half {
                    8 => {
                        let mut re = [0u8; 8];
                        let mut im = [0u8; 8];
                        re.copy_from_slice(&bytes[..8]);
                        im.copy_from_slice(&bytes[8..16]);
                        if big_endian {
                            re.reverse();
                            im.reverse();
                        }
                        format!("({}+{}j)", f64::from_le_bytes(re), f64::from_le_bytes(im))
                    }
                    4 => {
                        let mut re = [0u8; 4];
                        let mut im = [0u8; 4];
                        re.copy_from_slice(&bytes[..4]);
                        im.copy_from_slice(&bytes[4..8]);
                        if big_endian {
                            re.reverse();
                            im.reverse();
                        }
                        format!("({}+{}j)", f32::from_le_bytes(re), f32::from_le_bytes(im))
                    }
                    _ => format!("unsupported-complex{itemsize}"),
                }
            }
            ("M", 8) | ("m", 8) => {
                // Datetime/timedelta: underlying i64 epoch value in dtype units.
                let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                if big_endian {
                    b.reverse();
                }
                i64::from_le_bytes(b).to_string()
            }
            ("S", _) => {
                // Fixed-length byte string: decode as UTF-8, strip trailing nulls.
                std::str::from_utf8(bytes)
                    .unwrap_or("")
                    .trim_end_matches('\0')
                    .to_string()
            }
            ("U", _) => {
                // UCS-4 (4 bytes per character); honor byte order per character.
                bytes
                    .chunks(4)
                    .filter_map(|c| {
                        let arr: [u8; 4] = c.try_into().ok()?;
                        let cp = if big_endian {
                            u32::from_be_bytes(arr)
                        } else {
                            u32::from_le_bytes(arr)
                        };
                        if cp == 0 {
                            return None;
                        }
                        char::from_u32(cp)
                    })
                    .collect::<String>()
            }
            _ => format!("unsupported dtype {kind}{itemsize}"),
        }
    };

    let num_elements = data.len() / itemsize;
    let mut output = String::new();

    // 2-D: rows × cols layout.
    if shape.len() == 2 {
        let cols = shape[1];
        for row in 0..shape[0] {
            if row > 0 {
                output.push('\n');
            }
            for col in 0..cols {
                if col > 0 {
                    output.push(',');
                }
                let i = row * cols + col;
                let start = i * itemsize;
                let end = start + itemsize;
                if end > data.len() {
                    break;
                }
                output.push_str(&format_value(&data[start..end]));
            }
        }
    } else {
        // 1-D or fallback: one value per line, row-major.
        for i in 0..num_elements {
            let start = i * itemsize;
            let end = start + itemsize;
            if end > data.len() {
                break;
            }
            if i > 0 {
                output.push('\n');
            }
            output.push_str(&format_value(&data[start..end]));
        }
    }

    Ok(bytes::Bytes::from(output))
}

/// HTML serializer for arrays (Python `serialize_html`, array.py:143-163).
///
/// Tries to render the array as a PNG image embedded in an `<img>` data URL.
/// Falls back to a plain CSV body when PNG encoding is unavailable or fails.
fn serialize_array_html(
    data: &[u8],
    metadata: &serde_json::Value,
) -> Result<bytes::Bytes, crate::registry::SerializeError> {
    #[cfg(feature = "image")]
    {
        if let Ok(png_bytes) = crate::image_array::encode_array_png(data, metadata) {
            use base64::Engine as _;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&png_bytes);
            return Ok(bytes::Bytes::from(format!(
                r#"<html><body><img src="data:image/png;base64,{b64}"/></body></html>"#
            )));
        }
    }
    let csv = serialize_array_csv(data, metadata)?;
    let text = String::from_utf8_lossy(&csv);
    Ok(bytes::Bytes::from(format!(
        "<html><body>{text}</body></html>"
    )))
}

/// JSON serializer for arrays (Python `array.py:33-38`, `safe_json_dump(array)`).
///
/// orjson serializes a numpy array to a nested, row-major JSON array whose
/// nesting matches the shape (a 1-D array → `[v0, v1, ...]`, a 2-D array →
/// `[[...], [...]]`, a 0-D array → the bare scalar). Floating NaN/inf become
/// JSON `null` (orjson's float rule, mirrored by `serde_json::From<f64>`).
///
/// Scope: the numeric and boolean dtypes the array adapters emit (`f`/`i`/`u`/
/// `b`), plus `datetime64` (`M`) which orjson renders as ISO-8601 strings (see
/// `datetime64_to_json`). A complex/timedelta/string array is a hard error
/// rather than a silent mis-encode — orjson's numpy fast-path likewise rejects
/// those dtypes (timedelta64/complex are not in its `OPT_SERIALIZE_NUMPY` set),
/// so Python also fails on them.
fn serialize_array_json(
    data: &[u8],
    metadata: &serde_json::Value,
) -> Result<bytes::Bytes, crate::registry::SerializeError> {
    let itemsize = metadata
        .get("itemsize")
        .and_then(|v| v.as_u64())
        .unwrap_or(8) as usize;
    if itemsize == 0 {
        return Err("itemsize must be > 0".into());
    }
    let kind = metadata.get("kind").and_then(|v| v.as_str()).unwrap_or("f");
    let shape: Vec<usize> = metadata
        .get("shape")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();
    let big_endian = metadata
        .get("byteorder")
        .and_then(|v| v.as_str())
        .unwrap_or("<")
        == ">";
    // datetime64 unit, e.g. "[ns]" → "ns"; only consulted for kind 'M'.
    let dt_unit: Option<&str> = metadata
        .get("dt_units")
        .and_then(|v| v.as_str())
        .map(|s| s.trim_start_matches('[').trim_end_matches(']'));

    let decode = |bytes: &[u8]| -> Result<serde_json::Value, crate::registry::SerializeError> {
        macro_rules! le {
            ($t:ty, $n:literal) => {{
                let mut b: [u8; $n] = bytes.try_into().unwrap_or([0u8; $n]);
                if big_endian {
                    b.reverse();
                }
                <$t>::from_le_bytes(b)
            }};
        }
        Ok(match (kind, itemsize) {
            // `From<f64>`/`From<f32>` map NaN/inf to Value::Null (orjson rule).
            ("f", 8) => serde_json::Value::from(le!(f64, 8)),
            ("f", 4) => serde_json::Value::from(le!(f32, 4)),
            ("i", 8) => serde_json::Value::from(le!(i64, 8)),
            ("i", 4) => serde_json::Value::from(le!(i32, 4)),
            ("i", 2) => serde_json::Value::from(le!(i16, 2)),
            ("i", 1) => {
                serde_json::Value::from(i8::from_le_bytes(bytes.try_into().unwrap_or([0u8; 1])))
            }
            ("u", 8) => serde_json::Value::from(le!(u64, 8)),
            ("u", 4) => serde_json::Value::from(le!(u32, 4)),
            ("u", 2) => serde_json::Value::from(le!(u16, 2)),
            ("u", 1) => serde_json::Value::from(bytes.first().copied().unwrap_or(0)),
            ("b", _) => serde_json::Value::from(bytes.iter().any(|&b| b != 0)),
            // datetime64 is always an 8-byte int64 tick count; render ISO-8601.
            ("M", 8) => datetime64_to_json(le!(i64, 8), dt_unit)?,
            _ => {
                return Err(format!(
                    "application/json array serializer does not support dtype {kind}{itemsize}"
                )
                .into());
            }
        })
    };

    let num_elements = data.len() / itemsize;
    let expected: usize = shape.iter().product();
    // `shape.iter().product()` is 1 for a 0-D array ([]), which holds exactly
    // one element. Any other mismatch means metadata and bytes disagree.
    if expected != num_elements {
        return Err(format!(
            "array shape {shape:?} ({expected} elements) does not match data length \
             ({num_elements} elements)"
        )
        .into());
    }

    let mut flat = Vec::with_capacity(num_elements);
    for i in 0..num_elements {
        let start = i * itemsize;
        flat.push(decode(&data[start..start + itemsize])?);
    }

    let nested = nest_array(&flat, &shape);
    let body = serde_json::to_vec(&nested).map_err(|e| format!("json encode: {e}"))?;
    Ok(bytes::Bytes::from(body))
}

/// Out-of-representable-range error for a `datetime64` tick count.
fn dt_oor(value: i64, unit: &str) -> crate::registry::SerializeError {
    format!("datetime64 value {value} ({unit}) is out of the representable range").into()
}

/// Render a numpy `datetime64` tick count as the JSON value orjson produces for
/// it under `OPT_SERIALIZE_NUMPY` (the array `application/json` path is
/// `safe_json_dump(array)`, array.py:33-38). `value` is the count of `unit`s
/// since the naive `1970-01-01T00:00:00` epoch.
///
/// Format — verified byte-for-byte against orjson 3.11.5: a full RFC 3339
/// datetime `YYYY-MM-DDTHH:MM:SS` for every unit (orjson does NOT truncate to
/// the unit's resolution the way `str(numpy.datetime64)` does — e.g. a `[D]`
/// value still renders `...T00:00:00`), with a fractional part only when the
/// microsecond component is non-zero, then exactly 6 digits. orjson formats at
/// microsecond resolution, so a `[ns]` sub-microsecond remainder is TRUNCATED
/// (`.123456789` → `.123456`). No timezone suffix (datetime64 is naive).
///
/// NaT (the `i64::MIN` sentinel) → JSON `null`. This is a deliberate, documented
/// deviation from orjson, whose NaT handling is broken and unit-dependent (for
/// ns/us where the sentinel lands in datetime's range it emits a garbage
/// in-range instant, e.g. `1677-09-21T00:12:43.145224`; for s/ms it raises →
/// HTTP 500). `null` matches the table path's `_series_to_json_safe`
/// (NaT → None, table.py:113-139) and the numeric arms' missing→null rule.
///
/// A missing or unrecognised unit (including finer-than-nanosecond units chrono
/// cannot represent) is an error so the caller falls through to the
/// unsupported-dtype path rather than emitting wrong data.
fn datetime64_to_json(
    value: i64,
    unit: Option<&str>,
) -> Result<serde_json::Value, crate::registry::SerializeError> {
    use chrono::{Duration, NaiveDate, NaiveDateTime, Timelike};

    // numpy NaT → null (see doc: deliberate deviation from orjson).
    if value == i64::MIN {
        return Ok(serde_json::Value::Null);
    }
    let unit = unit.ok_or_else(|| -> crate::registry::SerializeError {
        "datetime64 array is missing its dt_units; cannot decode".into()
    })?;
    let epoch: NaiveDateTime = NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .expect("1970-01-01T00:00:00 is a valid datetime");

    // The unit only affects how `value` maps to an instant; the output FORMAT is
    // uniform across units (full datetime + optional µs), so each arm yields a
    // NaiveDateTime and the formatting below is shared.
    let from_delta = |d: Option<Duration>| {
        d.and_then(|d| epoch.checked_add_signed(d))
            .ok_or_else(|| dt_oor(value, unit))
    };
    let from_ym = |year: i64, month: u32| {
        i32::try_from(year)
            .ok()
            .and_then(|y| NaiveDate::from_ymd_opt(y, month, 1))
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .ok_or_else(|| dt_oor(value, unit))
    };
    let dt: NaiveDateTime = match unit {
        // Calendar units are not fixed-duration; offset the epoch's Y/M fields.
        "Y" => from_ym(1970 + value, 1)?,
        "M" => from_ym(1970 + value.div_euclid(12), value.rem_euclid(12) as u32 + 1)?,
        "W" => from_delta(Duration::try_weeks(value))?,
        "D" => from_delta(Duration::try_days(value))?,
        "h" => from_delta(Duration::try_hours(value))?,
        "m" => from_delta(Duration::try_minutes(value))?,
        "s" => from_delta(Duration::try_seconds(value))?,
        "ms" => from_delta(Duration::try_milliseconds(value))?,
        // µs/ns constructors are infallible for any i64 (within TimeDelta range);
        // only the epoch addition can overflow.
        "us" => from_delta(Some(Duration::microseconds(value)))?,
        "ns" => from_delta(Some(Duration::nanoseconds(value)))?,
        other => {
            return Err(format!(
                "application/json array serializer does not support datetime64 unit '{other}'"
            )
            .into());
        }
    };

    // orjson: omit the fractional part when the microsecond component is zero,
    // else exactly 6 digits (ns remainder truncated, matching orjson's µs cap).
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = dt.nanosecond() / 1000;
    let s = if micros == 0 {
        base
    } else {
        format!("{base}.{micros:06}")
    };
    Ok(serde_json::Value::String(s))
}

/// Nest a flat, row-major value list into the JSON shape numpy/orjson produce:
/// `[]` shape → the bare scalar; `[n]` → a flat array; higher rank → arrays of
/// arrays. `shape` and `flat` are pre-validated to agree on element count.
fn nest_array(flat: &[serde_json::Value], shape: &[usize]) -> serde_json::Value {
    match shape {
        [] => flat.first().cloned().unwrap_or(serde_json::Value::Null),
        [_] => serde_json::Value::Array(flat.to_vec()),
        [first, rest @ ..] => {
            // Always emit exactly `first` inner arrays. `inner_len` can be 0
            // (e.g. shape [2, 0] → `[[], []]`), so slice by an explicit index
            // rather than `chunks(inner_len)` (which panics on a 0 stride).
            let inner_len: usize = rest.iter().product();
            let mut out = Vec::with_capacity(*first);
            for i in 0..*first {
                let chunk = &flat[i * inner_len..(i + 1) * inner_len];
                out.push(nest_array(chunk, rest));
            }
            serde_json::Value::Array(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{SerializationRegistry, UnsupportedShape};

    fn csv_serializer() -> std::sync::Arc<crate::registry::SerializerFn> {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        reg.dispatch(StructureFamily::Array, mime::CSV).unwrap()
    }

    /// L1: `numpy.savetxt(fmt="%s")` emits "1.0" for 1.0_f64, not "1".
    /// Rust Display (Ryu) emits "1"; the fix appends ".0" when the output
    /// contains no decimal point, 'e', or letter (NaN/inf excluded).
    #[test]
    fn csv_float_whole_number_gets_decimal_point() {
        let ser = csv_serializer();
        // f64: 0.0, 1.0, -2.0 must have decimal point.
        let values_f64: &[f64] = &[0.0, 1.0, -2.0, 1e10];
        let data: Vec<u8> = values_f64.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [4]});
        let out = ser(&data, &meta).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&out).unwrap().lines().collect();
        assert_eq!(lines, vec!["0.0", "1.0", "-2.0", "10000000000.0"]);

        // f32: same rule.
        let values_f32: &[f32] = &[0.0, 1.0, -3.0];
        let data32: Vec<u8> = values_f32.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta32 = serde_json::json!({"itemsize": 4, "kind": "f", "shape": [3]});
        let out32 = ser(&data32, &meta32).unwrap();
        let lines32: Vec<&str> = std::str::from_utf8(&out32).unwrap().lines().collect();
        assert_eq!(lines32, vec!["0.0", "1.0", "-3.0"]);

        // Non-whole values must NOT get an extra ".0".
        let frac: Vec<u8> = 1.5f64.to_le_bytes().to_vec();
        let out_frac = ser(
            &frac,
            &serde_json::json!({"itemsize": 8, "kind": "f", "shape": [1]}),
        )
        .unwrap();
        assert_eq!(std::str::from_utf8(&out_frac).unwrap(), "1.5");
    }

    /// Finding 4: a >2-D array must error like Python `serialize_csv`
    /// (UnsupportedShape, array.py:42-43), not silently flatten to one column.
    #[test]
    fn csv_rejects_ndim_gt_2_as_unsupported_shape() {
        let ser = csv_serializer();
        // 2x2x2 f64 (ndim 3).
        let data: Vec<u8> = (0..8u64).flat_map(|i| (i as f64).to_le_bytes()).collect();
        let meta = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [2, 2, 2]});
        let err = ser(&data, &meta).expect_err("ndim>2 CSV must error, not flatten");
        let shape = err
            .downcast_ref::<UnsupportedShape>()
            .expect("error must be UnsupportedShape so the router answers 406");
        assert_eq!(shape.shape, vec![2, 2, 2]);
    }

    /// 1-D and 2-D arrays remain valid CSV (the supported shapes).
    #[test]
    fn csv_accepts_1d_and_2d() {
        let ser = csv_serializer();
        let data: Vec<u8> = (0..6u64).flat_map(|i| (i as f64).to_le_bytes()).collect();

        let meta_1d = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [6]});
        let out_1d = ser(&data, &meta_1d).expect("1-D CSV is supported");
        assert_eq!(std::str::from_utf8(&out_1d).unwrap().lines().count(), 6);

        let meta_2d = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [2, 3]});
        let out_2d = ser(&data, &meta_2d).expect("2-D CSV is supported");
        assert_eq!(std::str::from_utf8(&out_2d).unwrap().lines().count(), 2);
    }

    /// M3: `text/html` is registered for arrays and wraps CSV in a minimal HTML
    /// body when PNG is unavailable or fails.  A zero-rank shape ([]) causes
    /// encode_image to return Err("array has zero rank") regardless of whether
    /// the `image` feature is compiled in, so this test covers the CSV fallback
    /// path in both configurations.
    #[test]
    fn html_array_csv_fallback() {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        let ser = reg
            .dispatch(StructureFamily::Array, "text/html")
            .expect("text/html must be registered for array");
        // 0-rank array: PNG fails (encode_image rejects shape=[]) → CSV fallback.
        let meta = serde_json::json!({"itemsize": 8, "kind": "f", "shape": []});
        let out = ser(&[], &meta).unwrap();
        let html = std::str::from_utf8(&out).unwrap();
        assert!(
            html.starts_with("<html><body>"),
            "must wrap in html/body: {html}"
        );
        assert!(html.ends_with("</body></html>"), "must close html: {html}");
        assert!(
            !html.contains("<img"),
            "CSV fallback must not embed an img tag: {html}"
        );
    }

    /// M3: When the `image` feature is enabled, a 2-D array produces PNG-embed HTML.
    #[cfg(feature = "image")]
    #[test]
    fn html_array_png_path_embeds_data_url() {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        let ser = reg
            .dispatch(StructureFamily::Array, "text/html")
            .expect("text/html must be registered for array");
        // 2×2 u8 grayscale array → PNG succeeds → img data URL.
        let data: Vec<u8> = vec![0u8, 64, 128, 255];
        let meta = serde_json::json!({"itemsize": 1, "kind": "u", "shape": [2, 2]});
        let out = ser(&data, &meta).unwrap();
        let html = std::str::from_utf8(&out).unwrap();
        assert!(
            html.starts_with("<html><body>"),
            "must open html/body: {html}"
        );
        assert!(
            html.contains(r#"<img src="data:image/png;base64,"#),
            "must embed PNG as data URL: {html}"
        );
        assert!(html.ends_with("</body></html>"), "must close html: {html}");
    }

    fn json_array_serializer() -> std::sync::Arc<crate::registry::SerializerFn> {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        reg.dispatch(StructureFamily::Array, mime::JSON)
            .expect("array application/json must be registered")
    }

    /// M1: `application/json` is registered for the array family (array.py:33-38).
    #[test]
    fn json_registered_for_array() {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        assert!(
            reg.dispatch(StructureFamily::Array, mime::JSON).is_some(),
            "array application/json must be registered"
        );
    }

    /// M1: a 1-D array serializes to a flat JSON array.
    #[test]
    fn json_array_1d() {
        let ser = json_array_serializer();
        let data: Vec<u8> = [1.0f64, 2.0, 3.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let meta = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [3]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!([1.0, 2.0, 3.0]));
    }

    /// M1: a 2-D array nests row-major, matching numpy/orjson.
    #[test]
    fn json_array_2d_nested_row_major() {
        let ser = json_array_serializer();
        let data: Vec<u8> = [1i64, 2, 3, 4]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let meta = serde_json::json!({"itemsize": 8, "kind": "i", "shape": [2, 2]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!([[1, 2], [3, 4]]));
    }

    /// M1: floating NaN/inf become JSON null (orjson float rule).
    #[test]
    fn json_array_nan_inf_become_null() {
        let ser = json_array_serializer();
        let data: Vec<u8> = [1.0f64, f64::NAN, f64::INFINITY]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let meta = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [3]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!([1.0, null, null]));
    }

    /// M1: a 0-D array serializes to the bare scalar (no wrapping array).
    #[test]
    fn json_array_0d_is_scalar() {
        let ser = json_array_serializer();
        let data: Vec<u8> = 42i64.to_le_bytes().to_vec();
        let meta = serde_json::json!({"itemsize": 8, "kind": "i", "shape": []});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!(42));
    }

    /// M1: an unsupported dtype (complex) is a hard error, not a mis-encode.
    #[test]
    fn json_array_unsupported_dtype_errors() {
        let ser = json_array_serializer();
        let data: Vec<u8> = vec![0u8; 16];
        let meta = serde_json::json!({"itemsize": 16, "kind": "c", "shape": [1]});
        let err = ser(&data, &meta).expect_err("complex dtype must error");
        assert!(
            err.to_string().contains("does not support"),
            "error must name the unsupported dtype: {err}"
        );
    }

    /// orjson renders numpy `datetime64` arrays as ISO-8601 strings (array.py:33-38
    /// → `safe_json_dump` → `OPT_SERIALIZE_NUMPY`); the array JSON serializer must
    /// too, not 500. datetime64[ns]: epoch → fixed 9-digit fraction, a real
    /// instant, and NaT (`i64::MIN`) → JSON null.
    #[test]
    fn json_array_datetime64_ns_renders_iso() {
        let ser = json_array_serializer();
        // 0 = epoch; 1_609_459_200_000_000_000 ns = 2021-01-01T00:00:00; i64::MIN = NaT.
        let vals: [i64; 3] = [0, 1_609_459_200_000_000_000, i64::MIN];
        let data: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": "[ns]", "shape": [3]
        });
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        // orjson emits a full datetime with NO fractional part when the µs
        // component is zero (verified against orjson 3.11.5).
        assert_eq!(parsed[0], "1970-01-01T00:00:00");
        assert_eq!(parsed[1], "2021-01-01T00:00:00");
        assert_eq!(
            parsed[2],
            serde_json::Value::Null,
            "NaT must serialize to null"
        );
    }

    /// Fractional seconds: orjson formats datetime64 at MICROSECOND resolution,
    /// 6 digits, present only when non-zero; a nanosecond remainder is truncated
    /// (verified against orjson 3.11.5: `[ms].123`→`.123000`, `[us].123456`→
    /// `.123456`, `[ns].123456789`→`.123456`).
    #[test]
    fn json_array_datetime64_microsecond_fraction() {
        let ser = json_array_serializer();
        let cases: &[(&str, i64, &str)] = &[
            ("[ms]", 1_609_459_200_123, "2021-01-01T00:00:00.123000"),
            ("[us]", 1_609_459_200_123_456, "2021-01-01T00:00:00.123456"),
            (
                "[ns]",
                1_609_459_200_123_456_789,
                "2021-01-01T00:00:00.123456",
            ),
        ];
        for (unit, value, expected) in cases {
            let data: Vec<u8> = value.to_le_bytes().to_vec();
            let meta = serde_json::json!({
                "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": unit, "shape": [1]
            });
            let out = ser(&data, &meta).unwrap();
            let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
            assert_eq!(parsed, serde_json::json!([expected]), "unit {unit}");
        }
    }

    /// datetime64[s]: second resolution has no fractional part.
    #[test]
    fn json_array_datetime64_seconds_no_fraction() {
        let ser = json_array_serializer();
        let data: Vec<u8> = 1_609_459_200_i64.to_le_bytes().to_vec();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": "[s]", "shape": [1]
        });
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!(["2021-01-01T00:00:00"]));
    }

    /// datetime64[D] (a numpy date): day count since the epoch → midnight of
    /// that calendar date.
    #[test]
    fn json_array_datetime64_days() {
        let ser = json_array_serializer();
        // 18628 days after 1970-01-01 is 2021-01-01.
        let data: Vec<u8> = 18_628_i64.to_le_bytes().to_vec();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": "[D]", "shape": [1]
        });
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!(["2021-01-01T00:00:00"]));
    }

    /// datetime64 columns nest row-major exactly like the numeric arms (ms, 2×1).
    #[test]
    fn json_array_datetime64_nests_row_major() {
        let ser = json_array_serializer();
        let vals: [i64; 2] = [0, 1_609_459_200_000];
        let data: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": "[ms]", "shape": [2, 1]
        });
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!([["1970-01-01T00:00:00"], ["2021-01-01T00:00:00"]])
        );
    }

    /// A datetime64 array with no dt_units cannot be decoded → error, never a
    /// wrong instant.
    #[test]
    fn json_array_datetime64_missing_units_errors() {
        let ser = json_array_serializer();
        let data: Vec<u8> = 0_i64.to_le_bytes().to_vec();
        let meta = serde_json::json!({"itemsize": 8, "kind": "M", "shape": [1]});
        let err = ser(&data, &meta).expect_err("datetime64 without units must error");
        assert!(
            err.to_string().contains("dt_units"),
            "error must name the missing units: {err}"
        );
    }

    /// timedelta64 ('m') stays a hard error: orjson's `OPT_SERIALIZE_NUMPY` does
    /// not serialize timedelta64, so Python also fails on such an array.
    #[test]
    fn json_array_timedelta_still_errors() {
        let ser = json_array_serializer();
        let data: Vec<u8> = 5_i64.to_le_bytes().to_vec();
        let meta = serde_json::json!({"itemsize": 8, "kind": "m", "dt_units": "[s]", "shape": [1]});
        let err = ser(&data, &meta).expect_err("timedelta64 must error");
        assert!(
            err.to_string().contains("does not support"),
            "timedelta64 must remain unsupported: {err}"
        );
    }

    /// Finding 5: the array CSV serializer is registered under all four media
    /// types Python uses (array.py:49-56), so each yields CSV — not the
    /// octet-stream default (which would ship binary in a `.txt`/`.xls`).
    #[test]
    fn csv_registered_for_all_four_media_types() {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        let data: Vec<u8> = (0..3u64).flat_map(|i| (i as f64).to_le_bytes()).collect();
        let meta = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [3]});
        for mt in [
            mime::CSV,
            "text/x-comma-separated-values",
            mime::PLAIN,
            mime::EXCEL,
        ] {
            let ser = reg
                .dispatch(StructureFamily::Array, mt)
                .unwrap_or_else(|| panic!("array CSV must be registered for {mt}"));
            let out = ser(&data, &meta).unwrap();
            assert_eq!(
                std::str::from_utf8(&out)
                    .unwrap()
                    .lines()
                    .collect::<Vec<_>>(),
                vec!["0.0", "1.0", "2.0"],
                "{mt} must serialize as CSV with numpy-style float formatting"
            );
        }
    }
}
