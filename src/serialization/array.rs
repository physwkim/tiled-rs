//! Array serializers.
//!
//! Corresponds to `tiled/serialization/array.py`.

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::SerializationRegistry;

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

/// Decode one fixed-width numpy unicode (`U`, UCS-4 / UTF-32) element to a
/// `String`. `bytes` is a single element (itemsize = 4 × n_chars); each 4-byte
/// code unit is read little- or big-endian per `big_endian`. Trailing U+0000
/// padding is stripped — numpy renders a fixed-width `<U` element with its
/// trailing NULs removed (`ndarray.tolist()`); interior NULs are preserved and
/// invalid code points (surrogates / out-of-range) are skipped. Single source
/// of truth for `U` decoding, shared by the CSV and JSON array serializers.
fn decode_u_element(bytes: &[u8], big_endian: bool) -> String {
    let decoded: String = bytes
        .chunks_exact(4)
        .filter_map(|chunk| {
            let word: [u8; 4] = chunk.try_into().expect("chunks_exact(4) yields 4 bytes");
            let cp = if big_endian {
                u32::from_be_bytes(word)
            } else {
                u32::from_le_bytes(word)
            };
            char::from_u32(cp)
        })
        .collect();
    decoded.trim_end_matches('\0').to_string()
}

/// Strip trailing NUL padding from a fixed-width numpy byte-string (`S`)
/// element. numpy pads such elements to `itemsize` with NULs and treats the
/// trailing NULs as insignificant (interior NULs are preserved), so
/// `ndarray.tolist()` yields the bytes with trailing NULs removed.
fn strip_trailing_nuls(bytes: &[u8]) -> &[u8] {
    let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    &bytes[..end]
}

/// CSV serializer for 1-D/2-D arrays (Python `serialize_csv`, array.py:41-46).
fn serialize_array_csv(
    data: &[u8],
    metadata: &serde_json::Value,
) -> Result<bytes::Bytes, crate::serialization::registry::SerializeError> {
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
        return Err(crate::serialization::registry::UnsupportedShape {
            shape: shape.clone(),
        }
        .into());
    }
    let big_endian = metadata
        .get("byteorder")
        .and_then(|v| v.as_str())
        .unwrap_or("<")
        == ">";
    // datetime64/timedelta64 unit, e.g. "[ns]" → "ns"; consulted for kinds 'M'
    // (datetime64) and 'm' (timedelta64).
    let dt_unit: Option<&str> = metadata
        .get("dt_units")
        .and_then(|v| v.as_str())
        .map(|s| s.trim_start_matches('[').trim_end_matches(']'));

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
            ("M", 8) => {
                // datetime64 → str(numpy.datetime64): a resolution-truncated ISO
                // string (NOT the raw tick count). See datetime64_csv_cell.
                let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                if big_endian {
                    b.reverse();
                }
                datetime64_csv_cell(i64::from_le_bytes(b), dt_unit)
            }
            ("m", 8) => {
                // timedelta64 → str(numpy.timedelta64): "{n} {unit}" (e.g.
                // "5 seconds"), NOT the raw tick count. See timedelta64_csv_cell.
                let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                if big_endian {
                    b.reverse();
                }
                timedelta64_csv_cell(i64::from_le_bytes(b), dt_unit)
            }
            ("S", _) => {
                // Fixed-length byte string: decode as UTF-8, strip trailing nulls.
                std::str::from_utf8(bytes)
                    .unwrap_or("")
                    .trim_end_matches('\0')
                    .to_string()
            }
            // UCS-4 (4 bytes per character); honor byte order per character.
            // Single source of truth with the JSON serializer's `U` arm.
            ("U", _) => decode_u_element(bytes, big_endian),
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
) -> Result<bytes::Bytes, crate::serialization::registry::SerializeError> {
    #[cfg(feature = "image")]
    {
        if let Ok(png_bytes) = crate::serialization::image_array::encode_array_png(data, metadata) {
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
) -> Result<bytes::Bytes, crate::serialization::registry::SerializeError> {
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
    // datetime64/timedelta64 unit, e.g. "[ns]" → "ns"; consulted for kinds 'M'
    // (datetime64) and 'm' (timedelta64).
    let dt_unit: Option<&str> = metadata
        .get("dt_units")
        .and_then(|v| v.as_str())
        .map(|s| s.trim_start_matches('[').trim_end_matches(']'));

    let decode = |bytes: &[u8]| -> Result<serde_json::Value, crate::serialization::registry::SerializeError> {
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
            // timedelta64 is an 8-byte int64 tick count; orjson can't serialize
            // it, so tiled falls back to tolist() — see timedelta64_to_json.
            ("m", 8) => timedelta64_to_json(le!(i64, 8), dt_unit)?,
            // numpy fixed-width unicode (`<U`): orjson's OPT_SERIALIZE_NUMPY has
            // no fast-path for it, so upstream `safe_json_dump` falls to its
            // `default` → `array.tolist()` (utils.py:571-575), which renders each
            // element as a Python `str` (trailing NUL padding removed) and orjson
            // then emits a JSON string. Match that via the shared decoder.
            ("U", _) => serde_json::Value::String(decode_u_element(bytes, big_endian)),
            // numpy byte strings (`S`/bytes): orjson likewise has no fast-path, so
            // `tolist()` yields Python `bytes`; recursing on each, `safe_json_dump`'s
            // `default` hits its FIRST `isinstance(_, bytes)` branch and returns a
            // base64 data URI (utils.py:559-561) — NOT the utf-8 branch
            // (utils.py:576-577), which a `bytes` element can never reach. numpy's
            // `tolist()` has already stripped trailing NUL padding, so strip it
            // before encoding.
            ("S", _) => {
                use base64::Engine as _;
                let b64 = base64::engine::general_purpose::STANDARD.encode(strip_trailing_nuls(bytes));
                serde_json::Value::String(format!("data:application/octet-stream;base64,{b64}"))
            }
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
fn dt_oor(value: i64, unit: &str) -> crate::serialization::registry::SerializeError {
    format!("datetime64 value {value} ({unit}) is out of the representable range").into()
}

/// Map a numpy `datetime64` tick `value` in `unit` to a naive datetime.
///
/// `value` must NOT be the NaT sentinel (`i64::MIN`); callers render NaT per
/// their own format. The unit only affects how the tick count maps to an
/// instant — the per-serializer FORMAT (orjson for JSON, `str(numpy.datetime64)`
/// for CSV) is applied by the caller. Errors on an unrecognised unit or an
/// out-of-representable-range value.
fn datetime64_naive(
    value: i64,
    unit: &str,
) -> Result<chrono::NaiveDateTime, crate::serialization::registry::SerializeError> {
    use chrono::{Duration, NaiveDate, NaiveDateTime};

    let epoch: NaiveDateTime = NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .expect("1970-01-01T00:00:00 is a valid datetime");
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
    match unit {
        // Calendar units are not fixed-duration; offset the epoch's Y/M fields.
        "Y" => from_ym(1970 + value, 1),
        "M" => from_ym(1970 + value.div_euclid(12), value.rem_euclid(12) as u32 + 1),
        "W" => from_delta(Duration::try_weeks(value)),
        "D" => from_delta(Duration::try_days(value)),
        "h" => from_delta(Duration::try_hours(value)),
        "m" => from_delta(Duration::try_minutes(value)),
        "s" => from_delta(Duration::try_seconds(value)),
        "ms" => from_delta(Duration::try_milliseconds(value)),
        // µs/ns constructors are infallible for any i64 (within TimeDelta range);
        // only the epoch addition can overflow.
        "us" => from_delta(Some(Duration::microseconds(value))),
        "ns" => from_delta(Some(Duration::nanoseconds(value))),
        other => Err(format!("datetime64 unit '{other}' is not supported").into()),
    }
}

/// Render a numpy `datetime64` tick count as the JSON value orjson produces for
/// it under `OPT_SERIALIZE_NUMPY` (the array `application/json` path is
/// `safe_json_dump(array)`, array.py:33-38).
///
/// Format — verified byte-for-byte against orjson 3.11.5: a full RFC 3339
/// datetime `YYYY-MM-DDTHH:MM:SS` for every unit (orjson does NOT truncate to
/// the unit's resolution the way `str(numpy.datetime64)` does — e.g. a `[D]`
/// value still renders `...T00:00:00`), with a fractional part only when the
/// microsecond component is non-zero, then exactly 6 digits. orjson formats at
/// microsecond resolution, so a `[ns]` sub-microsecond remainder is TRUNCATED
/// (`.123456789` → `.123456`). No timezone suffix (datetime64 is naive).
///
/// NaT (the `i64::MIN` sentinel) is NOT special-cased uniformly — we reproduce
/// orjson's own unit-dependent NaT handling so a NaT element is byte-identical
/// to a Python tiled server (all three behaviors verified against orjson 3.11.5):
///   * `W`/`D`/`h`/`m`: orjson has an explicit NaT→epoch sentinel (NaT → epoch,
///     NaT+1 → epoch+1 unit, while a merely-large value raises — i.e. a sentinel
///     check, not arithmetic), so these emit `1970-01-01T00:00:00`.
///   * `ns`: no NaT branch; the sentinel lands in datetime's range and renders a
///     garbage in-range instant (`1677-09-21T00:12:43.145224`).
///   * `Y`/`M`/`s`/`ms`/`us`: no NaT branch; the sentinel overflows the calendar
///     or the representable range, so orjson raises → our `datetime64_naive`
///     errors → HTTP 500.
///
/// A missing/unrecognised unit is an error so the caller falls through to the
/// unsupported-dtype path rather than emitting wrong data.
fn datetime64_to_json(
    value: i64,
    unit: Option<&str>,
) -> Result<serde_json::Value, crate::serialization::registry::SerializeError> {
    use chrono::Timelike;

    let unit = unit.ok_or_else(|| -> crate::serialization::registry::SerializeError {
        "datetime64 array is missing its dt_units; cannot decode".into()
    })?;
    // orjson special-cases the NaT sentinel only for these four units, emitting
    // the Unix epoch. Every other unit flows through the normal conversion: ns
    // renders a garbage in-range instant, Y/M/s/ms/us overflow → error → 500.
    let dt = if value == i64::MIN && matches!(unit, "W" | "D" | "h" | "m") {
        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .expect("1970-01-01T00:00:00 is a valid datetime")
    } else {
        datetime64_naive(value, unit)?
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

/// Render a numpy `timedelta64` tick as the JSON value tiled's `safe_json_dump`
/// produces. orjson's `OPT_SERIALIZE_NUMPY` does NOT serialize timedelta64, so
/// `default()` (utils.py:558-578) returns `array.tolist()` and orjson then
/// serializes that Python list. The result is unit-dependent (verified against
/// orjson 3.11.5 + numpy 2.0.2):
///   * NaT (`i64::MIN`) → `None` → JSON `null`, for EVERY unit (numpy's
///     `NaT.tolist()` is `None` regardless of unit).
///   * `Y`/`M`/`ns` → a Python `int` (calendar units have no fixed duration, and
///     `datetime.timedelta` has only microsecond resolution so `ns` cannot be
///     one) → orjson emits the raw tick count.
///   * `W`/`D`/`h`/`m`/`s`/`ms`/`us` → a `datetime.timedelta`, which orjson
///     cannot serialize → it raises → HTTP 500. We mirror this by erroring;
///     because serialization aborts on the first such element, an array with any
///     non-NaT duration-unit value 500s exactly as Python does, while an all-NaT
///     duration-unit array still yields `[null, …]`.
fn timedelta64_to_json(
    value: i64,
    unit: Option<&str>,
) -> Result<serde_json::Value, crate::serialization::registry::SerializeError> {
    // NaT → null regardless of unit (numpy NaT.tolist() is None for every unit).
    if value == i64::MIN {
        return Ok(serde_json::Value::Null);
    }
    let unit = unit.ok_or_else(|| -> crate::serialization::registry::SerializeError {
        "timedelta64 array is missing its dt_units; cannot decode".into()
    })?;
    match unit {
        // tolist() → Python int → orjson emits the raw tick count.
        "Y" | "M" | "ns" => Ok(serde_json::Value::from(value)),
        // tolist() → datetime.timedelta → orjson raises → HTTP 500.
        _ => Err(format!(
            "timedelta64[{unit}] is not JSON-serializable (orjson raises on the \
             datetime.timedelta that numpy's tolist() produces)"
        )
        .into()),
    }
}

/// Render a numpy `datetime64` tick count as the CSV cell `numpy.savetxt(fmt=
/// "%s")` produces — i.e. `str(numpy.datetime64(value, unit))` (the array CSV
/// path is `serialize_csv` → `numpy.savetxt`, array.py:41-46). Unlike the JSON
/// path's orjson format, `str()` is RESOLUTION-TRUNCATED to the unit (verified
/// against numpy 2.0.2): `Y`→`2021`, `M`→`2021-01`, `W`/`D`→`2021-01-01`,
/// `h`→`...T00`, `m`→`...T00:00`, `s`→`...T00:00:00`, and `ms`/`us`/`ns` carry a
/// fixed 3/6/9-digit fraction that is ALWAYS present (and, unlike orjson, the
/// `ns` fraction is NOT truncated). NaT → `"NaT"`, matching `str`. A missing or
/// unsupported unit yields a visible placeholder rather than a wrong instant,
/// matching this serializer's existing "unsupported …" cell convention.
fn datetime64_csv_cell(value: i64, unit: Option<&str>) -> String {
    if value == i64::MIN {
        return "NaT".to_string(); // str(numpy.datetime64('NaT'))
    }
    let Some(unit) = unit else {
        return "datetime64(missing-units)".to_string();
    };
    let dt = match datetime64_naive(value, unit) {
        Ok(dt) => dt,
        Err(_) => return format!("datetime64(unsupported-unit:{unit})"),
    };
    let fmt = match unit {
        "Y" => "%Y",
        "M" => "%Y-%m",
        "W" | "D" => "%Y-%m-%d",
        "h" => "%Y-%m-%dT%H",
        "m" => "%Y-%m-%dT%H:%M",
        "s" => "%Y-%m-%dT%H:%M:%S",
        "ms" => "%Y-%m-%dT%H:%M:%S.%3f",
        "us" => "%Y-%m-%dT%H:%M:%S.%6f",
        "ns" => "%Y-%m-%dT%H:%M:%S.%9f",
        other => return format!("datetime64(unsupported-unit:{other})"),
    };
    dt.format(fmt).to_string()
}

/// Render a numpy `timedelta64` tick as the CSV cell `numpy.savetxt(fmt="%s")`
/// produces — i.e. `str(numpy.timedelta64(value, unit))` = `"{n} {unit_name}"`
/// (verified against numpy 2.0.2). The tick count is printed verbatim: numpy
/// does NOT pluralize by count (`1` → `"1 years"`, not `"1 year"`), prints `0`
/// and negatives literally, and renders NaT (`i64::MIN`) as `"NaT"`. A
/// missing/unsupported unit yields a visible placeholder rather than a wrong
/// value, matching `datetime64_csv_cell`'s convention.
fn timedelta64_csv_cell(value: i64, unit: Option<&str>) -> String {
    if value == i64::MIN {
        return "NaT".to_string(); // str(numpy.timedelta64('NaT'))
    }
    let Some(unit) = unit else {
        return "timedelta64(missing-units)".to_string();
    };
    let name = match unit {
        "Y" => "years",
        "M" => "months",
        "W" => "weeks",
        "D" => "days",
        "h" => "hours",
        "m" => "minutes",
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        other => return format!("timedelta64(unsupported-unit:{other})"),
    };
    format!("{value} {name}")
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
    use crate::serialization::registry::{SerializationRegistry, UnsupportedShape};

    fn csv_serializer() -> std::sync::Arc<crate::serialization::registry::SerializerFn> {
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

    fn json_array_serializer() -> std::sync::Arc<crate::serialization::registry::SerializerFn> {
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

    fn octet_stream_serializer() -> std::sync::Arc<crate::serialization::registry::SerializerFn> {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        reg.dispatch(StructureFamily::Array, mime::OCTET_STREAM)
            .expect("array application/octet-stream must be registered")
    }

    /// Encode `strings` as a numpy `<U{width}` buffer: UCS-4 little-endian,
    /// each element NUL-padded to `width` code points (itemsize = 4 × width).
    fn ucs4_le_buffer(strings: &[&str], width: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(strings.len() * width * 4);
        for s in strings {
            let mut chars = 0;
            for ch in s.chars() {
                buf.extend_from_slice(&(ch as u32).to_le_bytes());
                chars += 1;
            }
            for _ in chars..width {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }
        buf
    }

    /// Encode `items` as a numpy `S{width}` buffer: each element NUL-padded to
    /// `width` bytes (itemsize = width).
    fn s_buffer(items: &[&[u8]], width: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(items.len() * width);
        for it in items {
            let mut v = it.to_vec();
            v.resize(width, 0);
            buf.extend_from_slice(&v);
        }
        buf
    }

    fn b64(bytes: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// A 1-D `<U` array renders as JSON strings (upstream `safe_json_dump`
    /// falls back to `tolist()`; numpy strips trailing NUL padding).
    #[test]
    fn json_array_u_1d_renders_strings() {
        let ser = json_array_serializer();
        let data = ucs4_le_buffer(&["ab", "cdef"], 4); // <U4
        let meta = serde_json::json!({"itemsize": 16, "kind": "U", "shape": [2]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!(["ab", "cdef"]));
    }

    /// A 2-D `<U` array nests row-major, same as the numeric path.
    #[test]
    fn json_array_u_2d_nested_row_major() {
        let ser = json_array_serializer();
        let data = ucs4_le_buffer(&["a", "bb", "ccc", "d"], 3); // <U3, 2x2
        let meta = serde_json::json!({"itemsize": 12, "kind": "U", "shape": [2, 2]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!([["a", "bb"], ["ccc", "d"]]));
    }

    /// Boundary elements: an empty string (all NUL padding) and a full-width
    /// element (fills the itemsize, no padding to strip).
    #[test]
    fn json_array_u_empty_and_full_width_elements() {
        let ser = json_array_serializer();
        let data = ucs4_le_buffer(&["", "abcd"], 4); // <U4: "" then exactly 4 chars
        let meta = serde_json::json!({"itemsize": 16, "kind": "U", "shape": [2]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!(["", "abcd"]));
    }

    /// A numpy `S`/bytes array renders as base64 data URIs — matching
    /// upstream `safe_json_dump`'s first `isinstance(_, bytes)` branch
    /// (utils.py:559-561), NOT the utf-8 branch. Trailing NUL padding is
    /// stripped before encoding (`tolist()` does).
    #[test]
    fn json_array_s_renders_base64_data_uri() {
        let ser = json_array_serializer();
        let data = s_buffer(&[b"abc", b"de"], 3); // S3: "abc", "de\0"
        let meta = serde_json::json!({"itemsize": 3, "kind": "S", "shape": [2]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!([
                format!("data:application/octet-stream;base64,{}", b64(b"abc")),
                format!("data:application/octet-stream;base64,{}", b64(b"de")),
            ])
        );
    }

    /// An empty `S` element (all NUL) yields an empty-payload data URI.
    #[test]
    fn json_array_s_empty_element_is_empty_payload() {
        let ser = json_array_serializer();
        let data = s_buffer(&[b"", b"z"], 1); // S1: "\0", "z"
        let meta = serde_json::json!({"itemsize": 1, "kind": "S", "shape": [2]});
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!([
                "data:application/octet-stream;base64,",
                format!("data:application/octet-stream;base64,{}", b64(b"z")),
            ])
        );
    }

    /// Round-trip through the octet-stream dtype: the raw element bytes the
    /// octet-stream serializer emits are exactly what the JSON serializer
    /// consumes, and JSON decodes them back to the original strings.
    #[test]
    fn json_array_u_roundtrips_octet_stream_bytes() {
        let octet = octet_stream_serializer();
        let json = json_array_serializer();
        let strings = ["hi", "world"];
        let data = ucs4_le_buffer(&strings, 5); // <U5
        let meta = serde_json::json!({"itemsize": 20, "kind": "U", "shape": [2]});

        // octet-stream is the raw wire buffer (zero-copy passthrough).
        let raw = octet(&data, &meta).unwrap();
        assert_eq!(&raw[..], &data[..], "octet-stream must be the raw buffer");

        // Feeding that same buffer to JSON recovers the strings.
        let out = json(&raw, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!(["hi", "world"]));
    }

    /// orjson renders numpy `datetime64` arrays as ISO-8601 strings (array.py:33-38
    /// → `safe_json_dump` → `OPT_SERIALIZE_NUMPY`); the array JSON serializer must
    /// too, not 500. datetime64[ns]: epoch → no fraction (µs component zero) and a
    /// real 2021 instant. NaT for `[ns]` is exercised separately
    /// (`json_array_datetime64_nat_matches_orjson`) because orjson does NOT emit
    /// null there — it renders the in-range garbage instant.
    #[test]
    fn json_array_datetime64_ns_renders_iso() {
        let ser = json_array_serializer();
        // 0 = epoch; 1_609_459_200_000_000_000 ns = 2021-01-01T00:00:00.
        let vals: [i64; 2] = [0, 1_609_459_200_000_000_000];
        let data: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": "[ns]", "shape": [2]
        });
        let out = ser(&data, &meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        // orjson emits a full datetime with NO fractional part when the µs
        // component is zero (verified against orjson 3.11.5).
        assert_eq!(parsed[0], "1970-01-01T00:00:00");
        assert_eq!(parsed[1], "2021-01-01T00:00:00");
    }

    /// NaT (`i64::MIN`) in the JSON path replicates orjson 3.11.5's verified,
    /// unit-dependent behavior verbatim (no null deviation):
    ///   * `W`/`D`/`h`/`m` → epoch `1970-01-01T00:00:00` (orjson's NaT sentinel);
    ///   * `ns` → the in-range garbage instant `1677-09-21T00:12:43.145224`;
    ///   * `Y`/`M`/`s`/`ms`/`us` → error (orjson raises → HTTP 500).
    #[test]
    fn json_array_datetime64_nat_matches_orjson() {
        let ser = json_array_serializer();
        let one = |unit: &str| -> Result<serde_json::Value, _> {
            let data: Vec<u8> = i64::MIN.to_le_bytes().to_vec();
            let meta = serde_json::json!({
                "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": unit, "shape": [1]
            });
            ser(&data, &meta).map(|out| serde_json::from_slice::<serde_json::Value>(&out).unwrap())
        };
        // W/D/h/m: orjson's explicit NaT→epoch sentinel.
        for unit in ["[W]", "[D]", "[h]", "[m]"] {
            assert_eq!(
                one(unit).unwrap_or_else(|e| panic!("{unit} NaT must be epoch, got err {e}")),
                serde_json::json!(["1970-01-01T00:00:00"]),
                "NaT[{unit}] must be epoch (orjson sentinel)"
            );
        }
        // ns: no sentinel; the in-range garbage instant orjson actually emits.
        assert_eq!(
            one("[ns]").unwrap(),
            serde_json::json!(["1677-09-21T00:12:43.145224"]),
            "NaT[ns] must match orjson's in-range garbage instant"
        );
        // Y/M/s/ms/us: orjson raises → we error (→ HTTP 500), never wrong data.
        for unit in ["[Y]", "[M]", "[s]", "[ms]", "[us]"] {
            one(unit).expect_err(&format!("NaT[{unit}] must error (orjson raises)"));
        }
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

    /// timedelta64 ('m') JSON path mirrors tiled's `safe_json_dump` tolist()
    /// fallback exactly (verified against orjson 3.11.5 + numpy 2.0.2):
    ///   * NaT (`i64::MIN`) → `null` for EVERY unit;
    ///   * `Y`/`M`/`ns` non-NaT → the raw integer tick (tolist() → int);
    ///   * `W`/`D`/`h`/`m`/`s`/`ms`/`us` non-NaT → error (tolist() →
    ///     datetime.timedelta → orjson raises → HTTP 500).
    #[test]
    fn json_array_timedelta64_matches_orjson_tolist() {
        let ser = json_array_serializer();
        let one = |unit: &str, v: i64| -> Result<serde_json::Value, _> {
            let data: Vec<u8> = v.to_le_bytes().to_vec();
            let meta = serde_json::json!({
                "itemsize": 8, "kind": "m", "dt_units": unit, "shape": [1]
            });
            ser(&data, &meta).map(|out| serde_json::from_slice::<serde_json::Value>(&out).unwrap())
        };
        // Y/M/ns non-NaT → raw integer tick.
        for unit in ["[Y]", "[M]", "[ns]"] {
            assert_eq!(
                one(unit, 5).unwrap(),
                serde_json::json!([5]),
                "timedelta64{unit} must emit the raw integer tick"
            );
        }
        // Duration units non-NaT → error (orjson raises on datetime.timedelta).
        for unit in ["[W]", "[D]", "[h]", "[m]", "[s]", "[ms]", "[us]"] {
            one(unit, 5).expect_err(&format!("timedelta64{unit} non-NaT must 500"));
        }
        // NaT → null for EVERY unit, including the otherwise-erroring duration ones.
        for unit in [
            "[Y]", "[M]", "[W]", "[D]", "[h]", "[m]", "[s]", "[ms]", "[us]", "[ns]",
        ] {
            assert_eq!(
                one(unit, i64::MIN).unwrap(),
                serde_json::json!([serde_json::Value::Null]),
                "timedelta64{unit} NaT must be null"
            );
        }
    }

    /// A timedelta64 array with no dt_units cannot pick the int-vs-error branch,
    /// so a non-NaT value errors rather than guess; NaT is still null (it needs
    /// no unit).
    #[test]
    fn json_array_timedelta64_missing_units() {
        let ser = json_array_serializer();
        let meta_for = |v: i64| {
            (
                v.to_le_bytes().to_vec(),
                serde_json::json!({"itemsize": 8, "kind": "m", "shape": [1]}),
            )
        };
        let (data, meta) = meta_for(5);
        let err = ser(&data, &meta).expect_err("timedelta64 without units must error on a value");
        assert!(
            err.to_string().contains("dt_units"),
            "error must name the missing units: {err}"
        );
        let (data, meta) = meta_for(i64::MIN);
        let out = ser(&data, &meta).expect("NaT needs no unit");
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, serde_json::json!([serde_json::Value::Null]));
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

    /// The array CSV serializer renders datetime64 as `str(numpy.datetime64)` —
    /// a resolution-truncated ISO string — matching `numpy.savetxt(fmt="%s")`,
    /// NOT the raw integer tick (verified against numpy 2.0.2). This is a
    /// DIFFERENT format from the JSON path's orjson output (full datetime, µs):
    /// CSV truncates to the unit and keeps the full ns fraction.
    #[test]
    fn csv_array_datetime64_matches_numpy_str() {
        let ser = csv_serializer();
        // (unit, tick value, expected str(numpy.datetime64(value, unit)))
        let cases: &[(&str, i64, &str)] = &[
            ("[Y]", 51, "2021"),
            ("[M]", 612, "2021-01"),
            ("[D]", 18_628, "2021-01-01"),
            ("[h]", 447_072, "2021-01-01T00"),
            ("[s]", 1_609_459_200, "2021-01-01T00:00:00"),
            ("[ms]", 1_609_459_200_123, "2021-01-01T00:00:00.123"),
            ("[us]", 1_609_459_200_123_456, "2021-01-01T00:00:00.123456"),
            (
                "[ns]",
                1_609_459_200_123_456_789,
                "2021-01-01T00:00:00.123456789",
            ),
        ];
        for (unit, value, expected) in cases {
            let data: Vec<u8> = value.to_le_bytes().to_vec();
            let meta = serde_json::json!({
                "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": unit, "shape": [1]
            });
            let out = ser(&data, &meta).unwrap();
            assert_eq!(std::str::from_utf8(&out).unwrap(), *expected, "unit {unit}");
        }
    }

    /// CSV datetime64 NaT → "NaT" (matching `str(numpy.datetime64('NaT'))`),
    /// not the raw i64::MIN sentinel.
    #[test]
    fn csv_array_datetime64_nat_is_nat_string() {
        let ser = csv_serializer();
        let data: Vec<u8> = i64::MIN.to_le_bytes().to_vec();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": "[s]", "shape": [1]
        });
        let out = ser(&data, &meta).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "NaT");
    }

    /// CSV datetime64 lays out 2-D as comma-separated rows, one date per cell.
    #[test]
    fn csv_array_datetime64_2d_layout() {
        let ser = csv_serializer();
        let vals: [i64; 4] = [1_609_459_200, 0, 0, 1_609_459_200];
        let data: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "M", "byteorder": "<", "dt_units": "[s]", "shape": [2, 2]
        });
        let out = ser(&data, &meta).unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "2021-01-01T00:00:00,1970-01-01T00:00:00\n\
             1970-01-01T00:00:00,2021-01-01T00:00:00"
        );
    }

    /// The array CSV serializer renders timedelta64 as `str(numpy.timedelta64)` —
    /// `"{n} {unit_name}"` — matching `numpy.savetxt(fmt="%s")`, NOT the raw
    /// integer tick (verified against numpy 2.0.2). numpy does not pluralize by
    /// count, so even `1` is `"1 <plural>"`.
    #[test]
    fn csv_array_timedelta64_matches_numpy_str() {
        let ser = csv_serializer();
        // (unit, tick value, expected str(numpy.timedelta64(value, unit)))
        let cases: &[(&str, i64, &str)] = &[
            ("[Y]", 5, "5 years"),
            ("[M]", 5, "5 months"),
            ("[W]", 5, "5 weeks"),
            ("[D]", 5, "5 days"),
            ("[h]", 5, "5 hours"),
            ("[m]", 5, "5 minutes"),
            ("[s]", 5, "5 seconds"),
            ("[ms]", 5, "5 milliseconds"),
            ("[us]", 5, "5 microseconds"),
            ("[ns]", 5, "5 nanoseconds"),
            // No pluralization at 1; literal 0 and negatives.
            ("[s]", 1, "1 seconds"),
            ("[s]", 0, "0 seconds"),
            ("[Y]", -3, "-3 years"),
        ];
        for (unit, value, expected) in cases {
            let data: Vec<u8> = value.to_le_bytes().to_vec();
            let meta = serde_json::json!({
                "itemsize": 8, "kind": "m", "byteorder": "<", "dt_units": unit, "shape": [1]
            });
            let out = ser(&data, &meta).unwrap();
            assert_eq!(
                std::str::from_utf8(&out).unwrap(),
                *expected,
                "unit {unit} value {value}"
            );
        }
    }

    /// CSV timedelta64 NaT → "NaT" (matching `str(numpy.timedelta64('NaT'))`),
    /// not the raw i64::MIN sentinel.
    #[test]
    fn csv_array_timedelta64_nat_is_nat_string() {
        let ser = csv_serializer();
        let data: Vec<u8> = i64::MIN.to_le_bytes().to_vec();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "m", "byteorder": "<", "dt_units": "[s]", "shape": [1]
        });
        let out = ser(&data, &meta).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "NaT");
    }
}
