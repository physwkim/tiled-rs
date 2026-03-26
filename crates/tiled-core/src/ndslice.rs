//! N-dimensional slice representation.
//!
//! Corresponds to `tiled/ndslice.py` — `NDSlice`.
//!
//! Supports numpy-style string parsing (`"1:3,4,1:5:2,..."`), JSON serialization,
//! and conversion.

use serde::{Deserialize, Serialize};

use crate::error::{Result, TiledError};

/// A single dimension of an N-dimensional slice.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SliceDim {
    /// A single integer index (reduces dimensionality).
    Index(isize),
    /// A range slice with optional start, stop, step.
    Slice {
        start: Option<isize>,
        stop: Option<isize>,
        step: Option<isize>,
    },
    /// Ellipsis — fill remaining dimensions with full slices.
    Ellipsis,
}

impl SliceDim {
    /// A full slice (equivalent to `:` or `slice(None)`).
    pub fn full() -> Self {
        Self::Slice {
            start: None,
            stop: None,
            step: None,
        }
    }

    /// Whether this is a full slice (selects everything).
    pub fn is_full(&self) -> bool {
        matches!(
            self,
            Self::Slice {
                start: None,
                stop: None,
                step: None | Some(1),
            } | Self::Slice {
                start: Some(0),
                stop: None,
                step: None | Some(1),
            } | Self::Ellipsis
        )
    }
}

/// Serialize SliceDim to JSON (matching Python tiled wire format).
impl Serialize for SliceDim {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Self::Index(i) => serializer.serialize_i64(*i as i64),
            Self::Slice { start, stop, step } => {
                // Count non-None fields
                let count = start.is_some() as usize + stop.is_some() as usize + step.is_some() as usize;
                let mut map = serializer.serialize_map(Some(count))?;
                if let Some(s) = start {
                    map.serialize_entry("start", &(*s as i64))?;
                }
                if let Some(s) = stop {
                    map.serialize_entry("stop", &(*s as i64))?;
                }
                if let Some(s) = step {
                    map.serialize_entry("step", &(*s as i64))?;
                }
                map.end()
            }
            Self::Ellipsis => {
                // Ellipsis encoded as {"step": 0} — not a valid builtin.slice
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("step", &0)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for SliceDim {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match &value {
            serde_json::Value::Number(n) => {
                let i = n
                    .as_i64()
                    .ok_or_else(|| serde::de::Error::custom("Expected integer"))?;
                Ok(Self::Index(i as isize))
            }
            serde_json::Value::Object(map) => {
                // Check for ellipsis encoding: {"step": 0}
                if map.len() == 1
                    && map.get("step").and_then(|v| v.as_i64()) == Some(0)
                {
                    return Ok(Self::Ellipsis);
                }

                let start = map.get("start").and_then(|v| v.as_i64()).map(|v| v as isize);
                let stop = map.get("stop").and_then(|v| v.as_i64()).map(|v| v as isize);
                let step = map.get("step").and_then(|v| v.as_i64()).map(|v| v as isize);
                Ok(Self::Slice { start, stop, step })
            }
            _ => Err(serde::de::Error::custom(
                "SliceDim must be an integer or object",
            )),
        }
    }
}

/// An N-dimensional slice, composed of per-dimension slice specifications.
///
/// Maps to Python `NDSlice(tuple)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NDSlice(pub Vec<SliceDim>);

impl NDSlice {
    /// Create an empty slice (selects everything).
    pub fn empty() -> Self {
        Self(vec![])
    }

    /// Whether this slice selects everything (no restrictions).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty() || self.0.iter().all(|d| d.is_full())
    }

    /// Number of dimensions in the slice.
    pub fn ndim(&self) -> usize {
        self.0.len()
    }

    /// Parse a numpy-style string representation.
    ///
    /// Examples: `"1:3,4,1:5:2,..."`, `":,:,0"`, `"::2"`
    pub fn from_numpy_str(s: &str) -> Result<Self> {
        let s = s.trim_matches(|c| c == '(' || c == ')' || c == '[' || c == ']');
        let s = s.replace(' ', "");

        if s.is_empty() {
            return Ok(Self::empty());
        }

        let mut dims = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if part == "..." {
                dims.push(SliceDim::Ellipsis);
            } else if part == ":" || part == "::" {
                dims.push(SliceDim::full());
            } else if part.contains(':') {
                dims.push(parse_slice_part(part)?);
            } else {
                let idx: isize = part.parse().map_err(|_| {
                    TiledError::InvalidSlice(format!("Invalid index: '{part}'"))
                })?;
                dims.push(SliceDim::Index(idx));
            }
        }

        // Validate: at most one ellipsis
        let ellipsis_count = dims.iter().filter(|d| matches!(d, SliceDim::Ellipsis)).count();
        if ellipsis_count > 1 {
            return Err(TiledError::InvalidSlice(
                "NDSlice can only contain one Ellipsis".into(),
            ));
        }

        Ok(Self(dims))
    }

    /// Convert to a numpy-style string.
    pub fn to_numpy_str(&self) -> String {
        self.0
            .iter()
            .map(|d| match d {
                SliceDim::Index(i) => i.to_string(),
                SliceDim::Slice { start, stop, step } => {
                    let s = format!(
                        "{}:{}",
                        start.map(|v| v.to_string()).unwrap_or_default(),
                        stop.map(|v| v.to_string()).unwrap_or_default(),
                    );
                    match step {
                        Some(st) => format!("{s}:{st}"),
                        None => s,
                    }
                }
                SliceDim::Ellipsis => "...".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Convert to JSON representation, expanding Ellipsis to fill `ndim` dimensions.
    pub fn to_json(&self, ndim: Option<usize>) -> Result<Vec<serde_json::Value>> {
        let has_ellipsis = self.0.iter().any(|d| matches!(d, SliceDim::Ellipsis));

        if has_ellipsis && ndim.is_none() {
            // Check if ellipsis is at the end (OK without ndim)
            if self.0.last() != Some(&SliceDim::Ellipsis) {
                return Err(TiledError::InvalidSlice(
                    "Converting NDSlice with Ellipsis in non-last position requires ndim".into(),
                ));
            }
        }

        let total_ndim = ndim.unwrap_or(self.0.len());
        let non_ellipsis_count = self.0.iter().filter(|d| !matches!(d, SliceDim::Ellipsis)).count();

        if total_ndim < non_ellipsis_count {
            return Err(TiledError::InvalidSlice(
                "ndim is less than the number of non-ellipsis elements".into(),
            ));
        }

        let fill_count = total_ndim - non_ellipsis_count;

        let mut result = Vec::with_capacity(total_ndim);
        for dim in &self.0 {
            match dim {
                SliceDim::Ellipsis => {
                    for _ in 0..fill_count {
                        result.push(serde_json::json!({}));
                    }
                }
                other => {
                    result.push(serde_json::to_value(other).map_err(|e| {
                        TiledError::Serialization(format!("Cannot serialize SliceDim: {e}"))
                    })?);
                }
            }
        }

        Ok(result)
    }
}

/// Parse a colon-delimited slice part like `"1:3"`, `"::2"`, `"1:5:2"`.
fn parse_slice_part(s: &str) -> Result<SliceDim> {
    let parts: Vec<&str> = s.split(':').collect();
    let parse_opt = |s: &str| -> Result<Option<isize>> {
        if s.is_empty() {
            Ok(None)
        } else {
            s.parse::<isize>()
                .map(Some)
                .map_err(|_| TiledError::InvalidSlice(format!("Invalid number: '{s}'")))
        }
    };

    match parts.len() {
        2 => {
            let start = parse_opt(parts[0])?;
            let stop = parse_opt(parts[1])?;
            Ok(SliceDim::Slice {
                start,
                stop,
                step: None,
            })
        }
        3 => {
            let start = parse_opt(parts[0])?;
            let stop = parse_opt(parts[1])?;
            let step = parse_opt(parts[2])?;
            Ok(SliceDim::Slice { start, stop, step })
        }
        _ => Err(TiledError::InvalidSlice(format!(
            "Invalid slice part: '{s}'"
        ))),
    }
}

/// Regex pattern for validating slice query parameters.
pub const SLICE_REGEX: &str = r"^(?:(?:-?\d+)?:){0,2}(?:-?\d+)?(?:,(?:(?:-?\d+)?:){0,2}(?:-?\d+)?)*$";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let s = NDSlice::from_numpy_str("").unwrap();
        assert!(s.is_empty());
        assert_eq!(s.to_numpy_str(), "");
    }

    #[test]
    fn test_parse_single_index() {
        let s = NDSlice::from_numpy_str("5").unwrap();
        assert_eq!(s.0, vec![SliceDim::Index(5)]);
        assert_eq!(s.to_numpy_str(), "5");
    }

    #[test]
    fn test_parse_negative_index() {
        let s = NDSlice::from_numpy_str("-1").unwrap();
        assert_eq!(s.0, vec![SliceDim::Index(-1)]);
    }

    #[test]
    fn test_parse_simple_slice() {
        let s = NDSlice::from_numpy_str("1:3").unwrap();
        assert_eq!(
            s.0,
            vec![SliceDim::Slice {
                start: Some(1),
                stop: Some(3),
                step: None,
            }]
        );
        assert_eq!(s.to_numpy_str(), "1:3");
    }

    #[test]
    fn test_parse_full_slice() {
        let s = NDSlice::from_numpy_str(":").unwrap();
        assert_eq!(s.0, vec![SliceDim::full()]);
        assert!(s.is_empty()); // full slice = selects everything
    }

    #[test]
    fn test_parse_step_slice() {
        let s = NDSlice::from_numpy_str("1:5:2").unwrap();
        assert_eq!(
            s.0,
            vec![SliceDim::Slice {
                start: Some(1),
                stop: Some(5),
                step: Some(2),
            }]
        );
        assert_eq!(s.to_numpy_str(), "1:5:2");
    }

    #[test]
    fn test_parse_step_only() {
        let s = NDSlice::from_numpy_str("::2").unwrap();
        assert_eq!(
            s.0,
            vec![SliceDim::Slice {
                start: None,
                stop: None,
                step: Some(2),
            }]
        );
    }

    #[test]
    fn test_parse_multi_dim() {
        let s = NDSlice::from_numpy_str("1:3,4,1:5:2").unwrap();
        assert_eq!(s.ndim(), 3);
        assert_eq!(s.0[1], SliceDim::Index(4));
    }

    #[test]
    fn test_parse_ellipsis() {
        let s = NDSlice::from_numpy_str("1,...,3").unwrap();
        assert_eq!(s.0[0], SliceDim::Index(1));
        assert_eq!(s.0[1], SliceDim::Ellipsis);
        assert_eq!(s.0[2], SliceDim::Index(3));
    }

    #[test]
    fn test_double_ellipsis_error() {
        assert!(NDSlice::from_numpy_str("...,...").is_err());
    }

    #[test]
    fn test_json_roundtrip() {
        let s = NDSlice::from_numpy_str("1:3,4").unwrap();
        let json = serde_json::to_value(&s).unwrap();
        let s2: NDSlice = serde_json::from_value(json).unwrap();
        assert_eq!(s, s2);
    }

    #[test]
    fn test_to_json_with_ellipsis() {
        let s = NDSlice::from_numpy_str("1,...").unwrap();
        let json = s.to_json(Some(3)).unwrap();
        assert_eq!(json.len(), 3);
        assert_eq!(json[0], serde_json::json!(1));
        // Ellipsis fills two remaining dims with {}
        assert_eq!(json[1], serde_json::json!({}));
        assert_eq!(json[2], serde_json::json!({}));
    }

    #[test]
    fn test_slice_dim_is_full() {
        assert!(SliceDim::full().is_full());
        assert!(SliceDim::Ellipsis.is_full());
        assert!(SliceDim::Slice {
            start: Some(0),
            stop: None,
            step: Some(1),
        }
        .is_full());
        assert!(!SliceDim::Index(0).is_full());
        assert!(!SliceDim::Slice {
            start: Some(1),
            stop: Some(3),
            step: None,
        }
        .is_full());
    }
}
