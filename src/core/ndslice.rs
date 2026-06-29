//! N-dimensional slice representation.
//!
//! Corresponds to `tiled/ndslice.py` — `NDSlice`.
//!
//! Supports numpy-style string parsing (`"1:3,4,1:5:2,..."`), JSON serialization,
//! and conversion.

use serde::{Deserialize, Serialize};

use crate::core::error::{Result, TiledError};

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
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Self::Index(i) => serializer.serialize_i64(*i as i64),
            Self::Slice { start, stop, step } => {
                // Count non-None fields
                let count =
                    start.is_some() as usize + stop.is_some() as usize + step.is_some() as usize;
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
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
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
                if map.len() == 1 && map.get("step").and_then(|v| v.as_i64()) == Some(0) {
                    return Ok(Self::Ellipsis);
                }

                let start = map
                    .get("start")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as isize);
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
                let idx: isize = part
                    .parse()
                    .map_err(|_| TiledError::InvalidSlice(format!("Invalid index: '{part}'")))?;
                dims.push(SliceDim::Index(idx));
            }
        }

        // Validate: at most one ellipsis
        let ellipsis_count = dims
            .iter()
            .filter(|d| matches!(d, SliceDim::Ellipsis))
            .count();
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

    /// Whether the slice's resulting shape can be determined from the
    /// slice alone — no Ellipsis, no relative (negative or open-ended)
    /// indexing. Necessary precondition for [`Self::compose`]: the
    /// composer can't reason about a slice whose shape depends on the
    /// underlying array.
    ///
    /// Mirrors `NDSlice.is_expanded` from upstream tiled (PR #1337).
    pub fn is_expanded(&self) -> bool {
        self.0.iter().all(|d| match d {
            SliceDim::Ellipsis => false,
            SliceDim::Slice { start, stop, .. } => {
                start.unwrap_or(0) >= 0 && matches!(stop, Some(s) if *s >= 0)
            }
            SliceDim::Index(_) => true,
        })
    }

    /// Compose two slices: `arr[self][other]` is equivalent to
    /// `arr[self.compose(&other)]`. Mirrors upstream tiled
    /// `compose_slices` (PR #1337).
    ///
    /// Constraints:
    /// * `self` (the left slice) must be `is_expanded()` — no Ellipsis,
    ///   no relative indexing — because we cannot otherwise determine
    ///   the result shape.
    /// * Ellipsis is honoured in `other` only when it sits at the
    ///   **last** position; in that case the trailing dims of `self`
    ///   pass through unchanged. Other Ellipsis placements would
    ///   require shape-aware expansion and are rejected (caller can
    ///   pre-expand via [`Self::to_json`] + reconstruction if needed).
    pub fn compose(&self, other: &NDSlice) -> Result<NDSlice> {
        // Empty slice ↔ identity.
        if self.0.is_empty() {
            return Ok(other.clone());
        }
        if other.0.is_empty() {
            return Ok(self.clone());
        }
        if !self.is_expanded() {
            return Err(TiledError::InvalidSlice(
                "Composition with the left slice requires fully-expanded dims (no '...' or '/-1')"
                    .into(),
            ));
        }
        // Reject Ellipsis in `other` except as the trailing element.
        let trailing_ellipsis = matches!(other.0.last(), Some(SliceDim::Ellipsis));
        let interior_ellipsis = other
            .0
            .iter()
            .take(other.0.len().saturating_sub(1))
            .any(|d| matches!(d, SliceDim::Ellipsis));
        if interior_ellipsis {
            return Err(TiledError::InvalidSlice(
                "Composition with Ellipsis in non-last position is not supported".into(),
            ));
        }

        let mut out: Vec<SliceDim> = Vec::with_capacity(self.0.len());
        let mut i_other = 0usize;
        for s1 in &self.0 {
            // Integer dims of `self` reduce dimensionality — they pass
            // through and `other` indexes into the remaining dims only.
            if let SliceDim::Index(_) = s1 {
                out.push(s1.clone());
                continue;
            }
            // No more right-slice items? Trailing dim passes through.
            if i_other >= other.0.len() {
                out.push(s1.clone());
                continue;
            }
            let s2 = &other.0[i_other];
            i_other += 1;
            match s2 {
                SliceDim::Ellipsis => {
                    // "..." at the end → all remaining dims pass through.
                    out.push(s1.clone());
                    let _ = trailing_ellipsis; // silence unused
                    // Once we hit the ellipsis, drain the rest of self.
                    // (s2 itself isn't consumed against any further dim.)
                    out.extend(self.0[out.len()..].iter().cloned());
                    return Ok(NDSlice(out));
                }
                SliceDim::Index(idx) => {
                    out.push(SliceDim::Index(compose_slc_with_idx(s1, *idx)?));
                }
                SliceDim::Slice { .. } => {
                    out.push(compose_slc_with_slc(s1, s2)?);
                }
            }
        }
        Ok(NDSlice(out))
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
        let non_ellipsis_count = self
            .0
            .iter()
            .filter(|d| !matches!(d, SliceDim::Ellipsis))
            .count();

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

/// Apply `slc` then index by `idx`. Both must be normalised
/// (`is_expanded`) — required by upstream `_slc_with_int`.
fn compose_slc_with_idx(slc: &SliceDim, idx: isize) -> Result<isize> {
    let (start, stop, step) = match slc {
        SliceDim::Slice { start, stop, step } => (
            start.unwrap_or(0),
            stop.ok_or_else(|| {
                TiledError::InvalidSlice(
                    "Composition with relative indexing is not supported.".into(),
                )
            })?,
            step.unwrap_or(1),
        ),
        SliceDim::Index(_) => {
            return Err(TiledError::InvalidSlice(
                "Cannot index into a single-element dim".into(),
            ));
        }
        SliceDim::Ellipsis => {
            return Err(TiledError::InvalidSlice(
                "Composition with Ellipsis in left slice is not supported.".into(),
            ));
        }
    };
    if step == 0 {
        return Err(TiledError::InvalidSlice("slice step cannot be zero".into()));
    }
    let length: isize = if step > 0 {
        (stop - start + step - 1).max(0) / step
    } else {
        (start - stop - step - 1).max(0) / (-step)
    };
    let idx = if idx < 0 { idx + length } else { idx };
    if idx < 0 || idx >= length {
        return Err(TiledError::InvalidSlice(
            "Composition with out-of-bounds index".into(),
        ));
    }
    Ok(start + step * idx)
}

/// Compose two SliceDim::Slice items. Mirrors upstream `_slc_with_slc`.
fn compose_slc_with_slc(slc1: &SliceDim, slc2: &SliceDim) -> Result<SliceDim> {
    let (start1, stop1, step1) = match slc1 {
        SliceDim::Slice { start, stop, step } => (
            start.unwrap_or(0),
            stop.ok_or_else(|| {
                TiledError::InvalidSlice(
                    "Composition with relative indexing is not supported.".into(),
                )
            })?,
            step.unwrap_or(1),
        ),
        _ => {
            return Err(TiledError::InvalidSlice(
                "compose_slc_with_slc requires both args to be Slice variants".into(),
            ));
        }
    };
    if step1 == 0 {
        return Err(TiledError::InvalidSlice("slice step cannot be zero".into()));
    }
    let length: isize = if step1 > 0 {
        (stop1 - start1 + step1 - 1).max(0) / step1
    } else {
        (start1 - stop1 - step1 - 1).max(0) / (-step1)
    };
    // Apply slice2 to a 0..length range — this is Python's slice.indices().
    let (start2, stop2, step2) = match slc2 {
        SliceDim::Slice { start, stop, step } => {
            let step2 = step.unwrap_or(1);
            if step2 == 0 {
                return Err(TiledError::InvalidSlice("slice step cannot be zero".into()));
            }
            let (default_start, default_stop) = if step2 > 0 {
                (0, length)
            } else {
                (length - 1, -1)
            };
            // Normalise relative starts/stops against the post-slc1 length.
            let normalise = |v: Option<isize>, default_v: isize| -> isize {
                let mut x = v.unwrap_or(default_v);
                if x < 0 {
                    x += length;
                }
                x.clamp(if step2 > 0 { 0 } else { -1 }, length)
            };
            let s = normalise(*start, default_start);
            let t = normalise(*stop, default_stop);
            (s, t, step2)
        }
        _ => {
            return Err(TiledError::InvalidSlice(
                "compose_slc_with_slc requires both args to be Slice variants".into(),
            ));
        }
    };
    Ok(SliceDim::Slice {
        start: Some(start1 + step1 * start2),
        stop: Some(start1 + step1 * stop2),
        step: Some(step1 * step2),
    })
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
pub const SLICE_REGEX: &str =
    r"^(?:(?:-?\d+)?:){0,2}(?:-?\d+)?(?:,(?:(?:-?\d+)?:){0,2}(?:-?\d+)?)*$";

// ---- NDBlock ---------------------------------------------------------------

/// A slice over the *chunk grid* (block indices), not element indices.
///
/// Corresponds to Python `NDBlock(NDSlice)` in `tiled/ndslice.py:595`.
///
/// Invariant: every `SliceDim::Slice` must have `step == None` or `step == Some(1)`
/// (only contiguous block ranges are representable).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NDBlock(pub Vec<SliceDim>);

impl NDBlock {
    /// Construct and validate: all slice steps must be 1 or None.
    pub fn new(dims: Vec<SliceDim>) -> Result<Self> {
        for dim in &dims {
            if let SliceDim::Slice { step: Some(s), .. } = dim
                && *s != 1
            {
                return Err(TiledError::InvalidSlice(format!(
                    "NDBlock can only contain slices with step 1; got step={s}"
                )));
            }
        }
        Ok(Self(dims))
    }

    /// Returns the `NDSlice` over the full array that covers the region this
    /// block occupies.
    ///
    /// Corresponds to `NDBlock.slice_from_chunks` in `tiled/ndslice.py:619`.
    pub fn slice_from_chunks(&self, chunks: &[Vec<usize>]) -> NDSlice {
        let mut dims = Vec::with_capacity(self.0.len());
        for (dim_spec, dim_chunks) in self.0.iter().zip(chunks.iter()) {
            let cumsum = prefix_sums(dim_chunks);
            let n = dim_chunks.len();
            match dim_spec {
                SliceDim::Index(i) => {
                    let i = norm_chunk_idx(*i, n);
                    dims.push(SliceDim::Slice {
                        start: Some(cumsum[i] as isize),
                        stop: Some((cumsum[i] + dim_chunks[i]) as isize),
                        step: None,
                    });
                }
                SliceDim::Slice { start, stop, .. } => {
                    // clamp chunk indices into [0, n]
                    let clamp_chunk = |v: isize| -> usize {
                        let v = if v < 0 { v + n as isize } else { v };
                        (v.max(0) as usize).min(n)
                    };
                    let array_start = start.map(|s| cumsum[clamp_chunk(s)] as isize);
                    let array_stop = stop.map(|s| cumsum[clamp_chunk(s)] as isize);
                    dims.push(SliceDim::Slice {
                        start: array_start,
                        stop: array_stop,
                        step: None,
                    });
                }
                // Treat Ellipsis as a full-range slice (full array region).
                SliceDim::Ellipsis => {
                    dims.push(SliceDim::Slice {
                        start: None,
                        stop: None,
                        step: None,
                    });
                }
            }
        }
        NDSlice(dims)
    }

    /// Returns every n-dimensional chunk-index tuple inside this block, sorted.
    ///
    /// Each element of the returned `Vec` is a `Vec<usize>` of length `ndim`.
    ///
    /// Corresponds to `NDBlock.chunk_indices` in `tiled/ndslice.py:634`.
    pub fn chunk_indices(&self, chunks: &[Vec<usize>]) -> Vec<Vec<usize>> {
        let mut per_dim: Vec<Vec<usize>> = Vec::with_capacity(self.0.len());

        for (dim_spec, dim_chunks) in self.0.iter().zip(chunks.iter()) {
            let n = dim_chunks.len();
            match dim_spec {
                SliceDim::Index(i) => {
                    per_dim.push(vec![norm_chunk_idx(*i, n)]);
                }
                SliceDim::Slice { start, stop, .. } => {
                    let start = start
                        .map(|s| {
                            let s = if s < 0 { s + n as isize } else { s };
                            s.max(0) as usize
                        })
                        .unwrap_or(0);
                    let stop = stop
                        .map(|s| {
                            let s = if s < 0 { s + n as isize } else { s };
                            (s.max(0) as usize).min(n)
                        })
                        .unwrap_or(n);
                    per_dim.push((start..stop).collect());
                }
                SliceDim::Ellipsis => {
                    per_dim.push((0..n).collect());
                }
            }
        }

        if per_dim.is_empty() {
            return vec![];
        }

        // Cartesian product of per-dim index lists.
        let mut result: Vec<Vec<usize>> = vec![vec![]];
        for indices in per_dim {
            if indices.is_empty() {
                return vec![]; // Empty dim → no chunks intersected
            }
            result = result
                .into_iter()
                .flat_map(|prefix| {
                    indices.iter().map(move |&i| {
                        let mut row = prefix.clone();
                        row.push(i);
                        row
                    })
                })
                .collect();
        }
        result.sort();
        result
    }
}

// ---- block_for_slice -------------------------------------------------------

/// Compute which block of chunks an `NDSlice` touches, and the adjusted slice
/// within the concatenated block to recover the final result.
///
/// Returns `(block, slice_within_block)` where:
/// * `block` is an `NDBlock` selecting the contiguous range of chunks touched.
/// * `slice_within_block` is the `NDSlice` to apply to `np.block(read_chunks(block))`
///   to obtain `array[slice]`.
///
/// Corresponds to `block_for_slice` in `tiled/ndslice.py:669`.
pub fn block_for_slice(
    chunks: &[Vec<usize>],
    slice: Option<&NDSlice>,
) -> Result<(NDBlock, NDSlice)> {
    let ndim = chunks.len();

    // Empty (no-dims) slice → all blocks, identity adjusted slice.
    // Mirrors: `if not slice: return (NDBlock(*all_ranges), NDSlice())`
    let is_nodim = slice.map(|s| s.0.is_empty()).unwrap_or(true);
    if is_nodim {
        let block_dims = chunks
            .iter()
            .map(|dim| SliceDim::Slice {
                start: Some(0),
                stop: Some(dim.len() as isize),
                step: None,
            })
            .collect();
        return Ok((NDBlock(block_dims), NDSlice::empty()));
    }

    let slice = slice.unwrap();

    // Per-dim shape from chunks.
    let shape: Vec<usize> = chunks.iter().map(|dim| dim.iter().sum()).collect();

    // Expand Ellipsis to full slices, pad trailing dims.
    let expanded = expand_slice_for_shape(slice, &shape)?;

    let mut block_dims = Vec::with_capacity(ndim);
    let mut adjusted_dims = Vec::with_capacity(ndim);

    for (dim_idx, (slc, dim_chunks)) in expanded.iter().zip(chunks.iter()).enumerate() {
        let bounds = prefix_sums(dim_chunks); // [0, c0, c0+c1, ...]
        let dim_len = shape[dim_idx];

        match slc {
            SliceDim::Index(i) => {
                let n = dim_len as isize;
                let i_norm = if *i < 0 { *i + n } else { *i };
                if i_norm < 0 || i_norm >= n {
                    return Err(TiledError::InvalidSlice(format!(
                        "Index {i} out of bounds for dimension {dim_idx} with shape {n}"
                    )));
                }
                let chunk_idx = bisect_right(&bounds, i_norm as usize) - 1;
                block_dims.push(SliceDim::Index(chunk_idx as isize));
                adjusted_dims.push(SliceDim::Index(i_norm - bounds[chunk_idx] as isize));
            }

            SliceDim::Slice { start, stop, step } => {
                let (start, stop, step) = py_slice_indices(*start, *stop, *step, dim_len)?;

                if step > 0 {
                    if start >= stop {
                        // Empty slice — no chunks touched.
                        block_dims.push(empty_range());
                        adjusted_dims.push(empty_range());
                        continue;
                    }
                    let first = bisect_right(&bounds, start as usize) - 1;
                    let last = bisect_right(&bounds, (stop - 1) as usize) - 1;
                    block_dims.push(make_block_range(first, last));

                    let first_start = bounds[first] as isize;
                    let norm_step = if step == 1 { None } else { Some(step) };
                    adjusted_dims.push(SliceDim::Slice {
                        start: Some(start - first_start),
                        stop: Some(stop - first_start),
                        step: norm_step,
                    });
                } else {
                    // step < 0
                    if start <= stop {
                        // Empty slice.
                        block_dims.push(empty_range());
                        adjusted_dims.push(empty_range());
                        continue;
                    }
                    let first_raw = bisect_right(&bounds, start as usize) - 1;
                    // stop+1 >= 0 always (py_slice_indices clamps stop >= -1 for neg step)
                    let last_raw = bisect_right(&bounds, (stop + 1) as usize) - 1;
                    let (first, last) = (first_raw.min(last_raw), first_raw.max(last_raw));
                    block_dims.push(make_block_range(first, last));

                    let first_start = bounds[first] as isize;
                    // If stop falls before the start of the first_chunk, adjusted stop is None
                    // (means "go all the way to the beginning of the block").
                    let adj_stop = if stop < first_start {
                        None
                    } else {
                        Some(stop - first_start)
                    };
                    adjusted_dims.push(SliceDim::Slice {
                        start: Some(start - first_start),
                        stop: adj_stop,
                        step: Some(step),
                    });
                }
            }

            SliceDim::Ellipsis => {
                unreachable!("Ellipsis should have been expanded by expand_slice_for_shape");
            }
        }
    }

    Ok((NDBlock(block_dims), NDSlice(adjusted_dims)))
}

// ---- private helpers -------------------------------------------------------

/// Prefix sums (chunk boundaries): `[0, c0, c0+c1, …, total]`.
fn prefix_sums(dim_chunks: &[usize]) -> Vec<usize> {
    let mut sums = Vec::with_capacity(dim_chunks.len() + 1);
    sums.push(0usize);
    let mut acc = 0usize;
    for &c in dim_chunks {
        acc += c;
        sums.push(acc);
    }
    sums
}

/// Right-biased binary search: first index `i` s.t. `sorted[i] > val`.
/// Mirrors `bisect.bisect_right(sorted, val)`.
fn bisect_right(sorted: &[usize], val: usize) -> usize {
    sorted.partition_point(|&x| x <= val)
}

/// Normalise a possibly-negative chunk index into `[0, n)` as a `usize`.
fn norm_chunk_idx(i: isize, n: usize) -> usize {
    (if i < 0 { i + n as isize } else { i }) as usize
}

/// Produce a `SliceDim` for a contiguous inclusive range `[first, last]` of
/// chunk indices.  Single-chunk ranges become `Index` variants.
fn make_block_range(first: usize, last: usize) -> SliceDim {
    if first == last {
        SliceDim::Index(first as isize)
    } else {
        SliceDim::Slice {
            start: Some(first as isize),
            stop: Some((last + 1) as isize),
            step: None,
        }
    }
}

/// `SliceDim` representing an empty range `[0, 0)`.
fn empty_range() -> SliceDim {
    SliceDim::Slice {
        start: Some(0),
        stop: Some(0),
        step: None,
    }
}

/// Expand an `NDSlice` to `shape.len()` dims, replacing Ellipsis with full
/// slices and padding trailing dims.  Mirrors `NDSlice.expand_for_shape`.
fn expand_slice_for_shape(slice: &NDSlice, shape: &[usize]) -> Result<Vec<SliceDim>> {
    let ndim = shape.len();
    let non_ellipsis = slice
        .0
        .iter()
        .filter(|d| !matches!(d, SliceDim::Ellipsis))
        .count();
    if non_ellipsis > ndim {
        return Err(TiledError::InvalidSlice(format!(
            "Slice has {non_ellipsis} dims but array has {ndim} dims"
        )));
    }
    let fill = ndim - non_ellipsis;
    let mut result = Vec::with_capacity(ndim);
    for dim in &slice.0 {
        if matches!(dim, SliceDim::Ellipsis) {
            for _ in 0..fill {
                result.push(SliceDim::full());
            }
        } else {
            result.push(dim.clone());
        }
    }
    // Pad if slice has fewer dims than ndim and no ellipsis.
    while result.len() < ndim {
        result.push(SliceDim::full());
    }
    Ok(result)
}

/// Mirror Python's `slice.indices(length)`: normalise start/stop/step for a
/// sequence of `length` elements.  Returns `(start, stop, step)`.
fn py_slice_indices(
    start: Option<isize>,
    stop: Option<isize>,
    step: Option<isize>,
    length: usize,
) -> Result<(isize, isize, isize)> {
    let step = step.unwrap_or(1);
    if step == 0 {
        return Err(TiledError::InvalidSlice("slice step cannot be zero".into()));
    }
    let n = length as isize;
    let (lo, hi, start_def, stop_def) = if step > 0 {
        (0isize, n, 0isize, n)
    } else {
        (-1isize, n - 1, n - 1, -1isize)
    };
    let clamp = |v: isize| v.max(lo).min(hi);
    let norm = |v: Option<isize>, default: isize| match v {
        None => default,
        Some(x) => clamp(if x < 0 { x + n } else { x }),
    };
    Ok((norm(start, start_def), norm(stop, stop_def), step))
}

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
    fn test_is_expanded() {
        // No ellipsis, absolute starts/stops → expanded.
        assert!(NDSlice::from_numpy_str("1:5,2:4").unwrap().is_expanded());
        assert!(NDSlice::from_numpy_str("3,1:5").unwrap().is_expanded());
        // Ellipsis → not expanded.
        assert!(!NDSlice::from_numpy_str("1:5,...").unwrap().is_expanded());
        // Open-ended (None stop) → not expanded.
        assert!(!NDSlice::from_numpy_str("1:").unwrap().is_expanded());
        // Negative start → not expanded.
        let s = NDSlice(vec![SliceDim::Slice {
            start: Some(-1),
            stop: Some(5),
            step: None,
        }]);
        assert!(!s.is_expanded());
    }

    #[test]
    fn test_compose_simple() {
        // arr[0:10][2:8] = arr[2:8]
        let a = NDSlice::from_numpy_str("0:10").unwrap();
        let b = NDSlice::from_numpy_str("2:8").unwrap();
        let c = a.compose(&b).unwrap();
        assert_eq!(
            c.0[0],
            SliceDim::Slice {
                start: Some(2),
                stop: Some(8),
                step: Some(1)
            }
        );
    }

    #[test]
    fn test_compose_strided() {
        // arr[1:9:2][0:3] → start=1, length=4 (1,3,5,7), [0:3]→1,3,5
        // expected: start=1, stop=7, step=2
        let a = NDSlice::from_numpy_str("1:9:2").unwrap();
        let b = NDSlice::from_numpy_str("0:3").unwrap();
        let c = a.compose(&b).unwrap();
        assert_eq!(
            c.0[0],
            SliceDim::Slice {
                start: Some(1),
                stop: Some(7),
                step: Some(2)
            }
        );
    }

    #[test]
    fn test_compose_with_index() {
        // arr[2:10][3] → arr[2 + 1*3] = arr[5]
        let a = NDSlice::from_numpy_str("2:10").unwrap();
        let b = NDSlice::from_numpy_str("3").unwrap();
        let c = a.compose(&b).unwrap();
        assert_eq!(c.0[0], SliceDim::Index(5));
    }

    #[test]
    fn test_compose_passthrough_integer_dim() {
        // arr[5,1:10][0:3]: integer dim (5) is consumed; right slice
        // applies to the second dim only.
        // Result: [5, 1:4]
        let a = NDSlice::from_numpy_str("5,1:10").unwrap();
        let b = NDSlice::from_numpy_str("0:3").unwrap();
        let c = a.compose(&b).unwrap();
        assert_eq!(c.0[0], SliceDim::Index(5));
        assert_eq!(
            c.0[1],
            SliceDim::Slice {
                start: Some(1),
                stop: Some(4),
                step: Some(1)
            }
        );
    }

    #[test]
    fn test_compose_empty_is_identity() {
        let a = NDSlice::from_numpy_str("1:5").unwrap();
        let empty = NDSlice::empty();
        assert_eq!(a.compose(&empty).unwrap(), a);
        assert_eq!(empty.compose(&a).unwrap(), a);
    }

    #[test]
    fn test_compose_left_unexpanded_rejected() {
        // Left slice with ellipsis cannot be composed.
        let a = NDSlice::from_numpy_str("...,2:4").unwrap();
        let b = NDSlice::from_numpy_str("0:1").unwrap();
        assert!(a.compose(&b).is_err());
    }

    #[test]
    fn test_compose_right_interior_ellipsis_rejected() {
        let a = NDSlice::from_numpy_str("0:5,0:5,0:5").unwrap();
        let b = NDSlice::from_numpy_str("0,...,0").unwrap();
        assert!(a.compose(&b).is_err());
    }

    #[test]
    fn test_slice_dim_is_full() {
        assert!(SliceDim::full().is_full());
        assert!(SliceDim::Ellipsis.is_full());
        assert!(
            SliceDim::Slice {
                start: Some(0),
                stop: None,
                step: Some(1),
            }
            .is_full()
        );
        assert!(!SliceDim::Index(0).is_full());
        assert!(
            !SliceDim::Slice {
                start: Some(1),
                stop: Some(3),
                step: None,
            }
            .is_full()
        );
    }

    // ---- NDBlock / block_for_slice tests ------------------------------------
    // Tests are organised by boundary, not by narrative scenario.

    fn chunks_2d() -> Vec<Vec<usize>> {
        vec![vec![10, 10], vec![20, 20]]
    }

    // boundary: None / empty slice → all blocks returned, identity adjusted
    #[test]
    fn block_for_slice_full_array_none() {
        let chunks = chunks_2d();
        let (block, adj) = block_for_slice(&chunks, None).unwrap();
        assert_eq!(
            block.0,
            vec![
                SliceDim::Slice {
                    start: Some(0),
                    stop: Some(2),
                    step: None
                },
                SliceDim::Slice {
                    start: Some(0),
                    stop: Some(2),
                    step: None
                },
            ]
        );
        assert!(adj.is_empty());
    }

    // boundary: empty NDSlice (no dims) behaves identically to None
    #[test]
    fn block_for_slice_full_array_empty_slice() {
        let chunks = chunks_2d();
        let empty = NDSlice::empty();
        let (block, adj) = block_for_slice(&chunks, Some(&empty)).unwrap();
        assert_eq!(
            block.0[0],
            SliceDim::Slice {
                start: Some(0),
                stop: Some(2),
                step: None
            }
        );
        assert!(adj.is_empty());
    }

    // boundary: slice stays fully inside a single chunk on every dimension
    #[test]
    fn block_for_slice_fully_inside_one_chunk() {
        let chunks = chunks_2d(); // shape (20, 40)
        let slice = NDSlice(vec![
            SliceDim::Slice {
                start: Some(2),
                stop: Some(8),
                step: None,
            },
            SliceDim::Slice {
                start: Some(5),
                stop: Some(15),
                step: None,
            },
        ]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        // Both dims are entirely within chunk 0
        assert_eq!(block.0, vec![SliceDim::Index(0), SliceDim::Index(0)]);
        // Adjusted start equals slice start (first_chunk_start == 0)
        assert_eq!(
            adj.0[0],
            SliceDim::Slice {
                start: Some(2),
                stop: Some(8),
                step: None
            }
        );
        assert_eq!(
            adj.0[1],
            SliceDim::Slice {
                start: Some(5),
                stop: Some(15),
                step: None
            }
        );
    }

    // boundary: slice crosses a chunk boundary on both dimensions
    #[test]
    fn block_for_slice_spans_chunk_boundary() {
        let chunks = chunks_2d(); // shape (20, 40)
        let slice = NDSlice(vec![
            SliceDim::Slice {
                start: Some(5),
                stop: Some(15),
                step: None,
            },
            SliceDim::Slice {
                start: Some(15),
                stop: Some(35),
                step: None,
            },
        ]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        assert_eq!(
            block.0,
            vec![
                SliceDim::Slice {
                    start: Some(0),
                    stop: Some(2),
                    step: None
                },
                SliceDim::Slice {
                    start: Some(0),
                    stop: Some(2),
                    step: None
                },
            ]
        );
        // Adjusted: start relative to first chunk (chunk 0 starts at 0)
        assert_eq!(
            adj.0[0],
            SliceDim::Slice {
                start: Some(5),
                stop: Some(15),
                step: None
            }
        );
        assert_eq!(
            adj.0[1],
            SliceDim::Slice {
                start: Some(15),
                stop: Some(35),
                step: None
            }
        );
    }

    // boundary: integer index selects a single element (and hence a single chunk)
    #[test]
    fn block_for_slice_integer_index() {
        let chunks = chunks_2d(); // shape (20, 40), 2×2 blocks of 10×20
        // indices (15, 25): dim0 → chunk 1 (10..20), dim1 → chunk 1 (20..40)
        let slice = NDSlice(vec![SliceDim::Index(15), SliceDim::Index(25)]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        assert_eq!(block.0, vec![SliceDim::Index(1), SliceDim::Index(1)]);
        // Adjusted: offset within chunk → 15-10=5, 25-20=5
        assert_eq!(adj.0, vec![SliceDim::Index(5), SliceDim::Index(5)]);
    }

    // boundary: step > 1 crosses chunk boundary — step preserved in adjusted
    #[test]
    fn block_for_slice_with_step() {
        let chunks = chunks_2d(); // shape (20, 40)
        let slice = NDSlice(vec![
            SliceDim::Slice {
                start: Some(5),
                stop: Some(25),
                step: Some(2),
            },
            SliceDim::Slice {
                start: Some(10),
                stop: Some(30),
                step: None,
            },
        ]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        // dim0 spans chunks 0 and 1
        assert_eq!(
            block.0[0],
            SliceDim::Slice {
                start: Some(0),
                stop: Some(2),
                step: None
            }
        );
        // Adjusted start relative to first chunk (start=0)
        assert_eq!(
            adj.0[0],
            SliceDim::Slice {
                start: Some(5),
                stop: Some(20),
                step: Some(2)
            }
        );
    }

    // boundary: step=-1 crossing a chunk boundary
    #[test]
    fn block_for_slice_negative_step_crossing_boundary() {
        let chunks = chunks_2d(); // shape (20, 40)
        let slice = NDSlice(vec![
            SliceDim::Slice {
                start: Some(15),
                stop: Some(5),
                step: Some(-1),
            },
            SliceDim::Slice {
                start: Some(10),
                stop: Some(30),
                step: None,
            },
        ]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        // dim0: traverses 15..6, spans chunks 0 and 1
        assert_eq!(
            block.0[0],
            SliceDim::Slice {
                start: Some(0),
                stop: Some(2),
                step: None
            }
        );
        assert_eq!(
            adj.0[0],
            SliceDim::Slice {
                start: Some(15),
                stop: Some(5),
                step: Some(-1)
            }
        );
    }

    // boundary: step=-1 traversing to the very beginning of the array (stop=None in adjusted)
    #[test]
    fn block_for_slice_negative_step_to_beginning() {
        let chunks = chunks_2d(); // shape (20, 40)
        // slice(None, None, -1) on dim0: Python normalises to start=19, stop=-1 (sentinel)
        let slice = NDSlice(vec![
            SliceDim::Slice {
                start: None,
                stop: None,
                step: Some(-1),
            },
            SliceDim::Slice {
                start: Some(10),
                stop: Some(30),
                step: None,
            },
        ]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        assert_eq!(
            block.0[0],
            SliceDim::Slice {
                start: Some(0),
                stop: Some(2),
                step: None
            }
        );
        // adjusted stop must be None (goes past the start of the first chunk)
        match &adj.0[0] {
            SliceDim::Slice { start, stop, step } => {
                assert_eq!(*start, Some(19));
                assert_eq!(*stop, None); // sentinel: before index 0 in the block
                assert_eq!(*step, Some(-1));
            }
            other => panic!("Expected Slice, got {other:?}"),
        }
    }

    // boundary: empty slice (start >= stop for positive step) → empty block range
    #[test]
    fn block_for_slice_empty_slice_positive_step() {
        let chunks = chunks_2d();
        let slice = NDSlice(vec![
            // start=8 >= stop=5 with positive step → empty
            SliceDim::Slice {
                start: Some(8),
                stop: Some(5),
                step: None,
            },
            SliceDim::Slice {
                start: Some(0),
                stop: Some(10),
                step: None,
            },
        ]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        assert_eq!(
            block.0[0],
            SliceDim::Slice {
                start: Some(0),
                stop: Some(0),
                step: None
            }
        );
        assert_eq!(
            adj.0[0],
            SliceDim::Slice {
                start: Some(0),
                stop: Some(0),
                step: None
            }
        );
    }

    // boundary: partial final chunk (slice ends mid-last-chunk)
    #[test]
    fn block_for_slice_partial_final_chunk() {
        // shape (30,): three chunks of 10 each
        let chunks = vec![vec![10usize, 10, 10]];
        let slice = NDSlice(vec![SliceDim::Slice {
            start: Some(5),
            stop: Some(25),
            step: None,
        }]);
        let (block, adj) = block_for_slice(&chunks, Some(&slice)).unwrap();
        // touches chunks 0 (0..10) and 1 (10..20) and 2 (20..30), but stop=25 is inside chunk 2
        assert_eq!(
            block.0[0],
            SliceDim::Slice {
                start: Some(0),
                stop: Some(3),
                step: None
            }
        );
        // adjusted: first_chunk_start=0, adj_start=5, adj_stop=25
        assert_eq!(
            adj.0[0],
            SliceDim::Slice {
                start: Some(5),
                stop: Some(25),
                step: None
            }
        );
    }

    // boundary: out-of-bounds integer index → error
    #[test]
    fn block_for_slice_out_of_bounds() {
        let chunks = chunks_2d(); // shape (20, 40)
        let slice = NDSlice(vec![SliceDim::Index(50), SliceDim::Index(10)]);
        assert!(block_for_slice(&chunks, Some(&slice)).is_err());
    }

    // boundary: NDBlock::chunk_indices — single integer dims only
    #[test]
    fn chunk_indices_single_chunk() {
        let chunks = vec![vec![10usize, 10, 10], vec![20, 20]];
        let block = NDBlock(vec![SliceDim::Index(1), SliceDim::Index(0)]);
        assert_eq!(block.chunk_indices(&chunks), vec![vec![1usize, 0]]);
    }

    // boundary: NDBlock::chunk_indices — mix of int and range dims
    #[test]
    fn chunk_indices_mixed_dims() {
        let chunks = vec![vec![10usize, 10, 10], vec![20, 20]];
        // slice over dim0=[0,1], int dim1=1
        let block = NDBlock(vec![
            SliceDim::Slice {
                start: Some(0),
                stop: Some(2),
                step: None,
            },
            SliceDim::Index(1),
        ]);
        let mut expected = vec![vec![0usize, 1], vec![1, 1]];
        expected.sort();
        assert_eq!(block.chunk_indices(&chunks), expected);
    }

    // boundary: NDBlock::chunk_indices — 2D Cartesian product
    #[test]
    fn chunk_indices_2d_cartesian_product() {
        let chunks = vec![vec![10usize, 10, 10], vec![20, 20]];
        let block = NDBlock(vec![
            SliceDim::Slice {
                start: Some(0),
                stop: Some(2),
                step: None,
            },
            SliceDim::Slice {
                start: Some(0),
                stop: Some(2),
                step: None,
            },
        ]);
        let expected = vec![vec![0usize, 0], vec![0, 1], vec![1, 0], vec![1, 1]];
        assert_eq!(block.chunk_indices(&chunks), expected);
    }

    // boundary: NDBlock::slice_from_chunks — integer dim → precise element slice
    #[test]
    fn slice_from_chunks_integer_dim() {
        // chunks shape (30,): three chunks of 10
        let chunks = vec![vec![10usize, 10, 10]];
        // block selects chunk 1 (element range 10..20)
        let block = NDBlock(vec![SliceDim::Index(1)]);
        let s = block.slice_from_chunks(&chunks);
        assert_eq!(
            s.0,
            vec![SliceDim::Slice {
                start: Some(10),
                stop: Some(20),
                step: None
            }]
        );
    }

    // boundary: NDBlock::slice_from_chunks — range dim → element range
    #[test]
    fn slice_from_chunks_range_dim() {
        let chunks = vec![vec![10usize, 10, 10]];
        // block selects chunks [0, 2) → element range 0..20
        let block = NDBlock(vec![SliceDim::Slice {
            start: Some(0),
            stop: Some(2),
            step: None,
        }]);
        let s = block.slice_from_chunks(&chunks);
        assert_eq!(
            s.0,
            vec![SliceDim::Slice {
                start: Some(0),
                stop: Some(20),
                step: None
            }]
        );
    }

    // boundary: NDBlock::slice_from_chunks — None start/stop pass through as None
    #[test]
    fn slice_from_chunks_none_start_stop() {
        let chunks = vec![vec![10usize, 10]];
        let block = NDBlock(vec![SliceDim::Slice {
            start: None,
            stop: None,
            step: None,
        }]);
        let s = block.slice_from_chunks(&chunks);
        // None start/stop → cumsum[0] = 0 is used for start; cumsum[n] = total for stop —
        // but since original is None, they stay None.
        assert_eq!(
            s.0,
            vec![SliceDim::Slice {
                start: None,
                stop: None,
                step: None
            }]
        );
    }

    // boundary: NDBlock::new validates step constraint
    #[test]
    fn ndblock_new_rejects_nonunit_step() {
        let bad = vec![SliceDim::Slice {
            start: Some(0),
            stop: Some(4),
            step: Some(2),
        }];
        assert!(NDBlock::new(bad).is_err());
    }
}
