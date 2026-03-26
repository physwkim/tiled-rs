//! NumPy dtype system mapped to Rust types.
//!
//! Corresponds to `tiled/structures/array.py` — `BuiltinDtype`, `StructDtype`, `Field`, `Kind`, `Endianness`.

use serde::{Deserialize, Serialize};

use crate::error::{Result, TiledError};

/// Byte order of numeric data.
///
/// Maps to Python `Endianness(str, enum.Enum)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endianness {
    Big,
    Little,
    NotApplicable,
}

impl Endianness {
    /// Convert from numpy byte-order character.
    pub fn from_numpy_char(c: char) -> Result<Self> {
        match c {
            '>' => Ok(Self::Big),
            '<' => Ok(Self::Little),
            '|' => Ok(Self::NotApplicable),
            '=' => {
                // Native endianness
                if cfg!(target_endian = "big") {
                    Ok(Self::Big)
                } else {
                    Ok(Self::Little)
                }
            }
            _ => Err(TiledError::InvalidDType(format!(
                "Unknown endianness char: '{c}'"
            ))),
        }
    }

    /// Convert to numpy byte-order character.
    pub fn to_numpy_char(self) -> char {
        match self {
            Self::Big => '>',
            Self::Little => '<',
            Self::NotApplicable => '|',
        }
    }
}

/// NumPy dtype kind codes.
///
/// See <https://numpy.org/devdocs/reference/arrays.interface.html#object.__array_interface__>
///
/// Maps to Python `Kind(str, enum.Enum)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Kind {
    /// Bit field (`t`)
    #[serde(rename = "t")]
    BitField,
    /// Boolean (`b`)
    #[serde(rename = "b")]
    Boolean,
    /// Signed integer (`i`)
    #[serde(rename = "i")]
    Integer,
    /// Unsigned integer (`u`)
    #[serde(rename = "u")]
    UnsignedInteger,
    /// IEEE floating point (`f`)
    #[serde(rename = "f")]
    Float,
    /// Complex floating point (`c`)
    #[serde(rename = "c")]
    ComplexFloat,
    /// Timedelta (`m`)
    #[serde(rename = "m")]
    Timedelta,
    /// Datetime (`M`)
    #[serde(rename = "M")]
    Datetime,
    /// Fixed-length byte string (`S`)
    #[serde(rename = "S")]
    String,
    /// Fixed-length unicode string (`U`)
    #[serde(rename = "U")]
    Unicode,
    /// Void / other fixed-size chunk (`V`)
    #[serde(rename = "V")]
    Other,
}

impl Kind {
    /// Convert from numpy kind character.
    pub fn from_numpy_char(c: char) -> Result<Self> {
        match c {
            't' => Ok(Self::BitField),
            'b' => Ok(Self::Boolean),
            'i' => Ok(Self::Integer),
            'u' => Ok(Self::UnsignedInteger),
            'f' => Ok(Self::Float),
            'c' => Ok(Self::ComplexFloat),
            'm' => Ok(Self::Timedelta),
            'M' => Ok(Self::Datetime),
            'S' => Ok(Self::String),
            'U' => Ok(Self::Unicode),
            'V' => Ok(Self::Other),
            _ => Err(TiledError::InvalidDType(format!(
                "Unknown dtype kind char: '{c}'"
            ))),
        }
    }

    /// Convert to numpy kind character.
    pub fn to_numpy_char(self) -> char {
        match self {
            Self::BitField => 't',
            Self::Boolean => 'b',
            Self::Integer => 'i',
            Self::UnsignedInteger => 'u',
            Self::Float => 'f',
            Self::ComplexFloat => 'c',
            Self::Timedelta => 'm',
            Self::Datetime => 'M',
            Self::String => 'S',
            Self::Unicode => 'U',
            Self::Other => 'V',
        }
    }
}

/// A built-in (scalar) NumPy dtype.
///
/// Maps to Python `BuiltinDtype` dataclass.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BuiltinDType {
    pub endianness: Endianness,
    pub kind: Kind,
    /// Size in bytes.
    pub itemsize: usize,
    /// Datetime/timedelta units, e.g. `"[ns]"`, `"[us]"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dt_units: Option<String>,
}

impl BuiltinDType {
    pub fn new(endianness: Endianness, kind: Kind, itemsize: usize) -> Self {
        Self {
            endianness,
            kind,
            itemsize,
            dt_units: None,
        }
    }

    /// Construct from a numpy dtype string like `"<f8"`, `">i4"`, `"<M8[ns]"`.
    pub fn from_numpy_str(s: &str) -> Result<Self> {
        // Dtype strings are ASCII: "<f8", ">i4", "<M8[ns]" — safe to index bytes.
        let bytes = s.as_bytes();
        if bytes.len() < 3 {
            return Err(TiledError::InvalidDType(format!(
                "Dtype string too short: '{s}'"
            )));
        }

        let endianness = Endianness::from_numpy_char(bytes[0] as char)?;
        let kind = Kind::from_numpy_char(bytes[1] as char)?;

        let rest = &s[2..];
        let (size_str, dt_units) = if let Some(bracket_pos) = rest.find('[') {
            (&rest[..bracket_pos], Some(rest[bracket_pos..].to_string()))
        } else {
            (rest, None)
        };

        let size: usize = size_str.parse().map_err(|_| {
            TiledError::InvalidDType(format!("Cannot parse itemsize from '{s}'"))
        })?;

        // Unicode stores UCS4 (4 bytes per char), but numpy string format uses char count
        let itemsize = if kind == Kind::Unicode {
            size * 4
        } else {
            size
        };

        Ok(Self {
            endianness,
            kind,
            itemsize,
            dt_units,
        })
    }

    /// Convert to a numpy dtype string like `"<f8"`.
    pub fn to_numpy_str(&self) -> String {
        let endian = self.endianness.to_numpy_char();
        let kind = self.kind.to_numpy_char();
        // Unicode: numpy reports itemsize in bytes but string format uses char count
        let size = if self.kind == Kind::Unicode {
            self.itemsize / 4
        } else {
            self.itemsize
        };
        let units = self.dt_units.as_deref().unwrap_or("");
        format!("{endian}{kind}{size}{units}")
    }

    /// Convert from JSON representation (as used in tiled wire format).
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone()).map_err(|e| {
            TiledError::InvalidDType(format!("Cannot parse BuiltinDType from JSON: {e}"))
        })
    }

    /// Size of a single element in bytes.
    pub fn element_size(&self) -> usize {
        self.itemsize
    }
}

/// A single field within a structured dtype.
///
/// Maps to Python `Field` dataclass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub dtype: DType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shape: Option<Vec<usize>>,
}

impl Field {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        let name = value["name"]
            .as_str()
            .ok_or_else(|| TiledError::InvalidDType("Field missing 'name'".into()))?
            .to_string();

        let dtype_val = &value["dtype"];
        let dtype = if dtype_val.get("fields").is_some() {
            DType::Struct(StructDType::from_json(dtype_val)?)
        } else {
            DType::Builtin(BuiltinDType::from_json(dtype_val)?)
        };

        let shape = value.get("shape").and_then(|v| {
            if v.is_null() {
                None
            } else {
                v.as_array().map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_u64().map(|n| n as usize))
                        .collect()
                })
            }
        });

        Ok(Self { name, dtype, shape })
    }
}

/// A structured (record) NumPy dtype containing named fields.
///
/// Maps to Python `StructDtype` dataclass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructDType {
    pub itemsize: usize,
    pub fields: Vec<Field>,
}

impl StructDType {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        let itemsize = value["itemsize"]
            .as_u64()
            .ok_or_else(|| TiledError::InvalidDType("StructDtype missing 'itemsize'".into()))?
            as usize;

        let fields = value["fields"]
            .as_array()
            .ok_or_else(|| TiledError::InvalidDType("StructDtype missing 'fields'".into()))?
            .iter()
            .map(Field::from_json)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { itemsize, fields })
    }

    /// Maximum nesting depth.
    pub fn max_depth(&self) -> usize {
        self.fields
            .iter()
            .map(|f| match &f.dtype {
                DType::Builtin(_) => 1,
                DType::Struct(s) => 1 + s.max_depth(),
            })
            .max()
            .unwrap_or(0)
    }
}

/// Either a builtin or structured dtype.
///
/// Maps to Python `Union[BuiltinDtype, StructDtype]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DType {
    Builtin(BuiltinDType),
    Struct(StructDType),
}

impl DType {
    /// Parse from JSON, detecting whether it's a builtin or struct dtype.
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        if value.get("fields").is_some() {
            Ok(Self::Struct(StructDType::from_json(value)?))
        } else {
            Ok(Self::Builtin(BuiltinDType::from_json(value)?))
        }
    }

    /// Size of a single element in bytes.
    pub fn element_size(&self) -> usize {
        match self {
            Self::Builtin(b) => b.element_size(),
            Self::Struct(s) => s.itemsize,
        }
    }
}

/// Runtime dynamic N-dimensional array.
///
/// Holds raw bytes interpreted by a `BuiltinDType`. Uses `bytes::Bytes` for zero-copy
/// slicing and reference-counted sharing.
///
/// This is the Rust equivalent of `numpy.ndarray` — deliberately *not* using Rust generics
/// (like `ndarray::Array<f64, IxDyn>`) to avoid generic type explosion, since the dtype
/// is only known at runtime (matching Python's dynamic typing model).
#[derive(Debug, Clone)]
pub struct DynNDArray {
    /// Raw element data (reference-counted, zero-copy sliceable).
    pub data: bytes::Bytes,
    /// Interpretation of the raw bytes.
    pub dtype: BuiltinDType,
    /// Shape of the array, e.g. `[1000, 1000]`.
    pub shape: Vec<usize>,
    /// Byte strides per dimension (C-order by default).
    pub strides: Vec<isize>,
}

impl DynNDArray {
    /// Create a new DynNDArray with C-contiguous strides.
    pub fn new(data: bytes::Bytes, dtype: BuiltinDType, shape: Vec<usize>) -> Self {
        let strides = c_strides(&shape, dtype.element_size());
        Self {
            data,
            dtype,
            shape,
            strides,
        }
    }

    /// Total number of elements.
    pub fn len(&self) -> usize {
        self.shape.iter().product()
    }

    /// Whether the array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of dimensions.
    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// Total size in bytes.
    pub fn nbytes(&self) -> usize {
        self.len() * self.dtype.element_size()
    }
}

/// Compute C-contiguous strides for a given shape and element size.
fn c_strides(shape: &[usize], element_size: usize) -> Vec<isize> {
    let mut strides = vec![0isize; shape.len()];
    if !shape.is_empty() {
        strides[shape.len() - 1] = element_size as isize;
        for i in (0..shape.len() - 1).rev() {
            strides[i] = strides[i + 1] * shape[i + 1] as isize;
        }
    }
    strides
}

/// Arrow-based table (analogous to pandas.DataFrame).
///
/// Wraps one or more Arrow `RecordBatch`es with a shared schema.
#[derive(Debug, Clone)]
pub struct ArrowTable {
    pub batches: Vec<arrow::array::RecordBatch>,
    pub schema: arrow::datatypes::SchemaRef,
}

impl ArrowTable {
    pub fn new(batches: Vec<arrow::array::RecordBatch>, schema: arrow::datatypes::SchemaRef) -> Self {
        Self { batches, schema }
    }

    /// Total number of rows across all batches.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// Column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.schema.fields().iter().map(|f| f.name().as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_dtype_f64() {
        let dt = BuiltinDType::from_numpy_str("<f8").unwrap();
        assert_eq!(dt.endianness, Endianness::Little);
        assert_eq!(dt.kind, Kind::Float);
        assert_eq!(dt.itemsize, 8);
        assert_eq!(dt.to_numpy_str(), "<f8");
    }

    #[test]
    fn test_builtin_dtype_datetime() {
        let dt = BuiltinDType::from_numpy_str("<M8[ns]").unwrap();
        assert_eq!(dt.kind, Kind::Datetime);
        assert_eq!(dt.itemsize, 8);
        assert_eq!(dt.dt_units.as_deref(), Some("[ns]"));
        assert_eq!(dt.to_numpy_str(), "<M8[ns]");
    }

    #[test]
    fn test_builtin_dtype_unicode() {
        let dt = BuiltinDType::from_numpy_str("<U10").unwrap();
        assert_eq!(dt.kind, Kind::Unicode);
        assert_eq!(dt.itemsize, 40); // 10 chars * 4 bytes
        assert_eq!(dt.to_numpy_str(), "<U10");
    }

    #[test]
    fn test_builtin_dtype_bool() {
        let dt = BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1);
        assert_eq!(dt.to_numpy_str(), "|b1");
    }

    #[test]
    fn test_builtin_dtype_roundtrip_json() {
        let dt = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let json = serde_json::to_value(&dt).unwrap();
        let dt2 = BuiltinDType::from_json(&json).unwrap();
        assert_eq!(dt, dt2);
    }

    #[test]
    fn test_dtype_struct_from_json() {
        let json = serde_json::json!({
            "itemsize": 12,
            "fields": [
                {"name": "x", "dtype": {"endianness": "little", "kind": "f", "itemsize": 4}, "shape": null},
                {"name": "y", "dtype": {"endianness": "little", "kind": "f", "itemsize": 8}, "shape": null}
            ]
        });
        let sd = StructDType::from_json(&json).unwrap();
        assert_eq!(sd.itemsize, 12);
        assert_eq!(sd.fields.len(), 2);
        assert_eq!(sd.fields[0].name, "x");
    }

    #[test]
    fn test_dyn_ndarray() {
        let data = bytes::Bytes::from(vec![0u8; 800]);
        let dt = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let arr = DynNDArray::new(data, dt, vec![10, 10]);
        assert_eq!(arr.len(), 100);
        assert_eq!(arr.ndim(), 2);
        assert_eq!(arr.nbytes(), 800);
        assert_eq!(arr.strides, vec![80, 8]);
    }

    #[test]
    fn test_c_strides() {
        assert_eq!(c_strides(&[3, 4, 5], 8), vec![160, 40, 8]);
        assert_eq!(c_strides(&[10], 4), vec![4]);
        assert_eq!(c_strides(&[], 8), Vec::<isize>::new());
    }
}
