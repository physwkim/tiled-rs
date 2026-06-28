//! Shared HDF5 dataset-writing primitives.
//!
//! The array, table, and container HDF5 exporters all write the same two shapes
//! of dataset — a raw numeric byte buffer reinterpreted as a typed N-D array,
//! and an Arrow column flattened to a 1-D array — so the byte-swap/transmute and
//! the Arrow-dtype dispatch live here once. Every writer targets an [`H5Group`]
//! (a file's `root_group()` for a single leaf, a sub-group for a container
//! tree), so the same code serves both the flat serializers and the recursive
//! container builder.

#![cfg(feature = "hdf5")]

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use rust_hdf5::types::{HBool, VarLenUnicode};
use rust_hdf5::{H5Dataset, H5File, H5Group};
use serde_json::Value;

pub(crate) type DynError = Box<dyn std::error::Error + Send + Sync>;

/// A target that can carry HDF5 attributes: a file (root attrs), a group
/// (node attrs), or a dataset (leaf attrs). It unifies the two distinct
/// rust-hdf5 attribute APIs — `set_attr_string`/`set_attr_numeric` plus their
/// `_array` counterparts on files/groups, the `new_attr` builder on datasets —
/// so [`write_metadata_attrs`] serves all three with one mapping. The `put_*`
/// methods write scalar attributes; the `put_*_array` methods write 1-D array
/// attributes (Python's `attrs.update`/`create` storing a homogeneous list as a
/// numpy array).
trait AttrTarget {
    fn put_str(&self, name: &str, value: &str) -> Result<(), DynError>;
    fn put_bool(&self, name: &str, value: bool) -> Result<(), DynError>;
    fn put_i64(&self, name: &str, value: i64) -> Result<(), DynError>;
    fn put_f64(&self, name: &str, value: f64) -> Result<(), DynError>;
    fn put_str_array(&self, name: &str, values: &[&str]) -> Result<(), DynError>;
    fn put_bool_array(&self, name: &str, values: &[HBool]) -> Result<(), DynError>;
    fn put_i64_array(&self, name: &str, values: &[i64]) -> Result<(), DynError>;
    fn put_f64_array(&self, name: &str, values: &[f64]) -> Result<(), DynError>;
}

impl AttrTarget for H5File {
    fn put_str(&self, name: &str, value: &str) -> Result<(), DynError> {
        self.set_attr_string(name, value)?;
        Ok(())
    }
    fn put_bool(&self, name: &str, value: bool) -> Result<(), DynError> {
        self.set_attr_numeric(name, &HBool::from(value))?;
        Ok(())
    }
    fn put_i64(&self, name: &str, value: i64) -> Result<(), DynError> {
        self.set_attr_numeric(name, &value)?;
        Ok(())
    }
    fn put_f64(&self, name: &str, value: f64) -> Result<(), DynError> {
        self.set_attr_numeric(name, &value)?;
        Ok(())
    }
    fn put_str_array(&self, name: &str, values: &[&str]) -> Result<(), DynError> {
        self.set_attr_string_array(name, values)?;
        Ok(())
    }
    fn put_bool_array(&self, name: &str, values: &[HBool]) -> Result<(), DynError> {
        self.set_attr_array_numeric(name, values)?;
        Ok(())
    }
    fn put_i64_array(&self, name: &str, values: &[i64]) -> Result<(), DynError> {
        self.set_attr_array_numeric(name, values)?;
        Ok(())
    }
    fn put_f64_array(&self, name: &str, values: &[f64]) -> Result<(), DynError> {
        self.set_attr_array_numeric(name, values)?;
        Ok(())
    }
}

impl AttrTarget for H5Group {
    fn put_str(&self, name: &str, value: &str) -> Result<(), DynError> {
        self.set_attr_string(name, value)?;
        Ok(())
    }
    fn put_bool(&self, name: &str, value: bool) -> Result<(), DynError> {
        self.set_attr_numeric(name, &HBool::from(value))?;
        Ok(())
    }
    fn put_i64(&self, name: &str, value: i64) -> Result<(), DynError> {
        self.set_attr_numeric(name, &value)?;
        Ok(())
    }
    fn put_f64(&self, name: &str, value: f64) -> Result<(), DynError> {
        self.set_attr_numeric(name, &value)?;
        Ok(())
    }
    fn put_str_array(&self, name: &str, values: &[&str]) -> Result<(), DynError> {
        self.set_attr_string_array(name, values)?;
        Ok(())
    }
    fn put_bool_array(&self, name: &str, values: &[HBool]) -> Result<(), DynError> {
        self.set_attr_array_numeric(name, values)?;
        Ok(())
    }
    fn put_i64_array(&self, name: &str, values: &[i64]) -> Result<(), DynError> {
        self.set_attr_array_numeric(name, values)?;
        Ok(())
    }
    fn put_f64_array(&self, name: &str, values: &[f64]) -> Result<(), DynError> {
        self.set_attr_array_numeric(name, values)?;
        Ok(())
    }
}

impl AttrTarget for H5Dataset {
    fn put_str(&self, name: &str, value: &str) -> Result<(), DynError> {
        self.new_attr::<VarLenUnicode>()
            .shape(())
            .create(name)?
            .write_string(value)?;
        Ok(())
    }
    fn put_bool(&self, name: &str, value: bool) -> Result<(), DynError> {
        self.new_attr::<HBool>()
            .shape(())
            .create(name)?
            .write_numeric(&HBool::from(value))?;
        Ok(())
    }
    fn put_i64(&self, name: &str, value: i64) -> Result<(), DynError> {
        self.new_attr::<i64>()
            .shape(())
            .create(name)?
            .write_numeric(&value)?;
        Ok(())
    }
    fn put_f64(&self, name: &str, value: f64) -> Result<(), DynError> {
        self.new_attr::<f64>()
            .shape(())
            .create(name)?
            .write_numeric(&value)?;
        Ok(())
    }
    fn put_str_array(&self, name: &str, values: &[&str]) -> Result<(), DynError> {
        self.new_attr::<VarLenUnicode>()
            .shape([values.len()])
            .create(name)?
            .write_string_array(values)?;
        Ok(())
    }
    fn put_bool_array(&self, name: &str, values: &[HBool]) -> Result<(), DynError> {
        self.new_attr::<HBool>()
            .shape([values.len()])
            .create(name)?
            .write_array(values)?;
        Ok(())
    }
    fn put_i64_array(&self, name: &str, values: &[i64]) -> Result<(), DynError> {
        self.new_attr::<i64>()
            .shape([values.len()])
            .create(name)?
            .write_array(values)?;
        Ok(())
    }
    fn put_f64_array(&self, name: &str, values: &[f64]) -> Result<(), DynError> {
        self.new_attr::<f64>()
            .shape([values.len()])
            .create(name)?
            .write_array(values)?;
        Ok(())
    }
}

/// Write a JSON metadata object as HDF5 attributes on `target`.
///
/// Mirrors Python `serialize_hdf5`'s `file/group.attrs.update(metadata)` and
/// `dataset.attrs.create(k, v)`, where h5py runs each value through
/// `numpy.asarray`: the four scalar JSON kinds (string/bool/integer/float) map
/// to scalar attributes, and a homogeneous JSON array maps to a 1-D array
/// attribute (`[1,2,3]` → int array, any-float → float array, `[true,false]` →
/// bool array, `["a","b"]` → vlen-string array; an empty array is a float64
/// empty array, matching `numpy.asarray([])`). Anything h5py cannot store —
/// a nested object, a `null`, a `null`-bearing or mixed-kind array — is a hard
/// error that fails the whole export, the same fail-fast contract as Python's
/// `except TypeError: raise SerializationError`, surfaced here as an `Err`.
///
/// One divergence remains, dictated by the rust-hdf5 array-attr API being 1-D
/// only: a nested *numeric* array (`[[1,2],[3,4]]`), which Python would store as
/// a 2-D array, is rejected here rather than stored.
///
/// (A non-object `meta`, e.g. `Null`, writes nothing — there are no attrs to set.)
fn write_metadata_attrs<T: AttrTarget>(target: &T, meta: &Value) -> Result<(), DynError> {
    let Some(obj) = meta.as_object() else {
        return Ok(());
    };
    for (key, value) in obj {
        match value {
            Value::String(s) => target.put_str(key, s)?,
            Value::Bool(b) => target.put_bool(key, *b)?,
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    target.put_i64(key, i)?;
                } else if let Some(f) = n.as_f64() {
                    target.put_f64(key, f)?;
                } else {
                    return Err(format!(
                        "metadata attribute '{key}': numeric value is not representable \
                         as i64 or f64"
                    )
                    .into());
                }
            }
            Value::Array(arr) => write_array_attr(target, key, arr)?,
            Value::Object(_) | Value::Null => {
                return Err(format!(
                    "metadata attribute '{key}' has a value (nested object / null) that \
                     HDF5 cannot store as an attribute; export fails (Python's h5py raises \
                     the same as SerializationError)"
                )
                .into());
            }
        }
    }
    Ok(())
}

/// Write a homogeneous JSON array as a 1-D HDF5 array attribute on `target`.
///
/// Mirrors h5py running the list through `numpy.asarray`: the element kind picks
/// the on-disk type. An empty list is a float64 empty array (numpy's default
/// dtype for `[]`). A mixed-kind list, a `null`-bearing list, or a nested array
/// has no homogeneous numpy dtype h5py can store as a 1-D attribute and is a hard
/// error (Python's `TypeError → SerializationError`).
fn write_array_attr<T: AttrTarget>(target: &T, key: &str, arr: &[Value]) -> Result<(), DynError> {
    if arr.is_empty() {
        // numpy.asarray([]) is a float64 array of shape (0,).
        return target.put_f64_array(key, &[]);
    }
    if arr.iter().all(Value::is_boolean) {
        let values: Vec<HBool> = arr
            .iter()
            .map(|v| HBool::from(v.as_bool().expect("checked is_boolean")))
            .collect();
        return target.put_bool_array(key, &values);
    }
    if arr.iter().all(Value::is_string) {
        let values: Vec<&str> = arr
            .iter()
            .map(|v| v.as_str().expect("checked is_string"))
            .collect();
        return target.put_str_array(key, &values);
    }
    if arr.iter().all(Value::is_number) {
        // int array if every element fits i64, else float array — numpy's
        // promotion rule (a single non-integer value floats the whole array),
        // matching the scalar Number arm's i64-then-f64 fallback.
        if let Some(ints) = arr.iter().map(Value::as_i64).collect::<Option<Vec<i64>>>() {
            return target.put_i64_array(key, &ints);
        }
        if let Some(floats) = arr.iter().map(Value::as_f64).collect::<Option<Vec<f64>>>() {
            return target.put_f64_array(key, &floats);
        }
        return Err(format!(
            "metadata attribute '{key}': numeric array has a value not representable \
             as i64 or f64"
        )
        .into());
    }
    Err(format!(
        "metadata attribute '{key}' is a mixed-kind, null-bearing, or nested array that \
         HDF5 cannot store as a 1-D attribute; export fails (Python's h5py raises the same \
         as SerializationError)"
    )
    .into())
}

/// Write a metadata object as attributes on a file (HDF5 root attrs).
pub(crate) fn write_file_attrs(file: &H5File, meta: &Value) -> Result<(), DynError> {
    write_metadata_attrs(file, meta)
}

/// Write a metadata object as attributes on a group (node attrs).
pub(crate) fn write_group_attrs(group: &H5Group, meta: &Value) -> Result<(), DynError> {
    write_metadata_attrs(group, meta)
}

/// Write a raw numeric byte buffer as a dataset `name` (shape `shape`) under
/// `group`. `kind`/`itemsize` are the numpy dtype kind char (`f`/`i`/`u`) and
/// element size; `big_endian` byte-swaps each element to native before storing
/// (matching the CSV/array serializers' `>`-buffer handling). `attrs` is the
/// array node's metadata, written as dataset attributes (Python
/// `dataset.attrs.create`); pass `&Value::Null` for a leaf with no metadata.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_array_dataset(
    group: &H5Group,
    name: &str,
    data: &[u8],
    kind: char,
    itemsize: usize,
    big_endian: bool,
    shape: &[usize],
    attrs: &Value,
) -> Result<(), DynError> {
    match (kind, itemsize) {
        ('f', 8) => write_typed_into::<f64>(group, name, data, shape, 8, big_endian, attrs),
        ('f', 4) => write_typed_into::<f32>(group, name, data, shape, 4, big_endian, attrs),
        ('i', 8) => write_typed_into::<i64>(group, name, data, shape, 8, big_endian, attrs),
        ('i', 4) => write_typed_into::<i32>(group, name, data, shape, 4, big_endian, attrs),
        ('i', 2) => write_typed_into::<i16>(group, name, data, shape, 2, big_endian, attrs),
        ('i', 1) => write_typed_into::<i8>(group, name, data, shape, 1, big_endian, attrs),
        ('u', 8) => write_typed_into::<u64>(group, name, data, shape, 8, big_endian, attrs),
        ('u', 4) => write_typed_into::<u32>(group, name, data, shape, 4, big_endian, attrs),
        ('u', 2) => write_typed_into::<u16>(group, name, data, shape, 2, big_endian, attrs),
        ('u', 1) => write_typed_into::<u8>(group, name, data, shape, 1, big_endian, attrs),
        other => Err(format!("unsupported dtype kind/itemsize: {other:?}").into()),
    }
}

fn write_typed_into<T>(
    group: &H5Group,
    name: &str,
    data: &[u8],
    shape: &[usize],
    itemsize: usize,
    big_endian: bool,
    attrs: &Value,
) -> Result<(), DynError>
where
    T: rust_hdf5::types::H5Type + Copy,
{
    if !data.len().is_multiple_of(itemsize) {
        return Err(format!(
            "byte buffer length {} not aligned to itemsize {itemsize}",
            data.len()
        )
        .into());
    }
    let n = data.len() / itemsize;
    // Reinterpret the byte slice as &[T]. Caller has validated itemsize matches
    // T. The host is little-endian (x86/arm); a big-endian source buffer is
    // byte-swapped per element to native LE first, so the value stored in `T` is
    // correct and the native HDF5 datatype matches it. Cast goes through a Vec to
    // satisfy alignment.
    let mut typed: Vec<T> = Vec::with_capacity(n);
    let chunk = itemsize;
    let mut buf = vec![0u8; chunk];
    for i in 0..n {
        buf.copy_from_slice(&data[i * chunk..(i + 1) * chunk]);
        if big_endian {
            buf.reverse();
        }
        // SAFETY: we write `itemsize` bytes (now in native byte order) that match
        // T's layout.
        unsafe {
            let mut value = std::mem::MaybeUninit::<T>::uninit();
            std::ptr::copy_nonoverlapping(buf.as_ptr(), value.as_mut_ptr() as *mut u8, chunk);
            typed.push(value.assume_init());
        }
    }
    let dataset = group.new_dataset::<T>().shape(shape).create(name)?;
    dataset.write_raw(&typed)?;
    write_metadata_attrs(&dataset, attrs)?;
    Ok(())
}

/// Write one Arrow column as a 1-D HDF5 dataset named `name` under `group`.
///
/// HDF5 has no native null: float nulls become NaN (matching the JSON
/// serializer's NaN↔null rule and h5py's float behavior), and integer/boolean
/// nulls carry their underlying buffer value (pandas promotes nullable integer
/// columns to float, so a genuinely null-bearing integer column does not reach
/// the integer arms). Booleans store as u8 (0/1). String/temporal columns are
/// unsupported — rust-hdf5 0.2.20 has no string dataset type (its VarLenUnicode
/// works only for attributes), so they are a hard error naming the column.
pub(crate) fn write_table_column(
    group: &H5Group,
    name: &str,
    array: &dyn Array,
) -> Result<(), DynError> {
    let n = array.len();

    // Integer/unsigned: copy the underlying values buffer verbatim.
    macro_rules! write_copy {
        ($arr_ty:ty, $native:ty) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| -> DynError {
                    format!("downcast to {} failed", stringify!($arr_ty)).into()
                })?;
            let values: Vec<$native> = a.values().to_vec();
            group
                .new_dataset::<$native>()
                .shape([n])
                .create(name)?
                .write_raw(&values)?;
        }};
    }

    // Float: map each slot, turning Arrow nulls into NaN.
    macro_rules! write_float {
        ($arr_ty:ty, $native:ty) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| -> DynError {
                    format!("downcast to {} failed", stringify!($arr_ty)).into()
                })?;
            let values: Vec<$native> = (0..n)
                .map(|i| {
                    if a.is_null(i) {
                        <$native>::NAN
                    } else {
                        a.value(i)
                    }
                })
                .collect();
            group
                .new_dataset::<$native>()
                .shape([n])
                .create(name)?
                .write_raw(&values)?;
        }};
    }

    match array.data_type() {
        DataType::Int8 => write_copy!(Int8Array, i8),
        DataType::Int16 => write_copy!(Int16Array, i16),
        DataType::Int32 => write_copy!(Int32Array, i32),
        DataType::Int64 => write_copy!(Int64Array, i64),
        DataType::UInt8 => write_copy!(UInt8Array, u8),
        DataType::UInt16 => write_copy!(UInt16Array, u16),
        DataType::UInt32 => write_copy!(UInt32Array, u32),
        DataType::UInt64 => write_copy!(UInt64Array, u64),
        DataType::Float32 => write_float!(Float32Array, f32),
        DataType::Float64 => write_float!(Float64Array, f64),
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| -> DynError { "downcast to BooleanArray failed".into() })?;
            // No native bool dataset path; store as u8 (0/1), null → 0.
            let values: Vec<u8> = (0..n)
                .map(|i| u8::from(!a.is_null(i) && a.value(i)))
                .collect();
            group
                .new_dataset::<u8>()
                .shape([n])
                .create(name)?
                .write_raw(&values)?;
        }
        other => {
            return Err(format!(
                "hdf5 does not support column '{name}' of type {other:?} \
                 (rust-hdf5 has no string/temporal dataset support)"
            )
            .into());
        }
    }
    Ok(())
}
