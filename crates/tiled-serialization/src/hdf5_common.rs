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
use rust_hdf5::H5Group;

pub(crate) type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Write a raw numeric byte buffer as a dataset `name` (shape `shape`) under
/// `group`. `kind`/`itemsize` are the numpy dtype kind char (`f`/`i`/`u`) and
/// element size; `big_endian` byte-swaps each element to native before storing
/// (matching the CSV/array serializers' `>`-buffer handling).
pub(crate) fn write_array_dataset(
    group: &H5Group,
    name: &str,
    data: &[u8],
    kind: char,
    itemsize: usize,
    big_endian: bool,
    shape: &[usize],
) -> Result<(), DynError> {
    match (kind, itemsize) {
        ('f', 8) => write_typed_into::<f64>(group, name, data, shape, 8, big_endian),
        ('f', 4) => write_typed_into::<f32>(group, name, data, shape, 4, big_endian),
        ('i', 8) => write_typed_into::<i64>(group, name, data, shape, 8, big_endian),
        ('i', 4) => write_typed_into::<i32>(group, name, data, shape, 4, big_endian),
        ('i', 2) => write_typed_into::<i16>(group, name, data, shape, 2, big_endian),
        ('i', 1) => write_typed_into::<i8>(group, name, data, shape, 1, big_endian),
        ('u', 8) => write_typed_into::<u64>(group, name, data, shape, 8, big_endian),
        ('u', 4) => write_typed_into::<u32>(group, name, data, shape, 4, big_endian),
        ('u', 2) => write_typed_into::<u16>(group, name, data, shape, 2, big_endian),
        ('u', 1) => write_typed_into::<u8>(group, name, data, shape, 1, big_endian),
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
