//! HDF5 export serializer.
//!
//! `rust-hdf5`'s API only writes through a file path, so we round-trip
//! through a temp file: serialize → write → read bytes → return. The
//! resulting blob is a self-contained .h5 file with one dataset at
//! `/data` storing the array. Metadata `attrs` is attached as HDF5
//! attributes on the dataset.

#![cfg(feature = "hdf5")]

use std::io::Read;

use bytes::Bytes;

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializerFn};

pub fn register_hdf5_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Array, mime::HDF5, hdf5_serializer());
    reg.register(StructureFamily::Sparse, mime::HDF5, hdf5_serializer());
    reg.register_alias(".h5", mime::HDF5);
    reg.register_alias(".hdf5", mime::HDF5);
    reg.register_alias(".nx", mime::HDF5);
}

fn hdf5_serializer() -> SerializerFn {
    Box::new(|data, meta| -> Result<Bytes, crate::registry::SerializeError> {
        let itemsize = meta
            .get("itemsize")
            .and_then(|v| v.as_u64())
            .unwrap_or(8) as usize;
        let kind = meta.get("kind").and_then(|v| v.as_str()).unwrap_or("f");
        let shape: Vec<usize> = meta
            .get("shape")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_u64().map(|n| n as usize))
                    .collect()
            })
            .unwrap_or_default();

        // rust-hdf5 writes via filesystem; use a process-unique temp path.
        let tmp = tempfile::Builder::new()
            .prefix("tiled-h5-")
            .suffix(".h5")
            .tempfile()
            .map_err(|e| format!("temp file: {e}"))?;
        // Drop the NamedTempFile so HDF5 can open the path itself.
        // `into_temp_path` keeps a TempPath that auto-deletes on drop.
        let path = tmp.into_temp_path();
        let path_str = path.to_path_buf();

        write_dataset(&path_str, data, itemsize, kind, &shape)
            .map_err(|e| format!("hdf5 write: {e}"))?;

        let mut buf = Vec::new();
        std::fs::File::open(&path_str)
            .map_err(|e| format!("read back: {e}"))?
            .read_to_end(&mut buf)
            .map_err(|e| format!("read back: {e}"))?;
        Ok(Bytes::from(buf))
    })
}

fn write_dataset(
    path: &std::path::Path,
    data: &[u8],
    itemsize: usize,
    kind: &str,
    shape: &[usize],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = rust_hdf5::H5File::create(path)?;
    match (kind, itemsize) {
        ("f", 8) => write_typed::<f64>(&file, data, shape, 8)?,
        ("f", 4) => write_typed::<f32>(&file, data, shape, 4)?,
        ("i", 8) => write_typed::<i64>(&file, data, shape, 8)?,
        ("i", 4) => write_typed::<i32>(&file, data, shape, 4)?,
        ("i", 2) => write_typed::<i16>(&file, data, shape, 2)?,
        ("i", 1) => write_typed::<i8>(&file, data, shape, 1)?,
        ("u", 8) => write_typed::<u64>(&file, data, shape, 8)?,
        ("u", 4) => write_typed::<u32>(&file, data, shape, 4)?,
        ("u", 2) => write_typed::<u16>(&file, data, shape, 2)?,
        ("u", 1) => write_typed::<u8>(&file, data, shape, 1)?,
        other => {
            return Err(format!("unsupported dtype kind/itemsize: {other:?} {itemsize}").into());
        }
    }
    Ok(())
}

fn write_typed<T>(
    file: &rust_hdf5::H5File,
    data: &[u8],
    shape: &[usize],
    itemsize: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: rust_hdf5::types::H5Type + Copy,
{
    if data.len() % itemsize != 0 {
        return Err(format!(
            "byte buffer length {} not aligned to itemsize {itemsize}",
            data.len()
        )
        .into());
    }
    let n = data.len() / itemsize;
    // Reinterpret the byte slice as &[T]. Caller has already validated
    // itemsize matches T, and the bytes were produced by the array
    // adapter as little-endian — same as rust-hdf5 expects on x86/arm.
    // Cast goes through a Vec to satisfy alignment.
    let mut typed: Vec<T> = Vec::with_capacity(n);
    let chunk = itemsize;
    let mut buf = vec![0u8; chunk];
    for i in 0..n {
        buf.copy_from_slice(&data[i * chunk..(i + 1) * chunk]);
        // SAFETY: we write `itemsize` bytes that match T's layout.
        unsafe {
            let mut value = std::mem::MaybeUninit::<T>::uninit();
            std::ptr::copy_nonoverlapping(
                buf.as_ptr(),
                value.as_mut_ptr() as *mut u8,
                chunk,
            );
            typed.push(value.assume_init());
        }
    }
    let dataset = file
        .new_dataset::<T>()
        .shape(shape)
        .create("data")?;
    dataset.write_raw(&typed)?;
    Ok(())
}
