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
    Box::new(
        |data, meta| -> Result<Bytes, crate::registry::SerializeError> {
            let itemsize = meta.get("itemsize").and_then(|v| v.as_u64()).unwrap_or(8) as usize;
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
            // The array adapter may emit big-endian (`>`) buffers (e.g. zarr/numpy
            // `>f8` dtypes). The host is little-endian (x86/arm), so a BE buffer
            // must be byte-swapped to native before reinterpreting as `T` — same
            // rule as the CSV serializer (array.rs `if big_endian { b.reverse() }`).
            let big_endian = meta
                .get("byteorder")
                .and_then(|v| v.as_str())
                .unwrap_or("<")
                == ">";

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

            let file =
                rust_hdf5::H5File::create(&path_str).map_err(|e| format!("hdf5 create: {e}"))?;
            crate::hdf5_common::write_array_dataset(
                &file.root_group(),
                "data",
                data,
                kind.chars().next().unwrap_or('f'),
                itemsize,
                big_endian,
                &shape,
            )
            .map_err(|e| format!("hdf5 write: {e}"))?;
            // Close the file so all buffered bytes hit the path before we read
            // them back.
            drop(file);

            let mut buf = Vec::new();
            std::fs::File::open(&path_str)
                .map_err(|e| format!("read back: {e}"))?
                .read_to_end(&mut buf)
                .map_err(|e| format!("read back: {e}"))?;
            Ok(Bytes::from(buf))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write the serializer's `.h5` blob to disk and read `/data` back.
    fn roundtrip_f64(bytes: &[u8], byteorder: &str, n: usize) -> Vec<f64> {
        let reg = SerializationRegistry::new();
        register_hdf5_serializer(&reg);
        let ser = reg.dispatch(StructureFamily::Array, mime::HDF5).unwrap();
        let meta = serde_json::json!({
            "itemsize": 8, "kind": "f", "byteorder": byteorder, "shape": [n],
        });
        let h5 = ser(bytes, &meta).expect("serialize");
        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &h5).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();
        file.dataset("data")
            .unwrap()
            .read_slice::<f64>(&[0], &[n])
            .unwrap()
            .to_vec()
    }

    /// Finding 2: a big-endian (`>`) source array must store its TRUE values,
    /// not byte-swapped. Before the fix, `write_typed` copied raw BE bytes into
    /// native (LE) `f64` and wrote a native datatype → double corruption.
    #[test]
    fn hdf5_serializer_honors_big_endian_byteorder() {
        let values = [1.5f64, -2.0, 3.25];
        let be: Vec<u8> = values.iter().flat_map(|v| v.to_be_bytes()).collect();
        let got = roundtrip_f64(&be, ">", values.len());
        assert_eq!(
            got,
            values.to_vec(),
            "big-endian source must store true values, not byte-swapped"
        );
    }

    /// LE and BE encodings of the same logical array read back identically.
    #[test]
    fn hdf5_serializer_le_and_be_agree() {
        let values = [10.0f64, -20.5, 30.0, 40.25];
        let le: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let be: Vec<u8> = values.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(roundtrip_f64(&le, "<", values.len()), values.to_vec());
        assert_eq!(roundtrip_f64(&be, ">", values.len()), values.to_vec());
    }
}
