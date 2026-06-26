//! Container deep-export to a single HDF5 file.
//!
//! Mirrors Python `container.serialize_hdf5` (`tiled/serialization/container.py:46`):
//! walk the subtree, encode every intermediate node as a Group and every array
//! and table column as a Dataset. Python hands the whole DataFrame to
//! `create_dataset` once per column (ill-defined for mixed dtypes); tiled-rs
//! writes **each column as its own 1-D dataset** named after the column, the
//! same rule the array-leaf and single-table exporters use.
//!
//! Unlike the flat array/table serializers, this cannot be a [`SerializerFn`]:
//! the byte-in/byte-out signature has no access to the adapter tree, so the
//! actual walk + per-leaf read lives in the server router (`container_full`),
//! exactly as the `application/zip` deep-export does. This module provides
//! [`Hdf5TreeBuilder`], which the router drives with already-read leaves, and
//! registers the `(Container, application/x-hdf5)` media type so the format is
//! advertised by the About endpoint and resolvable via content negotiation.
//!
//! Parity gap (UNFIXED, deliberate): Python also copies each node's `metadata()`
//! into HDF5 group/dataset `attrs`. tiled-rs does not write metadata as HDF5
//! attributes here — same scope decision as the array/table exporters (a faithful
//! JSON→HDF5 attribute mapping is its own task). Only the group/dataset tree is
//! built. Sparse/awkward leaves are skipped (Python's `walk` also has no defined
//! dataset shape for them).

#![cfg(feature = "hdf5")]

use std::io::Read;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::SerializationRegistry;

/// Boxed error unifying HDF5, IO, and dtype failures from the builder. Boxed so
/// the public builder API does not leak rust-hdf5's error type.
pub type Hdf5BuildError = Box<dyn std::error::Error + Send + Sync>;

/// Accumulates array and table-column datasets into one HDF5 file, creating
/// intermediate groups on demand. Backed by a temp file (rust-hdf5 writes only
/// through a path); [`finish`](Self::finish) reads the bytes back.
pub struct Hdf5TreeBuilder {
    file: rust_hdf5::H5File,
    tmp: tempfile::TempPath,
    /// Group paths already created in this build. rust-hdf5's `group()` returns
    /// an `Ok` handle unconditionally in write mode (existence is only checked
    /// in read mode), so we cannot probe-then-create; we track what we made and
    /// `create_group` only the first time a path is seen.
    created: std::collections::HashSet<String>,
}

impl Hdf5TreeBuilder {
    pub fn new() -> Result<Self, Hdf5BuildError> {
        let tmp = tempfile::Builder::new()
            .prefix("tiled-h5c-")
            .suffix(".h5")
            .tempfile()?
            .into_temp_path();
        let file = rust_hdf5::H5File::create(&tmp)?;
        Ok(Self {
            file,
            tmp,
            created: std::collections::HashSet::new(),
        })
    }

    /// Resolve `group_path` ("a/b/c") to its `H5Group`, creating each level the
    /// first time it is seen and reopening a handle thereafter. Empty path → the
    /// file root.
    fn ensure_group(&mut self, group_path: &str) -> Result<rust_hdf5::H5Group, Hdf5BuildError> {
        let mut cur = self.file.root_group();
        let mut acc = String::new();
        for comp in group_path.split('/') {
            if comp.is_empty() {
                continue;
            }
            if !acc.is_empty() {
                acc.push('/');
            }
            acc.push_str(comp);
            cur = if self.created.contains(&acc) {
                // Already created earlier in this build — reopen a handle.
                cur.group(comp)?
            } else {
                self.created.insert(acc.clone());
                cur.create_group(comp)?
            };
        }
        Ok(cur)
    }

    /// Write a raw numeric array as dataset `name` under `group_path`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_array(
        &mut self,
        group_path: &str,
        name: &str,
        data: &[u8],
        kind: char,
        itemsize: usize,
        big_endian: bool,
        shape: &[usize],
    ) -> Result<(), Hdf5BuildError> {
        let group = self.ensure_group(group_path)?;
        crate::hdf5_common::write_array_dataset(
            &group, name, data, kind, itemsize, big_endian, shape,
        )
    }

    /// Write each column of `batch` as its own 1-D dataset under `group_path`
    /// (the column-per-dataset rule, named after the column).
    pub fn add_table_columns(
        &mut self,
        group_path: &str,
        batch: &RecordBatch,
    ) -> Result<(), Hdf5BuildError> {
        let group = self.ensure_group(group_path)?;
        for (i, field) in batch.schema().fields().iter().enumerate() {
            crate::hdf5_common::write_table_column(&group, field.name(), batch.column(i).as_ref())?;
        }
        Ok(())
    }

    /// Close the file and return the complete `.h5` bytes.
    pub fn finish(self) -> Result<Bytes, Hdf5BuildError> {
        // Drop the H5File so all buffered bytes are flushed/closed to the path.
        drop(self.file);
        let mut buf = Vec::new();
        std::fs::File::open(&self.tmp)?.read_to_end(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

/// Register `(Container, application/x-hdf5)` for content negotiation and About
/// advertising. The actual file is assembled in the router (`container_full`)
/// via [`Hdf5TreeBuilder`] — a byte serializer cannot reach the adapter tree —
/// so this fn is never invoked for a real export and errors if it ever is.
pub fn register_hdf5_container_serializer(reg: &SerializationRegistry) {
    reg.register(
        StructureFamily::Container,
        mime::HDF5,
        Box::new(|_data, _meta| {
            Err("container HDF5 export is assembled in the server router \
                 (container_full), not via a byte serializer"
                .to_string()
                .into())
        }),
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn container_hdf5_registered() {
        let reg = SerializationRegistry::new();
        register_hdf5_container_serializer(&reg);
        assert!(
            reg.dispatch(StructureFamily::Container, mime::HDF5)
                .is_some(),
            "container application/x-hdf5 must be registered for negotiation/About"
        );
    }

    /// A nested tree — a root array, a grouped array, and a table group — builds
    /// the expected group/dataset layout, with each table column its own dataset.
    #[test]
    fn builder_writes_nested_groups_and_datasets() {
        let mut builder = Hdf5TreeBuilder::new().unwrap();

        // root-level array `top` (shape [3], f64, little-endian)
        let top: Vec<u8> = [1.0f64, 2.0, 3.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        builder
            .add_array("", "top", &top, 'f', 8, false, &[3])
            .unwrap();

        // array `grp/inner` under a group
        let inner: Vec<u8> = [10.0f64, 20.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        builder
            .add_array("grp", "inner", &inner, 'f', 8, false, &[2])
            .unwrap();

        // table at group `grp/tbl` → one dataset per column
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7, 8, 9])),
                Arc::new(Float64Array::from(vec![0.5, 1.5, 2.5])),
            ],
        )
        .unwrap();
        builder.add_table_columns("grp/tbl", &batch).unwrap();

        let bytes = builder.finish().unwrap();
        assert_eq!(&bytes[..8], b"\x89HDF\r\n\x1a\n", "HDF5 magic signature");

        // Read the tree back and verify layout + values.
        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &bytes).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();

        let top_rd = file
            .dataset("top")
            .unwrap()
            .read_slice::<f64>(&[0], &[3])
            .unwrap();
        assert_eq!(top_rd.to_vec(), vec![1.0, 2.0, 3.0]);

        let inner_rd = file
            .dataset("grp/inner")
            .unwrap()
            .read_slice::<f64>(&[0], &[2])
            .unwrap();
        assert_eq!(inner_rd.to_vec(), vec![10.0, 20.0]);

        let x = file
            .dataset("grp/tbl/x")
            .unwrap()
            .read_slice::<i64>(&[0], &[3])
            .unwrap();
        assert_eq!(x.to_vec(), vec![7, 8, 9]);
        let y = file
            .dataset("grp/tbl/y")
            .unwrap()
            .read_slice::<f64>(&[0], &[3])
            .unwrap();
        assert_eq!(y.to_vec(), vec![0.5, 1.5, 2.5]);
    }
}
