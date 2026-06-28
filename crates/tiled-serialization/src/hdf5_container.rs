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
//! Node metadata is written as HDF5 attributes, matching Python: the root
//! container's metadata → file (root group) attrs, each intermediate container's
//! metadata → group attrs, each array's metadata → dataset attrs, and a table
//! node's metadata → its group's attrs. Scalar JSON values map to scalar
//! attributes and a homogeneous JSON array maps to an array attribute — nested
//! rectangular arrays to N-D ones (Python's `attrs.update`/`create` running each
//! value through `numpy.asarray`); a value h5py cannot store — a nested object, a
//! null, a mixed-kind or `null`-bearing array, or a ragged (non-rectangular)
//! nested array — fails the whole export, the same fail-fast contract as Python's
//! `except TypeError: raise SerializationError`. See
//! [`crate::hdf5_common::write_file_attrs`].
//!
//! Remaining parity gaps (UNFIXED, library-bound): sparse/awkward leaves are
//! skipped — Python's `walk` has no defined dataset shape for them either — and
//! string/temporal columns are unsupported because rust-hdf5 0.2.20 has no string
//! *dataset* type (h5py does), so those columns hard-error.

#![cfg(feature = "hdf5")]

use std::io::Read;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde_json::Value;

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
    /// Metadata to write as group attributes, keyed by full group path ("a/b").
    /// Applied the moment a group is first created in [`ensure_group`], so the
    /// attrs land whichever leaf forces the group into existence. Registered up
    /// front via [`register_group_attrs`](Self::register_group_attrs).
    group_attrs: std::collections::HashMap<String, Value>,
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
            group_attrs: std::collections::HashMap::new(),
        })
    }

    /// Record the root container's metadata as file (root group) attributes —
    /// Python `file.attrs.update(metadata)`. Call once before adding leaves.
    pub fn set_root_attrs(&mut self, meta: &Value) -> Result<(), Hdf5BuildError> {
        crate::hdf5_common::write_file_attrs(&self.file, meta)
    }

    /// Register an intermediate container's metadata to be written as group
    /// attributes when that group is created. Must be called before the leaves
    /// that create the group are added (the whole point is to apply attrs at
    /// create time, since a reopened write-mode group handle cannot be trusted).
    pub fn register_group_attrs(&mut self, group_path: String, meta: Value) {
        if group_path.is_empty() {
            return;
        }
        self.group_attrs.insert(group_path, meta);
    }

    /// Resolve `group_path` ("a/b/c") to its `H5Group`, creating each level the
    /// first time it is seen and reopening a handle thereafter. On first creation
    /// of a level, any registered group attributes for that path are written.
    /// Empty path → the file root.
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
                let group = cur.create_group(comp)?;
                if let Some(meta) = self.group_attrs.get(&acc) {
                    crate::hdf5_common::write_group_attrs(&group, meta)?;
                }
                group
            };
        }
        Ok(cur)
    }

    /// Write a raw numeric array as dataset `name` under `group_path`, with the
    /// array node's `metadata` as dataset attributes.
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
        metadata: &Value,
    ) -> Result<(), Hdf5BuildError> {
        let group = self.ensure_group(group_path)?;
        crate::hdf5_common::write_array_dataset(
            &group, name, data, kind, itemsize, big_endian, shape, metadata,
        )
    }

    /// Write each column of `batch` as its own 1-D dataset under `group_path`
    /// (the column-per-dataset rule, named after the column). The table node's
    /// `metadata` is written as attributes on the table's group — Python copies
    /// it onto each per-column dataset, but with one group per table the group is
    /// the single natural carrier (and the columns stay attribute-free).
    pub fn add_table_columns(
        &mut self,
        group_path: &str,
        batch: &RecordBatch,
        metadata: &Value,
    ) -> Result<(), Hdf5BuildError> {
        let group = self.ensure_group(group_path)?;
        crate::hdf5_common::write_group_attrs(&group, metadata)?;
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
            .add_array("", "top", &top, 'f', 8, false, &[3], &Value::Null)
            .unwrap();

        // array `grp/inner` under a group
        let inner: Vec<u8> = [10.0f64, 20.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        builder
            .add_array("grp", "inner", &inner, 'f', 8, false, &[2], &Value::Null)
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
        builder
            .add_table_columns("grp/tbl", &batch, &Value::Null)
            .unwrap();

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

    /// Node metadata lands as HDF5 attributes: root metadata on the file, an
    /// intermediate container's metadata on its group, and an array's metadata on
    /// its dataset, with the four scalar JSON kinds mapping to scalar attributes.
    /// Group/file attrs are checked by name (rust-hdf5 reads only dataset attr
    /// *values*); dataset attrs are checked by value across string/int/float/bool.
    #[test]
    fn builder_writes_metadata_as_attrs() {
        let mut builder = Hdf5TreeBuilder::new().unwrap();
        builder
            .set_root_attrs(&serde_json::json!({"experiment": "alpha", "run": 42}))
            .unwrap();
        builder.register_group_attrs("grp".to_string(), serde_json::json!({"kind": "detector"}));

        let img: Vec<u8> = [1.0f64, 2.0].iter().flat_map(|v| v.to_le_bytes()).collect();
        builder
            .add_array(
                "grp",
                "img",
                &img,
                'f',
                8,
                false,
                &[2],
                &serde_json::json!({
                    "units": "counts", "gain": 2.5, "channels": 7, "ok": true,
                }),
            )
            .unwrap();

        let bytes = builder.finish().unwrap();
        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &bytes).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();

        // Root attrs on the file.
        let root_attrs = file.attr_names().unwrap();
        assert!(root_attrs.contains(&"experiment".to_string()));
        assert!(root_attrs.contains(&"run".to_string()));

        // Group attrs on the intermediate container's group.
        let grp_attrs = file
            .root_group()
            .group("grp")
            .unwrap()
            .attr_names()
            .unwrap();
        assert!(grp_attrs.contains(&"kind".to_string()));

        // Dataset attrs on the array leaf — values, one per scalar kind.
        let ds = file.dataset("grp/img").unwrap();
        assert_eq!(ds.attr("units").unwrap().read_string().unwrap(), "counts");
        assert_eq!(ds.attr("gain").unwrap().read_numeric::<f64>().unwrap(), 2.5);
        assert_eq!(
            ds.attr("channels").unwrap().read_numeric::<i64>().unwrap(),
            7
        );
        assert_eq!(
            ds.attr("ok")
                .unwrap()
                .read_numeric::<rust_hdf5::types::HBool>()
                .unwrap()
                .0,
            1
        );
    }

    /// Homogeneous array metadata lands as HDF5 array attributes (nested
    /// rectangular arrays as N-D) — Python's `attrs.update`/`create` running each
    /// list through `numpy.asarray`. File and group array attrs are checked by name
    /// (rust-hdf5 reads only dataset attr *values*); dataset numeric array attrs
    /// are checked by value via `read_raw` (row-major bytes, N-D flattened).
    #[test]
    fn builder_writes_array_metadata_as_attrs() {
        let mut builder = Hdf5TreeBuilder::new().unwrap();
        builder
            .set_root_attrs(&serde_json::json!({"ints": [1, 2, 3], "names": ["a", "b"]}))
            .unwrap();
        builder.register_group_attrs(
            "grp".to_string(),
            serde_json::json!({"flags": [true, false]}),
        );

        let img: Vec<u8> = [1.0f64, 2.0].iter().flat_map(|v| v.to_le_bytes()).collect();
        builder
            .add_array(
                "grp",
                "img",
                &img,
                'f',
                8,
                false,
                &[2],
                &serde_json::json!({
                    "dims": [4, 8, 12],
                    "scales": [0.5, 1.5],
                    "labels": ["x", "y"],
                    "empty": [],
                    "grid": [[1, 2], [3, 4]],
                }),
            )
            .unwrap();

        let bytes = builder.finish().unwrap();
        let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
        std::fs::write(tmp.path(), &bytes).unwrap();
        let file = rust_hdf5::H5File::open(tmp.path()).unwrap();

        // Root + group array attrs present by name.
        let root_attrs = file.attr_names().unwrap();
        assert!(root_attrs.contains(&"ints".to_string()));
        assert!(root_attrs.contains(&"names".to_string()));
        let grp_attrs = file
            .root_group()
            .group("grp")
            .unwrap()
            .attr_names()
            .unwrap();
        assert!(grp_attrs.contains(&"flags".to_string()));

        // Dataset numeric array attrs by value: decode the raw little-endian bytes.
        let ds = file.dataset("grp/img").unwrap();
        let dims: Vec<i64> = ds
            .attr("dims")
            .unwrap()
            .read_raw()
            .unwrap()
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(dims, vec![4, 8, 12]);
        let scales: Vec<f64> = ds
            .attr("scales")
            .unwrap()
            .read_raw()
            .unwrap()
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(scales, vec![0.5, 1.5]);

        // Nested 2-D int array: read_raw is row-major, so [[1,2],[3,4]] -> 1,2,3,4.
        let grid: Vec<i64> = ds
            .attr("grid")
            .unwrap()
            .read_raw()
            .unwrap()
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(grid, vec![1, 2, 3, 4]);

        // String array and empty array attrs exist on the dataset.
        assert!(ds.attr("labels").is_ok());
        assert!(ds.attr("empty").is_ok());
    }

    /// Python parity: metadata values h5py cannot store fail the whole export
    /// (h5py's `TypeError → SerializationError`). Rectangular homogeneous arrays
    /// (incl. nested) are storable; the unstorable cases are a nested object, a
    /// null, a mixed-kind array, a ragged nested array, and an array mixing arrays
    /// and scalars at one level.
    #[test]
    fn builder_rejects_unstorable_metadata() {
        // Nested object.
        let err = Hdf5TreeBuilder::new()
            .unwrap()
            .set_root_attrs(&serde_json::json!({"calibration": {"slope": 1.0}}))
            .expect_err("a nested-object metadata value must fail the export");
        assert!(err.to_string().contains("calibration"));

        // Null value.
        let err = Hdf5TreeBuilder::new()
            .unwrap()
            .set_root_attrs(&serde_json::json!({"missing": null}))
            .expect_err("a null metadata value must fail the export");
        assert!(err.to_string().contains("missing"));

        // Mixed-kind array.
        let err = Hdf5TreeBuilder::new()
            .unwrap()
            .set_root_attrs(&serde_json::json!({"mixed": [1, "a"]}))
            .expect_err("a mixed-kind array metadata value must fail the export");
        assert!(err.to_string().contains("mixed"));

        // Ragged nested array: numpy cannot build a rectangular array.
        let err = Hdf5TreeBuilder::new()
            .unwrap()
            .set_root_attrs(&serde_json::json!({"ragged": [[1, 2], [3]]}))
            .expect_err("a ragged nested array must fail the export");
        assert!(err.to_string().contains("ragged"));

        // Array mixing sub-arrays and scalars at one level.
        let err = Hdf5TreeBuilder::new()
            .unwrap()
            .set_root_attrs(&serde_json::json!({"jagged": [1, [2, 3]]}))
            .expect_err("an array mixing arrays and scalars must fail the export");
        assert!(err.to_string().contains("jagged"));
    }
}
