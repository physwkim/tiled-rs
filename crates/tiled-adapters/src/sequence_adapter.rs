//! File-sequence adapter — stacks N single-file Array adapters along
//! axis 0.
//!
//! Use case: a Bluesky `NPY_SEQ` resource that points at `frame_000.npy`,
//! `frame_001.npy`, … — each file is a 2-D image, the sequence as a whole
//! is a 3-D array. Inner-shape consistency is checked at construction
//! time.
//!
//! The sequence loads frames lazily on `read_block`; `read` (full)
//! concatenates all of them into one DynNDArray. Random sub-sequence
//! `slice` extraction is the caller's responsibility — same contract the
//! per-file adapters keep.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

/// Strategy for opening one file in the sequence. Returns the adapter
/// for that single frame.
pub trait FrameOpener: Send + Sync {
    fn open(
        &self,
        path: PathBuf,
        index: usize,
    ) -> Result<Box<dyn ArrayAdapterRead>>;
}

/// Stacks `paths.len()` frames along a new leading axis.
pub struct SequenceAdapter {
    paths: Vec<PathBuf>,
    opener: Arc<dyn FrameOpener>,
    inner_shape: Vec<usize>,
    dtype: BuiltinDType,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl SequenceAdapter {
    /// `paths` are the per-frame files in order. The first path is opened
    /// eagerly to learn the per-frame shape and dtype; remaining frames
    /// must agree.
    pub fn from_paths(
        paths: Vec<PathBuf>,
        opener: Arc<dyn FrameOpener>,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(TiledError::Validation(
                "sequence requires at least one path".into(),
            ));
        }
        let first = opener.open(paths[0].clone(), 0)?;
        let first_struct = first.structure();
        let inner_shape = first_struct.shape.clone();
        let dtype = match &first_struct.data_type {
            DType::Builtin(b) => b.clone(),
            other => {
                return Err(TiledError::Validation(format!(
                    "sequence adapter only supports builtin dtypes, got {other:?}"
                )));
            }
        };
        let mut full_shape = vec![paths.len()];
        full_shape.extend_from_slice(&inner_shape);
        // Each frame is one chunk along axis 0; inner axes are single
        // chunks (matches the per-file adapter convention).
        let mut chunks: Vec<Vec<usize>> = vec![vec![1; paths.len()]];
        for &d in &inner_shape {
            chunks.push(vec![d]);
        }
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: full_shape,
            dims: None,
            resizable: Default::default(),
        };
        Ok(Self {
            paths,
            opener,
            inner_shape,
            dtype,
            structure,
            metadata,
            specs: vec![Spec::new("file_sequence")],
        })
    }

    fn frame_size_bytes(&self) -> usize {
        self.inner_shape.iter().product::<usize>() * self.dtype.element_size()
    }
}

impl BaseAdapter for SequenceAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Array
    }
    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }
    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ArrayAdapterRead for SequenceAdapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }

    fn read<'a>(&'a self, _slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            let frame_bytes = self.frame_size_bytes();
            let mut buf = BytesMut::with_capacity(frame_bytes * self.paths.len());
            for (i, path) in self.paths.iter().enumerate() {
                let frame = self.opener.open(path.clone(), i)?;
                let dyn_arr = frame.read(&NDSlice::empty()).await?;
                if dyn_arr.shape != self.inner_shape {
                    return Err(TiledError::Validation(format!(
                        "frame {i} has shape {:?}, expected {:?}",
                        dyn_arr.shape, self.inner_shape
                    )));
                }
                buf.extend_from_slice(&dyn_arr.data);
            }
            Ok(DynNDArray::new(
                buf.freeze(),
                self.dtype.clone(),
                self.structure.shape.clone(),
            ))
        })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        _slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            if block.len() != self.structure.shape.len() {
                return Err(TiledError::Validation(format!(
                    "expected {} block indices, got {}",
                    self.structure.shape.len(),
                    block.len()
                )));
            }
            let frame_idx = block[0];
            if frame_idx >= self.paths.len() {
                return Err(TiledError::Validation(format!(
                    "frame {frame_idx} out of range ({} frames)",
                    self.paths.len()
                )));
            }
            for (axis, &b) in block.iter().enumerate().skip(1) {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "axis {axis} is single-chunk; block index must be 0"
                    )));
                }
            }
            let frame = self
                .opener
                .open(self.paths[frame_idx].clone(), frame_idx)?;
            let dyn_arr = frame.read(&NDSlice::empty()).await?;
            if dyn_arr.shape != self.inner_shape {
                return Err(TiledError::Validation(format!(
                    "frame {frame_idx} has shape {:?}, expected {:?}",
                    dyn_arr.shape, self.inner_shape
                )));
            }
            // The block is a single frame so its shape is `1 × inner_shape`.
            let mut block_shape = vec![1usize];
            block_shape.extend_from_slice(&self.inner_shape);
            Ok(DynNDArray::new(
                dyn_arr.data,
                self.dtype.clone(),
                block_shape,
            ))
        })
    }
}

/// Helper opener for `.npy` files — what NPY_SEQ uses.
pub struct NpyFrameOpener;

impl FrameOpener for NpyFrameOpener {
    fn open(
        &self,
        path: PathBuf,
        _index: usize,
    ) -> Result<Box<dyn ArrayAdapterRead>> {
        Ok(Box::new(crate::NpyAdapter::from_path(
            path,
            serde_json::json!({}),
        )?))
    }
}

#[cfg(feature = "tiff")]
pub struct TiffFrameOpener;

#[cfg(feature = "tiff")]
impl FrameOpener for TiffFrameOpener {
    fn open(
        &self,
        path: PathBuf,
        _index: usize,
    ) -> Result<Box<dyn ArrayAdapterRead>> {
        Ok(Box::new(crate::TiffAdapter::from_path(
            path,
            serde_json::json!({}),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_simple_npy(path: &std::path::Path, value: f64, w: usize, h: usize) {
        let header = format!(
            "{{'descr': '<f8', 'fortran_order': False, 'shape': ({h}, {w}), }}"
        );
        let mut header = header.into_bytes();
        // Pad to 64-byte alignment of (10 + len) per spec.
        while (10 + header.len()) % 64 != 63 {
            header.push(b' ');
        }
        header.push(b'\n');
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(b"\x93NUMPY").unwrap();
        f.write_all(&[1, 0]).unwrap();
        f.write_all(&(header.len() as u16).to_le_bytes()).unwrap();
        f.write_all(&header).unwrap();
        for _ in 0..(w * h) {
            f.write_all(&value.to_le_bytes()).unwrap();
        }
    }

    #[tokio::test]
    async fn stacks_two_npy_frames() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.npy");
        let p2 = dir.path().join("b.npy");
        write_simple_npy(&p1, 1.0, 4, 3);
        write_simple_npy(&p2, 2.0, 4, 3);

        let seq = SequenceAdapter::from_paths(
            vec![p1, p2],
            Arc::new(NpyFrameOpener),
            serde_json::json!({}),
        )
        .unwrap();
        let s = seq.structure();
        assert_eq!(s.shape, vec![2, 3, 4]);
        assert_eq!(s.chunks[0], vec![1, 1]);

        let block0 = seq.read_block(&[0, 0, 0], &NDSlice::empty()).await.unwrap();
        assert_eq!(block0.shape, vec![1, 3, 4]);
        let f = f64::from_le_bytes(block0.data[0..8].try_into().unwrap());
        assert_eq!(f, 1.0);

        let block1 = seq.read_block(&[1, 0, 0], &NDSlice::empty()).await.unwrap();
        let f = f64::from_le_bytes(block1.data[0..8].try_into().unwrap());
        assert_eq!(f, 2.0);

        let full = seq.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(full.shape, vec![2, 3, 4]);
        assert_eq!(full.data.len(), 2 * 3 * 4 * 8);
    }
}
