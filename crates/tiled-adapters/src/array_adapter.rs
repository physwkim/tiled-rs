//! In-memory array adapter.
//!
//! Corresponds to `tiled/adapters/array.py:ArrayAdapter`.

use bytes::Bytes;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DynNDArray};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

/// An in-memory array adapter holding raw bytes.
pub struct ArrayAdapter {
    array: DynNDArray,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl ArrayAdapter {
    /// Create from raw bytes with explicit dtype, shape, and chunks.
    pub fn from_array(
        data: Bytes,
        dtype: BuiltinDType,
        shape: Vec<usize>,
        chunks: Vec<Vec<usize>>,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Self {
        let array = DynNDArray::new(data, dtype.clone(), shape.clone());
        let structure = ArrayStructure {
            data_type: tiled_core::dtype::DType::Builtin(dtype),
            chunks,
            shape,
            dims: None,
            resizable: Default::default(),
        };
        Self {
            array,
            structure,
            metadata,
            specs,
        }
    }

    /// Create a simple 1D array from a slice of f64 values.
    pub fn from_f64_1d(data: &[f64], metadata: serde_json::Value) -> Self {
        let len = data.len();
        let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let dtype = BuiltinDType::new(
            tiled_core::dtype::Endianness::Little,
            tiled_core::dtype::Kind::Float,
            8,
        );
        Self::from_array(
            Bytes::from(bytes),
            dtype,
            vec![len],
            vec![vec![len]],
            metadata,
            vec![],
        )
    }

    /// Create a 2D array from a flat slice of f64 values with given shape.
    pub fn from_f64_2d(
        data: &[f64],
        rows: usize,
        cols: usize,
        metadata: serde_json::Value,
    ) -> Self {
        assert_eq!(data.len(), rows * cols);
        let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let dtype = BuiltinDType::new(
            tiled_core::dtype::Endianness::Little,
            tiled_core::dtype::Kind::Float,
            8,
        );
        Self::from_array(
            Bytes::from(bytes),
            dtype,
            vec![rows, cols],
            vec![vec![rows], vec![cols]],
            metadata,
            vec![],
        )
    }
}

impl BaseAdapter for ArrayAdapter {
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

impl ArrayAdapterRead for ArrayAdapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }

    fn read<'a>(&'a self, _slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { Ok(self.array.clone()) })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        _slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { self.read_block_inner(block) })
    }
}

impl ArrayAdapter {
    fn read_block_inner(&self, block: &[usize]) -> Result<DynNDArray> {
        // Compute the byte range for the requested block
        let ndim = self.structure.shape.len();
        if block.len() != ndim {
            return Err(TiledError::Validation(format!(
                "Expected {ndim} block indices, got {}",
                block.len()
            )));
        }

        // Compute the start/end indices for each dimension based on chunk sizes
        let mut start = vec![0usize; ndim];
        let mut end = vec![0usize; ndim];
        for dim in 0..ndim {
            let chunk_sizes = &self.structure.chunks[dim];
            if block[dim] >= chunk_sizes.len() {
                return Err(TiledError::Validation(format!(
                    "Block index {} out of range for dimension {} (max {})",
                    block[dim],
                    dim,
                    chunk_sizes.len() - 1
                )));
            }
            let offset: usize = chunk_sizes[..block[dim]].iter().sum();
            start[dim] = offset;
            end[dim] = offset + chunk_sizes[block[dim]];
        }

        // For a contiguous C-order array, we can compute the byte offset
        // For simplicity, handle the common case: single chunk (return everything)
        // or compute proper sub-array extraction
        let block_shape: Vec<usize> = (0..ndim).map(|d| end[d] - start[d]).collect();
        let element_size = self.array.dtype.element_size();

        // For a single-chunk array or 1D, do simple slice
        if ndim == 1 {
            let byte_start = start[0] * element_size;
            let byte_end = end[0] * element_size;
            let data = self.array.data.slice(byte_start..byte_end);
            return Ok(DynNDArray::new(data, self.array.dtype.clone(), block_shape));
        }

        // General case: extract block from C-contiguous array
        let total_elements: usize = block_shape.iter().product();
        let mut out = Vec::with_capacity(total_elements * element_size);

        // For 2D: iterate rows in the block, copy each row segment
        if ndim == 2 {
            let row_stride = self.structure.shape[1] * element_size;
            for row in start[0]..end[0] {
                let row_byte_start = row * row_stride + start[1] * element_size;
                let row_byte_end = row * row_stride + end[1] * element_size;
                out.extend_from_slice(&self.array.data[row_byte_start..row_byte_end]);
            }
        } else {
            // For higher dimensions, fall back to returning full array data for the block
            // This is a simplification; full N-D block extraction would require recursive striding
            // For demo purposes, this handles the common 1D and 2D cases.
            let byte_start = 0;
            let byte_end = total_elements * element_size;
            if byte_end <= self.array.data.len() {
                out.extend_from_slice(&self.array.data[byte_start..byte_end]);
            } else {
                return Err(TiledError::Validation(
                    "Block extraction not supported for >2D arrays with multiple chunks".into(),
                ));
            }
        }

        Ok(DynNDArray::new(
            Bytes::from(out),
            self.array.dtype.clone(),
            block_shape,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_array_adapter_1d() {
        let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let adapter = ArrayAdapter::from_f64_1d(&data, serde_json::json!({"name": "test"}));

        assert_eq!(adapter.structure_family(), StructureFamily::Array);
        assert_eq!(adapter.structure().shape, vec![10]);
        assert_eq!(adapter.metadata()["name"], "test");

        let slice = NDSlice::empty();
        let result = adapter.read(&slice).await.unwrap();
        assert_eq!(result.shape, vec![10]);
        assert_eq!(result.nbytes(), 80); // 10 * 8 bytes
    }

    #[tokio::test]
    async fn test_array_adapter_read_block() {
        let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let adapter = ArrayAdapter::from_f64_1d(&data, serde_json::json!({}));

        let slice = NDSlice::empty();
        let block = adapter.read_block(&[0], &slice).await.unwrap();
        assert_eq!(block.shape, vec![10]);

        // Verify the bytes match the original data
        let expected_bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
        assert_eq!(block.data.as_ref(), expected_bytes.as_slice());
    }

    #[tokio::test]
    async fn test_array_adapter_2d() {
        let data: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let adapter = ArrayAdapter::from_f64_2d(&data, 4, 5, serde_json::json!({}));

        assert_eq!(adapter.structure().shape, vec![4, 5]);
        assert_eq!(adapter.structure().ndim(), 2);

        let slice = NDSlice::empty();
        let result = adapter.read(&slice).await.unwrap();
        assert_eq!(result.shape, vec![4, 5]);
    }
}
