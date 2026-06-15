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
    ///
    /// Panics on shape mismatch, since this is a constructor convenience —
    /// for non-trusted input use [`ArrayAdapter::from_f64_2d_checked`].
    pub fn from_f64_2d(
        data: &[f64],
        rows: usize,
        cols: usize,
        metadata: serde_json::Value,
    ) -> Self {
        Self::from_f64_2d_checked(data, rows, cols, metadata).expect("rows*cols == data.len()")
    }

    /// Like [`from_f64_2d`] but returns `Err` on shape mismatch instead of
    /// panicking.
    pub fn from_f64_2d_checked(
        data: &[f64],
        rows: usize,
        cols: usize,
        metadata: serde_json::Value,
    ) -> std::result::Result<Self, TiledError> {
        let need = rows
            .checked_mul(cols)
            .ok_or_else(|| TiledError::Validation("rows * cols overflowed usize".into()))?;
        if data.len() != need {
            return Err(TiledError::Validation(format!(
                "data length {} != rows({rows}) * cols({cols}) = {need}",
                data.len()
            )));
        }
        let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let dtype = BuiltinDType::new(
            tiled_core::dtype::Endianness::Little,
            tiled_core::dtype::Kind::Float,
            8,
        );
        Ok(Self::from_array(
            Bytes::from(bytes),
            dtype,
            vec![rows, cols],
            vec![vec![rows], vec![cols]],
            metadata,
            vec![],
        ))
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

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { self.array.apply_slice(slice) })
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
        let ndim = self.structure.shape.len();
        if block.len() != ndim {
            return Err(TiledError::Validation(format!(
                "Expected {ndim} block indices, got {}",
                block.len()
            )));
        }
        if self.structure.chunks.len() != ndim {
            return Err(TiledError::Validation(format!(
                "Malformed structure: chunks has {} dims, shape has {ndim}",
                self.structure.chunks.len()
            )));
        }

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

        // General N-D extraction from a C-contiguous source array.
        let total_elements: usize = block_shape.iter().product();
        let mut out = Vec::with_capacity(total_elements * element_size);

        // C-order strides over the parent shape (in elements).
        let parent_shape = &self.structure.shape;
        let mut parent_strides = vec![1usize; ndim];
        for i in (0..ndim - 1).rev() {
            parent_strides[i] = parent_strides[i + 1] * parent_shape[i + 1];
        }

        // Iterate every contiguous row of the block (last axis is contiguous).
        // Outer indices range over block_shape[..ndim-1]; for each tuple we
        // copy `block_shape[ndim-1]` elements from the source.
        let outer_dims = ndim - 1;
        let last_count = block_shape[outer_dims];
        let last_byte_run = last_count * element_size;

        // Mixed-radix index over outer dims.
        let mut idx = vec![0usize; outer_dims];
        loop {
            // Compute the source linear element offset for (start + idx, start[last]).
            let mut elem_offset = 0usize;
            for d in 0..outer_dims {
                elem_offset += (start[d] + idx[d]) * parent_strides[d];
            }
            elem_offset += start[outer_dims] * parent_strides[outer_dims];
            let byte_offset = elem_offset * element_size;
            let end_byte = byte_offset + last_byte_run;
            if end_byte > self.array.data.len() {
                return Err(TiledError::Validation(format!(
                    "block extraction reads past array end (offset {end_byte}, data {} bytes)",
                    self.array.data.len()
                )));
            }
            out.extend_from_slice(&self.array.data[byte_offset..end_byte]);

            // Increment outer index, last-axis-most-significant style.
            let mut d = outer_dims;
            loop {
                if d == 0 {
                    return Ok(DynNDArray::new(
                        Bytes::from(out),
                        self.array.dtype.clone(),
                        block_shape,
                    ));
                }
                d -= 1;
                idx[d] += 1;
                if idx[d] < block_shape[d] {
                    break;
                }
                idx[d] = 0;
            }
        }
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

    #[tokio::test]
    async fn test_read_block_wrong_ndim() {
        let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let adapter = ArrayAdapter::from_f64_1d(&data, serde_json::json!({}));
        let slice = NDSlice::empty();

        // 1D array but 2 block indices → error
        let err = adapter.read_block(&[0, 0], &slice).await;
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("Expected 1 block indices")
        );
    }

    #[tokio::test]
    async fn test_read_block_out_of_range() {
        let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let adapter = ArrayAdapter::from_f64_1d(&data, serde_json::json!({}));
        let slice = NDSlice::empty();

        // Block index 1 but only 1 chunk → out of range
        let err = adapter.read_block(&[1], &slice).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("out of range"));
    }

    #[tokio::test]
    async fn test_empty_array() {
        let adapter = ArrayAdapter::from_f64_1d(&[], serde_json::json!({}));
        assert_eq!(adapter.structure().shape, vec![0]);

        let slice = NDSlice::empty();
        let result = adapter.read(&slice).await.unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(result.nbytes(), 0);
    }

    #[tokio::test]
    async fn test_read_with_slice_returns_subarray() {
        // 4x5 array, values 0..20. arr[1:3, 1:3] → rows 1-2, cols 1-2.
        let data: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let adapter = ArrayAdapter::from_f64_2d(&data, 4, 5, serde_json::json!({}));

        let slice = NDSlice::from_numpy_str("1:3,1:3").unwrap();
        let result = adapter.read(&slice).await.unwrap();
        assert_eq!(result.shape, vec![2, 2]);
        let floats: Vec<f64> = result
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(floats, vec![6.0, 7.0, 11.0, 12.0]);
    }

    #[tokio::test]
    async fn test_2d_block_data_correctness() {
        // 4x5 array, single chunk — block [0,0] should return all data
        let data: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let adapter = ArrayAdapter::from_f64_2d(&data, 4, 5, serde_json::json!({}));
        let slice = NDSlice::empty();

        let block = adapter.read_block(&[0, 0], &slice).await.unwrap();
        assert_eq!(block.shape, vec![4, 5]);

        // Verify first and last values
        let first = f64::from_le_bytes(block.data[0..8].try_into().unwrap());
        let last = f64::from_le_bytes(block.data[152..160].try_into().unwrap());
        assert_eq!(first, 0.0);
        assert_eq!(last, 19.0);
    }
}
