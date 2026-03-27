//! File handlers for external data references.
//!
//! When Bluesky stores large data (Area Detector images, etc.), the event
//! document contains a `datum_id` instead of inline data. The datum points
//! to a resource (file path + spec), and the handler reads the actual bytes.
//!
//! Spec → Handler mapping:
//!   "AD_HDF5"     → HDF5 file (Area Detector)
//!   "NPY_SEQ"     → NumPy .npy sequence files
//!   "AD_TIFF"     → TIFF image files
//!   "npy"         → Single NumPy .npy file

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tiled_core::error::{Result, TiledError};

/// A file handler reads data from an external file given resource/datum kwargs.
pub trait FileHandler: Send + Sync {
    /// Read a single datum from the file.
    /// Returns raw bytes in C-contiguous order + shape.
    fn read(
        &self,
        datum_kwargs: &serde_json::Value,
    ) -> Result<(Vec<u8>, Vec<usize>)>;
}

/// Registry of spec → handler factory.
pub struct HandlerRegistry {
    factories: HashMap<String, Arc<dyn Fn(&str, &str, &serde_json::Value) -> Result<Box<dyn FileHandler>> + Send + Sync>>,
    root_map: HashMap<String, String>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            factories: HashMap::new(),
            root_map: HashMap::new(),
        };
        reg.register_builtins();
        reg
    }

    pub fn with_root_map(mut self, root_map: HashMap<String, String>) -> Self {
        self.root_map = root_map;
        self
    }

    fn register_builtins(&mut self) {
        // NPY_SEQ: numpy sequence files like {filename}_{index}.npy
        self.factories.insert(
            "NPY_SEQ".into(),
            Arc::new(|root, path, _kwargs| {
                Ok(Box::new(NpySeqHandler::new(root, path)) as Box<dyn FileHandler>)
            }),
        );

        // npy: single numpy file
        self.factories.insert(
            "npy".into(),
            Arc::new(|root, path, _kwargs| {
                Ok(Box::new(NpySingleHandler::new(root, path)) as Box<dyn FileHandler>)
            }),
        );

        // AD_HDF5: Area Detector HDF5 files
        #[cfg(feature = "hdf5")]
        self.factories.insert(
            "AD_HDF5".into(),
            Arc::new(|root, path, kwargs| {
                let frame_per_point = kwargs
                    .get("frame_per_point")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1) as usize;
                Ok(Box::new(Hdf5Handler::new(root, path, frame_per_point)) as Box<dyn FileHandler>)
            }),
        );

        // AD_TIFF: Area Detector TIFF files
        #[cfg(feature = "tiff")]
        self.factories.insert(
            "AD_TIFF".into(),
            Arc::new(|root, path, _kwargs| {
                Ok(Box::new(TiffHandler::new(root, path)) as Box<dyn FileHandler>)
            }),
        );
    }

    /// Create a handler for a given resource.
    pub fn create_handler(
        &self,
        spec: &str,
        root: &str,
        resource_path: &str,
        resource_kwargs: &serde_json::Value,
    ) -> Result<Box<dyn FileHandler>> {
        // Apply root_map if applicable.
        let resolved_root = self
            .root_map
            .get(root)
            .map(|s| s.as_str())
            .unwrap_or(root);

        let factory = self.factories.get(spec).ok_or_else(|| {
            TiledError::Validation(format!("No handler registered for spec: {spec}"))
        })?;

        factory(resolved_root, resource_path, resource_kwargs)
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// NPY handlers
// ---------------------------------------------------------------------------

/// Handles NPY_SEQ: files like `{base}_{index}.npy`
struct NpySeqHandler {
    base_path: PathBuf,
}

impl NpySeqHandler {
    fn new(root: &str, path: &str) -> Self {
        Self {
            base_path: Path::new(root).join(path),
        }
    }
}

impl FileHandler for NpySeqHandler {
    fn read(&self, datum_kwargs: &serde_json::Value) -> Result<(Vec<u8>, Vec<usize>)> {
        let index = datum_kwargs
            .get("index")
            .or_else(|| datum_kwargs.get("point_number"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let file_path = format!("{}_{index}.npy", self.base_path.display());
        read_npy_file(&file_path)
    }
}

/// Handles single .npy files.
struct NpySingleHandler {
    file_path: PathBuf,
}

impl NpySingleHandler {
    fn new(root: &str, path: &str) -> Self {
        Self {
            file_path: Path::new(root).join(path),
        }
    }
}

impl FileHandler for NpySingleHandler {
    fn read(&self, _datum_kwargs: &serde_json::Value) -> Result<(Vec<u8>, Vec<usize>)> {
        read_npy_file(&self.file_path.to_string_lossy())
    }
}

/// Read a .npy file and return (raw_bytes, shape).
fn read_npy_file(path: &str) -> Result<(Vec<u8>, Vec<usize>)> {
    let data = std::fs::read(path)
        .map_err(|e| TiledError::Internal(format!("Failed to read {path}: {e}")))?;

    // NPY format: 6-byte magic + 2-byte version + 2-byte header_len + header + data
    // Minimal parsing: skip the header, return raw data bytes.
    if data.len() < 10 || &data[..6] != b"\x93NUMPY" {
        return Err(TiledError::Validation(format!("Not a valid .npy file: {path}")));
    }

    let header_len = u16::from_le_bytes([data[8], data[9]]) as usize;
    let header_end = 10 + header_len;
    if data.len() < header_end {
        return Err(TiledError::Validation(format!("Truncated .npy header: {path}")));
    }

    // Parse shape from header string like "{'descr': '<f8', 'fortran_order': False, 'shape': (480, 640), }"
    let header_str = std::str::from_utf8(&data[10..header_end]).unwrap_or("");
    let shape = parse_npy_shape(header_str);

    let raw_data = data[header_end..].to_vec();
    Ok((raw_data, shape))
}

/// Extract shape tuple from numpy header string.
fn parse_npy_shape(header: &str) -> Vec<usize> {
    // Find "'shape': (" and extract until ")"
    if let Some(start) = header.find("'shape': (") {
        let rest = &header[start + 10..];
        if let Some(end) = rest.find(')') {
            let shape_str = &rest[..end];
            return shape_str
                .split(',')
                .filter_map(|s| s.trim().parse::<usize>().ok())
                .collect();
        }
    }
    vec![]
}

// ---------------------------------------------------------------------------
// HDF5 handler
// ---------------------------------------------------------------------------

#[cfg(feature = "hdf5")]
struct Hdf5Handler {
    file_path: PathBuf,
    frame_per_point: usize,
}

#[cfg(feature = "hdf5")]
impl Hdf5Handler {
    fn new(root: &str, path: &str, frame_per_point: usize) -> Self {
        Self {
            file_path: Path::new(root).join(path),
            frame_per_point,
        }
    }
}

#[cfg(feature = "hdf5")]
impl FileHandler for Hdf5Handler {
    fn read(&self, datum_kwargs: &serde_json::Value) -> Result<(Vec<u8>, Vec<usize>)> {
        let point_number = datum_kwargs
            .get("point_number")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        let file = hdf5::File::open(&self.file_path)
            .map_err(|e| TiledError::Internal(format!("HDF5 open failed: {e}")))?;

        // Area Detector convention: dataset at /entry/data/data
        let dataset = file
            .dataset("entry/data/data")
            .or_else(|_| file.dataset("data"))
            .map_err(|e| TiledError::Internal(format!("HDF5 dataset not found: {e}")))?;

        let full_shape: Vec<usize> = dataset.shape().iter().map(|&s| s as usize).collect();

        // Extract frames for this point.
        let start = point_number * self.frame_per_point;
        let end = start + self.frame_per_point;

        if full_shape.is_empty() || start >= full_shape[0] {
            return Err(TiledError::Validation(format!(
                "Point {point_number} out of range for dataset with {} frames",
                full_shape.first().unwrap_or(&0)
            )));
        }

        // Read as f64 and convert to bytes.
        let data: Vec<f64> = dataset
            .read_slice_1d(start..end.min(full_shape[0]))
            .map_err(|e| TiledError::Internal(format!("HDF5 read error: {e}")))?
            .to_vec();

        let mut shape = vec![end.min(full_shape[0]) - start];
        shape.extend_from_slice(&full_shape[1..]);

        let raw: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
        Ok((raw, shape))
    }
}

// ---------------------------------------------------------------------------
// TIFF handler
// ---------------------------------------------------------------------------

#[cfg(feature = "tiff")]
struct TiffHandler {
    base_path: PathBuf,
}

#[cfg(feature = "tiff")]
impl TiffHandler {
    fn new(root: &str, path: &str) -> Self {
        Self {
            base_path: Path::new(root).join(path),
        }
    }
}

#[cfg(feature = "tiff")]
impl FileHandler for TiffHandler {
    fn read(&self, datum_kwargs: &serde_json::Value) -> Result<(Vec<u8>, Vec<usize>)> {
        let point_number = datum_kwargs
            .get("point_number")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // AD_TIFF convention: files like {base}_{point:06d}.tiff
        let file_path = format!("{}_{point_number:06}.tiff", self.base_path.display());

        let img = image::open(&file_path)
            .map_err(|e| TiledError::Internal(format!("TIFF read failed ({file_path}): {e}")))?;

        let gray = img.to_luma16();
        let (width, height) = gray.dimensions();
        let shape = vec![height as usize, width as usize];

        // Convert u16 pixels to f64 bytes for consistency.
        let raw: Vec<u8> = gray
            .pixels()
            .flat_map(|p| (p.0[0] as f64).to_le_bytes())
            .collect();

        Ok((raw, shape))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_npy_shape() {
        let header = "{'descr': '<f8', 'fortran_order': False, 'shape': (480, 640), }";
        assert_eq!(parse_npy_shape(header), vec![480, 640]);

        let header = "{'descr': '<f4', 'fortran_order': False, 'shape': (100,), }";
        assert_eq!(parse_npy_shape(header), vec![100]);

        let header = "{'descr': '<f8', 'fortran_order': False, 'shape': (), }";
        assert_eq!(parse_npy_shape(header), Vec::<usize>::new());
    }

    #[test]
    fn test_handler_registry_has_builtins() {
        let reg = HandlerRegistry::new();
        assert!(reg.factories.contains_key("NPY_SEQ"));
        assert!(reg.factories.contains_key("npy"));
    }
}
