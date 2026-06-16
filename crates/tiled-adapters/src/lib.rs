pub mod array_adapter;
pub mod coo_adapter;
pub mod map_adapter;
pub mod npy_adapter;
pub mod ragged_adapter;
pub mod sequence_adapter;

#[cfg(feature = "csv-adapter")]
pub mod csv_adapter;
#[cfg(feature = "hdf5")]
pub mod hdf5_adapter;
#[cfg(feature = "parquet")]
pub mod parquet_adapter;
pub mod png_jpeg_adapter;
#[cfg(feature = "tiff")]
pub mod tiff_adapter;
#[cfg(feature = "zarr")]
pub mod zarr_adapter;

pub use array_adapter::ArrayAdapter;
pub use coo_adapter::CooAdapter;
pub use map_adapter::MapAdapter;
pub use npy_adapter::NpyAdapter;
pub use ragged_adapter::{RaggedAdapter, RaggedAdapterRead, RaggedData};
pub use sequence_adapter::{FrameOpener, NpyFrameOpener, SequenceAdapter};

#[cfg(feature = "csv-adapter")]
pub use csv_adapter::CsvAdapter;
#[cfg(feature = "hdf5")]
pub use hdf5_adapter::{Hdf5Adapter, Hdf5Locking};
#[cfg(feature = "parquet")]
pub use parquet_adapter::ParquetAdapter;
pub use png_jpeg_adapter::ImageAdapter;
#[cfg(feature = "tiff")]
pub use tiff_adapter::TiffAdapter;
#[cfg(feature = "zarr")]
pub use zarr_adapter::ZarrAdapter;
