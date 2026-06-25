pub mod array_adapter;
pub mod coo_adapter;
pub mod map_adapter;
pub mod npy_adapter;
pub mod ragged_adapter;
pub mod sequence_adapter;

#[cfg(feature = "arrow-ipc")]
pub mod arrow_adapter;
#[cfg(feature = "csv-adapter")]
pub mod csv_adapter;
#[cfg(feature = "csv-adapter")]
pub mod csv_array_adapter;
#[cfg(feature = "excel-adapter")]
pub mod excel_adapter;
#[cfg(feature = "hdf5")]
pub mod hdf5_adapter;
#[cfg(feature = "parquet")]
pub mod parquet_adapter;
pub mod png_jpeg_adapter;
#[cfg(feature = "sql-adapter")]
pub mod sql_adapter;
#[cfg(feature = "tiff")]
pub mod tiff_adapter;
#[cfg(feature = "zarr")]
pub mod zarr_adapter;

pub use array_adapter::ArrayAdapter;
pub use coo_adapter::CooAdapter;
pub use map_adapter::MapAdapter;
pub use npy_adapter::{NpyAdapter, init_storage_npy, npy_bytes};
pub use ragged_adapter::RaggedAdapter;
// `RaggedAdapterRead` and `RaggedData` now live in tiled-core (alongside
// `AnyAdapter::Ragged`); re-export them here for source compatibility.
pub use sequence_adapter::{FrameOpener, NpyFrameOpener, SequenceAdapter};
pub use tiled_core::adapters::{RaggedAdapterRead, RaggedData};

#[cfg(feature = "arrow-ipc")]
pub use arrow_adapter::ArrowIpcAdapter;
#[cfg(feature = "csv-adapter")]
pub use csv_adapter::{CsvAdapter, init_storage_csv};
#[cfg(feature = "csv-adapter")]
pub use csv_array_adapter::CsvArrayAdapter;
#[cfg(feature = "excel-adapter")]
pub use excel_adapter::ExcelAdapter;
#[cfg(feature = "hdf5")]
pub use hdf5_adapter::{Hdf5Adapter, Hdf5Locking};
#[cfg(feature = "parquet")]
pub use parquet_adapter::{ParquetAdapter, init_storage_parquet};
pub use png_jpeg_adapter::ImageAdapter;
#[cfg(feature = "sql-adapter")]
pub use sql_adapter::SqlTableAdapter;
#[cfg(feature = "tiff")]
pub use tiff_adapter::TiffAdapter;
#[cfg(feature = "zarr")]
pub use zarr_adapter::{MANAGED_ARRAY_PATH, ZarrAdapter, init_storage_zarr};
