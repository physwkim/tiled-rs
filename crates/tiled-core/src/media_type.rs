//! MIME type constants and serialization/compression registry types.
//!
//! Corresponds to `tiled/media_type_registration.py`.

/// Common MIME types used in Tiled.
pub mod mime {
    pub const OCTET_STREAM: &str = "application/octet-stream";
    pub const JSON: &str = "application/json";
    pub const JSON_SEQ: &str = "application/json-seq";
    pub const MSGPACK: &str = "application/x-msgpack";
    pub const CSV: &str = "text/csv";
    pub const PLAIN: &str = "text/plain";
    pub const HTML: &str = "text/html";
    pub const ARROW_FILE: &str = "application/vnd.apache.arrow.file";
    pub const PARQUET: &str = "application/x-parquet";
    pub const HDF5: &str = "application/x-hdf5";
    pub const TIFF: &str = "image/tiff";
    pub const PNG: &str = "image/png";
    pub const EXCEL: &str = "application/vnd.ms-excel";
    pub const ZIP: &str = "application/zip";
    pub const NETCDF: &str = "application/x-netcdf4";
    pub const ZARR: &str = "application/x-zarr";
}

/// Content encodings (compression).
pub mod encoding {
    pub const GZIP: &str = "gzip";
    pub const ZSTD: &str = "zstd";
    pub const LZ4: &str = "lz4";
    pub const BLOSC2: &str = "blosc2";
}

/// File extension to MIME type aliases.
pub fn resolve_alias(ext: &str) -> Option<&'static str> {
    match ext.trim_start_matches('.').to_lowercase().as_str() {
        "h5" | "hdf5" | "hdf" => Some(mime::HDF5),
        "parquet" | "pq" => Some(mime::PARQUET),
        "arrow" | "feather" | "ipc" => Some(mime::ARROW_FILE),
        "csv" => Some(mime::CSV),
        "json" => Some(mime::JSON),
        "tif" | "tiff" => Some(mime::TIFF),
        "png" => Some(mime::PNG),
        "xlsx" | "xls" => Some(mime::EXCEL),
        "nc" | "nc4" => Some(mime::NETCDF),
        "zarr" => Some(mime::ZARR),
        "msgpack" => Some(mime::MSGPACK),
        "txt" | "text" => Some(mime::PLAIN),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_alias() {
        assert_eq!(resolve_alias("h5"), Some(mime::HDF5));
        assert_eq!(resolve_alias(".parquet"), Some(mime::PARQUET));
        assert_eq!(resolve_alias("CSV"), Some(mime::CSV));
        assert_eq!(resolve_alias("unknown"), None);
    }
}
