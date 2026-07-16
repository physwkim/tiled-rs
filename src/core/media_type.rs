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
    /// Legacy `.xls` (BIFF) workbooks.
    pub const EXCEL: &str = "application/vnd.ms-excel";
    /// Modern `.xlsx` (OOXML) workbooks — the type Python registers for
    /// `.xlsx` and the one tiled-serialization's Excel writer emits.
    pub const EXCEL_XLSX: &str =
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    pub const ZIP: &str = "application/zip";
    pub const NETCDF: &str = "application/netcdf";
    pub const ZARR: &str = "application/x-zarr";
    /// SQL-backed ragged array storage — Python `RAGGED_SQL_MIMETYPE`
    /// (`tiled/mimetypes.py`). The managed-write backend for ragged nodes.
    pub const RAGGED_SQL: &str = "application/x-ragged+sql";
    /// Directory-of-buffers awkward-array storage — Python
    /// `AWKWARD_BUFFERS_MIMETYPE` (`tiled/mimetypes.py:13`). The managed-write
    /// backend for awkward nodes; `DEFAULT_CREATION_MIMETYPE[awkward]`
    /// (`tiled/catalog/adapter.py:120`).
    pub const AWKWARD_BUFFERS: &str = "application/x-awkward-buffers";
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
        "xlsx" => Some(mime::EXCEL_XLSX),
        "xls" => Some(mime::EXCEL),
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

    #[test]
    fn xlsx_resolves_to_ooxml_not_legacy_xls() {
        // .xlsx is the OOXML spreadsheet type (what Python registers and
        // what the Excel serializer emits); only legacy .xls is vnd.ms-excel.
        assert_eq!(
            resolve_alias("xlsx"),
            Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        );
        assert_eq!(resolve_alias("xls"), Some("application/vnd.ms-excel"));
    }

    #[test]
    fn netcdf_alias_matches_python() {
        // Python DEFAULT_ALIASES: nc -> application/netcdf (not x-netcdf4).
        assert_eq!(resolve_alias("nc"), Some("application/netcdf"));
        assert_eq!(resolve_alias("nc4"), Some("application/netcdf"));
    }
}
