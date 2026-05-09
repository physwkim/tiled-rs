//! File-format leaf resolver.
//!
//! Wires the catalog's `LeafResolver` trait to the file-system adapters in
//! `tiled-adapters`. Picks the right adapter based on the data source's
//! mimetype and decodes the first asset's `data_uri`. Multi-asset
//! sequences (NPY_SEQ etc.) and the more exotic formats are intentionally
//! not handled here — operators with those workloads can wrap or replace
//! this resolver.

use std::path::PathBuf;

use tiled_catalog::Catalog;
use tiled_catalog::adapter::LeafResolver;
use tiled_catalog::error::CatalogError;
use tiled_catalog::orm::Node;
use tiled_core::adapters::AnyAdapter;

pub struct FileLeafResolver;

impl LeafResolver for FileLeafResolver {
    fn resolve(
        &self,
        catalog: &Catalog,
        node: &Node,
    ) -> std::result::Result<AnyAdapter, CatalogError> {
        let data_sources = block_on(catalog.list_data_sources(node.id))?;
        let ds = data_sources.first().ok_or_else(|| {
            CatalogError::Validation(format!(
                "node {} has no data_sources to resolve",
                node.key
            ))
        })?;
        let assets = block_on(catalog.list_assets(ds.id))?;
        let asset = assets.first().ok_or_else(|| {
            CatalogError::Validation(format!(
                "data_source {} has no assets",
                ds.id
            ))
        })?;
        let path = uri_to_path(&asset.data_uri)?;
        let metadata = node.metadata.clone();

        let mimetype = ds.mimetype.as_str();
        let any_adapter = match mimetype {
            "application/x-npy" | "application/x-numpy" | "npy" => {
                let adapter = tiled_adapters::NpyAdapter::from_path(path, metadata)
                    .map_err(|e| CatalogError::Validation(e.to_string()))?;
                AnyAdapter::Array(Box::new(adapter))
            }
            "image/tiff" | "image/x-tiff" | "tiff" => {
                #[cfg(feature = "tiff-adapter")]
                {
                    let adapter = tiled_adapters::TiffAdapter::from_path(path, metadata)
                        .map_err(|e| CatalogError::Validation(e.to_string()))?;
                    AnyAdapter::Array(Box::new(adapter))
                }
                #[cfg(not(feature = "tiff-adapter"))]
                {
                    return Err(CatalogError::Validation(
                        "tiff support not built in".into(),
                    ));
                }
            }
            "image/png" | "image/jpeg" => {
                #[cfg(feature = "tiff-adapter")]
                {
                    let adapter = tiled_adapters::ImageAdapter::from_path(path, metadata)
                        .map_err(|e| CatalogError::Validation(e.to_string()))?;
                    AnyAdapter::Array(Box::new(adapter))
                }
                #[cfg(not(feature = "tiff-adapter"))]
                {
                    return Err(CatalogError::Validation(
                        "image (png/jpeg) support not built in".into(),
                    ));
                }
            }
            "application/x-hdf5" | "application/x-nexus" => {
                #[cfg(feature = "hdf5-adapter")]
                {
                    let dataset = ds
                        .parameters
                        .get("dataset")
                        .and_then(|v| v.as_str())
                        .unwrap_or("entry/data/data");
                    let adapter =
                        tiled_adapters::Hdf5Adapter::from_path(path, dataset, metadata)
                            .map_err(|e| CatalogError::Validation(e.to_string()))?;
                    AnyAdapter::Array(Box::new(adapter))
                }
                #[cfg(not(feature = "hdf5-adapter"))]
                {
                    return Err(CatalogError::Validation(
                        "hdf5 support not built in".into(),
                    ));
                }
            }
            "text/csv" => {
                #[cfg(feature = "csv-adapter")]
                {
                    let adapter = tiled_adapters::CsvAdapter::from_path(path, metadata)
                        .map_err(|e| CatalogError::Validation(e.to_string()))?;
                    AnyAdapter::Table(Box::new(adapter))
                }
                #[cfg(not(feature = "csv-adapter"))]
                {
                    return Err(CatalogError::Validation(
                        "csv support not built in".into(),
                    ));
                }
            }
            "application/x-parquet" => {
                #[cfg(feature = "parquet-adapter")]
                {
                    let adapter = tiled_adapters::ParquetAdapter::from_path(path, metadata)
                        .map_err(|e| CatalogError::Validation(e.to_string()))?;
                    AnyAdapter::Table(Box::new(adapter))
                }
                #[cfg(not(feature = "parquet-adapter"))]
                {
                    return Err(CatalogError::Validation(
                        "parquet support not built in".into(),
                    ));
                }
            }
            other => {
                return Err(CatalogError::Validation(format!(
                    "no built-in adapter for mimetype: {other}"
                )));
            }
        };
        Ok(any_adapter)
    }
}

fn uri_to_path(uri: &str) -> std::result::Result<PathBuf, CatalogError> {
    if let Some(rest) = uri.strip_prefix("file://") {
        Ok(PathBuf::from(rest))
    } else if uri.starts_with('/') {
        Ok(PathBuf::from(uri))
    } else {
        Err(CatalogError::Validation(format!(
            "data_uri {uri} is not a file:// URI"
        )))
    }
}

fn block_on<T>(
    fut: impl std::future::Future<Output = std::result::Result<T, CatalogError>>,
) -> std::result::Result<T, CatalogError> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|_| CatalogError::Validation("FileLeafResolver outside async runtime".into()))?;
    handle.block_on(fut)
}
