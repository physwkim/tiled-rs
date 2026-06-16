//! `TableClient` — read tabular data via the Arrow IPC partition endpoint.
//!
//! Mirrors `tiled/client/dataframe.py`. The Python client switches between
//! pandas and dask; we hand back Arrow `RecordBatch`es so the caller picks
//! their own format (polars, datafusion, ndarray::ArrowArray, …).

use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::ipc::reader::FileReader;
use tiled_core::structures::TableStructure;
use url::Url;

use crate::base::{BaseClient, Item, ParsedStructure};
use crate::context::Context;
use crate::error::{ClientError, Result};
use crate::utils::{ARROW_FILE_MIME_TYPE, retry};

/// A single partition decoded into Arrow record batches.
#[derive(Debug, Clone)]
pub struct TablePartition {
    pub schema: arrow::datatypes::SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Client over a `table` node.
#[derive(Debug, Clone)]
pub struct TableClient {
    base: BaseClient,
}

impl TableClient {
    pub fn from_item(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let base = BaseClient::new(context, item, include_data_sources)?;
        if !matches!(base.structure(), ParsedStructure::Table(_)) {
            return Err(ClientError::StructureMismatch {
                expected: "table".into(),
                got: base
                    .structure_family()
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "unknown".into()),
            });
        }
        Ok(Self { base })
    }

    pub fn base(&self) -> &BaseClient {
        &self.base
    }

    pub fn structure(&self) -> &TableStructure {
        match self.base.structure() {
            ParsedStructure::Table(s) => s,
            _ => unreachable!("TableClient guards on construction"),
        }
    }

    /// Column names of the table.
    pub fn columns(&self) -> &[String] {
        &self.structure().columns
    }

    /// Number of partitions.
    pub fn npartitions(&self) -> usize {
        self.structure().npartitions
    }

    /// Read one partition as Arrow record batches. Optional column projection
    /// emits one `column=` query param per name, matching the server's repeated
    /// `column=` key and the upstream Python client (`params["column"] = columns`).
    pub async fn read_partition(
        &self,
        partition: usize,
        columns: Option<&[&str]>,
    ) -> Result<TablePartition> {
        let link = self.base.require_link("partition")?;

        // Mirror Python `_get_partition` (`dataframe.py:117-149`): estimate the
        // GET URL length and, if the column projection would overflow
        // `URL_CHARACTER_LIMIT`, move the columns into a POST JSON body instead
        // of repeated `column=` query params. A wide table (hundreds of
        // columns) otherwise blows past server URI limits → HTTP 414.
        const URL_CHARACTER_LIMIT: usize = 2_000; // base.py BaseClient.URL_CHARACTER_LIMIT
        const EXTRA_CHARS_PER_ITEM: usize = "&column=".len(); // dataframe.py:26
        let projected_len = link.len()
            + columns
                .map(|cols| {
                    cols.iter()
                        .map(|c| EXTRA_CHARS_PER_ITEM + c.len())
                        .sum::<usize>()
                })
                .unwrap_or(0);

        // Cap concurrent bulk-data fetches across the whole context, mirroring
        // Python's `with self.context.throttle()` around `_get_partition`
        // (`dataframe.py:122`). Held across retries, released on drop.
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes = if projected_len > URL_CHARACTER_LIMIT {
            // POST: `partition` stays a query param; the columns become the JSON
            // body (`json=columns`, dataframe.py:130).
            let mut url = Url::parse(link)?;
            url.query_pairs_mut()
                .append_pair("partition", &partition.to_string());
            let body = serde_json::Value::Array(
                columns
                    .unwrap_or(&[])
                    .iter()
                    .map(|c| serde_json::Value::String((*c).to_string()))
                    .collect(),
            );
            retry(|| async {
                self.base
                    .context
                    .post_bytes(&url, ARROW_FILE_MIME_TYPE, &body)
                    .await
            })
            .await?
        } else {
            let mut url = Url::parse(link)?;
            {
                let mut q = url.query_pairs_mut();
                q.append_pair("partition", &partition.to_string());
                if let Some(cols) = columns {
                    for col in cols {
                        q.append_pair("column", col);
                    }
                }
            }
            retry(|| async {
                self.base
                    .context
                    .get_bytes(&url, ARROW_FILE_MIME_TYPE)
                    .await
            })
            .await?
        };

        let cursor = Cursor::new(bytes.to_vec());
        let reader = FileReader::try_new(cursor, None)?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }
        Ok(TablePartition { schema, batches })
    }

    /// Read every partition, in order.
    pub async fn read(&self, columns: Option<&[&str]>) -> Result<Vec<TablePartition>> {
        let n = self.npartitions();
        let mut out = Vec::with_capacity(n);
        for p in 0..n {
            out.push(self.read_partition(p, columns).await?);
        }
        Ok(out)
    }
}
