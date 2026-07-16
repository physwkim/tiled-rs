//! `TableClient` — read and write tabular data via `/api/v1/table/`.
//!
//! Mirrors `tiled/client/dataframe.py`. The Python client switches between
//! pandas and dask; we hand back Arrow `RecordBatch`es so the caller picks
//! their own format. `write` uploads an Arrow IPC FILE stream to
//! `PUT /api/v1/table/full`.

use std::io::Cursor;

use crate::core::structures::TableStructure;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use url::Url;

use crate::client::any_client::AnyClient;
use crate::client::base::{BaseClient, Item, ParsedStructure};
use crate::client::context::Context;
use crate::client::error::{ClientError, Result};
use crate::client::utils::{ARROW_FILE_MIME_TYPE, decode_response, resolve_export_format, retry};

/// Characters escaped when a column name becomes one path segment of the
/// child-metadata URL, so `?`, `#`, `/`, `%`, ... in a name cannot reshape the
/// request. Matches `container.rs`/`constructors.rs`, which each keep the same
/// per-module copy (the client's established convention for this set).
const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'#')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b'%');

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

    /// Address one `column` of this table as a child *array* node.
    ///
    /// Mirrors upstream `DataFrameClient.__getitem__` (`dataframe.py:202-220`):
    /// issue a metadata GET of the child path `<self>/<column>` and dispatch the
    /// returned item to its family client via [`client_for_item`]. The server
    /// resolves a `[table, column]` metadata path to a synthesized array node
    /// (`ArrayAdapter.from_array(self.read([column])[column].values)`,
    /// `adapters/table.py:143-146`), so this yields an [`AnyClient::Array`] — call
    /// [`AnyClient::into_array`] to narrow and then `read`/`read_block`. A column
    /// absent from the table is a 404, surfaced as [`ClientError::KeyNotFound`]
    /// (upstream raises `KeyError(column)`).
    ///
    /// The request is a bare metadata GET, identical to the one upstream issues;
    /// the resulting client inherits this table's `include_data_sources` flag,
    /// matching how [`ContainerClient::get`] propagates it to a fetched child.
    ///
    /// [`client_for_item`]: crate::client::AnyClient::from_item
    /// [`AnyClient::Array`]: crate::client::AnyClient::Array
    /// [`AnyClient::into_array`]: crate::client::AnyClient::into_array
    /// [`ContainerClient::get`]: crate::client::ContainerClient::get
    pub async fn get_column(&self, column: &str) -> Result<AnyClient> {
        // Append `/<column>` to this node's metadata self link. self link points
        // to /metadata/.../<table>; the extra segment walks into the column.
        let mut url = Url::parse(self.base.require_link("self")?)?;
        let encoded = utf8_percent_encode(column, PATH_SEGMENT).to_string();
        let new_path = if url.path().ends_with('/') {
            format!("{}{}", url.path(), encoded)
        } else {
            format!("{}/{}", url.path(), encoded)
        };
        url.set_path(&new_path);

        let result = retry(|| async {
            let r = self.base.context.get(&url).await?;
            decode_response::<MetadataEnvelope>(r).await
        })
        .await;

        let envelope = match result {
            Ok(env) => env,
            // Upstream maps a 404 on the column path to `KeyError(column)`.
            Err(ClientError::Server { status: 404, .. }) => {
                return Err(ClientError::KeyNotFound(format!("no column '{column}'")));
            }
            Err(e) => return Err(e),
        };
        let item = envelope
            .data
            .ok_or_else(|| ClientError::KeyNotFound(format!("no column '{column}'")))?;
        AnyClient::from_item(
            self.base.context.clone(),
            item,
            self.base.include_data_sources,
        )
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

    /// Write the table. Encodes `batches` as an Arrow IPC FILE stream and sends
    /// it to `PUT /api/v1/table/full`. All batches must share `schema`; the
    /// server validates that the column names match the node's declared columns.
    pub async fn write(&self, schema: &SchemaRef, batches: &[RecordBatch]) -> Result<()> {
        let link = self.base.require_link("full")?;
        let url = Url::parse(link)?;
        let body = encode_arrow_ipc(schema, batches)?;

        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .put_bytes(&url, body.clone())
                .await
                .map(|_| ())
        })
        .await
    }

    /// Overwrite one partition. Encodes `batches` as an Arrow IPC FILE stream
    /// and sends it to `PUT /api/v1/table/partition?partition=N` with
    /// `Content-Type: <Arrow IPC FILE>`. Mirrors Python
    /// `DataFrameClient.write_partition(partition, dataframe)`
    /// (`dataframe.py:241-261`). The server validates the partition index and
    /// that the column names match the node's declared columns.
    pub async fn write_partition(
        &self,
        partition: usize,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<()> {
        let link = self.base.require_link("partition")?;
        let mut url = Url::parse(link)?;
        url.query_pairs_mut()
            .append_pair("partition", &partition.to_string());
        let body = encode_arrow_ipc(schema, batches)?;

        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .put_bytes_typed(&url, body.clone(), ARROW_FILE_MIME_TYPE)
                .await
                .map(|_| ())
        })
        .await
    }

    /// Append rows to one partition. Encodes `batches` as an Arrow IPC FILE
    /// stream and sends it to `PATCH /api/v1/table/partition?partition=N` with
    /// `Content-Type: <Arrow IPC FILE>`. Mirrors Python
    /// `DataFrameClient.append_partition(partition, dataframe)`
    /// (`dataframe.py:263-285`). Rows are appended to the existing partition
    /// data rather than overwriting it.
    pub async fn append_partition(
        &self,
        partition: usize,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<()> {
        let link = self.base.require_link("partition")?;
        let mut url = Url::parse(link)?;
        url.query_pairs_mut()
            .append_pair("partition", &partition.to_string());
        let body = encode_arrow_ipc(schema, batches)?;

        let _permit = self.base.context.data_fetch_permit().await;
        retry(|| async {
            self.base
                .context
                .patch_bytes_typed(&url, body.clone(), ARROW_FILE_MIME_TYPE)
                .await
                .map(|_| ())
        })
        .await
    }

    /// Export the whole table to a file at `dest`, mirroring Python
    /// `DataFrameClient.export`: sends `GET /api/v1/table/full/{path}?format=<fmt>`
    /// and writes the body to `dest`.
    ///
    /// `format` is resolved by the shared `resolve_export_format` helper:
    /// `Some(fmt)` (e.g. `"csv"`, `"json"`, `"parquet"`) is used as given with a
    /// single leading `.` stripped, while `None` infers the format from `dest`'s
    /// file extension (e.g. `table.csv` → `csv`). The server resolves the result
    /// as an alias or media type; an unsupported format returns a server error.
    pub async fn export(&self, dest: &std::path::Path, format: Option<&str>) -> Result<()> {
        let resolved = resolve_export_format(dest, format)?;
        let link = self.base.require_link("full")?;
        let mut url = Url::parse(link)?;
        url.query_pairs_mut().append_pair("format", &resolved);
        let _permit = self.base.context.data_fetch_permit().await;
        let bytes = retry(|| async { self.base.context.get_bytes(&url, "*/*").await }).await?;
        std::fs::write(dest, &bytes)
            .map_err(|e| ClientError::Invalid(format!("write {}: {e}", dest.display())))
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

/// Single-resource envelope for a `/metadata/.../<column>` response
/// (`{data: <item>}`), the shape [`TableClient::get_column`] decodes. Mirrors
/// the private `ResourceEnvelope` the container client uses for a child fetch.
#[derive(Debug, serde::Deserialize)]
struct MetadataEnvelope {
    data: Option<Item>,
}

/// Encode `batches` as an Arrow IPC FILE stream — the body shape every table
/// write endpoint (`/table/full`, `/table/partition`) expects. All batches
/// must share `schema`.
fn encode_arrow_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<bytes::Bytes> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| ClientError::Invalid(format!("Arrow IPC writer: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| ClientError::Invalid(format!("Arrow IPC write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| ClientError::Invalid(format!("Arrow IPC finish: {e}")))?;
    }
    Ok(bytes::Bytes::from(buf))
}
