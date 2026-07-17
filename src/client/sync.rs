//! `copy` — replicate a node or whole tree from one Tiled instance to another.
//!
//! Port of `tiled/client/sync.py` (`copy` + the per-family `_copy_*` helpers +
//! the `_DISPATCH` table). Given a *source* container and a *destination*
//! container — typically on two different servers — [`copy`] walks the source's
//! children, re-creates each at the destination with its metadata, specs, and a
//! reconstructed data source, and streams the leaf data across block by block
//! (arrays/sparse), partition by partition (tables), or whole (awkward/ragged).
//! Nested containers recurse.
//!
//! ## Deviations from upstream
//!
//! * **Argument shape.** Upstream `copy(source, dest, on_conflict)` is duck-typed
//!   and polymorphic: `source`/`dest` may be a container *or* a single leaf node
//!   (a leaf source fills an already-created same-family `dest` node via
//!   `_copy_array`/etc.). Rust's static types can't express that one signature,
//!   so [`copy`] ports the container→container tree copy — the primary capability
//!   and the one `_copy_container` (`sync.py:96-158`) implements. To fill a single
//!   pre-existing leaf node, call that leaf client's own `write_block` /
//!   `write_partition` / `write` (all public); those are the exact operations the
//!   leaf `_copy_*` helpers invoke.
//!
//! * **Conflict status.** Upstream keys `on_conflict` skip/warn on HTTP `409`
//!   (`sync.py:143`). The tiled-rs *server* currently maps a duplicate-key create
//!   (catalog `UNIQUE` violation → `CatalogError::Conflict`) through
//!   `map_catalog_err` to `ServerError::Validation` → HTTP `422`, not `409` like
//!   upstream Python tiled. [`is_conflict`] therefore recognizes both: a `409`
//!   from any server, and the tiled-rs server's specific `422` whose detail
//!   reports the key already exists — without swallowing other validation errors.
//!
//! * **`warn` is skip-with-log.** Upstream `on_conflict='warn'` calls
//!   `warnings.warn("Skipped existing entry")` and then `continue`s
//!   (`sync.py:144-146`); it does **not** overwrite. [`OnConflict::Warn`] matches
//!   that exactly, emitting a `tracing::warn!` and skipping the existing key.

use std::future::Future;
use std::pin::Pin;

use crate::client::any_client::AnyClient;
use crate::client::array::ArrayClient;
use crate::client::awkward::AwkwardClient;
use crate::client::container::ContainerClient;
use crate::client::dataframe::TableClient;
use crate::client::error::{ClientError, Result};
use crate::client::ragged::RaggedClient;
use crate::client::sparse::SparseClient;
use crate::core::data_source::{DataSource, Management};
use crate::core::structures::StructureFamily;

/// Policy for a key that already exists at the destination.
///
/// Mirrors upstream's `on_conflict` string argument (`sync.py:12-24`), which
/// governs collisions when `_copy_container` calls `dest.new(...)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnConflict {
    /// Propagate the server's conflict error — the copy fails and the existing
    /// destination entry is left untouched. Upstream default (`on_conflict='error'`).
    #[default]
    Error,
    /// Emit a `tracing::warn!` and skip the existing entry (its siblings are still
    /// copied). This is **skip-with-log**, not overwrite — matching upstream
    /// `warnings.warn("Skipped existing entry")` + `continue` (`sync.py:144-146`).
    Warn,
    /// Silently skip the existing entry; its siblings are still copied.
    Skip,
}

/// Copy every child of `source` into `dest`, recursing into sub-containers.
///
/// Ports `copy(source, dest, on_conflict)` for the container→container case
/// (`sync.py:12-53` dispatching to `_copy_container`, `sync.py:96-158`). For each
/// child of `source`:
///
/// * its metadata and specs travel with it;
/// * a data source is reconstructed at the destination — external sources are
///   carried by reference (no data copy), writable leaves get a fresh writable
///   data source with the same `mimetype`/`structure` and then have their data
///   streamed across, and containers are created empty and recursed into;
/// * an existing destination key is handled per [`OnConflict`].
///
/// A child with no data source that is not a container, or a child with more than
/// one data source, is an error — exactly as upstream raises `ValueError` /
/// `NotImplementedError` (`sync.py:100-131`).
pub async fn copy(
    source: &ContainerClient,
    dest: &ContainerClient,
    on_conflict: OnConflict,
) -> Result<()> {
    copy_container(source, dest, on_conflict).await
}

/// Boxed recursive future — `copy_container` calls itself for sub-containers, so
/// the returned future must be heap-allocated to have a finite size.
type CopyFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

fn copy_container<'a>(
    source: &'a ContainerClient,
    dest: &'a ContainerClient,
    on_conflict: OnConflict,
) -> CopyFuture<'a> {
    Box::pin(async move {
        for key in source.keys().await? {
            let child = source.get(&key).await?;
            copy_one_child(&key, &child, dest, on_conflict).await?;
        }
        Ok(())
    })
}

/// Create one `child` under `dest` and, when appropriate, fill its data or
/// recurse. Factored out of the loop only for readability; it is always awaited
/// in-line, so it does not itself need boxing.
async fn copy_one_child(
    key: &str,
    child: &AnyClient,
    dest: &ContainerClient,
    on_conflict: OnConflict,
) -> Result<()> {
    // A resolver-substituted `Custom` client exposes no `BaseClient`, so its
    // metadata / specs / data sources are unreachable — refuse rather than
    // silently drop it. Built-in family clients always have a base.
    let base = child.base().ok_or_else(|| {
        ClientError::Invalid(format!(
            "cannot copy '{key}': node resolved to a custom client with no base"
        ))
    })?;
    let family = child.structure_family();

    // `include_data_sources()` + `data_sources()` (base.py:299-307): fetched
    // lazily when the source client was not built with the flag set.
    let original_data_sources = base.data_sources().await?.unwrap_or_default();
    let metadata = base.metadata().clone();
    let specs = base
        .specs()
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Reconstruct the destination data source, mirroring `_copy_container`
    // (`sync.py:98-131`).
    let create_data_sources: Vec<DataSource> = match original_data_sources.len() {
        0 => {
            if family == StructureFamily::Container {
                // A container with no data sources is a pure organizational node.
                Vec::new()
            } else {
                return Err(ClientError::Invalid(format!(
                    "cannot copy '{key}': a {family} node has no data sources"
                )));
            }
        }
        1 => {
            let ods = &original_data_sources[0];
            if ods.management == Management::External {
                // External data stays external — reference the same asset, no copy.
                vec![ods.clone()]
            } else if family == StructureFamily::Container {
                Vec::new()
            } else {
                // Writable leaf: a fresh writable data source with the same
                // mimetype + structure; parameters/properties/assets/id are
                // dropped (Python dataclass defaults) so the destination server
                // generates its own storage.
                vec![DataSource {
                    structure_family: ods.structure_family,
                    structure: ods.structure.clone(),
                    id: None,
                    mimetype: ods.mimetype.clone(),
                    parameters: serde_json::json!({}),
                    properties: serde_json::json!({}),
                    assets: Vec::new(),
                    management: ods.management,
                }]
            }
        }
        _ => {
            // As of this writing this is impossible, but upstream anticipates it
            // may be added someday (`sync.py:126-131`).
            return Err(ClientError::Invalid(format!(
                "cannot copy '{key}': multiple data sources in one node is not supported"
            )));
        }
    };

    let created_key = match dest
        .create_node(Some(key), family, metadata, specs, create_data_sources)
        .await
    {
        Ok(created_key) => created_key,
        Err(err) if is_conflict(&err) && on_conflict != OnConflict::Error => {
            if on_conflict == OnConflict::Warn {
                tracing::warn!(
                    target: "tiled.client.sync",
                    key,
                    "skipped existing entry at destination"
                );
            }
            return Ok(());
        }
        Err(err) => return Err(err),
    };

    // Recurse / fill only for a writable leaf (copy its data) or a container
    // (copy its children). External leaves already reference their asset; an
    // external-backed node needs nothing further (`sync.py:149-158`).
    let has_writable_ds = original_data_sources
        .first()
        .is_some_and(|d| d.management != Management::External);
    let is_empty_container =
        family == StructureFamily::Container && original_data_sources.is_empty();
    if has_writable_ds || is_empty_container {
        let dest_child = dest.get(&created_key).await?;
        fill(child, &dest_child, on_conflict).await?;
    }
    Ok(())
}

/// Dispatch on family to stream the data (leaf) or recurse (container), the
/// destination node having just been created. Mirrors `_DISPATCH`
/// (`sync.py:161-168`).
async fn fill(source: &AnyClient, dest: &AnyClient, on_conflict: OnConflict) -> Result<()> {
    match source.structure_family() {
        StructureFamily::Container => {
            let sc = source
                .as_container()
                .expect("source family checked as container");
            let dc = dest
                .as_container()
                .expect("destination created with container family");
            copy_container(sc, dc, on_conflict).await
        }
        StructureFamily::Array => copy_array(as_array(source)?, as_array(dest)?).await,
        StructureFamily::Table => copy_table(as_table(source)?, as_table(dest)?).await,
        StructureFamily::Sparse => copy_sparse(as_sparse(source)?, as_sparse(dest)?).await,
        StructureFamily::Awkward => copy_awkward(as_awkward(source)?, as_awkward(dest)?).await,
        StructureFamily::Ragged => copy_ragged(as_ragged(source)?, as_ragged(dest)?).await,
    }
}

/// `_copy_array` (`sync.py:56-61`): read each block by chunk-index, write it to
/// the same chunk-index at the destination. Per-block — the whole array is never
/// held in memory at once.
async fn copy_array(source: &ArrayClient, dest: &ArrayClient) -> Result<()> {
    for block in block_indices(source.chunks()) {
        let read = source.read_block(&block).await?;
        dest.write_block(&block, read.data, true).await?;
    }
    Ok(())
}

/// `_copy_sparse` (`sync.py:72-77`): read each block's COO, write its coords/data
/// to the same chunk-index. Per-block.
async fn copy_sparse(source: &SparseClient, dest: &SparseClient) -> Result<()> {
    for block in block_indices(&source.structure().chunks) {
        let read = source.read_block(&block).await?;
        dest.write_block(&block, &read.coords, &read.data).await?;
    }
    Ok(())
}

/// Port of `_copy_table` (`sync.py:90-93`): copy a table's data across.
///
/// Reads every source partition (`read_partition`, per-partition — the whole
/// table is streamed a partition at a time on the read side) and writes them
/// with a single full-table `PUT /table/full` ([`TableClient::write`]).
///
/// **Deviation from upstream.** Upstream writes each partition to the same
/// partition index (`dest.write_partition(partition, df)`), relying on
/// `dest.new(structure)` having pre-sized the destination to `npartitions`. The
/// tiled-rs server instead derives a *managed* table's partition count from its
/// physical storage — a freshly-created parquet skeleton has zero row groups, so
/// the node reports `npartitions == 0` and rejects `write_partition(0)` until a
/// full write establishes the data. A single full-table PUT is the write the
/// server accepts against a just-created managed table (the same call
/// `Container::write_table` makes). Consequently the read batches are buffered in
/// memory and the destination is written as one partition; a multi-partition
/// source therefore lands as a single partition (its row data is preserved).
///
/// [`TableClient::write`]: crate::client::dataframe::TableClient::write
async fn copy_table(source: &TableClient, dest: &TableClient) -> Result<()> {
    let mut schema: Option<arrow::datatypes::SchemaRef> = None;
    let mut batches: Vec<arrow::array::RecordBatch> = Vec::new();
    for partition in 0..source.npartitions() {
        let part = source.read_partition(partition, None).await?;
        schema.get_or_insert(part.schema);
        batches.extend(part.batches);
    }
    // Nothing to write for a partition-less (empty) source table; the created
    // skeleton already stands in for it.
    if let Some(schema) = schema {
        dest.write(&schema, &batches).await?;
    }
    Ok(())
}

/// `_copy_awkward` (`sync.py:64-69`): read the buffer map whole and write it.
/// Awkward has no per-block read, so the array is loaded whole (matching upstream,
/// which calls `source.read()` then `awkward.to_buffers`).
async fn copy_awkward(source: &AwkwardClient, dest: &AwkwardClient) -> Result<()> {
    let read = source.read().await?;
    dest.write(read.buffers).await
}

/// `_copy_ragged` (`sync.py:80-87`): read the whole array and write it in a
/// single chunk. Loaded whole — upstream does the same and notes it does not
/// preserve the source's chunk boundaries.
async fn copy_ragged(source: &RaggedClient, dest: &RaggedClient) -> Result<()> {
    let data = source.read().await?;
    dest.write(&data, true).await
}

/// The per-axis chunk indices of an array/sparse structure, in row-major order —
/// the Cartesian product of `0..chunks[axis].len()` over every axis. Mirrors
/// upstream `itertools.product(*(range(len(n)) for n in source.chunks))`
/// (`sync.py:57-59`, `73-75`). A 0-dim structure (`chunks == []`) yields one empty
/// block index `[]`, matching `itertools.product()` with no arguments.
fn block_indices(chunks: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let mut result: Vec<Vec<usize>> = vec![Vec::new()];
    for axis in chunks {
        let n = axis.len();
        let mut next = Vec::with_capacity(result.len() * n);
        for prefix in &result {
            for i in 0..n {
                let mut idx = prefix.clone();
                idx.push(i);
                next.push(idx);
            }
        }
        result = next;
    }
    result
}

/// A destination create conflicts (the key already exists) — the signal that
/// governs [`OnConflict`] skip/warn. Recognizes upstream Python tiled's `409`
/// *and* the tiled-rs server's current `422`-with-"already exists" detail (see
/// module docs), while leaving every other validation error to propagate.
fn is_conflict(err: &ClientError) -> bool {
    match err {
        ClientError::Server { status: 409, .. } => true,
        ClientError::Server {
            status: 422,
            detail,
            ..
        } => detail.contains("already exists"),
        _ => false,
    }
}

// Narrowing helpers: a `fill` dispatch has already matched the family, so a
// non-matching client is an internal invariant break, surfaced as a clean error
// rather than a panic.
fn as_array(c: &AnyClient) -> Result<&ArrayClient> {
    c.as_array()
        .ok_or_else(|| ClientError::Invalid("expected an array client".into()))
}
fn as_table(c: &AnyClient) -> Result<&TableClient> {
    c.as_table()
        .ok_or_else(|| ClientError::Invalid("expected a table client".into()))
}
fn as_sparse(c: &AnyClient) -> Result<&SparseClient> {
    c.as_sparse()
        .ok_or_else(|| ClientError::Invalid("expected a sparse client".into()))
}
fn as_awkward(c: &AnyClient) -> Result<&AwkwardClient> {
    c.as_awkward()
        .ok_or_else(|| ClientError::Invalid("expected an awkward client".into()))
}
fn as_ragged(c: &AnyClient) -> Result<&RaggedClient> {
    c.as_ragged()
        .ok_or_else(|| ClientError::Invalid("expected a ragged client".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_conflict_default_is_error() {
        assert_eq!(OnConflict::default(), OnConflict::Error);
    }

    #[test]
    fn block_indices_scalar_is_single_empty_block() {
        // 0-dim (scalar): one block, indexed by the empty tuple.
        assert_eq!(block_indices(&[]), vec![Vec::<usize>::new()]);
    }

    #[test]
    fn block_indices_single_chunk_per_axis() {
        // 2-D array with one chunk per axis → exactly one block [0, 0].
        assert_eq!(block_indices(&[vec![4], vec![3]]), vec![vec![0, 0]]);
    }

    #[test]
    fn block_indices_multi_chunk_row_major() {
        // axis0: 2 chunks, axis1: 3 chunks → 6 blocks, row-major (last axis fastest).
        assert_eq!(
            block_indices(&[vec![2, 2], vec![1, 1, 1]]),
            vec![
                vec![0, 0],
                vec![0, 1],
                vec![0, 2],
                vec![1, 0],
                vec![1, 1],
                vec![1, 2],
            ]
        );
    }

    #[test]
    fn block_indices_empty_axis_yields_no_blocks() {
        // An axis with zero chunks selects no data → no blocks (product of an
        // empty range is empty), matching itertools.product with an empty range.
        assert_eq!(block_indices(&[vec![], vec![2]]), Vec::<Vec<usize>>::new());
    }

    #[test]
    fn is_conflict_recognizes_409_and_422_already_exists() {
        let c409 = ClientError::Server {
            status: 409,
            detail: "Conflict".into(),
            correlation_id: None,
            retry_after: None,
        };
        assert!(is_conflict(&c409));

        let c422 = ClientError::Server {
            status: 422,
            detail: "a node with this key already exists at the same level".into(),
            correlation_id: None,
            retry_after: None,
        };
        assert!(is_conflict(&c422), "tiled-rs server maps a conflict to 422");

        // A different 422 (real validation error) must NOT read as a conflict.
        let c422_other = ClientError::Server {
            status: 422,
            detail: "invalid structure".into(),
            correlation_id: None,
            retry_after: None,
        };
        assert!(!is_conflict(&c422_other));

        assert!(!is_conflict(&ClientError::KeyNotFound("x".into())));
    }
}
