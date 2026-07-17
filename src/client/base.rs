//! `BaseClient` — fields and methods common to every node client.
//!
//! Mirrors `tiled/client/base.py::BaseClient`. The Python class is the parent
//! of `Container`, `ArrayClient`, `DataFrameClient`, etc., and stores:
//! - the `Context`,
//! - the `item` (the `data` payload from `/api/v1/metadata/.../{path}`),
//! - the parsed structure (`ArrayStructure`, `TableStructure`, ...),
//! - per-family helpers like `metadata`, `specs`, `uri`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use url::Url;

use crate::core::data_source::{Asset, DataSource};
use crate::core::schemas::{NodeAttributes, Resource};
use crate::core::structures::{
    ArrayStructure, AwkwardStructure, ContainerStructure, RaggedStructure, SparseStructure, Spec,
    StructureFamily, TableStructure,
};

use crate::client::context::Context;
use crate::client::error::{ClientError, Result};
use crate::client::utils::{OCTET_STREAM_MIME_TYPE, decode_response, retry};

/// Content-type discriminator sent in the `PATCH /metadata` body — mirrors
/// Python's `base.py:741-757` where the body `content-type` field selects
/// the patch algorithm (RFC 6902 vs RFC 7396).
pub const JSON_PATCH_MIME: &str = "application/json-patch+json";
pub const MERGE_PATCH_MIME: &str = "application/merge-patch+json";

/// Which patch algorithm a [`BaseClient::patch_metadata`] call applies —
/// selects the wire `content-type` field of the `PATCH /metadata` body.
/// Mirrors Python's `content_type` parameter (`base.py:713-741`), which
/// accepts the two MIME strings directly; here the two valid values are an
/// enum so an invalid MIME string cannot be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchContentType {
    /// RFC 6902 JSON Patch: `metadata`/`specs`/`access_blob` are each an
    /// array of patch operations, applied directly to that document.
    JsonPatch,
    /// RFC 7396 JSON Merge Patch: `metadata`/`specs`/`access_blob` are each
    /// a partial document merged in (`null` deletes a key; an absent field
    /// means "no change").
    MergePatch,
}

impl PatchContentType {
    fn mime(self) -> &'static str {
        match self {
            Self::JsonPatch => JSON_PATCH_MIME,
            Self::MergePatch => MERGE_PATCH_MIME,
        }
    }
}

/// One value in a metadata update tree passed to
/// [`BaseClient::update_metadata`] / [`BaseClient::build_metadata_patches`].
///
/// Mirrors the value slot of upstream's nested `metadata` dict, whose three
/// cases in `metadata_update.py::_update_obj` (`base.py:516`) map 1:1 onto the
/// three variants here:
/// - [`Set`](Self::Set) ↔ `else: result[key] = value`. A JSON **object** value
///   merges into the existing object leaf-by-leaf (upstream always *recurses*
///   into `dict` values — it never wholesale-replaces one); a scalar or array
///   value replaces the existing value wholesale.
/// - [`Delete`](Self::Delete) ↔ `value is DELETE_KEY: result.pop(key, None)` —
///   removes the key. This is the Rust analogue of upstream's `DELETE_KEY`
///   sentinel object.
/// - [`Merge`](Self::Merge) ↔ the `isinstance(value, dict)` recurse. Because a
///   [`Set`] holding an object can only carry plain JSON (no nested
///   [`Delete`]), `Merge` is how you place a [`Delete`] — or mix deletes and
///   sets — inside a nested object while leaving its sibling keys untouched.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataUpdate {
    /// Add or replace this key. An object value merges into the existing
    /// object; a scalar or array replaces wholesale.
    Set(serde_json::Value),
    /// Remove this key (upstream's `DELETE_KEY` sentinel).
    Delete,
    /// Merge a nested tree of updates into the existing object at this key,
    /// leaving unmentioned sibling keys untouched.
    Merge(BTreeMap<String, MetadataUpdate>),
}

impl From<serde_json::Value> for MetadataUpdate {
    /// A bare JSON value is a [`Set`](MetadataUpdate::Set), matching upstream
    /// where any non-`DELETE_KEY`, non-`dict` value is assigned directly.
    fn from(value: serde_json::Value) -> Self {
        MetadataUpdate::Set(value)
    }
}

/// The three RFC 6902 patch documents produced by
/// [`BaseClient::build_metadata_patches`], ready to hand to
/// [`BaseClient::patch_metadata`] under [`PatchContentType::JsonPatch`].
///
/// Mirrors upstream's `(metadata_patch, specs_patch, access_blob_patch)` tuple
/// (`base.py:585`): `metadata` is **always** a (possibly empty) ops array;
/// `specs` / `access_blob` are `None` when that facet was not part of the
/// update (specs not supplied; `access_tags` absent or empty).
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataPatches {
    /// RFC 6902 patch for the metadata document. Always an array; `[]` when no
    /// metadata update was requested or the update produced no change.
    pub metadata: serde_json::Value,
    /// RFC 6902 patch for the specs list, or `None` when specs were not part of
    /// this update.
    pub specs: Option<serde_json::Value>,
    /// RFC 6902 patch for the `access_blob`, or `None` when no (non-empty)
    /// `access_tags` were supplied.
    pub access_blob: Option<serde_json::Value>,
}

/// Port of `metadata_update.py::_update_obj` (with `position=None`): merge the
/// update tree into `target` in place. A non-object `target` is reset to an
/// empty object first, matching upstream's `if not isinstance(result, dict)`.
fn apply_update(target: &mut serde_json::Value, update: &BTreeMap<String, MetadataUpdate>) {
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    let map = target
        .as_object_mut()
        .expect("target was just coerced to an object");
    for (key, upd) in update {
        match upd {
            MetadataUpdate::Delete => {
                // upstream: `result.pop(key, None)` — absent key is a no-op.
                map.remove(key);
            }
            MetadataUpdate::Merge(nested) => {
                let child = map
                    .entry(key.clone())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                apply_update(child, nested);
            }
            MetadataUpdate::Set(serde_json::Value::Object(obj)) => {
                // upstream recurses into dict values rather than replacing them.
                let child = map
                    .entry(key.clone())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                merge_raw_object(child, obj);
            }
            MetadataUpdate::Set(value) => {
                map.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Merge a plain JSON object (no sentinels) into `target`, recursing into
/// nested objects the same way [`apply_update`] does. This is the branch
/// upstream takes for a `dict` value already stripped of any `DELETE_KEY`.
fn merge_raw_object(
    target: &mut serde_json::Value,
    source: &serde_json::Map<String, serde_json::Value>,
) {
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    let map = target
        .as_object_mut()
        .expect("target was just coerced to an object");
    for (key, value) in source {
        if let serde_json::Value::Object(obj) = value {
            let child = map
                .entry(key.clone())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            merge_raw_object(child, obj);
        } else {
            map.insert(key.clone(), value.clone());
        }
    }
}

/// Serialize an RFC 6902 patch to its JSON ops array. Infallible for the
/// `json_patch::Patch` produced from already-valid JSON documents.
fn patch_to_value(patch: json_patch::Patch) -> serde_json::Value {
    serde_json::to_value(patch).expect("an RFC 6902 patch is always JSON-serializable")
}

/// Pure core of [`BaseClient::build_metadata_patches`]: compute the three RFC
/// 6902 patches from the current values (`current_metadata`,
/// `current_spec_names`, `current_access_blob`) and the requested changes.
/// Split out from the method so the diff behavior is unit-testable without a
/// live client/server.
fn compute_metadata_patches(
    current_metadata: &serde_json::Value,
    current_spec_names: &[String],
    current_access_blob: &serde_json::Value,
    metadata: Option<&BTreeMap<String, MetadataUpdate>>,
    specs: Option<&[String]>,
    access_tags: Option<&[String]>,
) -> MetadataPatches {
    let metadata_patch = match metadata {
        None => serde_json::Value::Array(Vec::new()),
        Some(update) => {
            let mut merged = current_metadata.clone();
            apply_update(&mut merged, update);
            patch_to_value(json_patch::diff(current_metadata, &merged))
        }
    };

    let specs_patch = specs.map(|specs| {
        let current: Vec<serde_json::Value> = current_spec_names
            .iter()
            .map(|name| serde_json::Value::String(name.clone()))
            .collect();
        let target: Vec<serde_json::Value> = specs
            .iter()
            .map(|name| serde_json::Value::String(name.clone()))
            .collect();
        let mut ops = patch_to_value(json_patch::diff(
            &serde_json::Value::Array(current),
            &serde_json::Value::Array(target),
        ));
        normalize_spec_ops(&mut ops);
        ops
    });

    let access_blob_patch = match access_tags {
        Some(tags) if !tags.is_empty() => {
            let mut merged = current_access_blob.clone();
            let mut update = BTreeMap::new();
            update.insert(
                "tags".to_string(),
                MetadataUpdate::Set(serde_json::Value::Array(
                    tags.iter()
                        .cloned()
                        .map(serde_json::Value::String)
                        .collect(),
                )),
            );
            apply_update(&mut merged, &update);
            Some(patch_to_value(json_patch::diff(
                current_access_blob,
                &merged,
            )))
        }
        _ => None,
    };

    MetadataPatches {
        metadata: metadata_patch,
        specs: specs_patch,
        access_blob: access_blob_patch,
    }
}

/// Rewrite each `add`/`replace` op's bare-string spec name into a `Spec`
/// object, mirroring upstream's normalization of the string-list specs diff
/// into `asdict(Spec(value))` before it hits the wire (`base.py:776-782`).
fn normalize_spec_ops(ops: &mut serde_json::Value) {
    let Some(arr) = ops.as_array_mut() else {
        return;
    };
    for op in arr {
        let Some(name) = op.get("value").and_then(|v| v.as_str()).map(str::to_owned) else {
            continue; // `remove` ops carry no value.
        };
        if let Some(obj) = op.as_object_mut() {
            obj.insert(
                "value".to_string(),
                serde_json::to_value(Spec::new(name)).expect("Spec is always JSON-serializable"),
            );
        }
    }
}

/// The `data` field of a `/metadata/.../<path>` response. We carry it whole so
/// the family-specific clients can reach for whatever attribute they need.
pub type Item = Resource<NodeAttributes>;

/// Parsed structure variant — one per family.
#[derive(Debug, Clone)]
pub enum ParsedStructure {
    Container(Option<ContainerStructure>),
    Array(ArrayStructure),
    Table(TableStructure),
    Sparse(SparseStructure),
    Awkward(AwkwardStructure),
    Ragged(RaggedStructure),
}

impl ParsedStructure {
    pub fn from_item(item: &Item) -> Result<Self> {
        let attrs = &item.attributes;
        let family = attrs
            .structure_family
            .ok_or_else(|| ClientError::Invalid("item missing structure_family".into()))?;
        match family {
            StructureFamily::Container => {
                // Container structure is `{contents, count}` — we don't parse contents
                // upfront because it is server-set and may be lazy.
                Ok(Self::Container(None))
            }
            StructureFamily::Array => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("array missing structure".into()))?;
                Ok(Self::Array(ArrayStructure::from_json(s).map_err(|e| {
                    ClientError::Invalid(format!("invalid array structure: {e}"))
                })?))
            }
            StructureFamily::Table => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("table missing structure".into()))?;
                Ok(Self::Table(TableStructure::from_json(s).map_err(|e| {
                    ClientError::Invalid(format!("invalid table structure: {e}"))
                })?))
            }
            StructureFamily::Sparse => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("sparse missing structure".into()))?;
                Ok(Self::Sparse(SparseStructure::from_json(s).map_err(
                    |e| ClientError::Invalid(format!("invalid sparse structure: {e}")),
                )?))
            }
            StructureFamily::Awkward => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("awkward missing structure".into()))?;
                Ok(Self::Awkward(AwkwardStructure::from_json(s).map_err(
                    |e| ClientError::Invalid(format!("invalid awkward structure: {e}")),
                )?))
            }
            StructureFamily::Ragged => {
                let s = attrs
                    .structure
                    .as_ref()
                    .ok_or_else(|| ClientError::Invalid("ragged missing structure".into()))?;
                Ok(Self::Ragged(RaggedStructure::from_json(s).map_err(
                    |e| ClientError::Invalid(format!("invalid ragged structure: {e}")),
                )?))
            }
        }
    }
}

/// Common state for any client — held by family-specific clients.
#[derive(Debug, Clone)]
pub struct BaseClient {
    pub(crate) context: Context,
    pub(crate) item: Item,
    pub(crate) structure: ParsedStructure,
    pub(crate) include_data_sources: bool,
}

impl BaseClient {
    pub fn new(context: Context, item: Item, include_data_sources: bool) -> Result<Self> {
        let structure = ParsedStructure::from_item(&item)?;
        Ok(Self {
            context,
            item,
            structure,
            include_data_sources,
        })
    }

    /// The HTTP context.
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// The full item payload (id + attributes + links).
    pub fn item(&self) -> &Item {
        &self.item
    }

    /// The node's id (last path segment).
    pub fn id(&self) -> &str {
        &self.item.id
    }

    /// User metadata. Returns `&Value::Null` if the server omitted it.
    pub fn metadata(&self) -> &serde_json::Value {
        const NULL: serde_json::Value = serde_json::Value::Null;
        self.item.attributes.metadata.as_ref().unwrap_or(&NULL)
    }

    /// Specs the node conforms to.
    pub fn specs(&self) -> &[Spec] {
        self.item.attributes.specs.as_deref().unwrap_or(&[])
    }

    /// Path ancestors (e.g. `["root", "subgroup"]`).
    pub fn ancestors(&self) -> &[String] {
        &self.item.attributes.ancestors
    }

    /// Family of the underlying structure.
    pub fn structure_family(&self) -> Option<StructureFamily> {
        self.item.attributes.structure_family
    }

    /// Parsed structure dataclass.
    pub fn structure(&self) -> &ParsedStructure {
        &self.structure
    }

    /// `self` link as a fully-qualified URL string.
    pub fn uri(&self) -> Option<&str> {
        self.item.links.self_link.as_deref()
    }

    /// Look up a link by name (`self`, `search`, `full`, `block`, `partition`,
    /// `buffers`).
    pub fn link(&self, name: &str) -> Option<&str> {
        match name {
            "self" => self.item.links.self_link.as_deref(),
            "search" => self.item.links.search.as_deref(),
            "full" => self.item.links.full.as_deref(),
            other => self.item.links.extra.get(other).map(String::as_str),
        }
    }

    /// Helper: required link, or `MissingLink` error.
    pub(crate) fn require_link(&self, name: &str) -> Result<&str> {
        self.link(name)
            .ok_or_else(|| ClientError::MissingLink(name.to_string()))
    }

    /// Delete this node via `DELETE /api/v1/metadata/{path}`.
    ///
    /// `recursive`: when `false` (default) the server refuses to delete a
    /// non-empty container; pass `true` to delete the node and its whole
    /// subtree in one call. `external_only`: when `true` (default) the server
    /// refuses to delete a node that has internally-managed storage anywhere in
    /// the affected subtree; pass `false` to also remove managed storage files.
    /// Parameter order mirrors Python `BaseClient.delete(recursive=False,
    /// external_only=True)` (base.py:918-936).
    pub async fn delete(&self, recursive: bool, external_only: bool) -> Result<()> {
        let self_link = self.require_link("self")?;
        let mut url = Url::parse(self_link)?;
        if recursive {
            url.query_pairs_mut().append_pair("recursive", "true");
        }
        if !external_only {
            url.query_pairs_mut().append_pair("external_only", "false");
        }
        // Wrapped in `retry` to match upstream `BaseClient.delete`, which issues
        // the DELETE inside `retry_context` (`base.py:931-936`).
        retry(|| async { self.context.delete(&url).await })
            .await
            .map(|_| ())
    }

    /// Declare the end of this node's stream of writes via
    /// `DELETE /api/v1/stream/close/{path}`.
    ///
    /// A stream *producer* calls this to signal end-of-stream: the server ends
    /// the node's stream — every live subscriber's WebSocket closes cleanly with
    /// 1000 "Producer ended stream" — and fires a `stream-closed` webhook.
    /// Mirrors Python `BaseClient.close_stream` (`base.py:940-945`): the
    /// close-stream link is this node's `self` link with its first `/metadata`
    /// segment rewritten to `/stream/close` (so a node whose key is literally
    /// `metadata` is unaffected, matching [`revisions`](Self::revisions)). The
    /// server gates the call on `write:data` for the node.
    pub async fn close_stream(&self) -> Result<()> {
        let self_link = self.require_link("self")?;
        let link = self_link.replacen("/metadata", "/stream/close", 1);
        let url = Url::parse(&link)?;
        retry(|| async { self.context.delete(&url).await })
            .await
            .map(|_| ())
    }

    /// Patch this node's metadata, specs, and/or access_blob via
    /// `PATCH /api/v1/metadata/{path}`. Mirrors Python
    /// `BaseClient.patch_metadata` (`base.py:713-834`).
    ///
    /// `content_type` selects the patch algorithm: with
    /// [`PatchContentType::JsonPatch`], each of `metadata`/`specs`/`access_blob`
    /// is an RFC 6902 patch-operations array applied directly to that
    /// document; with [`PatchContentType::MergePatch`], each is an RFC 7396
    /// partial document merged in (`null` deletes a key). `metadata`, `specs`,
    /// and `access_blob` are three independent patch documents — pass `None`
    /// to leave a field unchanged. `drop_revision`, when true, discards the
    /// pre-patch version instead of recording it in the revision history
    /// (`?drop_revision=true`).
    pub async fn patch_metadata(
        &self,
        metadata: Option<serde_json::Value>,
        specs: Option<serde_json::Value>,
        access_blob: Option<serde_json::Value>,
        content_type: PatchContentType,
        drop_revision: bool,
    ) -> Result<()> {
        let self_link = self.require_link("self")?;
        let mut url = Url::parse(self_link)?;
        if drop_revision {
            url.query_pairs_mut().append_pair("drop_revision", "true");
        }
        let body = serde_json::json!({
            "content-type": content_type.mime(),
            "metadata": metadata,
            "specs": specs,
            "access_blob": access_blob,
        });
        // Wrapped in `retry` to match upstream `BaseClient.patch_metadata`, which
        // issues the PATCH inside `retry_context` (`base.py:800-834`). This also
        // covers `update_metadata`, which delegates here.
        retry(|| async { self.context.patch_json(&url, &body).await })
            .await
            .map(|_| ())
    }

    /// Replace this node's metadata, specs, and/or access_blob wholesale via
    /// `PUT /api/v1/metadata/{path}`. Mirrors Python
    /// `BaseClient.replace_metadata` (`base.py:836-889`).
    ///
    /// Unlike [`patch_metadata`](Self::patch_metadata) (a partial patch), each
    /// of `metadata`, `specs`, `access_blob` is the *full* replacement
    /// document. `None` leaves that field unchanged; the server treats an
    /// explicit `Some(serde_json::Value::Null)` identically to `None` (both
    /// mean "keep the current value") — to clear a field, pass an empty
    /// document (`Some(json!({}))` / `Some(json!([]))`) instead.
    /// `drop_revision` behaves as in [`patch_metadata`](Self::patch_metadata).
    pub async fn replace_metadata(
        &self,
        metadata: Option<serde_json::Value>,
        specs: Option<serde_json::Value>,
        access_blob: Option<serde_json::Value>,
        drop_revision: bool,
    ) -> Result<()> {
        let self_link = self.require_link("self")?;
        let mut url = Url::parse(self_link)?;
        if drop_revision {
            url.query_pairs_mut().append_pair("drop_revision", "true");
        }
        let body = serde_json::json!({
            "metadata": metadata,
            "specs": specs,
            "access_blob": access_blob,
        });
        // Wrapped in `retry` to match upstream `BaseClient.replace_metadata`,
        // which issues the PUT inside `retry_context` (`base.py:880-889`).
        retry(|| async { self.context.put_json(&url, &body).await })
            .await
            .map(|_| ())
    }

    /// Build the RFC 6902 patches that [`update_metadata`](Self::update_metadata)
    /// would send, without sending them. Mirrors Python
    /// `BaseClient.build_metadata_patches` (`base.py:585-691`).
    ///
    /// Each patch is computed by merging the caller's desired changes into a
    /// copy of the *current* value (the snapshot this client holds) and diffing
    /// old-vs-new — the JSON-Merge-Patch-as-intermediary approach upstream uses
    /// so the surface reads like `dict.update` while still emitting a minimal
    /// RFC 6902 patch:
    /// - `metadata`: the merged tree (see [`MetadataUpdate`]) diffed against the
    ///   current metadata. **Always** an ops array — `[]` when `metadata` is
    ///   `None` or the merge changes nothing, matching upstream where
    ///   `metadata_patch` is never `None`.
    /// - `specs`: `None` when `specs` is `None`; otherwise the current spec
    ///   *names* diffed against the requested names, with each add/replace value
    ///   normalized from a bare name to a `Spec` object. Passing this call only
    ///   sets spec names (versions become unset), exactly as upstream's
    ///   name-list diff does.
    /// - `access_blob`: `None` when `access_tags` is `None` or empty (an empty
    ///   list is a no-op upstream); otherwise `{"tags": access_tags}` merged
    ///   into the current `access_blob` and diffed.
    pub fn build_metadata_patches(
        &self,
        metadata: Option<&BTreeMap<String, MetadataUpdate>>,
        specs: Option<&[String]>,
        access_tags: Option<&[String]>,
    ) -> MetadataPatches {
        const NULL: serde_json::Value = serde_json::Value::Null;
        let current_spec_names: Vec<String> = self.specs().iter().map(|s| s.name.clone()).collect();
        let current_access_blob = self.item.attributes.access_blob.as_ref().unwrap_or(&NULL);
        compute_metadata_patches(
            self.metadata(),
            &current_spec_names,
            current_access_blob,
            metadata,
            specs,
            access_tags,
        )
    }

    /// Update this node's metadata/specs/access via a `dict.update`-like
    /// interface, building the RFC 6902 patches from the desired changes and
    /// PATCHing them. Mirrors Python `BaseClient.update_metadata`
    /// (`base.py:516-583`): a convenience wrapper over
    /// [`patch_metadata`](Self::patch_metadata) that always sends
    /// [`PatchContentType::JsonPatch`].
    ///
    /// `metadata` is a tree of [`MetadataUpdate`]s — [`Set`](MetadataUpdate::Set)
    /// to add/replace, [`Delete`](MetadataUpdate::Delete) to remove a key,
    /// [`Merge`](MetadataUpdate::Merge) to descend into a nested object. `specs`
    /// replaces the spec-name set; `access_tags`, when non-empty, sets the
    /// `access_blob` tags. `drop_revision` behaves as in
    /// [`patch_metadata`](Self::patch_metadata).
    ///
    /// Like upstream, this always issues the PATCH even when the diff is empty
    /// (an empty metadata ops array), so the server still records a revision
    /// unless `drop_revision` is set. This client's held snapshot is **not**
    /// mutated; re-fetch the node to observe the server's post-patch state.
    pub async fn update_metadata(
        &self,
        metadata: Option<&BTreeMap<String, MetadataUpdate>>,
        specs: Option<&[String]>,
        access_tags: Option<&[String]>,
        drop_revision: bool,
    ) -> Result<()> {
        let patches = self.build_metadata_patches(metadata, specs, access_tags);
        self.patch_metadata(
            Some(patches.metadata),
            patches.specs,
            patches.access_blob,
            PatchContentType::JsonPatch,
            drop_revision,
        )
        .await
    }

    /// Access this node's metadata revision history, served by
    /// `GET`/`DELETE /api/v1/revisions/{path}`.
    ///
    /// Mirrors Python `BaseClient.metadata_revisions` (`base.py:910`): the
    /// revisions link is the node's `self` link with its `/metadata` segment
    /// rewritten to `/revisions` (the first occurrence, so a node whose key is
    /// literally `metadata` is unaffected). Revisions are a catalog capability
    /// — against a server with no catalog every call returns a `405`
    /// [`ClientError::Server`].
    pub fn revisions(&self) -> Result<MetadataRevisions> {
        let self_link = self.require_link("self")?;
        let link = self_link.replacen("/metadata", "/revisions", 1);
        Ok(MetadataRevisions::new(self.context.clone(), link))
    }

    /// The node's data sources — its storage backends and their assets —
    /// lazily fetching them when this client was not constructed with
    /// `include_data_sources=true`.
    ///
    /// Mirrors Python `BaseClient.data_sources` (`base.py:299`) composed with
    /// `include_data_sources` (`base.py:307`): when the flag was set at
    /// construction the already-attached sources are returned with no extra
    /// round-trip; otherwise the node's own `self` URI is re-fetched with
    /// `?include_data_sources=true` (upstream's `refresh`, `base.py:207`) so the
    /// server attaches the sources and their asset database ids, rather than
    /// erroring. The re-fetch lands on a throwaway item and does *not* mutate
    /// `self`, matching upstream, whose `include_data_sources()` refreshes a
    /// `new_variation`, not the original client.
    ///
    /// Returns `Ok(None)` when the node reports no `data_sources` attribute even
    /// after the fetch (upstream returns `None` for a missing attribute).
    pub async fn data_sources(&self) -> Result<Option<Vec<DataSource>>> {
        if self.include_data_sources {
            return Ok(self.item.attributes.data_sources.clone());
        }
        // Lazily enable: re-fetch this node's own `self` URI with the flag set
        // and read the sources off the returned item. Preserving any existing
        // query on the link and appending the flag mirrors upstream `refresh`,
        // which merges `include_data_sources` into `parse_qs(self.uri)`.
        let self_link = self.require_link("self")?;
        let mut url = Url::parse(self_link)?;
        url.query_pairs_mut()
            .append_pair("include_data_sources", "true");
        let item = retry(|| async {
            let resp = self.context.get(&url).await?;
            let envelope: ResourceEnvelope = decode_response(resp).await?;
            envelope
                .data
                .ok_or_else(|| ClientError::Invalid("metadata response missing data".into()))
        })
        .await?;
        Ok(item.attributes.data_sources)
    }

    /// The formats the server can export this node as — the media types it can
    /// serialize the node's structure family (and any of its specs) into,
    /// sorted and de-duplicated.
    ///
    /// Mirrors Python `BaseClient.formats` (`base.py:503`): the union of the
    /// server's registered formats for each of the node's spec names and for
    /// its structure family. The format table is read from the server's About
    /// payload (`GET /api/v1/`, fetched once and cached on the [`Context`] via
    /// [`Context::server_info`](crate::client::Context::server_info)).
    ///
    /// Parity note: the Rust server keys its About `formats` map solely by
    /// structure family (`serialization_registry.all_formats`), never by spec
    /// name, so the per-spec lookups contribute nothing against a stock Rust
    /// server; the loop is kept for algorithmic parity with upstream (and to
    /// pick up spec-keyed formats should a server register any). Where upstream
    /// indexes the structure-family entry directly (a `KeyError` if absent),
    /// this treats an absent family key as contributing no formats rather than
    /// erroring — every family the Rust registry serves is always present in
    /// the map, so the softening only affects an out-of-contract server.
    pub async fn formats(&self) -> Result<Vec<String>> {
        let about = self.context.server_info().await?;
        let table = &about.formats;
        let mut formats: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for spec in self.specs() {
            if let Some(list) = table.get(spec.name.as_str()) {
                formats.extend(list.iter().cloned());
            }
        }
        if let Some(family) = self.structure_family()
            && let Some(list) = table.get(family.to_string().as_str())
        {
            formats.extend(list.iter().cloned());
        }
        Ok(formats.into_iter().collect())
    }

    /// The node's `self` link with its first `/metadata` segment rewritten to
    /// `/asset/manifest`. Mirrors Python `asset_manifest`'s
    /// `self.item["links"]["self"].replace("/metadata", "/asset/manifest", 1)`.
    fn asset_manifest_link(&self) -> Result<String> {
        Ok(self
            .require_link("self")?
            .replacen("/metadata", "/asset/manifest", 1))
    }

    /// The node's `self` link with its first `/metadata` segment rewritten to
    /// `/asset/bytes`. Mirrors Python `raw_export`'s
    /// `self.item["links"]["self"].replace("/metadata", "/asset/bytes", 1)`.
    fn asset_bytes_link(&self) -> Result<String> {
        Ok(self
            .require_link("self")?
            .replacen("/metadata", "/asset/bytes", 1))
    }

    /// GET `{manifest link}?id={asset id}` and return its `manifest` list — the
    /// forward-slash paths of the directory asset's files, relative to the
    /// asset directory. The asset must be a directory and carry an id.
    async fn fetch_manifest(&self, manifest_link: &str, asset: &Asset) -> Result<Vec<String>> {
        let id = require_asset_id(asset)?;
        let mut url = Url::parse(manifest_link)?;
        url.query_pairs_mut().append_pair("id", &id.to_string());
        // The manifest endpoint answers with a bare `{"manifest": [...]}` body
        // (axum `Json`), not the standard `{data, error, ...}` envelope, so it
        // is decoded straight into `ManifestBody` rather than via `get_json`.
        let body: ManifestBody =
            retry(|| async { decode_response(self.context.get(&url).await?).await }).await?;
        Ok(body.manifest)
    }

    /// GET the raw bytes of one asset (optionally one file within a directory
    /// asset via `relative_path`) from `{bytes link}?id={asset id}`.
    async fn fetch_asset_bytes(
        &self,
        bytes_link: &str,
        asset: &Asset,
        relative_path: Option<&str>,
    ) -> Result<bytes::Bytes> {
        let id = require_asset_id(asset)?;
        let mut url = Url::parse(bytes_link)?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("id", &id.to_string());
            if let Some(rel) = relative_path {
                q.append_pair("relative_path", rel);
            }
        }
        // The endpoint streams `application/octet-stream`; it never applies the
        // blosc2 content-encoding to a raw asset, so `get_bytes` returns the
        // file verbatim.
        retry(|| async { self.context.get_bytes(&url, OCTET_STREAM_MIME_TYPE).await }).await
    }

    /// Build a manifest of the relative paths backing each of this node's
    /// assets.
    ///
    /// Mirrors Python `BaseClient.asset_manifest` (`base.py:342`): for every
    /// asset of every data source, an asset backed by a single file maps to
    /// `None` (no manifest) and an asset backed by a directory maps to the
    /// server's `/asset/manifest` listing (the forward-slash paths of its files
    /// relative to the asset directory). Upstream returns a `dict` keyed on
    /// asset id; the idiomatic equivalent here is an ordered [`AssetEntry`] per
    /// asset, preserving data-source and asset order.
    ///
    /// The data sources are resolved through
    /// [`data_sources`](Self::data_sources), which lazily fetches them (with
    /// their asset ids) when this client was not built with
    /// `include_data_sources=true`; a node reporting no data sources yields an
    /// empty manifest.
    pub async fn asset_manifest(&self) -> Result<Vec<AssetEntry>> {
        let data_sources = self.data_sources().await?.unwrap_or_default();
        let manifest_link = self.asset_manifest_link()?;
        let mut out = Vec::new();
        for ds in &data_sources {
            for asset in &ds.assets {
                let relative_paths = if asset.is_directory {
                    Some(self.fetch_manifest(&manifest_link, asset).await?)
                } else {
                    None
                };
                out.push(AssetEntry {
                    asset_id: asset.id,
                    relative_paths,
                });
            }
        }
        Ok(out)
    }

    /// Download the raw assets backing this node into `dest_dir`, returning the
    /// paths written (in download order).
    ///
    /// Mirrors Python `BaseClient.raw_export` (`base.py:380`): it refuses a node
    /// backed by more than one data source
    /// (["Export of multiple data sources not yet supported"]), and for the
    /// single data source writes each asset — a single-file asset as one file,
    /// a directory asset as its manifest walk, each file fetched with its
    /// `relative_path` and written preserving the relative layout. When the data
    /// source has exactly one asset the files land directly under `dest_dir`;
    /// when it has several, each asset is namespaced under `dest_dir/{asset id}`
    /// (matching upstream's `Path(destination, str(asset.id))`).
    ///
    /// A single-file asset's on-disk name is the basename of its `file://`
    /// `data_uri`, which is exactly the `Content-Disposition` attachment
    /// filename the server sends (upstream writes to that header via
    /// `ATTACHMENT_FILENAME_PLACEHOLDER`; deriving it locally avoids a
    /// dependency on response headers, which [`Context::get_bytes`] discards).
    ///
    /// Deviation from upstream: downloads run sequentially — the `max_workers`
    /// parallelism is not reproduced (parity does not depend on it, and no
    /// progress callback is invented). The data sources are resolved through
    /// [`data_sources`](Self::data_sources), which lazily fetches them (with
    /// their asset ids) when this client was not built with
    /// `include_data_sources=true`.
    pub async fn raw_export(&self, dest_dir: &Path) -> Result<Vec<PathBuf>> {
        let data_sources = self.data_sources().await?.unwrap_or_default();
        // Upstream guard (base.py:435): a node backed by anything other than
        // exactly one data source is refused (0 or >1 both hit this).
        if data_sources.len() != 1 {
            return Err(ClientError::Invalid(
                "Export of multiple data sources not yet supported".into(),
            ));
        }
        let manifest_link = self.asset_manifest_link()?;
        let bytes_link = self.asset_bytes_link()?;
        let mut written = Vec::new();
        for ds in &data_sources {
            // Single asset → files land directly in `dest_dir`; several assets →
            // namespace each by its id (base.py:444-451).
            let namespace_by_id = ds.assets.len() != 1;
            for asset in &ds.assets {
                let base = if namespace_by_id {
                    dest_dir.join(require_asset_id(asset)?.to_string())
                } else {
                    dest_dir.to_path_buf()
                };
                if asset.is_directory {
                    for rel in self.fetch_manifest(&manifest_link, asset).await? {
                        let bytes = self
                            .fetch_asset_bytes(&bytes_link, asset, Some(&rel))
                            .await?;
                        let target = base.join(&rel);
                        write_asset_file(&target, &bytes).await?;
                        written.push(target);
                    }
                } else {
                    let bytes = self.fetch_asset_bytes(&bytes_link, asset, None).await?;
                    let target = base.join(single_file_name(asset)?);
                    write_asset_file(&target, &bytes).await?;
                    written.push(target);
                }
            }
        }
        Ok(written)
    }
}

/// One asset's contribution to a node's raw-download manifest, as produced by
/// [`BaseClient::asset_manifest`].
///
/// Mirrors one key/value pair of Python `asset_manifest`'s dict (`base.py:342`),
/// which is keyed on asset id.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetEntry {
    /// The asset's database id (`Asset.id`) — the `?id=` query parameter used
    /// to download it. Always populated when the node was fetched with
    /// `include_data_sources=true`.
    pub asset_id: Option<i64>,
    /// `None` for an asset backed by a single file (no manifest); `Some(paths)`
    /// for an asset backed by a directory, where each entry is a forward-slash
    /// path relative to the asset directory (the server's `/asset/manifest`
    /// listing) usable directly as a `relative_path` download argument.
    pub relative_paths: Option<Vec<String>>,
}

/// Wire shape of the `/asset/manifest` response body: a bare
/// `{"manifest": [relative paths]}` object (not the `{data, ...}` envelope).
#[derive(Debug, Deserialize)]
struct ManifestBody {
    #[serde(default)]
    manifest: Vec<String>,
}

/// Wire shape of a `/metadata/.../{path}` response envelope — its `data` field
/// is the node [`Item`]. Decoded by [`BaseClient::data_sources`] when lazily
/// re-fetching the node with `include_data_sources=true`. Mirrors the private
/// envelope the constructors and container client decode for the same GET.
#[derive(Debug, Deserialize)]
struct ResourceEnvelope {
    data: Option<Item>,
}

/// An asset's database id, or [`ClientError::Invalid`] when it is absent (the
/// node was not fetched with `include_data_sources=true`, so the server never
/// assigned one) — every asset download needs it as the `?id=` parameter.
fn require_asset_id(asset: &Asset) -> Result<i64> {
    asset.id.ok_or_else(|| {
        ClientError::Invalid(format!(
            "asset backed by '{}' has no id; fetch the node with \
             include_data_sources=true so the server assigns one",
            asset.data_uri
        ))
    })
}

/// The on-disk filename for a single-file asset: the last path segment of its
/// `file://` `data_uri`, percent-decoded. This reproduces the server's
/// `Content-Disposition` attachment filename (`get_asset_bytes` sets it from
/// the same basename).
///
/// The derivation stays at the URI-string level on purpose. `data_uri` names a
/// *server-side* path, so running it through the *client*-platform
/// [`file_uri_to_path`] would let client path semantics intrude: a
/// drive-letterless Unix server path (`file:///data/x.h5`) is not a valid
/// absolute path on Windows, so the conversion yields `None` and no filename.
/// Splitting the URL path is platform-independent by construction — `C:` is
/// just another segment on every platform.
///
/// [`file_uri_to_path`]: crate::core::file_uri::file_uri_to_path
fn single_file_name(asset: &Asset) -> Result<String> {
    let invalid = || {
        ClientError::Invalid(format!(
            "cannot derive a filename from asset data_uri '{}'",
            asset.data_uri
        ))
    };
    let url = Url::parse(&asset.data_uri).map_err(|_| invalid())?;
    if url.scheme() != "file" {
        return Err(invalid());
    }
    // Last path segment, rejecting an empty tail (a trailing slash or missing
    // path names a directory or the root, which has no filename).
    let segment = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|s| !s.is_empty())
        .ok_or_else(invalid)?;
    percent_encoding::percent_decode_str(segment)
        .decode_utf8()
        .map(|name| name.into_owned())
        .map_err(|_| invalid())
}

/// Write `bytes` to `target`, creating parent directories first so a directory
/// asset's nested layout is reproduced. Filesystem errors surface as
/// [`ClientError::Invalid`] with the offending path (the client has no
/// dedicated I/O error variant).
async fn write_asset_file(target: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            ClientError::Invalid(format!("creating directory '{}': {e}", parent.display()))
        })?;
    }
    tokio::fs::write(target, bytes)
        .await
        .map_err(|e| ClientError::Invalid(format!("writing file '{}': {e}", target.display())))
}

/// One historical version of a node's metadata, as served by a single item of
/// `GET /api/v1/revisions/{path}`.
///
/// Mirrors the item shape the server's `get_revisions` builds (Python
/// `construct_revisions_response`): `{revision_number, attributes: {metadata,
/// specs, time_updated}}`. The server stores an `access_blob` snapshot on each
/// revision but intentionally omits it from this listing (matching upstream),
/// so there is no access-control field to surface here.
#[derive(Debug, Clone, PartialEq)]
pub struct Revision {
    /// Per-node sequential revision number, 1-based and ascending with age
    /// (revision 1 is the oldest recorded version).
    pub revision_number: i64,
    /// The node's user metadata as of this revision.
    pub metadata: serde_json::Value,
    /// The specs the node conformed to as of this revision.
    pub specs: Vec<Spec>,
    /// When this version was superseded (the stored `time_created`), as the
    /// server-formatted timestamp string.
    pub time_updated: String,
}

/// Wire shape of one `data` item from the revisions endpoint.
#[derive(Debug, Deserialize)]
struct RevisionWire {
    revision_number: i64,
    #[serde(default)]
    attributes: RevisionAttributesWire,
}

/// Wire shape of a revision item's `attributes` object.
#[derive(Debug, Default, Deserialize)]
struct RevisionAttributesWire {
    #[serde(default)]
    metadata: serde_json::Value,
    /// `Option` so an explicit `null` (a node that had no specs) decodes to
    /// `None` rather than failing; both `None` and absent become an empty vec.
    #[serde(default)]
    specs: Option<Vec<Spec>>,
    #[serde(default)]
    time_updated: String,
}

impl From<RevisionWire> for Revision {
    fn from(w: RevisionWire) -> Self {
        Self {
            revision_number: w.revision_number,
            metadata: w.attributes.metadata,
            specs: w.attributes.specs.unwrap_or_default(),
            time_updated: w.attributes.time_updated,
        }
    }
}

/// Accessor for a node's metadata revision history, backed by the server's
/// `/api/v1/revisions/{path}` endpoint. Obtained via [`BaseClient::revisions`].
///
/// Mirrors Python `tiled.client.base.MetadataRevisions` (`base.py:28`), whose
/// `len()` / `[i]` / `[start:stop]` / `delete_revision(n)` map here to
/// [`count`](Self::count) / [`get`](Self::get) / [`list`](Self::list) /
/// [`delete`](Self::delete). Unlike the Python class this holds no length
/// cache — each [`count`](Self::count) call refetches.
#[derive(Debug, Clone)]
pub struct MetadataRevisions {
    context: Context,
    /// Fully-qualified `/api/v1/revisions/{path}` URL.
    link: String,
}

impl MetadataRevisions {
    fn new(context: Context, link: String) -> Self {
        Self { context, link }
    }

    /// Total number of stored revisions for the node.
    ///
    /// Wire: `GET {revisions link}?page[offset]=0&page[limit]=0`, returning
    /// `meta.count` — the node-wide total, independent of pagination (correct
    /// post-#1409). Mirrors Python `MetadataRevisions.__len__` (`base.py:34`).
    pub async fn count(&self) -> Result<usize> {
        let mut url = Url::parse(&self.link)?;
        url.query_pairs_mut()
            .append_pair("page[offset]", "0")
            .append_pair("page[limit]", "0");
        let resp =
            retry(|| async { self.context.get_json::<Vec<RevisionWire>>(&url).await }).await?;
        let count = resp
            .meta
            .as_ref()
            .and_then(|m| m.get("count"))
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| ClientError::Invalid("revisions response missing meta.count".into()))?;
        Ok(count as usize)
    }

    /// Fetch a single revision by its **offset** in the oldest-first ordering
    /// (offset 0 = the oldest revision), not by its `revision_number`.
    ///
    /// Wire: `GET {revisions link}?page[offset]={offset}&page[limit]=1`,
    /// returning the one `data` entry. Mirrors Python
    /// `MetadataRevisions.__getitem__` for an integer index (`base.py:61`). An
    /// offset past the end maps to [`ClientError::KeyNotFound`].
    pub async fn get(&self, offset: usize) -> Result<Revision> {
        let mut url = Url::parse(&self.link)?;
        url.query_pairs_mut()
            .append_pair("page[offset]", &offset.to_string())
            .append_pair("page[limit]", "1");
        let resp =
            retry(|| async { self.context.get_json::<Vec<RevisionWire>>(&url).await }).await?;
        resp.data
            .unwrap_or_default()
            .into_iter()
            .next()
            .map(Revision::from)
            .ok_or_else(|| ClientError::KeyNotFound(format!("no revision at offset {offset}")))
    }

    /// List revisions starting at `offset`, oldest first, following the
    /// server's `next` pagination links until the history is exhausted.
    ///
    /// `limit` bounds the per-request page size (the server caps it at its own
    /// maximum); `None` uses the server default. Either way every revision from
    /// `offset` onward is returned — a small `limit` just means more
    /// round-trips. Mirrors Python `MetadataRevisions.__getitem__` for a slice
    /// (`base.py:84`): the same `page[offset]`/`page[limit]` request followed
    /// by the `links.next` walk.
    pub async fn list(&self, offset: usize, limit: Option<usize>) -> Result<Vec<Revision>> {
        let mut url = Url::parse(&self.link)?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("page[offset]", &offset.to_string());
            if let Some(l) = limit {
                q.append_pair("page[limit]", &l.to_string());
            }
        }
        let mut out: Vec<Revision> = Vec::new();
        let mut next = Some(url);
        while let Some(page_url) = next {
            let resp =
                retry(|| async { self.context.get_json::<Vec<RevisionWire>>(&page_url).await })
                    .await?;
            if let Some(page) = resp.data {
                out.extend(page.into_iter().map(Revision::from));
            }
            next = match resp.links.as_ref().and_then(|l| l.get("next")) {
                Some(serde_json::Value::String(s)) => Some(Url::parse(s)?),
                _ => None,
            };
        }
        Ok(out)
    }

    /// Delete one stored revision by its `revision_number`.
    ///
    /// Wire: `DELETE {revisions link}?number={number}`. Mirrors Python
    /// `MetadataRevisions.delete_revision` (`base.py:112`). Deleting a
    /// nonexistent revision surfaces the server's `404` as
    /// [`ClientError::Server`].
    pub async fn delete(&self, number: i64) -> Result<()> {
        let mut url = Url::parse(&self.link)?;
        url.query_pairs_mut()
            .append_pair("number", &number.to_string());
        retry(|| async { self.context.delete(&url).await })
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(item: serde_json::Value) -> Revision {
        serde_json::from_value::<RevisionWire>(item).unwrap().into()
    }

    #[test]
    fn revision_wire_maps_attributes_and_specs() {
        let rev = wire(serde_json::json!({
            "revision_number": 3,
            "attributes": {
                "metadata": {"a": 1},
                "specs": [{"name": "foo", "version": "1.0"}, {"name": "bar"}],
                "time_updated": "2026-07-17T00:00:00Z",
            },
        }));
        assert_eq!(rev.revision_number, 3);
        assert_eq!(rev.metadata, serde_json::json!({"a": 1}));
        assert_eq!(rev.time_updated, "2026-07-17T00:00:00Z");
        assert_eq!(rev.specs.len(), 2);
        assert_eq!(rev.specs[0].name, "foo");
        assert_eq!(rev.specs[0].version.as_deref(), Some("1.0"));
        assert_eq!(rev.specs[1].name, "bar");
        assert_eq!(rev.specs[1].version, None);
    }

    #[test]
    fn revision_wire_null_specs_becomes_empty_vec() {
        // A node that never had specs may serialize `specs: null`; the
        // `Option<Vec<Spec>>` shape must decode that to an empty vec, not error.
        let rev = wire(serde_json::json!({
            "revision_number": 1,
            "attributes": {"metadata": {}, "specs": null, "time_updated": "t"},
        }));
        assert!(rev.specs.is_empty());
    }

    #[test]
    fn revision_wire_absent_fields_default() {
        // Absent metadata → Null, absent specs → empty, absent time → empty.
        let rev = wire(serde_json::json!({
            "revision_number": 7,
            "attributes": {},
        }));
        assert_eq!(rev.revision_number, 7);
        assert_eq!(rev.metadata, serde_json::Value::Null);
        assert!(rev.specs.is_empty());
        assert_eq!(rev.time_updated, "");
    }

    fn asset(data_uri: &str, id: Option<i64>) -> Asset {
        Asset {
            data_uri: data_uri.into(),
            is_directory: false,
            parameter: None,
            num: None,
            id,
        }
    }

    #[test]
    fn require_asset_id_errors_when_absent() {
        // A missing asset id (node not fetched with include_data_sources) can't
        // build the `?id=` download query → Invalid, not a panic or silent skip.
        let err = require_asset_id(&asset("file:///d/x.h5", None)).unwrap_err();
        assert!(matches!(err, ClientError::Invalid(_)));
        // A present id passes straight through.
        assert_eq!(
            require_asset_id(&asset("file:///d/x.h5", Some(7))).unwrap(),
            7
        );
    }

    #[test]
    fn single_file_name_derives_basename_cross_platform() {
        // Derivation is at the URI string level, so each case yields the same
        // filename on every platform — no cfg-gated assertions. Matches the
        // server's Content-Disposition attachment filename.
        //
        // Unix-style server path (drive-letterless) — the Windows-CI regression:
        // this previously ran through the client's PathBuf conversion, which
        // rejects a driveletterless path on Windows.
        assert_eq!(
            single_file_name(&asset("file:///data/scan001.h5", Some(1))).unwrap(),
            "scan001.h5"
        );
        // Windows-style server path — `C:` is just a leading path segment.
        assert_eq!(
            single_file_name(&asset("file:///C:/data/scan001.h5", Some(1))).unwrap(),
            "scan001.h5"
        );
        // A percent-encoded segment decodes back to the literal filename.
        assert_eq!(
            single_file_name(&asset("file:///data/my%20scan.h5", Some(1))).unwrap(),
            "my scan.h5"
        );
    }

    #[test]
    fn single_file_name_rejects_unusable_uris() {
        // A non-file scheme, a directory (trailing slash), and the bare root all
        // lack a usable filename segment → Invalid.
        for uri in ["s3://bucket/obj", "file:///data/", "file://"] {
            let err = single_file_name(&asset(uri, Some(1))).unwrap_err();
            assert!(
                matches!(err, ClientError::Invalid(_)),
                "expected Invalid for {uri}, got {err:?}"
            );
        }
    }

    // --- update_metadata diff-builder (compute_metadata_patches) ---------------

    use serde_json::json;

    /// Build a metadata-only patch from `current` and `update`.
    fn md_patch(
        current: serde_json::Value,
        update: BTreeMap<String, MetadataUpdate>,
    ) -> serde_json::Value {
        compute_metadata_patches(
            &current,
            &[],
            &serde_json::Value::Null,
            Some(&update),
            None,
            None,
        )
        .metadata
    }

    /// Apply an emitted RFC 6902 ops array back onto `doc`, returning the result.
    fn apply_ops(mut doc: serde_json::Value, ops: &serde_json::Value) -> serde_json::Value {
        let patch: json_patch::Patch = serde_json::from_value(ops.clone()).unwrap();
        json_patch::patch(&mut doc, &patch).unwrap();
        doc
    }

    fn upd(pairs: Vec<(&str, MetadataUpdate)>) -> BTreeMap<String, MetadataUpdate> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    #[test]
    fn update_metadata_adds_new_key() {
        let current = json!({"a": 1});
        let ops = md_patch(
            current.clone(),
            upd(vec![("b", MetadataUpdate::Set(json!(2)))]),
        );
        assert_eq!(ops, json!([{"op": "add", "path": "/b", "value": 2}]));
        assert_eq!(apply_ops(current, &ops), json!({"a": 1, "b": 2}));
    }

    #[test]
    fn update_metadata_replaces_existing_key() {
        let current = json!({"a": 1});
        let ops = md_patch(
            current.clone(),
            upd(vec![("a", MetadataUpdate::Set(json!(2)))]),
        );
        assert_eq!(ops, json!([{"op": "replace", "path": "/a", "value": 2}]));
        assert_eq!(apply_ops(current, &ops), json!({"a": 2}));
    }

    #[test]
    fn update_metadata_deletes_key_via_sentinel() {
        let current = json!({"a": 1, "b": 2});
        let ops = md_patch(current.clone(), upd(vec![("b", MetadataUpdate::Delete)]));
        assert_eq!(ops, json!([{"op": "remove", "path": "/b"}]));
        assert_eq!(apply_ops(current, &ops), json!({"a": 1}));
    }

    #[test]
    fn update_metadata_delete_absent_key_is_noop() {
        // upstream `result.pop(key, None)` — deleting a missing key changes
        // nothing, so the diff is empty.
        let current = json!({"a": 1});
        let ops = md_patch(current, upd(vec![("missing", MetadataUpdate::Delete)]));
        assert_eq!(ops, json!([]));
    }

    #[test]
    fn update_metadata_nested_merge_patches_only_changed_leaf() {
        // Merge into `a`: only `a/b` changes; sibling `a/c` is untouched, so the
        // emitted patch names only `/a/b`.
        let current = json!({"a": {"b": 1, "c": 2}, "d": 9});
        let ops = md_patch(
            current.clone(),
            upd(vec![(
                "a",
                MetadataUpdate::Merge(upd(vec![("b", MetadataUpdate::Set(json!(10)))])),
            )]),
        );
        assert_eq!(ops, json!([{"op": "replace", "path": "/a/b", "value": 10}]));
        assert_eq!(
            apply_ops(current, &ops),
            json!({"a": {"b": 10, "c": 2}, "d": 9})
        );
    }

    #[test]
    fn update_metadata_set_object_merges_like_nested_dict() {
        // A `Set` holding an object merges (upstream recurses into dict values)
        // rather than replacing wholesale: `a/c` survives.
        let current = json!({"a": {"b": 1, "c": 2}});
        let ops = md_patch(
            current.clone(),
            upd(vec![("a", MetadataUpdate::Set(json!({"b": 10})))]),
        );
        assert_eq!(ops, json!([{"op": "replace", "path": "/a/b", "value": 10}]));
        assert_eq!(apply_ops(current, &ops), json!({"a": {"b": 10, "c": 2}}));
    }

    #[test]
    fn update_metadata_nested_delete_leaves_siblings() {
        let current = json!({"a": {"b": 1, "c": 2}});
        let ops = md_patch(
            current.clone(),
            upd(vec![(
                "a",
                MetadataUpdate::Merge(upd(vec![("c", MetadataUpdate::Delete)])),
            )]),
        );
        assert_eq!(ops, json!([{"op": "remove", "path": "/a/c"}]));
        assert_eq!(apply_ops(current, &ops), json!({"a": {"b": 1}}));
    }

    #[test]
    fn update_metadata_noop_yields_empty_patch() {
        let current = json!({"a": 1});
        // metadata = None → always [] (mirrors upstream `metadata_patch = []`).
        assert_eq!(
            compute_metadata_patches(&current, &[], &serde_json::Value::Null, None, None, None)
                .metadata,
            json!([])
        );
        // Empty update map → [].
        assert_eq!(md_patch(current.clone(), BTreeMap::new()), json!([]));
        // Setting a key to its existing value → [].
        assert_eq!(
            md_patch(current, upd(vec![("a", MetadataUpdate::Set(json!(1)))])),
            json!([])
        );
    }

    #[test]
    fn update_metadata_specs_add_replace_remove_normalize_to_spec_objects() {
        // add: ["foo"] -> ["foo", "bar"]
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &["foo".into()],
            &serde_json::Value::Null,
            None,
            Some(&["foo".into(), "bar".into()]),
            None,
        );
        assert_eq!(
            p.specs.unwrap(),
            json!([{"op": "add", "path": "/1", "value": {"name": "bar"}}]),
            "added spec name normalizes to a Spec object (no null version)"
        );

        // replace: ["foo"] -> ["bar"]
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &["foo".into()],
            &serde_json::Value::Null,
            None,
            Some(&["bar".into()]),
            None,
        );
        assert_eq!(
            p.specs.unwrap(),
            json!([{"op": "replace", "path": "/0", "value": {"name": "bar"}}])
        );

        // remove: ["foo", "bar"] -> ["foo"]
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &["foo".into(), "bar".into()],
            &serde_json::Value::Null,
            None,
            Some(&["foo".into()]),
            None,
        );
        assert_eq!(
            p.specs.unwrap(),
            json!([{"op": "remove", "path": "/1"}]),
            "remove ops carry no value to normalize"
        );
    }

    #[test]
    fn update_metadata_specs_none_vs_empty() {
        // specs = None → no specs patch at all.
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &["foo".into()],
            &serde_json::Value::Null,
            None,
            None,
            None,
        );
        assert!(p.specs.is_none());

        // specs = [] → clear all specs (a real patch, not None).
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &["foo".into()],
            &serde_json::Value::Null,
            None,
            Some(&[]),
            None,
        );
        assert_eq!(p.specs.unwrap(), json!([{"op": "remove", "path": "/0"}]));
    }

    #[test]
    fn update_metadata_access_tags_none_and_empty_are_noops() {
        // access_tags = None → no access_blob patch.
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &[],
            &json!({"tags": ["old"]}),
            None,
            None,
            None,
        );
        assert!(p.access_blob.is_none());
        // access_tags = [] → still a no-op (upstream: empty list is a no-op).
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &[],
            &json!({"tags": ["old"]}),
            None,
            None,
            Some(&[]),
        );
        assert!(p.access_blob.is_none());
    }

    #[test]
    fn update_metadata_access_tags_sets_tags() {
        let current_blob = json!({"tags": ["old"], "owner": "u"});
        let p = compute_metadata_patches(
            &serde_json::Value::Null,
            &[],
            &current_blob,
            None,
            None,
            Some(&["new".into()]),
        );
        let ops = p.access_blob.unwrap();
        // Only the tags key is touched; `owner` is left alone.
        assert_eq!(
            apply_ops(current_blob, &ops),
            json!({"tags": ["new"], "owner": "u"})
        );
    }

    #[test]
    fn metadata_update_from_value_is_set() {
        assert_eq!(
            MetadataUpdate::from(json!(5)),
            MetadataUpdate::Set(json!(5))
        );
    }
}
