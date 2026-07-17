# Upstream tiled PR audit

Status of every notable [bluesky/tiled](https://github.com/bluesky/tiled)
PR against this Rust port. Each row is one of:

* **Ported** — code shipped, with a commit reference. Behaviour matches
  upstream (or is documented where it diverges).
* **Already covered** — the upstream change targets a feature we built
  during the initial port; nothing to do.
* **Deferred** — substantive port that's tracked in the workspace task
  list (`#NNN` task IDs) for a future batch.
* **N/A** — the upstream PR targets a Python-specific path
  (alembic, dask, pydantic, FastAPI deps, frontend npm, or a feature
  family this port doesn't carry — Composite, etc.).

Audit is sweep-based, ordered newest-first within each batch. Older
PRs that pre-date our schema are surveyed but typically N/A — they
fix problems in code we never wrote.

## Ported

| PR | Title | Commit | Notes |
|----|-------|--------|-------|
| #1325 | Fix PNG slice param for large 2D arrays | e495f17 | Slice formatting in ArrayND (`{cuts},::stride,::stride`, no leading comma when no cuts). |
| #1326 | SequenceAdapter reshape | d48e5f4 | (initial port) |
| #1330 | HDF5 slice-aware read | ddb130f | Hyperslab `(offsets, counts)` instead of slurping the whole dataset; strided reads decimate in Rust. |
| #1337 | NDSlice composition | ddb130f | `NDSlice::compose` + `is_expanded` precondition + 8 unit tests. |
| #1339 | DataAppended emit | ec00c22 | (initial port) |
| #1343 | OIDC ProxiedHeader fix | d48e5f4 | (initial port) |
| #1349 | Customizable WebUI (spec_views) | 49115a7 | `/settings.json` with `web.spec_views` YAML config; SPA renders matched-spec links instead of dynamic JS plugins (paradigm difference, link-based substitute). |
| #1350 | Authentication in WebUI | 069846b | Token store, auth context, login page, Bearer header, 401-refresh; `/api/v1/` made public so SPA can discover providers; identity in login response. |
| #1351 | WebSocket first-message auth | d48e5f4 | (initial port) |
| #1353 | Webhooks | e2e6896 | Migrations, ORM, CRUD, REST router, dispatcher, HMAC signing, retry/backoff, SSRF protection, `--webhooks-allow-http` / `--webhooks-allow-private-addresses`. |
| #1364 | Entra/external-OIDC validator | d48e5f4 | (initial port) |
| #1365 | spec_views metadata field | 49115a7 | `{path}/{metadata}` placeholder substitution in spec view URL. |
| #1367 | Color image display | e495f17 | `(H,W,3/4)` heuristic + PNG path for color, canvas for grayscale. |
| #1374 | Grayscale + float rendering | e495f17 | Canvas + colormaps (Gray/Viridis/Plasma/Inferno/Magma) + log scale + dtype-aware decode (b1/u1/u2/u4/i1/i2/i4/u8/i8/f4/f8). |
| #218  | Subscription retry | ec00c22 | (initial port) |
| #262  | structure+reference caps | ec00c22 | (initial port) |
| #287  | access policy | ec00c22 | (initial port) |
| #503  | Reject DELETE on non-empty container | 48c46e3 | rmdir-style 422 instead of CASCADE. |
| #622  | In/NotIn/Contains SQL queries | 62d6c88 | Real predicates instead of no-op stubs; #746 empty-list semantics; LIKE escape. |
| #640  | Postgres FTS | cd061f9 | `to_tsvector @@ plainto_tsquery` + GIN migration; SQLite stays on LIKE. |
| #657  | POST alternatives for long URLs | cd061f9 | `POST /api/v1/array/full`, `POST /api/v1/container/full`. |
| #660  | Container deep export (zip) | cd061f9 + a2c8269 | Two-phase: collect leaf bytes async, build zip sync; arrays `.bin`, tables `.arrow`, sub-containers recurse, sparse/awkward `.json` breadcrumb. Also HTML/json-seq via existing serializer dispatch. |
| #688  | PATCH content-type dispatch | cd061f9 | `application/json-patch+json` (RFC 6902 via `json-patch` crate), `application/merge-patch+json` (RFC 7396), default partial replace. |
| #762  | Range requests | cd061f9 | `serve_with_range` helper; 206 + Content-Range for `bytes=…`; `Accept-Ranges: bytes` always; multi-range bypassed. |
| #802  | Appendable arrays (interface) | cd061f9 + a2c8269 | `ArrayAdapterRead::as_writable()` + `ArrayAdapterWrite::append()` default-impl; `PATCH /api/v1/array/full/{path}?append_along=N` route + `data-appended` event. zarrs write integration has since landed in `zarr_adapter.rs` (`write`/`write_block`/`append`/`patch` over `store_array_subset`/`set_shape`, with integration tests) — see the Resolved-out-of-Deferred note below. |
| #944  | Accept scalar / empty data in HDF5 | 138fa4b | Zero-rank promoted to `shape=[1]`; empty arrays returned as zero-byte buffers. |
| #972  | drop_revision flag | 138fa4b | `?drop_revision=true` on PATCH metadata. |
| #1084 | access_blob on revisions | 70bcf47 | Migration 0003 + ORM + insert. |
| #1176 | Stream metadata-update events | 559290f | `MetadataUpdated` kind already emitted; added `specs` field to payload. |
| #1178 | OIDC authorization-code + PKCE flow | 5e68486 + fd18601 | Full code-flow port. `OidcProvider` extended with `client_id`, `client_secret`, `authorization_endpoint`, `token_endpoint`, `redirect_on_success`, `redirect_on_failure`. `PendingAuthStore` (in-memory, 10-min expiry) holds PKCE verifier + nonce between /authorize and /callback. `GET /api/v1/auth/provider/{p}/authorize` (302 to IdP with PKCE S256 + nonce + state). `GET /api/v1/auth/provider/{p}/callback` (code exchange → id_token validation via same JWKS machinery + nonce check → ensure_principal + mint tiled session). redirect_on_success redirects browser with percent-encoded tokens. Single-process limitation documented (in-memory store). 559290f = false-advertise removal (prerequisite). |
| #1302 | Multi-chunk `?block=` range | e3660db | `BlockSpec::Range` + cartesian-product walk + ND row-major copy; 7 unit tests. Single-chunk fast-path preserved. |
| #143  | RGB TIFF support | (this batch) | Color TIFFs were silently coerced to grayscale + u16; now `[h, w, channels]` shape with native dtype (u8/u16/u32/f32/f64). |
| #1164 | HDF5 locking parameter | (this batch) | rust-hdf5 0.2.8 added `H5File::options().locking(...)` (mirrors libhdf5's `HDF5_USE_FILE_LOCKING`); ported `Hdf5Locking::{Default,Disabled,BestEffort}` + `from_path_with_locking` constructor. Earlier audit marked N/A because 0.2.0 was lock-less. |
| #1018 | Background-task lifecycle | 3e14c4a + 72a0e77 | `AppState::background_tasks` (`BackgroundTasks`: JoinSet registry + watch-channel cancellation) is the single owner for process-lifetime tasks; the webhook dispatcher registers with it and selects on `cancellation()` instead of a detached `tokio::spawn`. `cli::run()` calls `background_tasks.shutdown()` (signal + await, exactly once) after the HTTP listener stops accepting connections. On shutdown the dispatcher drains queued + in-flight webhook deliveries — it owns a per-delivery `JoinSet`, drains the bus backlog into it on cancellation, then awaits every delivery bounded by `WebhookConfig::drain_timeout` (default 30 s; abandons the rest cleanly). Mirrors upstream `WebhookDispatcher.shutdown` (`tiled/server/webhooks.py:352`, `asyncio.gather` of `_pending_tasks`), with the wait bounded rather than unbounded. |
| #157  | vlen-string HDF5 datasets | 0be4d0e | `dtype_from_hdf5` detects the vlen-string CLASS instead of erroring; elements packed into null-padded `S<N>` fixed-width bytes (N = longest element), matching upstream's `numpy.asarray(ds[()], dtype=bytes)` coercion. #127 SWMR remains documented-unsupported (6bf3689): rust-hdf5 0.2.27 exposes no `swmr`/`libver` on `H5File`, and its `SwmrFileReader` lacks the datatype-CLASS accessor the adapter's Kind detection needs; default (`TILED_HDF5_SWMR_DEFAULT=0`) behaviour already matches upstream. |
| #1096 | Postgres approximate length | ceb03ab | `lbound_count_children` (LIMIT-bounded), `approx_count_children` (`pg_stats.most_common_vals/freqs` × `pg_class.reltuples`), `count_children_or_approx` gate wired into the single-node metadata route via `exact_count_limit`; SQLite stays exact. Follow-up extends it to per-entry search-page counts + batches the SQLite page counts (one `GROUP BY parent_id` per page). |
| #649  | include_data_sources on search | a30e258 | `SearchEntry.data_sources: Option<Vec<DataSource>>`; `search_page` takes the flag; SQL catalog batches the page with `list_data_sources_for_nodes` (two IN-clause queries, no N+1); `data_sources` removed from `?fields=` pruning to match upstream's flag-only gating. |
| #774  | Zarr protocol routers (/zarr/v2 + /zarr/v3) | fa0f521 + 99360f1 + 6fbb894 | Every catalog node served as a read-only zarr store: v2 `.zgroup`/`.zarray`/`.zattrs` + dotted chunk keys, v3 `zarr.json` + `c/…` keys, N-D zero-padded boundary chunks, per-node access policy. Deliberate divergence: chunks are uncompressed (`compressor: null` / `bytes` codec) — a valid zarr subset — instead of upstream's blosc(lz4), which the in-repo blosc2 tooling cannot reproduce verifiably. The two former follow-ups landed: **sparse densify** (6fbb894 — `SparseData::densify` COO→dense scatter serves `.zarray`/`zarr.json`/chunk reads for `StructureFamily::Sparse` with boundary zero-fill instead of 404); **table-column resolution** — a table's columns are served as array members (see the "Table columns walkable" row below). |
| (core) | Auto-generated node keys | f75d760 + 4d21c3a | Omitted-key create now generates `Uuid::new_v4()` (upstream `Context.key_maker` default, catalog/adapter.py:188); client `create_node`/`create_container` take `Option<&str>`. The former wire-field gap is closed (4d21c3a): `PostMetadataRequest.key` renamed to `id` (serde `alias = "key"` retained for existing Rust clients) to match upstream `PostMetadataRequest.id` (schemas.py:462); the three client create-body sites now emit `"id"`. A real Python server previously ignored the `key` field and auto-named the node. |
| (core) | NotEq missing-key semantics | a866467 | `push_neq` no longer includes rows whose metadata lacks the key (`IS NULL OR` arm removed, both dialects) — matches upstream `attr != value` NULL-exclusion and our own `NotIn`. |
| (core) | Table columns walkable as array nodes | 68ec1c2 + a7855a0 | A dataframe column is addressable as a child array node — upstream `TableAdapter.__getitem__` → `ArrayAdapter.from_array(self.read([col])[col].values)` (adapters/table.py:137). `walk_tree` synthesizes the column-array hop when the final segment names a column of a `Table` node (`core::table_column_as_array`): dtype from the Arrow schema, shape `[nrows]`, concatenated across partitions (float nulls → NaN, bool → u8), absent column → 404. One change reaches every read route (metadata, `/array/full`, `/array/block`, `/zarr/v2`+`/zarr/v3` column URLs). Catalog-backed servers additionally port upstream's `lookup_adapter` fallback (catalog/adapter.py:549-566): `resolve_entry_catalog` (auth gate) and `catalog_metadata_resource` defer to the table's `get(column)` on a final-segment DB miss whose parent is a table, so the gate no longer 404s before `walk_tree` runs. Numeric columns land here; string/temporal in the next row. |
| (core) | String + temporal table column dtypes | c42ae12 + e73209b | Extends table-column walkability to non-numeric Arrow types, mirroring `ArrayAdapter.from_array` on object/datetime pandas columns. **String** (`Utf8`/`LargeUtf8`) → numpy fixed-width unicode `<U{n}` (UCS4/UTF-32-LE, right-padded `U+0000`) where `n` is the longest value over the *concatenated* column; null → literal `"None"` (`str(None)`, array.py:78), empty → `<U0` — the one place a dtype is data-derived rather than schema-fixed. **Temporal** (`Timestamp`/`Date32`/`Date64`) → numpy `datetime64` `<M8[unit]` int64 ticks (Timestamp keeps its unit, Date32 `[D]`, Date64 `[ms]`); null → `i64::MIN` (NaT), tz dropped (numpy `datetime64` is tz-naive — upstream parity). Served by `/array/full`, `text/csv`, and zarr v2; **zarr v3 → 422** (the v3 spec has no fixed-width-unicode / `datetime64` core data type — a clean parity ceiling, where upstream feeds the dtype to `parse_data_type(zarr_format=3)` unguarded at zarr.py:314). Non-numeric/non-string/non-temporal columns (nested) still rejected. |
| (core) | JSON serialization of `U`/`S` arrays | e559716 | `/array/full?format=json` previously errored on unicode/bytes arrays; now matches upstream `safe_json_dump` (serialization/array.py:33-37 → orjson + `default` fallback, utils.py:558-582). **U** (`<U`) → JSON strings via `tolist()` (orjson has no numpy unicode fast-path; trailing NUL padding stripped at code-point level, interior NULs preserved). **S** (bytes) → base64 data URI `data:application/octet-stream;base64,…` (utils.py's first `isinstance bytes` branch wins over the utf-8 branch — S is base64, not text). U decoder shared with the CSV serializer (`decode_u_element`); S diverges by design (CSV renders bytes as utf-8 text). Behaviour traced from source (numpy not installed), not run. |
| (core) | Awkward managed-write backend + `write_awkward` | e35cfbf + ef45a60 + e28bb1c | `structure_family=awkward` nodes are now creatable over managed storage and served back — resolving the former "Awkward writes" N/A. `AwkwardBuffersAdapter` (e35cfbf) persists each buffer one-file-per-form_key to a directory (filename == form_key, raw bytes), matching upstream `DirectoryContainer` (`tiled/storage.py:437-439`) and `AwkwardBuffersAdapter` (`tiled/adapters/awkward.py:93-160`), so a Python tiled server reads the same tree; `init_storage_awkward` mkdirs the directory + registers one `is_directory` asset (`awkward.py:120-138`). Server wiring (ef45a60): `default_creation_mimetype(Awkward)` → `application/x-awkward-buffers` (upstream `DEFAULT_CREATION_MIMETYPE[awkward]`, `catalog/adapter.py:120`; `mimetypes.py:13`), plus `managed_init_storage` + `build_leaf_adapter` awkward arms — the resolver forwards the catalog `ds.structure` (form/length, not stored on disk) into the adapter (`file_resolver.rs:213`→`:406`). Client `ContainerClient::write_awkward` (e28bb1c) mirrors upstream `container.py:942`; the buffer routes (`GET`/`PUT /awkward/full`, `/awkward/buffers`) and `AwkwardClient::write`/`AnyClient::into_awkward` predate this (b9eb33f). Deviations: `read`/`read_buffers` list the directory rather than enumerating buffer keys via the awkward form (no awkward runtime, and the `/awkward/full` contract is buffer-map level); `write` validates form_keys against path traversal (keys arrive from a client ZIP unpacked verbatim). Family coverage of managed writes is now array/table/ragged/awkward/sparse — sparse landed via PR #40 (`default_creation_mimetype` sparse arm at `router.rs:5498`, `managed_init_storage` sparse arm at `:5665`); see the sparse `(core)` row below. |
| (core) | Sparse (COO) managed-write backend + `write_sparse` | 8cd661b + bf67a50 + 3534be1 + b081a90 + e1dfee7 + f42e6c3 | `structure_family=sparse` nodes are now creatable over managed storage and served back. Core (8cd661b): `SparseAdapterWrite` trait (`write`/`write_block`) behind an `as_writable` hook (`src/core/adapters.rs:290`/`:277`), mirroring upstream `SparseBlocksParquetAdapter.write`/`write_block` (`tiled/adapters/sparse_blocks_parquet.py:105`/`:91`). Adapter (bf67a50): each COO block is written to one parquet file, and `init_storage_sparse_parquet` lays out the block directory (`sparse_blocks_parquet_adapter.rs:252`/`:721`), matching upstream `init_storage` (`sparse_blocks_parquet.py:65`) and the `load_block` read helper (`:26`). Server wiring (3534be1): `default_creation_mimetype(Sparse)` → `application/x-parquet;structure=sparse` (upstream `DEFAULT_CREATION_MIMETYPE[sparse]`, `catalog/adapter.py:122`; `mimetypes.py:11`; mimetype→adapter dispatch `catalog/adapter.py:141`) at `router.rs:5498`, plus the `managed_init_storage` sparse arm (`:5665`) and the `file_resolver` sparse arm that reassembles the node from *all* block assets (`file_resolver.rs:206`→`build_sparse_blocks_adapter` `:537`). Server PUT (b081a90): `PUT /array/full` + `/block` accept COO Arrow-IPC bodies (`deserialize_sparse_coo`, `router.rs:1102`), mirroring upstream's sparse deserializer dispatch on `PUT /array/full` (`router.py:2018`/`:2051`). Client (e1dfee7): `ContainerClient::write_sparse` (`container.rs:649`) + `SparseClient::write`/`write_block` (`sparse.rs:138`/`:163`) mirror upstream `client/sparse.py:107`/`:125`. Windows test-root fix (f42e6c3): drop `canonicalize` from sparse test roots (verbatim-path family). Both wave-17 follow-ups have since landed. (a) The untagged-`AnyStructure` Array/Sparse family-ambiguity is fixed structurally (PR #42): `structure` is now parsed under `structure_family` authority everywhere instead of by untagged field-shape guessing — `AnyStructure::from_family_json` is the single family→variant owner (d9e6120, `core/structures.rs:547`), `DataSource`'s `Deserialize` narrows `structure` under that authority (c14eddd) and catalog DB reads narrow through the same owner (61dcb33), `managed_sparse_structure` collapses to a direct `AnyStructure::Sparse` match (873acf1, `router.rs:5729`), and the bare untagged `Deserialize` was removed from `AnyStructure` (eb7d86e — now **Serialize-only**, `core/structures.rs:505`/`:506`), so the mis-parse is unrepresentable by construction rather than worked around. Migration caveat: any sparse row persisted by the *old* untagged path with a non-default `coord_data_type` had that field silently dropped before storage and must be rewritten. A regression test pins this migration case (PR #44, c0dae72): an Array-shaped sparse row (data_type present, no `coord_data_type`/`layout`) still loads under `structure_family = sparse`, re-defaulting the absent coordinate dtype to uint64-LE rather than dropping the structure to `None` (`to_core_data_source_loads_array_shaped_sparse_row`, `catalog/data_source.rs:537`). (b) The sparse read stored-vs-declared dtype coupling is fixed (PR #41, 3dbc068): the read now labels values with the dtype **actually stored** in the parquet column and sizes `nnz` by the stored width (`SparseBlocksParquetAdapter::to_sparse_data`) rather than the node's declared `data_type` — upstream parity with `load_block`, which returns `df["data"].values` at the stored dtype (`sparse_blocks_parquet.py:29`/`:31`); the write-boundary 422 guard (`ensure_sparse_data_dtype`, `router.rs:1177`) remains as defense in depth. The sparse read was further hardened for externally-registered blocks (PR #45): (b0c3a40) `extract_data_bytes`/`read_sparse_parquet` now consult the Arrow null bitmap — a null in a float value column decodes to NaN (`is_null` → NaN, `sparse_blocks_parquet_adapter.rs:630`; upstream pandas parity `sparse_blocks_parquet.py:30-31`, blocks concatenated at `:124`), while a null in an integer value column or any coordinate column is a hard 422 (upstream promotes int+null to float64+NaN, which a typed int buffer and a COO index cannot represent — a deliberate parity ceiling; re-register with a float value dtype to read it); (7134aeb) `arrow_to_builtin_dtype`/`extract_data_bytes` (`:516`/`:565`) widen read coverage to every Int/UInt width (i8/i16, u8–u64) plus dictionary-encoded columns wrapping a supported primitive, all round-tripped through `dyn_ndarray_to_arrow`; Float16/Boolean/Timestamp/Date/Decimal/Utf8 stay 422-rejected (`:509-513`). The widening is read-only — managed writes still store only the four typed-client dtypes. |
| (core) | CSV array-adapter null / empty-cell handling | c05e6a1 + b4d0d10 | `read_csv_array` (`csv_array_adapter.rs:113`) decoded every cell via `arr.value(r)` without the Arrow null bitmap, so an empty cell in a numeric CSV column emitted a garbage int (Arrow infers an int-looking column that contains an empty cell as Int64 + a null bitmap). Now it matches upstream's pandas/dask read (`dask.dataframe.read_csv`, `tiled/adapters/csv.py:290`): (c05e6a1) any null in any column forces float64 output (`use_float`, `:156`) — reproducing pandas' int+missing → float64 promotion plus numpy's whole-array upcast — and the float decode is null-aware (`is_null(r)` → NaN, `:191`); (b4d0d10) an all-empty column, which Arrow infers as `DataType::Null`, is treated by `decide_dtype` (`:235`/`:239`) as a float-promotion signal, so the array becomes float64 with that column all-NaN instead of the previous hard rejection. A file of only blank lines still errors — Arrow infers 0 fields, matching pandas `EmptyDataError`. That wave-19 follow-up — the *table* CSV adapter (`csv_adapter.rs`) reporting an all-empty column as Arrow `Null` in `TableStructure.arrow_schema` (`:61`) rather than upstream's float64 — has since landed (PR #49, 612a701): `promote_null_columns` (`csv_adapter.rs:335`) casts every all-empty `Null` column to nullable `Float64` in both the served schema and the record batches at load, so the schema now matches upstream, which reads the same file with pandas and reports an all-NaN float64 column (`CSVAdapter` → `TableStructure.from_dask_dataframe`, `tiled/adapters/csv.py:57` → `tiled/structures/table.py:44-54`). A *partially*-empty int column is deliberately left `Int64`-with-nulls (a client's `to_pandas()` reads int64+null back as float64/NaN observably, and promoting the stored column would risk >2^53 precision loss), pinned by a regression test. |
| (core) | Float16 array serving (json / csv / zarr v3 scalar) | 2748706 + b9d55f3 | A float16 array (constructible from a `<f2` `.npy` or a 2-byte-float HDF5 dataset) fell through the element serializers' `(kind, itemsize)` match: CSV emitted `"unsupported dtype f2"` in every cell (silent content corruption), JSON hard-errored. Upstream serves it on both paths — `numpy.savetxt(fmt="%s")` prints each element and `safe_json_dump` falls back to `array.tolist()`, which widens each np.float16 to a Python float (`tiled/utils.py:575`). (2748706) Added a `(Float, 2)` arm to both formatters (`serialization/array.rs:178` CSV, `:427` JSON), decoding via `half::f16` and widening f16 → f32 (lossless, 11→24 significand bits); NaN/inf → JSON null like the f4/f8 arms. (b9d55f3) `decode_v3_scalar` (`server/zarr_router.rs:178`) gained the same `(Float, 2)` arm (`:208`), so a 0-d float16 array served over `/zarr/v3` reports its real scalar fill value instead of 0.0. The default `application/octet-stream` array route already served f16 transparently (raw C-buffer + metadata), and the PNG/JPEG/TIFF image serializer now serves it too (PR #51, 9f3030c — see the image-serializer row below). Documented parity ceilings: CSV prints the f32-widened decimal (`0.099975586`) rather than numpy's shortest float16 repr (`0.1`) — same stored value, more digits (no Rust primitive yields numpy's shortest-at-f16 repr); hdf5 f16 re-export stays rejected (needs f16 support in the downstream hdf5 crate); sparse COO f16 stays unreachable behind the sparse adapter's 422 (`sparse_blocks_parquet_adapter.rs:509-513`). Ragged Arrow f16 is **not** a parity gap: upstream ragged registers only the json and zip serializers (`tiled/serialization/ragged.py:70`/`:90`), so there is no upstream Arrow-ragged path to match — tiled-rs's Arrow ragged route is an extension that already errors loudly on f16. |
| (core) | Image serializer numeric-dtype rendering (int / float / complex) | 9f3030c + 2efe74f | The PNG/JPEG/TIFF array serializer dispatched pixels on a `match (kind, itemsize)` with a `_ => data.to_vec()` catch-all, so any dtype without an explicit arm had its raw element bytes reinterpreted as u8 pixels and truncated to `h·w` — a silent 200-OK garbage image. (9f3030c) added the `("f", 2)` float16 arm; (2efe74f) then closed the family structurally: the width-keyed match is replaced by one uniform `decode_numeric_to_f32` → `normalize_floats` path (`serialization/image_array.rs:168`, dispatch at `:105`) that widens **every** numeric kind to f32 before scaling — unsigned/signed int at any width (i32/i64/u64 now render instead of falling through), float 2/4/8, and complex → real part only (matching numpy's imaginary-discarding `astype(numpy.float32)`, `tiled/serialization/array.py:76`). Booleans keep their direct 0/255 map. Non-renderable kinds (U/S/M/m/…) now raise a **loud error** (`image_array.rs:235`) instead of emitting garbage, so the Array HTML serializer falls back to CSV exactly like upstream `serialize_html` (`array.py:143-153`). Behavior change: the former per-width integer scaling (`u1`/`u2`/`u4` and `i1`/`i2` special cases) is folded into the uniform float32 path, toward upstream's uniform treatment. Deliberate (unscheduled) display divergence: `normalize_floats` (`:241`) scales by the array's own min/max, whereas upstream auto-contrasts by `numpy.percentile(1, 99)` then clips to [0, 1] (`array.py:76-80`) — a display-contrast-only difference; upstream's percentile path propagates NaN (any NaN ⇒ all-black), while ours maps non-finite → 0. That wave-20 follow-up has since landed (PR #53, 9cf84d5): npy `datetime64`/`timedelta64` (`M`/`m` descr) now load — see the npy header-parser row below. |
| (core) | npy header parser unified on `from_numpy_str` (datetime64 / timedelta64 + U / V / t) | 9cf84d5 | The npy header parser rejected `<M8[ns]` / `<m8[us]` descrs (`unsupported descr kind: M`) because `parse_descr` was a bespoke duplicate of the crate's canonical `BuiltinDType::from_numpy_str` lacking the `M`/`m` kinds and the `[unit]` bracket split. `parse_header` now parses the descr through `from_numpy_str` (`npy_adapter.rs:374` → `core/dtype.rs:162`) and `parse_descr` is deleted — a single dtype parser for the whole crate. datetime64/timedelta64 `.npy` files now load with their `dt_units` and serve payload bytes verbatim (upstream serves them via `numpy.load`). Collapsing onto the shared `Kind::from_numpy_char` (`dtype.rs:100`) also widens acceptance to `V` (void) and `t` (bit-field) descrs; this is intentional and safe — every serving route is byte-faithful (`/array/full` octet-stream and zarr v2 emit the raw void elements verbatim under a faithful void dtype string, i.e. numpy's own void round-trip) or loud (zarr v3 → 422 in `dtype_v3_name` `zarr_router.rs:145`; JSON → hard `Err` `array.rs:472`; the image serializer → loud error; CSV → a per-cell visible `unsupported dtype` placeholder `array.rs:302`), so no path reinterprets void bytes as numbers. The same collapse corrects Unicode itemsize to bytes (`<U3` → 12 via `dtype.rs:187` `size*4`), matching upstream `BuiltinDtype.from_numpy_dtype`, which stores numpy's `dtype.itemsize` (`structures/array.py:113`); the old char-count made the body-length check (`n × element_size`) reject every real `<U*` `.npy` file, so this **enables a previously-dead path** rather than changing a working one. Open (blocked): zarr v3 still 422s non-numeric dtypes (datetime64 / unicode / bytes) where upstream feeds them to `parse_data_type(zarr_format=3)` — reconciling against upstream's v3 *extension* data types needs a zarr-python wire-format reference before a decision. |
| (core) | Table / Sparse CSV pandas `to_csv` byte parity | 3c16eb2 + abb729b | The table CSV serializer re-serves Arrow IPC through arrow-csv, whose writer diverges from upstream `serialize_csv` = `DataFrame.to_csv(index=False)` (`tiled/serialization/table.py:57-62`) on several column kinds — emitting 200-OK bytes pandas would never produce. (3c16eb2) adds one structural **normalization pass** (`normalize_batches_for_pandas_csv`, `serialization/csv_table.rs:347`) that rewrites each record batch to the pandas-equivalent arrays *before* the writer runs, so arrow's output equals pandas' by construction (no string post-patching). A per-column `ColPlan` (`csv_table.rs:148`) is decided by `plan_column` (`:297`) scanning **all** batches, so a column split across partitions formats uniformly: Boolean → `True`/`False`; integer-with-any-null → Float64 (pandas' int+missing promotion, `5`→`5.0`); float `NaN` → empty (pandas `na_rep=""`); naive Timestamp → space separator with `is_dates_only` date-only when the whole column is midnight and a per-column `{3,6,9}` fractional width (`frac_digits` `:234`, `fmt_datetime` `:270`); Time32/Time64 formatted per element as `datetime.time.isoformat` (`fmt_time` `:287`). Every rule was pinned against a pandas 3.0.3 + pyarrow 25.0.0 oracle running the exact upstream pipeline (reference only, not in the repo). The shared serializer also backs the Sparse family CSV (`sparse.rs:64`/`:70`), so both families are fixed at once. (abb729b) fixes the array-CSV boolean cell to `True`/`False` (`array.rs:237`), matching `numpy.savetxt(fmt="%s")` = `str(np.bool_)` (`array.py:45`); the JSON bool arm stays lowercase (correct JSON). Residual deliberate divergences: a tz-aware Timestamp column keeps arrow's RFC3339 form (reproducing pandas' wall-clock offset needs a tz database, and no tiled-rs adapter emits such a column — unreachable); `time64[ns]` with non-zero sub-microsecond nanoseconds truncates to microseconds (`fmt_time` has no `datetime.time` representation for it — upstream `to_pandas` raises, so no upstream output exists to match). |
| (core) | Catalog write-path parity: recursive DELETE + PATCH `access_blob` | f86d28a + 6aa2fab + e9a3f95 | Three catalog write-path parity fixes. (f86d28a) `DELETE /metadata` gains a `?recursive=` query param (`delete_metadata`, `router.rs:6697`) defaulting to false, mirroring upstream `recursive: bool = Query(False)` (`server/router.py:1980`); the non-empty-container refusal (#503) is now gated `if !recursive` (`router.rs:6723`), matching upstream's `if not recursive` guard that raises `Conflicts` (`catalog/adapter.py:1069-1085`) — the catalog's own `delete_node` was already a cascading recursive delete, so `recursive=true` just skips the empty-check and lets it cascade. (6aa2fab) the client threads `recursive` through `BaseClient::delete(recursive, external_only)` (`client/base.rs:225`) and `Container::delete_contents(recursive, external_only)` (`client/container.rs:693`), forwarding it to each child delete — parameter order mirrors Python `BaseClient.delete(recursive=False, external_only=True)`. (e9a3f95) PATCH `access_blob` now runs through the **same** json-patch / merge-patch step as `metadata` and `specs` before `policy.modify_node` sees it (`patch_metadata`, `router.rs:5997`): the mode dispatch applies `apply_json_patch_field` / `merge_patch_apply` to the stored blob and hands the *result* — not the raw patch document — to the policy, mirroring upstream `apply_json_patch(entry.access_blob, …)` (`router.py:2351`) and the merge-patch path (`:2364-2367`). A null/absent `access_blob` means "no change": no patched blob is produced and the policy is not consulted, so the stored blob is preserved. |
| #1409 | Metadata-revision pagination (`revisions_count`) | 8868a1a | `GET /revisions` set `meta.count = revisions.len()` (the *page* length), so `pagination_links` derived `last_offset = 0` and emitted no `next`/`last` — revisions past the first page were unreachable (upstream #1409, closes #1389). Added `Catalog::count_revisions(node_id)` — a page-independent `SELECT COUNT(*) FROM revisions WHERE node_id = ?` (SQLite/Postgres split mirroring `count_children`) — and fed its total to both `meta.count` and the links; the page still comes from `list_revisions` (already `ORDER BY revision`). Upstream folds count + page into one windowed `COUNT() OVER()` query (`revisions_with_count`, catalog/adapter.py); the separate COUNT is semantically identical (the total is offset/limit-independent) and sidesteps the empty-window edge that forces upstream's own COUNT fallback. |
| #1415 | HDF5 non-string object dtype | 0d082bf | A dataset whose numpy dtype is object `O` but is not a vlen string (a vlen array / HDF5 object reference) can't be read as an array; upstream serves an empty placeholder of the same shape, dtype `S0` (`numpy.empty(ds.shape, dtype="S0")`, hdf5.py:204-235). `from_path` intercepts the `VarLenSequence` datatype CLASS before `dtype_from_hdf5` — parallel to the vlen-string interception (#157) — and materialises an `S0` (zero-width bytes) structure over the shape with an empty buffer; `apply_slice` already returns empty bytes for itemsize 0, so `/array/full`, `/array/block`, and zarr serve it uniformly, plus a warning. Only `VarLenSequence` is reachable — rust-hdf5 has no `Reference` (class 7) `DatatypeMessage` variant, so object references are structurally unreachable from this port. The sibling `dtype_from_hdf5` rejections (FixedString→`S<N>`, Compound, Enum, Array) are distinct numpy kinds, not object dtype, so #1415 does not cover them. |
| (core) | Single-user API key confined to single-user mode + `SINGLE_USER_SCOPES` | 2c317e7 | The static single-user API key (`--api-key` / `TILED_SINGLE_USER_API_KEY`) was a full-admin backdoor when set alongside an auth DB in multi-user mode: `resolve_auth_inner`'s multi-user DB branch had no `else`, so a failed DB lookup fell through to the single-user-key compare and granted `ScopeSet::full()` (uncapped, incl. `admin`). Closed structurally on two axes (PR #57). (a) **Mode exclusivity by construction**: `AppState::enforce_auth_mode_exclusivity` (`server/state.rs:258`), run once in the single `build_app` funnel (`app.rs:30`, WARN "single-user API key ignored: an auth database is configured"), nulls `api_key` whenever an `auth_db` is also present — the invariant `auth_db.is_some()` forces `api_key = None`, so the multi-user fall-through is unreachable rather than runtime-guarded. (b) **Least privilege**: an authenticating single-user key now carries `ScopeSet::single_user()` (`auth/scopes.rs:151`) — the 11 scopes upstream's `SINGLE_USER_SCOPES` grants (`access_control/scopes.py:32-46`: read/write/delete of metadata+data, `create:node`, `register`, `metrics`, read/write `webhooks` — no `admin`, no principal/apikey management) — instead of `full()`. Upstream parity: `get_scopes_from_api_key` returns `SINGLE_USER_SCOPES` only inside the mode-exclusive `if not authenticated` branch (`authentication.py:347`/`:350`). Pinned by `single_user_matches_upstream_single_user_scopes` (`scopes.rs:449`). |
| (core) | `default_login_scopes` defaults to `full()` (role scopes pass through) | 42ef92b | `default_login_scopes` was hardcoded `read_only()` in the CLI and intersected into every credential (`role_scopes.intersect(&state.default_login_scopes)`, `auth_router.rs:96` plus four sibling login/refresh sites), so a stock multi-user server silently capped **every** credential — including `admin` — at `{read:metadata, read:data}`, stripping all write/create/delete scopes and contradicting the field's own doc ("the full set"). The single-source-of-truth owner `AppState::default_login_scopes()` (`server/state.rs:277`) now returns `full()`, making the intersection an identity (`role ∩ full == role`) unless an operator sets a tighter cap; the CLI wires to it so default and doc cannot drift. Upstream parity: role scopes are minted into the session token with no global login cap (`"scp": list(role scopes)`, `authentication.py:856`). **Intentional behavior change** (PR #57): stock multi-user now grants write-capable role scopes by default (was read-only). |
| (core) | `register` scope required on PUT /data_source | 1dfb2da | `PUT /api/v1/data_source` rewrites a node's storage mapping (structure + parameters) but was gated by `WriteData or WriteMetadata`, so a plain `user` (holds write:data/write:metadata, lacks `register`) could repoint any node at different storage. `put_data_source` now requires **both** `write:metadata` and `register` via two sequential `auth.require` after the per-ancestor narrow (`router.rs:6623`/`:6624`), matching upstream `Security(check_scopes, ["write:metadata","register"])` + `get_entry(path, ["write:metadata","register"])` (`router.py:1944`/`:1948`). PR #58. **Same-family follow-up still open**: `delete_metadata` requires only `delete:node` where upstream also requires `delete:revision` — see the auth-hardening backlog below. |
| (core) | Zarr group-listing access filter | b08c60a | Zarr group listings (`/zarr/v2/{container}`, `/zarr/v3/{container}`, and bare-root `/zarr/vN/`) enumerated children via raw `ContainerAdapter::keys()` with no access filter, leaking the names/existence of access-restricted children when `access_policy=Some` — `resolve_zarr_node` gated only the container node, never its child enumeration. Child enumeration now routes through one owner, `filtered_child_keys` (`server/zarr_router.rs:430`), which injects `policy.list_filter(read:metadata)` as a `Query::AccessBlobFilter` into `container.search(&[..])` — the same filtered path `/search` and `/container/full` use. `group_listing` takes `&AuthContext` and both the root (`state.root_tree`) and resolved-container branches go through it; table-column listings are unchanged and `access_policy=None` returns raw `keys()` as before, so no raw-`keys()` path survives for a caller-facing listing. Scope `read:metadata` matches upstream's `filter_for_access` on the resolved entry (`zarr.py:198-207` v2 / `:484-498` v3). PR #59. |

## Already covered (no code change)

| PR | Title | Where it lives in our port |
|----|-------|---------------------------|
| #1025 | KeyPresent queries | `Query::KeyPresent` in `tiled-core/queries.rs`. |
| #1083 | Tolerate missing access_blob | `access_blob: Option<Value>` already optional. |
| #1115 | Default None for optional patch | PUT /data_source takes raw JSON; field implicitly optional. |
| #1286 | ALL_ACCESS sentinel→[] refactor | Decision uses `Option<Value>::None` for "no filter". |
| #960  | SQL LIKE | `Query::Like` already in our enum. |
| #968  | npy adapter | already present. |
| #989  | Zarr v3 | covered by zarrs 0.20. |
| #786  | Admin can delete other users' apikeys | `api_key_revoke` already accepts `Scope::Admin` override. |
| #673  | Correlation ID | `correlation_id_middleware`. |
| #695  | pydantic v2 | N/A — we don't use pydantic. |
| #752  | Safe array slice parser (no eval) | `NDSlice::from_numpy_str` is a pure parser. |
| #746  | In/NotIn empty list | Handled inside #622 port. |
| #1370 | register `--include-ext` is_dir | matched in commit 933c5d0. |
| #1364 / #1343 / #1326 / #1351 / #1339 / #1218 / #287 / #262 | various gaps | initial-port gap commits (`d48e5f4`, `ec00c22`). |
| #564  | confirmation_message on Authenticators | `ProviderInfo.confirmation_message` already passed through to SPA. |
| #1196 / #1197 | Subscription Executor | tokio runtime + bus already covers it. |
| #1391 | Allow empty `sort=` param | `parse_sort` drops truly-empty items (`router.rs:523-525`), test `empty_param_yields_no_sorting` (`:6604`); the `Vec<(String,String)>` parser never had pydantic's strict regex, so the 422 never existed here. |
| #1381 | EntraAuthenticator: code-flow secret, `scp`-absent scopes, human username | `external_oidc.rs`: `client_secret` + code flow (`:121`, `:476`); `translate_scp` grants the union of all mapped scopes when `scp` is absent/empty (`:1008-1013`); `derive_entra_identity` derives a human-readable username (`:872`). |
| #1382 | Downstream-OBO token storage | `exchange_code_flow` stores `entra_access_token`/`entra_refresh_token` in the session `state` claim (`external_oidc.rs:819`, `:968-980`) — the OBO-carrier substrate from #465. |
| #1383 | Readable-username priority + `extra_scopes` for OBO `aud` | Username from nameID/preferred_username/upn/email (`derive_entra_identity`, `external_oidc.rs:872`); `extra_scopes` appended to the token-POST `scope` so the returned `access_token` carries the resource `aud` (`:515`, doc `:126-137`). |
| #1385 | Union token scopes with real role scopes (incl. admin) | `token_scopes ∪ role_scopes` with role from the principal's *actual* role — `for_role` maps `admin`→full (`scopes.rs:159`), unioned at `app.rs:572-576`; refresh re-derives from the current role (`auth_router.rs:156`). External-session tracking + `access_token`-in-Principal are consequences of tiled-rs minting its own OIDC session (see #1384 note), N/A. |

## Deferred (substantive port, tracked)

| PR | Title | Reason |
|----|-------|--------|
| #1320 | Postgres `nodes` parent_id index | We have the index; production-scale planner tuning is the actual issue. |
| #588  | btree_gin (vs plain GIN) | We have plain GIN over jsonb; btree_gin only helps hybrid btree+gin queries. |
| #1271 | Preemptive reshape | Our HDF5 reads `ds.shape()` directly; can't diverge by design. |

Resolved out of Deferred: #1096 (ported, see above); #1378 (confirmed
correct — single-row json-seq framing covered by a unit + HTTP smoke
test, b07dde3, no code change); #802 zarrs write side (implemented in
`zarr_adapter.rs` — `write`/`write_block`/`append`/`patch` over
`store_array_subset`/`set_shape`, with integration tests); #1314
(reclassified **N/A — structurally absent**, no code change). #1314 is
an *open upstream issue* (not a merged PR; upstream HEAD `da03df0f`) with
no fix to port: its 500 is `force_reshape(self._array,
self._structure.shape)` (`tiled/adapters/array.py`, commit `92c890d4`
"#1271") reconciling a DB-cached `structure.shape` against a live
external file a SWMR/zarr writer is growing — any element-count mismatch
raises `ValueError` (`tiled/adapters/utils.py`, same commit). This port
has no such reconciliation: every read resolves a *fresh* leaf adapter
(`FileLeafResolver::resolve`, `src/server/file_resolver.rs:150`) whose
`from_path` reads the shape from the same live file the read then draws
from (`src/adapters/hdf5_adapter.rs:173` builds `structure.shape`, `:746`
feeds it to `read_hdf5_slice`; `src/adapters/zarr_adapter.rs:69` +
`:159`), and metadata is served from that same fresh adapter
(`adapter.structure_json()`, `src/server/core.rs:491`) — structure and
data come from one file open, so no cross-source mismatch can be
constructed. The DB `ds.structure` is never fed to the HDF5/zarr read
path: the resolver forwards it only to the ragged-SQL branch
(`file_resolver.rs:174`) and, since the awkward managed-write landing, the
awkward branch (`:213`→`:406`) — neither of which is an HDF5/zarr adapter.
Same structural reason as #1271 below: our external-array reads take
`ds.shape()`/`array.shape()` directly and can't diverge by design.

#465 "Support Adapters that Interact with Services" (merged upstream
`7e72ee24`; current form in `tiled/server/protocols.py`,
`dependencies.py:68`, `authentication.py:237`/`:857`,
`authn_database/orm.py:223`) splits into two halves; neither remains a
deferred port:

* **Transport substrate — already covered.** #465 added a `Session.state`
  JSON column and embedded it as a JWT `state` claim so per-session
  secrets survive to the read path. This port already carries that
  substrate, built for the Entra on-behalf-of flow (#1364): migration
  `0006_add_session_state` (`src/auth/migrations/{sqlite,postgres}/`)
  adds `sessions.state`, `AccessTokenClaims.state`
  (`src/auth/jwt.rs:40`) embeds it verbatim, and `create_session`
  stores + round-trips it through refresh (`src/auth/session.rs:41`).
* **Adapter-consumption half — N/A (architectural).** The feature's
  point is a config-loaded custom adapter exposing
  `with_session_state(state)`, which upstream reaches by duck typing:
  `get_entry` does `hasattr(entry, "with_session_state")`
  (`dependencies.py:68`) on a Python `Adapter` class named by config
  `object_path`. This port has no such surface: `AnyAdapter` is a
  **closed enum** of six concrete categories (`src/core/adapters.rs:582`)
  and leaf adapters come from a fixed `match mimetype` dispatch over
  built-in file formats (`src/server/file_resolver.rs:253`) — there is
  no runtime custom/plugin adapter loader (`from_config` exists only for
  *authenticators*, not adapters/trees), so no duck-typed `entry` on
  which to call `with_session_state`. The `Authenticator` trait returns
  a `Subject` (`src/auth/authenticator.rs:38`), not a state dict, and
  nothing on the read path consumes the `state` claim to bind an
  adapter. Wiring a `with_session_state` hook now would be dead code:
  there is **no Rust service adapter to consume it** (the stored state
  exists only as an OBO token carrier). Providing the plugin/extension
  system that would give it a consumer is a large, unbounded
  architectural change, not a bounded port — revisit only if/when a
  concrete Rust service-backed adapter lands.

## Auth-hardening backlog (wave-22)

The wave-22 auth-security sweep landed the four fixes in the Ported table
above (single-user-key confinement + `SINGLE_USER_SCOPES`,
`default_login_scopes = full()`, `register` on `PUT /data_source`, and the
zarr group-listing access filter). The same sweep surveyed these further
auth-parity items and confirmed each is **still unimplemented** in this
port — tracked for a later batch, not silent divergences:

* **`delete:revision` on `delete_metadata`** — `DELETE /metadata` requires
  only `delete:node`; upstream gates it with `delete:node` **and**
  `delete:revision`. Same family as the `PUT /data_source` register-scope
  fix above.
* **Sliding-session `session_max_age`** — an absolute session-lifetime cap
  on refresh, not yet ported.
* **Webhook per-node access gate** — webhook routes are not gated by the
  per-node access policy.
* **WebSocket `read:data` scope** — the streaming WS path does not require
  `read:data` for subscribed nodes.
* **Device-code plaintext** — device-authorization code handling.
* **`expires_in` wire rename** — token-response field-name parity.
* **API-key / session count limits** — per-principal `API_KEY` / `SESSION`
  limits.
* **`resolve_entry` scope-set variant** — a multi-scope variant of the entry
  resolver for routes that require several scopes at once.

## N/A (Python-specific or feature not in our port)

A non-exhaustive sample of PRs that don't apply because the corresponding
behaviour lives outside this port:

- **Python tooling**: pyproject/pixi/dask/pydantic updates, alembic
  migrations (we use sqlx), npm frontend dep bumps, sphinx, asv, type
  hints, Docker/helm CI, ruff/black config, `tiled.client` Python-only
  paths.
- **Features we never built**: Composite spec family (#1093, #1119,
  #949, #959); SQL-array adapter
  (#1010, #998); SimpleTiledServer (#1346); mount_node configuration
  (#1348, #970, #971); `tiled register` CLI (#1254, #1260, #1370);
  `read_partition`-style SQL serializer; redis streaming cache (#1192).
- **Python client behaviour**: chunked-response decoders, dask
  conversion, `Context` repr, write_dataframe overloads, profile
  cleanup, pickling, local cache layouts. Our Rust client is a
  separate implementation with its own APIs.
- **Older internal refactors**: Catalog→Tree rename (#21), structure
  consolidation (#537), config-loading rewrites — we built the Rust
  port from the post-rename schema.

If you find a Python PR that you think belongs in "Ported" or "Deferred",
open it and check the file list — most of the alembic/dask/pydantic
churn was infrastructure, not behaviour change.

## Sweep: PR #1381 – #1435 (wave-6, above the #1378 high-water mark)

Prior batches account through **#1378** (the Ported / Already-covered tables
top out at #1374; #1378 is resolved-out-of-Deferred above). This batch swept
every merged upstream PR numbered above #1378 — 24 PRs, #1381 through #1435
(upstream local checkout HEAD `da03df0f` = the #1434 merge; #1435 is a
GitHub-only pixi change). Classification: **5 already covered**, **2 ported**,
**0 deferred / actionable**, **17 N/A**.

* **Already covered (5)** — rows in the Already-covered table: #1381, #1382,
  #1383, #1385 (the Entra/OIDC + role-scope follow-ups to #1364 / #1178 / #465
  were already folded into `external_oidc.rs` / `auth_router.rs` / `app.rs`)
  and #1391 (empty `sort=`).
* **Ported (2)** — rows in the Ported table: **#1415** (HDF5 non-string
  object-dtype `S0` placeholder, `0d082bf`) and **#1409** (metadata-revision
  total-count pagination — a real defect: multi-page revision listings were
  uncapped-count and un-pageable; `8868a1a`).
* **Deferred / actionable (0)** — both former actionable items from this sweep
  are now ported.

N/A batch (17), each with the structural reason it does not apply:

* **Python client — separate Rust client with its own APIs (7):** #1400
  (retries + progress bar), #1395 (httpx connection cap), #1406 (no-retry on
  unsupported protocol), #1408 (client raises on non-JSON-object metadata
  update), #1411 (skip re-auth in `from_context`), #1418 (whoami auth-link
  check), #1386 (`TILED_API_KEY` CLI auth).
* **Python tooling / frontend (4):** #1435 (pixi ARM64 + `ragged` dep), #1392
  (WebUI catalog pagination / table UX — our SPA is a separate, link-based
  paradigm), #1397 (sphinx-click CLI-docs compat), #1396 (client logger +
  server `logging_config.py` cleanup — Python logging config).
* **Structurally absent in this port (6):**
  * **#1434** — guards the `array-ref` streaming-cache update in
    `put_data_source` against non-array nodes (upstream `KeyError: 'shape'`).
    tiled-rs emits a shape-free bus `DataAppended { partition: None }`
    uniformly across all structure families (`router.rs:6335`) and has no
    redis `streaming_cache` (#1192, N/A) — the bug cannot be constructed.
  * **#1429** — widens `assets.size` int32→int64 (PostgreSQL overflow on
    files >2.1 GB). tiled-rs's `assets` table has no `size` column at all
    (`migrations/{sqlite,postgres}/0001_initial.sql`), so the overflow can't
    occur.
  * **#1424** — strips `Asset.size` from responses for pre-v0.2.13
    `python-tiled` clients whose dataclass lacks the field. tiled-rs carries
    no `size` field to strip, and the target is the Python client's dataclass
    strictness.
  * **#1384** — provides the OAuth2 `scope` to the IdP token endpoint on
    refresh (Entra refresh follow-up to #1381). tiled-rs's refresh always
    re-mints via its own `/auth/refresh` and never re-calls the IdP token
    endpoint (deliberate divergence, `router.rs:281-284`), so there is no IdP
    refresh call to attach scopes to.
  * **#1402** — deterministic row ordering / `order_by_args` / `primary_key`
    for the *writable* SQL storage adapter (`_partition_id` / `_dataset_id`
    INSERT machinery, `tiled/adapters/sql.py`). tiled-rs carries no such
    write-side SQL adapter (#1010 / #998, N/A); its ragged-SQL path is a
    read-only `sqlite://` resolver.
  * **#1394** — internal `NDSlice` helper methods (`chunk_indices`,
    `block_for_slice`, `build_nested_grid`) consumed only by the Python ragged
    adapter's `read()`→`read_block()` slice decomposition; no wire/behaviour
    change, and tiled-rs resolves slices in its own read path.

## Older-PR sweep (PR #1 - #465)

Surveyed the entire 235-PR backlog from upstream's first commit
(2021-02-26 #1 "benchmarks") through #465 (2023-07-11 "Service
Adapters") via title-and-body skim. Findings:

* **~95% of older PRs are foundational Python work** that predates the
  Rust port's 2024 schema baseline: catalog→Tree rename (#21), OAuth2
  setup (#4), sliding sessions (#13), early server-side caching (#65),
  Tree refactors, pydantic-versioned wire formats, dask integration,
  alembic migrations, FastAPI/starlette adapters, npm frontend, sphinx
  docs, Docker/CI/lint config, type hints. Mark all N/A.
* **Already covered by our enum/handlers** (no separate row above):
  #161 sorting API, #218 binary-operator queries (Comparison),
  #233 delete route, #234 In/NotIn (covered by #622), #238 NotEq,
  #239 `__ne__`, #266 metadata revision history, #307 specs/family
  search, #266/188 writing scopes, #161/229 Arrow tables, #210
  metadata path-parts, #318 AccessControlPolicy reshape (we have
  AccessPolicy), #356 enriched specs, #358 v1 HTTP API lock-down,
  #379 generic-sequence queries (In/NotIn), #422 client version,
  #463 catalog deletion (we have it; #503 layered the empty-check).
* **One actionable port**: #143 RGB TIFF (see Ported table). Our
  TiffAdapter was hardcoded to `[h, w]` u16 grayscale.
* **Adapter improvements**: #157 vlen-string HDF5 datasets — ported
  (0be4d0e, see Ported table). #127 SWMR-mode HDF5 reads —
  documented-unsupported (6bf3689): rust-hdf5 exposes no `swmr`/`libver`.
  Still deferred: #394 HDF5 inlined-contents env var (needs an HDF5
  *container* adapter plus a server-side inlined-contents mechanism,
  neither of which exists in this port), #62 file→multi-node mapping.

The full sweep filter:

```sh
gh pr list --repo bluesky/tiled --state merged --limit 1000 --json number,title --jq '
  .[] | select(.number < 465) | "#\(.number) | \(.title)"
'
```

## Re-running the audit

```sh
gh pr list --repo bluesky/tiled --state merged --limit 1000 \
    --json number,title,mergedAt --jq 'sort_by(.mergedAt) | reverse' \
  | python3 -c '...'   # group by month / status
```

The high-water mark for "audited" is the most recent merged PR plus
this file's coverage of older ones. New audit batches typically only
need to look at PRs merged since the last commit timestamp on this
file.
