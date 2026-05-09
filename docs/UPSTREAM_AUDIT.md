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
  family this port doesn't carry — Composite, Awkward writes, etc.).

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
| #802  | Appendable arrays (interface) | cd061f9 + a2c8269 | `ArrayAdapterRead::as_writable()` + `ArrayAdapterWrite::append()` default-impl; `PATCH /api/v1/array/full/{path}?append_along=N` route + `data-appended` event. zarrs write integration is the next step (separate task — adapter-level work). |
| #944  | Accept scalar / empty data in HDF5 | 138fa4b | Zero-rank promoted to `shape=[1]`; empty arrays returned as zero-byte buffers. |
| #972  | drop_revision flag | 138fa4b | `?drop_revision=true` on PATCH metadata. |
| #1084 | access_blob on revisions | 70bcf47 | Migration 0003 + ORM + insert. |
| #1176 | Stream metadata-update events | 559290f | `MetadataUpdated` kind already emitted; added `specs` field to payload. |
| #1178 | OAuth /authorize endpoint (false-advertise removal) | 559290f | Stopped advertising a non-functional `oidc` provider in `/api/v1/`'s providers list. Full code-flow port is deferred (needs OidcProvider extension). |
| #1302 | Multi-chunk `?block=` range | e3660db | `BlockSpec::Range` + cartesian-product walk + ND row-major copy; 7 unit tests. Single-chunk fast-path preserved. |
| #143  | RGB TIFF support | (this batch) | Color TIFFs were silently coerced to grayscale + u16; now `[h, w, channels]` shape with native dtype (u8/u16/u32/f32/f64). |

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

## Deferred (substantive port, tracked)

| PR | Title | Reason |
|----|-------|--------|
| #1096 | Postgres approximate length for large containers | `pg_class.reltuples` estimate — premature without 50M+ row workload. |
| #1018 | Background-task lifecycle | Our webhook_dispatch spawn isn't registered with axum's graceful shutdown. Soft issue. |
| #1314 | External-array shape race during streaming | Race between SWMR/zarr writer + cached reader metadata. Complex to fix deterministically. |
| #1320 | Postgres `nodes` parent_id index | We have the index; production-scale planner tuning is the actual issue. |
| #588  | btree_gin (vs plain GIN) | We have plain GIN over jsonb; btree_gin only helps hybrid btree+gin queries. |
| #1271 | Preemptive reshape | Our HDF5 reads `ds.shape()` directly; can't diverge by design. |
| #1378 | json-seq single-row response | Likely N/A but worth a smoke test before changing anything. |
| #802 (zarrs write side) | Appendable Zarr — actual write impl | Trait + route landed; zarrs write integration is a separate adapter-level task. |
| #465  | Adapters that interact with services | Large feature surface; likely many sub-PRs. |

## N/A (Python-specific or feature not in our port)

A non-exhaustive sample of PRs that don't apply because the corresponding
behaviour lives outside this port:

- **Python tooling**: pyproject/pixi/dask/pydantic updates, alembic
  migrations (we use sqlx), npm frontend dep bumps, sphinx, asv, type
  hints, Docker/helm CI, ruff/black config, `tiled.client` Python-only
  paths.
- **Features we never built**: Composite spec family (#1093, #1119,
  #949, #959); Awkward writing/buffer routes; SQL-array adapter
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
* **Adapter improvements deferred**: #127 SWMR-mode HDF5 reads,
  #157 vlen-string HDF5 datasets, #394 HDF5 inlined-contents env var,
  #62 file→multi-node mapping. All adapter-internal work; tracked in
  the deferred list above.

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
