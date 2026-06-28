# Changelog

All notable changes to **tiled-rs** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This is the first changelog for the project; the `[0.1.1]` entry summarizes the
work merged on the `catalog-m4-async-container` branch since the `v0.1.0` tag
(124 commits). Earlier history is not retroactively itemized.

## [0.1.1] - 2026-06-28

This release fills in the **write path**, a set of new **read adapters** and
**export serializers**, and the **authentication / access-control** surface,
bringing the server and client substantially closer to Python `tiled` parity.

### Added

#### Write path (server + client)

- **Array writes**: NPY managed-array write subsystem (`init_storage` + `PUT
  /array/full`); multi-chunk zarr managed-array writes (Python-parity default);
  partial writes via `PUT /array/block` (split `write` vs `write_block`);
  appendable zarr arrays via `PATCH /array/full` (grow + catalog sync).
- **`ArrayAdapterWrite::patch`** trait method — slice-write with extend; zarr
  and `/array/full` implementations do faithful slice-write + extend/resize.
- **Table writes**: `PUT /table/full` (CSV backend); opt-in parquet table write
  (parity default for tables); `PUT`/`PATCH /table/partition`.
- **Ragged / Awkward writes**: writable SQLite chunk store and `RaggedSQLAdapter`
  (read/write/patch) wired into managed storage and the create/resolver path;
  ragged write routes (`PUT`/`PATCH /ragged/full`, `PUT /ragged/block`); Awkward
  core (`as_writable`/`as_awkward_arc`), in-memory adapter, serializer
  (`application/zip` identity), and server routes; Awkward buffer codec exposed
  for the write path.
- **Client write API**: `Context` `put_bytes`/`patch_bytes` helpers;
  `ArrayClient` write/write_block/`patch` (slice-write + extend), export;
  `TableClient` write/`write_partition`/`append_partition`, export;
  `RaggedClient` write/write_block/patch; `AwkwardClient::write` +
  `AnyClient::into_awkward`; base `delete`/`patch_metadata`; container
  `create`/`delete_contents` + `AnyClient::base`; end-to-end write tests.

#### Read adapters

- NetCDF-3 (classic) via the pure-Rust `netcdf3` crate.
- NetCDF-4 container adapter via `rust-hdf5`.
- `SparseBlocksParquet` read adapter.
- `SqlTableAdapter` (SQLite) SQL table read adapter.
- `CsvArrayAdapter` (CSV-as-array) read adapter.
- Excel (`.xlsx`) table read adapter via `calamine`.
- Arrow / Feather IPC table read adapter.

#### Export serializers

- **HDF5**: one 1-D dataset per table column; container deep-export in
  `container_full`; node metadata written as HDF5 attributes; homogeneous
  JSON metadata arrays mapped to 1-D HDF5 array attributes, and nested
  rectangular arrays to N-D array attributes (h5py `numpy.asarray` parity;
  ragged / mixed-kind / nested-object / null values fail-fast, mirroring
  Python's `TypeError → SerializationError`). Shared dataset writers factored
  into `hdf5_common`; `hdf5-serializer` feature wired into server + CLI.
- Sparse `.xlsx` spreadsheet serializer.
- **blosc2** content-encoding: server middleware + client `Accept-Encoding`
  negotiation and decode.

#### Authentication & access control

- **OIDC** authorization-code flow: `OidcProvider` config with PKCE/state store,
  `/authorize` redirect + `/callback` code exchange (#1178); providers wired
  from config with well-known discovery; Entra identity mapping (uuid5 subject +
  username derivation); on-behalf-of session state (persist upstream tokens,
  embed in access JWT); IdP-brokered OAuth2 device-code flow; DB-backed PKCE
  flow store (multi-process safe); `end_session_endpoint` advertised as logout;
  login-capable external providers advertised in `About`; `offline_access` +
  Entra `extra_scopes` plumbing.
- **SAML 2.0** SP-init: `SamlConfig` + `SamlProvider` `AuthnRequest` redirect,
  ACS route with signature validation + session mint, `/login` + `/acs` routes.
- **LDAP** authenticator (`ldap3`, pure-Rust) and **PAM** authenticator
  (portable libpam FFI), wired into config + `AppState`.
- **Access policy**: `init_node`/`modify_node` added to the `AccessPolicy` trait
  and wired into the router; `TagBasedPolicy` backed by a SQLite/Postgres tag
  store (migration 0005 + per-tag scope methods) with per-tag scope resolution,
  `is_tag_defined` validation on create/PATCH, and an `unremovable_scopes`
  self-lockout guard; `access_tags` on API keys + `authn_access_tags` filter,
  enforced on direct node access (not just listings).
- Additional principal / session / API-key auth endpoints;
  `AuthDb::get_principal_by_uuid`; `authentication.tiled_admins` bootstrap;
  `secret_keys`, `access_token_max_age`, and `refresh_token_max_age` honoured
  from config.

#### Configuration & HTTP

- Honour `catalog.writable_storage` / `catalog.readable_storage`,
  `catalog_pool_size` (→ sqlx `max_connections`), and `exact_count_limit`
  (→ search `meta.count` cap) from config; warn on unmodelled config keys.
- `ETag` emission + `If-None-Match` handling (304) on JSON responses.
- `?fields=` projection honoured on `/search`.

### Changed

- Streaming WebSocket route converged on the upstream `/stream/single/{path}`
  path.
- `ArrayClient` patch is now slice-write + extend; the old `append` shape was
  dropped.
- `rust-hdf5` dependency bumped `0.2.20` → `0.2.24` → `0.2.26` → `0.2.27`
  (array-attribute and threadsafe support; lockfile).

### Fixed

- **Catalog / search**: real FTS5 full-text search on SQLite (M2); dotted nested
  keys honoured in the in-memory matcher (M3); `MapAdapter` query semantics
  matched to Python in-memory behavior; managed `file://` asset files reclaimed
  on delete (M5).
- **Managed-asset safety** (S2): reject out-of-storage managed assets at register
  time (source side); contain managed-asset physical delete to storage dirs.
- `allow_origins` honoured from YAML config (was CLI-flag-only).
- Reject `Regex` query on the SQLite catalog instead of returning unfiltered
  rows.
- `parse_structure_family` now accepts `ragged` (was 422 on every ragged catalog
  node).
- SAML signature validation on libxml2 2.15.x (collapse dual-libxml2 link;
  arch-aware libxml2 link override for Apple Silicon + Intel).
- Drop the query string from the request trace span (L2).
- Keep the OIDC test `TempDir` alive to fix a `SQLITE_CANTOPEN` flake.

[0.1.1]: https://github.com/physwkim/tiled-rs/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/physwkim/tiled-rs/releases/tag/v0.1.0
