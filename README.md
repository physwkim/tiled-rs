# tiled-rs

[![CI](https://github.com/physwkim/tiled-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/physwkim/tiled-rs/actions/workflows/ci.yml)

Rust port of [Tiled](https://github.com/bluesky/tiled) — a structured scientific data access service from Brookhaven National Laboratory.

Builds and tests on Linux, macOS, and Windows with the default, pure-Rust feature set (rustls TLS, bundled SQLite, pure-Rust HDF5/NetCDF/Excel/Mongo client).

## Benchmark: Python Tiled vs tiled-rs

Identical demo data served from both servers on the same machine.
100 requests per endpoint, median latency reported.

| Endpoint | Python (ms) | Rust (ms) | Speedup |
|----------|----------:|----------:|--------:|
| about | 1.53 | 0.20 | **7.6x** |
| root metadata | 4.90 | 0.18 | **26.7x** |
| array metadata (small) | 4.58 | 0.19 | **23.5x** |
| array metadata (100k) | 4.48 | 0.19 | **23.5x** |
| search root | 5.46 | 0.21 | **26.4x** |
| array block 800B | 5.07 | 0.19 | **26.8x** |
| array block 800KB | 5.57 | 0.35 | **16.1x** |
| array block 8MB | 10.62 | 3.23 | **3.3x** |
| nested metadata | 4.52 | 0.19 | **23.3x** |
| search paginated | 5.37 | 0.20 | **26.6x** |

- Metadata/search paths: **23–27x** faster (Python ~5ms overhead → Rust 0.2ms)
- Small data transfer: **27x** faster (framework overhead dominant)
- Large data transfer: **3.3x** faster (I/O bound, still significant)
- p99 latency: Rust ≤0.4ms vs Python ~6ms (stable tail latency)

Environment: Python Tiled 0.2.8 / tiled-rs 0.1.0, rustc 1.94.0, macOS, in-memory adapters, single-client sequential requests.

### Run the benchmark

```bash
# Build release binary
cargo build --release

# Run comparison (requires Python Tiled installed)
python3 benchmarks/bench.py
```

## Quick Start

```bash
cargo run --release -- serve --demo
# Server starts on http://localhost:8000

curl http://localhost:8000/api/v1/           # About
curl http://localhost:8000/api/v1/metadata/  # Root metadata
curl http://localhost:8000/api/v1/search/    # Browse entries
```

## Project Structure

A single crate (`tiled-rs`, library name `tiled_rs`) that builds the `tiled`
binary. Each subsystem is a module under `src/`:

```
tiled-rs/
├── src/
│   ├── lib.rs            # Library crate root (module declarations)
│   ├── main.rs           # Binary entry point (the `tiled` CLI)
│   ├── core/             # Types, traits, schemas, wire format, queries
│   ├── serialization/    # Serialization registry (JSON, CSV, Arrow IPC, Parquet, HDF5, ...)
│   ├── catalog/          # SQL-backed catalog (nodes, adapter, search, migrations)
│   ├── adapters/         # Format adapters (CSV, Parquet, Arrow, HDF5, Zarr, NPY, Excel, ...)
│   ├── auth/             # Authentication (JWT, OIDC, LDAP, SAML, PAM, API keys, sessions)
│   ├── access/           # Per-node access policies
│   ├── server/           # Axum HTTP server (router, app, state, file resolver)
│   ├── web/              # Web admin UI / SPA serving (feature `web`)
│   ├── client/           # Async Rust client for the Tiled HTTP API
│   ├── cli/              # CLI (serve, config) and the production app builder
│   └── mongo/            # MongoDB streaming file handlers
├── crates/
│   └── tiled-web-spa/    # WASM single-page app (separate crate, built by build.rs)
├── benchmarks/           # Python vs Rust comparison scripts
└── tests/                # Integration tests
```

All pure-Rust features are enabled by default; only `saml` and `pam` (which
link system C libraries and are unavailable on Windows) are opt-in.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /ready` | Readiness probe |
| `GET /api/v1/` | Server info |
| `GET /api/v1/metadata/{path}` | Node metadata |
| `GET /api/v1/search/{path}` | Browse/search container |
| `GET /api/v1/array/block/{path}` | Array block data |
| `GET /api/v1/table/partition/{path}` | Table partition data |

## License

BSD-3-Clause
