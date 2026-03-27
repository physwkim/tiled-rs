# tiled-rs

Rust port of [Tiled](https://github.com/bluesky/tiled) — a structured scientific data access service from Brookhaven National Laboratory.

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

```
tiled-rs/
├── crates/
│   ├── tiled-core           # Types, traits, schemas, wire format
│   ├── tiled-adapters       # MapAdapter, ArrayAdapter (in-memory)
│   ├── tiled-serialization  # Serialization registry (octet-stream, CSV, Arrow IPC)
│   ├── tiled-server         # Axum HTTP server (5 API + 2 operational endpoints)
│   └── tiled-cli            # CLI (serve --demo)
├── benchmarks/              # Python vs Rust comparison scripts
└── src/main.rs              # Entry point
```

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
