#!/usr/bin/env python3
"""
Benchmark: Python Tiled vs tiled-rs

Starts both servers with identical demo data, then runs the same HTTP
requests against each and compares latency and throughput.
"""
import subprocess
import sys
import time
import statistics
import json

PYTHON = "/Users/stevek/mamba/envs/daq/bin/python3"
RUST_BIN = "/Users/stevek/codes/beamalignment/tiled-rs/target/release/tiled"
BENCH_DIR = "/Users/stevek/codes/beamalignment/tiled-rs/benchmarks"

PY_PORT = 9000
RS_PORT = 9001
N_WARMUP = 5
N_REQUESTS = 100

# Endpoints to benchmark (path, description)
ENDPOINTS = [
    ("/api/v1/", "about"),
    ("/api/v1/metadata/", "root metadata"),
    ("/api/v1/metadata/small_1d", "array metadata (small)"),
    ("/api/v1/metadata/large_1d", "array metadata (100k)"),
    ("/api/v1/search/", "search root"),
    ("/api/v1/array/block/small_1d?block=0", "array block 100 f64 (800B)"),
    ("/api/v1/array/block/large_1d?block=0", "array block 100k f64 (800KB)"),
    ("/api/v1/array/block/large_2d?block=0,0", "array block 1Mx f64 (8MB)"),
    ("/api/v1/metadata/sample_data/spectrum", "nested metadata"),
    ("/api/v1/search/?page[offset]=0&page[limit]=5", "search paginated"),
]


def wait_for_server(port, timeout=15):
    """Wait until server responds on the given port."""
    import urllib.request
    import urllib.error
    start = time.time()
    while time.time() - start < timeout:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/v1/", timeout=1)
            return True
        except (urllib.error.URLError, ConnectionRefusedError, OSError):
            time.sleep(0.2)
    return False


def bench_endpoint(port, path, n_warmup, n_requests):
    """Benchmark a single endpoint and return latencies in ms."""
    import urllib.request
    url = f"http://127.0.0.1:{port}{path}"

    # Warmup
    for _ in range(n_warmup):
        try:
            urllib.request.urlopen(url, timeout=10).read()
        except Exception:
            pass

    latencies = []
    total_bytes = 0
    for _ in range(n_requests):
        start = time.perf_counter()
        try:
            resp = urllib.request.urlopen(url, timeout=10)
            data = resp.read()
            elapsed = (time.perf_counter() - start) * 1000  # ms
            latencies.append(elapsed)
            total_bytes += len(data)
        except Exception as e:
            latencies.append(float("inf"))

    return latencies, total_bytes


def print_results(name, py_lats, rs_lats, py_bytes, rs_bytes):
    """Print comparison for one endpoint."""
    py_med = statistics.median(py_lats)
    rs_med = statistics.median(rs_lats)
    py_p99 = sorted(py_lats)[int(len(py_lats) * 0.99)]
    rs_p99 = sorted(rs_lats)[int(len(rs_lats) * 0.99)]
    speedup = py_med / rs_med if rs_med > 0 else float("inf")

    print(f"  {'Python':>10s}  median={py_med:7.2f}ms  p99={py_p99:7.2f}ms  bytes={py_bytes:>10,}")
    print(f"  {'Rust':>10s}  median={rs_med:7.2f}ms  p99={rs_p99:7.2f}ms  bytes={rs_bytes:>10,}")
    print(f"  {'Speedup':>10s}  {speedup:5.1f}x")


def main():
    print("=" * 70)
    print("  Python Tiled vs tiled-rs Benchmark")
    print(f"  Warmup: {N_WARMUP} requests, Measure: {N_REQUESTS} requests each")
    print("=" * 70)

    # Start Python Tiled server
    print("\nStarting Python Tiled on port", PY_PORT, "...")
    py_proc = subprocess.Popen(
        [PYTHON, f"{BENCH_DIR}/python_server.py", str(PY_PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not wait_for_server(PY_PORT):
        print("ERROR: Python Tiled server failed to start")
        py_proc.kill()
        sys.exit(1)
    print("  Python Tiled ready.")

    # Start Rust Tiled server
    print("Starting tiled-rs on port", RS_PORT, "...")
    rs_proc = subprocess.Popen(
        [RUST_BIN, "serve", "--demo", "--port", str(RS_PORT), "--host", "127.0.0.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not wait_for_server(RS_PORT):
        print("ERROR: tiled-rs server failed to start")
        py_proc.kill()
        rs_proc.kill()
        sys.exit(1)
    print("  tiled-rs ready.")

    print(f"\n{'─' * 70}")

    results = []
    for path, desc in ENDPOINTS:
        print(f"\n▸ {desc}")
        print(f"  {path}")

        py_lats, py_bytes = bench_endpoint(PY_PORT, path, N_WARMUP, N_REQUESTS)
        rs_lats, rs_bytes = bench_endpoint(RS_PORT, path, N_WARMUP, N_REQUESTS)

        print_results(desc, py_lats, rs_lats, py_bytes, rs_bytes)

        results.append({
            "endpoint": desc,
            "path": path,
            "python_median_ms": round(statistics.median(py_lats), 2),
            "rust_median_ms": round(statistics.median(rs_lats), 2),
            "speedup": round(statistics.median(py_lats) / max(statistics.median(rs_lats), 0.001), 1),
        })

    # Summary table
    print(f"\n{'═' * 70}")
    print(f"  {'Endpoint':<35s} {'Python':>9s} {'Rust':>9s} {'Speedup':>8s}")
    print(f"  {'─' * 35} {'─' * 9} {'─' * 9} {'─' * 8}")
    for r in results:
        print(f"  {r['endpoint']:<35s} {r['python_median_ms']:>7.2f}ms {r['rust_median_ms']:>7.2f}ms {r['speedup']:>6.1f}x")
    print(f"{'═' * 70}")

    # Cleanup
    py_proc.terminate()
    rs_proc.terminate()
    py_proc.wait()
    rs_proc.wait()


if __name__ == "__main__":
    main()
