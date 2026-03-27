"""Start a Python Tiled server with demo data matching tiled-rs --demo."""
import sys
import numpy as np
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.array import ArrayAdapter
from tiled.config import Authentication
from tiled.server.app import build_app
import uvicorn

def build_demo_tree():
    # Match tiled-rs demo tree exactly
    small_1d = ArrayAdapter.from_array(
        np.arange(100, dtype="float64") * 0.1,
        metadata={"description": "A 1D array of 100 floats"},
    )
    medium_2d = ArrayAdapter.from_array(
        (np.arange(200, dtype="float64") * 0.5).reshape(10, 20),
        metadata={"description": "A 10x20 array of floats"},
    )
    spectrum = ArrayAdapter.from_array(
        np.arange(50, dtype="float64"),
        metadata={"element": "Cu", "edge": "K"},
    )
    sample_data = MapAdapter(
        {"spectrum": spectrum},
        metadata={"sample": "copper_foil"},
    )

    # Add larger arrays for more realistic benchmarks
    large_1d = ArrayAdapter.from_array(
        np.random.randn(100_000).astype("float64"),
        metadata={"description": "100k element array"},
    )
    large_2d = ArrayAdapter.from_array(
        np.random.randn(1000, 1000).astype("float64"),
        metadata={"description": "1000x1000 array"},
    )

    tree = MapAdapter(
        {
            "small_1d": small_1d,
            "medium_2d": medium_2d,
            "sample_data": sample_data,
            "large_1d": large_1d,
            "large_2d": large_2d,
        },
        metadata={"description": "Tiled demo catalog"},
    )
    return tree

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    tree = build_demo_tree()
    auth = Authentication(allow_anonymous_access=True)
    app = build_app(tree, authentication=auth)
    print(f"Python Tiled server starting on port {port}")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
