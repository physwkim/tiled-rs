//! MongoDB adapter for Bluesky experiment data.
//!
//! Reads from the "normalized" MongoDB layout used by databroker:
//! collections: run_start, run_stop, event_descriptor, event, resource, datum.
//!
//! Supports both inline data (scalars in MongoDB) and external file references
//! (Area Detector HDF5, NPY sequences, TIFF images) via the handler/filler system.

pub mod array_col;
pub mod catalog;
pub mod filler;
pub mod handler;
pub mod run;
pub mod stream;

pub use catalog::MongoCatalog;
pub use handler::HandlerRegistry;
