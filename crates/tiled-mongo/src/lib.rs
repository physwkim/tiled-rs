//! MongoDB adapter for Bluesky experiment data.
//!
//! Reads from the "normalized" MongoDB layout used by databroker:
//! collections: run_start, run_stop, event_descriptor, event, resource, datum.

pub mod catalog;
pub mod run;
pub mod stream;
pub mod array_col;

pub use catalog::MongoCatalog;
