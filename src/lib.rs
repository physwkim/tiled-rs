//! tiled-rs — a Rust port of [Tiled](https://github.com/bluesky/tiled), a
//! structured scientific data access service.
//!
//! This crate is organised as one module per former workspace crate. The
//! dependency layering that used to be enforced across crate boundaries is
//! preserved as a module layering here:
//!
//! ```text
//! core
//!  ├─ serialization ─┐
//!  ├─ auth ──────────┤
//!  ├─ catalog ───────┤
//!  ├─ mongo          │
//!  ├─ adapters ◀─────┘ (serialization)
//!  ├─ access ◀── auth
//!  ├─ web ◀── auth        (feature = "web")
//!  ├─ server ◀── adapters, serialization, catalog, auth, access, web
//!  ├─ client ◀── serialization, server, …
//!  └─ cli ◀── server, web, mongo, …
//! ```

pub mod access;
pub mod adapters;
pub mod auth;
pub mod catalog;
pub mod cli;
pub mod client;
pub mod core;
pub mod env_expand;
pub mod mongo;
pub mod serialization;
pub mod server;

#[cfg(feature = "web")]
pub mod web;
