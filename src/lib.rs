//! High-frequency sensor time-series storage:
//! SQLite catalog + immutable XOR-compressed segment files.
//!
//! See `docs/FORMAT.md` for the on-disk format and semantic guarantees.

pub mod api;
pub mod bits;
pub mod catalog;
pub mod codec;
pub mod error;
pub mod model;
pub mod segment;
pub mod store;

pub use error::{Error, Result};
pub use store::Store;
