//! Core library for the distributed file system.
//!
//! This crate provides **pure**, deterministic logic for:
//! - **Erasure coding**: Reed-Solomon encode/decode of byte payloads.
//! - **Data placement**: Rendezvous hashing to assign shards to nodes.
//! - **Membership**: Ring versioning and health tracking.
//! - **Block metadata**: Checksum computation and integrity verification.
//! - **Shared types**: Ring, ErasureConfig, StorageClass, NodeId, etc.
//!
//! All functions are synchronous and side-effect-free. This makes it
//! possible to use this crate inside both production Tokio runtimes and
//! [turmoil](https://crates.io/crates/turmoil) deterministic simulations
//! without any conditional compilation.
//!
//! # Example: encode, place, and decode
//!
//! ```
//! # #![allow(clippy::unwrap_used)] // doc-test convenience
//! use dfs_core::types::*;
//! use dfs_core::placement;
//!
//! let config = ErasureConfig { k: 3, m: 2 };
//! let shards = dfs_core::erasure::encode(b"my payload", &config).unwrap();
//!
//! // Place shards on a 5-node ring.
//! let ring = Ring::new(vec![
//!     uuid::Uuid::new_v4(),
//!     uuid::Uuid::new_v4(),
//!     uuid::Uuid::new_v4(),
//!     uuid::Uuid::new_v4(),
//!     uuid::Uuid::new_v4(),
//! ]);
//! let placements = placement::place("my-object",
//!     StorageClass::new(3, 2), &ring).unwrap();
//!
//! // dispatch to nodes ...
//!
//! // Decode: just k shards are enough.
//! let subset: Vec<_> = shards.iter().take(3).map(|s| Some(s.clone())).collect();
//! let original = dfs_core::erasure::decode(subset, &config).unwrap();
//! assert_eq!(&original[..], b"my payload");
//! ```

pub mod types;
pub mod error;
pub mod block;
pub mod erasure;
pub mod placement;
pub mod membership;

// Re-export commonly used items.
pub use types::{NodeId, ObjectId, ShardIndex, Ring, RingVersion, ErasureConfig, StorageClass};
pub use error::{CoreError, Result};
pub use block::{BlockId, BlockMeta, Shard};