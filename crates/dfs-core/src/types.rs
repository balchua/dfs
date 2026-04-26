//! Core types for the distributed file system.
//!
//! This module provides the foundational types used throughout the DFS,
//! including node and object identification, ring state, erasure coding
//! configuration, and storage class definitions.
//!
//! All types in this crate are deterministic — given the same inputs,
//! operations produce the same outputs. This is essential for turmoil-based
//! simulation testing.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a storage node in the cluster.
pub type NodeId = Uuid;

/// Unique identifier for an object stored in the DFS.
pub type ObjectId = String;

/// Index of a shard within an erasure-coded stripe.
/// Range: `0..(k+m-1)` where data shards are `0..k-1` and parity shards are `k..k+m-1`.
pub type ShardIndex = u8;

/// Monotonically increasing ring version.
///
/// Incremented on every membership change (node added, removed, or failed).
/// Used to detect stale ring caches and prevent split-brain operations.
pub type RingVersion = u64;

/// Configuration for erasure coding.
///
/// A `(k, m)` configuration splits data into `k` data shards and generates
/// `m` parity shards. Any `k` shards are sufficient to reconstruct the original
/// data. The system can tolerate up to `m` simultaneous shard losses.
///
/// # Constraints
/// - `k >= 1` — at least one data shard.
/// - `m >= 0` — zero or more parity shards (m=0 means no EC, just splitting).
/// - `k + m <= 255` — reed-solomon library limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureConfig {
    /// Number of data shards.
    pub k: u8,
    /// Number of parity shards.
    pub m: u8,
}

impl ErasureConfig {
    /// Create a new erasure coding configuration.
    ///
    /// Returns `Err` if `k == 0` or `k + m` exceeds 255.
    pub fn new(k: u8, m: u8) -> Result<Self, &'static str> {
        if k == 0 {
            return Err("k must be at least 1");
        }
        if (k as u16) + (m as u16) > 255 {
            return Err("k + m must not exceed 255");
        }
        Ok(Self { k, m })
    }

    /// Total number of shards (data + parity).
    #[inline]
    pub const fn total_shards(&self) -> u8 {
        self.k + self.m
    }

    /// Minimum shards needed for decoding.
    #[inline]
    pub const fn min_read(&self) -> u8 {
        self.k
    }

    /// Maximum number of shard failures the system can tolerate.
    #[inline]
    pub const fn max_faults(&self) -> u8 {
        self.m
    }

    /// Whether this config applies erasure coding at all.
    /// `m == 0` means just data splitting with no redundancy.
    #[inline]
    pub const fn has_redundancy(&self) -> bool {
        self.m > 0
    }
}

/// The membership ring — an ordered list of currently healthy nodes.
///
/// The ring is versioned: every membership change (node added, removed,
/// failed) produces a new ring with an incremented `version`. Rendezvous
/// hashing is used to deterministically map objects to nodes given a
/// ring at a specific version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ring {
    /// Ordered list of node IDs in the ring.
    pub nodes: Vec<NodeId>,
    /// Monotonically increasing version number.
    pub version: RingVersion,
}

impl Ring {
    /// Create a new ring with the given nodes and version 1.
    ///
    /// # Panics
    /// Panics if `nodes` is empty.
    pub fn new(nodes: Vec<NodeId>) -> Self {
        assert!(!nodes.is_empty(), "ring must have at least one node");
        Self { nodes, version: 1 }
    }

    /// Number of nodes in the ring.
    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the ring is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Create a new ring with an incremented version and updated node list.
    pub fn with_nodes(&self, nodes: Vec<NodeId>) -> Self {
        Self {
            nodes,
            version: self.version + 1,
        }
    }
}

/// Specifies how data is stored across nodes.
///
/// All objects use erasure coding for space-efficient durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageClass {
    /// Erasure coding configuration.
    pub ec: ErasureConfig,
}

impl StorageClass {
    /// Create a storage class with the given EC configuration.
    pub const fn new(k: u8, m: u8) -> Self {
        Self {
            ec: ErasureConfig { k, m },
        }
    }
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self { k: 3, m: 2 }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "Test code uses unwrap for ergonomic assertions"
)]
mod tests {
    use super::*;

    #[test]
    fn erasure_config_default() {
        let c = ErasureConfig::default();
        assert_eq!(c.k, 3);
        assert_eq!(c.m, 2);
        assert_eq!(c.total_shards(), 5);
        assert_eq!(c.min_read(), 3);
        assert_eq!(c.max_faults(), 2);
        assert!(c.has_redundancy());
    }

    #[test]
    fn erasure_config_no_redundancy() {
        let c = ErasureConfig::new(1, 0).unwrap();
        assert_eq!(c.total_shards(), 1);
        assert_eq!(c.min_read(), 1);
        assert_eq!(c.max_faults(), 0);
        assert!(!c.has_redundancy());
    }

    #[test]
    fn erasure_config_roundtrip_serde() {
        let c = ErasureConfig { k: 7, m: 3 };
        let json = serde_json::to_string(&c).unwrap();
        let decoded: ErasureConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(c, decoded);
    }

    #[test]
    fn erasure_config_invalid_k_zero() {
        assert!(ErasureConfig::new(0, 2).is_err());
    }

    #[test]
    fn erasure_config_too_large() {
        assert!(ErasureConfig::new(200, 100).is_err());
    }

    #[test]
    fn ring_new_and_versioning() {
        let nodes = vec![
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
        ];
        let ring = Ring::new(nodes.clone());
        assert_eq!(ring.len(), 3);
        assert!(!ring.is_empty());
        assert_eq!(ring.version, 1);

        let ring2 = ring.with_nodes(nodes.clone());
        assert_eq!(ring2.version, 2);
        assert_eq!(ring2.nodes, nodes);
    }

    #[test]
    #[should_panic(expected = "ring must have at least one node")]
    fn ring_new_empty_panics() {
        Ring::new(vec![]);
    }

    #[test]
    fn ring_serde_roundtrip() {
        let ring = Ring::new(vec![uuid::Uuid::from_u128(0x42)]);
        let json = serde_json::to_string(&ring).unwrap();
        let decoded: Ring = serde_json::from_str(&json).unwrap();
        assert_eq!(ring, decoded);
    }

    #[test]
    fn storage_class_new() {
        let sc = StorageClass::new(3, 2);
        assert_eq!(sc.ec.k, 3);
        assert_eq!(sc.ec.m, 2);
    }

    #[test]
    fn storage_class_serde_roundtrip() {
        let sc = StorageClass::new(5, 2);
        let json = serde_json::to_string(&sc).unwrap();
        let decoded: StorageClass = serde_json::from_str(&json).unwrap();
        assert_eq!(sc, decoded);
    }
}