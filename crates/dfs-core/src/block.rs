//! Block metadata and verification.
//!
//! Each shard stored on a data node is called a *block*. A block has a
//! [`BlockId`] derived from the object it belongs to, a [`BlockMeta`]
//! with checksum and sizing information, and the actual shard data.

use crate::types::*;
use serde::{Deserialize, Serialize};

/// Identifies a single block (shard) stored on a data node.
///
/// The ID encodes: which object this block belongs to, which shard
/// index it represents, and at which ring version it was placed.
/// Format: `{object_id}-{shard_index:02}-v{ring_version}`
///
/// # Example
/// ```
/// use dfs_core::types::*;
/// use dfs_core::block::BlockId;
///
/// let id = BlockId::new("obj-abc123", 0, 3);
/// assert_eq!(id.0, "obj-abc123-00-v3");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockId(pub String);

impl BlockId {
    /// Create a block ID from its constituent parts.
    ///
    /// Format: `{object_id}-{shard_index:02}-v{ring_version}`
    pub fn new(
        object_id: &str,
        shard_index: ShardIndex,
        ring_version: RingVersion,
    ) -> Self {
        Self(format!("{}-{:02}-v{}", object_id, shard_index, ring_version))
    }

    /// Returns the raw string value.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for BlockId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Metadata describing a single block stored on a data node.
///
/// Block metadata is persisted alongside each block's shard data on disk
/// and includes a blake3 checksum for integrity verification on read.
///
/// # Integrity
/// Call [`BlockMeta::verify`] on read to detect bit-rot or silent
/// corruption before returning data to the client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    /// Unique block identifier.
    pub block_id: BlockId,

    /// The object this block belongs to.
    pub object_id: ObjectId,

    /// Which shard in the stripe (0..k+m-1).
    pub shard_index: ShardIndex,

    /// The ring version at which this block was placed.
    pub ring_version: RingVersion,

    /// Blake3 checksum of the shard data (32 bytes).
    pub checksum: [u8; 32],

    /// Size of the shard data in bytes.
    pub size: u64,
}

impl BlockMeta {
    /// Create block metadata, computing the checksum from the provided data.
    ///
    /// # Example
    /// ```
    /// use dfs_core::block::{BlockId, BlockMeta};
    ///
    /// let meta = BlockMeta::new("obj", 2, 1u64, b"shard data");
    /// assert_eq!(meta.shard_index, 2);
    /// assert!(meta.verify(b"shard data"));
    /// ```
    pub fn new(
        object_id: &str,
        shard_index: ShardIndex,
        ring_version: RingVersion,
        data: &[u8],
    ) -> Self {
        let checksum = blake3::hash(data).into();
        Self {
            block_id: BlockId::new(object_id, shard_index, ring_version),
            object_id: object_id.to_owned(),
            shard_index,
            ring_version,
            checksum,
            size: data.len() as u64,
        }
    }

    /// Verify that the given data matches the stored checksum.
    ///
    /// Returns `true` if the data is intact, `false` if corruption is detected.
    pub fn verify(&self, data: &[u8]) -> bool {
        blake3::hash(data).as_bytes() == &self.checksum
    }
}

/// An erasure-coded shard ready for storage.
///
/// Contains the shard data along with its index in the stripe.
/// This is the output of [`erasure::encode`](crate::erasure::encode)
/// and the input to [`erasure::decode`](crate::erasure::decode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shard {
    /// Index of this shard in the stripe (0..k+m-1).
    pub index: ShardIndex,

    /// The shard data (all shards in a stripe have the same size after padding).
    pub data: Vec<u8>,
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "Test code uses unwrap for ergonomic assertions"
)]
mod tests {
    use super::*;

    #[test]
    fn block_id_format() {
        let id = BlockId::new("obj-42", 0, 1);
        assert_eq!(id.to_string(), "obj-42-00-v1");

        let id = BlockId::new("long-id", 15, 999);
        assert_eq!(id.to_string(), "long-id-15-v999");
    }

    #[test]
    fn block_id_eq_and_hash() {
        let a = BlockId::new("obj", 0, 1);
        let b = BlockId::new("obj", 0, 1);
        assert_eq!(a, b);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
    }

    #[test]
    fn block_meta_verify() {
        let data = b"hello shard";
        let meta = BlockMeta::new("test-obj", 3, 5, data);
        assert!(meta.verify(data));
        assert!(!meta.verify(b"corrupted"));
    }

    #[test]
    fn block_meta_fields() {
        let meta = BlockMeta::new("my-obj", 2, 7, b"hello shard");
        assert_eq!(meta.object_id, "my-obj");
        assert_eq!(meta.shard_index, 2);
        assert_eq!(meta.ring_version, 7);
        assert_eq!(meta.size, 11);
        assert_eq!(meta.checksum, *blake3::hash(b"hello shard").as_bytes());
    }

    #[test]
    fn block_meta_serde_roundtrip() {
        let meta = BlockMeta::new("obj", 0, 1, b"payload");
        let bytes = bincode::serialize(&meta).unwrap();
        let decoded: BlockMeta = bincode::deserialize(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn shard_equality() {
        let a = Shard { index: 0, data: vec![1, 2, 3] };
        let b = Shard { index: 0, data: vec![1, 2, 3] };
        let c = Shard { index: 1, data: vec![1, 2, 3] };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}