//! Deterministic data placement using rendezvous hashing.
//!
//! Rendezvous hashing maps `(object_key, shard_index)` → `NodeId`
//! without any centralized state, making placement predictable and
//! reproducible — critical for deterministic simulation.
//!
//! # Theory
//! For each `(object_key, shard_index, node_id)` triple we compute a
//! blake3 hash. The node with the highest hash score "wins" that shard.
//! When the ring changes (nodes added/removed), only the shards that
//! hashed to the affected nodes need redistribution.
//!
//! # References
//! - <https://en.wikipedia.org/wiki/Rendezvous_hashing>

use crate::types::*;
use crate::error::*;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Assign a single shard to a node using rendezvous hashing.
///
/// Given `(object_key, shard_index)` and the current ring, returns the
/// `NodeId` that should own that shard. The result is deterministic:
/// same `(key, shard_index, ring)` → same node every time.
///
/// # Panics
/// Panics if `ring.nodes` is empty.
pub fn assign_node(
    object_key: &str,
    shard_index: ShardIndex,
    ring: &Ring,
) -> NodeId {
    assert!(!ring.nodes.is_empty(), "cannot assign to an empty ring");

    let input = format!("{object_key}-{shard_index}");
    let mut best = ring.nodes[0];
    let mut best_score = 0u64;

    for &node in &ring.nodes {
        let score = rendezvous_hash(&input, node);
        if score > best_score {
            best_score = score;
            best = node;
        }
    }
    best
}

/// Assign all shards of an object to nodes.
///
/// Produces `k+m` assignments via [`assign_node`].
///
/// # Errors
/// - [`CoreError::NoNodesLeft`] if the ring is empty.
pub fn place(
    object_key: &str,
    class: StorageClass,
    ring: &Ring,
) -> Result<Vec<(ShardIndex, NodeId)>> {
    if ring.nodes.is_empty() {
        return Err(CoreError::NoNodesLeft);
    }

    let total = class.ec.total_shards() as usize;
    let mut assignments = Vec::with_capacity(total);

    for shard in 0..total as ShardIndex {
        let node = assign_node(object_key, shard, ring);
        assignments.push((shard, node));
    }

    Ok(assignments)
}

/// Compute the rendezvous hash for `(key, node)`.
fn rendezvous_hash(key: &str, node: NodeId) -> u64 {
    use blake3::Hasher;
    let mut h = Hasher::new();
    h.update(key.as_bytes());
    h.update(node.as_bytes());
    let hash = h.finalize();
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "Test code uses unwrap and panic for ergonomic assertions"
)]
mod tests {
    use super::*;

    fn ring_5() -> Ring {
        let ids: Vec<NodeId> = (0..5).map(|_| uuid::Uuid::new_v4()).collect();
        Ring::new(ids)
    }

    #[test]
    fn assign_deterministic() {
        let ring = ring_5();
        let first = assign_node("obj", 0, &ring);
        for _ in 0..100 {
            assert_eq!(assign_node("obj", 0, &ring), first);
        }
    }

    #[test]
    fn different_keys_different_assignments() {
        let ring = ring_5();
        let mut counts = std::collections::HashMap::new();
        for i in 0..1000u32 {
            let n = assign_node(&i.to_string(), 0, &ring);
            *counts.entry(n).or_insert(0) += 1;
        }
        for (_, c) in counts {
            assert!(c > 100, "node had {c} assignments, expected spread");
            assert!(c < 300, "node had {c} assignments, expected spread");
        }
    }

    #[test]
    fn place_returns_k_plus_m_assignments() {
        let ring = ring_5();
        let assignments = place("obj", StorageClass::new(3, 2), &ring).unwrap();
        assert_eq!(assignments.len(), 5);
        // Each entry is (ShardIndex, NodeId)
        for (i, (_shard, _node)) in assignments.iter().enumerate() {
            assert_eq!(*_shard, i as u8);
        }
    }

    #[test]
    fn place_empty_ring_fails() {
        let ring = Ring { nodes: vec![], version: 1 };
        let e = place("obj", StorageClass::new(3, 2), &ring).unwrap_err();
        assert_eq!(e, CoreError::NoNodesLeft);
    }

    #[test]
    fn adding_node_preserves_most_mappings() {
        let mut ids: Vec<NodeId> = (0..5).map(|_| uuid::Uuid::new_v4()).collect();
        let ring5 = Ring::new(ids.clone());
        ids.push(uuid::Uuid::new_v4());
        let ring6 = ring5.with_nodes(ids);

        let mut changed = 0u32;
        for object in 0..1000u32 {
            let n5 = assign_node(&object.to_string(), 0, &ring5);
            let n6 = assign_node(&object.to_string(), 0, &ring6);
            if n5 != n6 { changed += 1; }
        }

        let pct = (changed as f64 / 1000.0) * 100.0;
        assert!(pct > 5.0 && pct < 30.0,
                "expected remap ~16%, got {pct:.1}%");
    }
}