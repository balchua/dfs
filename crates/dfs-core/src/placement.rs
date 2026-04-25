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
/// For [`StorageClass::Permanent`], produces `k+m` assignments via
/// [`assign_node`], with best-effort node uniqueness.
///
/// For [`StorageClass::Transient`], assigns the full object to
/// `r_factor` distinct nodes.
///
/// # Returns
/// Either a `Vec<(ShardIndex, NodeId)>` for permanent objects, or
/// `Vec<NodeId>` for transient replicas.
#[derive(Debug, Clone)]
pub enum Placement {
    /// EC shard placements: (shard_index, target_node).
    Permanent(Vec<(ShardIndex, NodeId)>),

    /// Full-object replica placements.
    Transient(Vec<NodeId>),
}

/// Full placement of an object across the ring.
///
/// # Errors
/// - [`CoreError::NoNodesLeft`] if the ring is empty.
/// - [`CoreError::InsufficientNodes`] if not enough distinct nodes are
///   available for the required placement count.
pub fn place(
    object_key: &str,
    class: StorageClass,
    ring: &Ring,
) -> Result<Placement> {
    if ring.nodes.is_empty() {
        return Err(CoreError::NoNodesLeft);
    }

    match class {
        StorageClass::Permanent { ec } => place_permanent(object_key, ec, ring),
        StorageClass::Transient { r_factor } => {
            place_transient(object_key, r_factor, ring)
        }
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn place_permanent(
    object_key: &str,
    config: ErasureConfig,
    ring: &Ring,
) -> Result<Placement> {
    let total = config.total_shards() as usize;

    let mut assignments = Vec::with_capacity(total);

    for shard in 0..total as ShardIndex {
        let node = assign_node(object_key, shard, ring);
        assignments.push((shard, node));
    }

    Ok(Placement::Permanent(assignments))
}

fn place_transient(
    object_key: &str,
    r_factor: u8,
    ring: &Ring,
) -> Result<Placement> {
    let r = r_factor as usize;
    if r > ring.nodes.len() {
        return Err(CoreError::InsufficientNodes {
            needed: r,
            available: ring.nodes.len(),
        });
    }

    let mut nodes: Vec<NodeId> = (0..r as u8)
        .map(|i| assign_node(object_key, i, ring))
        .collect();

    // Deduplicate: shift indices and re-assign if collisions occur.
    // Simple approach: increment a counter and re-hash.
    let mut seen = std::collections::HashSet::new();
    for (i, node_ref) in nodes.iter_mut().enumerate() {
        let mut attempt = 0u8;
        while !seen.insert(*node_ref) {
            attempt += 1;
            let key = format!("{object_key}-rep-{i}-{attempt}");
            *node_ref = assign_node(&key, 0, ring);
        }
    }

    Ok(Placement::Transient(nodes))
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
        // Over many keys, assignments should spread across nodes.
        let mut counts = std::collections::HashMap::new();
        for i in 0..1000u32 {
            let n = assign_node(&i.to_string(), 0, &ring);
            *counts.entry(n).or_insert(0) += 1;
        }
        // Each of 5 nodes should get ~20% ± margin.
        for (_, c) in counts {
            assert!(c > 100, "node had {c} assignments, expected spread");
            assert!(c < 300, "node had {c} assignments, expected spread");
        }
    }

    #[test]
    fn place_permanent_returns_k_plus_m() {
        let ring = ring_5();
        match place("obj", StorageClass::permanent(3, 2), &ring).unwrap() {
            Placement::Permanent(assignments) => {
                assert_eq!(assignments.len(), 5);
            }
            Placement::Transient(_) => panic!("expected Permanent"),
        }
    }

    #[test]
    fn place_transient_with_r_factor() {
        let ring = ring_5();
        match place("part", StorageClass::transient(3), &ring).unwrap() {
            Placement::Transient(nodes) => {
                assert_eq!(nodes.len(), 3);
                // All distinct
                let mut set = std::collections::HashSet::new();
                for n in &nodes {
                    set.insert(*n);
                }
                assert_eq!(set.len(), 3);
            }
            Placement::Permanent(_) => panic!("expected Transient"),
        }
    }

    #[test]
    fn place_empty_ring_fails() {
        let ring = Ring { nodes: vec![], version: 1 };
        let e = place("obj", StorageClass::permanent(3, 2), &ring).unwrap_err();
        assert_eq!(e, CoreError::NoNodesLeft);
    }

    #[test]
    fn transient_too_few_nodes_fails() {
        let ring = Ring::new(vec![uuid::Uuid::from_u128(0x1)]);
        let e = place("x", StorageClass::transient(5), &ring).unwrap_err();
        assert!(matches!(e, CoreError::InsufficientNodes { .. }));
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

        // Adding one node should only remap ~1/6th of the keys.
        let pct = (changed as f64 / 1000.0) * 100.0;
        assert!(pct > 5.0 && pct < 30.0,
                "expected remap ~16%, got {pct:.1}%");
    }
}