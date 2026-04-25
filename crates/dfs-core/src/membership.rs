//! Membership and ring management.
//!
//! Tracks the health of every node in the cluster and manages ring
//! version transitions. On failure detection, produces a new ring
//! (with bumped version) and lists repair jobs for shards that
//! need redistribution.
//!
//! This module is **pure logic** — it does NOT perform network calls,
//! gossip, or health checks. Those belong in `dfs-node`.

use crate::types::*;
use std::collections::HashMap;

/// Health status of a single node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    /// Node is up and participating.
    Alive,

    /// Currently being probed — may be a transient failure.
    #[allow(dead_code)] // used in future SWIM integration
    Suspect(u64),

    /// Confirmed dead at generation `N`.
    Dead(u64),
}

/// Cluster membership state.
///
/// Tracks the ring and health of every known node. The ring version
/// increments on every membership change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    /// Current ring (ordered, versioned node list).
    pub ring: Ring,

    /// Health of every node known to the cluster.
    pub health: HashMap<NodeId, NodeHealth>,
}

impl Membership {
    /// Create a new membership with every node marked [`NodeHealth::Alive`].
    ///
    /// # Example
    /// ```
    /// use dfs_core::types::Ring;
    /// use dfs_core::membership::Membership;
    /// use uuid::Uuid;
    ///
    /// let nodes = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let m = Membership::new(nodes);
    /// assert_eq!(m.ring.len(), 2);
    /// assert_eq!(m.ring.version, 1);
    /// ```
    ///
    /// # Panics
    /// Panics if `nodes` is empty.
    pub fn new(nodes: Vec<NodeId>) -> Self {
        assert!(!nodes.is_empty(), "membership requires at least one node");
        let ring = Ring::new(nodes);
        let health = ring.nodes.iter().map(|&n| (n, NodeHealth::Alive)).collect();
        Self { ring, health }
    }

    /// Number of alive nodes.
    #[inline]
    pub fn alive_count(&self) -> usize {
        self.health
            .values()
            .filter(|h| matches!(h, NodeHealth::Alive))
            .count()
    }

    /// List of currently alive nodes.
    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.ring
            .nodes
            .iter()
            .filter(|n| self.health.get(n) == Some(&NodeHealth::Alive))
            .copied()
            .collect()
    }

    /// Mark nodes as dead and produce a new ring.
    ///
    /// All dead nodes are removed from the ring; the version is bumped.
    /// Returns the **new** [`Ring`].
    ///
    /// # Errors
    /// - [`NoNodesLeft`](crate::error::CoreError::NoNodesLeft) if ALL nodes are dead.
    pub fn rebalance_dead_nodes(
        &mut self,
        dead: &[NodeId],
    ) -> crate::error::Result<Ring> {
        for &node in dead {
            self.health.insert(node, NodeHealth::Dead(self.ring.version));
        }

        let new_nodes: Vec<NodeId> = self
            .ring
            .nodes
            .iter()
            .filter(|n| self.health.get(n) == Some(&NodeHealth::Alive))
            .copied()
            .collect();

        if new_nodes.is_empty() {
            return Err(crate::error::CoreError::NoNodesLeft);
        }

        let new_ring = self.ring.with_nodes(new_nodes);
        self.ring = new_ring.clone();
        Ok(new_ring)
    }

    ///ullo Add a healthy node to the ring (and its health entry).
    ///
    /// Returns the new ring.
    pub fn add_node(&mut self, node: NodeId) -> Ring {
        let mut nodes = self.ring.nodes.clone();
        if !nodes.contains(&node) {
            nodes.push(node);
        }
        self.health.entry(node).or_insert(NodeHealth::Alive);
        let new_ring = self.ring.with_nodes(nodes);
        self.ring = new_ring.clone();
        new_ring
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "Test code uses unwrap for ergonomic assertions"
)]
mod tests {
    use super::*;
   use crate::types::NodeId;

    fn node_ids(n: u8) -> Vec<NodeId> {
        (0..n).map(|_| uuid::Uuid::new_v4()).collect()
    }

    #[test]
    fn new_membership_all_alive() {
        let ids = node_ids(5);
        let m = Membership::new(ids.clone());
        assert_eq!(m.ring.version, 1);
        assert_eq!(m.ring.len(), 5);
        assert_eq!(m.alive_count(), 5);
        for id in &ids {
            assert_eq!(m.health[id], NodeHealth::Alive);
        }
    }

    #[test]
    fn rebalance_removes_dead_nodes() {
        let mut m = Membership::new(node_ids(5));
        let dead = vec![m.ring.nodes[1], m.ring.nodes[3]];
        let new_ring = m.rebalance_dead_nodes(&dead).unwrap();

        assert_eq!(new_ring.version, 2);
        assert_eq!(new_ring.len(), 3);
        for node in &dead {
            assert_eq!(m.health[node], NodeHealth::Dead(1));
        }
        assert!(!new_ring.nodes.contains(&dead[0]));
        assert!(!new_ring.nodes.contains(&dead[1]));
    }

    #[test]
    fn rebalance_all_dead_fails() {
        let ids = node_ids(3);
        let mut m = Membership::new(ids.clone());
        let result = m.rebalance_dead_nodes(&ids);
        assert_eq!(result, Err(crate::error::CoreError::NoNodesLeft));
    }

    #[test]
    fn add_node_bumps_version() {
        let mut m = Membership::new(node_ids(3));
        let new_id = uuid::Uuid::new_v4();
        let new_ring = m.add_node(new_id);

        assert_eq!(new_ring.version, 2);
        assert_eq!(new_ring.len(), 4);
        assert!(new_ring.nodes.contains(&new_id));
        assert_eq!(m.health[&new_id], NodeHealth::Alive);
    }

    #[test]
    fn add_duplicate_node_no_effect() {
        let ids = node_ids(3);
        let mut m = Membership::new(ids.clone());
        let new_ring = m.add_node(ids[0]);
        assert_eq!(new_ring.len(), 3);
        assert_eq!(new_ring.version, 2); // still bumped (changed semantics)
        // Health stays Alive.
        assert_eq!(m.health[&ids[0]], NodeHealth::Alive);
    }

    #[test]
    fn alive_nodes_filtering() {
        let mut m = Membership::new(node_ids(4));
        let dead_id = m.ring.nodes[2];
        m.rebalance_dead_nodes(&[dead_id]).unwrap();
        let alive = m.alive_nodes();
        assert_eq!(alive.len(), 3);
        assert!(!alive.contains(&dead_id));
    }
}