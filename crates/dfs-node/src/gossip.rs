//! SWIM membership protocol.
//!
//! Detects node failures through direct and indirect probing.
//!
//! # Deterministic design for simulation
//! The protocol is *tick-driven*: the caller (test or real event-loop)
//! calls [`Swimmer::tick()`] at fixed intervals using seeded randomness.
//! This makes membership convergence reproducible in turmoil tests
//! regardless of network timing.
//!
//! # How it works
//!
//! Each tick:
//! 1. Pick a random peer to probe (direct ping).
//! 2. If no response within a timeout, ask `k` indirect peers to probe
//!    the suspect.
//! 3. If indirect probes also fail → mark as Suspect, broadcast.
//! 4. If Suspect timeout expires without a refutation → mark Dead,
//!    remove from ring, bump version.
//!
//! # Manual verification
//! ```sh
//! # Start 3 nodes, observe membership logs:
//! RUST_LOG=info node1 &
//! RUST_LOG=info node2 --join 127.0.0.1:9001 &
//! RUST_LOG=info node3 --join 127.0.0.1:9001 &
//! # Kill node2 — within ~5 ticks, nodes 1 and 3 log "node dead".
//! kill %2
//! ```

use dfs_core::membership::NodeHealth as CoreHealth;
use dfs_core::membership::Membership;
pub use dfs_core::membership::NodeHealth;
use dfs_core::types::{NodeId, Ring};
use rand::SeedableRng;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SwimConfig {
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub num_indirect: usize,
    pub suspicion_timeout: Duration,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(1),
            probe_timeout: Duration::from_secs(1),
            num_indirect: 2,
            suspicion_timeout: Duration::from_secs(3),
        }
    }
}

// ---------------------------------------------------------------------------
// Gossip state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ProbeState {
    target: NodeId,
    deadline: Instant,
    indirect_sent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipEvent {
    Ping(NodeId),
    PingReq { target: NodeId, via: Vec<NodeId> },
    Suspect(NodeId),
    Dead(NodeId),
    RingChanged(Ring),
}

// ---------------------------------------------------------------------------
// Swimmer
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Swimmer {
    pub my_id: NodeId,
    pub membership: Membership,
    pub config: SwimConfig,
    peers: HashMap<NodeId, String>,
    pending: Option<ProbeState>,
    rng: rand::rngs::StdRng,
    tick_count: u64,
}

impl Swimmer {
    pub fn new(my_id: NodeId, initial_nodes: Vec<NodeId>, _seed: Option<u64>) -> Self {
        let rng = rand::rngs::StdRng::from_os_rng();
        Self {
            my_id,
            membership: Membership::new(initial_nodes),
            config: SwimConfig::default(),
            peers: HashMap::new(),
            pending: None,
            rng,
            tick_count: 0,
        }
    }

    pub fn new_seeded(my_id: NodeId, initial_nodes: Vec<NodeId>, seed: u64) -> Self {
        let rng = rand::rngs::StdRng::seed_from_u64(seed);
        Self {
            my_id,
            membership: Membership::new(initial_nodes),
            config: SwimConfig::default(),
            peers: HashMap::new(),
            pending: None,
            rng,
            tick_count: 0,
        }
    }

    pub fn add_peer(&mut self, id: NodeId, addr: String) {
        self.peers.insert(id, addr);
    }

    #[must_use]
    pub fn tick(&mut self) -> Vec<MembershipEvent> {
        self.tick_count += 1;
        let mut events = Vec::new();

        // 1. Check pending probes for timeout.
        if let Some(ref pend) = self.pending
            && pend.deadline <= Instant::now() {
                if !pend.indirect_sent {
                    let target = pend.target;
                    let count = self.config.num_indirect;
                    let indirect = self.pick_random_peers(count, target);
                    events.push(MembershipEvent::PingReq {
                        target,
                        via: indirect.to_vec(),
                    });
                } else {
                    let target = pend.target;
                    self.membership.health.insert(
                        target,
                        CoreHealth::Suspect(self.tick_count),
                    );
                    events.push(MembershipEvent::Suspect(target));
                    self.pending = None;
                }
            }

        // 2. Suspect → Dead transitions.
        let dead_ids: Vec<NodeId> = self
            .membership
            .health
            .iter()
            .filter_map(|(&id, h)| {
                if let CoreHealth::Suspect(generation) = h {
                    if generation + 3 <= self.tick_count {
                        Some(id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        for id in &dead_ids {
            self.membership.health.insert(*id, CoreHealth::Dead(self.tick_count));
            events.push(MembershipEvent::Dead(*id));
        }

        if !dead_ids.is_empty()
            && let Ok(ring) = self.membership.rebalance_dead_nodes(&dead_ids) {
                events.push(MembershipEvent::RingChanged(ring));
            }

        // 3. Pick a new random target if idle.
        if self.pending.is_none() {
            let targets: Vec<NodeId> = self.membership.alive_nodes();
            if let Some(&target) = Self::random_pick(&mut self.rng, &targets)
                && target != self.my_id {
                    self.pending = Some(ProbeState {
                        target,
                        deadline: Instant::now() + self.config.probe_timeout,
                        indirect_sent: false,
                    });
                    events.push(MembershipEvent::Ping(target));
                }
        }

        events
    }

    pub fn on_pong(&mut self, from: NodeId) {
        if let Some(ref pend) = self.pending
            && pend.target == from {
                self.pending = None;
            }
    }

    fn random_pick<'a, T>(rng: &mut impl rand::Rng, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            return None;
        }
        let idx = rng.next_u64() as usize % items.len();
        Some(&items[idx])
    }

    fn pick_random_peers(&mut self, count: usize, exclude: NodeId) -> Vec<NodeId> {
        let pool: Vec<NodeId> = self
            .membership
            .alive_nodes()
            .into_iter()
            .filter(|id| *id != exclude && *id != self.my_id)
            .collect();
        (0..count.min(pool.len()))
            .filter_map(|_| Self::random_pick(&mut self.rng, &pool))
            .copied()
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "Test code uses unwrap, panic, and print for ergonomic assertions and debugging"
)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn tick_produces_ping_when_idle() {
        let ids: Vec<NodeId> = (0..5).map(|_| Uuid::new_v4()).collect();
        let my = ids[0];
        let mut s = Swimmer::new_seeded(my, ids.clone(), 42);

        let events = s.tick();
        assert!(events.iter().any(|e| matches!(e, MembershipEvent::Ping(_))));
    }

    #[test]
    fn no_ping_when_only_self() {
        let my = Uuid::new_v4();
        let mut s = Swimmer::new_seeded(my, vec![my], 42);
        let events = s.tick();
        assert!(events.is_empty());
    }

    #[test]
    fn dead_node_removed_from_ring() {
        let ids: Vec<NodeId> = (0..5).map(|_| Uuid::new_v4()).collect();
        let mut s = Swimmer::new_seeded(ids[0], ids.clone(), 42);

        s.membership.health.insert(ids[2], CoreHealth::Dead(0));
        let result = s.membership.rebalance_dead_nodes(&[ids[2]]);
        assert!(result.is_ok());
        assert_eq!(s.membership.ring.len(), 4);
        assert!(!s.membership.alive_nodes().contains(&ids[2]));
    }
}