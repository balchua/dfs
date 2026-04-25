//! Background SWIM gossip loop.
//!
//! Drives the [`crate::gossip::Swimmer`] at regular intervals. Each tick
//! produces membership events (ping, suspect, dead, ring-changed) which
//! this loop dispatches over gRPC to peer nodes.
//!
//! # Flow
//!
//! 1. Tick the Swimmer every `probe_interval`.
//! 2. For each event:
//!    - `Ping(id)` → send gRPC `Ping` RPC to `id`
//!    - `PingReq { target, via }` → ask `via` nodes to ping `target`
//!    - `Suspect(id)` / `Dead(id)` → broadcast via gossip to all peers
//!    - `RingChanged(ring)` → notify repair loop (via channel)

use crate::gossip::{MembershipEvent, Swimmer};
use crate::server::proto::data_node_client::DataNodeClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
use tokio::time;
use tracing::{debug, info, warn};

/// Channel messages for ring changes (consumed by repair loop).
#[derive(Debug, Clone)]
pub struct RingChange {
    pub old_version: u64,
    pub new_version: u64,
    pub dead_nodes: Vec<dfs_core::NodeId>,
}

/// Run the gossip loop in the background.
pub async fn run(
    swimmer: Arc<Mutex<Swimmer>>,
    ring_tx: broadcast::Sender<RingChange>,
    peer_addrs: Vec<(dfs_core::NodeId, String)>,
) {
    let peer_lookup: HashMap<dfs_core::NodeId, String> =
        peer_addrs.into_iter().collect();
    let peer_lookup = Arc::new(peer_lookup);

    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let events = {
            let mut s = swimmer.lock().await;
            s.tick()
        };

        for event in events {
            match event {
                MembershipEvent::Ping(target) => {
                    let lookup = Arc::clone(&peer_lookup);
                    let swimmer_clone = Arc::clone(&swimmer);
                    if let Some(addr) = lookup.get(&target).cloned() {
                        let target_id = target;
                        tokio::spawn(async move {
                            match ping_node(&addr).await {
                                Ok(()) => {
                                    debug!(%target_id, "pong received");
                                    swimmer_clone.lock().await.on_pong(target_id);
                                }
                                Err(e) => {
                                    warn!(%target_id, %e, "ping failed");
                                }
                            }
                        });
                    }
                }

                MembershipEvent::PingReq { target, via } => {
                    let lookup = Arc::clone(&peer_lookup);
                    for proxy in &via {
                        if let Some(addr) = lookup.get(proxy).cloned() {
                            let msg = crate::server::proto::GossipRequest {
                                payload: format!("ping-req:{target}"),
                            };
                            let _ = send_gossip_one(&addr, &msg).await;
                        }
                    }
                }

                MembershipEvent::Suspect(node) => {
                    info!(%node, "node suspected");
                    let msg = crate::server::proto::GossipRequest {
                        payload: format!("suspect:{node}"),
                    };
                    broadcast_gossip(&peer_lookup, &msg).await;
                }

                MembershipEvent::Dead(node) => {
                    info!(%node, "node confirmed dead");
                    let msg = crate::server::proto::GossipRequest {
                        payload: format!("dead:{node}"),
                    };
                    broadcast_gossip(&peer_lookup, &msg).await;
                }

                MembershipEvent::RingChanged(new_ring) => {
                    info!(version = new_ring.version, nodes = new_ring.nodes.len(),
                          "ring changed");
                    let _ = ring_tx.send(RingChange {
                        old_version: new_ring.version - 1,
                        new_version: new_ring.version,
                        dead_nodes: Vec::new(),
                    });
                }
            }
        }
    }
}

async fn ping_node(addr: &str) -> Result<(), tonic::Status> {
    let mut client = DataNodeClient::connect(format!("http://{addr}"))
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
    let _response = client.ping(crate::server::proto::PingRequest {})
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
    Ok(())
}

async fn send_gossip_one(
    addr: &str,
    msg: &crate::server::proto::GossipRequest,
) {
    let Ok(mut client) = DataNodeClient::connect(format!("http://{addr}")).await else {
        return;
    };
    let _ = client.gossip(msg.clone()).await;
}

async fn broadcast_gossip(
    peer_lookup: &HashMap<dfs_core::NodeId, String>,
    msg: &crate::server::proto::GossipRequest,
) {
    for addr in peer_lookup.values() {
        let _ = send_gossip_one(addr, msg).await;
    }
}