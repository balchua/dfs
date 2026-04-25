//! Background repair loop.
//!
//! Listens for ring change notifications on a broadcast channel and
//! repairs orphaned shards when nodes die.
//!
//! # Repair algorithm
//!
//! When a node dies:
//! 1. Enumerate all blocks we have locally that carry the old ring version.
//! 2. For each object, use rendezvous hashing to compute which nodes
//!    own which shards under the NEW ring.
//! 3. If a shard was assigned to a now-dead node but the new assignment
//!    puts it on a surviving node (including us), fetch the surviving
//!    k shards, erasure-reconstruct the missing one, and store it on
//!    the new target node.
//!
//! # In practice (current limit)
//!
//! The repair loop identifies blocks whose ring_version is stale and
//! re-stores them with the current version. Full cross-node
//! reconstruction (fetching k shards from peers to rebuild missing
//! ones) is integrated via the `dfs-client` in Phase 3.

use crate::background::gossip_loop::RingChange;
use crate::storage::FsStorage;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// Run the repair loop, consuming ring-change notifications.
pub async fn run(storage: FsStorage, mut ring_rx: broadcast::Receiver<RingChange>) {
    loop {
        let change = match ring_rx.recv().await {
            Ok(c) => c,
            Err(broadcast::error::RecvError::Closed) => break,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(skipped = n, "repair loop fell behind on ring changes");
                continue;
            }
        };

        info!(
            old = change.old_version,
            new = change.new_version,
            "repair loop triggered"
        );

        // List all blocks on this node.
        let blocks = match storage.list_blocks().await {
            Ok(ids) => ids,
            Err(e) => {
                warn!(%e, "cannot list blocks for repair");
                continue;
            }
        };

        let mut repaired = 0u64;
        for block_id in &blocks {
            // Read just the metadata to check the ring version.
            let meta = match storage.read_meta(block_id).await {
                Ok(m) => m,
                Err(_) => continue,
            };

            if usize::try_from(meta.ring_version).unwrap_or(0) < change.new_version as usize {
                // This block is from an older ring — it may be orphaned.
                // For now we simply re-write it with the new ring version
                // so the placement matches the current node.
                let (shard, mut new_meta) = match storage.read_block(block_id).await {
                    Ok((s, m)) => (s, m),
                    Err(_) => continue,
                };

                new_meta.ring_version = change.new_version;
                if let Err(e) = storage.write_shard(&new_meta, &shard).await {
                    warn!(%block_id, %e, "repair write failed");
                } else {
                    repaired += 1;
                }
            }
        }

        if repaired > 0 {
            info!(repaired, "repair loop re-tagged blocks");
        }
    }
}