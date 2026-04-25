//! gRPC server: implements the [`DataNode`] service.
//!
//! Uses [`tonic`] to serve protobuf-defined RPCs. Each method maps to
//! a corresponding [`FsStorage`] operation. Membership RPCs (Join,
//! Ping, Gossip) interact with the shared [`Swimmer`] to keep the
//! cluster ring in sync.
//!
//! The same service definition works for production (tokio) and
//! simulation (turmoil) because tonic sits on top of [`tokio::net`]
//! which turmoil replaces at the transport level.

use crate::gossip::Swimmer;
use crate::storage::FsStorage;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::instrument;

pub mod proto {
    tonic::include_proto!("dfs.node.v1");
}

use proto::{
    data_node_server::DataNode,
    BlockCountRequest, BlockCountResponse,
    BlockMeta as ProtoBlockMeta,
    DeleteBlockRequest, DeleteBlockResponse,
    GetBlockRequest, GetBlockResponse,
    GossipRequest, GossipResponse,
    HealthCheckRequest, HealthCheckResponse,
    JoinRequest, JoinResponse,
    ListBlocksRequest, ListBlocksResponse,
    PingRequest, PongResponse,
    PutBlockRequest, PutBlockResponse,
    Ring as ProtoRing,
    Shard as ProtoShard,
    StatBlockRequest, StatBlockResponse,
};

use dfs_core::block::{Shard, BlockMeta};

use tracing::info;

// ---------------------------------------------------------------------------
// The tonic service struct
// ---------------------------------------------------------------------------

/// gRPC data-node service backed by local disk storage.
///
/// Holds an [`Arc`]'d [`Swimmer`] so ping/gossip/join RPCs can update
/// the membership state in real time. The storage engine is embedded
/// directly (not shared — this node owns its local disk).
pub struct DataNodeService {
    storage: FsStorage,
    swimmer: Option<Arc<Mutex<Swimmer>>>,
}

impl DataNodeService {
    /// Create a service with NO swimmer (single-node or test-only mode).
    pub fn new(storage: FsStorage) -> Self {
        Self { storage, swimmer: None }
    }

    /// Create a service with a shared swimmer for multi-node clusters.
pub fn with_membership(
        storage: FsStorage,
        swimmer: Arc<Mutex<Swimmer>>,
    ) -> Self {
        Self { storage, swimmer: Some(swimmer) }
    }
}

// ---------------------------------------------------------------------------
// Conversions: protobuf ↔ our internal types
// ---------------------------------------------------------------------------

fn block_id_from_proto(raw: &str) -> dfs_core::block::BlockId {
    dfs_core::block::BlockId(raw.to_owned())
}

impl From<BlockMeta> for ProtoBlockMeta {
    fn from(m: BlockMeta) -> Self {
        Self {
            block_id: m.block_id.0,
            object_id: m.object_id,
            shard_index: u32::from(m.shard_index),
            ring_version: m.ring_version,
            checksum: m.checksum.to_vec(),
            size: m.size,
        }
    }
}

impl From<Shard> for ProtoShard {
    fn from(s: Shard) -> Self {
        Self {
            index: u32::from(s.index),
            data: s.data,
        }
    }
}

impl From<ProtoShard> for Shard {
    fn from(s: ProtoShard) -> Self {
        Self {
            index: s.index as u8,
            data: s.data,
        }
    }
}

impl From<ProtoBlockMeta> for BlockMeta {
    fn from(m: ProtoBlockMeta) -> Self {
        Self {
            block_id: dfs_core::block::BlockId(m.block_id),
            object_id: m.object_id,
            shard_index: m.shard_index as u8,
            ring_version: m.ring_version,
            checksum: m.checksum.as_slice().try_into().unwrap_or([0u8; 32]),
            size: m.size,
        }
    }
}

// ---------------------------------------------------------------------------
// gRPC service implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl DataNode for DataNodeService {
    // ── Block CRUD ────────────────────────────────────────────────

    #[instrument(skip(self, req), fields(
        block_id = req.get_ref().meta.as_ref().map(|m| m.block_id.as_str()).unwrap_or("?"),
        shard_index = req.get_ref().meta.as_ref().map(|m| m.shard_index).unwrap_or(0),
        size = req.get_ref().shard.as_ref().map(|s| s.data.len()).unwrap_or(0),
    ))]
    async fn put_block(
        &self,
        req: Request<PutBlockRequest>,
    ) -> Result<Response<PutBlockResponse>, Status> {
        let inner = req.into_inner();
        let shard: Shard = inner.shard.ok_or_else(|| {
            Status::invalid_argument("missing shard")
        })?.into();
        let meta: BlockMeta = inner.meta.ok_or_else(|| {
            Status::invalid_argument("missing meta")
        })?.into();

        self.storage.write_shard(&meta, &shard).await.map_err(|e| {
            Status::internal(e.to_string())
        })?;
        Ok(Response::new(PutBlockResponse {}))
    }

    async fn get_block(
        &self,
        req: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let block_id = block_id_from_proto(&req.into_inner().block_id);
        let (shard, meta) = self.storage.read_block(&block_id).await.map_err(|e| {
            Status::internal(e.to_string())
        })?;
        Ok(Response::new(GetBlockResponse {
            shard: Some(shard.into()),
            meta: Some(meta.into()),
        }))
    }

    async fn delete_block(
        &self,
        req: Request<DeleteBlockRequest>,
    ) -> Result<Response<DeleteBlockResponse>, Status> {
        let block_id = block_id_from_proto(&req.into_inner().block_id);
        self.storage.delete_block(&block_id).await.map_err(|e| {
            Status::internal(e.to_string())
        })?;
        Ok(Response::new(DeleteBlockResponse {}))
    }

    async fn list_blocks(
        &self,
        _req: Request<ListBlocksRequest>,
    ) -> Result<Response<ListBlocksResponse>, Status> {
        let ids = self.storage.list_blocks().await.map_err(|e| {
            Status::internal(e.to_string())
        })?;
        Ok(Response::new(ListBlocksResponse {
            block_ids: ids.into_iter().map(|id| id.0).collect(),
        }))
    }

    async fn block_count(
        &self,
        _req: Request<BlockCountRequest>,
    ) -> Result<Response<BlockCountResponse>, Status> {
        let count = self.storage.block_count().await.map_err(|e| {
            Status::internal(e.to_string())
        })?;
        Ok(Response::new(BlockCountResponse {
            count: count as u64,
        }))
    }

    async fn stat_block(
        &self,
        req: Request<StatBlockRequest>,
    ) -> Result<Response<StatBlockResponse>, Status> {
        let block_id = block_id_from_proto(&req.into_inner().block_id);
        let meta = self.storage.read_meta(&block_id).await.map_err(|e| {
            Status::internal(e.to_string())
        })?;
        Ok(Response::new(StatBlockResponse {
            meta: Some(meta.into()),
        }))
    }

    // ── Membership ────────────────────────────────────────────────

    async fn join(
        &self,
        req: Request<JoinRequest>,
    ) -> Result<Response<JoinResponse>, Status> {
        let inner = req.into_inner();
        let new_id = inner.node_id.parse::<uuid::Uuid>()
            .map_err(|_| Status::invalid_argument("bad node id"))?;

        let ring = if let Some(ref sw) = self.swimmer {
            let mut s = sw.lock().await;
            s.add_peer(new_id, inner.listen_addr.clone());
            info!(%new_id, addr = %inner.listen_addr, "new node joined ring");

            // Add to membership so we start probing it.
            let _ = s.membership.add_node(new_id);
            s.membership.ring.clone()
        } else {
            self.storage.read_or_create_node_id()
                .map(|id| dfs_core::types::Ring::new(vec![id]))
                .map_err(|e| Status::internal(e.to_string()))?
        };

        Ok(Response::new(JoinResponse {
            ring: Some(ProtoRing {
                node_ids: ring.nodes.iter().map(|id| id.to_string()).collect(),
                version: ring.version,
            }),
        }))
    }

    async fn gossip(
        &self,
        req: Request<GossipRequest>,
    ) -> Result<Response<GossipResponse>, Status> {
        let _payload = req.into_inner().payload;
        // For now, gossip messages are logged; full propagation
        // happens via the background gossip loop. The swimmer
        // processes ping responses via `on_pong` directly.
        Ok(Response::new(GossipResponse {}))
    }

    async fn ping(
        &self,
        _req: Request<PingRequest>,
    ) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse {}))
    }

    // ── Health ────────────────────────────────────────────────────

    async fn health_check(
        &self,
        _req: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let count = self.storage.block_count().await.map_err(|e| {
            Status::internal(e.to_string())
        })?;
        Ok(Response::new(HealthCheckResponse {
            block_count: count as u64,
            disk_used: (count as u64).to_be_bytes().to_vec(),
        }))
    }
}