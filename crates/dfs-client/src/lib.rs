//! DFS client for S3 frontend integration.
//!
//! Erasure-coding, shard placement, and node communication are
//! encapsulated behind simple `put`/`get`/`delete`/`stat` methods.
//! Your application works with byte payloads, not shards.
//!
//! # Example
//!
//! ```rust,no_run
//! use dfs_client::DfsClient;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let client = DfsClient::new(vec!["127.0.0.1:9001".into()]);
//! let meta = client.put(b"hello").await?;
//! let data = client.get(&meta.object_id).await?;
//! assert_eq!(data, b"hello");
//! # Ok(())
//! # }
//! ```

use dfs_core::block::{BlockId, BlockMeta, Shard};
use dfs_core::erasure;
use dfs_core::types::ErasureConfig;
use tracing::debug;
use uuid::Uuid;

pub mod proto {
    tonic::include_proto!("dfs.node.v1");
}
use proto::{
    DeleteBlockRequest, GetBlockRequest, PutBlockRequest, StatBlockRequest,
    data_node_client::DataNodeClient as DnClient,
};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("gRPC: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("encoding: {0}")]
    Encoding(String),
    #[error("object not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub object_id: String,
    pub checksum: [u8; 32],
    pub size: u64,
}

// ---------------------------------------------------------------------------
// DfsClient
// ---------------------------------------------------------------------------

pub struct DfsClient {
    node_addrs: Vec<String>,
    ec: ErasureConfig,
}

impl DfsClient {
    pub fn new(node_addrs: Vec<String>) -> Self {
        Self {
            node_addrs,
            ec: ErasureConfig { k: 3, m: 2 },
        }
    }

    pub fn with_config(node_addrs: Vec<String>, ec: ErasureConfig) -> Self {
        Self { node_addrs, ec }
    }

// ── Permanent (erasure-coded) ────────────────────────────────

pub async fn put(&self, data: &[u8]) -> Result<ObjectMetadata> {
        if data.len() > 20 * 1024 * 1024 {
            return Err(ClientError::Encoding(
                "object too large for single put (>20MB)".into(),
            ));
        }
        let object_id = Uuid::now_v7().to_string();
        let checksum = blake3::hash(data).into();
        let shards = erasure::encode(data, &self.ec)
            .map_err(|e| ClientError::Encoding(e.to_string()))?;

        let mut written = 0usize;
        for (i, shard) in shards.iter().enumerate() {
            let meta = BlockMeta::new(&object_id, i as u8, 1, &shard.data);
            let req = PutBlockRequest {
                shard: Some(shard.clone().into()),
                meta: Some(meta.into()),
            };
            let preferred = i % self.node_addrs.len();
            let mut ok = false;
            for offset in 0..self.node_addrs.len() {
                let addr = &self.node_addrs[(preferred + offset) % self.node_addrs.len()];
                if try_put_shard(addr, &req).await.is_ok() {
                    ok = true;
                    break;
                }
            }
            if ok {
                written += 1;
            }
        }

        if written < self.ec.k as usize + 1 {
            return Err(ClientError::Encoding(format!(
                "put: only {written}/{total} shards written",
                total = shards.len()
            )));
        }

        Ok(ObjectMetadata {
            object_id,
            checksum,
            size: data.len() as u64,
        })
    }

    pub async fn get(&self, object_id: &str) -> Result<Vec<u8>> {
        let total = (self.ec.k + self.ec.m) as usize;
        let k = self.ec.k as usize;
        let mut collected: Vec<Option<Shard>> = (0..total).map(|_| None).collect();
        let mut got = 0usize;

        for i in 0..total {
            if got >= k {
                break;
            }
            let block_id = BlockId::new(object_id, i as u8, 1);
            let preferred = i % self.node_addrs.len();
            let mut found = false;
            for offset in 0..self.node_addrs.len() {
                let addr = &self.node_addrs[(preferred + offset) % self.node_addrs.len()];
                match try_fetch_shard(addr, &block_id).await {
                    Ok(shard) => {
                        collected[i] = Some(shard);
                        got += 1;
                        found = true;
                        break;
                    }
                    Err(e) => {
                        debug!(shard = i, node = %addr, "shard missing: {e}");
                    }
                }
            }
            if !found {
                debug!(shard = i, "shard not found on any node");
            }
        }

        if got < k {
            return Err(ClientError::NotFound(format!(
                "insufficient shards for get: got {got}, need {k}"
            )));
        }

        erasure::decode(collected, &self.ec).map_err(|e| ClientError::NotFound(e.to_string()))
    }

    pub async fn delete(&self, object_id: &str) -> Result<()> {
        let total = (self.ec.k + self.ec.m) as usize;
        for i in 0..total {
            let block_id = BlockId::new(object_id, i as u8, 1);
            let addr = &self.node_addrs[i % self.node_addrs.len()];
            let mut client = DnClient::connect(format!("http://{addr}")).await?;
            let req = DeleteBlockRequest {
                block_id: block_id.0.clone(),
            };
            client.delete_block(req).await?;
        }
        Ok(())
    }

    pub async fn stat(&self, object_id: &str) -> Result<ObjectMetadata> {
        let block_id = BlockId::new(object_id, 0, 1);
        let addr = &self.node_addrs[0];
        let mut client = DnClient::connect(format!("http://{addr}")).await?;
        let req = StatBlockRequest {
            block_id: block_id.0.clone(),
        };
        let resp = client.stat_block(req).await?;
        let inner = resp.into_inner();
        let meta: BlockMeta = inner
            .meta
            .ok_or_else(|| tonic::Status::internal("empty meta in response"))?
            .into();
        Ok(ObjectMetadata {
            object_id: meta.object_id,
            checksum: meta.checksum,
            size: meta.size,
        })
    }
}

// ── Resilient helpers: catch connect + gRPC errors ─────────────

async fn try_put_shard(addr: &str, req: &PutBlockRequest) -> Result<()> {
    let mut client = match DnClient::connect(format!("http://{addr}")).await {
        Ok(c) => c,
        Err(e) => {
            return Err(ClientError::Transport(e));
        }
    };
    match client.put_block(req.clone()).await {
        Ok(_) => Ok(()),
        Err(e) => Err(ClientError::Grpc(e)),
    }
}

async fn try_fetch_shard(addr: &str, block_id: &BlockId) -> std::result::Result<Shard, String> {
    let mut client = match DnClient::connect(format!("http://{addr}")).await {
        Ok(c) => c,
        Err(e) => return Err(e.to_string()),
    };
    let req = GetBlockRequest {
        block_id: block_id.0.clone(),
    };
    match client.get_block(req).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            match inner.shard {
                Some(s) => Ok(s.into()),
                None => Err("empty shard".into()),
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

// ── Convenience conversions for proto types ─────────────────────

impl From<BlockMeta> for crate::proto::BlockMeta {
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

impl From<Shard> for crate::proto::Shard {
    fn from(s: Shard) -> Self {
        Self {
            index: u32::from(s.index),
            data: s.data,
        }
    }
}

impl From<crate::proto::Shard> for Shard {
    fn from(s: crate::proto::Shard) -> Self {
        Self {
            index: s.index as u8,
            data: s.data,
        }
    }
}

impl From<crate::proto::BlockMeta> for BlockMeta {
    fn from(m: crate::proto::BlockMeta) -> Self {
        Self {
            block_id: BlockId(m.block_id),
            object_id: m.object_id,
            shard_index: m.shard_index as u8,
            ring_version: m.ring_version,
            checksum: m.checksum.as_slice().try_into().unwrap_or([0u8; 32]),
            size: m.size,
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory storage helpers (for integration tests)
// ---------------------------------------------------------------------------

/// Minimal in-memory key-value store implementing the gRPC
/// [`DataNode`][crate::proto::data_node_server::DataNode] trait.
/// No filesystem required — used in integration tests to exercise
/// the client without external data-node processes.
pub mod mem_store {
    use crate::proto::data_node_server::DataNode;
    use crate::proto::{
        BlockCountRequest, BlockCountResponse, DeleteBlockRequest, DeleteBlockResponse,
        GetBlockRequest, GetBlockResponse, GossipRequest, GossipResponse, HealthCheckRequest,
        HealthCheckResponse, JoinRequest, JoinResponse, ListBlocksRequest, ListBlocksResponse,
        PingRequest, PongResponse, PutBlockRequest, PutBlockResponse, Ring, StatBlockRequest,
        StatBlockResponse,
    };

    use super::{BlockMeta, Shard};
    use crate::proto::Shard as ProtoShard;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use tonic::{Request, Response, Status};

    pub struct MemStorage {
        pub blocks: StdMutex<HashMap<String, (Vec<u8>, BlockMeta)>>,
    }

    impl Default for MemStorage {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MemStorage {
        pub fn new() -> Self {
            Self {
                blocks: StdMutex::new(HashMap::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl DataNode for MemStorage {
        async fn put_block(
            &self,
            req: Request<PutBlockRequest>,
        ) -> std::result::Result<Response<PutBlockResponse>, Status> {
            let inner = req.into_inner();
            let meta: BlockMeta = inner
                .meta
                .ok_or_else(|| Status::invalid_argument("no meta"))
                .map(Into::into)?;
            let shard: Shard = inner
                .shard
                .ok_or_else(|| Status::invalid_argument("no shard"))
                .map(Into::into)?;
            self.blocks
                .lock()
                .unwrap()
                .insert(meta.block_id.0.clone(), (shard.data, meta.clone()));
            Ok(Response::new(PutBlockResponse {}))
        }

        async fn get_block(
            &self,
            req: Request<GetBlockRequest>,
        ) -> std::result::Result<Response<GetBlockResponse>, Status> {
            let id = req.into_inner().block_id;
            let map = self.blocks.lock().unwrap();
            let (data, meta) = map.get(&id).ok_or_else(|| Status::not_found(id.clone()))?;
            let shard = ProtoShard {
                index: u32::from(meta.shard_index),
                data: data.clone(),
            };
            Ok(Response::new(GetBlockResponse {
                shard: Some(shard),
                meta: Some((*meta).clone().into()),
            }))
        }

        async fn delete_block(
            &self,
            req: Request<DeleteBlockRequest>,
        ) -> std::result::Result<Response<DeleteBlockResponse>, Status> {
            self.blocks
                .lock()
                .unwrap()
                .remove(&req.into_inner().block_id);
            Ok(Response::new(DeleteBlockResponse {}))
        }

        async fn list_blocks(
            &self,
            _: Request<ListBlocksRequest>,
        ) -> std::result::Result<Response<ListBlocksResponse>, Status> {
            let ids = self.blocks.lock().unwrap().keys().cloned().collect();
            Ok(Response::new(ListBlocksResponse { block_ids: ids }))
        }

        async fn block_count(
            &self,
            _: Request<BlockCountRequest>,
        ) -> std::result::Result<Response<BlockCountResponse>, Status> {
            Ok(Response::new(BlockCountResponse {
                count: self.blocks.lock().unwrap().len() as u64,
            }))
        }

        async fn stat_block(
            &self,
            req: Request<StatBlockRequest>,
        ) -> std::result::Result<Response<StatBlockResponse>, Status> {
            let id = req.into_inner().block_id;
            let map = self.blocks.lock().unwrap();
            let (_, meta) = map.get(&id).ok_or_else(|| Status::not_found(id.clone()))?;
            Ok(Response::new(StatBlockResponse {
                meta: Some((*meta).clone().into()),
            }))
        }

        async fn join(
            &self,
            _: Request<JoinRequest>,
        ) -> std::result::Result<Response<JoinResponse>, Status> {
            Ok(Response::new(JoinResponse {
                ring: Some(Ring {
                    node_ids: vec![],
                    version: 1,
                }),
            }))
        }

        async fn gossip(
            &self,
            _: Request<GossipRequest>,
        ) -> std::result::Result<Response<GossipResponse>, Status> {
            Ok(Response::new(GossipResponse {}))
        }

        async fn ping(
            &self,
            _: Request<PingRequest>,
        ) -> std::result::Result<Response<PongResponse>, Status> {
            Ok(Response::new(PongResponse {}))
        }

        async fn health_check(
            &self,
            _: Request<HealthCheckRequest>,
        ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
            Ok(Response::new(HealthCheckResponse {
                block_count: 0,
                disk_used: vec![],
            }))
        }
    }
}
