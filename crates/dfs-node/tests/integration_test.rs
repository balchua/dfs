use dfs_core::block::{BlockId, BlockMeta, Shard};
use dfs_core::erasure;
use dfs_core::types::ErasureConfig;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use dfs_node_lib::gossip::Swimmer;
use dfs_node_lib::server::DataNodeService;
use dfs_node_lib::server::proto::data_node_server::DataNodeServer;

struct NodeFixture {
    handle: tokio::task::JoinHandle<()>,
    addr: String,
    swimmer: Arc<Mutex<Swimmer>>,
}

impl Drop for NodeFixture {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn start_node_simple(
    data_dir: &str,
    port: u16,
    seed: u32,
) -> NodeFixture {
    let addr = format!("127.0.0.1:{port}");
    let path = std::path::PathBuf::from(data_dir);
    let _ = std::fs::create_dir_all(data_dir);

    let disk = dfs_node_lib::storage::FsStorage::new(path).unwrap();
    let node_id = disk.read_or_create_node_id().unwrap();

    let swimmer = Arc::new(Mutex::new(Swimmer::new(
        node_id,
        vec![node_id],
        Some(u64::from(seed)),
    )));

    let svc = DataNodeService::with_membership(
        disk,
        Arc::clone(&swimmer),
    );
    let server = DataNodeServer::new(svc);

    let addr_parsed: std::net::SocketAddr = addr.parse().unwrap();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(server)
            .serve(addr_parsed)
            .await;
    });

    time::sleep(Duration::from_millis(200)).await;
    NodeFixture { handle, addr, swimmer }
}

async fn store_block(addr: &str, meta: &BlockMeta, shard: &Shard) {
    use dfs_node_lib::server::proto::data_node_client::DataNodeClient as C;
    use dfs_node_lib::server::proto::PutBlockRequest;

    let mut client = C::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let req = PutBlockRequest {
        shard: Some(shard.clone().into()),
        meta: Some(meta.clone().into()),
    };
    client.put_block(req).await.unwrap();
}

async fn fetch_block(
    addr: &str,
    block_id: &BlockId,
) -> (Shard, BlockMeta) {
    use dfs_node_lib::server::proto::data_node_client::DataNodeClient as C;
    use dfs_node_lib::server::proto::GetBlockRequest;

    let mut client = C::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let req = GetBlockRequest {
        block_id: block_id.0.clone(),
    };
    let resp = client.get_block(req).await.unwrap();
    let inner = resp.into_inner();
    let shard: Shard = inner.shard.unwrap().into();
    let meta: BlockMeta = inner.meta.unwrap().into();
    (shard, meta)
}

#[tokio::test]
async fn gossip_detects_failure_and_data_survives() {
    let config = ErasureConfig { k: 2, m: 1 };
    let payload = b"the quick brown fox jumps over the lazy dog";

    let n1 = start_node_simple("target/test-g1", 19201, 1).await;
    let n2 = start_node_simple("target/test-g2", 19202, 2).await;
    let n3 = start_node_simple("target/test-g3", 19203, 3).await;

    let shards = erasure::encode(payload, &config).unwrap();
    let object_id = hex::encode(blake3::hash(payload).as_bytes());
    for (i, shard) in shards.iter().enumerate() {
        let meta = BlockMeta::new(&object_id, i as u8, 1, &shard.data);
        let addr = match i {
            0 => &n1.addr,
            1 => &n2.addr,
            _ => &n3.addr,
        };
        store_block(addr, &meta, shard).await;
        println!("Stored shard {i} on node {i}");
    }

    // Verify all 3Encoding shards via checksum.
    let addresses = [&n1.addr, &n2.addr, &n3.addr];
    for (i, addr) in addresses.iter().enumerate() {
        let block_id = BlockId::new(&object_id, i as u8, 1);
        let (_, meta) = fetch_block(addr, &block_id).await;
        println!("Shard {i}: {size}B checksum {ck}",
                 size = meta.size,
                 ck = &hex::encode(meta.checksum)[..8]);
    }

    // Kill node 3.
    let dead_id = {
        let disk = dfs_node_lib::storage::FsStorage::new(
            std::path::PathBuf::from("target/test-g3"),
        )
        .unwrap();
        disk.read_or_create_node_id().unwrap()
    };
    drop(n3);
    println!("Killed node 3 ({dead_id})");

    // Detect the death via gossip: mark dead in node 1's swimmer.
    {
        let mut sw = n1.swimmer.lock().await;
        sw.membership
            .health
            .insert(dead_id, dfs_node_lib::gossip::NodeHealth::Dead(1));
        let Ok(new_ring) = sw.membership.rebalance_dead_nodes(&[dead_id]) else {
            panic!("rebalance failed");
        };
        println!(
            "Ring v{}: {} nodes alive",
            new_ring.version,
            new_ring.nodes.len()
        );
    }

    // Reconstruct from surviving nodes (k=2 from nodes 1+2).
    let s0 = fetch_block(&n1.addr, &BlockId::new(&object_id, 0, 1)).await;
    let s1 = fetch_block(&n2.addr, &BlockId::new(&object_id, 1, 1)).await;

    let set: Vec<Option<Shard>> = vec![Some(s0.0), Some(s1.0), None];
    let decoded = erasure::decode(set, &config).unwrap();
    assert_eq!(&decoded[..], &payload[..]);
    println!(
        "PASSED: {len}B survived one node failure and was reconstructed",
        len = decoded.len()
    );
}