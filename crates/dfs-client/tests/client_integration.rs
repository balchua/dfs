//! Integration tests for the DFS client.
//!
//! Starts in-memory gRPC nodes, then exercises put/get/delete/stat
//! and multipart transient uploads.
//!
//! # Run
//!
//! ```sh
//! cargo test -p dfs-client --test client_integration -- --nocapture --test-threads=1
//! ```

use dfs_client::{DfsClient, mem_store::MemStorage};
use dfs_client::proto::data_node_server::DataNodeServer;
use tokio::time::{self, Duration};

static BASE_PORT: u16 = 19601;

async fn start_nodes(count: usize) -> Vec<String> {
    let mut addrs = Vec::with_capacity(count);
    for i in 0..count {
        let port = BASE_PORT + (i as u16);
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        addrs.push(format!("127.0.0.1:{port}"));

        let svc = MemStorage::new();
        let server = DataNodeServer::new(svc);
        let incoming = tonic::transport::server::TcpIncoming::bind(addr).unwrap();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(incoming)
                .await;
        });
    }
    time::sleep(Duration::from_millis(300)).await;
    addrs
}

#[tokio::test]
#[allow(clippy::unwrap_used, reason = "tests use unwrap for clarity")]
async fn put_get_roundtrip() {
    let addrs = start_nodes(3).await;
    let client = DfsClient::new(addrs);
    let payload = b"integration-test-payload";
    let meta = client.put(payload).await.unwrap();
    println!("put: {} bytes, id {}", meta.size, &meta.object_id[..8]);
    let data = client.get(&meta.object_id).await.unwrap();
    assert_eq!(&data[..], payload);
    println!("get: OK");
}

#[tokio::test]
#[allow(clippy::unwrap_used, reason = "tests use unwrap for clarity")]
async fn stat_roundtrip() {
    let addrs = start_nodes(3).await;
    let client = DfsClient::new(addrs);
    let meta = client.put(b"stat-me").await.unwrap();
    let info = client.stat(&meta.object_id).await.unwrap();
    // stat returns shard-level meta; checksum is the key field.
    assert!(info.size > 0);
    println!("stat: cksum {} OK", hex::encode(info.checksum));
}

#[tokio::test]
#[allow(clippy::unwrap_used, reason = "tests use unwrap for clarity")]
async fn delete_then_not_found() {
    let addrs = start_nodes(3).await;
    let client = DfsClient::new(addrs);
    let meta = client.put(b"delete-me").await.unwrap();
    client.delete(&meta.object_id).await.unwrap();
    assert!(client.get(&meta.object_id).await.is_err());
    println!("delete + not-found: OK");
}

#[tokio::test]
#[allow(clippy::unwrap_used, reason = "tests use unwrap for clarity")]
async fn multipart_transient() {
    let addrs = start_nodes(1).await;
    let client = DfsClient::new(addrs);

    let chunks = [b"abc", b"def", b"ghi"];
    let mut parts = Vec::new();
    for (i, chunk) in chunks.iter().enumerate() {
        let pm = client.put_part("mpu-1", (i as u32) + 1, *chunk).await.unwrap();
        parts.push(pm);
    }
    let mut assembled = Vec::new();
    for p in &parts {
        assembled.extend_from_slice(&client.get_part(&p.part_object_id).await.unwrap());
    }
    assert_eq!(&assembled[..], b"abcdefghi");
    for p in &parts {
        client.delete_part(&p.part_object_id).await.unwrap();
    }
    println!("multipart transient: OK");
}