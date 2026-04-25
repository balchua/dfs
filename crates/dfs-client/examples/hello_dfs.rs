//! Minimal example: put a string blob, retrieve it, verify
//! the checksum.
//!
//! # Running
//!
//! ```sh
//! # 1. Start a node:
//! cargo run -p dfs-node -- --data-dir /tmp/hello-node --listen :9001
//!
//! # 2. In another terminal:
//! cargo run -p dfs-client --example hello_dfs -- \
//!     --nodes 127.0.0.1:9001
//! ```

use dfs_client::DfsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes: Vec<String> = std::env::args()
        .find(|a| a == "--nodes")
        .and_then(|_| {
            let idx = std::env::args().position(|a| a == "--nodes").unwrap() + 1;
            let val = std::env::args().nth(idx)?;
            Some(val.split(&[',', ' '][..]).map(str::to_string).collect())
        })
        .unwrap_or_else(|| vec!["127.0.0.1:9001".into()]);

    let client = DfsClient::new(nodes);

    let payload = b"Hello from the DFS client example!";
    let meta = client.put(payload).await?;
    println!("put: {} bytes → object_id {}", meta.size, meta.object_id);

    let data = client.get(&meta.object_id).await?;
    println!("get: {} bytes", data.len());
    assert_eq!(&data[..], payload);

    client.delete(&meta.object_id).await?;
    println!("delete: OK");
    Ok(())
}