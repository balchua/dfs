//! Example: ISO 20022 pain.001 payment file stored via the DFS client.
//!
//! Demonstrates how to integrate the DfsClient into a real application:
//!   - Generate a large ISO 20022 payment XML payload.
//!   - Upload it via chunked transient parts (simulating S3 multipart).
//!   - Assemble parts, erasure-code the final blob, and store permanently.
//!   - Retrieve, verify checksum, and delete.
//!
//! # Prerequisites: running data nodes
//!
//! Start 3 nodes before running this example:
//!
//! ```sh
//! # In 3 terminals:
//! cargo run -p dfs-node -- --data-dir /tmp/e1 --listen :9001 &
//! cargo run -p dfs-node -- --data-dir /tmp/e2 --listen :9002 --join 127.0.0.1:9001 &
//! cargo run -p dfs-node -- --data-dir /tmp/e3 --listen :9003 --join 127.0.0.1:9001 &
//! ```
//!
//! Then run the example:
//!
//! ```sh
//! cargo run -p dfs-client --example iso20022_payment -- \
//!     --nodes 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
//! ```

#[path = "common/iso20022.rs"]
mod iso20022;

use dfs_client::DfsClient;
use std::time::Instant;

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
    println!("Connected to {} nodes", 3 /* args parsed above */);

    // // ── Generate ~1.8 MB ISO 20022 pain.001 ────────────────────
    // let started = Instant::now();
    // let xml = iso20022::generate("PAIN001-V42", 1000, 42);
    // let total = xml.len();
    // println!(
    //     "Generated {total} bytes ISO 20022 XML ({elapsed:?})",
    //     elapsed = started.elapsed()
    // );

    // // ── Multipart upload (64 KB chunks) ─────────────────────────
    // let started = Instant::now();
    // let chunk_size = 64_096usize; // 16 KB
    // let mut parts = Vec::new();
    // for (i, chunk) in xml.as_bytes().chunks(chunk_size).enumerate() {
    //     let pm = client
    //         .put_part("iso20022-demo", (i as u32) + 1, chunk)
    //         .await?;
    //     parts.push(pm);
    // }
    // println!(
    //     "Staged {count} transient parts ({elapsed:?})",
    //     count = parts.len(),
    //     elapsed = started.elapsed()
    // );

    // // ── Assemble transient parts ────────────────────────────────
    // let started = Instant::now();
    // let mut assembled = Vec::with_capacity(total);
    // for part in &parts {
    //     let chunk = client.get_part(&part.part_object_id).await?;
    //     assembled.extend_from_slice(&chunk);
    // }
    // println!(
    //     "Assembled {len} bytes from transient parts ({elapsed:?})",
    //     len = assembled.len(),
    //     elapsed = started.elapsed()
    // );

    // // ── Store permanently (erasure-coded) ───────────────────────
    // let started = Instant::now();
    // let meta = client.put(&assembled).await?;
    // println!(
    //     "EC encoded + stored ({elapsed:?}) → id {} cksum {}",
    //     &meta.object_id[..8],
    //     hex::encode(meta.checksum),
    //     elapsed = started.elapsed()
    // );

    // // ── Cleanup transient parts ─────────────────────────────────
    // for part in &parts {
    //     client.delete_part(&part.part_object_id).await?;
    // }

    // // ── Read back and verify ────────────────────────────────────
    // let started = Instant::now();
    // let retrieved = client.get(&meta.object_id).await?;
    // let cksum = blake3::hash(&retrieved);
    // assert_eq!(retrieved.len(), total);
    // assert_eq!(
    //     cksum.as_bytes(),
    //     &meta.checksum,
    //     "round-trip checksum mismatch"
    // );
    // println!(
    //     "Read + verified ({elapsed:?}) ← {total} bytes OK",
    //     elapsed = started.elapsed()
    // );

    // ── Delete ──────────────────────────────────────────────────
    // client.delete(&meta.object_id).await?;
    // let missing = client.get(&meta.object_id).await;
    // assert!(
    //     missing.is_err(),
    //     "object should be deleted"
    // );
    // println!("Deleted + confirmed gone");

    let retrieved = client
        .get("b3aead2514a1ad00c84295283f57c4a6cea42d3cd7c29b0bb31de77fff9e2c58")
        .await?;

    println!("Retrieved object: {} bytes", retrieved.len());
    println!("print all:\n{}", String::from_utf8_lossy(&retrieved));
    Ok(())
}
