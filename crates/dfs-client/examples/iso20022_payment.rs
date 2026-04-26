//! Example: ISO 20022 pain.001 payment file stored via the DFS client.
//!
//! Demonstrates:
//!   - Generate a large ISO 20022 payment XML payload **directly to a
//!     temporary file** (never fully resident in memory).
//!   - Upload it in chunks via multiple `put()` calls (each chunk is
//!     EC-encoded independently).
//!   - Read back each chunk by object ID and assemble into a temp file.
//!   - Verify the assembled size matches the original.
//!   - Delete all chunks.
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
//!     --nodes 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
//!     [--txns <num_txns>] [--chunk-size <bytes>]
//! ```

#[path = "common/iso20022.rs"]
mod iso20022;

use dfs_client::DfsClient;
use std::io::{Read, Seek, Write};
use std::time::Instant;

// ── CLI args ───────────────────────────────────────────────────

struct Args {
    nodes: Vec<String>,
    num_txns: u32,
    chunk_size: usize,
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let get = |key: &str, default: &str| {
        raw.windows(2)
            .find(|w| w[0] == key)
            .map(|w| w[1].clone())
            .unwrap_or_else(|| default.to_string())
    };

    let nodes = if let Some(pos) = raw.iter().position(|a| a == "--nodes") {
        raw.get(pos + 1)
            .map(|v| v.split(&[',', ' '][..]).map(str::to_string).collect())
            .unwrap_or_default()
    } else {
        vec!["127.0.0.1:9001".into()]
    };

    let num_txns: u32 = get("--txns", "100000").parse().expect("--txns <u32>");
    let chunk_size: usize = get("--chunk-size", "10485760")
        .parse()
        .expect("--chunk-size <bytes>");

    Args {
        nodes,
        num_txns,
        chunk_size,
    }
}

/// Read up to `buf.len()` bytes from `reader`, returning the
/// actual number of bytes read (0 = EOF).
fn read_full<R: Read>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let client = DfsClient::new(args.nodes);
    println!(
        "ISO 20022 — {} txns, {} byte chunks",
        args.num_txns, args.chunk_size,
    );

    // ── Generate XML to a temp file ─────────────────────────────
    let started = Instant::now();
    let mut tmp = tempfile::NamedTempFile::new()?;
    iso20022::generate_to(tmp.as_file_mut(), "PAIN001-V42", args.num_txns, 42);
    let total = tmp.path().metadata()?.len();
    tmp.as_file_mut().rewind()?;
    println!(
        "Written {total} bytes ISO 20022 XML ({elapsed:?}) — never fully in memory",
        elapsed = started.elapsed(),
    );

    // ── Upload in chunks via put() ─────────────────────────────
    let started = Instant::now();
    let mut chunk_buf = vec![0u8; args.chunk_size];
    let mut part_ids = Vec::new();
    loop {
        let n = read_full(tmp.as_file_mut(), &mut chunk_buf)?;
        if n == 0 {
            break;
        }
        let meta = client.put(&chunk_buf[..n]).await?;
        part_ids.push(meta.object_id);
    }
    drop(tmp);
    println!(
        "Uploaded {count} EC-encoded chunks ({elapsed:?})",
        count = part_ids.len(),
        elapsed = started.elapsed()
    );

    // ── Read back each chunk and assemble into a temp file ─────
    let started = Instant::now();
    let mut out = tempfile::NamedTempFile::new()?;
    let mut assembled_size = 0u64;
    for id in &part_ids {
        let chunk = client.get(id).await?;
        assembled_size += chunk.len() as u64;
        out.as_file_mut().write_all(&chunk)?;
    }
    out.as_file_mut().rewind()?;
    println!(
        "Assembled {len} bytes from {count} chunks ({elapsed:?})",
        len = assembled_size,
        count = part_ids.len(),
        elapsed = started.elapsed()
    );
    assert_eq!(
        assembled_size, total,
        "assembled size does not match original"
    );
    println!("Size verification: OK ({total} bytes)");

    // ── Delete all chunks ──────────────────────────────────────
    let started = Instant::now();
    for id in &part_ids {
        client.delete(id).await?;
    }
    println!(
        "Deleted {count} chunks ({elapsed:?})",
        count = part_ids.len(),
        elapsed = started.elapsed()
    );
    out.keep()?;

    Ok(())
}
