# DFS — Distributed File System (Prototype)

> **⚠️ Prototype Status** — This is an experimental, incomplete implementation.
> It is **not** production-ready. See [docs/limitations.md](docs/limitations.md).

DFS is a Rust-based distributed file system that provides erasure-coded,
content-addressed object storage across a cluster of data nodes. It uses
Reed-Solomon erasure coding for durability, SWIM gossip for failure
detection, rendezvous hashing for deterministic data placement, and
gRPC for client–node and node–node communication.

**Disclaimer: This project is written for educational purposes and is not intended for production use. It lacks many critical features such as security, access control, etc. Use at your own risk.  This is implemented with the help of Deepseek V4 Pro and DeepSeek V4 Flash**

## Quick Start

```sh
# Build the node binary
cargo build --release -p dfs-node

# Start a 3-node cluster (3 terminals)
./target/release/dfs-node --data-dir /tmp/node1 --listen :9001
./target/release/dfs-node --data-dir /tmp/node2 --listen :9002 --join 127.0.0.1:9001
./target/release/dfs-node --data-dir /tmp/node3 --listen :9003 --join 127.0.0.1:9001

# Upload and retrieve a file using the client
cargo run -p dfs-client --example hello_dfs -- --nodes 127.0.0.1:9001
```

## Repository Structure

| Crate                              | Purpose                                                          |
| ---------------------------------- | ---------------------------------------------------------------- |
| [`dfs-core`](crates/dfs-core/)     | Pure domain logic — types, erasure coding, placement, membership |
| [`dfs-node`](crates/dfs-node/)     | Data node binary — gRPC server, disk storage, SWIM gossip        |
| [`dfs-client`](crates/dfs-client/) | Client library — `put`/`get`/`delete`, multipart upload          |

## Documentation

- [Architecture & Design](docs/architecture.md) — system design, data flow, protocol details
- [Runbook](docs/runbook.md) — building, running, manual verification, and testing
- [Limitations](docs/limitations.md) — known gaps and missing features