# DFS Data Node — Manual Runbook

## Prerequisites

Rust toolchain (stable ≥ 1.85), `protoc` (protobuf compiler):

```sh
rustup update stable
# Ubuntu/Debian:
sudo apt install protobuf-compiler
# macOS:
brew install protobuf
```

## Build

```sh
cargo build --release -p dfs-node
```

The binary is at `target/release/dfs-node`.

---

## 1. Start a single node

```sh
mkdir -p /tmp/node1
./target/release/dfs-node --data-dir /tmp/node1 --listen 127.0.0.1:9001
```

Check the on-disk layout:

```sh
cat /tmp/node1/node.id           # → a UUID (stable across restarts)
ls /tmp/node1/blocks/            # → empty initially
```

## 2. Start multiple nodes on one machine

In three separate terminals:

```sh
# Terminal 1
./target/release/dfs-node --data-dir /tmp/node1 --listen 127.0.0.1:9001

# Terminal 2
./target/release/dfs-node --data-dir /tmp/node2 --listen 127.0.0.1:9002 --join 127.0.0.1:9001

# Terminal 3
./target/release/dfs-node --data-dir /tmp/node3 --listen 127.0.0.1:9003 --join 127.0.0.1:9001
```

Each node gets its own `node.id` and `blocks/` directory. They share nothing.

## 3. Store a block via gRPC

Use `grpcurl` to interact with the running nodes:

```sh
# Store a shard
grpcurl -plaintext \
  -d '{"shard":{"index":0,"data":"aGVsbG8gd29ybGQ="},"meta":{"block_id":"obj-1-00-v1","object_id":"obj-1","shard_index":0,"ring_version":1,"checksum":"AAAA...","size":11}}' \
  127.0.0.1:9001 dfs.node.v1.DataNode/PutBlock

# List blocks
grpcurl -plaintext -d '{}' 127.0.0.1:9001 dfs.node.v1.DataNode/ListBlocks

# Get a block
grpcurl -plaintext \
  -d '{"block_id":"obj-1-00-v1"}' \
  127.0.0.1:9001 dfs.node.v1.DataNode/GetBlock

# Delete a block
grpcurl -plaintext \
  -d '{"block_id":"obj-1-00-v1"}' \
  127.0.0.1:9001 dfs.node.v1.DataNode/DeleteBlock
```

## 4. Verify on-disk integrity

After storing a block, you can inspect the files:

```sh
# The blob file contains raw shard data:
xxd /tmp/node1/blocks/obj-1-00-v1.blob | head

# The meta file contains the bincode-encoded BlockMeta:
ls -la /tmp/node1/blocks/obj-1-00-v1.meta
```

**Tamper test**: corrupt the blob file, then `GetBlock` the same block — the gRPC response will be an error because the checksum fails.

```sh
echo "corrupted" > /tmp/node1/blocks/obj-1-00-v1.blob
grpcurl -plaintext -d '{"block_id":"obj-1-00-v1"}' 127.0.0.1:9001 dfs.node.v1.DataNode/GetBlock
# ERROR: checksum mismatch
```

## 5. Verify erasure coding manually

Use the `dfs-core` unit tests as the reference implementation for EC:

```sh
cargo test -p dfs-core -- erasure
# All 12 tests pass: encode/decode roundtrip, missing shards, empty data, 1MB payload
```

Or write a small script that calls `dfs_core::erasure::encode()` and
`dfs_core::erasure::decode()` with known inputs.

### Manual EC verification with grpcurl

1. Encode a known payload (e.g. "hello") with k=2,m=1.
   Write the 3 shards to 3 different nodes.

2. Delete shard 0 on node 1:
   ```sh
   grpcurl -plaintext -d '{"block_id":"obj-0-00-v1"}' 127.0.0.1:9001 dfs.node.v1.DataNode/DeleteBlock
   ```

3. Attempt to decode using shards 1 and 2 — it should succeed
   because m=0 allows the missing shard to be reconstructed from
   any k remaining.

## 6. Membership / gossip verification (automated)

Run the integration test that starts 3 real gRPC nodes, stores an
erasure-coded payload, kills one node, and verifies data survival:

```sh
cargo test -p dfs-node --test integration_test -- --nocapture
```

Expected output:

```
Stored shard 0 on node 0
Stored shard 1 on node 1
Stored shard 2 on node 2
Shard 0: 22B checksum 9b75f448
Shard 1: 22B checksum dc88a8ac
Shard 2: 22B checksum 66e9e953
Killed node 3 (06778c28-...)
Ring v2: 1 nodes alive
PASSED: 43B survived one node failure and was reconstructed
```

### Manual gossip verification

Start 3 nodes (as above). Check the log output:

```sh
RUST_LOG=info ./target/release/dfs-node --data-dir /tmp/node1 --listen :9001
```

Kill node 2 (`kill %2`). Within ~5 seconds, nodes 1 and 3 will log:

```
INFO  gossip::tick: node dead node_id=...
```

## 7.Encoding OTLP trace export

Start a Jaeger collector (Docker):

```sh
docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 -p 4317:4317 -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

Start the node with OTLP enabled:

```sh
./target/release/dfs-node \
  --data-dir /tmp/node1 --listen :9001 \
  --otlp-endpoint http://localhost:4317 \
  --service-name dfs-node
```

Open `http://localhost:16686`, search for service `dfs-node`. Every
gRPC call generates a trace with nested spans for `write_block`,
`read_block`, etc. Click a span to see correlated log events.

## 8. Clean up

```sh
kill %1 %2 %3             # stop all background nodes
rm -rf /tmp/node{1,2,3}   # wipe all data dirs
```

## 9. Client examples

### Hello DFS

```sh
# Terminal 1: start a node
cargo run -p dfs-node -- --data-dir /tmp/hello --listen 0.0.0.0:9001

# Terminal 2: run the hello example
cargo run -p dfs-client --example hello_dfs -- --nodes 127.0.0.1:9001
```

### ISO 20022 payment file

```sh
# Start 3 nodes (in 3 terminals):
cargo run -p dfs-node -- --data-dir /tmp/e1 --listen :9001 &
cargo run -p dfs-node -- --data-dir /tmp/e2 --listen :9002 --join 127.0.0.1:9001 &
cargo run -p dfs-node -- --data-dir /tmp/e3 --listen :9003 --join 127.0.0.1:9001 &

# Run the example:
cargo run -p dfs-client --example iso20022_payment -- \
    --nodes 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
```

This generates ~1.8 MB of ISO 20022 pain.001 XML, uploads it in
chunks via separate `put()` calls, reads each chunk back, assembles
them into a temp file, verifies the size, and deletes all chunks.

## 10. Integration tests (automated)

```sh
# In-memory node tests (no external processes):
cargo test -p dfs-client --test client_integration -- --nocapture --test-threads=1

# 3-node gossip + repair test:
cargo test -p dfs-node --test integration_test -- --nocapture

# Full workspace suite:
cargo test --workspace -- --test-threads=1
```

## 11. Clippy & docs

```sh
cargo clippy --workspace --all-targets
cargo doc --workspace --no-deps
```