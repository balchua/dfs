# Limitations

> This project is a **prototype** and has numerous gaps. The following
> are known limitations, many of which are acceptable for a proof-of-concept
> but would need to be addressed for production use.

## Data Plane

### Streaming API

The client library (`dfs-client`) only accepts `&[u8]` for `put()`. There is no `Read`, `AsyncRead`, or `Stream`-based API. For objects over 20 MB the `put()` call is rejected outright.

A striped encoding path exists in `dfs-core` (`encode_striped` +
`StripeConfig`) but it is **not wired into the client**.

### 20 MB Single-Put Limit

`put()` enforces a 20 MB limit to keep gRPC message sizes manageable.
Objects larger than this must be split into chunks and uploaded via
separate `put()` calls, tracked by the caller.

### No Streaming gRPC

All gRPC endpoints are unary. There are no streaming RPCs. A 100 MB
shard would be a single 100 MB gRPC message.

### No Encryption

Data is stored in cleartext (`.blob` files). No at-rest encryption or
in-transit TLS (gRPC uses `http://`, not `https://`).

### No Authentication

No authentication, authorization, or access control on any gRPC
endpoint.

### No Compression

Payloads are transferred uncompressed.

## Control Plane

### Repair Is Incomplete

The repair loop re-tags blocks with the current ring version but does
**not** reconstruct shards that were assigned to a dead node. It cannot
restore fault tolerance after a permanent node loss.

### No Data Sync for Rejoining Nodes

When a node goes offline during a write and later rejoins, it never
receives the shards that were placed on other nodes while it was gone.
There is no mechanism to sync missing blocks from peers:
- No peer-to-peer shard transfer RPC exists
- No global object index to enumerate what's missing per node  
- The client's write fallback puts shards on live nodes, but the dead
  node stays empty after rejoining
- Reconstructing missing shards from `k` surviving peers and pushing
  them to the rejoined node is planned but not yet implemented

### SWIM Gossip Is Primitive

- The `Gossip` RPC handler does not process incoming gossip messages
- Joining nodes do not receive peer addresses from `Join` responses
- Peer address book is statically configured (currently empty in the
  gossip loop)
- No suspicion dissemination protocol
- No seed discovery or dynamic reconnection

### No Load Balancing

Placement ignores disk usage, CPU load, latency, or capacity.

### No Rebalancing

No background migration of shards to optimal nodes after ring changes.

### No Node Identity Verification

`Join` accepts any UUID without verification.

## Testing

### Simulation Harness Not Active

The `dfs-sim` crate (turmoil-based deterministic simulation) is
commented out. No deterministic fault-injection tests exist.

### Limited Integration Tests

- Single kill-and-recover scenario only
- Client tests use in-memory storage, single node
- No concurrent client tests
- No network partition tests
- No gradual degradation tests

### No Benchmarks

No benchmarks for EC throughput, gRPC latency, end-to-end latency, or
multi-node scaling.

## Operations

### No Monitoring

No Prometheus metrics, health aggregation, or dashboards beyond OTLP
tracing.

### No Graceful Shutdown

Background tasks are aborted — no drain or checkpoint mechanism.

### No Data Scrubbing

No background bit-rot detection on cold data.

### No Garbage Collection

No GC for orphaned data from interrupted multipart uploads.

### Single-Binary Deployment

No container image, systemd unit, or configuration file support.

## Storage

### No Tiered Storage

Single local directory only. No cloud backends or NAS support.

### No Deduplication

Content addressing enables object-level dedup but shard-level
deduplication is not implemented.

### Metadata Linearity

`list_blocks()` and `block_count()` are O(n) directory scans.

## Client

### No Ring Version Tracking

The client always uses `ring_version = 1` for block IDs and metadata,
making it incompatible with ring changes that occur during or between
operations. There is no mechanism to discover or cache the current ring
from any live node.

### No Retry with Backoff

Single attempt per shard (with fallback). No exponential backoff,
circuit breaker, or configurable timeouts.

### No Live Node Awareness

The client has a flat list of node addresses with no knowledge of which
nodes are alive. It does not participate in gossip and cannot skip dead
nodes proactively, wasting time on failed connections.

### No Connection Pooling

Every gRPC call creates a new HTTP/2 connection.

## Protocol

### Legacy Wire Protocol

The custom TCP protocol in `wire.rs` is redundant now that all
communication uses gRPC. It should be removed or deprecated.

### No Cross-Node Shard Transfer

No gRPC endpoint exists for peer-to-peer shard transfer. The repair
loop and any future rebalancer would need this.

## Documentation

### No API Reference

No generated API docs beyond Rustdoc. No interactive API explorer.