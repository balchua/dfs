# DFS Architecture & Design

## Overview

DFS is a content-addressed, erasure-coded object store. Clients upload
byte payloads that are split into Reed-Solomon shards and distributed
across independent data nodes. The system can tolerate up to `m`
simultaneous **shard** losses — but with fewer than `k+m` nodes,
multiple shards colocate on the same node, so the actual **node**
failure tolerance is lower. For example, with `k=3, m=2` and 3 nodes,
5 shards are spread across 3 nodes (round-robin), so only 1 node
failure is tolerable (losing 2 nodes loses at least 3 shards, exceeding
recovery capacity).

```
 ┌──────────┐   ┌──────────┐   ┌──────────┐
 │ Client   │   │ Node 1   │   │ Node 2   │   ... Node N
 │          │   │ gRPC svc │   │ gRPC svc │
 │ put(data)├──►│ FS disk  │   │ FS disk  │
 │ get(id)  │◄──┤ SWIM gsp │   │ SWIM gsp │
 └──────────┘   └──────────┘   └──────────┘
```

## Core Concepts

### Object

The unit of storage — a byte payload with an `ObjectId`. For permanent
objects, the `ObjectId` is derived from the blake3 content hash of the
payload (content-addressed). For transient parts, it is a UUID-like
path identifier.

### Shard

A fragment of the erasure-coded payload. With a `(k, m)` erasure
configuration, each object is split into `k` data shards plus `m`
parity shards. All shards in a stripe have identical byte size
(original data is zero-padded to a multiple of `k`).

### Block

The on-disk representation of a shard. Each block has:
- A `BlockId` in the format `{object_id}-{shard_index:02}-v{ring_version}`
- A `BlockMeta` with the blake3 checksum, size, ring version, and shard index
- The raw shard data

### Storage Class

Objects are stored under one of two storage classes:

| Class | Strategy | Use Case |
|---|---|---|
| `Permanent` | Erasure-coded (k+m shards) | Final objects, long-term storage |
| `Transient` | Full replication (r copies) | Multipart upload parts, staging |

### Ring

The membership ring is an ordered, versioned list of active node IDs.
Every membership change produces a new ring with an incremented version.
Rendezvous hashing maps `(object_key, shard_index) → NodeId` given a
specific ring version.

### Erasure Config

Default: `k=3, m=2` → 5 shards total, fault tolerance depends on the
number of distinct nodes available (see [Configuration](#configuration)
for details).

---

## Data Flow: Upload

```
Client                          Nodes
  │                               │
  ├─ put(data) ───────────────────┤
  │  1. blake3 hash → object_id   │
  │  2. EC encode → k+m shards    │
  │  3. For each shard:           │
  │     a. rendezvous → target    │
  │     b. PutBlock gRPC ────────►│ store .blob + .meta
  │  4. Require k+1 acks          │
  │◄──── ObjectMetadata ──────────┤
```

1. **Hash**: `object_id = hex(blake3::hash(data))`
2. **Encode**: Reed-Solomon `encode(data, k, m)` produces `k+m` shards
3. **Place**: For each shard, compute rendezvous hash of `(object_id, shard_index)` against the ring to pick a target node
4. **Write**: gRPC `PutBlock(shard, meta)` to each target node. Each node writes atomically (tmp + rename)
5. **Quorum**: Succeeds if at least `k+1` of `k+m` shards are acknowledged

### Node Failure During Upload

The client iterates all nodes for each shard, trying fallback nodes if
the preferred target is unreachable. The quorum of `k+1` ensures
reconstruction is still possible after a minority of shard writes fail.

## Data Flow: Download

```
Client                          Nodes
  │                               │
  ├─ get(object_id) ──────────────┤
  │  1. For shard 0..total:       │
  │     a. rendezvous → target    │
  │     b. GetBlock gRPC ────────►│ read + verify checksum
  │     c. if ok → collect, ++got │
  │  2. Stop when got == k        │
  │  3. EC decode → original      │
  │◄──── data ────────────────────┤
```

At least `k` shards must be successfully fetched. The decode uses
Reed-Solomon `reconstruct_data` which fills in any missing data shards
from the available parity before concatenating.

## Data Flow: Multipart Upload

For objects exceeding the 20 MB single-put limit:

1. Split the payload into chunks
2. `put_part(upload_id, part_number, chunk)` — stores each chunk as a
   full replica (Transient class) on all nodes
3. Reassemble parts in order
4. `put(assembled_data)` — EC-encode and store permanently
5. `delete_part()` — clean up transient parts

## Erasure Coding

All erasure coding uses the `reed-solomon-erasure` crate with GF(256).

### Encoding

```
Input:  [byte payload of length L]
          ↓
Pad to   next multiple of k  →  padded_len = ceil(L / k) * k
          ↓
Split    into k data shards  →  each of size padded_len / k
          ↓
RS(k,m)  encode             →  m parity shards (same size)
          ↓
Output:  k+m Shard structs
```

### Decoding

```
Input:  at least k Shard structs (some may be missing = None)
          ↓
RS(k,m) reconstruct_data    →  fills missing data shards 0..k-1
          ↓
Concat  first k shards      →  padded byte array
          ↓
Truncate trailing zeros     →  original payload
```

**Note on padding**: Trailing zero stripping works for payloads that
do not end in zero bytes. For production use, the original length
should be stored in object metadata.

### Striped Encoding

For large payloads, `encode_striped()` splits data into fixed-size
stripes (default: `max_shard_bytes * k` each), encodes each stripe
independently, and returns `Vec<Vec<Shard>>`. This keeps per-shard
sizes under gRPC message limits. Not yet integrated into the client.

## Data Placement

Placement uses **rendezvous hashing** (highest random weight):

```
score(object_key, shard_index, node_id) =
    u64_from_le_bytes(blake3("{object_key}-{shard_index}" || node_id)[0..8])

winner = argmax(score) over all nodes in ring
```

Properties:
- **Deterministic**: same inputs → same node every time
- **Minimal redistribution**: adding a node remaps only ~1/N of objects
- **No central state**: any node can compute placement independently

### Storage Class Placement

| Class | Placement |
|---|---|
| `Permanent` | Each of `k+m` shards placed independently via `assign_node()`. May collide on the same node in small rings. |
| `Transient` | `r_factor` nodes selected, with deduplication via index offsets. |

## Membership

### SWIM Protocol

DFS uses the SWIM (Scalable Weakly-consistent Infection-style Process
Group Membership) protocol for failure detection:

1. **Direct probe**: Each tick (1s), pick a random alive peer and send
   a gRPC `Ping`. Expect `Pong` within `probe_timeout` (1s).
2. **Indirect probe**: If direct probe times out, ask `num_indirect`
   (2) peers to probe the suspect node on our behalf.
3. **Suspicion**: If indirect probes also fail, mark the node as
   `Suspect`. Broadcast suspicion to all peers via gossip messages.
4. **Dead**: If the suspect does not refute within `suspicion_timeout`
   (3s, or 3 ticks), mark as `Dead`, remove from ring, bump version,
   broadcast new ring to all peers.

### Ring Versioning

- Version starts at 1 (single node) or seed node's ring
- Incremented on every membership change
- Blocks are tagged with the ring version at write time
- The repair loop re-tags orphaned blocks after ring changes
- Stale-version blocks are detected and re-registered lazily

### Gossip Loop

The `gossip_loop::run()` background task drives the `Swimmer` at 1s
intervals and dispatches events:
- `Ping(id)` → gRPC Ping to that node's address
- `PingReq { target, via }` → gRPC Gossip to proxy nodes
- `Suspect(id)` / `Dead(id)` → broadcast gossip to all peers
- `RingChanged(ring)` → notify `repair_loop` via broadcast channel

## Storage Engine

### On-Disk Layout

```
{data_dir}/
  node.id                  ← stable node UUID (generated once)
  blocks/
    {block_id}.blob        ← raw shard data
    {block_id}.meta        ← bincode-encoded BlockMeta
```

### Atomic Writes

Every `write_block()` call:
1. Writes data to `{block_id}.blob.tmp`
2. Calls `fs::rename()` to atomically move into place
3. Writes metadata to `{block_id}.meta.tmp`
4. Calls `fs::rename()` to atomically move into place

This ensures a crash during write never corrupts a valid block.
Stale `.tmp` files from interrupted writes are cleaned up at startup.

### Integrity Verification

Every `read_block()` call verifies the blake3 checksum stored in the
`.meta` file against the data in the `.blob` file. A mismatch returns
`StorageError::ChecksumMismatch`.

### Block ID Format

```
{object_id}-{shard_index:02}-v{ring_version}

Examples:
  b3aead2514a1ad00-00-v1    ← shard 0, ring v1
  b3aead2514a1ad00-03-v2    ← shard 3, ring v2
  mpu-demo-part-00001-00-v1 ← transient part (multipart upload)
```

## Client Architecture

The client library provides a higher-level API:

```
DfsClient
  ├── put(&[u8])        → ObjectMetadata    (EC-encodes, writes k+m shards)
  ├── get(&str)         → Vec<u8>           (fetches k shards, decodes)
  ├── delete(&str)      → ()                (removes all k+m shards)
  ├── stat(&str)        → ObjectMetadata    (reads metadata from shard 0)
  ├── put_part(…)       → PartMetadata      (full replica on all nodes)
  ├── get_part(&str)    → Vec<u8>           (fetch from first node that responds)
  └── delete_part(&str) → ()                (delete from all nodes)
```

### Shard Write Strategy

The `put()` method tries each shard's preferred node first, then falls
back through all remaining nodes. It requires at least `k+1` successful
writes to return success. This handles partial cluster failures during
writes.

### Multipart Limits

- `put()` rejects payloads > 20 MB (prevents oversized gRPC messages)
- No per-part limit on `put_part()` — but each part is stored as a full
  replica on every node (O(N) write amplification)

## Protobuf Service

All communication uses gRPC over HTTP/2 with protocol buffers:

```protobuf
service DataNode {
  // Block CRUD
  rpc PutBlock(PutBlockRequest) returns (PutBlockResponse);
  rpc GetBlock(GetBlockRequest) returns (GetBlockResponse);
  rpc DeleteBlock(DeleteBlockRequest) returns (DeleteBlockResponse);
  rpc ListBlocks(ListBlocksRequest) returns (ListBlocksResponse);
  rpc BlockCount(BlockCountRequest) returns (BlockCountResponse);
  rpc StatBlock(StatBlockRequest) returns (StatBlockResponse);

  // Membership
  rpc Join(JoinRequest) returns (JoinResponse);
  rpc Gossip(GossipRequest) returns (GossipResponse);
  rpc Ping(PingRequest) returns (PongResponse);

  // Health
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

A legacy custom TCP wire protocol (`Frame { len:u32, kind:u8, payload }`)
also exists in `wire.rs` but is not actively used by the current client
— all production paths use gRPC.

## Repair Loop

When the ring changes (a node dies):
1. The repair loop receives a `RingChange` notification via broadcast channel
2. It enumerates all locally stored blocks
3. For each block with a stale ring version, it re-reads the data and
   re-writes it with the current ring version (re-tagging)
4. Full cross-node reconstruction (fetching `k` shards from peers to
   rebuild shards assigned to dead nodes) is **not yet implemented**

## Tracing and Observability

DFS supports OpenTelemetry tracing via OTLP export. Every gRPC call
and storage operation is instrumented with `tracing` spans. When Jaeger
or Tempo is configured, trace spans show the full lifecycle of a
`PutBlock` or `GetBlock` call including nested `write_block` /
`read_block` spans with correlated log events.

## Deterministic Simulation

The core domain logic (`dfs-core`) is intentionally pure — no I/O, no
async, no network. This allows it to be driven inside the
[turmoil](https://crates.io/crates/turmoil) deterministic simulator for
reproducible fault-injection testing. The `dfs-sim` crate (currently
commented out of the workspace) is intended to host these simulations.

## Crate Architecture

```
dfs-core (pure, no I/O)
├── types        — ErasureConfig, Ring, StorageClass, NodeId, ObjectId
├── block        — BlockId, BlockMeta (blake3 checksums), Shard
├── erasure      — Reed-Solomon encode/decode, striped support
├── placement    — Rendezvous hashing placement
├── membership   — Ring versioning, health tracking, rebalance
└── error        — CoreError enum (8 variants)

dfs-node (gRPC server + disk storage)
├── main.rs      — binary entrypoint
├── args.rs      — CLI argument parsing (clap)
├── storage.rs   — FsStorage: atomic local disk I/O
├── server.rs    — gRPC DataNode service implementation
├── gossip.rs    — SWIM protocol (tick-driven)
├── wire.rs      — legacy TCP wire protocol
├── tracing_setup.rs — OTLP/OpenTelemetry tracing init
└── background/
    ├── gossip_loop.rs  — periodic SWIM tick + dispatch
    └── repair_loop.rs  — re-tag blocks on ring changes

dfs-client (client library)
├── lib.rs       — DfsClient (put/get/delete/stat, multipart)
├── mem_store    — in-memory gRPC node for tests
└── examples/
    ├── hello_dfs.rs         — minimal put/get/delete
    ├── iso20022_payment.rs  — ISO 20022 payment (streaming)
    ├── recover.rs           — get by object ID
    └── common/iso20022.rs   — XML generator
```

## Configuration

### Default Erasure Config

| Parameter | Default | Description |
|---|---|---|
| `k` | 3 | Data shards (minimum needed for decode) |
| `m` | 2 | Parity shards |
| Total | 5 | Shards per object |
| Max shard loss | 2 | Tolerated by Reed-Solomon |
| Min shards | 3 | Needed for decode |

**Importantly**, node fault tolerance is NOT `m`. The `k+m` shards are
distributed across available nodes by round-robin. When there are fewer
than `k+m` nodes, multiple shards land on the same node. You lose all
shards on a failed node, so:

| Nodes | Shards per node | Node failure tolerance | Reason |
|---|---|---|---|
| 5+ | 1 | 2 | Each shard on its own node |
| 4 | 2,1,1,1 | 1 | Losing the 2-shard node kills 2 shards |
| **3** | **2,2,1** | **1** | **Losing any two nodes loses ≥3 shards > k-1=2** |
| 2 | 3,2 | 0 | Losing either node loses ≥2 shards ≥ k |

With the default config (k=3, m=2), you need **5 nodes** to achieve
the full `m=2` fault tolerance. With 3 nodes (typical for manual
testing), you can tolerate at most **1 node failure**.

### Node CLI Arguments

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `/tmp/dfs-node` | Storage directory |
| `--listen` | `127.0.0.1:9001` | gRPC listen address |
| `--join` | (none) | Existing node to join |
| `--log-json` | `false` | JSON log format |
| `--otlp-endpoint` | (none) | OTLP collector URL |
| `--service-name` | `dfs-node` | OTLP service name |

### SWIM Configuration

| Parameter | Default | Description |
|---|---|---|
| `probe_interval` | 1s | Time between gossip ticks |
| `probe_timeout` | 1s | Ping response timeout |
| `num_indirect` | 2 | Indirect probes per suspect |
| `suspicion_timeout` | 3 ticks | Suspect → Dead timeout |