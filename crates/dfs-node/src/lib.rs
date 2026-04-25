//! Library for the DFS data node.
//!
//! Exposes the storage engine, gRPC service, gossip protocol, and
//! tracing setup so that integration tests and the `dfs-sim` harness
//! can embed a full data node in-process.

pub mod args;
pub mod background;
pub mod gossip;
pub mod server;
pub mod storage;
pub mod tracing_setup;