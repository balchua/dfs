//! CLI argument parsing via [`clap`].

use clap::Parser;
use std::path::PathBuf;

/// Distributed file-system data node.
///
/// Each data node stores erasure-coded shards on its local disk and
/// participates in the cluster membership ring via gossip. Run one
/// or more instances on the same machine for manual testing (just
/// use different `--data-dir` and `--listen` ports).
#[derive(Parser, Debug, Clone)]
#[command(name = "dfs-node", version, about)]
pub struct Args {
    /// Directory for block storage.
    ///
    /// `{data-dir}/blocks/` holds `.blob` and `.meta` files.
    /// `{data-dir}/node.id` stores the stable node UUID.
    /// Multiple nodes can share a parent directory:
    ///
    /// ```sh
    /// ./target/debug/dfs-node --data-dir /tmp/node1 --listen :9001
    /// ./target/debug/dfs-node --data-dir /tmp/node2 --listen :9002
    /// ```
    #[arg(short = 'd', long, default_value = "/tmp/dfs-node")]
    pub data_dir: PathBuf,

    /// TCP address to listen on.
    ///
    /// Use `--listen :9001` (binds all interfaces) or
    /// `--listen 127.0.0.1:9001` (localhost only).
    #[arg(short = 'l', long, default_value = "127.0.0.1:9001")]
    pub listen: String,

    /// Address of an existing cluster node to join.
    ///
    /// When set, the node contacts this peer to fetch the current
    /// ring and announces itself. When omitted, the node starts a
    /// new single-node cluster.
    #[arg(short = 'j', long)]
    pub join: Option<String>,

    /// Print logs in JSON format (useful for `jq` / ElasticSearch).
    #[arg(long, default_value_t = false)]
    pub log_json: bool,

    /// OpenTelemetry collector endpoint (gRPC).
    ///
    /// When set, traces are exported via OTLP. Example:
    /// `--otlp-endpoint http://localhost:4317`.
    /// This correlates log events with trace spans in Jaeger/Tempo.
    #[arg(long)]
    pub otlp_endpoint: Option<String>,

    /// Service name reported to the OTLP collector.
    #[arg(long, default_value = "dfs-node")]
    pub service_name: String,
}