//! Data node binary: starts a gRPC server with local disk storage.
//!
//! # Usage
//! ```sh
//! cargo run -p dfs-node -- --data-dir /tmp/node1 --listen :9001
//! cargo run -p dfs-node -- --data-dir /tmp/node2 --listen :9002 --join 127.0.0.1:9001
//! ```

use dfs_node_lib::background::gossip_loop;
use dfs_node_lib::background::repair_loop;
use dfs_node_lib::gossip::Swimmer;
use dfs_node_lib::server::DataNodeService;
use dfs_node_lib::server::proto::data_node_server::DataNodeServer;
use dfs_node_lib::tracing_setup::TraceConfig;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::{Mutex, broadcast};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = <dfs_node_lib::args::Args as clap::Parser>::parse();

    dfs_node_lib::tracing_setup::init_tracing(TraceConfig {
        json: args.log_json,
        otlp_endpoint: args.otlp_endpoint.clone(),
        service_name: args.service_name.clone(),
    })?;

    let _cleaned = dfs_node_lib::storage::cleanup_stale_tmp(&args.data_dir).await?;

    let disk = dfs_node_lib::storage::FsStorage::new(args.data_dir.clone())?;
    let node_id = disk.read_or_create_node_id()?;
    info!(
        %node_id,
        listen = %args.listen,
        data_dir = %args.data_dir.display(),
        "starting data node"
    );

    // ── Ring setup ────────────────────────────────────────────────
    let (ring_tx, ring_rx) = broadcast::channel::<gossip_loop::RingChange>(16);

    let swimmer = Arc::new(Mutex::new(Swimmer::new(
        node_id,
        vec![node_id],
        None, // production uses os_rng
    )));

    // ── Join cluster if requested ─────────────────────────────────
    if let Some(ref join_addr) = args.join {
        info!(%join_addr, "joining cluster");
    }

    // ── Background tasks ──────────────────────────────────────────
    let gossip_handle = tokio::spawn(gossip_loop::run(Arc::clone(&swimmer), ring_tx, vec![]));

    let repair_handle = tokio::spawn(repair_loop::run(disk.clone(), ring_rx));

    // ── gRPC server ───────────────────────────────────────────────
    let addr = args.listen.parse()?;
    let svc = DataNodeService::with_membership(disk, Arc::clone(&swimmer));
    let server = DataNodeServer::new(svc);

    let grpc = tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr);

    info!(%addr, "gRPC server listening");

    tokio::select! {
        res = grpc => {
            if let Err(e) = res {
                tracing::error!(error = %e, "gRPC server error");
            }
        }
        _ = signal::ctrl_c() => {
            info!("received shutdown signal, draining connections");
        }
    }

    gossip_handle.abort();
    repair_handle.abort();
    info!("node stopped");
    Ok(())
}
