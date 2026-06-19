//! Minimal control plane that serves one hard-coded snapshot to every
//! Envoy that connects, mirroring `internal/example` in go-control-plane.
//!
//! Run:
//!
//! ```sh
//! cargo run --example hello_snapshot
//! envoy -c examples/hello_snapshot/envoy.yaml
//! ```

use std::sync::Arc;

use envoy_proto::envoy::config::cluster::v3::{
    cluster::{ClusterDiscoveryType, DiscoveryType},
    Cluster,
};
use rust_control_plane::{Server, Snapshot, SnapshotCache};
use tonic::transport::Server as TonicServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache: Arc<SnapshotCache> = Arc::new(SnapshotCache::new());

    let snap = Snapshot::builder()
        .with(Cluster {
            name: "demo_cluster".into(),
            cluster_discovery_type: Some(ClusterDiscoveryType::Type(
                DiscoveryType::LogicalDns as i32,
            )),
            ..Default::default()
        })
        .version_all("v1")
        .build();
    cache.set_snapshot("test-id", snap).await;

    let services = Server::new(cache).into_services();

    let addr = "0.0.0.0:18000".parse()?;
    eprintln!("xDS listening on {addr}");
    TonicServer::builder()
        .add_service(services.ads)
        .add_service(services.cds)
        .serve(addr)
        .await?;

    Ok(())
}
