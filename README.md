# rust-control-plane

Library for building [Envoy](https://www.envoyproxy.io/) xDS control planes
in Rust, mirroring the shape of
[envoyproxy/go-control-plane](https://github.com/envoyproxy/go-control-plane)
and
[envoyproxy/java-control-plane](https://github.com/envoyproxy/java-control-plane).

Built on top of [envoy-proto-rs](https://github.com/phlax/envoy-proto-rs),
which provides the generated protobuf / tonic types.

> **Status:** pre-alpha. The current code is the MVP skeleton — see the
> tracking issues for the road to P0.

## Scope

**What this crate does**

- Serves the xDS wire protocol (ADS, SotW, REST; delta is P1).
- Tracks per-stream version / nonce / subscription state.
- Provides reference caches (`SnapshotCache` today; `LinearCache`,
  `MuxCache` in P2).

**What this crate explicitly does *not* do** — mirroring the
go-control-plane / java-control-plane scope:

- It does **not** translate from your domain model (CRDs, service
  registry entries, file watchers, …) into Envoy resources. That is
  *your* control plane's job.
- It does **not** define an operator-facing "submit config" API.
  Build your own.
- It does **not** persist state. Caches are in-memory.
- It does **not** ship a runnable daemon. Examples are examples.

If you want a runnable thing, write ~50 lines on top: build a snapshot,
push it to a `SnapshotCache`, register the services on a `tonic` server,
point Envoy at it.

## Quickstart

```rust,no_run
use std::sync::Arc;

use envoy_proto::envoy::config::cluster::v3::Cluster;
use rust_control_plane::{Server, Snapshot, SnapshotCache};
use tonic::transport::Server as TonicServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = Arc::new(SnapshotCache::new());

    let snap = Snapshot::builder()
        .with(Cluster { name: "demo".into(), ..Default::default() })
        .version_all("v1")
        .build();
    cache.set_snapshot("test-node", snap).await;

    let services = Server::new(cache).into_services();

    TonicServer::builder()
        .add_service(services.ads)
        .add_service(services.cds)
        .serve("0.0.0.0:18000".parse()?)
        .await?;

    Ok(())
}
```

## Layout

| Module      | What's in it                                              |
|-------------|-----------------------------------------------------------|
| `resource`  | `XdsResource` trait + type URLs                           |
| `snapshot`  | `Snapshot` + `SnapshotBuilder`                            |
| `cache`     | `Cache` trait, `SnapshotCache`, `Watch`, `StreamState`    |
| `callbacks` | `ServerCallbacks` trait (default-impls everything)        |
| `server`    | `Server`, `XdsServices`, internal SotW stream loop        |

## License

Apache-2.0.
