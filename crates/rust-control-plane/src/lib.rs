//! Rust library for building Envoy xDS control planes.
//!
//! Mirrors the shape of [envoyproxy/go-control-plane] and
//! [envoyproxy/java-control-plane]: a [`Cache`] trait with reference
//! implementations, a [`Server`] that exposes the xDS gRPC services over
//! tonic, and a [`ServerCallbacks`] hook for observability / admission.
//!
//! **Scope.** This crate handles the xDS wire protocol (version/nonce
//! bookkeeping, watch fan-out, resource ordering). It does *not* translate
//! from your domain model into Envoy resources, expose an operator-facing
//! "submit config" API, or persist state. Those are your control plane's
//! responsibility.
//!
//! [envoyproxy/go-control-plane]: https://github.com/envoyproxy/go-control-plane
//! [envoyproxy/java-control-plane]: https://github.com/envoyproxy/java-control-plane

pub mod cache;
pub mod callbacks;
pub mod resource;
pub mod server;
pub mod snapshot;

pub use cache::{Cache, FetchError, SnapshotCache, Watch};
pub use callbacks::ServerCallbacks;
pub use resource::{type_url, DynResource, XdsResource};
pub use server::{Server, XdsServices};
pub use snapshot::{Snapshot, SnapshotBuilder};
