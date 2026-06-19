//! The [`XdsResource`] trait and type-URL constants.
//!
//! `XdsResource` is the bridge between concrete generated prost types and
//! the heterogeneous storage in caches. It carries the resource's xDS
//! type URL as an associated const so callers never have to thread it
//! around manually, and exposes the resource's name (which lives in
//! different fields across types — `Cluster.name`, `Listener.name`,
//! `ClusterLoadAssignment.cluster_name`, …).

use std::any::Any;

use prost::Message;
use prost_types::Any as ProtoAny;

use envoy_proto::envoy::config::cluster::v3::Cluster;
use envoy_proto::envoy::config::endpoint::v3::ClusterLoadAssignment;
use envoy_proto::envoy::config::listener::v3::Listener;
use envoy_proto::envoy::config::route::v3::{RouteConfiguration, ScopedRouteConfiguration};
use envoy_proto::envoy::extensions::transport_sockets::tls::v3::Secret;
use envoy_proto::envoy::service::runtime::v3::Runtime;

/// xDS type URLs, matching `pkg/resource/v3` in go-control-plane.
pub mod type_url {
    pub const CLUSTER: &str = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
    pub const ENDPOINT: &str = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
    pub const LISTENER: &str = "type.googleapis.com/envoy.config.listener.v3.Listener";
    pub const ROUTE: &str = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
    pub const SCOPED_ROUTE: &str =
        "type.googleapis.com/envoy.config.route.v3.ScopedRouteConfiguration";
    pub const SECRET: &str = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret";
    pub const RUNTIME: &str = "type.googleapis.com/envoy.service.runtime.v3.Runtime";

    /// All root resource type URLs, in the canonical ordering Envoy
    /// expects (CDS before EDS before LDS before RDS, etc.). See
    /// `pkg/cache/v3/order.go` in go-control-plane.
    pub const ORDERED: &[&str] = &[
        CLUSTER,
        ENDPOINT,
        LISTENER,
        ROUTE,
        SCOPED_ROUTE,
        SECRET,
        RUNTIME,
    ];
}

/// A resource that can be served over xDS.
///
/// Implementors are generated prost messages plus a static type URL and a
/// way to extract the name. The crate provides impls for the seven root
/// resource types; downstream users can implement this for contrib /
/// extension types.
pub trait XdsResource: Message + Clone + Default + Send + Sync + 'static {
    const TYPE_URL: &'static str;

    /// The name used to look this resource up by Envoy.
    fn name(&self) -> &str;
}

/// Object-safe shadow of [`XdsResource`] used for heterogeneous storage in
/// caches. Implemented blanket-style for every `T: XdsResource`.
pub trait DynResource: Send + Sync + 'static {
    fn type_url(&self) -> &'static str;
    fn name(&self) -> &str;

    /// Pack into a `google.protobuf.Any` for serving over the wire.
    fn to_any(&self) -> ProtoAny;

    /// Downcast escape hatch. Mostly for tests.
    fn as_any(&self) -> &dyn Any;
}

impl<T: XdsResource> DynResource for T {
    fn type_url(&self) -> &'static str {
        T::TYPE_URL
    }

    fn name(&self) -> &str {
        XdsResource::name(self)
    }

    fn to_any(&self) -> ProtoAny {
        ProtoAny {
            type_url: T::TYPE_URL.to_string(),
            value: self.encode_to_vec(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

macro_rules! impl_xds_resource {
    ($ty:ty, $type_url:expr, $name_field:ident) => {
        impl XdsResource for $ty {
            const TYPE_URL: &'static str = $type_url;

            fn name(&self) -> &str {
                &self.$name_field
            }
        }
    };
}

impl_xds_resource!(Cluster, type_url::CLUSTER, name);
impl_xds_resource!(ClusterLoadAssignment, type_url::ENDPOINT, cluster_name);
impl_xds_resource!(Listener, type_url::LISTENER, name);
impl_xds_resource!(RouteConfiguration, type_url::ROUTE, name);
impl_xds_resource!(ScopedRouteConfiguration, type_url::SCOPED_ROUTE, name);
impl_xds_resource!(Secret, type_url::SECRET, name);
impl_xds_resource!(Runtime, type_url::RUNTIME, name);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_round_trip_through_dyn_resource() {
        let c = Cluster {
            name: "demo".into(),
            ..Default::default()
        };
        let dynr: &dyn DynResource = &c;
        assert_eq!(dynr.type_url(), type_url::CLUSTER);
        assert_eq!(dynr.name(), "demo");

        let any = dynr.to_any();
        assert_eq!(any.type_url, type_url::CLUSTER);
        let decoded = Cluster::decode(any.value.as_slice()).expect("decode");
        assert_eq!(decoded.name, "demo");
    }
}
