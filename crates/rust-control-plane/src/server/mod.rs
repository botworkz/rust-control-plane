//! gRPC service bundle that turns a [`Cache`] + [`ServerCallbacks`]
//! into the tonic services Envoy talks to.
//!
//! Mirrors `pkg/server/v3/server.go`. We deliberately do **not** own
//! the `tonic::transport::Server` — users plug our services into their
//! own gRPC server so they keep control of TLS, interceptors,
//! reflection, etc.
//!
//! Currently exposes ADS (the multiplexed bidi) and CDS (per-type
//! example). The other six per-type services are the same shape; once
//! the ADS impl bakes we'll generate them via a small macro or copy.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use envoy_proto::envoy::service::cluster::v3::cluster_discovery_service_server::{
    ClusterDiscoveryService, ClusterDiscoveryServiceServer,
};
use envoy_proto::envoy::service::discovery::v3::aggregated_discovery_service_server::{
    AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
};
use envoy_proto::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};

use crate::cache::Cache;
use crate::callbacks::{NoopCallbacks, ServerCallbacks};
use crate::resource::type_url;

mod stream;

/// User-facing entry point. Build with a cache, optionally a callbacks
/// impl, then call [`Server::into_services`] to plug into a tonic
/// `Server`.
pub struct Server<C: Cache, Cb: ServerCallbacks = NoopCallbacks> {
    cache: Arc<C>,
    callbacks: Arc<Cb>,
}

impl<C: Cache> Server<C, NoopCallbacks> {
    pub fn new(cache: Arc<C>) -> Self {
        Self {
            cache,
            callbacks: Arc::new(NoopCallbacks),
        }
    }
}

impl<C: Cache, Cb: ServerCallbacks> Server<C, Cb> {
    pub fn with_callbacks<Cb2: ServerCallbacks>(self, cb: Cb2) -> Server<C, Cb2> {
        Server {
            cache: self.cache,
            callbacks: Arc::new(cb),
        }
    }

    /// Produce the bundle of gRPC services Envoy expects to find on
    /// the management port.
    pub fn into_services(self) -> XdsServices<C, Cb> {
        let Self { cache, callbacks } = self;
        XdsServices {
            ads: AggregatedDiscoveryServiceServer::new(AdsHandler::new(
                cache.clone(),
                callbacks.clone(),
            )),
            cds: ClusterDiscoveryServiceServer::new(ClusterHandler::new(cache, callbacks)),
        }
    }
}

/// Bundle of tonic services to plug into a `tonic::transport::Server`.
pub struct XdsServices<C: Cache, Cb: ServerCallbacks> {
    pub ads: AggregatedDiscoveryServiceServer<AdsHandler<C, Cb>>,
    pub cds: ClusterDiscoveryServiceServer<ClusterHandler<C, Cb>>,
    // P0 follow-up: add EDS / LDS / RDS / SDS / RTDS / VHDS in the same
    // pattern. Tracked in #3.
}

// ---- ADS ----------------------------------------------------------------

pub struct AdsHandler<C: Cache, Cb: ServerCallbacks> {
    cache: Arc<C>,
    callbacks: Arc<Cb>,
}

impl<C: Cache, Cb: ServerCallbacks> AdsHandler<C, Cb> {
    fn new(cache: Arc<C>, callbacks: Arc<Cb>) -> Self {
        Self { cache, callbacks }
    }
}

#[async_trait]
impl<C: Cache, Cb: ServerCallbacks> AggregatedDiscoveryService for AdsHandler<C, Cb> {
    type StreamAggregatedResourcesStream = ReceiverStream<Result<DiscoveryResponse, Status>>;

    async fn stream_aggregated_resources(
        &self,
        request: Request<Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamAggregatedResourcesStream>, Status> {
        let inbound = request.into_inner();
        let stream = stream::run_sotw_stream(
            self.cache.clone(),
            self.callbacks.clone(),
            // ADS leaves type_url empty — every request carries its own.
            "",
            inbound,
        )
        .await;
        Ok(Response::new(stream))
    }

    type DeltaAggregatedResourcesStream = ReceiverStream<Result<DeltaDiscoveryResponse, Status>>;

    async fn delta_aggregated_resources(
        &self,
        _request: Request<Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<Response<Self::DeltaAggregatedResourcesStream>, Status> {
        // P1 work — see issue #4.
        Err(Status::unimplemented(
            "delta ADS is part of P1 — see https://github.com/phlax/rust-control-plane/issues",
        ))
    }
}

// ---- CDS (per-type example) ---------------------------------------------

pub struct ClusterHandler<C: Cache, Cb: ServerCallbacks> {
    cache: Arc<C>,
    callbacks: Arc<Cb>,
}

impl<C: Cache, Cb: ServerCallbacks> ClusterHandler<C, Cb> {
    fn new(cache: Arc<C>, callbacks: Arc<Cb>) -> Self {
        Self { cache, callbacks }
    }
}

#[async_trait]
impl<C: Cache, Cb: ServerCallbacks> ClusterDiscoveryService for ClusterHandler<C, Cb> {
    type StreamClustersStream = ReceiverStream<Result<DiscoveryResponse, Status>>;

    async fn stream_clusters(
        &self,
        request: Request<Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamClustersStream>, Status> {
        let inbound = request.into_inner();
        let stream = stream::run_sotw_stream(
            self.cache.clone(),
            self.callbacks.clone(),
            type_url::CLUSTER,
            inbound,
        )
        .await;
        Ok(Response::new(stream))
    }

    type DeltaClustersStream = ReceiverStream<Result<DeltaDiscoveryResponse, Status>>;

    async fn delta_clusters(
        &self,
        _request: Request<Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<Response<Self::DeltaClustersStream>, Status> {
        Err(Status::unimplemented("delta CDS is part of P1"))
    }

    async fn fetch_clusters(
        &self,
        request: Request<DiscoveryRequest>,
    ) -> Result<Response<DiscoveryResponse>, Status> {
        let mut req = request.into_inner();
        if req.type_url.is_empty() {
            req.type_url = type_url::CLUSTER.to_string();
        }
        self.callbacks.on_fetch_request(&req).await?;
        match self.cache.fetch(&req).await {
            Ok(resp) => {
                self.callbacks.on_fetch_response(&req, &resp).await;
                Ok(Response::new(resp))
            }
            Err(crate::cache::FetchError::UpToDate) => {
                // 304-equivalent. g-c-p uses a NOT_FOUND with a specific
                // string here; we mirror that until we have a better
                // story.
                Err(Status::not_found("up to date"))
            }
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }
}
