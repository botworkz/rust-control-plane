//! User-supplied hooks the server calls on stream lifecycle events.
//!
//! Mirrors `pkg/server/v3/server.go`'s `Callbacks` interface. All
//! methods have a default no-op implementation so callers override only
//! what they care about.

use async_trait::async_trait;
use tonic::Status;

use envoy_proto::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};

#[async_trait]
pub trait ServerCallbacks: Send + Sync + 'static {
    async fn on_stream_open(&self, _stream_id: i64, _type_url: &str) -> Result<(), Status> {
        Ok(())
    }

    async fn on_stream_closed(&self, _stream_id: i64) {}

    async fn on_stream_request(
        &self,
        _stream_id: i64,
        _request: &DiscoveryRequest,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn on_stream_response(
        &self,
        _stream_id: i64,
        _request: &DiscoveryRequest,
        _response: &DiscoveryResponse,
    ) {
    }

    async fn on_delta_stream_open(&self, _stream_id: i64, _type_url: &str) -> Result<(), Status> {
        Ok(())
    }

    async fn on_delta_stream_closed(&self, _stream_id: i64) {}

    async fn on_delta_stream_request(
        &self,
        _stream_id: i64,
        _request: &DeltaDiscoveryRequest,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn on_delta_stream_response(
        &self,
        _stream_id: i64,
        _request: &DeltaDiscoveryRequest,
        _response: &DeltaDiscoveryResponse,
    ) {
    }

    async fn on_fetch_request(&self, _request: &DiscoveryRequest) -> Result<(), Status> {
        Ok(())
    }

    async fn on_fetch_response(&self, _request: &DiscoveryRequest, _response: &DiscoveryResponse) {}
}

/// No-op callbacks. Used as the default when the user doesn't supply
/// their own.
#[derive(Default, Clone, Copy)]
pub struct NoopCallbacks;

#[async_trait]
impl ServerCallbacks for NoopCallbacks {}
