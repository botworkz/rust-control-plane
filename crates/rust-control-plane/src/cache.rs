//! The [`Cache`] trait and the reference [`SnapshotCache`] implementation.
//!
//! Mirrors `pkg/cache/v3` in go-control-plane: caches are *passive*
//! stores driven by the user calling [`SnapshotCache::set_snapshot`].
//! The server queries them through [`Cache::create_watch`] /
//! [`Cache::fetch`] and receives a [`Watch`] (a `Stream`) it can forward
//! to Envoy.
//!
//! The cancellation contract is "drop the [`Watch`]". The server loop
//! drops watches when its stream ends or when Envoy sends a request
//! with a newer subscription, and the cache cleans up its bookkeeping
//! on the next set_snapshot call.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures_core::Stream;
use thiserror::Error;
use tokio::sync::{watch, Mutex};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;

use envoy_proto::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};

use crate::resource::DynResource;
use crate::snapshot::Snapshot;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("no snapshot for node {0}")]
    NoSnapshot(String),

    #[error("snapshot version matches request; nothing to send")]
    UpToDate,
}

/// Per-stream bookkeeping the server hands to the cache. Right now this
/// only carries the version Envoy last ack'd; it'll grow as we add delta
/// and richer ordering.
#[derive(Clone, Debug, Default)]
pub struct StreamState {
    pub last_version: String,
    pub last_nonce: String,
}

/// A cache hand-back. The contained stream yields one [`DiscoveryResponse`]
/// per snapshot version that the cache wants to push. Dropping the watch
/// cancels the subscription.
pub struct Watch {
    pub responses: Pin<Box<dyn Stream<Item = DiscoveryResponse> + Send>>,
}

/// Delta variant of [`Watch`]. P1 work — declared here so the trait
/// shape is right.
pub struct DeltaWatch {
    pub responses: Pin<Box<dyn Stream<Item = DeltaDiscoveryResponse> + Send>>,
}

/// The cache API consumed by the server. Implementors store resources
/// and decide when a watch should fire.
#[async_trait]
pub trait Cache: Send + Sync + 'static {
    async fn create_watch(&self, request: &DiscoveryRequest, state: &StreamState) -> Watch;

    async fn create_delta_watch(
        &self,
        request: &DeltaDiscoveryRequest,
        state: &StreamState,
    ) -> DeltaWatch;

    async fn fetch(&self, request: &DiscoveryRequest) -> Result<DiscoveryResponse, FetchError>;
}

/// Selects which snapshot to serve for a given Envoy node.
///
/// Defaults to `node.id`, matching go-control-plane's `IDHash`.
pub trait NodeHash: Send + Sync + 'static {
    fn hash(&self, node: Option<&envoy_proto::envoy::config::core::v3::Node>) -> String;
}

#[derive(Default, Clone, Copy)]
pub struct IdHash;

impl NodeHash for IdHash {
    fn hash(&self, node: Option<&envoy_proto::envoy::config::core::v3::Node>) -> String {
        node.map(|n| n.id.clone()).unwrap_or_default()
    }
}

/// Per-node entry in the snapshot cache. Holds the latest snapshot and a
/// `tokio::sync::watch` channel so live streams pick up new versions.
///
/// Using `watch` (coalesce-latest) rather than `mpsc` because for the
/// SotW common case the right behaviour is "send the most recent snapshot
/// and drop intermediates". This will need to be configurable per-type
/// once we tackle EDS endpoint flap — see issue #10 question 2.
struct NodeEntry {
    /// `None` = no snapshot yet.
    tx: watch::Sender<Option<Snapshot>>,
    rx: watch::Receiver<Option<Snapshot>>,
}

impl NodeEntry {
    fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self { tx, rx }
    }
}

pub struct SnapshotCache<H: NodeHash = IdHash> {
    hasher: H,
    nodes: Mutex<HashMap<String, Arc<NodeEntry>>>,
    /// `true` matches go-control-plane's "ADS" mode: we only serve a
    /// snapshot if it has every type the stream has subscribed to. Left
    /// off for the skeleton; will be wired through when we add ADS
    /// ordering.
    _ads: bool,
}

impl SnapshotCache<IdHash> {
    pub fn new() -> Self {
        Self::with_hasher(IdHash, false)
    }
}

impl Default for SnapshotCache<IdHash> {
    fn default() -> Self {
        Self::new()
    }
}

impl<H: NodeHash> SnapshotCache<H> {
    pub fn with_hasher(hasher: H, ads: bool) -> Self {
        Self {
            hasher,
            nodes: Mutex::new(HashMap::new()),
            _ads: ads,
        }
    }

    pub async fn set_snapshot(&self, node_hash: impl Into<String>, snapshot: Snapshot) {
        let key = node_hash.into();
        let mut guard = self.nodes.lock().await;
        let entry = guard
            .entry(key)
            .or_insert_with(|| Arc::new(NodeEntry::new()))
            .clone();
        drop(guard);
        // `send` only fails if there are no receivers; we always hold
        // one ourselves on the entry, so this is infallible in practice.
        let _ = entry.tx.send(Some(snapshot));
    }

    async fn entry_for(
        &self,
        node: Option<&envoy_proto::envoy::config::core::v3::Node>,
    ) -> Arc<NodeEntry> {
        let key = self.hasher.hash(node);
        let mut guard = self.nodes.lock().await;
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(NodeEntry::new()))
            .clone()
    }
}

fn build_response(
    type_url: &str,
    version: &str,
    resources: impl IntoIterator<Item = Arc<dyn DynResource>>,
) -> DiscoveryResponse {
    DiscoveryResponse {
        version_info: version.to_string(),
        resources: resources.into_iter().map(|r| r.to_any()).collect(),
        canary: false,
        type_url: type_url.to_string(),
        // Nonce is set by the server when it dispatches the response.
        nonce: String::new(),
        control_plane: None,
        resource_errors: Vec::new(),
    }
}

#[async_trait]
impl<H: NodeHash> Cache for SnapshotCache<H> {
    async fn create_watch(&self, request: &DiscoveryRequest, state: &StreamState) -> Watch {
        let type_url = request.type_url.clone();
        let last_version = state.last_version.clone();
        let entry = self.entry_for(request.node.as_ref()).await;

        let rx = entry.rx.clone();
        let stream = WatchStream::new(rx).filter_map(move |snap| {
            let snap = snap?;
            let version = snap.version(&type_url);
            if version == last_version {
                // Same version as last ack: don't re-send.
                return None;
            }
            Some(build_response(
                &type_url,
                version,
                snap.dyn_resources(&type_url),
            ))
        });

        Watch {
            responses: Box::pin(stream),
        }
    }

    async fn create_delta_watch(
        &self,
        _request: &DeltaDiscoveryRequest,
        _state: &StreamState,
    ) -> DeltaWatch {
        // P1 work — see issue #4. For now hand back an empty stream so
        // the trait is fully implemented.
        DeltaWatch {
            responses: Box::pin(tokio_stream::empty()),
        }
    }

    async fn fetch(&self, request: &DiscoveryRequest) -> Result<DiscoveryResponse, FetchError> {
        let entry = self.entry_for(request.node.as_ref()).await;
        let snap = entry
            .rx
            .borrow()
            .clone()
            .ok_or_else(|| FetchError::NoSnapshot(self.hasher.hash(request.node.as_ref())))?;
        let version = snap.version(&request.type_url);
        if version == request.version_info {
            return Err(FetchError::UpToDate);
        }
        Ok(build_response(
            &request.type_url,
            version,
            snap.dyn_resources(&request.type_url),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::type_url;
    use crate::snapshot::Snapshot;
    use envoy_proto::envoy::config::cluster::v3::Cluster;
    use envoy_proto::envoy::config::core::v3::Node;

    fn cluster(name: &str) -> Cluster {
        Cluster {
            name: name.into(),
            ..Default::default()
        }
    }

    fn req(node_id: &str, type_url: &str, version: &str) -> DiscoveryRequest {
        DiscoveryRequest {
            version_info: version.into(),
            node: Some(Node {
                id: node_id.into(),
                ..Default::default()
            }),
            resource_names: vec![],
            resource_locators: vec![],
            type_url: type_url.into(),
            response_nonce: String::new(),
            error_detail: None,
        }
    }

    #[tokio::test]
    async fn fetch_returns_latest_snapshot() {
        let cache = SnapshotCache::new();
        let snap = Snapshot::builder()
            .with(cluster("a"))
            .version_all("v1")
            .build();
        cache.set_snapshot("node-1", snap).await;

        let resp = cache
            .fetch(&req("node-1", type_url::CLUSTER, ""))
            .await
            .expect("fetch");
        assert_eq!(resp.version_info, "v1");
        assert_eq!(resp.resources.len(), 1);
    }

    #[tokio::test]
    async fn fetch_up_to_date_errors() {
        let cache = SnapshotCache::new();
        cache
            .set_snapshot(
                "node-1",
                Snapshot::builder()
                    .with(cluster("a"))
                    .version_all("v1")
                    .build(),
            )
            .await;
        let err = cache
            .fetch(&req("node-1", type_url::CLUSTER, "v1"))
            .await
            .unwrap_err();
        assert!(matches!(err, FetchError::UpToDate));
    }

    #[tokio::test]
    async fn fetch_unknown_node_errors() {
        let cache = SnapshotCache::new();
        let err = cache
            .fetch(&req("ghost", type_url::CLUSTER, ""))
            .await
            .unwrap_err();
        assert!(matches!(err, FetchError::NoSnapshot(_)));
    }

    #[tokio::test]
    async fn watch_yields_on_new_snapshot() {
        let cache = SnapshotCache::new();

        let watch = cache
            .create_watch(
                &req("node-1", type_url::CLUSTER, ""),
                &StreamState::default(),
            )
            .await;
        let mut stream = watch.responses;

        cache
            .set_snapshot(
                "node-1",
                Snapshot::builder()
                    .with(cluster("a"))
                    .version_all("v1")
                    .build(),
            )
            .await;

        let resp = stream.next().await.expect("response");
        assert_eq!(resp.version_info, "v1");
        assert_eq!(resp.resources.len(), 1);
    }

    #[tokio::test]
    async fn watch_skips_identical_version() {
        let cache = SnapshotCache::new();
        cache
            .set_snapshot(
                "node-1",
                Snapshot::builder()
                    .with(cluster("a"))
                    .version_all("v1")
                    .build(),
            )
            .await;

        // Already at v1: opening a watch with last_version="v1" should
        // not immediately yield.
        let state = StreamState {
            last_version: "v1".into(),
            ..Default::default()
        };
        let mut watch = cache
            .create_watch(&req("node-1", type_url::CLUSTER, "v1"), &state)
            .await;

        // No new snapshot, no yield.
        tokio::select! {
            _ = watch.responses.next() => panic!("should not yield without a new snapshot"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {},
        }

        // Push v2 — now we should get a response.
        cache
            .set_snapshot(
                "node-1",
                Snapshot::builder()
                    .with(cluster("a"))
                    .with(cluster("b"))
                    .version_all("v2")
                    .build(),
            )
            .await;
        let resp = watch.responses.next().await.expect("v2 response");
        assert_eq!(resp.version_info, "v2");
        assert_eq!(resp.resources.len(), 2);
    }
}
