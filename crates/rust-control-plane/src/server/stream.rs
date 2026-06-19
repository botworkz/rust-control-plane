//! Shared SotW stream loop. Used by ADS and the per-type services.
//!
//! Receives [`DiscoveryRequest`]s from Envoy, opens [`Watch`]es against
//! the cache, multiplexes the cache's responses back out, and tracks
//! the per-type version/nonce state needed to interpret ack/nack.
//!
//! **What works:** initial requests, version-bump ack handling, cache
//! fan-out, drop-on-disconnect.
//!
//! **What's stubbed (P0 follow-ups):** NACK detection beyond "version
//! didn't match", resource_names subscription deltas, ADS ordering
//! across type URLs, the full `StreamState` g-c-p tracks. See issue #3.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use futures_core::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{debug, info, trace, warn, Instrument};

use envoy_proto::envoy::service::discovery::v3::{DiscoveryRequest, DiscoveryResponse};

use crate::cache::{Cache, StreamState};
use crate::callbacks::ServerCallbacks;

/// Process-wide stream id counter, handed to callbacks. Mirrors
/// g-c-p's `streamID` atomic in `pkg/server/sotw/v3/server.go`.
static NEXT_STREAM_ID: AtomicI64 = AtomicI64::new(1);

/// Monotonic nonce source. xDS only requires that nonces be unique
/// within a stream, but a global counter is just as correct and is
/// easier to debug across logs.
static NEXT_NONCE: AtomicU64 = AtomicU64::new(1);

fn new_stream_id() -> i64 {
    NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed)
}

fn new_nonce() -> String {
    NEXT_NONCE.fetch_add(1, Ordering::Relaxed).to_string()
}

/// Per-stream, per-type state. ADS multiplexes many type URLs across one
/// stream, so we keep one of these per type URL we've heard from.
#[derive(Default, Debug, Clone)]
struct TypeState {
    /// The version Envoy has most recently *successfully* applied.
    /// Updated whenever we observe an ack (response_nonce matches the
    /// last nonce we sent, no error_detail).
    acked_version: String,
    /// The nonce we sent on the most recent response of this type.
    /// Used to disambiguate acks ("nonce matches") from stale requests
    /// ("nonce doesn't match — ignore").
    last_sent_nonce: String,
}

/// Run a SotW bidi stream to completion.
///
/// - `default_type_url`: empty for ADS, the type URL for per-type
///   services. Mirrors how g-c-p's `process` is called.
/// - Returns immediately with a `ReceiverStream`; the actual loop runs
///   in a spawned task until Envoy hangs up or sends a malformed
///   request.
pub(crate) async fn run_sotw_stream<C: Cache, Cb: ServerCallbacks>(
    cache: Arc<C>,
    callbacks: Arc<Cb>,
    default_type_url: &'static str,
    mut requests: Streaming<DiscoveryRequest>,
) -> ReceiverStream<Result<DiscoveryResponse, Status>> {
    let stream_id = new_stream_id();
    let (resp_tx, resp_rx) = mpsc::channel::<Result<DiscoveryResponse, Status>>(16);

    let span = tracing::info_span!("xds_stream", stream_id, default_type_url);

    tokio::spawn(
        async move {
            info!("stream opened");
            if let Err(status) = callbacks.on_stream_open(stream_id, default_type_url).await {
                let _ = resp_tx.send(Err(status)).await;
                return;
            }

            // Per-type bookkeeping. Keyed by type URL.
            let mut type_states: HashMap<String, TypeState> = HashMap::new();

            // One in-flight watch per type URL. Dropping replaces it.
            let mut watches: HashMap<String, WatchTask> = HashMap::new();

            loop {
                tokio::select! {
                    // Envoy sent us a request.
                    maybe_req = requests.next() => {
                        let Some(req_result) = maybe_req else {
                            debug!("client closed stream");
                            break;
                        };
                        let mut req = match req_result {
                            Ok(req) => req,
                            Err(status) => {
                                warn!(?status, "stream error from client");
                                let _ = resp_tx.send(Err(status)).await;
                                break;
                            }
                        };

                        // Per-type services don't populate type_url on the
                        // request — g-c-p fills it in from the service the
                        // client connected to. Do the same.
                        if req.type_url.is_empty() && !default_type_url.is_empty() {
                            req.type_url = default_type_url.to_string();
                        }
                        if req.type_url.is_empty() {
                            let _ = resp_tx
                                .send(Err(Status::invalid_argument(
                                    "type_url is required on ADS requests",
                                )))
                                .await;
                            break;
                        }

                        if let Err(status) = callbacks
                            .on_stream_request(stream_id, &req)
                            .await
                        {
                            let _ = resp_tx.send(Err(status)).await;
                            break;
                        }

                        let type_state = type_states
                            .entry(req.type_url.clone())
                            .or_default();

                        let kind = classify_request(&req, type_state);
                        trace!(?kind, type_url = %req.type_url, "request");

                        match kind {
                            RequestKind::AckOf(version) => {
                                type_state.acked_version = version;
                                // No new watch — the existing one is
                                // still valid and will fire on the next
                                // snapshot.
                            }
                            RequestKind::Nack { rejected_version, message } => {
                                warn!(
                                    rejected_version,
                                    message,
                                    type_url = %req.type_url,
                                    "client NACK"
                                );
                                // g-c-p re-arms the watch from
                                // last-acked. We do the same by leaving
                                // the existing watch in place; the next
                                // snapshot will retry.
                            }
                            RequestKind::Stale => {
                                // Old nonce, ignore.
                            }
                            RequestKind::InitialOrResubscribe => {
                                // (Re)open a watch with the current
                                // acked-version. Dropping the previous
                                // task tells the cache we no longer
                                // care.
                                let state = StreamState {
                                    last_version: type_state.acked_version.clone(),
                                    last_nonce: type_state.last_sent_nonce.clone(),
                                };
                                let watch = cache.create_watch(&req, &state).await;
                                let task = WatchTask::spawn(req.clone(), watch.responses);
                                watches.insert(req.type_url.clone(), task);
                            }
                        }
                    }

                    // The cache pushed a new response for some type.
                    Some((type_url, request_snapshot, response)) =
                        next_watch_response(&mut watches) =>
                    {
                        let mut response = response;
                        let nonce = new_nonce();
                        response.nonce = nonce.clone();

                        if let Some(state) = type_states.get_mut(&type_url) {
                            state.last_sent_nonce = nonce.clone();
                        }

                        callbacks
                            .on_stream_response(stream_id, &request_snapshot, &response)
                            .await;

                        if resp_tx.send(Ok(response)).await.is_err() {
                            debug!("response channel closed by transport");
                            break;
                        }
                    }
                }
            }

            callbacks.on_stream_closed(stream_id).await;
            info!("stream closed");
        }
        .instrument(span),
    );

    ReceiverStream::new(resp_rx)
}

/// What kind of message did Envoy just send us?
#[derive(Debug)]
enum RequestKind {
    /// First request for this type, or one that asks us to re-subscribe
    /// (new resource_names, etc.). We need to (re)open a cache watch.
    InitialOrResubscribe,
    /// An ack of the version we last sent. Update bookkeeping; no need
    /// to re-watch.
    AckOf(String),
    /// A nack. Same handling as ack for now (re-arm via cache); logged.
    Nack {
        rejected_version: String,
        message: String,
    },
    /// Nonce doesn't match anything we sent recently; old in-flight
    /// message we should ignore.
    Stale,
}

fn classify_request(req: &DiscoveryRequest, state: &TypeState) -> RequestKind {
    // Initial: no nonce on the wire at all.
    if req.response_nonce.is_empty() {
        return RequestKind::InitialOrResubscribe;
    }
    // Not the nonce we last sent → it's a late ack for something we've
    // already moved past. Drop it.
    if req.response_nonce != state.last_sent_nonce {
        return RequestKind::Stale;
    }
    if let Some(err) = &req.error_detail {
        return RequestKind::Nack {
            rejected_version: req.version_info.clone(),
            message: err.message.clone(),
        };
    }
    RequestKind::AckOf(req.version_info.clone())
}

/// A live cache watch + the request that opened it (so callbacks can
/// see what Envoy asked for).
struct WatchTask {
    rx: mpsc::Receiver<DiscoveryResponse>,
    request: DiscoveryRequest,
}

impl WatchTask {
    fn spawn(
        request: DiscoveryRequest,
        mut stream: std::pin::Pin<Box<dyn Stream<Item = DiscoveryResponse> + Send>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(resp) = stream.next().await {
                if tx.send(resp).await.is_err() {
                    break;
                }
            }
        });
        Self { rx, request }
    }
}

/// Poll every active watch; return the first response we see along with
/// its type URL and the originating request.
///
/// Implemented by hand (rather than `futures::stream::SelectAll`) so we
/// can carry the type URL out without an extra wrapper type, and so we
/// can quickly evict watches that have closed.
async fn next_watch_response(
    watches: &mut HashMap<String, WatchTask>,
) -> Option<(String, DiscoveryRequest, DiscoveryResponse)> {
    if watches.is_empty() {
        // `tokio::select!` polls this branch even when it returns
        // `None`, so we have to genuinely never resolve here.
        std::future::pending::<()>().await;
        return None;
    }

    // O(n) over open watches per response. Fine for the watches a
    // single Envoy holds (max ~7 type URLs); revisit if we ever fan one
    // stream across many.
    //
    // We loop until either some `try_recv` returns a payload or every
    // watch is disconnected; in the latter case we fall back to
    // `pending` so `select!` keeps the other branch alive.
    loop {
        let mut closed: Vec<String> = Vec::new();
        for (type_url, task) in watches.iter_mut() {
            match task.rx.try_recv() {
                Ok(resp) => return Some((type_url.clone(), task.request.clone(), resp)),
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => closed.push(type_url.clone()),
            }
        }
        for k in closed {
            watches.remove(&k);
        }
        if watches.is_empty() {
            std::future::pending::<()>().await;
            return None;
        }
        // Yield so we don't tight-loop. A proper implementation would
        // `select_all` over `Notified` handles per task; this is the
        // smallest correct thing. Tracked as a follow-up in #3.
        tokio::task::yield_now().await;
    }
}
