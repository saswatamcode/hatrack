mod http;
mod metrics;
mod replica_selector;
mod util;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use crate::http::HTTPClientConfig;
use crate::http::HttpClient;
use crate::http::ProxyBody;
use crate::http::empty_response;
use crate::http::header_value;
use crate::http::streaming_body_from_axum;
use crate::metrics::ProxyMetrics;
use crate::replica_selector::Replica;
use crate::replica_selector::ReplicaSelector;
use crate::replica_selector::spawn_idle_cluster_eviction;
use crate::util::error::BoxError;
use crate::util::parse::parse_addr;
use crate::util::shutdown::shutdown_signal;
use crate::util::upstream_target::UpstreamTarget;

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{Method, Response, StatusCode, header},
    response::IntoResponse,
    routing::{any, get},
};
use clap::Parser;
use hyper_util::rt::TokioExecutor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser, Clone, Debug)]
#[command(version, about, long_about = None)]
pub struct ProxyConfig {
    /// Address/port to start proxy server on.
    #[arg(short, long, default_value = ":8080", value_parser = parse_addr)]
    pub listen_address: SocketAddr,

    /// Upstream URL to forward requests to.
    #[arg(long, value_parser = UpstreamTarget::parse_url)]
    pub upstream_url: UpstreamTarget,

    /// Internal health server address for metrics/healthchecks.
    #[arg(short, long, default_value = ":8081", value_parser = parse_addr)]
    pub internal_listen_address: SocketAddr,

    /// Duration of time after which HA ordinal is considered inactive.
    #[arg(long, default_value = "30")]
    pub inactive_window_seconds: u64,

    /// Header name for HA ordinal grouping.
    #[arg(long, default_value = "cluster")]
    pub ordinal_grouping_header: String,

    /// Header name for HA ordinal.
    #[arg(long, default_value = "HATRACK-ORDINAL")]
    pub ordinal_header: String,

    /// Possible ordinals for the HA tracker, these will show up as header values.
    #[arg(
        short,
        long,
        default_value = "prometheus-replica-0,prometheus-replica-1"
    )]
    pub possible_ordinals: Vec<String>,
}

#[derive(Clone)]
struct AppState {
    client: HttpClient,
    upstream_target: UpstreamTarget,
    proxy_config: ProxyConfig,
    replica_selector: Arc<ReplicaSelector>,
    metrics: Arc<ProxyMetrics>,
}

async fn metrics_handler(
    State(metrics): State<Arc<ProxyMetrics>>,
    method: Method,
) -> Response<Body> {
    if method != Method::GET {
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }

    match metrics.encode() {
        Ok(body) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "failed to encode metrics");
            empty_response(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn proxy_handler(State(state): State<AppState>, req: Request) -> Response<Body> {
    state.metrics.connection_opened();
    let start = Instant::now();
    let method = req.method().clone();
    let response = proxy(&state, req).await;
    state.metrics.record_server_request(
        method.as_str(),
        response.status().as_u16(),
        start.elapsed(),
    );
    state.metrics.connection_closed();
    response
}

async fn proxy(state: &AppState, req: Request) -> Response<Body> {
    let method = req.method().clone();
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let cluster = match header_value(&req, &state.proxy_config.ordinal_grouping_header) {
        Some(value) => value,
        None => {
            debug!(
                header = %state.proxy_config.ordinal_grouping_header,
                "missing ordinal grouping header"
            );
            return empty_response(StatusCode::BAD_REQUEST);
        }
    };

    let replica_id = match header_value(&req, &state.proxy_config.ordinal_header) {
        Some(value) => value,
        None => {
            debug!(
                header = %state.proxy_config.ordinal_header,
                "missing ordinal header"
            );
            return empty_response(StatusCode::BAD_REQUEST);
        }
    };

    let accepted = state.replica_selector.should_accept(cluster, replica_id);

    if !accepted {
        debug!(%cluster, %replica_id, "dropping inactive replica request");
        return empty_response(StatusCode::ACCEPTED);
    }

    let upstream_uri = match state.upstream_target.map_request(req.uri()) {
        Ok(uri) => uri,
        Err(error) => {
            error!(%error, path = %path_and_query, "failed to map upstream uri");
            return empty_response(StatusCode::BAD_GATEWAY);
        }
    };

    debug!(
        method = %req.method(),
        path = %path_and_query,
        upstream = %upstream_uri,
        %cluster,
        %replica_id,
        "forwarding request"
    );

    let (mut parts, body) = req.into_parts();
    parts.uri = upstream_uri;

    let upstream_body = streaming_body_from_axum(body);
    let upstream_req: Request<ProxyBody> = Request::from_parts(parts, upstream_body);

    let client_start = Instant::now();
    let upstream_resp = match state.client.request(upstream_req).await {
        Ok(response) => {
            state.metrics.record_client_request(
                method.as_str(),
                response.status().as_u16(),
                client_start.elapsed(),
            );
            response
        }
        Err(error) => {
            state
                .metrics
                .record_client_error(method.as_str(), client_start.elapsed());
            error!(%error, "upstream request failed");
            return empty_response(StatusCode::BAD_GATEWAY);
        }
    };

    let (parts, body) = upstream_resp.into_parts();
    Response::from_parts(parts, Body::new(body))
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let env_filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(error) => {
            warn!(%error, "invalid RUST_LOG filter, using default");
            EnvFilter::new("debug")
        }
    };

    tracing_subscriber::registry()
        .with(tracing_logfmt::layer())
        .with(env_filter)
        .init();

    let proxy_config = ProxyConfig::parse();

    info!(
        listen_address = %proxy_config.listen_address,
        upstream_target = ?proxy_config.upstream_url,
        "starting hatrack proxy server"
    );

    let metrics = ProxyMetrics::new()?;
    let executor = TokioExecutor::new();

    let metrics_listener = TcpListener::bind(proxy_config.internal_listen_address).await?;
    let metrics_for_server = metrics.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(Arc::new(metrics_for_server));
        info!(addr = %proxy_config.internal_listen_address, "metrics server listening");
        if let Err(e) = axum::serve(metrics_listener, app).await {
            error!(error = %e, "metrics server exited");
        }
    });

    let replicas = proxy_config
        .possible_ordinals
        .iter()
        .map(|id| Replica { id: id.clone() })
        .collect();

    let inactive_window = Duration::from_secs(proxy_config.inactive_window_seconds);
    let replica_selector = Arc::new(ReplicaSelector::new(
        replicas,
        inactive_window,
        inactive_window,
        Some(metrics.replica_selector_metrics()),
    )?);
    spawn_idle_cluster_eviction(Arc::clone(&replica_selector), inactive_window);

    let app_state = AppState {
        client: HTTPClientConfig::default().build_default_client(&executor),
        upstream_target: proxy_config.upstream_url.clone(),
        proxy_config: proxy_config.clone(),
        replica_selector,
        metrics: Arc::new(metrics),
    };

    let app = Router::new()
        .fallback(any(proxy_handler))
        .with_state(app_state);
    let listener = TcpListener::bind(proxy_config.listen_address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("shutdown complete");
    Ok(())
}

#[cfg(test)]
#[path = "main_test.rs"]
mod main_test;
