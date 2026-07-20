use crate::util::error::BoxError;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::histogram::exponential_buckets;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

#[derive(Clone)]
pub struct ReplicaSelectorMetrics {
    failovers_total: Counter,
    failbacks_total: Counter,
    idle_evictions_total: Counter,
}

impl std::fmt::Debug for ReplicaSelectorMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaSelectorMetrics").finish()
    }
}

impl ReplicaSelectorMetrics {
    pub fn record_failover(&self) {
        self.failovers_total.inc();
    }

    pub fn record_failback(&self) {
        self.failbacks_total.inc();
    }

    pub fn record_idle_evictions(&self, count: u64) {
        self.idle_evictions_total.inc_by(count);
    }
}

fn new_http_histogram() -> Histogram {
    Histogram::new(exponential_buckets(0.005, 2.0, 12))
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpRequestLabels {
    method: String,
    code: String,
}

#[derive(Clone)]
pub struct ProxyMetrics {
    registry: Arc<Registry>,
    server_requests: Family<HttpRequestLabels, Counter>,
    server_requests_duration_seconds: Family<HttpRequestLabels, Histogram>,
    client_requests: Family<HttpRequestLabels, Counter>,
    client_requests_duration_seconds: Family<HttpRequestLabels, Histogram>,
    server_connections_active: Gauge,
    replica_selector_metrics: ReplicaSelectorMetrics,
}

impl ProxyMetrics {
    pub fn new() -> Result<Self, BoxError> {
        let mut registry = Registry::default();

        if let Err(error) =
            kubert_prometheus_process::register(registry.sub_registry_with_prefix("process"))
        {
            warn!(%error, "failed to register process metrics");
        }

        let server_requests = Family::default();
        registry.register(
            "http_server_requests_total",
            "Total inbound HTTP requests handled by the proxy",
            server_requests.clone(),
        );

        let server_requests_duration_seconds =
            Family::new_with_constructor(new_http_histogram as fn() -> Histogram);
        registry.register(
            "http_server_request_duration_seconds",
            "Inbound HTTP request duration in seconds",
            server_requests_duration_seconds.clone(),
        );

        let client_requests = Family::default();
        registry.register(
            "http_client_requests_total",
            "Total outbound HTTP requests to the upstream",
            client_requests.clone(),
        );

        let client_requests_duration_seconds =
            Family::new_with_constructor(new_http_histogram as fn() -> Histogram);
        registry.register(
            "http_client_request_duration_seconds",
            "Upstream HTTP request duration in seconds",
            client_requests_duration_seconds.clone(),
        );

        let server_connections_active = Gauge::default();
        registry.register(
            "http_server_connections_active",
            "Currently active inbound TCP connections",
            server_connections_active.clone(),
        );

        let failovers_total = Counter::default();
        registry.register(
            "replica_selector_failovers_total",
            "Total failover events where the active replica went silent and a secondary took over",
            failovers_total.clone(),
        );

        let failbacks_total = Counter::default();
        registry.register(
            "replica_selector_failbacks_total",
            "Total failback events where the primary replica recovered after probation",
            failbacks_total.clone(),
        );

        let idle_evictions_total = Counter::default();
        registry.register(
            "replica_selector_idle_evictions_total",
            "Total idle cluster states evicted from the replica selector",
            idle_evictions_total.clone(),
        );

        let replica_selector_metrics = ReplicaSelectorMetrics {
            failovers_total,
            failbacks_total,
            idle_evictions_total,
        };

        Ok(Self {
            registry: Arc::new(registry),
            server_requests,
            server_requests_duration_seconds,
            client_requests,
            client_requests_duration_seconds,
            server_connections_active,
            replica_selector_metrics,
        })
    }

    pub fn encode(&self) -> Result<String, BoxError> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)?;
        Ok(buffer)
    }

    pub fn connection_opened(&self) {
        self.server_connections_active.inc();
    }

    pub fn connection_closed(&self) {
        self.server_connections_active.dec();
    }

    pub fn record_server_request(&self, method: &str, code: u16, duration: Duration) {
        let lset = HttpRequestLabels {
            method: method.to_owned(),
            code: itoa::Buffer::new().format(code).to_owned(),
        };

        self.server_requests.get_or_create(&lset).inc();

        self.server_requests_duration_seconds
            .get_or_create(&lset)
            .observe(duration.as_secs_f64());
    }

    pub fn record_client_request(&self, method: &str, code: u16, duration: Duration) {
        let lset = HttpRequestLabels {
            method: method.to_owned(),
            code: itoa::Buffer::new().format(code).to_owned(),
        };

        self.client_requests.get_or_create(&lset).inc();

        self.client_requests_duration_seconds
            .get_or_create(&lset)
            .observe(duration.as_secs_f64());
    }

    pub fn record_client_error(&self, method: &str, duration: Duration) {
        self.record_client_request(method, 0, duration);
    }

    pub fn replica_selector_metrics(&self) -> ReplicaSelectorMetrics {
        self.replica_selector_metrics.clone()
    }
}
