use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tracing::info;

const THANOS_IMAGE: &str = "quay.io/thanos/thanos";
const THANOS_TAG: &str = "v0.37.2";
const PROMETHEUS_IMAGE: &str = "quay.io/prometheus/prometheus";
const PROMETHEUS_TAG: &str = "v3.2.1";

const NETWORK: &str = "hatrack-e2e";

const HATRACK_PROXY_PORT: ContainerPort = ContainerPort::Tcp(8080);
const HATRACK_INTERNAL_PORT: ContainerPort = ContainerPort::Tcp(8081);
const PROMETHEUS_PORT: ContainerPort = ContainerPort::Tcp(9090);
const THANOS_HTTP_PORT: ContainerPort = ContainerPort::Tcp(8080);
const THANOS_GRPC_PORT: ContainerPort = ContainerPort::Tcp(9091);
const THANOS_RW_PORT: ContainerPort = ContainerPort::Tcp(8081);

fn prom_config(cluster: &str, replica: u32, remote_write_endpoint: &str) -> String {
    format!(
        r#"
global:
  external_labels:
    prometheus: {cluster}
    replica: "{replica}"

scrape_configs:
- job_name: 'myself'
  fallback_scrape_protocol: 'PrometheusText0.0.4'
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: ['localhost:9090']

remote_write:
- url: "{remote_write_endpoint}"
  headers:
    X-Prometheus-Cluster: "{cluster}"
    X-Prometheus-Replica: "{replica}"
  queue_config:
    min_backoff: 2s
    max_backoff: 10s
"#
    )
}

#[derive(Debug, Deserialize)]
struct PromQueryResponse {
    status: String,
    data: PromQueryData,
}

#[derive(Debug, Deserialize)]
struct PromQueryData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PromQueryResult>,
}

#[derive(Debug, Deserialize)]
struct PromQueryResult {
    metric: HashMap<String, String>,
    value: (f64, String),
}

async fn query_thanos(
    host: &str,
    port: u16,
    query: &str,
    dedup: bool,
) -> Result<Vec<PromQueryResult>, Box<dyn std::error::Error>> {
    let url = format!(
        "http://{}:{}/api/v1/query?query={}&dedup={}",
        host, port, query, dedup
    );
    let resp: PromQueryResponse = reqwest::get(&url).await?.json().await?;
    assert_eq!(resp.status, "success", "query failed: {:?}", resp);
    assert_eq!(resp.data.result_type, "vector");
    Ok(resp.data.result)
}

async fn wait_for_metric(
    host: &str,
    port: u16,
    query: &str,
    dedup: bool,
    timeout: Duration,
) -> Vec<PromQueryResult> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(results) = query_thanos(host, port, query, dedup).await
            && !results.is_empty()
        {
            return results;
        }
        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for metric '{}' after {:?}",
                query, timeout
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_for_metric_with_label(
    host: &str,
    port: u16,
    query: &str,
    dedup: bool,
    label: &str,
    value: &str,
    timeout: Duration,
) -> Vec<PromQueryResult> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(results) = query_thanos(host, port, query, dedup).await
            && results
                .iter()
                .any(|r| r.metric.get(label).map(|v| v.as_str()) == Some(value))
        {
            return results;
        }
        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for metric '{}' with {}={} after {:?}",
                query, label, value, timeout
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Wait until the replica with the newest *sample* timestamp is `expected_replica`,
/// and that sample is fresher than `after_unix`.
///
/// Instant-query `value.0` is the evaluation time (identical across series), so we
/// query `timestamp(<metric>)` and compare the returned sample-time values instead.
/// Stale secondary series may still appear within the lookback window after failback.
async fn wait_for_newest_replica(
    host: &str,
    port: u16,
    metric_query: &str,
    expected_replica: &str,
    after_unix: f64,
    timeout: Duration,
) -> Vec<PromQueryResult> {
    let query = format!("timestamp({metric_query})");
    let start = std::time::Instant::now();
    loop {
        if let Ok(results) = query_thanos(host, port, &query, false).await
            && let Some(newest) = results.iter().max_by(|a, b| {
                let ta = a.value.1.parse::<f64>().unwrap_or(f64::NAN);
                let tb = b.value.1.parse::<f64>().unwrap_or(f64::NAN);
                ta.partial_cmp(&tb).unwrap_or(std::cmp::Ordering::Equal)
            })
        {
            let newest_ts = newest.value.1.parse::<f64>().unwrap_or(f64::NAN);
            if newest_ts > after_unix
                && newest.metric.get("replica").map(|s| s.as_str()) == Some(expected_replica)
            {
                return results;
            }
        }
        if start.elapsed() > timeout {
            let latest = query_thanos(host, port, &query, false)
                .await
                .unwrap_or_default();
            panic!(
                "timed out waiting for newest sample of '{}' to be replica={} after unix {:.0} ({:?}); last timestamp() results: {:?}",
                metric_query, expected_replica, after_unix, timeout, latest
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_for_hatrack_counter(
    host: &str,
    port: u16,
    metric_name: &str,
    min_value: f64,
    timeout: Duration,
) -> f64 {
    let start = std::time::Instant::now();
    loop {
        if let Ok(resp) = reqwest::get(format!("http://{host}:{port}/metrics")).await
            && let Ok(body) = resp.text().await
        {
            for line in body.lines() {
                if line.starts_with('#') {
                    continue;
                }
                let mut parts = line.split_whitespace();
                if let (Some(name), Some(val)) = (parts.next(), parts.next())
                    && name == metric_name
                    && let Ok(v) = val.parse::<f64>()
                    && v >= min_value
                {
                    return v;
                }
            }
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for {metric_name} >= {min_value} after {timeout:?}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// If `HATRACK_E2E_INTERACTIVE` is set, return how long to keep containers alive
/// after assertions so a human can poke at them.
///
/// Accepted values:
/// - unset / empty / `0` / `false` → no hold
/// - `1` / `true` → default 10 minutes
/// - positive integer → that many seconds
fn interactive_hold_duration() -> Option<Duration> {
    const DEFAULT_HOLD: Duration = Duration::from_secs(600);

    match std::env::var("HATRACK_E2E_INTERACTIVE") {
        Err(_) => None,
        Ok(val) => {
            let val = val.trim();
            if val.is_empty() || val == "0" || val.eq_ignore_ascii_case("false") {
                None
            } else if val == "1" || val.eq_ignore_ascii_case("true") {
                Some(DEFAULT_HOLD)
            } else {
                let secs: u64 = val.parse().unwrap_or_else(|_| {
                    panic!(
                        "HATRACK_E2E_INTERACTIVE must be seconds, true/1, or false/0 (got {val:?})"
                    )
                });
                Some(Duration::from_secs(secs))
            }
        }
    }
}

async fn host_port(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: ContainerPort,
) -> String {
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let mapped = container
        .get_host_port_ipv4(port)
        .await
        .expect("failed to get mapped port");
    format!("{host}:{mapped}")
}

/// Hold until `duration` elapses or the user interrupts (Ctrl+C / SIGTERM),
/// so containers can still be dropped cleanly on manual termination.
async fn interactive_hold(duration: Duration) {
    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {}
            Err(e) => {
                eprintln!("failed to install Ctrl+C handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                eprintln!("failed to install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = tokio::time::sleep(duration) => {
            info!("interactive hold expired, tearing down containers");
        }
        _ = ctrl_c => {
            eprintln!("interrupt received, tearing down containers");
            info!("interactive hold interrupted, tearing down containers");
        }
        _ = terminate => {
            eprintln!("terminate received, tearing down containers");
            info!("interactive hold terminated, tearing down containers");
        }
    }
}

#[tokio::test]
async fn test_ha_dedup_failover_and_failback() {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // --- 1. Start Thanos Receive ---
    let receive = GenericImage::new(THANOS_IMAGE, THANOS_TAG)
        .with_exposed_port(THANOS_HTTP_PORT)
        .with_exposed_port(THANOS_GRPC_PORT)
        .with_exposed_port(THANOS_RW_PORT)
        .with_wait_for(WaitFor::Http(
            HttpWaitStrategy::new("/-/ready")
                .with_port(THANOS_HTTP_PORT)
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_secs(60))
        .with_container_name("thanos-receive")
        .with_network(NETWORK)
        .with_cmd([
            "receive",
            "--grpc-address=:9091",
            "--http-address=:8080",
            "--remote-write.address=:8081",
            "--label=receive=\"receive-1\"",
            "--tsdb.path=/tmp/receive-data",
            "--log.level=info",
        ])
        .start()
        .await
        .expect("failed to start thanos receive");

    // --- 2. Start Hatrack (built from local Dockerfile) ---
    let hatrack = GenericImage::new("hatrack", "e2e-test")
        .with_exposed_port(HATRACK_PROXY_PORT)
        .with_exposed_port(HATRACK_INTERNAL_PORT)
        .with_wait_for(WaitFor::Http(
            HttpWaitStrategy::new("/metrics")
                .with_port(HATRACK_INTERNAL_PORT)
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_container_name("hatrack")
        .with_network(NETWORK)
        .with_cmd([
            "--listen-address=:8080",
            "--internal-listen-address=:8081",
            "--upstream-url=http://thanos-receive:8081",
            "--ordinal-header=X-Prometheus-Replica",
            "--ordinal-grouping-header=X-Prometheus-Cluster",
            "--possible-ordinals=0",
            "--possible-ordinals=1",
            "--inactive-window-seconds=10",
        ])
        .start()
        .await
        .expect(
            "failed to start hatrack (did you run `docker build -t hatrack:e2e-test .` first?)",
        );

    // --- 3. Start Prometheus HA pair ---
    let prom0_config = prom_config("prom-ha", 0, "http://hatrack:8080/api/v1/receive");
    let prom1_config = prom_config("prom-ha", 1, "http://hatrack:8080/api/v1/receive");

    let prom0 = GenericImage::new(PROMETHEUS_IMAGE, PROMETHEUS_TAG)
        .with_exposed_port(PROMETHEUS_PORT)
        .with_wait_for(WaitFor::Http(
            HttpWaitStrategy::new("/-/ready")
                .with_port(PROMETHEUS_PORT)
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_secs(60))
        .with_container_name("prom-0")
        .with_network(NETWORK)
        .with_copy_to("/etc/prometheus/prometheus.yml", prom0_config.into_bytes())
        .with_cmd([
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.path=/prometheus",
            "--storage.tsdb.max-block-duration=2h",
            "--log.level=info",
            "--web.listen-address=:9090",
            "--web.enable-remote-write-receiver",
        ])
        .start()
        .await
        .expect("failed to start prometheus replica 0");

    let prom1 = GenericImage::new(PROMETHEUS_IMAGE, PROMETHEUS_TAG)
        .with_exposed_port(PROMETHEUS_PORT)
        .with_wait_for(WaitFor::Http(
            HttpWaitStrategy::new("/-/ready")
                .with_port(PROMETHEUS_PORT)
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_secs(60))
        .with_container_name("prom-1")
        .with_network(NETWORK)
        .with_copy_to("/etc/prometheus/prometheus.yml", prom1_config.into_bytes())
        .with_cmd([
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.path=/prometheus",
            "--storage.tsdb.max-block-duration=2h",
            "--log.level=info",
            "--web.listen-address=:9090",
            "--web.enable-remote-write-receiver",
        ])
        .start()
        .await
        .expect("failed to start prometheus replica 1");

    // --- 4. Start Thanos Query ---
    let query = GenericImage::new(THANOS_IMAGE, THANOS_TAG)
        .with_exposed_port(THANOS_HTTP_PORT)
        .with_exposed_port(THANOS_GRPC_PORT)
        .with_wait_for(WaitFor::Http(
            HttpWaitStrategy::new("/-/ready")
                .with_port(THANOS_HTTP_PORT)
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_secs(60))
        .with_container_name("thanos-query")
        .with_network(NETWORK)
        .with_cmd([
            "query",
            "--grpc-address=:9091",
            "--http-address=:8080",
            "--endpoint=thanos-receive:9091",
            "--query.replica-label=replica",
            "--query.replica-label=receive",
            "--log.level=info",
            "--store.sd-dns-interval=5s",
        ])
        .start()
        .await
        .expect("failed to start thanos query");

    let query_host = query.get_host().await.expect("failed to get query host");
    let query_port = query
        .get_host_port_ipv4(THANOS_HTTP_PORT)
        .await
        .expect("failed to get query port");
    let query_addr = query_host.to_string();

    // --- Phase 1: Wait for data and verify dedup ---
    info!("waiting for metrics to appear in thanos query");

    let results = wait_for_metric(
        &query_addr,
        query_port,
        "up{job=\"myself\"}",
        false,
        Duration::from_secs(120),
    )
    .await;

    // With hatrack dedup and a single receive (RF=1), only one replica's data
    // reaches the receive. So without dedup we should see exactly 1 series.
    assert_eq!(
        results.len(),
        1,
        "expected 1 series without dedup (hatrack filters at the proxy level), got {}: {:?}",
        results.len(),
        results
    );

    let primary_replica = results[0]
        .metric
        .get("replica")
        .expect("missing 'replica' label")
        .clone();
    info!(replica = %primary_replica, "hatrack primary replica selected");

    // The series should have value 1 (up == 1 means scrape succeeded).
    let up_value: f64 = results[0]
        .value
        .1
        .parse()
        .expect("failed to parse up value");
    assert_eq!(up_value, 1.0, "expected up=1, got {}", up_value);

    // With dedup enabled, should also be 1 series (same data, dedup is a no-op here).
    let dedup_results = query_thanos(&query_addr, query_port, "up{job=\"myself\"}", true)
        .await
        .expect("dedup query failed");
    assert_eq!(
        dedup_results.len(),
        1,
        "expected 1 series with dedup, got {}: {:?}",
        dedup_results.len(),
        dedup_results
    );

    // Verify the deduped result has the right cluster label.
    assert_eq!(
        dedup_results[0]
            .metric
            .get("prometheus")
            .map(|s| s.as_str()),
        Some("prom-ha"),
        "expected prometheus=prom-ha label"
    );

    info!(replica = %primary_replica, "phase 1 passed: dedup verified");

    // --- Phase 2: Failover ---
    let secondary_replica = if primary_replica == "0" { "1" } else { "0" };

    info!(
        primary = %primary_replica,
        secondary = %secondary_replica,
        "stopping primary prometheus, expecting failover"
    );

    if primary_replica == "0" {
        prom0.stop().await.expect("failed to stop prom-0");
    } else {
        prom1.stop().await.expect("failed to stop prom-1");
    }

    // Wait for hatrack's inactive window (10s) + time for new data to flow.
    // The secondary should now become active, and new data should appear with
    // the secondary's replica label.
    info!("waiting for failover (inactive_window=10s + buffer for new scrapes)");

    let failover_results = wait_for_metric_with_label(
        &query_addr,
        query_port,
        "up{job=\"myself\"}",
        false,
        "replica",
        secondary_replica,
        Duration::from_secs(60),
    )
    .await;

    // After failover, querying the latest data should show the secondary replica.
    let has_secondary = failover_results
        .iter()
        .any(|r| r.metric.get("replica").map(|s| s.as_str()) == Some(secondary_replica));
    assert!(
        has_secondary,
        "expected data from secondary replica {} after failover, got: {:?}",
        secondary_replica, failover_results
    );

    info!(replica = %secondary_replica, "phase 2 passed: failover verified");

    // --- Phase 3: Failback ---
    // Restart the primary. Hatrack accepts it during a silence-window probation,
    // then switches active rank back to primary and rejects the secondary.
    let failback_started_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs_f64();

    info!(
        primary = %primary_replica,
        "restarting primary prometheus, expecting failback"
    );

    if primary_replica == "0" {
        prom0.start().await.expect("failed to restart prom-0");
    } else {
        prom1.start().await.expect("failed to restart prom-1");
    }

    let hatrack_metrics_host = hatrack
        .get_host()
        .await
        .expect("failed to get hatrack host")
        .to_string();
    let hatrack_metrics_port = hatrack
        .get_host_port_ipv4(HATRACK_INTERNAL_PORT)
        .await
        .expect("failed to get hatrack metrics port");

    // Failback completes after inactive_window (10s) of continuous primary traffic.
    info!("waiting for failback (inactive_window=10s probation + buffer for new scrapes)");

    let failbacks = wait_for_hatrack_counter(
        &hatrack_metrics_host,
        hatrack_metrics_port,
        "replica_selector_failbacks_total",
        1.0,
        Duration::from_secs(90),
    )
    .await;
    info!(failbacks, "hatrack reported failback");

    // After failback, only the primary should keep advancing. Secondary series may
    // still appear in the lookback window, but timestamp() must show primary newest.
    let failback_results = wait_for_newest_replica(
        &query_addr,
        query_port,
        "up{job=\"myself\"}",
        &primary_replica,
        failback_started_unix,
        Duration::from_secs(60),
    )
    .await;

    let newest = failback_results
        .iter()
        .max_by(|a, b| {
            let ta = a.value.1.parse::<f64>().unwrap_or(f64::NAN);
            let tb = b.value.1.parse::<f64>().unwrap_or(f64::NAN);
            ta.partial_cmp(&tb).unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("expected at least one series after failback");
    let newest_ts: f64 = newest
        .value
        .1
        .parse()
        .expect("failed to parse timestamp() value");
    assert_eq!(
        newest.metric.get("replica").map(|s| s.as_str()),
        Some(primary_replica.as_str()),
        "expected newest thanos sample to revert to primary replica {}, got: {:?}",
        primary_replica,
        failback_results
    );
    assert!(
        newest_ts > failback_started_unix,
        "expected primary sample timestamp after failback start, got {} <= {}",
        newest_ts,
        failback_started_unix
    );

    // Secondary may still be in lookback, but its last sample must be older once
    // hatrack stops forwarding it after probation.
    if let Some(secondary) = failback_results
        .iter()
        .find(|r| r.metric.get("replica").map(|s| s.as_str()) == Some(secondary_replica))
    {
        let secondary_ts: f64 = secondary
            .value
            .1
            .parse()
            .expect("failed to parse secondary timestamp() value");
        assert!(
            newest_ts > secondary_ts,
            "expected primary sample ({newest_ts}) newer than secondary ({secondary_ts}) after failback; results: {failback_results:?}"
        );
    }

    info!(replica = %primary_replica, "phase 3 passed: failback verified via query");

    if let Some(hold) = interactive_hold_duration() {
        let thanos_query_http = host_port(&query, THANOS_HTTP_PORT).await;
        let hatrack_proxy = host_port(&hatrack, HATRACK_PROXY_PORT).await;
        let hatrack_metrics = host_port(&hatrack, HATRACK_INTERNAL_PORT).await;
        let receive_http = host_port(&receive, THANOS_HTTP_PORT).await;
        let receive_rw = host_port(&receive, THANOS_RW_PORT).await;
        let prom0_http = format!("http://{}", host_port(&prom0, PROMETHEUS_PORT).await);
        let prom1_http = format!("http://{}", host_port(&prom1, PROMETHEUS_PORT).await);

        eprintln!();
        eprintln!("=== HATRACK_E2E_INTERACTIVE ===");
        eprintln!(
            "Assertions passed; holding containers for {:?} so you can interact.",
            hold
        );
        eprintln!("Docker network: {NETWORK}");
        eprintln!("Container names: thanos-receive, hatrack, prom-0, prom-1, thanos-query");
        eprintln!("  (primary prom-{primary_replica} failed over then failed back)");
        eprintln!();
        eprintln!("Host-mapped endpoints:");
        eprintln!("  thanos-query:     http://{thanos_query_http}");
        eprintln!("  hatrack proxy:    http://{hatrack_proxy}");
        eprintln!("  hatrack metrics:  http://{hatrack_metrics}/metrics");
        eprintln!("  thanos-receive:   http://{receive_http}  (remote-write http://{receive_rw})");
        eprintln!("  prom-0:           {prom0_http}");
        eprintln!("  prom-1:           {prom1_http}");
        eprintln!();
        eprintln!("Example: curl 'http://{thanos_query_http}/api/v1/query?query=up'");
        eprintln!("Press Ctrl+C to tear down early, or wait for the hold to expire.");
        eprintln!("==============================");
        eprintln!();

        interactive_hold(hold).await;
    }

    // Cleanup happens automatically when containers are dropped.
    drop(query);
    drop(hatrack);
    drop(receive);
    drop(prom0);
    drop(prom1);
}
