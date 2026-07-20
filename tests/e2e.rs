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

#[tokio::test]
async fn test_ha_dedup_and_failover() {
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

    // Cleanup happens automatically when containers are dropped.
    drop(query);
    drop(hatrack);
    drop(receive);
    drop(prom0);
    drop(prom1);
}
