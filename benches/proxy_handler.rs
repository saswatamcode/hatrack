use axum::http::header;
use axum::{body::Body, extract::Request};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use hatrack::replica_selector::{Replica, ReplicaSelector};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn create_test_request(cluster: &str, replica: &str) -> Request {
    Request::builder()
        .method("POST")
        .uri("/api/v1/push")
        .header("cluster", cluster)
        .header("HATRACK-ORDINAL", replica)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .unwrap()
}

fn header_value<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

fn bench_header_extraction(c: &mut Criterion) {
    let req = create_test_request("test-cluster", "prometheus-replica-0");

    c.bench_function("header_extraction", |b| {
        b.iter(|| {
            let cluster = header_value(black_box(&req), black_box("cluster"));
            let replica = header_value(black_box(&req), black_box("HATRACK-ORDINAL"));
            (cluster, replica)
        });
    });
}

fn bench_proxy_decision_path(c: &mut Criterion) {
    let replicas = vec![
        Replica {
            id: "prometheus-replica-0".to_string(),
        },
        Replica {
            id: "prometheus-replica-1".to_string(),
        },
    ];

    let selector = Arc::new(
        ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30), None).unwrap(),
    );

    let req = create_test_request("test-cluster", "prometheus-replica-0");

    c.bench_function("proxy_decision_path", |b| {
        b.iter(|| {
            let cluster = header_value(black_box(&req), "cluster").unwrap();
            let replica = header_value(black_box(&req), "HATRACK-ORDINAL").unwrap();
            selector.should_accept(black_box(cluster), black_box(replica))
        });
    });
}

fn bench_proxy_decision_path_many_clusters(c: &mut Criterion) {
    let replicas = vec![
        Replica {
            id: "prometheus-replica-0".to_string(),
        },
        Replica {
            id: "prometheus-replica-1".to_string(),
        },
    ];

    let selector = Arc::new(
        ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30), None).unwrap(),
    );

    let requests: Vec<Request> = (0..100)
        .map(|i| create_test_request(&format!("cluster-{}", i), "prometheus-replica-0"))
        .collect();

    c.bench_function("proxy_decision_path_100_clusters", |b| {
        let mut idx = 0;
        b.iter(|| {
            let req = &requests[idx % requests.len()];
            idx += 1;
            let cluster = header_value(black_box(req), "cluster").unwrap();
            let replica = header_value(black_box(req), "HATRACK-ORDINAL").unwrap();
            selector.should_accept(black_box(cluster), black_box(replica))
        });
    });
}

fn bench_proxy_decision_with_rejection(c: &mut Criterion) {
    let replicas = vec![
        Replica {
            id: "prometheus-replica-0".to_string(),
        },
        Replica {
            id: "prometheus-replica-1".to_string(),
        },
    ];

    let selector = Arc::new(
        ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30), None).unwrap(),
    );

    let active_req = create_test_request("test-cluster", "prometheus-replica-0");
    let inactive_req = create_test_request("test-cluster", "prometheus-replica-1");

    let mut group = c.benchmark_group("proxy_decision_active_vs_inactive");

    group.bench_function("active_replica", |b| {
        b.iter(|| {
            let cluster = header_value(black_box(&active_req), "cluster").unwrap();
            let replica = header_value(black_box(&active_req), "HATRACK-ORDINAL").unwrap();
            selector.should_accept(black_box(cluster), black_box(replica))
        });
    });

    group.bench_function("inactive_replica", |b| {
        b.iter(|| {
            let cluster = header_value(black_box(&inactive_req), "cluster").unwrap();
            let replica = header_value(black_box(&inactive_req), "HATRACK-ORDINAL").unwrap();
            selector.should_accept(black_box(cluster), black_box(replica))
        });
    });

    group.finish();
}

fn bench_request_parsing_varying_header_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("request_parsing_by_header_count");

    for header_count in [2, 5, 10, 20].iter() {
        let mut req_builder = Request::builder().method("POST").uri("/api/v1/push");

        for i in 0..*header_count {
            req_builder = req_builder.header(format!("X-Custom-{}", i), format!("value-{}", i));
        }

        let req = req_builder
            .header("cluster", "test-cluster")
            .header("HATRACK-ORDINAL", "prometheus-replica-0")
            .body(Body::empty())
            .unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(header_count),
            header_count,
            |b, _| {
                b.iter(|| {
                    let cluster = header_value(black_box(&req), "cluster");
                    let replica = header_value(black_box(&req), "HATRACK-ORDINAL");
                    (cluster, replica)
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_header_extraction,
        bench_proxy_decision_path,
        bench_proxy_decision_path_many_clusters,
        bench_proxy_decision_with_rejection,
        bench_request_parsing_varying_header_counts
}
criterion_main!(benches);
