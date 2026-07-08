use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use hatrack::replica_selector::{Replica, ReplicaSelector};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn bench_should_accept_single_cluster(c: &mut Criterion) {
    let replicas = vec![
        Replica {
            id: "prometheus-replica-0".to_string(),
        },
        Replica {
            id: "prometheus-replica-1".to_string(),
        },
    ];

    let selector: Arc<ReplicaSelector> = Arc::new(
        ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30)).unwrap(),
    );

    c.bench_function("should_accept_single_cluster", |b| {
        b.iter(|| {
            selector.should_accept(black_box("test-cluster"), black_box("prometheus-replica-0"))
        });
    });
}

fn bench_should_accept_many_clusters(c: &mut Criterion) {
    let replicas = vec![
        Replica {
            id: "prometheus-replica-0".to_string(),
        },
        Replica {
            id: "prometheus-replica-1".to_string(),
        },
    ];

    let selector = Arc::new(
        ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30)).unwrap(),
    );

    let cluster_names: Vec<String> = (0..100).map(|i| format!("cluster-{}", i)).collect();

    c.bench_function("should_accept_100_clusters_round_robin", |b| {
        let mut idx = 0;
        b.iter(|| {
            let cluster = &cluster_names[idx % cluster_names.len()];
            idx += 1;
            selector.should_accept(black_box(cluster), black_box("prometheus-replica-0"))
        });
    });
}

fn bench_should_accept_varying_replica_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("should_accept_by_replica_count");

    for replica_count in [2, 5, 10, 20].iter() {
        let replicas: Vec<Replica> = (0..*replica_count)
            .map(|i| Replica {
                id: format!("replica-{}", i),
            })
            .collect();

        let selector = Arc::new(
            ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30))
                .unwrap(),
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(replica_count),
            replica_count,
            |b, _| {
                b.iter(|| {
                    selector.should_accept(black_box("test-cluster"), black_box("replica-0"))
                });
            },
        );
    }
    group.finish();
}

fn bench_ranked_replica_indices(c: &mut Criterion) {
    let replicas = vec![
        Replica {
            id: "prometheus-replica-0".to_string(),
        },
        Replica {
            id: "prometheus-replica-1".to_string(),
        },
    ];

    let selector =
        ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30)).unwrap();

    c.bench_function("ranked_replica_indices", |b| {
        b.iter(|| selector.ranked_replica_indices(black_box("test-cluster")));
    });
}

fn bench_ranked_replica_indices_varying_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("ranked_replica_indices_by_count");

    for replica_count in [2, 5, 10, 20].iter() {
        let replicas: Vec<Replica> = (0..*replica_count)
            .map(|i| Replica {
                id: format!("replica-{}", i),
            })
            .collect();

        let selector =
            ReplicaSelector::new(replicas, Duration::from_secs(30), Duration::from_secs(30))
                .unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(replica_count),
            replica_count,
            |b, _| {
                b.iter(|| selector.ranked_replica_indices(black_box("test-cluster")));
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_should_accept_single_cluster,
        bench_should_accept_many_clusters,
        bench_should_accept_varying_replica_counts,
        bench_ranked_replica_indices,
        bench_ranked_replica_indices_varying_counts
}
criterion_main!(benches);
