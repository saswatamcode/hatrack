#[cfg(test)]
mod tests {
    use super::super::{Replica, ReplicaSelector};
    use std::time::Duration;

    fn create_test_replicas() -> Vec<Replica> {
        vec![
            Replica {
                id: "replica-0".to_string(),
            },
            Replica {
                id: "replica-1".to_string(),
            },
            Replica {
                id: "replica-2".to_string(),
            },
        ]
    }

    #[test]
    fn test_new_requires_at_least_one_replica() {
        let result = ReplicaSelector::new(
            vec![],
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        );
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "at least one replica ordinal is required"
        );
    }

    #[test]
    fn test_new_with_valid_replicas() {
        let replicas = create_test_replicas();
        let result = ReplicaSelector::new(
            replicas,
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_accept_initial_primary_replica() {
        let replicas = create_test_replicas();
        let selector = ReplicaSelector::new(
            replicas,
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        )
        .unwrap();

        let cluster = "test-cluster";
        let primary_id = selector
            .ranked_replica_indices(cluster)
            .first()
            .map(|&idx| selector.replicas[idx].id.clone())
            .unwrap();

        assert!(selector.should_accept(cluster, &primary_id));
    }

    #[test]
    fn test_should_reject_non_primary_when_primary_active() {
        let replicas = create_test_replicas();
        let selector = ReplicaSelector::new(
            replicas,
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        )
        .unwrap();

        let cluster = "test-cluster";
        let ranked = selector.ranked_replica_indices(cluster);
        let primary_id = &selector.replicas[ranked[0]].id;
        let secondary_id = &selector.replicas[ranked[1]].id;

        // Accept primary first
        assert!(selector.should_accept(cluster, primary_id));

        // Reject secondary while primary is active
        assert!(!selector.should_accept(cluster, secondary_id));
    }

    #[test]
    fn test_failover_to_secondary_after_timeout() {
        let replicas = create_test_replicas();
        let silence_timeout = Duration::from_millis(100);
        let selector =
            ReplicaSelector::new(replicas, silence_timeout, Duration::from_secs(60), None).unwrap();

        let cluster = "test-cluster";
        let ranked = selector.ranked_replica_indices(cluster);
        let primary_id = &selector.replicas[ranked[0]].id;
        let secondary_id = &selector.replicas[ranked[1]].id;

        // Accept primary
        assert!(selector.should_accept(cluster, primary_id));

        // Wait for silence timeout
        std::thread::sleep(silence_timeout + Duration::from_millis(50));

        // Secondary should now be accepted after primary silence
        assert!(selector.should_accept(cluster, secondary_id));
    }

    #[test]
    fn test_failback_to_primary_after_probation() {
        let replicas = create_test_replicas();
        let silence_timeout = Duration::from_millis(100);
        let selector =
            ReplicaSelector::new(replicas, silence_timeout, Duration::from_secs(60), None).unwrap();

        let cluster = "test-cluster";
        let ranked = selector.ranked_replica_indices(cluster);
        let primary_id = &selector.replicas[ranked[0]].id;
        let secondary_id = &selector.replicas[ranked[1]].id;

        // Accept primary initially
        assert!(selector.should_accept(cluster, primary_id));

        // Wait for timeout and failover to secondary
        std::thread::sleep(silence_timeout + Duration::from_millis(50));
        assert!(selector.should_accept(cluster, secondary_id));

        // Primary comes back should be accepted during probation
        assert!(selector.should_accept(cluster, primary_id));

        // Secondary should still be accepted during probation
        assert!(selector.should_accept(cluster, secondary_id));

        // Wait for probation period
        std::thread::sleep(silence_timeout + Duration::from_millis(50));

        // Primary should continue being accepted (failback complete)
        assert!(selector.should_accept(cluster, primary_id));

        // Secondary should now be rejected
        assert!(!selector.should_accept(cluster, secondary_id));
    }

    #[test]
    fn test_multiple_independent_clusters() {
        let replicas = create_test_replicas();
        let selector = ReplicaSelector::new(
            replicas,
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        )
        .unwrap();

        let cluster1 = "cluster-1";
        let cluster2 = "cluster-2";

        let ranked1 = selector.ranked_replica_indices(cluster1);
        let ranked2 = selector.ranked_replica_indices(cluster2);

        let primary1 = &selector.replicas[ranked1[0]].id;
        let primary2 = &selector.replicas[ranked2[0]].id;

        // Accept primaries for both clusters
        assert!(selector.should_accept(cluster1, primary1));
        assert!(selector.should_accept(cluster2, primary2));

        // Verify cluster count
        assert_eq!(selector.cluster_count(), 2);
    }

    #[test]
    fn test_evict_idle_clusters() {
        let replicas = create_test_replicas();
        let idle_ttl = Duration::from_millis(100);
        let selector =
            ReplicaSelector::new(replicas, Duration::from_secs(30), idle_ttl, None).unwrap();

        let cluster = "test-cluster";
        let ranked = selector.ranked_replica_indices(cluster);
        let primary_id = &selector.replicas[ranked[0]].id;

        // Create cluster state
        assert!(selector.should_accept(cluster, primary_id));
        assert_eq!(selector.cluster_count(), 1);

        // Wait for idle TTL
        std::thread::sleep(idle_ttl + Duration::from_millis(50));

        // Evict idle clusters
        selector.evict_idle_clusters();
        assert_eq!(selector.cluster_count(), 0);
    }

    #[test]
    fn test_hrw_weight_consistency() {
        let replicas = create_test_replicas();
        let selector = ReplicaSelector::new(
            replicas,
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        )
        .unwrap();

        let cluster = "test-cluster";

        // Same cluster should always get same ranking
        let ranked1 = selector.ranked_replica_indices(cluster);
        let ranked2 = selector.ranked_replica_indices(cluster);

        assert_eq!(ranked1, ranked2);
    }

    #[test]
    fn test_hrw_weight_distribution() {
        let replicas = create_test_replicas();
        let selector = ReplicaSelector::new(
            replicas,
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        )
        .unwrap();

        // Different clusters should potentially have different primaries
        let mut primary_distribution = std::collections::HashMap::new();

        for i in 0..100 {
            let cluster = format!("cluster-{}", i);
            let ranked = selector.ranked_replica_indices(&cluster);
            let primary_idx = ranked[0];

            *primary_distribution.entry(primary_idx).or_insert(0) += 1;
        }

        // All replicas should get at least some clusters as primary
        assert!(
            primary_distribution.len() >= 2,
            "Expected at least 2 different primaries across 100 clusters"
        );
    }

    #[test]
    fn test_should_accept() {
        let replicas = vec![
            Replica {
                id: "replica-0".to_string(),
            },
            Replica {
                id: "replica-1".to_string(),
            },
        ];

        let selector = ReplicaSelector::new(
            replicas,
            Duration::from_secs(30),
            Duration::from_secs(60),
            None,
        )
        .unwrap();

        // Determine primary for cluster-a
        let ranked_a = selector.ranked_replica_indices("cluster-a");
        let primary_a = selector.replicas[ranked_a[0]].id.clone();
        let secondary_a = selector.replicas[ranked_a[1]].id.clone();

        struct TestCase {
            name: &'static str,
            cluster: &'static str,
            replica_id: String,
            expected: bool,
        }

        let tests = vec![
            TestCase {
                name: "accept primary replica for cluster-a",
                cluster: "cluster-a",
                replica_id: primary_a.clone(),
                expected: true,
            },
            TestCase {
                name: "reject secondary replica for cluster-a when primary active",
                cluster: "cluster-a",
                replica_id: secondary_a.clone(),
                expected: false,
            },
            TestCase {
                name: "reject unknown replica",
                cluster: "cluster-a",
                replica_id: "unknown-replica".to_string(),
                expected: false,
            },
        ];

        for test in tests {
            let result = selector.should_accept(test.cluster, &test.replica_id);
            assert_eq!(
                result, test.expected,
                "test '{}' failed: expected {}, got {}",
                test.name, test.expected, result
            );
        }
    }
}
