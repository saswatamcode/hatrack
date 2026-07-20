#[cfg(test)]
mod tests {
    use super::super::http::{HTTPClientConfig, empty_response, header_value};
    use super::super::replica_selector::{Replica, ReplicaSelector};
    use super::super::util::upstream_target::UpstreamTarget;
    use super::super::{AppState, ProxyConfig, proxy};
    use axum::Router;
    use axum::body::Body;
    use axum::extract::Request;
    use axum::http::StatusCode;
    use axum::routing::any;
    use hyper_util::rt::TokioExecutor;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::net::TcpListener;

    fn create_test_config() -> ProxyConfig {
        ProxyConfig {
            listen_address: "127.0.0.1:8080".parse().unwrap(),
            upstream_url: UpstreamTarget::parse_url("http://localhost:9090").unwrap(),
            internal_listen_address: "127.0.0.1:8081".parse().unwrap(),
            inactive_window_seconds: 30,
            ordinal_grouping_header: "cluster".to_string(),
            ordinal_header: "HATRACK-ORDINAL".to_string(),
            possible_ordinals: vec!["replica-0".to_string(), "replica-1".to_string()],
        }
    }

    async fn start_mock_upstream() -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
        let captured_paths = Arc::new(Mutex::new(Vec::new()));
        let captured_for_handler = Arc::clone(&captured_paths);

        let app = Router::new().fallback(any(move |req: Request| {
            let captured = Arc::clone(&captured_for_handler);
            async move {
                let path = req
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.as_str().to_string())
                    .unwrap_or_else(|| "/".to_string());
                captured.lock().unwrap().push(path);
                StatusCode::OK
            }
        }));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (addr, captured_paths)
    }

    fn create_test_app_state(config: ProxyConfig) -> AppState {
        let replicas = config
            .possible_ordinals
            .iter()
            .map(|id| Replica { id: id.clone() })
            .collect();

        let replica_selector = Arc::new(
            ReplicaSelector::new(
                replicas,
                Duration::from_secs(config.inactive_window_seconds),
                Duration::from_secs(config.inactive_window_seconds),
                None,
            )
            .unwrap(),
        );

        let executor = TokioExecutor::new();
        let metrics = super::super::metrics::ProxyMetrics::new().unwrap();

        AppState {
            client: HTTPClientConfig::default().build_default_client(&executor),
            upstream_target: config.upstream_url.clone(),
            proxy_config: config,
            replica_selector,
            metrics: Arc::new(metrics),
        }
    }

    struct ProxyTestCase {
        name: &'static str,
        cluster_header: Option<&'static str>,
        ordinal_header: Option<&'static str>,
        expected_status: StatusCode,
        description: &'static str,
    }

    #[tokio::test]
    async fn test_proxy_missing_headers_table_driven() {
        let tests = vec![
            ProxyTestCase {
                name: "missing cluster header",
                cluster_header: None,
                ordinal_header: Some("replica-0"),
                expected_status: StatusCode::BAD_REQUEST,
                description: "should return BAD_REQUEST when cluster header is missing",
            },
            ProxyTestCase {
                name: "missing ordinal header",
                cluster_header: Some("test-cluster"),
                ordinal_header: None,
                expected_status: StatusCode::BAD_REQUEST,
                description: "should return BAD_REQUEST when ordinal header is missing",
            },
            ProxyTestCase {
                name: "missing both headers",
                cluster_header: None,
                ordinal_header: None,
                expected_status: StatusCode::BAD_REQUEST,
                description: "should return BAD_REQUEST when both headers are missing",
            },
        ];

        let config = create_test_config();
        let state = create_test_app_state(config.clone());

        for test in tests {
            let mut req = Request::builder().uri("/test");

            if let Some(cluster) = test.cluster_header {
                req = req.header(&config.ordinal_grouping_header, cluster);
            }

            if let Some(ordinal) = test.ordinal_header {
                req = req.header(&config.ordinal_header, ordinal);
            }

            let request = req.body(Body::empty()).unwrap();
            let response = proxy(&state, request).await;

            assert_eq!(
                response.status(),
                test.expected_status,
                "test '{}' failed: {}",
                test.name,
                test.description
            );
        }
    }

    #[tokio::test]
    async fn test_proxy_inactive_replica_rejection() {
        let config = create_test_config();
        let state = create_test_app_state(config.clone());

        let cluster = "test-cluster";
        let ranked = state.replica_selector.ranked_replica_indices(cluster);
        let primary_id = &state.replica_selector.replicas[ranked[0]].id;
        let secondary_id = &state.replica_selector.replicas[ranked[1]].id;

        // First request from primary should be accepted
        let req1 = Request::builder()
            .uri("/test")
            .header(&config.ordinal_grouping_header, cluster)
            .header(&config.ordinal_header, primary_id.as_str())
            .body(Body::empty())
            .unwrap();

        let response1 = proxy(&state, req1).await;
        // Will fail to connect to upstream, but should not be rejected by replica selector
        // Check that it's not ACCEPTED (which is our rejection status)
        assert_ne!(
            response1.status(),
            StatusCode::ACCEPTED,
            "primary replica should not be rejected"
        );

        // Second request from secondary should be rejected with ACCEPTED status
        let req2 = Request::builder()
            .uri("/test")
            .header(&config.ordinal_grouping_header, cluster)
            .header(&config.ordinal_header, secondary_id.as_str())
            .body(Body::empty())
            .unwrap();

        let response2 = proxy(&state, req2).await;
        assert_eq!(
            response2.status(),
            StatusCode::ACCEPTED,
            "secondary replica should be rejected when primary is active"
        );
    }

    struct HeaderTestCase {
        name: &'static str,
        headers: Vec<(&'static str, &'static str)>,
        header_to_check: &'static str,
        expected_value: Option<&'static str>,
    }

    #[test]
    fn test_header_value() {
        let tests = vec![
            HeaderTestCase {
                name: "header exists",
                headers: vec![("cluster", "test-cluster"), ("ordinal", "replica-0")],
                header_to_check: "cluster",
                expected_value: Some("test-cluster"),
            },
            HeaderTestCase {
                name: "header missing",
                headers: vec![("cluster", "test-cluster")],
                header_to_check: "ordinal",
                expected_value: None,
            },
            HeaderTestCase {
                name: "case insensitive header name",
                headers: vec![("Cluster", "test-cluster")],
                header_to_check: "cluster",
                expected_value: Some("test-cluster"),
            },
            HeaderTestCase {
                name: "multiple headers, retrieve second",
                headers: vec![("x-first", "value1"), ("x-second", "value2")],
                header_to_check: "x-second",
                expected_value: Some("value2"),
            },
        ];

        for test in tests {
            let mut req_builder = Request::builder().uri("/");

            for (name, value) in test.headers {
                req_builder = req_builder.header(name, value);
            }

            let request = req_builder.body(Body::empty()).unwrap();
            let result = header_value(&request, test.header_to_check);

            assert_eq!(
                result, test.expected_value,
                "test '{}' failed: expected {:?}, got {:?}",
                test.name, test.expected_value, result
            );
        }
    }

    struct EmptyResponseTestCase {
        name: &'static str,
        status: StatusCode,
    }

    #[test]
    fn test_empty_response() {
        let tests = vec![
            EmptyResponseTestCase {
                name: "200 OK",
                status: StatusCode::OK,
            },
            EmptyResponseTestCase {
                name: "400 BAD REQUEST",
                status: StatusCode::BAD_REQUEST,
            },
            EmptyResponseTestCase {
                name: "404 NOT FOUND",
                status: StatusCode::NOT_FOUND,
            },
            EmptyResponseTestCase {
                name: "500 INTERNAL SERVER ERROR",
                status: StatusCode::INTERNAL_SERVER_ERROR,
            },
            EmptyResponseTestCase {
                name: "502 BAD GATEWAY",
                status: StatusCode::BAD_GATEWAY,
            },
        ];

        for test in tests {
            let response = empty_response(test.status);

            assert_eq!(
                response.status(),
                test.status,
                "test '{}' failed: expected status {:?}",
                test.name,
                test.status
            );
        }
    }

    #[tokio::test]
    async fn test_proxy_multiple_clusters() {
        let config = create_test_config();
        let state = create_test_app_state(config.clone());

        let cluster1 = "cluster-1";
        let cluster2 = "cluster-2";

        let ranked1 = state.replica_selector.ranked_replica_indices(cluster1);
        let ranked2 = state.replica_selector.ranked_replica_indices(cluster2);

        let primary1 = &state.replica_selector.replicas[ranked1[0]].id;
        let primary2 = &state.replica_selector.replicas[ranked2[0]].id;

        // Request from cluster1's primary
        let req1 = Request::builder()
            .uri("/test")
            .header(&config.ordinal_grouping_header, cluster1)
            .header(&config.ordinal_header, primary1.as_str())
            .body(Body::empty())
            .unwrap();

        let response1 = proxy(&state, req1).await;
        assert_ne!(
            response1.status(),
            StatusCode::ACCEPTED,
            "cluster1 primary should not be rejected"
        );

        // Request from cluster2's primary
        let req2 = Request::builder()
            .uri("/test")
            .header(&config.ordinal_grouping_header, cluster2)
            .header(&config.ordinal_header, primary2.as_str())
            .body(Body::empty())
            .unwrap();

        let response2 = proxy(&state, req2).await;
        assert_ne!(
            response2.status(),
            StatusCode::ACCEPTED,
            "cluster2 primary should not be rejected"
        );

        // Verify both clusters are tracked
        assert_eq!(state.replica_selector.cluster_count(), 2);
    }

    struct HTTPMethodTestCase {
        name: &'static str,
        method: &'static str,
        expected_not_rejected: bool,
    }

    #[tokio::test]
    async fn test_proxy_http_methods() {
        let config = create_test_config();
        let state = create_test_app_state(config.clone());

        let cluster = "test-cluster";
        let ranked = state.replica_selector.ranked_replica_indices(cluster);
        let primary_id = &state.replica_selector.replicas[ranked[0]].id;

        let tests = vec![
            HTTPMethodTestCase {
                name: "GET request",
                method: "GET",
                expected_not_rejected: true,
            },
            HTTPMethodTestCase {
                name: "POST request",
                method: "POST",
                expected_not_rejected: true,
            },
            HTTPMethodTestCase {
                name: "PUT request",
                method: "PUT",
                expected_not_rejected: true,
            },
            HTTPMethodTestCase {
                name: "DELETE request",
                method: "DELETE",
                expected_not_rejected: true,
            },
            HTTPMethodTestCase {
                name: "PATCH request",
                method: "PATCH",
                expected_not_rejected: true,
            },
        ];

        for test in tests {
            let req = Request::builder()
                .method(test.method)
                .uri("/test")
                .header(&config.ordinal_grouping_header, cluster)
                .header(&config.ordinal_header, primary_id.as_str())
                .body(Body::empty())
                .unwrap();

            let response = proxy(&state, req).await;

            if test.expected_not_rejected {
                assert_ne!(
                    response.status(),
                    StatusCode::ACCEPTED,
                    "test '{}' failed: {} request should not be rejected by replica selector",
                    test.name,
                    test.method
                );
            }
        }
    }

    #[tokio::test]
    async fn test_proxy_preserves_request_path() {
        let (upstream_addr, captured_paths) = start_mock_upstream().await;

        let mut config = create_test_config();
        config.upstream_url =
            UpstreamTarget::parse_url(&format!("http://{upstream_addr}")).unwrap();
        let state = create_test_app_state(config.clone());

        let cluster = "test-cluster";
        let ranked = state.replica_selector.ranked_replica_indices(cluster);
        let primary_id = &state.replica_selector.replicas[ranked[0]].id;

        struct PathTestCase {
            name: &'static str,
            path: &'static str,
        }

        let tests = vec![
            PathTestCase {
                name: "simple path",
                path: "/api/v1/metrics",
            },
            PathTestCase {
                name: "path with query",
                path: "/api/v1/query?query=up",
            },
            PathTestCase {
                name: "root path",
                path: "/",
            },
            PathTestCase {
                name: "path with multiple segments",
                path: "/api/v1/targets/metadata",
            },
        ];

        for test in tests {
            captured_paths.lock().unwrap().clear();

            let req = Request::builder()
                .uri(test.path)
                .header(&config.ordinal_grouping_header, cluster)
                .header(&config.ordinal_header, primary_id.as_str())
                .body(Body::empty())
                .unwrap();

            let response = proxy(&state, req).await;

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "test '{}' failed: proxy should forward successfully for {}",
                test.name,
                test.path
            );

            let paths = captured_paths.lock().unwrap();
            assert_eq!(
                paths.len(),
                1,
                "test '{}' failed: expected exactly one upstream request",
                test.name
            );
            assert_eq!(
                paths[0], test.path,
                "test '{}' failed: upstream path should be preserved",
                test.name
            );
        }
    }

    #[test]
    fn test_http_client_config_defaults() {
        let config = HTTPClientConfig::default();

        assert_eq!(config.connect_timeout, Some(Duration::from_secs(10)));
        assert_eq!(config.tcp_keepalive, Some(Duration::from_secs(60)));
        assert_eq!(config.pool_idle_timeout, Some(Duration::from_secs(90)));
        assert_eq!(config.pool_max_idle_per_host, 32);
    }

    #[test]
    fn test_http_client_config_custom() {
        let config = HTTPClientConfig {
            connect_timeout: Some(Duration::from_secs(5)),
            tcp_keepalive: Some(Duration::from_secs(30)),
            pool_idle_timeout: Some(Duration::from_secs(60)),
            pool_max_idle_per_host: 16,
        };

        assert_eq!(config.connect_timeout, Some(Duration::from_secs(5)));
        assert_eq!(config.tcp_keepalive, Some(Duration::from_secs(30)));
        assert_eq!(config.pool_idle_timeout, Some(Duration::from_secs(60)));
        assert_eq!(config.pool_max_idle_per_host, 16);
    }
}
