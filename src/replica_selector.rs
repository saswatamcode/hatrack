use crate::util::error::BoxError;

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::debug;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone)]
pub struct Replica {
    pub id: String,
}

#[derive(Debug)]
struct ClusterState {
    current_rank: usize,
    last_seen: Instant,
    last_touched: Instant,
    /// When the primary first reappeared during failback probation.
    failback_started: Option<Instant>,
    /// Last time the primary was accepted while a secondary is still active.
    failback_primary_last_seen: Option<Instant>,
}

impl ClusterState {
    fn new() -> Self {
        let now = Instant::now();

        Self {
            current_rank: 0,
            last_seen: now,
            last_touched: now,
            failback_started: None,
            failback_primary_last_seen: None,
        }
    }

    fn clear_failback(&mut self) {
        self.failback_started = None;
        self.failback_primary_last_seen = None;
    }

    fn expire_stale_failback(&mut self, silence_timeout: Duration) {
        if let Some(last) = self.failback_primary_last_seen
            && last.elapsed() > silence_timeout
        {
            self.clear_failback();
        }
    }

    fn should_accept(
        &mut self,
        incoming_replica_id: &str,
        ranked: &[usize],
        replicas: &[Replica],
        silence_timeout: Duration,
    ) -> bool {
        self.last_touched = Instant::now();

        // If active replica has gone quiet, move to next ranked replica.
        if self.last_seen.elapsed() > silence_timeout {
            self.current_rank = (self.current_rank + 1).min(ranked.len() - 1);
            self.last_seen = Instant::now();
            self.clear_failback();
        }

        if self.current_rank > 0 {
            self.expire_stale_failback(silence_timeout);
        }

        let primary_replica = &replicas[ranked[0]].id;
        let active_replica = &replicas[ranked[self.current_rank]].id;

        // Prefer primary if it comes back.
        if incoming_replica_id == primary_replica {
            if self.current_rank == 0 {
                self.last_seen = Instant::now();
                self.clear_failback();
                return true;
            }

            // Accept primary during probation so both replicas may forward briefly.
            let now = Instant::now();
            if self
                .failback_primary_last_seen
                .is_none_or(|last| last.elapsed() > silence_timeout)
            {
                self.failback_started = Some(now);
            }
            self.failback_primary_last_seen = Some(now);

            if self
                .failback_started
                .is_some_and(|started| started.elapsed() >= silence_timeout)
            {
                self.current_rank = 0;
                self.last_seen = now;
                self.clear_failback();
            }

            return true;
        }

        if incoming_replica_id == active_replica {
            self.last_seen = Instant::now();
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct ReplicaSelector {
    replicas: Vec<Replica>,
    clusters: DashMap<String, ClusterState>,
    silence_timeout: Duration,
    idle_ttl: Duration,
}

impl ReplicaSelector {
    pub fn new(
        replicas: Vec<Replica>,
        silence_timeout: Duration,
        idle_ttl: Duration,
    ) -> Result<Self, BoxError> {
        if replicas.is_empty() {
            return Err("at least one replica ordinal is required".into());
        }

        Ok(Self {
            replicas,
            clusters: DashMap::new(),
            silence_timeout,
            idle_ttl,
        })
    }

    pub fn should_accept(&self, cluster: &str, incoming_replica_id: &str) -> bool {
        let ranked = self.ranked_replica_indices(cluster);

        let mut state = self
            .clusters
            .entry(cluster.to_owned())
            .or_insert_with(ClusterState::new);

        state.should_accept(
            incoming_replica_id,
            &ranked,
            &self.replicas,
            self.silence_timeout,
        )
    }

    pub fn evict_idle_clusters(&self) {
        let idle_ttl = self.idle_ttl;

        self.clusters
            .retain(|_, state| state.last_touched.elapsed() <= idle_ttl);
    }

    pub fn cluster_count(&self) -> usize {
        self.clusters.len()
    }

    /// Weight function for the HRW algorithm using XOR and xxhash.
    fn hrw_weight(replica_id: &str, cluster: &str) -> u64 {
        xxh3_64(replica_id.as_bytes()) ^ xxh3_64(cluster.as_bytes())
    }

    fn ranked_replica_indices(&self, cluster: &str) -> Vec<usize> {
        let mut ranked: Vec<usize> = (0..self.replicas.len()).collect();

        ranked.sort_by(|&a, &b| {
            Self::hrw_weight(&self.replicas[b].id, cluster)
                .cmp(&Self::hrw_weight(&self.replicas[a].id, cluster))
                .then_with(|| self.replicas[a].id.cmp(&self.replicas[b].id))
        });

        ranked
    }
}

pub fn spawn_idle_cluster_eviction(selector: Arc<ReplicaSelector>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);

        loop {
            ticker.tick().await;

            let before = selector.cluster_count();
            selector.evict_idle_clusters();
            let after = selector.cluster_count();

            if before != after {
                debug!(before, after, "evicted idle clusters");
            }
        }
    });
}
