use crate::util::error::BoxError;

use std::sync::Arc;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio::time;
use tracing::debug;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone)]
pub struct Replica {
    pub id: String,
}

#[derive(Debug)]
pub struct ClusterState {
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
}

#[derive(Debug)]
pub struct ReplicaSelector {
    replicas: Vec<Replica>,
    clusters: HashMap<String, ClusterState>,
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
            clusters: HashMap::new(),
            silence_timeout,
            idle_ttl,
        })
    }

    pub fn should_accept(&mut self, cluster: &str, incoming_replica_id: &str) -> bool {
        let ranked = self.ranked_replica_indices(cluster);

        let state = self
            .clusters
            .entry(cluster.to_owned())
            .or_insert_with(ClusterState::new);

        state.last_touched = Instant::now();

        // If active replica has gone quiet, move to next ranked replica.
        if state.last_seen.elapsed() > self.silence_timeout {
            state.current_rank = (state.current_rank + 1).min(ranked.len() - 1);
            state.last_seen = Instant::now();
            state.clear_failback();
        }

        if state.current_rank > 0 {
            state.expire_stale_failback(self.silence_timeout);
        }

        let primary_replica = &self.replicas[ranked[0]].id;
        let active_replica = &self.replicas[ranked[state.current_rank]].id;

        // Prefer primary if it comes back.
        if incoming_replica_id == primary_replica {
            if state.current_rank == 0 {
                state.last_seen = Instant::now();
                state.clear_failback();
                return true;
            }

            // Accept primary during probation so both replicas may forward briefly.
            let now = Instant::now();
            if state
                .failback_primary_last_seen
                .is_none_or(|last| last.elapsed() > self.silence_timeout)
            {
                state.failback_started = Some(now);
            }
            state.failback_primary_last_seen = Some(now);

            if state
                .failback_started
                .is_some_and(|started| started.elapsed() >= self.silence_timeout)
            {
                state.current_rank = 0;
                state.last_seen = now;
                state.clear_failback();
            }

            return true;
        }

        if incoming_replica_id == active_replica {
            state.last_seen = Instant::now();
            true
        } else {
            false
        }
    }

    pub fn evict_idle_clusters(&mut self) {
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

pub fn spawn_idle_cluster_eviction(selector: Arc<Mutex<ReplicaSelector>>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);

        loop {
            ticker.tick().await;

            let mut selector = selector.lock().await;
            let before = selector.cluster_count();
            selector.evict_idle_clusters();
            let after = selector.cluster_count();

            if before != after {
                debug!(before, after, "evicted idle clusters");
            }
        }
    });
}
