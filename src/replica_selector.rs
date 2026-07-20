use crate::metrics::ReplicaSelectorMetrics;
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
    /// Cached ranked replica indices for this cluster (computed once).
    ranked_indices: Vec<usize>,
}

impl ClusterState {
    fn new(ranked_indices: Vec<usize>) -> Self {
        let now = Instant::now();

        Self {
            current_rank: 0,
            last_seen: now,
            last_touched: now,
            failback_started: None,
            failback_primary_last_seen: None,
            ranked_indices,
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
        replicas: &[Replica],
        silence_timeout: Duration,
    ) -> bool {
        let now = Instant::now();
        self.last_touched = now;

        // If active replica has gone quiet, move to next ranked replica.
        if self.last_seen.elapsed() > silence_timeout {
            self.current_rank = (self.current_rank + 1).min(self.ranked_indices.len() - 1);
            self.last_seen = now;
            self.clear_failback();
        }

        if self.current_rank > 0 {
            self.expire_stale_failback(silence_timeout);
        }

        let primary_replica = &replicas[self.ranked_indices[0]].id;
        let active_replica = &replicas[self.ranked_indices[self.current_rank]].id;

        // Prefer primary if it comes back.
        if incoming_replica_id == primary_replica {
            if self.current_rank == 0 {
                self.last_seen = now;
                self.clear_failback();
                return true;
            }

            // Accept primary during probation so both replicas may forward briefly.
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
            self.last_seen = now;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct ReplicaSelector {
    pub(crate) replicas: Vec<Replica>,
    clusters: DashMap<String, ClusterState>,
    silence_timeout: Duration,
    idle_ttl: Duration,
    metrics: Option<ReplicaSelectorMetrics>,
}

impl ReplicaSelector {
    pub fn new(
        replicas: Vec<Replica>,
        silence_timeout: Duration,
        idle_ttl: Duration,
        metrics: Option<ReplicaSelectorMetrics>,
    ) -> Result<Self, BoxError> {
        if replicas.is_empty() {
            return Err("at least one replica ordinal is required".into());
        }

        Ok(Self {
            replicas,
            clusters: DashMap::new(),
            silence_timeout,
            idle_ttl,
            metrics,
        })
    }

    pub fn should_accept(&self, cluster: &str, incoming_replica_id: &str) -> bool {
        use dashmap::mapref::entry::Entry;

        let mut state = match self.clusters.entry(cluster.to_owned()) {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => {
                let ranked = self.ranked_replica_indices(cluster);
                entry.insert(ClusterState::new(ranked))
            }
        };

        let prev_rank = state.current_rank;
        let accepted = state.should_accept(incoming_replica_id, &self.replicas, self.silence_timeout);

        if let Some(m) = &self.metrics {
            if state.current_rank > prev_rank {
                m.record_failover();
            } else if prev_rank > 0 && state.current_rank == 0 {
                m.record_failback();
            }
        }

        accepted
    }

    pub fn evict_idle_clusters(&self) {
        let before = self.clusters.len();

        self.clusters
            .retain(|_, state| state.last_touched.elapsed() <= self.idle_ttl);

        if let Some(m) = &self.metrics {
            let evicted = before.saturating_sub(self.clusters.len()) as u64;
            if evicted > 0 {
                m.record_idle_evictions(evicted);
            }
        }
    }

    pub fn cluster_count(&self) -> usize {
        self.clusters.len()
    }

    /// Weight function for the HRW algorithm using XOR and xxhash.
    fn hrw_weight(replica_id: &str, cluster: &str) -> u64 {
        xxh3_64(replica_id.as_bytes()) ^ xxh3_64(cluster.as_bytes())
    }

    pub fn ranked_replica_indices(&self, cluster: &str) -> Vec<usize> {
        // Fast path for HA pairs (most common case in production)
        if self.replicas.len() == 2 {
            let w0 = Self::hrw_weight(&self.replicas[0].id, cluster);
            let w1 = Self::hrw_weight(&self.replicas[1].id, cluster);
            return if w0 > w1 { vec![0, 1] } else { vec![1, 0] };
        }

        // General case: cache weights to avoid recomputation during sort
        let mut weights: Vec<(usize, u64)> = self
            .replicas
            .iter()
            .enumerate()
            .map(|(idx, replica)| (idx, Self::hrw_weight(&replica.id, cluster)))
            .collect();

        weights.sort_by(|(a_idx, a_weight), (b_idx, b_weight)| {
            b_weight
                .cmp(a_weight)
                .then_with(|| self.replicas[*a_idx].id.cmp(&self.replicas[*b_idx].id))
        });

        weights.into_iter().map(|(idx, _)| idx).collect()
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

#[cfg(test)]
#[path = "replica_selector_test.rs"]
mod replica_selector_test;
