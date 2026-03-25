//! Auto strategy planner for count-oriented query execution.

/// Chosen execution path for auto count planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoCountPath {
    /// Single command path using cached prepared execution.
    SingleCached,
    /// Pipeline path that parses each query in-batch.
    PipelineOneShot,
    /// Pipeline path that reuses cached prepared templates.
    PipelineCached,
    /// Parallel pool path using multiple connections.
    PoolParallel,
}

/// Auto execution plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoCountPlan {
    pub path: AutoCountPath,
    pub workers: usize,
    pub chunk_size: usize,
}

impl AutoCountPlan {
    /// Batch-size threshold where cached pipeline becomes preferable.
    pub const CACHE_MIN_BATCH: usize = 8;
    /// Batch-size threshold where pooled parallel execution becomes worthwhile.
    pub const POOL_MIN_BATCH: usize = 4_096;
    /// Keep enough work per worker to avoid coordination overhead.
    pub const POOL_MIN_CHUNK_PER_WORKER: usize = 1_024;
    /// Hard cap to avoid over-fragmenting a single request.
    pub const POOL_MAX_WORKERS: usize = 16;

    #[inline]
    pub fn for_driver(batch_len: usize) -> Self {
        if batch_len <= 1 {
            return Self {
                path: AutoCountPath::SingleCached,
                workers: 1,
                chunk_size: batch_len.max(1),
            };
        }

        if batch_len < Self::CACHE_MIN_BATCH {
            return Self {
                path: AutoCountPath::PipelineOneShot,
                workers: 1,
                chunk_size: batch_len,
            };
        }

        Self {
            path: AutoCountPath::PipelineCached,
            workers: 1,
            chunk_size: batch_len,
        }
    }

    #[inline]
    pub fn for_pool(batch_len: usize, max_connections: usize, available_slots: usize) -> Self {
        let driver_plan = Self::for_driver(batch_len);

        if batch_len < Self::POOL_MIN_BATCH || max_connections < 2 || available_slots < 2 {
            return driver_plan;
        }

        let by_chunk = batch_len / Self::POOL_MIN_CHUNK_PER_WORKER;
        if by_chunk < 2 {
            return driver_plan;
        }

        let workers = max_connections
            .min(available_slots)
            .min(Self::POOL_MAX_WORKERS)
            .min(by_chunk);

        if workers < 2 {
            return driver_plan;
        }

        let chunk_size = batch_len.div_ceil(workers);
        Self {
            path: AutoCountPath::PoolParallel,
            workers,
            chunk_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AutoCountPath, AutoCountPlan};

    #[test]
    fn driver_auto_plan_resolves_expected_paths() {
        let p0 = AutoCountPlan::for_driver(0);
        assert_eq!(p0.path, AutoCountPath::SingleCached);
        assert_eq!(p0.workers, 1);

        let p1 = AutoCountPlan::for_driver(1);
        assert_eq!(p1.path, AutoCountPath::SingleCached);
        assert_eq!(p1.chunk_size, 1);

        let p2 = AutoCountPlan::for_driver(2);
        assert_eq!(p2.path, AutoCountPath::PipelineOneShot);

        let p8 = AutoCountPlan::for_driver(8);
        assert_eq!(p8.path, AutoCountPath::PipelineCached);
    }

    #[test]
    fn pool_auto_plan_falls_back_when_parallel_not_worth_it() {
        let p = AutoCountPlan::for_pool(512, 10, 10);
        assert_eq!(p.path, AutoCountPath::PipelineCached);
        assert_eq!(p.workers, 1);
    }

    #[test]
    fn pool_auto_plan_uses_parallel_when_thresholds_met() {
        let p = AutoCountPlan::for_pool(16_384, 10, 9);
        assert_eq!(p.path, AutoCountPath::PoolParallel);
        assert!(p.workers >= 2);
        assert!(p.chunk_size >= AutoCountPlan::POOL_MIN_CHUNK_PER_WORKER);
    }
}
