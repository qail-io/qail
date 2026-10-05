//! Nesting levels: which slot set an acquire waits on.
//!
//! A task holding slots up to level `k` acquires at level `k + 1`, so every
//! wait points one level up and no cycle can form. Tasks holding one
//! connection while waiting for another cannot empty the shared slots
//! between them ([`super::PoolConfig::nested_reserve`]).

use super::lifecycle::PgPoolInner;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Held slots per task, counted by level (index 0 = the shared slots).
pub(super) type LevelHolders = Mutex<HashMap<tokio::task::Id, Vec<u32>>>;

/// One task's claim on a slot at `level`.
///
/// It is registered before the acquire waits, so a second acquire in the
/// same task (under `join!`, say) lands a level higher instead of queueing
/// behind the first at one reserve. Dropping it (acquire failed or cancelled,
/// or the slot it backs given back) removes the registration.
pub(super) struct LevelClaim {
    pool: Arc<PgPoolInner>,
    pub(super) level: usize,
    task: Option<tokio::task::Id>,
}

impl LevelClaim {
    /// Claim the level above the highest one this task holds. Without
    /// reserves, or outside a tokio task, every acquire is at level 0.
    pub(super) fn new(pool: &Arc<PgPoolInner>) -> Self {
        let levels = pool.nested_semaphores.len();
        let task = if levels == 0 {
            None
        } else {
            tokio::task::try_id()
        };
        let Some(task) = task else {
            return Self {
                pool: Arc::clone(pool),
                level: 0,
                task: None,
            };
        };

        let mut holders = pool
            .level_holders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let counts = holders.entry(task).or_insert_with(|| vec![0; levels + 1]);
        let above_held = counts.iter().rposition(|&n| n > 0).map_or(0, |top| top + 1);
        if above_held > levels {
            // Deeper than the reserves: these acquires share the last one,
            // where holders can wait on each other again.
            metrics::counter!("qail_pg_pool_nested_depth_exceeded_total").increment(1);
        }
        let level = above_held.min(levels);
        counts[level] += 1;
        if level > 0 {
            metrics::counter!("qail_pg_pool_nested_acquires_total", "level" => level.to_string())
                .increment(1);
        }
        Self {
            pool: Arc::clone(pool),
            level,
            task: Some(task),
        }
    }
}

impl Drop for LevelClaim {
    fn drop(&mut self) {
        let Some(task) = self.task else {
            return;
        };
        let mut holders = self
            .pool
            .level_holders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(counts) = holders.get_mut(&task) {
            counts[self.level] = counts[self.level].saturating_sub(1);
            if counts.iter().all(|&n| n == 0) {
                holders.remove(&task);
            }
        }
    }
}
