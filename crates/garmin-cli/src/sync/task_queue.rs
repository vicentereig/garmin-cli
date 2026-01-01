//! Task queue for sync operations with crash recovery
//!
//! Uses SQLite for task persistence, enabling concurrent read access to Parquet data files.

use std::sync::Arc;

use chrono::Duration;
use tokio::sync::Mutex;

#[cfg(test)]
use crate::db::models::SyncTaskType;
use crate::db::models::SyncTask;
use crate::storage::SyncDb;
use crate::Result;

/// Task queue backed by SQLite for persistence
pub struct TaskQueue {
    sync_db: SyncDb,
    profile_id: i32,
    rr_index: usize,
}

impl TaskQueue {
    /// Create a new task queue
    pub fn new(sync_db: SyncDb, profile_id: i32) -> Self {
        Self {
            sync_db,
            profile_id,
            rr_index: 0,
        }
    }

    /// Add a task to the queue
    pub fn push(&self, task: SyncTask) -> Result<i64> {
        self.sync_db.push_task(&task)
    }

    /// Get the next pending task
    pub fn pop(&self) -> Result<Option<SyncTask>> {
        self.sync_db.pop_task(self.profile_id)
    }

    /// Pop the next task using round-robin across primary task types
    pub fn pop_round_robin(&mut self) -> Result<Option<SyncTask>> {
        const TASK_TYPES: [&str; 4] = [
            "activities",
            "download_gpx",
            "performance",
            "daily_health",
        ];

        for _ in 0..TASK_TYPES.len() {
            let idx = self.rr_index % TASK_TYPES.len();
            self.rr_index = self.rr_index.wrapping_add(1);
            if let Some(task) = self
                .sync_db
                .pop_task_by_type(self.profile_id, TASK_TYPES[idx])?
            {
                return Ok(Some(task));
            }
        }

        // Fallback for other task types
        self.sync_db.pop_task(self.profile_id)
    }

    /// Mark a task as in progress
    pub fn mark_in_progress(&self, task_id: i64) -> Result<()> {
        self.sync_db.mark_task_in_progress(task_id)
    }

    /// Mark a task as completed
    pub fn mark_completed(&self, task_id: i64) -> Result<()> {
        self.sync_db.mark_task_completed(task_id)
    }

    /// Mark a task as failed with retry
    pub fn mark_failed(&self, task_id: i64, error: &str, retry_after: Duration) -> Result<()> {
        self.sync_db.mark_task_failed(task_id, error, retry_after.num_seconds())
    }

    /// Recover tasks that were in progress (crashed)
    pub fn recover_in_progress(&self) -> Result<u32> {
        self.sync_db.recover_in_progress_tasks()
    }

    /// Get count of pending tasks
    pub fn pending_count(&self) -> Result<u32> {
        self.sync_db.count_pending_tasks(self.profile_id)
    }

    /// Update the profile scope for queue operations
    pub fn set_profile_id(&mut self, profile_id: i32) {
        self.profile_id = profile_id;
    }

    /// Get task counts by status
    pub fn count_by_status(&self) -> Result<(u32, u32, u32, u32)> {
        self.sync_db.count_tasks_by_status(self.profile_id)
    }

    /// Get task counts by type (activities, gpx, health, performance)
    pub fn count_by_type(&self) -> Result<(u32, u32, u32, u32)> {
        self.sync_db.count_tasks_by_type(self.profile_id)
    }

    /// Clear completed tasks older than given days
    pub fn cleanup(&self, days: i32) -> Result<u32> {
        self.sync_db.cleanup_completed_tasks(days)
    }

    /// Get the sync database (for sync state operations)
    pub fn sync_db(&self) -> &SyncDb {
        &self.sync_db
    }

    /// Reset all failed tasks to pending
    pub fn reset_failed(&self) -> Result<u32> {
        self.sync_db.reset_failed_tasks()
    }

    /// Clear all pending and failed tasks
    pub fn clear_pending(&self) -> Result<u32> {
        self.sync_db.clear_pending_tasks()
    }
}

/// Thread-safe wrapper for TaskQueue for use in parallel sync
pub struct SharedTaskQueue {
    inner: Arc<Mutex<TaskQueue>>,
}

impl SharedTaskQueue {
    /// Create a new shared task queue
    pub fn new(queue: TaskQueue) -> Self {
        Self {
            inner: Arc::new(Mutex::new(queue)),
        }
    }

    /// Get the next pending task (thread-safe)
    pub async fn pop(&self) -> Result<Option<SyncTask>> {
        let guard = self.inner.lock().await;
        guard.pop()
    }

    /// Get the next pending task using round-robin scheduling (thread-safe)
    pub async fn pop_round_robin(&self) -> Result<Option<SyncTask>> {
        let mut guard = self.inner.lock().await;
        guard.pop_round_robin()
    }

    /// Add a task to the queue (thread-safe)
    pub async fn push(&self, task: SyncTask) -> Result<i64> {
        let guard = self.inner.lock().await;
        guard.push(task)
    }

    /// Mark a task as in progress (thread-safe)
    pub async fn mark_in_progress(&self, task_id: i64) -> Result<()> {
        let guard = self.inner.lock().await;
        guard.mark_in_progress(task_id)
    }

    /// Mark a task as completed (thread-safe)
    pub async fn mark_completed(&self, task_id: i64) -> Result<()> {
        let guard = self.inner.lock().await;
        guard.mark_completed(task_id)
    }

    /// Mark a task as failed with retry (thread-safe)
    pub async fn mark_failed(&self, task_id: i64, error: &str, retry_after: Duration) -> Result<()> {
        let guard = self.inner.lock().await;
        guard.mark_failed(task_id, error, retry_after)
    }

    /// Get count of pending tasks (thread-safe)
    pub async fn pending_count(&self) -> Result<u32> {
        let guard = self.inner.lock().await;
        guard.pending_count()
    }

    /// Update the profile scope for queue operations (thread-safe)
    pub async fn set_profile_id(&self, profile_id: i32) {
        let mut guard = self.inner.lock().await;
        guard.set_profile_id(profile_id);
    }

    /// Get task counts by status (thread-safe)
    pub async fn count_by_status(&self) -> Result<(u32, u32, u32, u32)> {
        let guard = self.inner.lock().await;
        guard.count_by_status()
    }

    /// Recover tasks that were in progress (crashed) (thread-safe)
    pub async fn recover_in_progress(&self) -> Result<u32> {
        let guard = self.inner.lock().await;
        guard.recover_in_progress()
    }

    /// Clear completed tasks older than given days (thread-safe)
    pub async fn cleanup(&self, days: i32) -> Result<u32> {
        let guard = self.inner.lock().await;
        guard.cleanup(days)
    }
}

impl Clone for SharedTaskQueue {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn setup() -> TaskQueue {
        let sync_db = SyncDb::open_in_memory().unwrap();
        TaskQueue::new(sync_db, 1)
    }

    #[test]
    fn test_push_and_pop() {
        let queue = setup();

        let task = SyncTask::new(
            1,
            SyncTaskType::Activities {
                start: 0,
                limit: 50,
            },
        );
        let id = queue.push(task).unwrap();
        assert!(id > 0);

        let popped = queue.pop().unwrap();
        assert!(popped.is_some());
        let popped = popped.unwrap();
        assert_eq!(popped.profile_id, 1);
    }

    #[test]
    fn test_mark_completed() {
        let queue = setup();

        let task = SyncTask::new(
            1,
            SyncTaskType::Activities {
                start: 0,
                limit: 50,
            },
        );
        let id = queue.push(task).unwrap();

        queue.mark_in_progress(id).unwrap();
        queue.mark_completed(id).unwrap();

        // Should not pop completed tasks
        let popped = queue.pop().unwrap();
        assert!(popped.is_none());
    }

    #[test]
    fn test_pending_count() {
        let queue = setup();

        assert_eq!(queue.pending_count().unwrap(), 0);

        queue
            .push(SyncTask::new(
                1,
                SyncTaskType::Activities {
                    start: 0,
                    limit: 50,
                },
            ))
            .unwrap();
        queue
            .push(SyncTask::new(
                1,
                SyncTaskType::DailyHealth {
                    date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                },
            ))
            .unwrap();

        assert_eq!(queue.pending_count().unwrap(), 2);
    }

    #[test]
    fn test_recover_in_progress() {
        let queue = setup();

        let task = SyncTask::new(
            1,
            SyncTaskType::Activities {
                start: 0,
                limit: 50,
            },
        );
        let id = queue.push(task).unwrap();
        queue.mark_in_progress(id).unwrap();

        // Simulate crash recovery
        let recovered = queue.recover_in_progress().unwrap();
        assert_eq!(recovered, 1);

        // Should be able to pop again
        let popped = queue.pop().unwrap();
        assert!(popped.is_some());
    }

    #[test]
    fn test_profile_id_update_affects_pending_count() {
        let sync_db = SyncDb::open_in_memory().unwrap();
        let mut queue = TaskQueue::new(sync_db, 1);

        queue
            .push(SyncTask::new(
                2,
                SyncTaskType::DailyHealth {
                    date: NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
                },
            ))
            .unwrap();

        assert_eq!(queue.pending_count().unwrap(), 0);

        queue.set_profile_id(2);
        assert_eq!(queue.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_pop_round_robin_prefers_activity_first() {
        let sync_db = SyncDb::open_in_memory().unwrap();
        let mut queue = TaskQueue::new(sync_db, 1);

        queue
            .push(SyncTask::new(
                1,
                SyncTaskType::DailyHealth {
                    date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                },
            ))
            .unwrap();
        queue
            .push(SyncTask::new(
                1,
                SyncTaskType::Activities { start: 0, limit: 50 },
            ))
            .unwrap();

        let first = queue.pop_round_robin().unwrap().unwrap();
        assert!(matches!(first.task_type, SyncTaskType::Activities { .. }));
    }
}
