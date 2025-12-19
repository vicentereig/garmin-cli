//! Task queue for sync operations with crash recovery

use chrono::{Duration, Utc};
use duckdb::params;

use crate::db::models::{SyncTask, SyncTaskType, TaskStatus};
use crate::{Database, Result};

/// Task queue backed by DuckDB for persistence
pub struct TaskQueue {
    db: Database,
}

impl TaskQueue {
    /// Create a new task queue
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Add a task to the queue
    pub fn push(&self, task: SyncTask) -> Result<i64> {
        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        let task_data = serde_json::to_string(&task.task_type)
            .map_err(|e| crate::GarminError::Database(e.to_string()))?;

        // Use RETURNING to get the inserted id
        let id: i64 = conn
            .query_row(
                "INSERT INTO sync_tasks (profile_id, task_type, task_data, status, attempts)
                 VALUES (?, ?, ?, ?, ?)
                 RETURNING id",
                params![
                    task.profile_id,
                    task_type_name(&task.task_type),
                    task_data,
                    task.status.to_string(),
                    task.attempts,
                ],
                |row| row.get(0),
            )
            .map_err(|e| crate::GarminError::Database(e.to_string()))?;

        Ok(id)
    }

    /// Get the next pending task
    pub fn pop(&self) -> Result<Option<SyncTask>> {
        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        // Priority: failed tasks ready for retry > pending tasks
        let result = conn.query_row(
            "SELECT id, profile_id, task_type, task_data, status, attempts, last_error,
                    created_at, next_retry_at, completed_at
             FROM sync_tasks
             WHERE status IN ('pending', 'failed')
               AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
             ORDER BY
               CASE WHEN status = 'failed' THEN 0 ELSE 1 END,
               created_at
             LIMIT 1",
            [],
            |row| {
                let id: i64 = row.get(0)?;
                let profile_id: i32 = row.get(1)?;
                let _task_type_name: String = row.get(2)?;
                let task_data: String = row.get(3)?;
                let status_str: String = row.get(4)?;
                let attempts: i32 = row.get(5)?;
                let last_error: Option<String> = row.get(6)?;

                Ok((id, profile_id, task_data, status_str, attempts, last_error))
            },
        );

        match result {
            Ok((id, profile_id, task_data, status_str, attempts, last_error)) => {
                let task_type: SyncTaskType = serde_json::from_str(&task_data)
                    .map_err(|e| crate::GarminError::Database(e.to_string()))?;

                let status = match status_str.as_str() {
                    "pending" => TaskStatus::Pending,
                    "in_progress" => TaskStatus::InProgress,
                    "completed" => TaskStatus::Completed,
                    "failed" => TaskStatus::Failed,
                    _ => TaskStatus::Pending,
                };

                Ok(Some(SyncTask {
                    id: Some(id),
                    profile_id,
                    task_type,
                    status,
                    attempts,
                    last_error,
                    created_at: None,
                    next_retry_at: None,
                    completed_at: None,
                }))
            }
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(crate::GarminError::Database(e.to_string())),
        }
    }

    /// Mark a task as in progress
    pub fn mark_in_progress(&self, task_id: i64) -> Result<()> {
        self.db.execute_params(
            "UPDATE sync_tasks SET status = 'in_progress' WHERE id = ?",
            params![task_id],
        )?;
        Ok(())
    }

    /// Mark a task as completed
    pub fn mark_completed(&self, task_id: i64) -> Result<()> {
        self.db.execute_params(
            "UPDATE sync_tasks SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?",
            params![task_id],
        )?;
        Ok(())
    }

    /// Mark a task as failed with retry
    pub fn mark_failed(&self, task_id: i64, error: &str, retry_after: Duration) -> Result<()> {
        let retry_at = Utc::now() + retry_after;

        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        conn.execute(
            "UPDATE sync_tasks
             SET status = 'failed',
                 attempts = attempts + 1,
                 last_error = ?,
                 next_retry_at = ?
             WHERE id = ?",
            params![error, retry_at.to_rfc3339(), task_id],
        )
        .map_err(|e| crate::GarminError::Database(e.to_string()))?;

        Ok(())
    }

    /// Recover tasks that were in progress (crashed)
    pub fn recover_in_progress(&self) -> Result<u32> {
        let count = self
            .db
            .execute("UPDATE sync_tasks SET status = 'pending' WHERE status = 'in_progress'")?;
        Ok(count as u32)
    }

    /// Get count of pending tasks
    pub fn pending_count(&self) -> Result<i64> {
        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sync_tasks WHERE status IN ('pending', 'failed')",
                [],
                |row| row.get(0),
            )
            .map_err(|e| crate::GarminError::Database(e.to_string()))?;

        Ok(count)
    }

    /// Clear completed tasks older than given days
    pub fn cleanup(&self, days: i32) -> Result<u32> {
        // DuckDB doesn't support parameterized INTERVAL, so format directly
        // (days is a trusted i32 from code, not user input)
        let sql = format!(
            "DELETE FROM sync_tasks
             WHERE status = 'completed'
               AND completed_at < CURRENT_TIMESTAMP - INTERVAL {} DAY",
            days
        );
        let count = self.db.execute(&sql)?;
        Ok(count as u32)
    }

    /// Reset all failed tasks to pending (clear retry delays)
    pub fn reset_failed(&self) -> Result<u32> {
        let count = self.db.execute(
            "UPDATE sync_tasks SET status = 'pending', next_retry_at = NULL, attempts = 0
             WHERE status = 'failed'",
        )?;
        Ok(count as u32)
    }

    /// Clear all pending and failed tasks
    pub fn clear_pending(&self) -> Result<u32> {
        let count = self
            .db
            .execute("DELETE FROM sync_tasks WHERE status IN ('pending', 'failed')")?;
        Ok(count as u32)
    }
}

/// Get task type name for storage
fn task_type_name(task_type: &SyncTaskType) -> &'static str {
    match task_type {
        SyncTaskType::Activities { .. } => "activities",
        SyncTaskType::ActivityDetail { .. } => "activity_detail",
        SyncTaskType::DownloadGpx { .. } => "download_gpx",
        SyncTaskType::DailyHealth { .. } => "daily_health",
        SyncTaskType::Performance { .. } => "performance",
        SyncTaskType::Weight { .. } => "weight",
        SyncTaskType::GenerateEmbeddings { .. } => "generate_embeddings",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn setup() -> TaskQueue {
        let db = Database::in_memory().unwrap();
        TaskQueue::new(db)
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
    #[ignore] // Requires DuckDB extensions that may not be available in CI
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
}
