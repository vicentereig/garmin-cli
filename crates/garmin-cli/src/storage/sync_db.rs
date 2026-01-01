//! SQLite-based sync state and task queue storage
//!
//! This module handles the operational data for sync:
//! - sync_state: tracks incremental sync progress
//! - sync_tasks: persistent task queue for crash recovery

use std::path::Path;

use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{params, Connection, OptionalExtension};

use crate::db::models::{SyncState, SyncTask, SyncTaskType, TaskStatus};
use crate::error::{GarminError, Result};

/// SQLite database for sync state and task queue
pub struct SyncDb {
    conn: Connection,
}

impl SyncDb {
    /// Open or create the sync database
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path.as_ref())
            .map_err(|e| GarminError::Database(format!("Failed to open sync database: {}", e)))?;

        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    /// Open an in-memory database (for testing)
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| GarminError::Database(format!("Failed to open in-memory database: {}", e)))?;

        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    /// Run migrations
    fn migrate(&self) -> Result<()> {
        self.conn
            .execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS sync_state (
                    profile_id INTEGER NOT NULL,
                    data_type TEXT NOT NULL,
                    last_sync_date TEXT,
                    last_activity_id INTEGER,
                    PRIMARY KEY (profile_id, data_type)
                );

                CREATE TABLE IF NOT EXISTS sync_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    task_type TEXT NOT NULL,
                    task_data TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    next_retry_at TEXT,
                    completed_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_sync_tasks_status
                ON sync_tasks(status, next_retry_at);

                CREATE TABLE IF NOT EXISTS profiles (
                    profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    display_name TEXT NOT NULL UNIQUE,
                    user_id INTEGER,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    last_sync_at TEXT
                );
                "#,
            )
            .map_err(|e| GarminError::Database(format!("Failed to run migrations: {}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Profile Management
    // =========================================================================

    /// Get or create a profile by display name
    pub fn get_or_create_profile(&self, display_name: &str) -> Result<i32> {
        // Try to get existing
        if let Some(id) = self.get_profile_id(display_name)? {
            return Ok(id);
        }

        // Create new
        self.conn
            .execute(
                "INSERT INTO profiles (display_name) VALUES (?)",
                params![display_name],
            )
            .map_err(|e| GarminError::Database(format!("Failed to create profile: {}", e)))?;

        Ok(self.conn.last_insert_rowid() as i32)
    }

    /// Get profile ID by display name
    pub fn get_profile_id(&self, display_name: &str) -> Result<Option<i32>> {
        self.conn
            .query_row(
                "SELECT profile_id FROM profiles WHERE display_name = ?",
                params![display_name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to get profile: {}", e)))
    }

    /// Update profile's last sync time
    pub fn update_profile_sync_time(&self, profile_id: i32) -> Result<()> {
        self.conn
            .execute(
                "UPDATE profiles SET last_sync_at = datetime('now') WHERE profile_id = ?",
                params![profile_id],
            )
            .map_err(|e| GarminError::Database(format!("Failed to update profile: {}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Sync State
    // =========================================================================

    /// Get sync state for a profile and data type
    pub fn get_sync_state(&self, profile_id: i32, data_type: &str) -> Result<Option<SyncState>> {
        self.conn
            .query_row(
                "SELECT profile_id, data_type, last_sync_date, last_activity_id
                 FROM sync_state
                 WHERE profile_id = ? AND data_type = ?",
                params![profile_id, data_type],
                |row| {
                    Ok(SyncState {
                        profile_id: row.get(0)?,
                        data_type: row.get(1)?,
                        last_sync_date: row
                            .get::<_, Option<String>>(2)?
                            .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                        last_activity_id: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to get sync state: {}", e)))
    }

    /// Update sync state
    pub fn update_sync_state(&self, state: &SyncState) -> Result<()> {
        let date_str = state.last_sync_date.map(|d| d.format("%Y-%m-%d").to_string());

        self.conn
            .execute(
                "INSERT INTO sync_state (profile_id, data_type, last_sync_date, last_activity_id)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT (profile_id, data_type) DO UPDATE SET
                     last_sync_date = excluded.last_sync_date,
                     last_activity_id = excluded.last_activity_id",
                params![state.profile_id, state.data_type, date_str, state.last_activity_id],
            )
            .map_err(|e| GarminError::Database(format!("Failed to update sync state: {}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Task Queue
    // =========================================================================

    /// Push a task to the queue
    pub fn push_task(&self, task: &SyncTask) -> Result<i64> {
        let task_data = serde_json::to_string(&task.task_type)
            .map_err(|e| GarminError::Database(format!("Failed to serialize task: {}", e)))?;

        self.conn
            .execute(
                "INSERT INTO sync_tasks (profile_id, task_type, task_data, status, attempts, last_error)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    task.profile_id,
                    task_type_name(&task.task_type),
                    task_data,
                    task.status.to_string(),
                    task.attempts,
                    task.last_error,
                ],
            )
            .map_err(|e| GarminError::Database(format!("Failed to push task: {}", e)))?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Pop the next task from the queue for a profile
    pub fn pop_task(&self, profile_id: i32) -> Result<Option<SyncTask>> {
        self.conn
            .query_row(
                "SELECT id, profile_id, task_type, task_data, status, attempts, last_error,
                        created_at, next_retry_at, completed_at
                 FROM sync_tasks
                 WHERE profile_id = ?
                   AND status IN ('pending', 'failed')
                   AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))
                 ORDER BY
                     CASE WHEN status = 'failed' THEN 0 ELSE 1 END,
                     created_at ASC
                 LIMIT 1",
                params![profile_id],
                |row| {
                    let task_data: String = row.get(3)?;
                    let task_type: SyncTaskType = serde_json::from_str(&task_data).unwrap();
                    let status_str: String = row.get(4)?;

                    Ok(SyncTask {
                        id: Some(row.get(0)?),
                        profile_id: row.get(1)?,
                        task_type,
                        status: parse_status(&status_str),
                        attempts: row.get(5)?,
                        last_error: row.get(6)?,
                        created_at: row
                            .get::<_, Option<String>>(7)?
                            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                            .map(|dt| dt.with_timezone(&Utc)),
                        next_retry_at: row
                            .get::<_, Option<String>>(8)?
                            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                            .map(|dt| dt.with_timezone(&Utc)),
                        completed_at: row
                            .get::<_, Option<String>>(9)?
                            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                            .map(|dt| dt.with_timezone(&Utc)),
                    })
                },
            )
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to pop task: {}", e)))
    }

    /// Pop the next task for a profile and task type
    pub fn pop_task_by_type(&self, profile_id: i32, task_type: &str) -> Result<Option<SyncTask>> {
        self.conn
            .query_row(
                "SELECT id, profile_id, task_type, task_data, status, attempts, last_error,
                        created_at, next_retry_at, completed_at
                 FROM sync_tasks
                 WHERE profile_id = ?
                   AND task_type = ?
                   AND status IN ('pending', 'failed')
                   AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))
                 ORDER BY
                     CASE WHEN status = 'failed' THEN 0 ELSE 1 END,
                     created_at ASC
                 LIMIT 1",
                params![profile_id, task_type],
                |row| {
                    let task_data: String = row.get(3)?;
                    let task_type: SyncTaskType = serde_json::from_str(&task_data).unwrap();
                    let status_str: String = row.get(4)?;

                    Ok(SyncTask {
                        id: Some(row.get(0)?),
                        profile_id: row.get(1)?,
                        task_type,
                        status: parse_status(&status_str),
                        attempts: row.get(5)?,
                        last_error: row.get(6)?,
                        created_at: row
                            .get::<_, Option<String>>(7)?
                            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                            .map(|dt| dt.with_timezone(&Utc)),
                        next_retry_at: row
                            .get::<_, Option<String>>(8)?
                            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                            .map(|dt| dt.with_timezone(&Utc)),
                        completed_at: row
                            .get::<_, Option<String>>(9)?
                            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                            .map(|dt| dt.with_timezone(&Utc)),
                    })
                },
            )
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to pop task by type: {}", e)))
    }

    /// Mark a task as in progress
    pub fn mark_task_in_progress(&self, task_id: i64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE sync_tasks SET status = 'in_progress', attempts = attempts + 1 WHERE id = ?",
                params![task_id],
            )
            .map_err(|e| GarminError::Database(format!("Failed to mark task in progress: {}", e)))?;

        Ok(())
    }

    /// Mark a task as completed
    pub fn mark_task_completed(&self, task_id: i64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE sync_tasks SET status = 'completed', completed_at = datetime('now') WHERE id = ?",
                params![task_id],
            )
            .map_err(|e| GarminError::Database(format!("Failed to mark task completed: {}", e)))?;

        Ok(())
    }

    /// Mark a task as failed
    pub fn mark_task_failed(&self, task_id: i64, error: &str, retry_delay_secs: i64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE sync_tasks SET
                     status = 'failed',
                     last_error = ?,
                     next_retry_at = datetime('now', '+' || ? || ' seconds')
                 WHERE id = ?",
                params![error, retry_delay_secs, task_id],
            )
            .map_err(|e| GarminError::Database(format!("Failed to mark task failed: {}", e)))?;

        Ok(())
    }

    /// Recover in-progress tasks (after crash)
    pub fn recover_in_progress_tasks(&self) -> Result<u32> {
        let count = self
            .conn
            .execute(
                "UPDATE sync_tasks SET status = 'pending' WHERE status = 'in_progress'",
                [],
            )
            .map_err(|e| GarminError::Database(format!("Failed to recover tasks: {}", e)))?;

        Ok(count as u32)
    }

    /// Count pending tasks
    pub fn count_pending_tasks(&self, profile_id: i32) -> Result<u32> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM sync_tasks
                 WHERE profile_id = ? AND status IN ('pending', 'in_progress', 'failed')",
                params![profile_id],
                |row| row.get(0),
            )
            .map_err(|e| GarminError::Database(format!("Failed to count tasks: {}", e)))
    }

    /// Count tasks by status
    pub fn count_tasks_by_status(&self, profile_id: i32) -> Result<(u32, u32, u32, u32)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT status, COUNT(*) FROM sync_tasks WHERE profile_id = ? GROUP BY status",
            )
            .map_err(|e| GarminError::Database(format!("Failed to prepare query: {}", e)))?;

        let mut pending = 0u32;
        let mut in_progress = 0u32;
        let mut completed = 0u32;
        let mut failed = 0u32;

        let rows = stmt
            .query_map(params![profile_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
            })
            .map_err(|e| GarminError::Database(format!("Failed to query tasks: {}", e)))?;

        for row in rows {
            let (status, count) = row.map_err(|e| GarminError::Database(e.to_string()))?;
            match status.as_str() {
                "pending" => pending = count,
                "in_progress" => in_progress = count,
                "completed" => completed = count,
                "failed" => failed = count,
                _ => {}
            }
        }

        Ok((pending, in_progress, completed, failed))
    }

    /// Count pending/in_progress tasks by type
    ///
    /// Returns (activities, gpx, health, performance) counts
    pub fn count_tasks_by_type(&self, profile_id: i32) -> Result<(u32, u32, u32, u32)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT task_type, COUNT(*) FROM sync_tasks
                 WHERE profile_id = ? AND status IN ('pending', 'in_progress')
                 GROUP BY task_type",
            )
            .map_err(|e| GarminError::Database(format!("Failed to prepare query: {}", e)))?;

        let mut activities = 0u32;
        let mut gpx = 0u32;
        let mut health = 0u32;
        let mut performance = 0u32;

        let rows = stmt
            .query_map(params![profile_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
            })
            .map_err(|e| GarminError::Database(format!("Failed to query tasks: {}", e)))?;

        for row in rows {
            let (task_type, count) = row.map_err(|e| GarminError::Database(e.to_string()))?;
            match task_type.as_str() {
                "activities" => activities = count,
                "download_gpx" => gpx = count,
                "daily_health" => health = count,
                "performance" => performance = count,
                _ => {}
            }
        }

        Ok((activities, gpx, health, performance))
    }

    /// Clean up old completed tasks
    pub fn cleanup_completed_tasks(&self, max_age_days: i32) -> Result<u32> {
        let count = self
            .conn
            .execute(
                "DELETE FROM sync_tasks
                 WHERE status = 'completed'
                   AND completed_at < datetime('now', '-' || ? || ' days')",
                params![max_age_days],
            )
            .map_err(|e| GarminError::Database(format!("Failed to cleanup tasks: {}", e)))?;

        Ok(count as u32)
    }

    /// Check if health data exists for a date
    pub fn has_health_data(&self, _profile_id: i32, _date: NaiveDate) -> Result<bool> {
        // For now, always return false since we're not tracking this in SQLite
        // The actual data check will be done against Parquet files
        Ok(false)
    }

    /// Check if performance data exists for a date
    pub fn has_performance_data(&self, _profile_id: i32, _date: NaiveDate) -> Result<bool> {
        // For now, always return false since we're not tracking this in SQLite
        // The actual data check will be done against Parquet files
        Ok(false)
    }

    /// Reset all failed tasks to pending (clear retry delays)
    pub fn reset_failed_tasks(&self) -> Result<u32> {
        let count = self
            .conn
            .execute(
                "UPDATE sync_tasks SET status = 'pending', next_retry_at = NULL, attempts = 0
                 WHERE status = 'failed'",
                [],
            )
            .map_err(|e| GarminError::Database(format!("Failed to reset tasks: {}", e)))?;

        Ok(count as u32)
    }

    /// Clear all pending and failed tasks
    pub fn clear_pending_tasks(&self) -> Result<u32> {
        let count = self
            .conn
            .execute(
                "DELETE FROM sync_tasks WHERE status IN ('pending', 'failed')",
                [],
            )
            .map_err(|e| GarminError::Database(format!("Failed to clear tasks: {}", e)))?;

        Ok(count as u32)
    }
}

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

fn parse_status(s: &str) -> TaskStatus {
    match s {
        "pending" => TaskStatus::Pending,
        "in_progress" => TaskStatus::InProgress,
        "completed" => TaskStatus::Completed,
        "failed" => TaskStatus::Failed,
        _ => TaskStatus::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_management() {
        let db = SyncDb::open_in_memory().unwrap();

        let id1 = db.get_or_create_profile("test_user").unwrap();
        let id2 = db.get_or_create_profile("test_user").unwrap();
        assert_eq!(id1, id2);

        let id3 = db.get_or_create_profile("another_user").unwrap();
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_sync_state() {
        let db = SyncDb::open_in_memory().unwrap();

        let state = SyncState {
            profile_id: 1,
            data_type: "health".to_string(),
            last_sync_date: Some(NaiveDate::from_ymd_opt(2024, 12, 15).unwrap()),
            last_activity_id: None,
        };

        db.update_sync_state(&state).unwrap();

        let retrieved = db.get_sync_state(1, "health").unwrap().unwrap();
        assert_eq!(retrieved.last_sync_date, state.last_sync_date);
    }

    #[test]
    fn test_task_queue() {
        let db = SyncDb::open_in_memory().unwrap();

        let task = SyncTask::new(1, SyncTaskType::DailyHealth {
            date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
        });

        let id = db.push_task(&task).unwrap();
        assert!(id > 0);

        let popped = db.pop_task(1).unwrap().unwrap();
        assert_eq!(popped.id, Some(id));

        db.mark_task_in_progress(id).unwrap();
        db.mark_task_completed(id).unwrap();

        // Should be no more pending tasks
        let next = db.pop_task(1).unwrap();
        assert!(next.is_none());
    }

    #[test]
    fn test_recover_in_progress() {
        let db = SyncDb::open_in_memory().unwrap();

        let task = SyncTask::new(1, SyncTaskType::DailyHealth {
            date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
        });

        let id = db.push_task(&task).unwrap();
        db.mark_task_in_progress(id).unwrap();

        // Simulate crash recovery
        let recovered = db.recover_in_progress_tasks().unwrap();
        assert_eq!(recovered, 1);

        // Task should be poppable again
        let popped = db.pop_task(1).unwrap();
        assert!(popped.is_some());
    }

    #[test]
    fn test_pop_task_scoped_by_profile() {
        let db = SyncDb::open_in_memory().unwrap();

        let task_profile_1 = SyncTask::new(1, SyncTaskType::DailyHealth {
            date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
        });
        let task_profile_2 = SyncTask::new(2, SyncTaskType::DailyHealth {
            date: NaiveDate::from_ymd_opt(2024, 12, 16).unwrap(),
        });

        let id1 = db.push_task(&task_profile_1).unwrap();
        let id2 = db.push_task(&task_profile_2).unwrap();

        let popped_profile_2 = db.pop_task(2).unwrap().unwrap();
        assert_eq!(popped_profile_2.id, Some(id2));

        let popped_profile_1 = db.pop_task(1).unwrap().unwrap();
        assert_eq!(popped_profile_1.id, Some(id1));
    }

    #[test]
    fn test_pop_task_by_type() {
        let db = SyncDb::open_in_memory().unwrap();

        let task_health = SyncTask::new(1, SyncTaskType::DailyHealth {
            date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
        });
        let task_perf = SyncTask::new(1, SyncTaskType::Performance {
            date: NaiveDate::from_ymd_opt(2024, 12, 22).unwrap(),
        });

        let id_health = db.push_task(&task_health).unwrap();
        let id_perf = db.push_task(&task_perf).unwrap();

        let popped_perf = db.pop_task_by_type(1, "performance").unwrap().unwrap();
        assert_eq!(popped_perf.id, Some(id_perf));

        let popped_health = db.pop_task_by_type(1, "daily_health").unwrap().unwrap();
        assert_eq!(popped_health.id, Some(id_health));
    }
}
