//! SQLite-based sync state and task queue storage
//!
//! This module handles the operational data for sync:
//! - sync_state: tracks incremental sync progress
//! - sync_tasks: persistent task queue for crash recovery

use std::path::Path;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::db::models::{SyncPipeline, SyncState, SyncTask, SyncTaskType, TaskStatus};
use crate::error::{GarminError, Result};

const IN_PROGRESS_RECOVERY_AFTER_SECS: i64 = 30 * 60;

/// Task currently marked in progress in the sync queue.
#[derive(Debug, Clone)]
pub struct ActiveSyncTask {
    pub id: i64,
    pub task_type: SyncTaskType,
    pub pipeline: SyncPipeline,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub in_progress_at: Option<DateTime<Utc>>,
}

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
        let conn = Connection::open_in_memory().map_err(|e| {
            GarminError::Database(format!("Failed to open in-memory database: {}", e))
        })?;

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
                    pipeline TEXT NOT NULL DEFAULT 'frontier',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    in_progress_at TEXT,
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

                -- Backfill tracking table
                CREATE TABLE IF NOT EXISTS backfill_state (
                    profile_id INTEGER NOT NULL,
                    data_type TEXT NOT NULL,
                    frontier_date TEXT NOT NULL,
                    target_date TEXT NOT NULL,
                    is_complete INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (profile_id, data_type)
                );
                "#,
            )
            .map_err(|e| GarminError::Database(format!("Failed to run migrations: {}", e)))?;

        if !self.column_exists("sync_tasks", "pipeline")? {
            self.conn
                .execute(
                    "ALTER TABLE sync_tasks ADD COLUMN pipeline TEXT NOT NULL DEFAULT 'frontier'",
                    [],
                )
                .map_err(|e| {
                    GarminError::Database(format!(
                        "Failed to add pipeline column to sync_tasks: {}",
                        e
                    ))
                })?;
        }

        if !self.column_exists("sync_tasks", "in_progress_at")? {
            self.conn
                .execute("ALTER TABLE sync_tasks ADD COLUMN in_progress_at TEXT", [])
                .map_err(|e| {
                    GarminError::Database(format!(
                        "Failed to add in_progress_at column to sync_tasks: {}",
                        e
                    ))
                })?;
        }

        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_tasks_pipeline_status
                 ON sync_tasks(pipeline, status, next_retry_at)",
                [],
            )
            .map_err(|e| {
                GarminError::Database(format!("Failed to create pipeline index: {}", e))
            })?;

        Ok(())
    }

    fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let query = format!("PRAGMA table_info({})", table);
        let mut stmt = self
            .conn
            .prepare(&query)
            .map_err(|e| GarminError::Database(format!("Failed to inspect table: {}", e)))?;

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| GarminError::Database(format!("Failed to read table info: {}", e)))?;

        for row in rows {
            if row.map_err(|e| GarminError::Database(e.to_string()))? == column {
                return Ok(true);
            }
        }

        Ok(false)
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

    /// Get the most recently synced profile, falling back to most recently created.
    ///
    /// Returns (profile_id, display_name) if at least one profile exists.
    pub fn get_latest_profile(&self) -> Result<Option<(i32, String)>> {
        self.conn
            .query_row(
                "SELECT profile_id, display_name
                 FROM profiles
                 ORDER BY
                     CASE WHEN last_sync_at IS NULL THEN 1 ELSE 0 END,
                     last_sync_at DESC,
                     created_at DESC,
                     profile_id DESC
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to get latest profile: {}", e)))
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
        let date_str = state
            .last_sync_date
            .map(|d| d.format("%Y-%m-%d").to_string());

        self.conn
            .execute(
                "INSERT INTO sync_state (profile_id, data_type, last_sync_date, last_activity_id)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT (profile_id, data_type) DO UPDATE SET
                     last_sync_date = excluded.last_sync_date,
                     last_activity_id = excluded.last_activity_id",
                params![
                    state.profile_id,
                    state.data_type,
                    date_str,
                    state.last_activity_id
                ],
            )
            .map_err(|e| GarminError::Database(format!("Failed to update sync state: {}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Backfill State
    // =========================================================================

    /// Get backfill state for a profile and data type
    /// Returns (frontier_date, target_date, is_complete)
    pub fn get_backfill_state(
        &self,
        profile_id: i32,
        data_type: &str,
    ) -> Result<Option<(NaiveDate, NaiveDate, bool)>> {
        self.conn
            .query_row(
                "SELECT frontier_date, target_date, is_complete
                 FROM backfill_state
                 WHERE profile_id = ? AND data_type = ?",
                params![profile_id, data_type],
                |row| {
                    let frontier: String = row.get(0)?;
                    let target: String = row.get(1)?;
                    let is_complete: bool = row.get(2)?;
                    Ok((
                        NaiveDate::parse_from_str(&frontier, "%Y-%m-%d").unwrap(),
                        NaiveDate::parse_from_str(&target, "%Y-%m-%d").unwrap(),
                        is_complete,
                    ))
                },
            )
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to get backfill state: {}", e)))
    }

    /// Initialize or update backfill state
    pub fn set_backfill_state(
        &self,
        profile_id: i32,
        data_type: &str,
        frontier_date: NaiveDate,
        target_date: NaiveDate,
        is_complete: bool,
    ) -> Result<()> {
        let frontier_str = frontier_date.format("%Y-%m-%d").to_string();
        let target_str = target_date.format("%Y-%m-%d").to_string();

        self.conn
            .execute(
                "INSERT INTO backfill_state (profile_id, data_type, frontier_date, target_date, is_complete, updated_at)
                 VALUES (?, ?, ?, ?, ?, datetime('now'))
                 ON CONFLICT (profile_id, data_type) DO UPDATE SET
                     frontier_date = excluded.frontier_date,
                     target_date = excluded.target_date,
                     is_complete = excluded.is_complete,
                     updated_at = datetime('now')",
                params![profile_id, data_type, frontier_str, target_str, is_complete],
            )
            .map_err(|e| GarminError::Database(format!("Failed to set backfill state: {}", e)))?;

        Ok(())
    }

    /// Update backfill frontier (moves the frontier date backward as we sync older data)
    pub fn update_backfill_frontier(
        &self,
        profile_id: i32,
        data_type: &str,
        new_frontier: NaiveDate,
    ) -> Result<()> {
        let frontier_str = new_frontier.format("%Y-%m-%d").to_string();

        self.conn
            .execute(
                "UPDATE backfill_state
                 SET frontier_date = ?, updated_at = datetime('now')
                 WHERE profile_id = ? AND data_type = ?",
                params![frontier_str, profile_id, data_type],
            )
            .map_err(|e| {
                GarminError::Database(format!("Failed to update backfill frontier: {}", e))
            })?;

        Ok(())
    }

    /// Mark backfill as complete
    pub fn mark_backfill_complete(&self, profile_id: i32, data_type: &str) -> Result<()> {
        self.conn
            .execute(
                "UPDATE backfill_state
                 SET is_complete = 1, updated_at = datetime('now')
                 WHERE profile_id = ? AND data_type = ?",
                params![profile_id, data_type],
            )
            .map_err(|e| {
                GarminError::Database(format!("Failed to mark backfill complete: {}", e))
            })?;

        Ok(())
    }

    /// Check if backfill is complete for a data type
    pub fn is_backfill_complete(&self, profile_id: i32, data_type: &str) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT is_complete FROM backfill_state WHERE profile_id = ? AND data_type = ?",
                params![profile_id, data_type],
                |row| row.get(0),
            )
            .optional()
            .map(|opt| opt.unwrap_or(false))
            .map_err(|e| GarminError::Database(format!("Failed to check backfill status: {}", e)))
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
                "INSERT INTO sync_tasks (profile_id, task_type, task_data, pipeline, status, attempts, last_error)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    task.profile_id,
                    task_type_name(&task.task_type),
                    task_data,
                    pipeline_name(task.pipeline),
                    task.status.to_string(),
                    task.attempts,
                    task.last_error,
                ],
            )
            .map_err(|e| GarminError::Database(format!("Failed to push task: {}", e)))?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Pop the next task from the queue for a profile
    pub fn pop_task(
        &self,
        profile_id: i32,
        pipeline: Option<SyncPipeline>,
    ) -> Result<Option<SyncTask>> {
        let (query, params) = if let Some(pipeline) = pipeline {
            (
                "SELECT id, profile_id, task_type, task_data, pipeline, status, attempts, last_error,
                        created_at, next_retry_at, completed_at
                 FROM sync_tasks
                 WHERE profile_id = ?
                   AND pipeline = ?
                   AND status IN ('pending', 'failed')
                   AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))
                 ORDER BY
                     CASE WHEN status = 'failed' THEN 0 ELSE 1 END,
                     created_at ASC
                 LIMIT 1",
                params![profile_id, pipeline_name(pipeline)],
            )
        } else {
            (
                "SELECT id, profile_id, task_type, task_data, pipeline, status, attempts, last_error,
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
            )
        };

        self.conn
            .query_row(query, params, sync_task_from_row)
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to pop task: {}", e)))
    }

    /// Pop the next task for a profile and task type
    pub fn pop_task_by_type(
        &self,
        profile_id: i32,
        task_type: &str,
        pipeline: Option<SyncPipeline>,
    ) -> Result<Option<SyncTask>> {
        let (query, params) = if let Some(pipeline) = pipeline {
            (
                "SELECT id, profile_id, task_type, task_data, pipeline, status, attempts, last_error,
                        created_at, next_retry_at, completed_at
                 FROM sync_tasks
                 WHERE profile_id = ?
                   AND task_type = ?
                   AND pipeline = ?
                   AND status IN ('pending', 'failed')
                   AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))
                 ORDER BY
                     CASE WHEN status = 'failed' THEN 0 ELSE 1 END,
                     created_at ASC
                 LIMIT 1",
                params![profile_id, task_type, pipeline_name(pipeline)],
            )
        } else {
            (
                "SELECT id, profile_id, task_type, task_data, pipeline, status, attempts, last_error,
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
            )
        };

        self.conn
            .query_row(query, params, sync_task_from_row)
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to pop task by type: {}", e)))
    }

    /// Atomically claim the next eligible task for a profile.
    pub fn claim_next_task(
        &self,
        profile_id: i32,
        pipeline: Option<SyncPipeline>,
    ) -> Result<Option<SyncTask>> {
        let (query, params) = if let Some(pipeline) = pipeline {
            (
                claim_task_query(
                    "profile_id = ?
                       AND pipeline = ?
                       AND status IN ('pending', 'failed')
                       AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))",
                ),
                params![profile_id, pipeline_name(pipeline)],
            )
        } else {
            (
                claim_task_query(
                    "profile_id = ?
                       AND status IN ('pending', 'failed')
                       AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))",
                ),
                params![profile_id],
            )
        };

        self.conn
            .query_row(&query, params, sync_task_from_row)
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to claim task: {}", e)))
    }

    /// Atomically claim the next eligible task for a profile and task type.
    pub fn claim_next_task_by_type(
        &self,
        profile_id: i32,
        task_type: &str,
        pipeline: Option<SyncPipeline>,
    ) -> Result<Option<SyncTask>> {
        let (query, params) = if let Some(pipeline) = pipeline {
            (
                claim_task_query(
                    "profile_id = ?
                       AND task_type = ?
                       AND pipeline = ?
                       AND status IN ('pending', 'failed')
                       AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))",
                ),
                params![profile_id, task_type, pipeline_name(pipeline)],
            )
        } else {
            (
                claim_task_query(
                    "profile_id = ?
                       AND task_type = ?
                       AND status IN ('pending', 'failed')
                       AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))",
                ),
                params![profile_id, task_type],
            )
        };

        self.conn
            .query_row(&query, params, sync_task_from_row)
            .optional()
            .map_err(|e| GarminError::Database(format!("Failed to claim task by type: {}", e)))
    }

    /// Mark a task as in progress
    pub fn mark_task_in_progress(&self, task_id: i64) -> Result<()> {
        let count = self
            .conn
            .execute(
                "UPDATE sync_tasks
                 SET status = 'in_progress',
                     attempts = attempts + 1,
                     in_progress_at = datetime('now'),
                     last_error = NULL
                 WHERE id = ?
                   AND status IN ('pending', 'failed')
                   AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))",
                params![task_id],
            )
            .map_err(|e| {
                GarminError::Database(format!("Failed to mark task in progress: {}", e))
            })?;

        if count == 0 {
            return Err(GarminError::Database(format!(
                "Task {} is not eligible to be marked in progress",
                task_id
            )));
        }

        Ok(())
    }

    /// Mark a task as completed
    pub fn mark_task_completed(&self, task_id: i64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE sync_tasks
                 SET status = 'completed',
                     completed_at = datetime('now'),
                     in_progress_at = NULL
                 WHERE id = ?",
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
                     in_progress_at = NULL,
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
                "UPDATE sync_tasks
                 SET status = 'pending',
                     in_progress_at = NULL
                 WHERE status = 'in_progress'
                   AND (
                       in_progress_at IS NULL
                       OR in_progress_at <= datetime('now', '-' || ? || ' seconds')
                   )",
                params![IN_PROGRESS_RECOVERY_AFTER_SECS],
            )
            .map_err(|e| GarminError::Database(format!("Failed to recover tasks: {}", e)))?;

        Ok(count as u32)
    }

    /// Count pending tasks
    pub fn count_pending_tasks(
        &self,
        profile_id: i32,
        pipeline: Option<SyncPipeline>,
    ) -> Result<u32> {
        let (query, params) = if let Some(pipeline) = pipeline {
            (
                "SELECT COUNT(*) FROM sync_tasks
                 WHERE profile_id = ? AND pipeline = ? AND status IN ('pending', 'in_progress', 'failed')",
                params![profile_id, pipeline_name(pipeline)],
            )
        } else {
            (
                "SELECT COUNT(*) FROM sync_tasks
                 WHERE profile_id = ? AND status IN ('pending', 'in_progress', 'failed')",
                params![profile_id],
            )
        };

        self.conn
            .query_row(query, params, |row| row.get(0))
            .map_err(|e| GarminError::Database(format!("Failed to count tasks: {}", e)))
    }

    /// Count tasks by status
    pub fn count_tasks_by_status(&self, profile_id: i32) -> Result<(u32, u32, u32, u32)> {
        let mut stmt = self
            .conn
            .prepare("SELECT status, COUNT(*) FROM sync_tasks WHERE profile_id = ? GROUP BY status")
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

    /// Count pending/in_progress/failed tasks by type
    ///
    /// Returns (activities, gpx, health, performance) counts
    pub fn count_tasks_by_type(
        &self,
        profile_id: i32,
        pipeline: Option<SyncPipeline>,
    ) -> Result<(u32, u32, u32, u32)> {
        let (query, params) = if let Some(pipeline) = pipeline {
            (
                "SELECT task_type, COUNT(*) FROM sync_tasks
                 WHERE profile_id = ? AND pipeline = ? AND status IN ('pending', 'in_progress', 'failed')
                 GROUP BY task_type",
                params![profile_id, pipeline_name(pipeline)],
            )
        } else {
            (
                "SELECT task_type, COUNT(*) FROM sync_tasks
                 WHERE profile_id = ? AND status IN ('pending', 'in_progress', 'failed')
                 GROUP BY task_type",
                params![profile_id],
            )
        };

        let mut stmt = self
            .conn
            .prepare(query)
            .map_err(|e| GarminError::Database(format!("Failed to prepare query: {}", e)))?;

        let mut activities = 0u32;
        let mut gpx = 0u32;
        let mut health = 0u32;
        let mut performance = 0u32;

        let rows = stmt
            .query_map(params, |row| {
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

    /// List tasks currently marked in progress for a profile.
    pub fn list_in_progress_tasks(&self, profile_id: i32) -> Result<Vec<ActiveSyncTask>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, task_data, pipeline, attempts, last_error, in_progress_at
                 FROM sync_tasks
                 WHERE profile_id = ? AND status = 'in_progress'
                 ORDER BY in_progress_at ASC, id ASC",
            )
            .map_err(|e| {
                GarminError::Database(format!("Failed to prepare active task query: {}", e))
            })?;

        let rows = stmt
            .query_map(params![profile_id], |row| {
                let task_data: String = row.get(1)?;
                let task_type: SyncTaskType = serde_json::from_str(&task_data).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                let pipeline_str: String = row.get(2)?;
                let in_progress_at = row
                    .get::<_, Option<String>>(5)?
                    .and_then(|s| parse_sqlite_datetime(&s));

                Ok(ActiveSyncTask {
                    id: row.get(0)?,
                    task_type,
                    pipeline: parse_pipeline(&pipeline_str),
                    attempts: row.get(3)?,
                    last_error: row.get(4)?,
                    in_progress_at,
                })
            })
            .map_err(|e| GarminError::Database(format!("Failed to query active tasks: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            tasks.push(row.map_err(|e| GarminError::Database(e.to_string()))?);
        }
        Ok(tasks)
    }

    #[cfg(test)]
    pub(crate) fn set_in_progress_at_seconds_ago_for_test(
        &self,
        task_id: i64,
        seconds_ago: i64,
    ) -> Result<()> {
        self.conn
            .execute(
                "UPDATE sync_tasks
                 SET in_progress_at = datetime('now', '-' || ? || ' seconds')
                 WHERE id = ?",
                params![seconds_ago, task_id],
            )
            .map_err(|e| GarminError::Database(format!("Failed to update test task: {}", e)))?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn clear_in_progress_at_for_test(&self, task_id: i64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE sync_tasks SET in_progress_at = NULL WHERE id = ?",
                params![task_id],
            )
            .map_err(|e| GarminError::Database(format!("Failed to update test task: {}", e)))?;
        Ok(())
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

fn pipeline_name(pipeline: SyncPipeline) -> &'static str {
    match pipeline {
        SyncPipeline::Frontier => "frontier",
        SyncPipeline::Backfill => "backfill",
    }
}

fn parse_pipeline(s: &str) -> SyncPipeline {
    match s {
        "backfill" => SyncPipeline::Backfill,
        _ => SyncPipeline::Frontier,
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

fn claim_task_query(where_clause: &str) -> String {
    format!(
        "UPDATE sync_tasks
         SET status = 'in_progress',
             attempts = attempts + 1,
             last_error = NULL,
             in_progress_at = datetime('now')
         WHERE id = (
             SELECT id
             FROM sync_tasks
             WHERE {where_clause}
             ORDER BY
                 CASE WHEN status = 'failed' THEN 0 ELSE 1 END,
                 created_at ASC
             LIMIT 1
         )
         RETURNING id, profile_id, task_type, task_data, pipeline, status, attempts, last_error,
                   created_at, next_retry_at, completed_at"
    )
}

fn sync_task_from_row(row: &Row<'_>) -> rusqlite::Result<SyncTask> {
    let task_data: String = row.get(3)?;
    let task_type: SyncTaskType = serde_json::from_str(&task_data).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let pipeline_str: String = row.get(4)?;
    let status_str: String = row.get(5)?;

    Ok(SyncTask {
        id: Some(row.get(0)?),
        profile_id: row.get(1)?,
        task_type,
        pipeline: parse_pipeline(&pipeline_str),
        status: parse_status(&status_str),
        attempts: row.get(6)?,
        last_error: row.get(7)?,
        created_at: row
            .get::<_, Option<String>>(8)?
            .and_then(|s| parse_sqlite_datetime(&s)),
        next_retry_at: row
            .get::<_, Option<String>>(9)?
            .and_then(|s| parse_sqlite_datetime(&s)),
        completed_at: row
            .get::<_, Option<String>>(10)?
            .and_then(|s| parse_sqlite_datetime(&s)),
    })
}

fn parse_sqlite_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

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

        let task = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            },
        );

        let id = db.push_task(&task).unwrap();
        assert!(id > 0);

        let popped = db.pop_task(1, None).unwrap().unwrap();
        assert_eq!(popped.id, Some(id));

        db.mark_task_in_progress(id).unwrap();
        db.mark_task_completed(id).unwrap();

        // Should be no more pending tasks
        let next = db.pop_task(1, None).unwrap();
        assert!(next.is_none());
    }

    #[test]
    fn test_list_in_progress_tasks_includes_started_at() {
        let db = SyncDb::open_in_memory().unwrap();

        let task = SyncTask::new(
            1,
            SyncPipeline::Backfill,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            },
        );

        let id = db.push_task(&task).unwrap();
        db.mark_task_in_progress(id).unwrap();

        let active = db.list_in_progress_tasks(1).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, id);
        assert_eq!(active[0].pipeline, SyncPipeline::Backfill);
        assert_eq!(active[0].attempts, 1);
        assert!(active[0].in_progress_at.is_some());
    }

    #[test]
    fn test_recover_in_progress_recovers_stale_claims() {
        let db = SyncDb::open_in_memory().unwrap();

        let task = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            },
        );

        let id = db.push_task(&task).unwrap();
        db.mark_task_in_progress(id).unwrap();
        db.set_in_progress_at_seconds_ago_for_test(id, IN_PROGRESS_RECOVERY_AFTER_SECS + 60)
            .unwrap();

        // Simulate crash recovery
        let recovered = db.recover_in_progress_tasks().unwrap();
        assert_eq!(recovered, 1);

        // Task should be poppable again
        let popped = db.pop_task(1, None).unwrap();
        assert!(popped.is_some());
    }

    #[test]
    fn test_recover_in_progress_preserves_fresh_claims() {
        let db = SyncDb::open_in_memory().unwrap();

        let id = db
            .push_task(&SyncTask::new(
                1,
                SyncPipeline::Frontier,
                SyncTaskType::DailyHealth {
                    date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
                },
            ))
            .unwrap();
        db.mark_task_in_progress(id).unwrap();

        let recovered = db.recover_in_progress_tasks().unwrap();
        assert_eq!(recovered, 0);

        let active = db.list_in_progress_tasks(1).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, id);
    }

    #[test]
    fn test_recover_in_progress_recovers_legacy_claims_without_timestamp() {
        let db = SyncDb::open_in_memory().unwrap();

        let id = db
            .push_task(&SyncTask::new(
                1,
                SyncPipeline::Frontier,
                SyncTaskType::DailyHealth {
                    date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
                },
            ))
            .unwrap();
        db.mark_task_in_progress(id).unwrap();
        db.clear_in_progress_at_for_test(id).unwrap();

        let recovered = db.recover_in_progress_tasks().unwrap();
        assert_eq!(recovered, 1);

        let popped = db.pop_task(1, None).unwrap().unwrap();
        assert_eq!(popped.id, Some(id));
    }

    #[test]
    fn test_pop_task_scoped_by_profile() {
        let db = SyncDb::open_in_memory().unwrap();

        let task_profile_1 = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            },
        );
        let task_profile_2 = SyncTask::new(
            2,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 16).unwrap(),
            },
        );

        let id1 = db.push_task(&task_profile_1).unwrap();
        let id2 = db.push_task(&task_profile_2).unwrap();

        let popped_profile_2 = db.pop_task(2, None).unwrap().unwrap();
        assert_eq!(popped_profile_2.id, Some(id2));

        let popped_profile_1 = db.pop_task(1, None).unwrap().unwrap();
        assert_eq!(popped_profile_1.id, Some(id1));
    }

    #[test]
    fn test_pop_task_by_type() {
        let db = SyncDb::open_in_memory().unwrap();

        let task_health = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            },
        );
        let task_perf = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::Performance {
                date: NaiveDate::from_ymd_opt(2024, 12, 22).unwrap(),
            },
        );

        let id_health = db.push_task(&task_health).unwrap();
        let id_perf = db.push_task(&task_perf).unwrap();

        let popped_perf = db
            .pop_task_by_type(1, "performance", None)
            .unwrap()
            .unwrap();
        assert_eq!(popped_perf.id, Some(id_perf));

        let popped_health = db
            .pop_task_by_type(1, "daily_health", None)
            .unwrap()
            .unwrap();
        assert_eq!(popped_health.id, Some(id_health));
    }

    #[test]
    fn test_pop_task_by_pipeline() {
        let db = SyncDb::open_in_memory().unwrap();

        let task_frontier = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            },
        );
        let task_backfill = SyncTask::new(
            1,
            SyncPipeline::Backfill,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 16).unwrap(),
            },
        );

        let id_frontier = db.push_task(&task_frontier).unwrap();
        let id_backfill = db.push_task(&task_backfill).unwrap();

        let popped_backfill = db
            .pop_task(1, Some(SyncPipeline::Backfill))
            .unwrap()
            .unwrap();
        assert_eq!(popped_backfill.id, Some(id_backfill));

        let popped_frontier = db
            .pop_task(1, Some(SyncPipeline::Frontier))
            .unwrap()
            .unwrap();
        assert_eq!(popped_frontier.id, Some(id_frontier));
    }

    #[test]
    fn test_update_backfill_frontier() {
        let db = SyncDb::open_in_memory().unwrap();

        let frontier = NaiveDate::from_ymd_opt(2025, 1, 31).unwrap();
        let target = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        db.set_backfill_state(1, "activities", frontier, target, false)
            .unwrap();

        let new_frontier = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        db.update_backfill_frontier(1, "activities", new_frontier)
            .unwrap();

        let state = db.get_backfill_state(1, "activities").unwrap().unwrap();
        assert_eq!(state.0, new_frontier);
        assert_eq!(state.1, target);
        assert!(!state.2);
    }

    #[test]
    fn test_count_tasks_by_type_includes_failed() {
        let db = SyncDb::open_in_memory().unwrap();

        let task = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            },
        );
        let id = db.push_task(&task).unwrap();

        db.mark_task_in_progress(id).unwrap();
        db.mark_task_failed(id, "boom", 60).unwrap();

        let (_activities, _gpx, health, _perf) = db.count_tasks_by_type(1, None).unwrap();
        assert_eq!(health, 1);
    }

    #[test]
    fn test_claim_next_task_marks_in_progress_once() {
        let db = SyncDb::open_in_memory().unwrap();

        let task = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2026, 7, 5).unwrap(),
            },
        );
        let id = db.push_task(&task).unwrap();

        let claimed = db.claim_next_task(1, None).unwrap().unwrap();
        assert_eq!(claimed.id, Some(id));
        assert_eq!(claimed.status, TaskStatus::InProgress);
        assert_eq!(claimed.attempts, 1);
        assert!(claimed.last_error.is_none());

        let next = db.claim_next_task(1, None).unwrap();
        assert!(next.is_none());

        let active = db.list_in_progress_tasks(1).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].attempts, 1);
    }

    #[test]
    fn test_claim_next_task_by_type_respects_pipeline() {
        let db = SyncDb::open_in_memory().unwrap();

        let frontier = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2026, 7, 5).unwrap(),
            },
        );
        let backfill = SyncTask::new(
            1,
            SyncPipeline::Backfill,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2026, 7, 4).unwrap(),
            },
        );

        let frontier_id = db.push_task(&frontier).unwrap();
        let backfill_id = db.push_task(&backfill).unwrap();

        let claimed_backfill = db
            .claim_next_task_by_type(1, "daily_health", Some(SyncPipeline::Backfill))
            .unwrap()
            .unwrap();
        assert_eq!(claimed_backfill.id, Some(backfill_id));
        assert_eq!(claimed_backfill.pipeline, SyncPipeline::Backfill);

        let claimed_frontier = db
            .claim_next_task_by_type(1, "daily_health", Some(SyncPipeline::Frontier))
            .unwrap()
            .unwrap();
        assert_eq!(claimed_frontier.id, Some(frontier_id));
        assert_eq!(claimed_frontier.pipeline, SyncPipeline::Frontier);
    }

    #[test]
    fn test_claim_next_task_single_winner_across_connections() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("sync.db");

        let db = SyncDb::open(&db_path).unwrap();
        db.push_task(&SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2026, 7, 5).unwrap(),
            },
        ))
        .unwrap();
        drop(db);

        let workers = 8;
        let barrier = Arc::new(Barrier::new(workers));
        let mut handles = Vec::new();
        for _ in 0..workers {
            let db_path = db_path.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let db = SyncDb::open(&db_path).unwrap();
                barrier.wait();
                db.claim_next_task(1, None)
                    .unwrap()
                    .map(|task| task.id.unwrap())
            }));
        }

        let claimed: Vec<i64> = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(claimed.len(), 1);

        let db = SyncDb::open(&db_path).unwrap();
        let active = db.list_in_progress_tasks(1).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].attempts, 1);
    }

    #[test]
    fn test_mark_task_in_progress_rejects_already_claimed_task() {
        let db = SyncDb::open_in_memory().unwrap();

        let id = db
            .push_task(&SyncTask::new(
                1,
                SyncPipeline::Frontier,
                SyncTaskType::DailyHealth {
                    date: NaiveDate::from_ymd_opt(2026, 7, 5).unwrap(),
                },
            ))
            .unwrap();

        db.mark_task_in_progress(id).unwrap();
        assert!(db.mark_task_in_progress(id).is_err());

        let active = db.list_in_progress_tasks(1).unwrap();
        assert_eq!(active[0].attempts, 1);
    }
}
