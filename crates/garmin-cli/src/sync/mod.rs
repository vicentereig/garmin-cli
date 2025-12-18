//! Sync module for Garmin data synchronization
//!
//! Provides:
//! - Rate-limited API access with parallel streams
//! - Persistent task queue for crash recovery
//! - Incremental sync with gap detection
//! - GPX parsing for track points
//! - Fancy TUI or simple progress output

pub mod progress;
pub mod rate_limiter;
pub mod task_queue;
pub mod ui;

use std::sync::Arc;

use chrono::{Duration, NaiveDate, Utc};

use crate::client::{GarminClient, OAuth2Token};
use crate::db::models::{SyncTask, SyncTaskType};
use crate::{Database, GarminError, Result};
use std::io::{self, Write};

pub use progress::{SharedProgress, SyncProgress};
pub use rate_limiter::{RateLimiter, SharedRateLimiter};
pub use task_queue::TaskQueue;

/// Sync engine for orchestrating data synchronization
pub struct SyncEngine {
    db: Database,
    client: GarminClient,
    token: OAuth2Token,
    rate_limiter: RateLimiter,
    queue: TaskQueue,
    profile_id: i32,
    display_name: Option<String>,
}

impl SyncEngine {
    /// Create a new sync engine
    pub fn new(db: Database, client: GarminClient, token: OAuth2Token, profile_id: i32) -> Self {
        let queue = TaskQueue::new(db.clone());
        Self {
            db,
            client,
            token,
            rate_limiter: RateLimiter::new(),
            queue,
            profile_id,
            display_name: None,
        }
    }

    /// Fetch and cache the user's display name
    async fn get_display_name(&mut self) -> Result<String> {
        if let Some(ref name) = self.display_name {
            return Ok(name.clone());
        }

        let profile: serde_json::Value = self
            .client
            .get_json(&self.token, "/userprofile-service/socialProfile")
            .await?;

        let name = profile
            .get("displayName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| GarminError::invalid_response("Could not get display name"))?;

        self.display_name = Some(name.clone());
        Ok(name)
    }

    /// Find the oldest activity date by querying the activities API
    async fn find_oldest_activity_date(&mut self) -> Result<NaiveDate> {
        print!("Finding oldest activity date...");
        let _ = io::stdout().flush();

        // The API returns activities sorted by date descending (newest first)
        // Use exponential search to find the end quickly, then fetch the last page

        self.rate_limiter.wait().await;

        // Step 1: Find approximate total count using exponential jumps
        let limit: u32 = 100;
        let mut jump: u32 = 100;
        let mut last_non_empty: u32 = 0;

        // Exponential search: 100, 200, 400, 800, 1600, 3200...
        while jump < 10000 {
            let path = format!(
                "/activitylist-service/activities/search/activities?limit=1&start={}",
                jump
            );

            let activities: Vec<serde_json::Value> =
                self.client.get_json(&self.token, &path).await?;

            if activities.is_empty() {
                break;
            }

            last_non_empty = jump;
            jump *= 2;
            self.rate_limiter.wait().await;
        }

        // Step 2: Binary search to find exact end
        let mut low = last_non_empty;
        let mut high = jump;

        while high - low > limit {
            let mid = (low + high) / 2;
            let path = format!(
                "/activitylist-service/activities/search/activities?limit=1&start={}",
                mid
            );

            self.rate_limiter.wait().await;
            let activities: Vec<serde_json::Value> =
                self.client.get_json(&self.token, &path).await?;

            if activities.is_empty() {
                high = mid;
            } else {
                low = mid;
            }
        }

        // Step 3: Fetch the last page to get the oldest activity
        let path = format!(
            "/activitylist-service/activities/search/activities?limit={}&start={}",
            limit, low
        );

        self.rate_limiter.wait().await;
        let activities: Vec<serde_json::Value> = self.client.get_json(&self.token, &path).await?;

        let oldest_date = activities
            .last()
            .and_then(|activity| activity.get("startTimeLocal"))
            .and_then(|v| v.as_str())
            .and_then(|date_str| date_str.split(' ').next())
            .and_then(|date_part| NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok());

        let result = oldest_date.unwrap_or_else(|| {
            // Default to 1 year ago if no activities found
            Utc::now().date_naive() - Duration::days(365)
        });

        println!(" {}", result);
        Ok(result)
    }

    /// Run the sync process
    pub async fn run(&mut self, opts: SyncOptions) -> Result<SyncStats> {
        // For now, always use sequential mode as parallel requires more refactoring
        // The TUI will be integrated in a future update
        if opts.fancy_ui {
            self.run_with_progress(&opts).await
        } else {
            self.run_sequential(&opts).await
        }
    }

    /// Run sync with fancy progress tracking (TUI or simple)
    async fn run_with_progress(&mut self, opts: &SyncOptions) -> Result<SyncStats> {
        let progress = Arc::new(SyncProgress::new());

        // Fetch display name early
        let display_name = self.get_display_name().await?;
        progress.set_profile(&display_name);

        // Recover any crashed tasks
        let _recovered = self.queue.recover_in_progress()?;

        // Plan phase
        if self.queue.pending_count()? == 0 {
            self.plan_sync(opts).await?;
        }

        // Count tasks by type for progress tracking
        self.count_tasks_for_progress(&progress)?;

        // Determine date range for display
        let from_date = opts
            .from_date
            .unwrap_or_else(|| Utc::now().date_naive() - Duration::days(365));
        let to_date = opts.to_date.unwrap_or_else(|| Utc::now().date_naive());
        progress.set_date_range(&from_date.to_string(), &to_date.to_string());

        // Spawn TUI in background
        let ui_progress = progress.clone();
        let ui_handle = tokio::spawn(async move {
            if let Err(e) = ui::run_tui(ui_progress).await {
                eprintln!("TUI error: {}", e);
            }
        });

        // Run sync with progress updates
        let stats = self
            .run_with_progress_tracking(opts, progress.clone())
            .await?;

        // Wait for TUI to finish
        ui_handle.abort();

        // Print final stats
        println!("\nSync complete: {}", stats);

        Ok(stats)
    }

    /// Run sync with progress tracking (no TUI)
    async fn run_with_progress_tracking(
        &mut self,
        opts: &SyncOptions,
        progress: SharedProgress,
    ) -> Result<SyncStats> {
        let mut stats = SyncStats::default();

        // Execute phase: process tasks
        while let Some(task) = self.queue.pop()? {
            if self.rate_limiter.should_pause() {
                tokio::time::sleep(self.rate_limiter.pause_duration()).await;
            }

            let task_id = task.id.unwrap();
            self.queue.mark_in_progress(task_id)?;

            // Update progress for current task
            update_progress_for_task(&task, &progress);

            self.rate_limiter.wait().await;
            progress.record_request();

            match self.execute_task(&task).await {
                Ok(()) => {
                    self.queue.mark_completed(task_id)?;
                    self.rate_limiter.on_success();
                    stats.completed += 1;
                    complete_progress_for_task(&task, &progress);
                }
                Err(GarminError::RateLimited) => {
                    self.rate_limiter.on_rate_limit();
                    let backoff = self.rate_limiter.current_backoff();
                    self.queue.mark_failed(
                        task_id,
                        "Rate limited",
                        Duration::from_std(backoff).unwrap_or(Duration::seconds(60)),
                    )?;
                    stats.rate_limited += 1;
                }
                Err(e) => {
                    let backoff = Duration::seconds(60);
                    self.queue.mark_failed(task_id, &e.to_string(), backoff)?;
                    stats.failed += 1;
                    fail_progress_for_task(&task, &progress);
                }
            }

            if opts.dry_run {
                break;
            }
        }

        // Cleanup old completed tasks
        self.queue.cleanup(7)?;

        Ok(stats)
    }

    /// Run sync sequentially (original behavior, simple output)
    async fn run_sequential(&mut self, opts: &SyncOptions) -> Result<SyncStats> {
        let mut stats = SyncStats::default();

        // Fetch display name early (needed for health endpoints)
        print!("Fetching user profile...");
        let _ = io::stdout().flush();
        let display_name = self.get_display_name().await?;
        println!(" {}", display_name);

        // Recover any crashed tasks
        let recovered = self.queue.recover_in_progress()?;
        if recovered > 0 {
            println!("  Recovered {} tasks from previous run", recovered);
            stats.recovered = recovered;
        }

        // Plan phase: generate tasks if queue is empty
        if self.queue.pending_count()? == 0 {
            println!("Planning sync tasks...");
            self.plan_sync(opts).await?;
        }

        let total_tasks = self.queue.pending_count()?;
        println!("  {} tasks queued\n", total_tasks);

        // Execute phase: process tasks
        while let Some(task) = self.queue.pop()? {
            if self.rate_limiter.should_pause() {
                println!(
                    "  Rate limited, pausing for {} seconds...",
                    self.rate_limiter.pause_duration().as_secs()
                );
                tokio::time::sleep(self.rate_limiter.pause_duration()).await;
            }

            let task_id = task.id.unwrap();
            self.queue.mark_in_progress(task_id)?;

            // Print task description
            print_task_status(&task, &stats);

            self.rate_limiter.wait().await;

            match self.execute_task(&task).await {
                Ok(()) => {
                    self.queue.mark_completed(task_id)?;
                    self.rate_limiter.on_success();
                    stats.completed += 1;
                    println!(" done");
                }
                Err(GarminError::RateLimited) => {
                    self.rate_limiter.on_rate_limit();
                    let backoff = self.rate_limiter.current_backoff();
                    self.queue.mark_failed(
                        task_id,
                        "Rate limited",
                        Duration::from_std(backoff).unwrap_or(Duration::seconds(60)),
                    )?;
                    stats.rate_limited += 1;
                    println!(" rate limited (retry in {}s)", backoff.as_secs());
                }
                Err(e) => {
                    let backoff = Duration::seconds(60);
                    self.queue.mark_failed(task_id, &e.to_string(), backoff)?;
                    stats.failed += 1;
                    println!(" failed: {}", e);
                }
            }

            if opts.dry_run {
                break;
            }
        }

        // Cleanup old completed tasks
        self.queue.cleanup(7)?;

        Ok(stats)
    }

    /// Count pending tasks by type and update progress
    fn count_tasks_for_progress(&self, progress: &SyncProgress) -> Result<()> {
        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        // Count activities tasks
        let act_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sync_tasks WHERE status IN ('pending', 'failed') AND task_type = 'activities'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        progress.activities.set_total(act_count as u32);

        // Count GPX tasks
        let gpx_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sync_tasks WHERE status IN ('pending', 'failed') AND task_type = 'download_gpx'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        progress.gpx.set_total(gpx_count as u32);

        // Count health tasks
        let health_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sync_tasks WHERE status IN ('pending', 'failed') AND task_type = 'daily_health'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        progress.health.set_total(health_count as u32);

        // Count performance tasks
        let perf_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sync_tasks WHERE status IN ('pending', 'failed') AND task_type = 'performance'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        progress.performance.set_total(perf_count as u32);

        Ok(())
    }

    /// Plan sync tasks based on current state
    async fn plan_sync(&mut self, opts: &SyncOptions) -> Result<()> {
        // Determine date range - auto-detect from oldest activity if not specified
        let from_date = match opts.from_date {
            Some(date) => date,
            None => self.find_oldest_activity_date().await?,
        };
        let to_date = opts.to_date.unwrap_or_else(|| Utc::now().date_naive());

        let total_days = (to_date - from_date).num_days();
        println!(
            "  Date range: {} to {} ({} days)",
            from_date, to_date, total_days
        );

        // Plan activity sync
        if opts.sync_activities {
            println!("  Planning activity sync...");
            self.plan_activities_sync()?;
        }

        // Plan health sync
        if opts.sync_health {
            let health_tasks = self.plan_health_sync(from_date, to_date)?;
            println!("  Planning health sync: {} days to fetch", health_tasks);
        }

        // Plan performance sync
        if opts.sync_performance {
            let perf_tasks = self.plan_performance_sync(from_date, to_date)?;
            println!("  Planning performance sync: {} weeks to fetch", perf_tasks);
        }

        Ok(())
    }

    /// Plan activity sync tasks
    fn plan_activities_sync(&self) -> Result<()> {
        // Start with first page, we'll add more as we discover them
        let task = SyncTask::new(
            self.profile_id,
            SyncTaskType::Activities {
                start: 0,
                limit: 50,
            },
        );
        self.queue.push(task)?;
        Ok(())
    }

    /// Plan health sync tasks for date range, returns count of tasks added
    fn plan_health_sync(&self, from: NaiveDate, to: NaiveDate) -> Result<u32> {
        let mut count = 0;
        let mut date = from;
        while date <= to {
            // Check if we already have data for this date
            if !self.has_health_data(date)? {
                let task = SyncTask::new(self.profile_id, SyncTaskType::DailyHealth { date });
                self.queue.push(task)?;
                count += 1;
            }
            date += Duration::days(1);
        }
        Ok(count)
    }

    /// Plan performance sync tasks, returns count of tasks added
    fn plan_performance_sync(&self, from: NaiveDate, to: NaiveDate) -> Result<u32> {
        // Performance metrics don't change daily, sync weekly
        let mut count = 0;
        let mut date = from;
        while date <= to {
            if !self.has_performance_data(date)? {
                let task = SyncTask::new(self.profile_id, SyncTaskType::Performance { date });
                self.queue.push(task)?;
                count += 1;
            }
            date += Duration::days(7);
        }
        Ok(count)
    }

    /// Check if health data exists for date
    fn has_health_data(&self, date: NaiveDate) -> Result<bool> {
        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM daily_health WHERE profile_id = ? AND date = ?",
                duckdb::params![self.profile_id, date.to_string()],
                |row| row.get(0),
            )
            .map_err(|e| GarminError::Database(e.to_string()))?;

        Ok(count > 0)
    }

    /// Check if performance data exists for date
    fn has_performance_data(&self, date: NaiveDate) -> Result<bool> {
        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM performance_metrics WHERE profile_id = ? AND date = ?",
                duckdb::params![self.profile_id, date.to_string()],
                |row| row.get(0),
            )
            .map_err(|e| GarminError::Database(e.to_string()))?;

        Ok(count > 0)
    }

    /// Execute a single sync task
    async fn execute_task(&mut self, task: &SyncTask) -> Result<()> {
        match &task.task_type {
            SyncTaskType::Activities { start, limit } => self.sync_activities(*start, *limit).await,
            SyncTaskType::ActivityDetail { activity_id } => {
                self.sync_activity_detail(*activity_id).await
            }
            SyncTaskType::DownloadGpx { activity_id, .. } => self.download_gpx(*activity_id).await,
            SyncTaskType::DailyHealth { date } => self.sync_daily_health(*date).await,
            SyncTaskType::Performance { date } => self.sync_performance(*date).await,
            SyncTaskType::Weight { from, to } => self.sync_weight(*from, *to).await,
            SyncTaskType::GenerateEmbeddings { activity_ids } => {
                self.generate_embeddings(activity_ids).await
            }
        }
    }

    /// Sync activities list
    async fn sync_activities(&mut self, start: u32, limit: u32) -> Result<()> {
        let path = format!(
            "/activitylist-service/activities/search/activities?limit={}&start={}",
            limit, start
        );
        let activities: Vec<serde_json::Value> = self.client.get_json(&self.token, &path).await?;

        for activity in &activities {
            // Store activity summary
            self.store_activity(activity)?;

            // Queue GPX download for activities with GPS
            if activity
                .get("hasPolyline")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                if let Some(id) = activity.get("activityId").and_then(|v| v.as_i64()) {
                    let activity_name = activity
                        .get("activityName")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let activity_date = activity
                        .get("startTimeLocal")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.split(' ').next())
                        .map(|s| s.to_string());

                    let task = SyncTask::new(
                        self.profile_id,
                        SyncTaskType::DownloadGpx {
                            activity_id: id,
                            activity_name,
                            activity_date,
                        },
                    );
                    self.queue.push(task)?;
                }
            }
        }

        // If we got a full page, there might be more
        if activities.len() == limit as usize {
            let task = SyncTask::new(
                self.profile_id,
                SyncTaskType::Activities {
                    start: start + limit,
                    limit,
                },
            );
            self.queue.push(task)?;
        }

        Ok(())
    }

    /// Store an activity in the database
    fn store_activity(&self, activity: &serde_json::Value) -> Result<()> {
        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        let activity_id = activity
            .get("activityId")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| GarminError::invalid_response("Missing activityId"))?;

        conn.execute(
            "INSERT INTO activities (
                activity_id, profile_id, activity_name, activity_type,
                start_time_local, duration_sec, distance_m, calories,
                avg_hr, max_hr, avg_speed, max_speed,
                elevation_gain, elevation_loss, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (activity_id) DO UPDATE SET
                activity_name = EXCLUDED.activity_name,
                activity_type = EXCLUDED.activity_type,
                raw_json = EXCLUDED.raw_json",
            duckdb::params![
                activity_id,
                self.profile_id,
                activity.get("activityName").and_then(|v| v.as_str()),
                activity
                    .get("activityType")
                    .and_then(|v| v.get("typeKey"))
                    .and_then(|v| v.as_str()),
                activity.get("startTimeLocal").and_then(|v| v.as_str()),
                activity.get("duration").and_then(|v| v.as_f64()),
                activity.get("distance").and_then(|v| v.as_f64()),
                activity
                    .get("calories")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                activity
                    .get("averageHR")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                activity
                    .get("maxHR")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                activity.get("averageSpeed").and_then(|v| v.as_f64()),
                activity.get("maxSpeed").and_then(|v| v.as_f64()),
                activity.get("elevationGain").and_then(|v| v.as_f64()),
                activity.get("elevationLoss").and_then(|v| v.as_f64()),
                activity.to_string(),
            ],
        )
        .map_err(|e| GarminError::Database(e.to_string()))?;

        Ok(())
    }

    /// Sync activity detail (not implemented yet)
    async fn sync_activity_detail(&mut self, _activity_id: i64) -> Result<()> {
        // TODO: Fetch detailed activity data
        Ok(())
    }

    /// Download and parse GPX
    async fn download_gpx(&mut self, activity_id: i64) -> Result<()> {
        let path = format!("/download-service/export/gpx/activity/{}", activity_id);
        let gpx_bytes = self.client.download(&self.token, &path).await?;
        let gpx_data = String::from_utf8_lossy(&gpx_bytes);
        self.parse_and_store_gpx(activity_id, &gpx_data)?;
        Ok(())
    }

    /// Parse GPX and store track points
    fn parse_and_store_gpx(&self, activity_id: i64, gpx_data: &str) -> Result<()> {
        use gpx::read;
        use std::io::BufReader;

        let reader = BufReader::new(gpx_data.as_bytes());
        let gpx = read(reader).map_err(|e| GarminError::invalid_response(e.to_string()))?;

        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        for track in gpx.tracks {
            for segment in track.segments {
                for point in segment.points {
                    let timestamp = point
                        .time
                        .map(|t| t.format().unwrap_or_default())
                        .unwrap_or_default();

                    conn.execute(
                        "INSERT INTO track_points (activity_id, timestamp, lat, lon, elevation)
                         VALUES (?, ?, ?, ?, ?)",
                        duckdb::params![
                            activity_id,
                            timestamp,
                            point.point().y(),
                            point.point().x(),
                            point.elevation,
                        ],
                    )
                    .map_err(|e| GarminError::Database(e.to_string()))?;
                }
            }
        }

        Ok(())
    }

    /// Sync daily health data
    async fn sync_daily_health(&mut self, date: NaiveDate) -> Result<()> {
        // Get user's display name for the endpoint
        let display_name = self.get_display_name().await?;

        let path = format!(
            "/usersummary-service/usersummary/daily/{}?calendarDate={}",
            display_name, date
        );

        // Try to fetch health data - may return 404/error for dates without data
        let health_result: std::result::Result<serde_json::Value, _> =
            self.client.get_json(&self.token, &path).await;

        let health = match health_result {
            Ok(data) => data,
            Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => {
                // No data for this date - store empty record to mark as synced
                // This prevents re-fetching dates that never had data (before device ownership, gaps, etc.)
                serde_json::json!({})
            }
            Err(e) => return Err(e),
        };

        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        conn.execute(
            "INSERT INTO daily_health (
                profile_id, date, steps, step_goal, total_calories, active_calories,
                resting_hr, sleep_seconds, avg_stress, max_stress,
                body_battery_start, body_battery_end, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (profile_id, date) DO UPDATE SET
                steps = EXCLUDED.steps,
                step_goal = EXCLUDED.step_goal,
                total_calories = EXCLUDED.total_calories,
                active_calories = EXCLUDED.active_calories,
                resting_hr = EXCLUDED.resting_hr,
                sleep_seconds = EXCLUDED.sleep_seconds,
                avg_stress = EXCLUDED.avg_stress,
                max_stress = EXCLUDED.max_stress,
                body_battery_start = EXCLUDED.body_battery_start,
                body_battery_end = EXCLUDED.body_battery_end,
                raw_json = EXCLUDED.raw_json",
            duckdb::params![
                self.profile_id,
                date.to_string(),
                health
                    .get("totalSteps")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("dailyStepGoal")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("totalKilocalories")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("activeKilocalories")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("restingHeartRate")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("sleepingSeconds")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("averageStressLevel")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("maxStressLevel")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("bodyBatteryChargedValue")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health
                    .get("bodyBatteryDrainedValue")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                health.to_string(),
            ],
        )
        .map_err(|e| GarminError::Database(e.to_string()))?;

        Ok(())
    }

    /// Sync performance metrics
    async fn sync_performance(&mut self, date: NaiveDate) -> Result<()> {
        // Fetch VO2 max
        let vo2max: Option<serde_json::Value> = self
            .client
            .get_json(&self.token, "/metrics-service/metrics/maxmet/latest")
            .await
            .ok();

        // Fetch race predictions
        let race_predictions: Option<serde_json::Value> = self
            .client
            .get_json(
                &self.token,
                "/metrics-service/metrics/racepredictions/latest",
            )
            .await
            .ok();

        let conn = self.db.connection();
        let conn = conn.lock().unwrap();

        let vo2max_value = vo2max
            .as_ref()
            .and_then(|v| v.get("generic"))
            .and_then(|v| v.get("vo2MaxValue"))
            .and_then(|v| v.as_f64());

        let fitness_age = vo2max
            .as_ref()
            .and_then(|v| v.get("generic"))
            .and_then(|v| v.get("fitnessAge"))
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);

        let race_5k = race_predictions
            .as_ref()
            .and_then(|v| v.get("time5K"))
            .and_then(|v| v.as_f64())
            .map(|v| v as i32);

        let race_10k = race_predictions
            .as_ref()
            .and_then(|v| v.get("time10K"))
            .and_then(|v| v.as_f64())
            .map(|v| v as i32);

        let race_half = race_predictions
            .as_ref()
            .and_then(|v| v.get("timeHalfMarathon"))
            .and_then(|v| v.as_f64())
            .map(|v| v as i32);

        let race_marathon = race_predictions
            .as_ref()
            .and_then(|v| v.get("timeMarathon"))
            .and_then(|v| v.as_f64())
            .map(|v| v as i32);

        conn.execute(
            "INSERT INTO performance_metrics (
                profile_id, date, vo2max, fitness_age,
                race_5k_sec, race_10k_sec, race_half_sec, race_marathon_sec
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (profile_id, date) DO UPDATE SET
                vo2max = EXCLUDED.vo2max,
                fitness_age = EXCLUDED.fitness_age,
                race_5k_sec = EXCLUDED.race_5k_sec,
                race_10k_sec = EXCLUDED.race_10k_sec,
                race_half_sec = EXCLUDED.race_half_sec,
                race_marathon_sec = EXCLUDED.race_marathon_sec",
            duckdb::params![
                self.profile_id,
                date.to_string(),
                vo2max_value,
                fitness_age,
                race_5k,
                race_10k,
                race_half,
                race_marathon,
            ],
        )
        .map_err(|e| GarminError::Database(e.to_string()))?;

        Ok(())
    }

    /// Sync weight data
    async fn sync_weight(&mut self, _from: NaiveDate, _to: NaiveDate) -> Result<()> {
        // TODO: Implement weight sync
        Ok(())
    }

    /// Generate embeddings for activities
    async fn generate_embeddings(&mut self, _activity_ids: &[i64]) -> Result<()> {
        // TODO: Implement embedding generation using fastembed
        Ok(())
    }
}

/// Options for sync operation
#[derive(Default)]
pub struct SyncOptions {
    /// Sync activities
    pub sync_activities: bool,
    /// Sync daily health
    pub sync_health: bool,
    /// Sync performance metrics
    pub sync_performance: bool,
    /// Start date for sync
    pub from_date: Option<NaiveDate>,
    /// End date for sync
    pub to_date: Option<NaiveDate>,
    /// Dry run (plan only, don't execute)
    pub dry_run: bool,
    /// Force re-sync (ignore existing data)
    pub force: bool,
    /// Use fancy TUI (default: true, set false for --simple)
    pub fancy_ui: bool,
    /// Number of concurrent API requests (default: 3)
    pub concurrency: usize,
}

impl SyncOptions {
    /// Create options for full sync
    pub fn full() -> Self {
        Self {
            sync_activities: true,
            sync_health: true,
            sync_performance: true,
            fancy_ui: true,
            concurrency: 3,
            ..Default::default()
        }
    }

    /// Create options for simple (non-TUI) mode
    pub fn simple() -> Self {
        Self {
            sync_activities: true,
            sync_health: true,
            sync_performance: true,
            fancy_ui: false,
            concurrency: 3,
            ..Default::default()
        }
    }
}

/// Statistics from sync operation
#[derive(Default)]
pub struct SyncStats {
    /// Tasks recovered from previous run
    pub recovered: u32,
    /// Tasks completed successfully
    pub completed: u32,
    /// Tasks that hit rate limits
    pub rate_limited: u32,
    /// Tasks that failed
    pub failed: u32,
}

impl std::fmt::Display for SyncStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Completed: {}, Failed: {}, Rate limited: {}",
            self.completed, self.failed, self.rate_limited
        )?;
        if self.recovered > 0 {
            write!(f, ", Recovered: {}", self.recovered)?;
        }
        Ok(())
    }
}

/// Print task status with nice formatting
fn print_task_status(task: &SyncTask, stats: &SyncStats) {
    let desc = match &task.task_type {
        SyncTaskType::Activities { start, limit } => {
            format!("Fetching activities {}-{}", start, start + limit)
        }
        SyncTaskType::ActivityDetail { activity_id } => {
            format!("Activity {} details", activity_id)
        }
        SyncTaskType::DownloadGpx {
            activity_id,
            activity_name,
            activity_date,
        } => {
            let name = activity_name.as_deref().unwrap_or("Unknown");
            let date = activity_date.as_deref().unwrap_or("");
            if date.is_empty() {
                format!("GPX: {} ({})", name, activity_id)
            } else {
                format!("GPX: {} {} ({})", date, name, activity_id)
            }
        }
        SyncTaskType::DailyHealth { date } => {
            format!("Health data for {}", date)
        }
        SyncTaskType::Performance { date } => {
            format!("Performance metrics for {}", date)
        }
        SyncTaskType::Weight { from, to } => {
            format!("Weight {} to {}", from, to)
        }
        SyncTaskType::GenerateEmbeddings { activity_ids } => {
            format!(
                "Generating embeddings for {} activities",
                activity_ids.len()
            )
        }
    };

    print!("[{:>4}] {}...", stats.completed + 1, desc);
    let _ = io::stdout().flush();
}

/// Update progress when starting a task
fn update_progress_for_task(task: &SyncTask, progress: &SyncProgress) {
    let desc = match &task.task_type {
        SyncTaskType::Activities { start, limit } => {
            format!("Activities {}-{}", start, start + limit)
        }
        SyncTaskType::DownloadGpx {
            activity_name,
            activity_date,
            ..
        } => {
            let name = activity_name.as_deref().unwrap_or("Unknown");
            let date = activity_date.as_deref().unwrap_or("");
            if date.is_empty() {
                name.to_string()
            } else {
                format!("{} {}", date, name)
            }
        }
        SyncTaskType::DailyHealth { date } => date.to_string(),
        SyncTaskType::Performance { date } => date.to_string(),
        _ => String::new(),
    };

    match &task.task_type {
        SyncTaskType::Activities { .. } => progress.activities.set_last_item(desc),
        SyncTaskType::DownloadGpx { .. } => progress.gpx.set_last_item(desc),
        SyncTaskType::DailyHealth { .. } => progress.health.set_last_item(desc),
        SyncTaskType::Performance { .. } => progress.performance.set_last_item(desc),
        _ => {}
    }
}

/// Mark a task as completed in progress
fn complete_progress_for_task(task: &SyncTask, progress: &SyncProgress) {
    match &task.task_type {
        SyncTaskType::Activities { .. } => {
            progress.activities.complete_one();
            // Activities can spawn GPX downloads, so update GPX total
            // This is handled dynamically when GPX tasks are added
        }
        SyncTaskType::DownloadGpx { .. } => progress.gpx.complete_one(),
        SyncTaskType::DailyHealth { .. } => progress.health.complete_one(),
        SyncTaskType::Performance { .. } => progress.performance.complete_one(),
        _ => {}
    }
}

/// Mark a task as failed in progress
fn fail_progress_for_task(task: &SyncTask, progress: &SyncProgress) {
    match &task.task_type {
        SyncTaskType::Activities { .. } => progress.activities.fail_one(),
        SyncTaskType::DownloadGpx { .. } => progress.gpx.fail_one(),
        SyncTaskType::DailyHealth { .. } => progress.health.fail_one(),
        SyncTaskType::Performance { .. } => progress.performance.fail_one(),
        _ => {}
    }
}
