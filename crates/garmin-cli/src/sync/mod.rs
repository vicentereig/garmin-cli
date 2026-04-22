//! Sync module for Garmin data synchronization
//!
//! Provides:
//! - Rate-limited API access with parallel streams
//! - Persistent task queue for crash recovery (SQLite)
//! - Incremental sync with gap detection
//! - GPX parsing for track points
//! - Parquet storage for concurrent read access
//! - Producer/consumer pipeline for concurrent fetching and writing
//! - Plain terminal progress output

pub mod progress;
pub mod rate_limiter;
pub mod task_queue;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use tokio::sync::{mpsc, Mutex as TokioMutex};

use crate::client::{GarminClient, OAuth2Token};
use crate::db::models::{
    Activity, DailyHealth, PerformanceMetrics, SyncPipeline, SyncTask, SyncTaskType, TrackPoint,
};
use crate::storage::{ParquetStore, Storage, SyncDb};
use crate::{GarminError, Result};
use std::io::{self, Write};

pub use progress::{PlanningStep, SharedProgress, SyncProgress};
pub use rate_limiter::{RateLimiter, SharedRateLimiter};
pub use task_queue::{SharedTaskQueue, TaskQueue};

fn pipeline_filter(mode: progress::SyncMode) -> Option<SyncPipeline> {
    match mode {
        progress::SyncMode::Latest => Some(SyncPipeline::Frontier),
        progress::SyncMode::Backfill => Some(SyncPipeline::Backfill),
    }
}

/// Data produced by API fetchers, consumed by Parquet writers
#[derive(Debug)]
enum SyncData {
    /// Activity list with parsed activities and potential follow-up tasks
    Activities {
        records: Vec<Activity>,
        gpx_tasks: Vec<SyncTask>,
        next_page: Option<SyncTask>,
        task_id: i64,
    },
    /// Daily health record
    Health { record: DailyHealth, task_id: i64 },
    /// Performance metrics record
    Performance {
        record: PerformanceMetrics,
        task_id: i64,
    },
    /// Track points from GPX
    TrackPoints {
        #[allow(dead_code)]
        activity_id: i64,
        date: NaiveDate,
        points: Vec<TrackPoint>,
        task_id: i64,
    },
}

/// Sync engine for orchestrating data synchronization
pub struct SyncEngine {
    storage: Storage,
    client: GarminClient,
    token: OAuth2Token,
    rate_limiter: RateLimiter,
    queue: TaskQueue,
    profile_id: i32,
    display_name: Option<String>,
}

#[derive(Clone)]
struct ProducerContext {
    rate_limiter: SharedRateLimiter,
    client: GarminClient,
    token: OAuth2Token,
    progress: SharedProgress,
    display_name: Arc<String>,
    profile_id: i32,
    stats: Arc<TokioMutex<SyncStats>>,
    in_flight: Arc<AtomicUsize>,
    parquet: Arc<ParquetStore>,
    force: bool,
    pipeline_filter: Option<SyncPipeline>,
}

type SleepMetrics = (
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
);

impl SyncEngine {
    /// Create a new sync engine with default storage location
    pub fn new(client: GarminClient, token: OAuth2Token) -> Result<Self> {
        let storage = Storage::open_default()?;
        Self::with_storage(storage, client, token)
    }

    /// Create a new sync engine with custom storage
    pub fn with_storage(
        storage: Storage,
        client: GarminClient,
        token: OAuth2Token,
    ) -> Result<Self> {
        // Get or create profile (will be updated with display name after API call)
        let profile_id = storage.sync_db.get_or_create_profile("default")?;

        // Create task queue using the sync database
        let sync_db = SyncDb::open(storage.base_path().join("sync.db"))?;
        let queue = TaskQueue::new(sync_db, profile_id, None);

        Ok(Self {
            storage,
            client,
            token,
            rate_limiter: RateLimiter::new(),
            queue,
            profile_id,
            display_name: None,
        })
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

        // Update profile in database
        self.profile_id = self.storage.sync_db.get_or_create_profile(&name)?;
        self.queue.set_profile_id(self.profile_id);

        self.display_name = Some(name.clone());
        Ok(name)
    }

    /// Find the oldest activity date and total count by querying the activities API
    /// Returns (oldest_date, total_activities, activities_with_gps)
    pub(crate) async fn find_oldest_activity_date(
        &mut self,
        progress: Option<&SyncProgress>,
    ) -> Result<(NaiveDate, u32, u32)> {
        if let Some(p) = progress {
            p.set_planning_step(PlanningStep::FindingOldestActivity);
        } else {
            print!("Finding oldest activity date...");
            let _ = io::stdout().flush();
        }

        // The API returns activities sorted by date descending (newest first)
        // Use exponential search to find the end quickly, then fetch the last page

        self.rate_limiter.wait().await;

        // Step 1: Find approximate total count using exponential jumps
        let limit: u32 = 100;
        let mut jump: u32 = 100;
        let mut last_non_empty: u32 = 0;
        let max_jump: u32 = 1_000_000;

        // Exponential search: 100, 200, 400, 800, 1600, 3200...
        while jump < max_jump {
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
            jump = jump.saturating_mul(2);
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

        // Calculate total activities
        let total_activities = low + activities.len() as u32;

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

        // Estimate activities with GPS (typically ~80% of activities have GPS)
        // This is a rough estimate - actual count will be refined during sync
        let estimated_gps = (total_activities as f32 * 0.8) as u32;

        if let Some(p) = progress {
            p.set_oldest_activity_date(&result.to_string());
        } else {
            println!(" {} ({} activities)", result, total_activities);
        }
        Ok((result, total_activities, estimated_gps))
    }

    async fn has_health_data(&mut self, display_name: &str, date: NaiveDate) -> Result<bool> {
        let path = format!(
            "/usersummary-service/usersummary/daily/{}?calendarDate={}",
            display_name, date
        );

        self.rate_limiter.wait().await;
        let result: std::result::Result<serde_json::Value, _> =
            self.client.get_json(&self.token, &path).await;

        match result {
            Ok(data) => Ok(!data.as_object().map(|o| o.is_empty()).unwrap_or(true)),
            Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn find_first_health_date(
        &mut self,
        progress: Option<&SyncProgress>,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Option<NaiveDate>> {
        if let Some(p) = progress {
            p.set_planning_step(PlanningStep::FindingFirstHealth);
        }

        if from > to {
            return Ok(None);
        }

        let display_name = self.get_display_name().await?;
        let mut low = from;
        let mut high = to;
        let mut found = None;

        while low <= high {
            let span = (high - low).num_days();
            let mid = low + Duration::days(span / 2);

            if self.has_health_data(&display_name, mid).await? {
                found = Some(mid);
                if mid == low {
                    break;
                }
                high = mid - Duration::days(1);
            } else {
                low = mid + Duration::days(1);
            }
        }

        Ok(found)
    }

    async fn has_performance_data(&mut self, date: NaiveDate) -> Result<bool> {
        let readiness_path = format!("/metrics-service/metrics/trainingreadiness/{}", date);
        self.rate_limiter.wait().await;
        let readiness: std::result::Result<serde_json::Value, _> =
            self.client.get_json(&self.token, &readiness_path).await;

        if let Ok(data) = readiness {
            if data.as_array().and_then(|arr| arr.first()).is_some() {
                return Ok(true);
            }
        }

        let status_path = format!(
            "/metrics-service/metrics/trainingstatus/aggregated/{}",
            date
        );
        self.rate_limiter.wait().await;
        let status: std::result::Result<serde_json::Value, _> =
            self.client.get_json(&self.token, &status_path).await;

        match status {
            Ok(data) => Ok(data.get("mostRecentTrainingStatus").is_some()),
            Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn find_first_performance_date(
        &mut self,
        progress: Option<&SyncProgress>,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Option<NaiveDate>> {
        if let Some(p) = progress {
            p.set_planning_step(PlanningStep::FindingFirstPerformance);
        }

        if from > to {
            return Ok(None);
        }

        let mut low = from;
        let mut high = to;
        let mut found = None;

        while low <= high {
            let span = (high - low).num_days();
            let mid = low + Duration::days(span / 2);

            if self.has_performance_data(mid).await? {
                found = Some(mid);
                if mid == low {
                    break;
                }
                high = mid - Duration::days(1);
            } else {
                low = mid + Duration::days(1);
            }
        }

        Ok(found)
    }

    /// Run the sync process
    pub async fn run(&mut self, opts: SyncOptions) -> Result<SyncStats> {
        self.run_with_progress(&opts).await
    }

    /// Run sync with plain terminal progress reporting.
    async fn run_with_progress(&mut self, opts: &SyncOptions) -> Result<SyncStats> {
        let progress = Arc::new(SyncProgress::new());

        // Set storage path and sync mode for display
        progress.set_storage_path(&self.storage.base_path().display().to_string());
        progress.set_sync_mode(opts.mode);
        println!("Using storage: {}", progress.get_storage_path());
        println!("Planning sync...");

        // Planning phase - updates progress instead of printing
        progress.set_planning_step(PlanningStep::FetchingProfile);
        let display_name = self.get_display_name().await?;
        progress.set_profile(&display_name);
        self.queue.set_pipeline(pipeline_filter(opts.mode));

        // Recover any crashed tasks
        let _recovered = self.queue.recover_in_progress()?;

        // Plan phase
        if self.queue.pending_count()? == 0 {
            self.plan_sync_with_progress(opts, &progress).await?;
        }

        // Mark planning complete
        progress.finish_planning();

        // Count tasks by type for progress tracking
        self.count_tasks_for_progress(&progress)?;

        // Set date ranges based on sync mode
        let today = Utc::now().date_naive();
        let to_date = opts.to_date.unwrap_or(today);

        match opts.mode {
            progress::SyncMode::Latest => {
                // Latest mode: sync from last sync date (or 7 days ago) to today
                let from_date = opts.from_date.unwrap_or_else(|| today - Duration::days(7));
                progress.set_latest_range(&from_date.to_string(), &to_date.to_string());
                progress.set_date_range(&from_date.to_string(), &to_date.to_string());
            }
            progress::SyncMode::Backfill => {
                // Backfill mode: sync from oldest activity backwards
                let oldest = progress
                    .get_oldest_activity_date()
                    .unwrap_or_else(|| (today - Duration::days(365)).to_string());
                let from_date = opts
                    .from_date
                    .map(|d| d.to_string())
                    .unwrap_or(oldest.clone());
                progress.set_backfill_range(&from_date, &oldest);
                progress.set_date_range(&oldest, &from_date);
            }
        }

        print_sync_overview(&progress);
        println!("Planning complete.");

        let stats_result = if opts.dry_run {
            println!("Dry run mode - no changes will be made");
            Ok(SyncStats::default())
        } else {
            let reporter_progress = progress.clone();
            let reporter_handle = tokio::spawn(async move {
                run_progress_reporter(reporter_progress).await;
            });

            let result = self
                .run_with_progress_tracking(opts, progress.clone())
                .await;
            progress.request_shutdown();
            let _ = reporter_handle.await;
            result
        };

        let stats = stats_result?;
        print_sync_errors(&progress);

        // Update sync state after successful completion
        if !opts.dry_run && stats.completed > 0 {
            self.update_sync_state_after_completion(opts, today).await?;
        }

        if !opts.dry_run {
            println!("\nSync complete: {}", stats);
        }
        Ok(stats)
    }

    /// Update sync state after successful sync completion
    async fn update_sync_state_after_completion(
        &self,
        opts: &SyncOptions,
        today: NaiveDate,
    ) -> Result<()> {
        use crate::db::models::SyncState;

        match opts.mode {
            progress::SyncMode::Latest => {
                // Update last_sync_date to today
                if opts.sync_activities {
                    let state = SyncState {
                        profile_id: self.profile_id,
                        data_type: "activities".to_string(),
                        last_sync_date: Some(today),
                        last_activity_id: None,
                    };
                    self.storage.sync_db.update_sync_state(&state)?;
                }
                if opts.sync_health {
                    let state = SyncState {
                        profile_id: self.profile_id,
                        data_type: "health".to_string(),
                        last_sync_date: Some(today),
                        last_activity_id: None,
                    };
                    self.storage.sync_db.update_sync_state(&state)?;
                }
            }
            progress::SyncMode::Backfill => {
                // Check if backfill is complete (all tasks done)
                let pending = self.queue.pending_count()?;
                if pending == 0 {
                    // Mark backfill as complete
                    self.storage
                        .sync_db
                        .mark_backfill_complete(self.profile_id, "activities")?;
                    self.storage
                        .sync_db
                        .mark_backfill_complete(self.profile_id, "health")?;
                    self.storage
                        .sync_db
                        .mark_backfill_complete(self.profile_id, "performance")?;
                }
            }
        }

        Ok(())
    }

    /// Run sync with progress tracking using parallel producer/consumer pipeline
    async fn run_with_progress_tracking(
        &mut self,
        opts: &SyncOptions,
        progress: SharedProgress,
    ) -> Result<SyncStats> {
        // Use parallel execution with producer/consumer pipeline
        self.run_parallel(opts, progress).await
    }

    /// Run parallel sync with producer/consumer pipeline
    ///
    /// Producers: Fetch data from Garmin API (rate-limited)
    /// Consumers: Write data to Parquet (partition-locked)
    async fn run_parallel(
        &mut self,
        opts: &SyncOptions,
        progress: SharedProgress,
    ) -> Result<SyncStats> {
        let rate_limiter = SharedRateLimiter::new(opts.concurrency);
        let queue = SharedTaskQueue::new(TaskQueue::new(
            SyncDb::open(self.storage.base_path().join("sync.db"))?,
            self.profile_id,
            pipeline_filter(opts.mode),
        ));

        self.run_parallel_with_resources(
            opts,
            progress,
            queue,
            rate_limiter,
            pipeline_filter(opts.mode),
        )
        .await
    }

    pub(crate) async fn run_parallel_with_resources(
        &mut self,
        opts: &SyncOptions,
        progress: SharedProgress,
        queue: SharedTaskQueue,
        rate_limiter: SharedRateLimiter,
        pipeline_filter: Option<SyncPipeline>,
    ) -> Result<SyncStats> {
        // Bounded channel for backpressure (100 items)
        let (tx, rx) = mpsc::channel::<SyncData>(100);

        // Shared resources
        let parquet = Arc::new(self.storage.parquet.clone());
        let client = self.client.clone();
        let token = self.token.clone();
        let stats = Arc::new(TokioMutex::new(SyncStats::default()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let display_name = Arc::new(self.get_display_name().await?);
        let profile_id = self.profile_id;

        // Spawn producers (API fetchers)
        let mut producer_handles = Vec::new();
        for id in 0..opts.concurrency {
            let tx = tx.clone();
            let queue = queue.clone();
            let context = ProducerContext {
                rate_limiter: rate_limiter.clone(),
                client: client.clone(),
                token: token.clone(),
                progress: progress.clone(),
                display_name: Arc::clone(&display_name),
                profile_id,
                stats: Arc::clone(&stats),
                in_flight: Arc::clone(&in_flight),
                parquet: Arc::clone(&parquet),
                force: opts.force,
                pipeline_filter,
            };

            producer_handles.push(tokio::spawn(async move {
                producer_loop(id, queue, tx, context).await
            }));
        }
        drop(tx); // Close sender so consumers know when done

        // Spawn consumers (Parquet writers)
        let rx = Arc::new(TokioMutex::new(rx));
        let mut consumer_handles = Vec::new();
        for id in 0..opts.concurrency {
            let rx = Arc::clone(&rx);
            let parquet = Arc::clone(&parquet);
            let queue = queue.clone();
            let stats = Arc::clone(&stats);
            let progress = progress.clone();
            let in_flight = Arc::clone(&in_flight);

            consumer_handles.push(tokio::spawn(async move {
                consumer_loop(id, rx, parquet, queue, stats, progress, in_flight).await
            }));
        }

        // Wait for all producers to finish
        for h in producer_handles {
            if let Err(e) = h.await {
                eprintln!("Producer error: {}", e);
            }
        }

        // Wait for all consumers to finish
        for h in consumer_handles {
            if let Err(e) = h.await {
                eprintln!("Consumer error: {}", e);
            }
        }

        // Cleanup old completed tasks
        queue.cleanup(7).await?;

        // Update profile sync time
        self.storage
            .sync_db
            .update_profile_sync_time(self.profile_id)?;

        // Extract final stats
        let final_stats = stats.lock().await;
        Ok(SyncStats {
            recovered: final_stats.recovered,
            completed: final_stats.completed,
            rate_limited: final_stats.rate_limited,
            failed: final_stats.failed,
        })
    }

    /// Count pending tasks by type and update progress
    ///
    /// Sets initial totals based on actual tasks in queue.
    /// GPX totals are updated dynamically as activities are discovered.
    fn count_tasks_for_progress(&self, progress: &SyncProgress) -> Result<()> {
        // Count actual tasks by type from the queue
        let (_activities, _gpx, health, performance) = self.queue.count_by_type()?;

        // Activities and GPX totals are set during planning from API discovery
        // Only set them if planning didn't provide accurate counts
        if progress.activities.get_total() == 0 {
            progress.activities.set_total(1); // At least 1 for pagination
            progress.activities.set_dynamic(true);
        }
        if progress.gpx.get_total() == 0 {
            progress.gpx.set_dynamic(true); // Will be discovered during sync
        }

        // Health and performance totals come from date range calculation
        progress.health.set_total(health);
        progress.performance.set_total(performance);

        Ok(())
    }

    /// Plan sync tasks with progress tracking.
    async fn plan_sync_with_progress(
        &mut self,
        opts: &SyncOptions,
        progress: &SyncProgress,
    ) -> Result<()> {
        let today = Utc::now().date_naive();

        match opts.mode {
            progress::SyncMode::Latest => self.plan_latest_sync(opts, progress, today).await,
            progress::SyncMode::Backfill => self.plan_backfill_sync(opts, progress, today).await,
        }
    }

    /// Plan Latest sync: from last_sync_date to today
    pub(crate) async fn plan_latest_sync(
        &mut self,
        opts: &SyncOptions,
        progress: &SyncProgress,
        today: NaiveDate,
    ) -> Result<()> {
        // Get last sync date from DB, default to 7 days ago
        let last_sync = self
            .storage
            .sync_db
            .get_sync_state(self.profile_id, "activities")?;
        let from_date = opts.from_date.unwrap_or_else(|| {
            last_sync
                .and_then(|s| s.last_sync_date)
                .unwrap_or_else(|| today - Duration::days(7))
        });
        let to_date = opts.to_date.unwrap_or(today);

        progress.set_latest_range(&from_date.to_string(), &to_date.to_string());
        progress.set_oldest_activity_date(&from_date.to_string());

        let total_days = (to_date - from_date).num_days().max(0) as u32 + 1;

        // Plan activity sync (paginated from newest)
        if opts.sync_activities {
            progress.set_planning_step(PlanningStep::PlanningActivities);
            self.plan_activities_sync(SyncPipeline::Frontier, Some(from_date), Some(to_date))?;
        }

        // Plan health sync for date range
        if opts.sync_health {
            progress.set_planning_step(PlanningStep::PlanningHealth { days: total_days });
            self.plan_health_sync(from_date, to_date, opts.force, SyncPipeline::Frontier)?;
        }

        // Plan performance sync
        if opts.sync_performance {
            let total_weeks = (total_days / 7).max(1);
            progress.set_planning_step(PlanningStep::PlanningPerformance { weeks: total_weeks });
            self.plan_performance_sync(from_date, to_date, opts.force, SyncPipeline::Frontier)?;
        }

        Ok(())
    }

    /// Plan Backfill sync: from oldest_activity_date to backfill_frontier
    pub(crate) async fn plan_backfill_sync(
        &mut self,
        opts: &SyncOptions,
        progress: &SyncProgress,
        today: NaiveDate,
    ) -> Result<()> {
        // Find oldest activity date (this also gives us total count)
        let (oldest_date, total_activities, estimated_gps) =
            self.find_oldest_activity_date(Some(progress)).await?;

        // Get or initialize backfill frontier
        let backfill_state = self
            .storage
            .sync_db
            .get_backfill_state(self.profile_id, "activities")?;

        let (frontier_date, activities_complete) = match backfill_state {
            Some((frontier, _target, complete)) => (frontier, complete),
            None => {
                // Initialize frontier from last_sync_date or today
                let last_sync = self
                    .storage
                    .sync_db
                    .get_sync_state(self.profile_id, "activities")?;
                let frontier = last_sync.and_then(|s| s.last_sync_date).unwrap_or(today);

                // Initialize backfill state
                self.storage.sync_db.set_backfill_state(
                    self.profile_id,
                    "activities",
                    frontier,
                    oldest_date,
                    false,
                )?;
                (frontier, false)
            }
        };

        // Backfill from oldest_date to frontier_date
        let activity_from = opts.from_date.unwrap_or(oldest_date);
        let activity_to = opts.to_date.unwrap_or(frontier_date);

        progress.set_backfill_range(&frontier_date.to_string(), &oldest_date.to_string());
        progress.set_oldest_activity_date(&oldest_date.to_string());

        // Plan activity sync with known totals
        if opts.sync_activities && !activities_complete {
            progress.set_planning_step(PlanningStep::PlanningActivities);
            self.plan_activities_sync(
                SyncPipeline::Backfill,
                Some(activity_from),
                Some(activity_to),
            )?;
            if total_activities > 0 {
                progress.activities.set_total(total_activities);
                progress.gpx.set_total(estimated_gps);
            }
        }

        if opts.sync_health {
            let health_state = self
                .storage
                .sync_db
                .get_backfill_state(self.profile_id, "health")?;

            let health_target = match health_state {
                Some((_frontier, target, complete)) => (!complete).then_some(target),
                None => {
                    let search_from = activity_from;
                    let search_to = activity_to;
                    let first_health = self
                        .find_first_health_date(Some(progress), search_from, search_to)
                        .await?;
                    match first_health {
                        Some(first) => {
                            self.storage.sync_db.set_backfill_state(
                                self.profile_id,
                                "health",
                                frontier_date,
                                first,
                                false,
                            )?;
                            Some(first)
                        }
                        None => {
                            self.storage.sync_db.set_backfill_state(
                                self.profile_id,
                                "health",
                                frontier_date,
                                frontier_date,
                                true,
                            )?;
                            None
                        }
                    }
                }
            };

            if let Some(target) = health_target {
                let health_from = std::cmp::max(activity_from, target);
                let health_to = activity_to;
                if health_from <= health_to {
                    let total_days = (health_to - health_from).num_days().max(0) as u32 + 1;
                    progress.set_planning_step(PlanningStep::PlanningHealth { days: total_days });
                    self.plan_health_sync(
                        health_from,
                        health_to,
                        opts.force,
                        SyncPipeline::Backfill,
                    )?;
                }
            }
        }

        if opts.sync_performance {
            let perf_state = self
                .storage
                .sync_db
                .get_backfill_state(self.profile_id, "performance")?;

            let perf_target = match perf_state {
                Some((_frontier, target, complete)) => (!complete).then_some(target),
                None => {
                    let search_from = activity_from;
                    let search_to = activity_to;
                    let first_perf = self
                        .find_first_performance_date(Some(progress), search_from, search_to)
                        .await?;
                    match first_perf {
                        Some(first) => {
                            self.storage.sync_db.set_backfill_state(
                                self.profile_id,
                                "performance",
                                frontier_date,
                                first,
                                false,
                            )?;
                            Some(first)
                        }
                        None => {
                            self.storage.sync_db.set_backfill_state(
                                self.profile_id,
                                "performance",
                                frontier_date,
                                frontier_date,
                                true,
                            )?;
                            None
                        }
                    }
                }
            };

            if let Some(target) = perf_target {
                let perf_from = std::cmp::max(activity_from, target);
                let perf_to = activity_to;
                if perf_from <= perf_to {
                    let total_weeks = ((perf_to - perf_from).num_days().max(0) as u32 / 7).max(1);
                    progress.set_planning_step(PlanningStep::PlanningPerformance {
                        weeks: total_weeks,
                    });
                    self.plan_performance_sync(
                        perf_from,
                        perf_to,
                        opts.force,
                        SyncPipeline::Backfill,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Plan activity sync tasks
    fn plan_activities_sync(
        &self,
        pipeline: SyncPipeline,
        min_date: Option<NaiveDate>,
        max_date: Option<NaiveDate>,
    ) -> Result<()> {
        // Start with first page, we'll add more as we discover them
        let task = SyncTask::new(
            self.profile_id,
            pipeline,
            SyncTaskType::Activities {
                start: 0,
                limit: 50,
                min_date,
                max_date,
            },
        );
        self.queue.push(task)?;
        Ok(())
    }

    /// Plan health sync tasks for date range, returns count of tasks added
    fn plan_health_sync(
        &self,
        from: NaiveDate,
        to: NaiveDate,
        force: bool,
        pipeline: SyncPipeline,
    ) -> Result<u32> {
        let mut count = 0;
        let mut date = from;
        while date <= to {
            if force
                || !self
                    .storage
                    .parquet
                    .has_daily_health(self.profile_id, date)?
            {
                let task = SyncTask::new(
                    self.profile_id,
                    pipeline,
                    SyncTaskType::DailyHealth { date },
                );
                self.queue.push(task)?;
                count += 1;
            }
            date += Duration::days(1);
        }
        Ok(count)
    }

    /// Plan performance sync tasks, returns count of tasks added
    fn plan_performance_sync(
        &self,
        from: NaiveDate,
        to: NaiveDate,
        force: bool,
        pipeline: SyncPipeline,
    ) -> Result<u32> {
        // Performance metrics don't change daily, sync weekly
        let mut count = 0;
        let mut date = from;
        while date <= to {
            if force
                || !self
                    .storage
                    .parquet
                    .has_performance_metrics(self.profile_id, date)?
            {
                let task = SyncTask::new(
                    self.profile_id,
                    pipeline,
                    SyncTaskType::Performance { date },
                );
                self.queue.push(task)?;
                count += 1;
            }
            date += Duration::days(7);
        }
        Ok(count)
    }
}

fn print_sync_overview(progress: &SyncProgress) {
    println!("Profile: {}", progress.get_profile());
    println!("Mode: {}", progress.get_sync_mode());
    println!("Range: {}", progress.get_date_range());
    println!(
        "Queued tasks: activities {}, gpx {}, health {}, performance {}",
        progress.activities.get_total(),
        progress.gpx.get_total(),
        progress.health.get_total(),
        progress.performance.get_total()
    );
}

fn print_sync_errors(progress: &SyncProgress) {
    let errors = progress.get_errors();
    if errors.is_empty() {
        return;
    }

    println!("\nRecent errors:");
    for error in errors.iter().take(5) {
        println!("  [{}] {}: {}", error.stream, error.item, error.error);
    }

    if errors.len() > 5 {
        println!("  ... and {} more", errors.len() - 5);
    }
}

async fn run_progress_reporter(progress: SharedProgress) {
    loop {
        progress.print_simple_status();

        if progress.should_shutdown() || progress.is_complete() {
            println!();
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// Options for sync operation
#[derive(Clone)]
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
    /// Number of concurrent API requests (default: 4)
    pub concurrency: usize,
    /// Sync mode (Latest or Backfill)
    pub mode: progress::SyncMode,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            sync_activities: false,
            sync_health: false,
            sync_performance: false,
            from_date: None,
            to_date: None,
            dry_run: false,
            force: false,
            concurrency: 4,
            mode: progress::SyncMode::Latest,
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

/// Update progress when starting a task
fn update_progress_for_task(task: &SyncTask, progress: &SyncProgress) {
    let desc = match &task.task_type {
        SyncTaskType::Activities { start, limit, .. } => {
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
        SyncTaskType::Activities { .. } => {
            progress.activities.set_current_item(desc.clone());
            progress.activities.set_last_item(desc);
        }
        SyncTaskType::DownloadGpx { .. } => {
            progress.gpx.set_current_item(desc.clone());
            progress.gpx.set_last_item(desc);
        }
        SyncTaskType::DailyHealth { .. } => {
            progress.health.set_current_item(desc.clone());
            progress.health.set_last_item(desc);
        }
        SyncTaskType::Performance { .. } => {
            progress.performance.set_current_item(desc.clone());
            progress.performance.set_last_item(desc);
        }
        _ => {}
    }
}

/// Mark a task as completed in progress
#[allow(dead_code)]
fn complete_progress_for_task(task: &SyncTask, progress: &SyncProgress) {
    match &task.task_type {
        SyncTaskType::Activities { .. } => {
            progress.activities.complete_one();
            progress.activities.clear_current_item();
        }
        SyncTaskType::DownloadGpx { .. } => {
            progress.gpx.complete_one();
            progress.gpx.clear_current_item();
        }
        SyncTaskType::DailyHealth { .. } => {
            progress.health.complete_one();
            progress.health.clear_current_item();
        }
        SyncTaskType::Performance { .. } => {
            progress.performance.complete_one();
            progress.performance.clear_current_item();
        }
        _ => {}
    }
}

/// Mark a task as failed in progress and record error details
fn fail_progress_for_task(task: &SyncTask, progress: &SyncProgress, error: &str) {
    let (stream_name, item_desc) = match &task.task_type {
        SyncTaskType::Activities { start, limit, .. } => {
            progress.activities.fail_one();
            progress.activities.clear_current_item();
            ("Activities", format!("{}-{}", start, start + limit))
        }
        SyncTaskType::DownloadGpx {
            activity_id,
            activity_name,
            ..
        } => {
            progress.gpx.fail_one();
            progress.gpx.clear_current_item();
            (
                "GPX",
                activity_name
                    .clone()
                    .unwrap_or_else(|| activity_id.to_string()),
            )
        }
        SyncTaskType::DailyHealth { date } => {
            progress.health.fail_one();
            progress.health.clear_current_item();
            ("Health", date.to_string())
        }
        SyncTaskType::Performance { date } => {
            progress.performance.fail_one();
            progress.performance.clear_current_item();
            ("Performance", date.to_string())
        }
        _ => return,
    };

    progress.add_error(stream_name, item_desc, error.to_string());
}

const MAX_IDLE_RETRIES: u32 = 10;

fn should_exit_when_idle(idle_loops: u32, in_flight: usize) -> bool {
    idle_loops >= MAX_IDLE_RETRIES && in_flight == 0
}

fn record_write_failure(data: &SyncData, progress: &SyncProgress, error: &str) {
    match data {
        SyncData::Activities { records, .. } => {
            progress.activities.fail_one();
            progress.activities.clear_current_item();
            let item = records
                .first()
                .map(|r| r.activity_id.to_string())
                .unwrap_or_else(|| "batch".to_string());
            progress.add_error("Activities", item, error.to_string());
        }
        SyncData::Health { record, .. } => {
            progress.health.fail_one();
            progress.health.clear_current_item();
            progress.add_error("Health", record.date.to_string(), error.to_string());
        }
        SyncData::Performance { record, .. } => {
            progress.performance.fail_one();
            progress.performance.clear_current_item();
            progress.add_error("Performance", record.date.to_string(), error.to_string());
        }
        SyncData::TrackPoints {
            activity_id, date, ..
        } => {
            progress.gpx.fail_one();
            progress.gpx.clear_current_item();
            progress.add_error(
                "GPX",
                format!("{} ({})", date, activity_id),
                error.to_string(),
            );
        }
    }
}

// =============================================================================
// Producer/Consumer Pipeline
// =============================================================================

/// Producer loop: fetches data from Garmin API and sends to channel
async fn producer_loop(
    _id: usize,
    queue: SharedTaskQueue,
    tx: mpsc::Sender<SyncData>,
    context: ProducerContext,
) {
    let mut empty_count = 0;

    loop {
        // Pop next task
        let next_task = match context.pipeline_filter {
            Some(pipeline) => queue.pop_round_robin_with_pipeline(Some(pipeline)).await,
            None => queue.pop_round_robin().await,
        };

        let task = match next_task {
            Ok(Some(task)) => {
                empty_count = 0; // Reset counter on successful pop
                task
            }
            Ok(None) => {
                // Queue is empty, but consumers might be adding new tasks
                // Wait a bit and retry before giving up
                empty_count += 1;
                if should_exit_when_idle(empty_count, context.in_flight.load(Ordering::Relaxed)) {
                    break; // No more tasks after multiple retries
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => {
                eprintln!("Queue error: {}", e);
                break;
            }
        };

        let task_id = task.id.unwrap();
        context.in_flight.fetch_add(1, Ordering::Relaxed);

        // Mark in progress
        if let Err(e) = queue.mark_in_progress(task_id).await {
            eprintln!("Failed to mark task in progress: {}", e);
            continue;
        }

        // Update progress display
        update_progress_for_task(&task, &context.progress);

        // Skip tasks that already exist unless forcing
        if !context.force {
            let should_skip = match &task.task_type {
                SyncTaskType::DailyHealth { date } => context
                    .parquet
                    .has_daily_health(context.profile_id, *date)
                    .unwrap_or(false),
                SyncTaskType::Performance { date } => context
                    .parquet
                    .has_performance_metrics(context.profile_id, *date)
                    .unwrap_or(false),
                SyncTaskType::DownloadGpx {
                    activity_id,
                    activity_date,
                    ..
                } => activity_date
                    .as_ref()
                    .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                    .and_then(|date| context.parquet.has_track_points(*activity_id, date).ok())
                    .unwrap_or(false),
                _ => false,
            };

            if should_skip {
                if let Err(e) = queue.mark_completed(task_id).await {
                    eprintln!("Failed to mark task completed: {}", e);
                }
                complete_progress_for_task(&task, &context.progress);
                {
                    let mut s = context.stats.lock().await;
                    s.completed += 1;
                }
                context.in_flight.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
        }

        // Acquire rate limiter permit
        let _permit = context.rate_limiter.acquire().await;
        context.progress.record_request();

        // Fetch data based on task type
        let result = fetch_task_data(
            &task,
            &context.client,
            &context.token,
            &context.display_name,
            context.profile_id,
        )
        .await;

        match result {
            Ok(data) => {
                context.rate_limiter.on_success();
                // Send to consumer
                if tx.send(data).await.is_err() {
                    // Channel closed, consumer is done
                    context.in_flight.fetch_sub(1, Ordering::Relaxed);
                    let backoff = Duration::seconds(60);
                    let _ = queue
                        .mark_failed(task_id, "Consumer channel closed", backoff)
                        .await;
                    break;
                }
            }
            Err(GarminError::RateLimited) => {
                context.rate_limiter.on_rate_limit();
                let backoff = Duration::seconds(60);
                if let Err(e) = queue.mark_failed(task_id, "Rate limited", backoff).await {
                    eprintln!("Failed to mark task as rate limited: {}", e);
                }
                fail_progress_for_task(&task, &context.progress, "Rate limited");
                {
                    let mut s = context.stats.lock().await;
                    s.rate_limited += 1;
                }
                context.in_flight.fetch_sub(1, Ordering::Relaxed);
            }
            Err(e) => {
                let backoff = Duration::seconds(60);
                let error_msg = e.to_string();
                if let Err(e) = queue.mark_failed(task_id, &error_msg, backoff).await {
                    eprintln!("Failed to mark task as failed: {}", e);
                }
                fail_progress_for_task(&task, &context.progress, &error_msg);
                {
                    let mut s = context.stats.lock().await;
                    s.failed += 1;
                }
                context.in_flight.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

fn value_to_i32(value: &serde_json::Value) -> Option<i32> {
    if let Some(int) = value.as_i64() {
        return Some(int as i32);
    }
    value.as_f64().map(|float| float.round() as i32)
}

fn first_entry(value: &serde_json::Value) -> Option<&serde_json::Value> {
    if let Some(array) = value.as_array() {
        array.first()
    } else {
        Some(value)
    }
}

fn parse_sleep_metrics(value: Option<&serde_json::Value>) -> SleepMetrics {
    let dto = value.and_then(|v| v.get("dailySleepDTO")).or(value);

    let dto = match dto {
        Some(dto) => dto,
        None => return (None, None, None, None, None),
    };

    let deep = dto.get("deepSleepSeconds").and_then(value_to_i32);
    let light = dto.get("lightSleepSeconds").and_then(value_to_i32);
    let rem = dto.get("remSleepSeconds").and_then(value_to_i32);

    let total = dto
        .get("sleepTimeSeconds")
        .and_then(value_to_i32)
        .or_else(|| match (deep, light, rem) {
            (Some(d), Some(l), Some(r)) => Some(d + l + r),
            _ => None,
        });

    let score = dto
        .get("sleepScores")
        .and_then(|v| v.get("overall"))
        .and_then(|v| v.get("value"))
        .and_then(value_to_i32);

    (total, deep, light, rem, score)
}

fn parse_hrv_metrics(
    value: Option<&serde_json::Value>,
) -> (Option<i32>, Option<i32>, Option<String>) {
    let summary = value.and_then(|v| v.get("hrvSummary")).or(value);

    let summary = match summary {
        Some(summary) => summary,
        None => return (None, None, None),
    };

    let weekly_avg = summary.get("weeklyAvg").and_then(value_to_i32);
    let last_night = summary
        .get("lastNight")
        .or_else(|| summary.get("lastNightAvg"))
        .and_then(value_to_i32);
    let status = summary
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    (weekly_avg, last_night, status)
}

fn parse_vo2max_value(value: Option<&serde_json::Value>) -> Option<f64> {
    let entry = value.and_then(first_entry)?;
    entry
        .get("generic")
        .and_then(|v| v.get("vo2MaxValue"))
        .and_then(|v| v.as_f64())
}

fn parse_vo2max_fitness_age(value: Option<&serde_json::Value>) -> Option<i32> {
    let entry = value.and_then(first_entry)?;
    entry
        .get("generic")
        .and_then(|v| v.get("fitnessAge"))
        .and_then(value_to_i32)
}

fn parse_fitness_age(value: Option<&serde_json::Value>) -> Option<i32> {
    value
        .and_then(|v| v.get("fitnessAge"))
        .and_then(value_to_i32)
}

fn parse_race_predictions(
    value: Option<&serde_json::Value>,
) -> (Option<i32>, Option<i32>, Option<i32>, Option<i32>) {
    let entry = match value.and_then(first_entry) {
        Some(entry) => entry,
        None => return (None, None, None, None),
    };

    let race_5k = entry.get("time5K").and_then(value_to_i32);
    let race_10k = entry.get("time10K").and_then(value_to_i32);
    let race_half = entry.get("timeHalfMarathon").and_then(value_to_i32);
    let race_marathon = entry.get("timeMarathon").and_then(value_to_i32);

    (race_5k, race_10k, race_half, race_marathon)
}

fn parse_overall_score(value: Option<&serde_json::Value>) -> Option<i32> {
    let entry = value.and_then(first_entry)?;
    entry.get("overallScore").and_then(value_to_i32)
}

fn parse_training_status(value: Option<&serde_json::Value>, date: NaiveDate) -> Option<String> {
    let root = value?;
    let date_str = date.to_string();

    if let Some(latest) = root
        .get("mostRecentTrainingStatus")
        .and_then(|v| v.get("latestTrainingStatusData"))
        .and_then(|v| v.as_object())
    {
        for entry in latest.values() {
            if entry.get("calendarDate").and_then(|v| v.as_str()) == Some(date_str.as_str()) {
                return entry
                    .get("trainingStatusFeedbackPhrase")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
        }
    }

    if let Some(history) = root.get("trainingStatusHistory").and_then(|v| v.as_array()) {
        for entry in history {
            if entry.get("calendarDate").and_then(|v| v.as_str()) == Some(date_str.as_str()) {
                return entry
                    .get("trainingStatusFeedbackPhrase")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
        }
    }

    None
}

/// Fetch data for a task from the Garmin API
async fn fetch_task_data(
    task: &SyncTask,
    client: &GarminClient,
    token: &OAuth2Token,
    display_name: &str,
    profile_id: i32,
) -> Result<SyncData> {
    let task_id = task.id.unwrap();

    match &task.task_type {
        SyncTaskType::Activities {
            start,
            limit,
            min_date,
            max_date,
        } => {
            let path = format!(
                "/activitylist-service/activities/search/activities?limit={}&start={}",
                limit, start
            );
            let activities: Vec<serde_json::Value> = client.get_json(token, &path).await?;

            let mut records = Vec::new();
            let mut gpx_tasks = Vec::new();
            let mut reached_min = false;

            for activity in &activities {
                let activity_date = activity
                    .get("startTimeLocal")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.split(' ').next())
                    .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

                if let Some(date) = activity_date {
                    if let Some(min) = *min_date {
                        if date < min {
                            reached_min = true;
                            break;
                        }
                    }

                    if let Some(max) = *max_date {
                        if date > max {
                            continue;
                        }
                    }
                }

                // Parse activity
                let parsed = parse_activity(activity, profile_id)?;
                records.push(parsed);

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

                        gpx_tasks.push(SyncTask::new(
                            profile_id,
                            task.pipeline,
                            SyncTaskType::DownloadGpx {
                                activity_id: id,
                                activity_name,
                                activity_date,
                            },
                        ));
                    }
                }
            }

            // Check if there's a next page
            let next_page = if activities.len() == *limit as usize && !reached_min {
                Some(SyncTask::new(
                    profile_id,
                    task.pipeline,
                    SyncTaskType::Activities {
                        start: start + limit,
                        limit: *limit,
                        min_date: *min_date,
                        max_date: *max_date,
                    },
                ))
            } else {
                None
            };

            Ok(SyncData::Activities {
                records,
                gpx_tasks,
                next_page,
                task_id,
            })
        }

        SyncTaskType::DownloadGpx {
            activity_id,
            activity_date,
            ..
        } => {
            let path = format!("/download-service/export/gpx/activity/{}", activity_id);
            let gpx_bytes = client.download(token, &path).await?;
            let gpx_data = String::from_utf8_lossy(&gpx_bytes);

            // Parse activity date for partitioning
            let date = activity_date
                .as_ref()
                .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                .unwrap_or_else(|| Utc::now().date_naive());

            let points = parse_gpx(*activity_id, &gpx_data)?;

            Ok(SyncData::TrackPoints {
                activity_id: *activity_id,
                date,
                points,
                task_id,
            })
        }

        SyncTaskType::DailyHealth { date } => {
            let path = format!(
                "/usersummary-service/usersummary/daily/{}?calendarDate={}",
                display_name, date
            );

            // Try to fetch health data - may return 404/error for dates without data
            let health_result: std::result::Result<serde_json::Value, _> =
                client.get_json(token, &path).await;

            let health = match health_result {
                Ok(data) => data,
                Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => {
                    // No data for this date - store empty record to mark as synced
                    serde_json::json!({})
                }
                Err(e) => return Err(e),
            };

            let sleep_path = format!(
                "/wellness-service/wellness/dailySleepData/{}?date={}",
                display_name, date
            );
            let sleep_data: Option<serde_json::Value> =
                match client.get_json(token, &sleep_path).await {
                    Ok(data) => Some(data),
                    Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => None,
                    Err(e) => return Err(e),
                };

            let hrv_path = format!("/hrv-service/hrv/{}", date);
            let hrv_data: Option<serde_json::Value> = match client.get_json(token, &hrv_path).await
            {
                Ok(data) => Some(data),
                Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => None,
                Err(e) => return Err(e),
            };

            let (sleep_total, deep_sleep, light_sleep, rem_sleep, sleep_score) =
                parse_sleep_metrics(sleep_data.as_ref());
            let (hrv_weekly_avg, hrv_last_night, hrv_status) = parse_hrv_metrics(hrv_data.as_ref());

            let record = DailyHealth {
                id: None,
                profile_id,
                date: *date,
                steps: health
                    .get("totalSteps")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                step_goal: health
                    .get("dailyStepGoal")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                total_calories: health
                    .get("totalKilocalories")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                active_calories: health
                    .get("activeKilocalories")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                bmr_calories: health
                    .get("bmrKilocalories")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                resting_hr: health
                    .get("restingHeartRate")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                sleep_seconds: sleep_total.or_else(|| {
                    health
                        .get("sleepingSeconds")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32)
                }),
                deep_sleep_seconds: deep_sleep,
                light_sleep_seconds: light_sleep,
                rem_sleep_seconds: rem_sleep,
                sleep_score,
                avg_stress: health
                    .get("averageStressLevel")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                max_stress: health
                    .get("maxStressLevel")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                body_battery_start: health
                    .get("bodyBatteryChargedValue")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                body_battery_end: health
                    .get("bodyBatteryDrainedValue")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                hrv_weekly_avg,
                hrv_last_night,
                hrv_status,
                avg_respiration: health
                    .get("averageRespirationValue")
                    .and_then(|v| v.as_f64()),
                avg_spo2: health
                    .get("averageSpo2Value")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                lowest_spo2: health
                    .get("lowestSpo2Value")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                hydration_ml: health
                    .get("hydrationIntakeGoal")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                moderate_intensity_min: health
                    .get("moderateIntensityMinutes")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                vigorous_intensity_min: health
                    .get("vigorousIntensityMinutes")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                raw_json: Some(health),
            };

            Ok(SyncData::Health { record, task_id })
        }

        SyncTaskType::Performance { date } => {
            let vo2_path = format!("/metrics-service/metrics/maxmet/daily/{}/{}", date, date);
            let vo2max: Option<serde_json::Value> = match client.get_json(token, &vo2_path).await {
                Ok(data) => Some(data),
                Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => None,
                Err(e) => return Err(e),
            };

            let race_path = format!(
                "/metrics-service/metrics/racepredictions/daily/{}?fromCalendarDate={}&toCalendarDate={}",
                display_name, date, date
            );
            let race_predictions: Option<serde_json::Value> =
                match client.get_json(token, &race_path).await {
                    Ok(data) => Some(data),
                    Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => None,
                    Err(e) => return Err(e),
                };

            // Fetch training readiness
            let readiness_path = format!("/metrics-service/metrics/trainingreadiness/{}", date);
            let training_readiness: Option<serde_json::Value> =
                client.get_json(token, &readiness_path).await.ok();

            // Fetch training status
            let status_path = format!(
                "/metrics-service/metrics/trainingstatus/aggregated/{}",
                date
            );
            let training_status: Option<serde_json::Value> =
                client.get_json(token, &status_path).await.ok();

            let endurance_path = format!(
                "/metrics-service/metrics/endurancescore?calendarDate={}",
                date
            );
            let endurance_score_data: Option<serde_json::Value> =
                match client.get_json(token, &endurance_path).await {
                    Ok(data) => Some(data),
                    Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => None,
                    Err(e) => return Err(e),
                };

            let hill_path = format!("/metrics-service/metrics/hillscore?calendarDate={}", date);
            let hill_score_data: Option<serde_json::Value> =
                match client.get_json(token, &hill_path).await {
                    Ok(data) => Some(data),
                    Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => None,
                    Err(e) => return Err(e),
                };

            let fitness_age_path = format!("/fitnessage-service/fitnessage/{}", date);
            let fitness_age_data: Option<serde_json::Value> =
                match client.get_json(token, &fitness_age_path).await {
                    Ok(data) => Some(data),
                    Err(GarminError::NotFound(_)) | Err(GarminError::Api { .. }) => None,
                    Err(e) => return Err(e),
                };

            let vo2max_value = parse_vo2max_value(vo2max.as_ref());
            let fitness_age = parse_fitness_age(fitness_age_data.as_ref())
                .or_else(|| parse_vo2max_fitness_age(vo2max.as_ref()));

            let readiness_entry = training_readiness
                .as_ref()
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first());

            let readiness_score = readiness_entry
                .and_then(|e| e.get("score"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);

            let training_status_str = parse_training_status(training_status.as_ref(), *date);
            let (race_5k, race_10k, race_half, race_marathon) =
                parse_race_predictions(race_predictions.as_ref());
            let endurance_score = parse_overall_score(endurance_score_data.as_ref());
            let hill_score = parse_overall_score(hill_score_data.as_ref());

            let record = PerformanceMetrics {
                id: None,
                profile_id,
                date: *date,
                vo2max: vo2max_value,
                fitness_age,
                training_readiness: readiness_score,
                training_status: training_status_str,
                lactate_threshold_hr: None,
                lactate_threshold_pace: None,
                race_5k_sec: race_5k,
                race_10k_sec: race_10k,
                race_half_sec: race_half,
                race_marathon_sec: race_marathon,
                endurance_score,
                hill_score,
                raw_json: None,
            };

            Ok(SyncData::Performance { record, task_id })
        }

        _ => {
            // Other task types not implemented for parallel yet
            Err(GarminError::invalid_response(
                "Unsupported task type for parallel sync",
            ))
        }
    }
}

/// Parse activity JSON into Activity struct (standalone version for producer)
fn parse_activity(activity: &serde_json::Value, profile_id: i32) -> Result<Activity> {
    let activity_id = activity
        .get("activityId")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| GarminError::invalid_response("Missing activityId"))?;

    let start_time_local = activity
        .get("startTimeLocal")
        .and_then(|v| v.as_str())
        .and_then(parse_garmin_datetime);

    let start_time_gmt = activity
        .get("startTimeGMT")
        .and_then(|v| v.as_str())
        .and_then(parse_garmin_datetime);

    Ok(Activity {
        activity_id,
        profile_id,
        activity_name: activity
            .get("activityName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        activity_type: activity
            .get("activityType")
            .and_then(|v| v.get("typeKey"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        start_time_local,
        start_time_gmt,
        duration_sec: activity.get("duration").and_then(|v| v.as_f64()),
        distance_m: activity.get("distance").and_then(|v| v.as_f64()),
        calories: activity
            .get("calories")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        avg_hr: activity
            .get("averageHR")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        max_hr: activity
            .get("maxHR")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        avg_speed: activity.get("averageSpeed").and_then(|v| v.as_f64()),
        max_speed: activity.get("maxSpeed").and_then(|v| v.as_f64()),
        elevation_gain: activity.get("elevationGain").and_then(|v| v.as_f64()),
        elevation_loss: activity.get("elevationLoss").and_then(|v| v.as_f64()),
        avg_cadence: activity
            .get("averageRunningCadenceInStepsPerMinute")
            .and_then(|v| v.as_f64()),
        avg_power: activity
            .get("avgPower")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        normalized_power: activity
            .get("normPower")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        training_effect: activity
            .get("aerobicTrainingEffect")
            .and_then(|v| v.as_f64()),
        training_load: activity
            .get("activityTrainingLoad")
            .and_then(|v| v.as_f64()),
        start_lat: activity.get("startLatitude").and_then(|v| v.as_f64()),
        start_lon: activity.get("startLongitude").and_then(|v| v.as_f64()),
        end_lat: activity.get("endLatitude").and_then(|v| v.as_f64()),
        end_lon: activity.get("endLongitude").and_then(|v| v.as_f64()),
        ground_contact_time: activity
            .get("avgGroundContactTime")
            .and_then(|v| v.as_f64()),
        vertical_oscillation: activity
            .get("avgVerticalOscillation")
            .and_then(|v| v.as_f64()),
        stride_length: activity.get("avgStrideLength").and_then(|v| v.as_f64()),
        location_name: activity
            .get("locationName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        raw_json: Some(activity.clone()),
    })
}

fn parse_garmin_datetime(value: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.with_timezone(&Utc));
    }

    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .ok()?;

    Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

/// Parse GPX data and return track points
fn parse_gpx(activity_id: i64, gpx_data: &str) -> Result<Vec<TrackPoint>> {
    use gpx::read;
    use std::io::BufReader;

    let reader = BufReader::new(gpx_data.as_bytes());
    let gpx = read(reader).map_err(|e| GarminError::invalid_response(e.to_string()))?;

    let mut points = Vec::new();

    for track in gpx.tracks {
        for segment in track.segments {
            for point in segment.points {
                let timestamp = point
                    .time
                    .map(|t| {
                        DateTime::parse_from_rfc3339(&t.format().unwrap_or_default())
                            .map(|dt| dt.with_timezone(&Utc))
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                points.push(TrackPoint {
                    id: None,
                    activity_id,
                    timestamp,
                    lat: Some(point.point().y()),
                    lon: Some(point.point().x()),
                    elevation: point.elevation,
                    heart_rate: None,
                    cadence: None,
                    power: None,
                    speed: None,
                });
            }
        }
    }

    Ok(points)
}

/// Consumer loop: receives data from channel and writes to Parquet
async fn consumer_loop(
    _id: usize,
    rx: Arc<TokioMutex<mpsc::Receiver<SyncData>>>,
    parquet: Arc<ParquetStore>,
    queue: SharedTaskQueue,
    stats: Arc<TokioMutex<SyncStats>>,
    progress: SharedProgress,
    in_flight: Arc<AtomicUsize>,
) {
    loop {
        // Receive next data item
        let data = {
            let mut rx = rx.lock().await;
            rx.recv().await
        };

        let data = match data {
            Some(d) => d,
            None => break, // Channel closed, all producers done
        };

        // Process and write data
        let result = match &data {
            SyncData::Activities {
                records,
                gpx_tasks,
                next_page,
                task_id,
            } => {
                // Write activities to Parquet
                let write_result = parquet.upsert_activities_async(records).await;

                if write_result.is_ok() {
                    // Queue GPX tasks and update progress totals
                    let mut gpx_added = 0u32;
                    for gpx_task in gpx_tasks {
                        let should_skip = match &gpx_task.task_type {
                            SyncTaskType::DownloadGpx {
                                activity_id,
                                activity_date,
                                ..
                            } => activity_date
                                .as_ref()
                                .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                                .and_then(|date| parquet.has_track_points(*activity_id, date).ok())
                                .unwrap_or(false),
                            _ => false,
                        };

                        if should_skip {
                            continue;
                        }

                        if let Err(e) = queue.push(gpx_task.clone()).await {
                            eprintln!("Failed to queue GPX task: {}", e);
                        } else {
                            gpx_added += 1;
                        }
                    }
                    if gpx_added > 0 {
                        progress.gpx.add_total(gpx_added);
                    }

                    // Queue next page if there is one
                    if let Some(next) = next_page {
                        if let Err(e) = queue.push(next.clone()).await {
                            eprintln!("Failed to queue next page: {}", e);
                        }
                        progress.activities.add_total(1);
                    }
                }

                (write_result, *task_id, "Activities")
            }

            SyncData::Health { record, task_id } => {
                let result = parquet
                    .upsert_daily_health_async(std::slice::from_ref(record))
                    .await;
                (result, *task_id, "Health")
            }

            SyncData::Performance { record, task_id } => {
                let result = parquet
                    .upsert_performance_metrics_async(std::slice::from_ref(record))
                    .await;
                (result, *task_id, "Performance")
            }

            SyncData::TrackPoints {
                date,
                points,
                task_id,
                ..
            } => {
                let result = parquet.write_track_points_async(*date, points).await;
                (result, *task_id, "GPX")
            }
        };

        let (write_result, task_id, task_type) = result;

        match write_result {
            Ok(()) => {
                // Mark task completed
                if let Err(e) = queue.mark_completed(task_id).await {
                    eprintln!("Failed to mark task completed: {}", e);
                }

                // Update stats
                {
                    let mut s = stats.lock().await;
                    s.completed += 1;
                }

                // Update progress based on task type
                match task_type {
                    "Activities" => {
                        progress.activities.complete_one();
                        progress.activities.clear_current_item();
                    }
                    "Health" => {
                        progress.health.complete_one();
                        progress.health.clear_current_item();
                    }
                    "Performance" => {
                        progress.performance.complete_one();
                        progress.performance.clear_current_item();
                    }
                    "GPX" => {
                        progress.gpx.complete_one();
                        progress.gpx.clear_current_item();
                    }
                    _ => {}
                }
                in_flight.fetch_sub(1, Ordering::Relaxed);
            }
            Err(e) => {
                // Mark task failed
                let backoff = Duration::seconds(60);
                let error_msg = e.to_string();
                if let Err(e) = queue.mark_failed(task_id, &error_msg, backoff).await {
                    eprintln!("Failed to mark task as failed: {}", e);
                }

                // Update stats
                {
                    let mut s = stats.lock().await;
                    s.failed += 1;
                }

                record_write_failure(&data, &progress, &error_msg);
                eprintln!("Write error for {}: {}", task_type, error_msg);
                in_flight.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::TaskStatus;
    use chrono::NaiveDate;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_should_exit_when_idle_requires_no_inflight() {
        assert!(!should_exit_when_idle(MAX_IDLE_RETRIES, 1));
        assert!(should_exit_when_idle(MAX_IDLE_RETRIES, 0));
        assert!(!should_exit_when_idle(MAX_IDLE_RETRIES - 1, 0));
    }

    #[test]
    fn test_record_write_failure_updates_progress() {
        let progress = SyncProgress::new();
        let record = DailyHealth {
            id: None,
            profile_id: 1,
            date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            steps: None,
            step_goal: None,
            total_calories: None,
            active_calories: None,
            bmr_calories: None,
            resting_hr: None,
            sleep_seconds: None,
            deep_sleep_seconds: None,
            light_sleep_seconds: None,
            rem_sleep_seconds: None,
            sleep_score: None,
            avg_stress: None,
            max_stress: None,
            body_battery_start: None,
            body_battery_end: None,
            hrv_weekly_avg: None,
            hrv_last_night: None,
            hrv_status: None,
            avg_respiration: None,
            avg_spo2: None,
            lowest_spo2: None,
            hydration_ml: None,
            moderate_intensity_min: None,
            vigorous_intensity_min: None,
            raw_json: None,
        };

        let data = SyncData::Health { record, task_id: 1 };
        record_write_failure(&data, &progress, "write failed");

        assert_eq!(progress.health.get_failed(), 1);
        let errors = progress.get_errors();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].stream, "Health");
    }

    fn test_token() -> OAuth2Token {
        OAuth2Token {
            scope: "test".to_string(),
            jti: "jti".to_string(),
            token_type: "Bearer".to_string(),
            access_token: "access".to_string(),
            refresh_token: "refresh".to_string(),
            expires_in: 3600,
            expires_at: Utc::now().timestamp() + 3600,
            refresh_token_expires_in: 86400,
            refresh_token_expires_at: Utc::now().timestamp() + 86400,
            client_id: None,
        }
    }

    #[tokio::test]
    async fn test_activity_pagination_respects_date_bounds() {
        let server = MockServer::start().await;

        let body = serde_json::json!([
            {
                "activityId": 1,
                "activityName": "Newest",
                "startTimeLocal": "2025-01-05 08:00:00",
                "startTimeGMT": "2025-01-05 07:00:00",
                "activityType": { "typeKey": "running" },
                "hasPolyline": false
            },
            {
                "activityId": 2,
                "activityName": "Mid",
                "startTimeLocal": "2025-01-04 08:00:00",
                "startTimeGMT": "2025-01-04 07:00:00",
                "activityType": { "typeKey": "running" },
                "hasPolyline": false
            },
            {
                "activityId": 3,
                "activityName": "Old",
                "startTimeLocal": "2025-01-03 08:00:00",
                "startTimeGMT": "2025-01-03 07:00:00",
                "activityType": { "typeKey": "running" },
                "hasPolyline": false
            }
        ]);

        Mock::given(method("GET"))
            .and(path("/activitylist-service/activities/search/activities"))
            .and(query_param("limit", "50"))
            .and(query_param("start", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let client = GarminClient::new_with_base_url(&server.uri());
        let task = SyncTask {
            id: Some(1),
            profile_id: 1,
            task_type: SyncTaskType::Activities {
                start: 0,
                limit: 50,
                min_date: Some(NaiveDate::from_ymd_opt(2025, 1, 4).unwrap()),
                max_date: Some(NaiveDate::from_ymd_opt(2025, 1, 5).unwrap()),
            },
            pipeline: SyncPipeline::Frontier,
            status: TaskStatus::Pending,
            attempts: 0,
            last_error: None,
            created_at: None,
            next_retry_at: None,
            completed_at: None,
        };

        let data = fetch_task_data(&task, &client, &test_token(), "TestUser", 1)
            .await
            .unwrap();

        match data {
            SyncData::Activities {
                records,
                gpx_tasks,
                next_page,
                ..
            } => {
                assert_eq!(records.len(), 2);
                assert!(gpx_tasks.is_empty());
                assert!(next_page.is_none());
            }
            _ => panic!("unexpected data type"),
        }
    }

    #[tokio::test]
    async fn test_daily_health_includes_sleep_and_hrv() {
        let server = MockServer::start().await;
        let date = NaiveDate::from_ymd_opt(2025, 12, 4).unwrap();

        let health_body = json!({
            "totalSteps": 1234,
            "sleepingSeconds": 1000,
            "averageStressLevel": 20
        });

        Mock::given(method("GET"))
            .and(path("/usersummary-service/usersummary/daily/TestUser"))
            .and(query_param("calendarDate", "2025-12-04"))
            .respond_with(ResponseTemplate::new(200).set_body_json(health_body))
            .mount(&server)
            .await;

        let sleep_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/sleep_2025-12-04.json"))
                .unwrap();

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/dailySleepData/TestUser"))
            .and(query_param("date", "2025-12-04"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sleep_fixture))
            .mount(&server)
            .await;

        let hrv_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/hrv.json")).unwrap();

        Mock::given(method("GET"))
            .and(path("/hrv-service/hrv/2025-12-04"))
            .respond_with(ResponseTemplate::new(200).set_body_json(hrv_fixture))
            .mount(&server)
            .await;

        let client = GarminClient::new_with_base_url(&server.uri());
        let mut task = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth { date },
        );
        task.id = Some(1);

        let data = fetch_task_data(&task, &client, &test_token(), "TestUser", 1)
            .await
            .unwrap();

        match data {
            SyncData::Health { record, .. } => {
                assert_eq!(record.sleep_seconds, Some(31920));
                assert_eq!(record.deep_sleep_seconds, Some(8100));
                assert_eq!(record.light_sleep_seconds, Some(15300));
                assert_eq!(record.rem_sleep_seconds, Some(8520));
                assert_eq!(record.sleep_score, Some(88));
                assert_eq!(record.hrv_weekly_avg, Some(65));
                assert_eq!(record.hrv_last_night, Some(68));
                assert_eq!(record.hrv_status.as_deref(), Some("BALANCED"));
            }
            _ => panic!("unexpected data type"),
        }
    }

    #[tokio::test]
    async fn test_daily_health_handles_missing_sleep_and_hrv() {
        let server = MockServer::start().await;
        let date = NaiveDate::from_ymd_opt(2025, 12, 5).unwrap();

        let health_body = json!({
            "totalSteps": 4321,
            "sleepingSeconds": 7200
        });

        Mock::given(method("GET"))
            .and(path("/usersummary-service/usersummary/daily/TestUser"))
            .and(query_param("calendarDate", "2025-12-05"))
            .respond_with(ResponseTemplate::new(200).set_body_json(health_body))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/wellness-service/wellness/dailySleepData/TestUser"))
            .and(query_param("date", "2025-12-05"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/hrv-service/hrv/2025-12-05"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = GarminClient::new_with_base_url(&server.uri());
        let mut task = SyncTask::new(
            1,
            SyncPipeline::Frontier,
            SyncTaskType::DailyHealth { date },
        );
        task.id = Some(1);

        let data = fetch_task_data(&task, &client, &test_token(), "TestUser", 1)
            .await
            .unwrap();

        match data {
            SyncData::Health { record, .. } => {
                assert_eq!(record.sleep_seconds, Some(7200));
                assert_eq!(record.deep_sleep_seconds, None);
                assert_eq!(record.hrv_weekly_avg, None);
                assert_eq!(record.hrv_status, None);
            }
            _ => panic!("unexpected data type"),
        }
    }

    #[test]
    fn test_parse_hrv_metrics_reads_last_night_avg() {
        let payload = json!({
            "hrvSummary": {
                "weeklyAvg": 60,
                "lastNightAvg": 58,
                "status": "BALANCED"
            }
        });

        let (weekly_avg, last_night, status) = parse_hrv_metrics(Some(&payload));
        assert_eq!(weekly_avg, Some(60));
        assert_eq!(last_night, Some(58));
        assert_eq!(status.as_deref(), Some("BALANCED"));
    }

    #[tokio::test]
    async fn test_performance_uses_date_scoped_endpoints() {
        let server = MockServer::start().await;
        let date = NaiveDate::from_ymd_opt(2025, 12, 10).unwrap();

        let vo2_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/vo2max.json")).unwrap();
        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/maxmet/daily/2025-12-10/2025-12-10",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(vo2_fixture))
            .mount(&server)
            .await;

        let race_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/race_predictions.json"))
                .unwrap();
        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/racepredictions/daily/TestUser",
            ))
            .and(query_param("fromCalendarDate", "2025-12-10"))
            .and(query_param("toCalendarDate", "2025-12-10"))
            .respond_with(ResponseTemplate::new(200).set_body_json(race_fixture))
            .mount(&server)
            .await;

        let readiness_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/training_readiness.json"))
                .unwrap();
        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/trainingreadiness/2025-12-10",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(readiness_fixture))
            .mount(&server)
            .await;

        let training_status_fixture = json!({
            "mostRecentTrainingStatus": {
                "latestTrainingStatusData": {
                    "123": {
                        "calendarDate": "2025-12-10",
                        "trainingStatusFeedbackPhrase": "PRODUCTIVE"
                    }
                }
            }
        });
        Mock::given(method("GET"))
            .and(path(
                "/metrics-service/metrics/trainingstatus/aggregated/2025-12-10",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(training_status_fixture))
            .mount(&server)
            .await;

        let endurance_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/endurance_score.json"))
                .unwrap();
        Mock::given(method("GET"))
            .and(path("/metrics-service/metrics/endurancescore"))
            .and(query_param("calendarDate", "2025-12-10"))
            .respond_with(ResponseTemplate::new(200).set_body_json(endurance_fixture))
            .mount(&server)
            .await;

        let hill_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/hill_score.json")).unwrap();
        Mock::given(method("GET"))
            .and(path("/metrics-service/metrics/hillscore"))
            .and(query_param("calendarDate", "2025-12-10"))
            .respond_with(ResponseTemplate::new(200).set_body_json(hill_fixture))
            .mount(&server)
            .await;

        let fitness_age_fixture = json!({
            "calendarDate": "2025-12-10",
            "fitnessAge": 37.0
        });
        Mock::given(method("GET"))
            .and(path("/fitnessage-service/fitnessage/2025-12-10"))
            .respond_with(ResponseTemplate::new(200).set_body_json(fitness_age_fixture))
            .mount(&server)
            .await;

        let client = GarminClient::new_with_base_url(&server.uri());
        let mut task = SyncTask::new(
            1,
            SyncPipeline::Backfill,
            SyncTaskType::Performance { date },
        );
        task.id = Some(1);

        let data = fetch_task_data(&task, &client, &test_token(), "TestUser", 1)
            .await
            .unwrap();

        match data {
            SyncData::Performance { record, .. } => {
                assert_eq!(record.vo2max, Some(53.0));
                assert_eq!(record.fitness_age, Some(37));
                assert_eq!(record.training_readiness, Some(69));
                assert_eq!(record.training_status.as_deref(), Some("PRODUCTIVE"));
                assert_eq!(record.race_5k_sec, Some(1245));
                assert_eq!(record.race_10k_sec, Some(2610));
                assert_eq!(record.race_half_sec, Some(5850));
                assert_eq!(record.race_marathon_sec, Some(12420));
                assert_eq!(record.endurance_score, Some(72));
                assert_eq!(record.hill_score, Some(58));
            }
            _ => panic!("unexpected data type"),
        }
    }

    #[test]
    fn test_parse_garmin_datetime_accepts_naive_strings() {
        let dt = parse_garmin_datetime("2025-01-03 07:00:00");
        assert!(dt.is_some());

        let dt = parse_garmin_datetime("2025-01-03 07:00:00.123");
        assert!(dt.is_some());
    }
}
