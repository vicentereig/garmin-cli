use std::time::Duration;

use chrono::Utc;
use tokio::time::MissedTickBehavior;

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::error::{GarminError, Result};
use crate::db::models::SyncPipeline;
use crate::storage::{default_storage_path, Storage, SyncDb};
use crate::sync::progress::SyncMode;
use crate::sync::{
    compute_backfill_window, SharedRateLimiter, SharedTaskQueue, SyncEngine, SyncOptions,
    SyncProgress, TaskQueue,
};

use super::auth::refresh_token;

pub fn parse_interval(input: &str) -> Result<Duration> {
    let input = input.trim();
    if input.is_empty() {
        return Err(GarminError::invalid_param("Interval is required"));
    }

    let (number_part, unit) = input.split_at(input.len() - 1);
    let value: u64 = number_part.parse().map_err(|_| {
        GarminError::invalid_param("Interval must be a number followed by s, m, or h")
    })?;

    if value == 0 {
        return Err(GarminError::invalid_param("Interval must be greater than 0"));
    }

    let seconds = match unit {
        "s" => value,
        "m" => value * 60,
        "h" => value * 60 * 60,
        _ => {
            return Err(GarminError::invalid_param(
                "Interval must end with s, m, or h",
            ))
        }
    };

    Ok(Duration::from_secs(seconds))
}

pub async fn watch(
    profile: Option<String>,
    db_path: Option<String>,
    interval: String,
    activities: bool,
    health: bool,
    performance: bool,
    backfill_window_days: u32,
    force: bool,
) -> Result<()> {
    let interval = parse_interval(&interval)?;
    let store = CredentialStore::new(profile.clone())?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    let storage_path = db_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    let storage = Storage::open(storage_path)?;
    let backfill_storage = storage.clone_with_new_db()?;

    let client = GarminClient::new(&oauth1.domain);
    let mut frontier_engine = SyncEngine::with_storage(storage, client.clone(), oauth2.clone())?;
    let mut backfill_engine = SyncEngine::with_storage(backfill_storage, client, oauth2)?;

    frontier_engine.ensure_display_name().await?;
    backfill_engine.ensure_display_name().await?;

    frontier_engine.set_queue_pipeline(Some(SyncPipeline::Frontier));
    backfill_engine.set_queue_pipeline(Some(SyncPipeline::Backfill));

    let _ = frontier_engine.recover_in_progress();
    let _ = backfill_engine.recover_in_progress();

    let sync_all = !activities && !health && !performance;
    let base_opts = SyncOptions {
        sync_activities: activities || sync_all,
        sync_health: health || sync_all,
        sync_performance: performance || sync_all,
        dry_run: false,
        force,
        fancy_ui: false,
        concurrency: 4,
        ..Default::default()
    };

    let frontier_opts = SyncOptions {
        mode: SyncMode::Latest,
        ..base_opts.clone()
    };
    let backfill_opts = SyncOptions {
        mode: SyncMode::Backfill,
        ..base_opts
    };

    let rate_limiter = SharedRateLimiter::new(frontier_opts.concurrency);

    let frontier_queue = SharedTaskQueue::new(TaskQueue::new(
        SyncDb::open(frontier_engine.storage_base_path().join("sync.db"))?,
        frontier_engine.profile_id(),
        Some(SyncPipeline::Frontier),
    ));

    let backfill_queue = SharedTaskQueue::new(TaskQueue::new(
        SyncDb::open(backfill_engine.storage_base_path().join("sync.db"))?,
        backfill_engine.profile_id(),
        Some(SyncPipeline::Backfill),
    ));

    let frontier_handle = tokio::spawn(run_frontier_loop(
        frontier_engine,
        frontier_opts,
        frontier_queue,
        rate_limiter.clone(),
        interval,
    ));

    let backfill_handle = tokio::spawn(run_backfill_loop(
        backfill_engine,
        backfill_opts,
        backfill_queue,
        rate_limiter,
        backfill_window_days,
    ));

    let (frontier_result, backfill_result) = tokio::join!(frontier_handle, backfill_handle);

    frontier_result.map_err(|e| GarminError::Other(e.to_string()))??;
    backfill_result.map_err(|e| GarminError::Other(e.to_string()))??;

    Ok(())
}

async fn run_frontier_loop(
    mut engine: SyncEngine,
    opts: SyncOptions,
    queue: SharedTaskQueue,
    rate_limiter: SharedRateLimiter,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        let _ = frontier_iteration(&mut engine, &opts, &queue, &rate_limiter).await?;
    }
}

async fn run_backfill_loop(
    mut engine: SyncEngine,
    opts: SyncOptions,
    queue: SharedTaskQueue,
    rate_limiter: SharedRateLimiter,
    backfill_window_days: u32,
) -> Result<()> {
    let sync_db_path = engine.storage_base_path().join("sync.db");
    let profile_id = engine.profile_id();

    loop {
        let did_work = backfill_iteration(
            &mut engine,
            &opts,
            &queue,
            &rate_limiter,
            &sync_db_path,
            profile_id,
            backfill_window_days,
        )
        .await?;

        if !did_work {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

async fn frontier_iteration(
    engine: &mut SyncEngine,
    opts: &SyncOptions,
    queue: &SharedTaskQueue,
    rate_limiter: &SharedRateLimiter,
) -> Result<bool> {
    let mut did_work = false;

    if engine.pending_count()? == 0 {
        let progress = SyncProgress::new();
        let today = Utc::now().date_naive();
        engine.plan_latest_sync(opts, &progress, today).await?;
        did_work = true;
    }

    if engine.pending_count()? > 0 {
        let progress = std::sync::Arc::new(SyncProgress::new());
        engine
            .run_parallel_with_resources(
                opts,
                progress,
                queue.clone(),
                rate_limiter.clone(),
                Some(SyncPipeline::Frontier),
            )
            .await?;
        did_work = true;
    }

    Ok(did_work)
}

async fn backfill_iteration(
    engine: &mut SyncEngine,
    opts: &SyncOptions,
    queue: &SharedTaskQueue,
    rate_limiter: &SharedRateLimiter,
    sync_db_path: &std::path::Path,
    profile_id: i32,
    backfill_window_days: u32,
) -> Result<bool> {
    let mut did_work = false;

    if engine.pending_count()? == 0 {
        let existing_state = SyncDb::open(sync_db_path)?
            .get_backfill_state(profile_id, "activities")?;

        let (frontier_date, target_date, is_complete) = match existing_state {
            Some((frontier, target, complete)) => (frontier, target, complete),
            None => {
                let today = Utc::now().date_naive();
                let last_sync = SyncDb::open(sync_db_path)?
                    .get_sync_state(profile_id, "activities")?;
                let frontier = last_sync
                    .and_then(|s| s.last_sync_date)
                    .unwrap_or(today);

                let (oldest_date, _total, _gps) = engine.find_oldest_activity_date(None).await?;

                SyncDb::open(sync_db_path)?
                    .set_backfill_state(profile_id, "activities", frontier, oldest_date, false)?;

                (frontier, oldest_date, false)
            }
        };

        if is_complete {
            return Ok(false);
        }

        let window = compute_backfill_window(
            frontier_date,
            target_date,
            backfill_window_days.max(1),
        );

        let (from, to, next_frontier, complete) = match window {
            Some(window) => window,
            None => {
                SyncDb::open(sync_db_path)?.mark_backfill_complete(profile_id, "activities")?;
                return Ok(false);
            }
        };

        let mut window_opts = opts.clone();
        window_opts.from_date = Some(from);
        window_opts.to_date = Some(to);

        let progress = SyncProgress::new();
        let today = Utc::now().date_naive();
        engine.plan_backfill_sync(&window_opts, &progress, today).await?;

        SyncDb::open(sync_db_path)?
            .update_backfill_frontier(profile_id, "activities", next_frontier)?;
        if complete {
            SyncDb::open(sync_db_path)?.mark_backfill_complete(profile_id, "activities")?;
        }

        did_work = true;
    }

    if engine.pending_count()? > 0 {
        let progress = std::sync::Arc::new(SyncProgress::new());
        engine
            .run_parallel_with_resources(
                opts,
                progress,
                queue.clone(),
                rate_limiter.clone(),
                Some(SyncPipeline::Backfill),
            )
            .await?;
        did_work = true;
    }

    Ok(did_work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::OAuth2Token;
    use chrono::NaiveDate;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_parse_interval_seconds() {
        let dur = parse_interval("30s").unwrap();
        assert_eq!(dur, Duration::from_secs(30));
    }

    #[test]
    fn test_parse_interval_minutes() {
        let dur = parse_interval("5m").unwrap();
        assert_eq!(dur, Duration::from_secs(300));
    }

    #[test]
    fn test_parse_interval_hours() {
        let dur = parse_interval("1h").unwrap();
        assert_eq!(dur, Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_interval_invalid() {
        assert!(parse_interval("").is_err());
        assert!(parse_interval("0m").is_err());
        assert!(parse_interval("10x").is_err());
        assert!(parse_interval("abc").is_err());
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
        }
    }

    async fn setup_profile_mock(server: &MockServer) {
        let body = serde_json::json!({ "displayName": "TestUser" });
        Mock::given(method("GET"))
            .and(path("/userprofile-service/socialProfile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    async fn setup_activity_list_mock(server: &MockServer, limit: u32, start: u32, date: &str) {
        let body = serde_json::json!([
            {
                "activityId": 1001,
                "activityName": "Test Activity",
                "startTimeLocal": format!("{} 07:00:00", date),
                "startTimeGMT": format!("{} 06:00:00", date),
                "activityType": { "typeKey": "running" },
                "hasPolyline": false
            }
        ]);

        Mock::given(method("GET"))
            .and(path("/activitylist-service/activities/search/activities"))
            .and(query_param("limit", limit.to_string()))
            .and(query_param("start", start.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn test_frontier_iteration_leaves_backfill_tasks() {
        let server = MockServer::start().await;
        setup_profile_mock(&server).await;
        setup_activity_list_mock(&server, 50, 0, "2025-01-03").await;

        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::open(temp_dir.path().to_path_buf()).unwrap();
        let client = GarminClient::new_with_base_url(&server.uri());
        let mut engine = SyncEngine::with_storage(storage, client, test_token()).unwrap();

        engine.ensure_display_name().await.unwrap();
        engine.set_queue_pipeline(Some(SyncPipeline::Frontier));

        let sync_db_path = engine.storage_base_path().join("sync.db");
        let sync_db = SyncDb::open(&sync_db_path).unwrap();
        let profile_id = engine.profile_id();

        let backfill_task = crate::db::models::SyncTask::new(
            profile_id,
            SyncPipeline::Backfill,
            crate::db::models::SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            },
        );
        sync_db.push_task(&backfill_task).unwrap();

        let queue = SharedTaskQueue::new(TaskQueue::new(
            SyncDb::open(engine.storage_base_path().join("sync.db")).unwrap(),
            profile_id,
            Some(SyncPipeline::Frontier),
        ));

        let opts = SyncOptions {
            sync_activities: true,
            sync_health: false,
            sync_performance: false,
            fancy_ui: false,
            concurrency: 1,
            mode: SyncMode::Latest,
            from_date: Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
            to_date: Some(NaiveDate::from_ymd_opt(2025, 1, 5).unwrap()),
            ..Default::default()
        };

        let rate_limiter = SharedRateLimiter::new(1);
        frontier_iteration(&mut engine, &opts, &queue, &rate_limiter)
            .await
            .unwrap();

        let backfill_pending = sync_db
            .count_pending_tasks(profile_id, Some(SyncPipeline::Backfill))
            .unwrap();
        assert_eq!(backfill_pending, 1);

        let activities_dir = temp_dir.path().join("activities");
        let file_count = std::fs::read_dir(&activities_dir)
            .map(|rd| {
                rd.filter(|e| {
                    e.as_ref()
                        .ok()
                        .map(|e| e.path().extension().is_some())
                        .unwrap_or(false)
                })
                .count()
            })
            .unwrap_or(0);
        assert!(file_count > 0);
    }

    #[tokio::test]
    async fn test_backfill_iteration_updates_frontier() {
        let server = MockServer::start().await;
        setup_profile_mock(&server).await;

        // find_oldest_activity_date probing uses limit=1; return empty for all
        Mock::given(method("GET"))
            .and(path("/activitylist-service/activities/search/activities"))
            .and(query_param("limit", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(Vec::<serde_json::Value>::new()))
            .mount(&server)
            .await;

        setup_activity_list_mock(&server, 100, 0, "2025-01-01").await;
        setup_activity_list_mock(&server, 50, 0, "2025-01-10").await;

        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::open(temp_dir.path().to_path_buf()).unwrap();
        let client = GarminClient::new_with_base_url(&server.uri());
        let mut engine = SyncEngine::with_storage(storage, client, test_token()).unwrap();

        engine.ensure_display_name().await.unwrap();
        engine.set_queue_pipeline(Some(SyncPipeline::Backfill));

        let sync_db_path = engine.storage_base_path().join("sync.db");
        let sync_db = SyncDb::open(&sync_db_path).unwrap();
        let profile_id = engine.profile_id();

        sync_db
            .set_backfill_state(
                profile_id,
                "activities",
                NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
                NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                false,
            )
            .unwrap();

        let queue = SharedTaskQueue::new(TaskQueue::new(
            SyncDb::open(&sync_db_path).unwrap(),
            profile_id,
            Some(SyncPipeline::Backfill),
        ));

        let opts = SyncOptions {
            sync_activities: true,
            sync_health: false,
            sync_performance: false,
            fancy_ui: false,
            concurrency: 1,
            mode: SyncMode::Backfill,
            ..Default::default()
        };

        let rate_limiter = SharedRateLimiter::new(1);
        backfill_iteration(
            &mut engine,
            &opts,
            &queue,
            &rate_limiter,
            &sync_db_path,
            profile_id,
            3,
        )
        .await
        .unwrap();

        let state = sync_db
            .get_backfill_state(profile_id, "activities")
            .unwrap()
            .unwrap();
        assert_eq!(state.0, NaiveDate::from_ymd_opt(2025, 1, 7).unwrap());

        let activities_dir = temp_dir.path().join("activities");
        let file_count = std::fs::read_dir(&activities_dir)
            .map(|rd| {
                rd.filter(|e| {
                    e.as_ref()
                        .ok()
                        .map(|e| e.path().extension().is_some())
                        .unwrap_or(false)
                })
                .count()
            })
            .unwrap_or(0);
        assert!(file_count > 0);
    }
}
