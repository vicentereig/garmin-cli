//! Sync commands for garmin-cli

use chrono::{NaiveDate, Utc};
use std::path::{Path, PathBuf};

use crate::client::{GarminClient, RequestLogger};
use crate::config::CredentialStore;
use crate::db::models::SyncTaskType;
use crate::error::{GarminError, Result};
use crate::storage::{default_storage_path, Storage, SyncDb};
use crate::sync::progress::SyncMode;
use crate::sync::{SyncEngine, SyncOptions, TaskQueue};

use super::auth::refresh_token;

/// Run sync operation
#[allow(clippy::too_many_arguments)]
pub async fn run(
    profile: Option<String>,
    storage_path: Option<String>,
    activities: bool,
    health: bool,
    performance: bool,
    from: Option<String>,
    to: Option<String>,
    dry_run: bool,
    backfill: bool,
    force: bool,
    log: Option<String>,
    verbose: bool,
) -> Result<()> {
    let store = CredentialStore::new(profile.clone())?;
    let (_, oauth2) = refresh_token(&store).await?;

    // Open storage
    let storage_path = storage_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    let storage = Storage::open(storage_path)?;

    // Determine sync mode
    let mode = if backfill {
        SyncMode::Backfill
    } else {
        SyncMode::Latest
    };

    // Build sync options
    let sync_all = !activities && !health && !performance;
    let opts = SyncOptions {
        sync_activities: activities || sync_all,
        sync_health: health || sync_all,
        sync_performance: performance || sync_all,
        from_date: from.as_ref().and_then(|s| parse_date(s)),
        to_date: to.as_ref().and_then(|s| parse_date(s)),
        dry_run,
        force,
        concurrency: 4,
        mode,
        logger: None,
    };

    let (logger, log_path) = create_sync_logger(log, verbose)?;
    if let Some(path) = &log_path {
        println!("Writing sync log: {}", path.display());
    }
    let mut opts = opts;
    opts.logger = logger.clone();

    // Create sync engine
    let client = GarminClient::new().with_logger(logger);
    let mut engine = SyncEngine::with_storage(storage, client, oauth2)?;
    engine.run(opts).await?;

    Ok(())
}

/// Show sync status
pub async fn status(profile: Option<String>, storage_path: Option<String>) -> Result<()> {
    let storage_path = storage_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    if !storage_path.exists() {
        println!("No storage found at: {}", storage_path.display());
        println!("Run 'garmin sync run' to create one.");
        return Ok(());
    }

    let sync_db_path = storage_path.join("sync.db");
    if !sync_db_path.exists() {
        println!("No sync database found at: {}", sync_db_path.display());
        println!("Run 'garmin sync run' to create one.");
        return Ok(());
    }

    let sync_db = SyncDb::open(&sync_db_path)?;

    // Resolve profile for status reporting.
    // CLI profile names are credential profiles and may not match Garmin display_name in sync.db.
    let requested_profile = profile.as_deref();
    let mut profile_note: Option<String> = None;
    let (profile_name, profile_id) = match requested_profile {
        Some(name) => match sync_db.get_profile_id(name)? {
            Some(id) => (name.to_string(), Some(id)),
            None => match sync_db.get_latest_profile()? {
                Some((id, resolved_name)) => {
                    profile_note = Some(format!(
                        "Requested profile '{}' not found in sync database; showing latest synced profile '{}'.",
                        name, resolved_name
                    ));
                    (resolved_name, Some(id))
                }
                None => {
                    profile_note = Some(format!(
                        "Requested profile '{}' not found in sync database.",
                        name
                    ));
                    (name.to_string(), None)
                }
            },
        },
        None => match sync_db.get_latest_profile()? {
            Some((id, resolved_name)) => (resolved_name, Some(id)),
            None => ("default".to_string(), None),
        },
    };

    // Count Parquet files
    let activity_files = count_partition_files(&storage_path, "activities");
    let health_files = count_partition_files(&storage_path, "daily_health");
    let performance_files = count_partition_files(&storage_path, "performance_metrics");
    let track_files = count_partition_files(&storage_path, "track_points");

    // Get task status counts
    let (pending, in_progress, completed, failed) = if let Some(pid) = profile_id {
        sync_db.count_tasks_by_status(pid)?
    } else {
        (0, 0, 0, 0)
    };
    let active_tasks = if let Some(pid) = profile_id {
        sync_db.list_in_progress_tasks(pid)?
    } else {
        Vec::new()
    };

    println!("Storage: {}", storage_path.display());
    println!("Profile: {}", profile_name);
    if let Some(note) = profile_note {
        println!("Note: {}", note);
    }
    println!();
    println!("Parquet files:");
    println!("  Activity partitions:    {:>4}", activity_files);
    println!("  Health partitions:      {:>4}", health_files);
    println!("  Performance partitions: {:>4}", performance_files);
    println!("  Track point partitions: {:>4}", track_files);
    println!();
    if profile_id.is_some() {
        println!("Sync tasks:");
        println!("  Pending:     {:>4}", pending);
        println!("  In progress: {:>4}", in_progress);
        println!("  Failed:      {:>4}", failed);
        println!("  Completed:   {:>4}", completed);
    }

    if !active_tasks.is_empty() {
        println!();
        println!("Active tasks:");
        for task in active_tasks {
            let elapsed = task
                .in_progress_at
                .map(|started| {
                    let seconds = (Utc::now() - started).num_seconds().max(0) as u64;
                    format_elapsed(seconds)
                })
                .unwrap_or_else(|| "unknown".to_string());
            let last_error = task
                .last_error
                .as_deref()
                .map(|error| format!("; last error: {}", error))
                .unwrap_or_default();
            println!(
                "  #{} [{}] {} for {} (attempt {}{})",
                task.id,
                task.pipeline,
                task_description(&task.task_type),
                elapsed,
                task.attempts,
                last_error
            );
        }
    }

    Ok(())
}

/// Parse date string to NaiveDate
fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

fn count_partition_files(storage_path: &Path, dirname: &str) -> usize {
    let partition_path = storage_path.join(dirname);
    if !partition_path.exists() {
        return 0;
    }

    std::fs::read_dir(&partition_path)
        .map(|entries| entries.filter(|entry| entry.is_ok()).count())
        .unwrap_or(0)
}

fn task_description(task_type: &SyncTaskType) -> String {
    match task_type {
        SyncTaskType::Activities { start, limit, .. } => {
            format!("Activities {}-{}", start, start + limit)
        }
        SyncTaskType::DownloadGpx {
            activity_id,
            activity_name,
            activity_date,
        } => {
            let name = activity_name
                .as_deref()
                .map(str::to_string)
                .unwrap_or_else(|| activity_id.to_string());
            activity_date
                .as_deref()
                .map(|date| format!("GPX {} {}", date, name))
                .unwrap_or_else(|| format!("GPX {}", name))
        }
        SyncTaskType::DailyHealth { date } => format!("Health {}", date),
        SyncTaskType::Performance { date } => format!("Performance {}", date),
        SyncTaskType::ActivityDetail { activity_id } => format!("Activity detail {}", activity_id),
        SyncTaskType::Weight { from, to } => format!("Weight {} -> {}", from, to),
        SyncTaskType::GenerateEmbeddings { activity_ids } => {
            format!("Embeddings for {} activities", activity_ids.len())
        }
    }
}

fn format_elapsed(secs: u64) -> String {
    let mins = secs / 60;
    let remaining_secs = secs % 60;

    if mins > 0 {
        format!("{}m {}s", mins, remaining_secs)
    } else {
        format!("{}s", secs)
    }
}

fn create_sync_logger(
    log: Option<String>,
    verbose: bool,
) -> Result<(Option<RequestLogger>, Option<PathBuf>)> {
    let Some(path) = resolve_log_path(log, verbose)? else {
        return Ok((None, None));
    };

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            GarminError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to create log directory {}: {}", parent.display(), e),
            ))
        })?;
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| {
            GarminError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to open log file {}: {}", path.display(), e),
            ))
        })?;

    Ok((Some(RequestLogger::new(file, verbose)), Some(path)))
}

fn resolve_log_path(log: Option<String>, verbose: bool) -> Result<Option<PathBuf>> {
    let Some(path) = log.map(PathBuf::from).or_else(|| {
        verbose.then(|| {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(default_log_filename())
        })
    }) else {
        return Ok(None);
    };

    if path.extension().and_then(|ext| ext.to_str()) == Some("log") {
        return Ok(Some(path));
    }

    if path.exists() && !path.is_dir() {
        return Ok(Some(path));
    }

    Ok(Some(path.join(default_log_filename())))
}

fn default_log_filename() -> String {
    format!("garmin-sync-{}.log", Utc::now().format("%Y%m%dT%H%M%SZ"))
}

/// Reset failed tasks to pending
pub async fn reset(storage_path: Option<String>) -> Result<()> {
    let storage_path = storage_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    let sync_db_path = storage_path.join("sync.db");
    if !sync_db_path.exists() {
        println!("No sync database found at: {}", sync_db_path.display());
        return Ok(());
    }

    let sync_db = SyncDb::open(&sync_db_path)?;
    let queue = TaskQueue::new(sync_db, 1, None); // profile_id doesn't matter for reset

    let reset_count = queue.reset_failed()?;
    println!("Reset {} failed tasks to pending", reset_count);

    Ok(())
}

/// Clear all pending tasks
pub async fn clear(storage_path: Option<String>) -> Result<()> {
    let storage_path = storage_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    let sync_db_path = storage_path.join("sync.db");
    if !sync_db_path.exists() {
        println!("No sync database found at: {}", sync_db_path.display());
        return Ok(());
    }

    let sync_db = SyncDb::open(&sync_db_path)?;
    let queue = TaskQueue::new(sync_db, 1, None); // profile_id doesn't matter for clear

    let cleared = queue.clear_pending()?;
    println!("Cleared {} pending tasks", cleared);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_partition_files_returns_zero_for_missing_directory() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert_eq!(count_partition_files(temp_dir.path(), "activities"), 0);
    }

    #[test]
    fn count_partition_files_counts_existing_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        let activities_dir = temp_dir.path().join("activities");
        std::fs::create_dir(&activities_dir).unwrap();
        std::fs::write(activities_dir.join("2026-W10.parquet"), b"test").unwrap();
        std::fs::write(activities_dir.join("2026-W11.parquet"), b"test").unwrap();

        assert_eq!(count_partition_files(temp_dir.path(), "activities"), 2);
    }

    #[test]
    fn task_description_describes_active_tasks_compactly() {
        assert_eq!(
            task_description(&SyncTaskType::DailyHealth {
                date: NaiveDate::from_ymd_opt(2026, 7, 5).unwrap()
            }),
            "Health 2026-07-05"
        );
        assert_eq!(
            task_description(&SyncTaskType::DownloadGpx {
                activity_id: 42,
                activity_name: Some("Tempo Run".to_string()),
                activity_date: Some("2026-07-04".to_string()),
            }),
            "GPX 2026-07-04 Tempo Run"
        );
    }
}
