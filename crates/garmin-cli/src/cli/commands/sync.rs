//! Sync commands for garmin-cli

use chrono::NaiveDate;
use std::path::Path;

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::error::Result;
use crate::storage::{default_storage_path, Storage, SyncDb};
use crate::sync::progress::SyncMode;
use crate::sync::{SyncEngine, SyncOptions, TaskQueue};

use super::auth::refresh_token;

/// Run sync operation
#[allow(clippy::too_many_arguments)]
pub async fn run(
    profile: Option<String>,
    db_path: Option<String>,
    activities: bool,
    health: bool,
    performance: bool,
    from: Option<String>,
    to: Option<String>,
    dry_run: bool,
    backfill: bool,
    force: bool,
) -> Result<()> {
    let store = CredentialStore::new(profile.clone())?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    // Open storage
    let storage_path = db_path
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
    };

    // Create sync engine
    let client = GarminClient::new(&oauth1.domain);
    let mut engine = SyncEngine::with_storage(storage, client, oauth2)?;
    engine.run(opts).await?;

    Ok(())
}

/// Show sync status
pub async fn status(profile: Option<String>, db_path: Option<String>) -> Result<()> {
    let storage_path = db_path
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

    // Get pending tasks
    let pending_count = if let Some(pid) = profile_id {
        sync_db.count_pending_tasks(pid, None)?
    } else {
        0
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
    if pending_count > 0 {
        println!("Pending sync tasks: {}", pending_count);
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

/// Reset failed tasks to pending
pub async fn reset(db_path: Option<String>) -> Result<()> {
    let storage_path = db_path
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
pub async fn clear(db_path: Option<String>) -> Result<()> {
    let storage_path = db_path
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
}
