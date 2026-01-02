//! Sync commands for garmin-cli

use chrono::NaiveDate;

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
    simple: bool,
    backfill: bool,
    force: bool,
) -> Result<()> {
    let store = CredentialStore::new(profile.clone())?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    // Open storage
    let storage_path = db_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    if simple {
        println!("Using storage: {}", storage_path.display());
    }

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
        fancy_ui: !simple, // Use fancy TUI unless --simple is specified
        concurrency: 4,
        mode,
    };

    if dry_run {
        println!("Dry run mode - no changes will be made");
    }

    if backfill && simple {
        println!("Running backfill sync (historical data)...");
    }

    // Create sync engine
    let client = GarminClient::new(&oauth1.domain);
    let mut engine = SyncEngine::with_storage(storage, client, oauth2)?;

    if !simple {
        // TUI mode - less initial output
        let stats = engine.run(opts).await?;
        // Stats printed by TUI
        let _ = stats; // Suppress unused warning
    } else {
        // Simple mode - traditional output
        println!("Starting sync...");
        let stats = engine.run(opts).await?;
        println!("\nSync complete: {}", stats);
    }

    Ok(())
}

/// Show sync status
pub async fn status(profile: Option<String>, db_path: Option<String>) -> Result<()> {
    let storage_path = db_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_storage_path);

    if !storage_path.exists() {
        println!("No storage found at: {}", storage_path.display());
        println!("Run 'garmin sync' to create one.");
        return Ok(());
    }

    let sync_db_path = storage_path.join("sync.db");
    if !sync_db_path.exists() {
        println!("No sync database found at: {}", sync_db_path.display());
        println!("Run 'garmin sync' to create one.");
        return Ok(());
    }

    let sync_db = SyncDb::open(&sync_db_path)?;

    // Get profile info
    let profile_name = profile.as_deref().unwrap_or("default");
    let profile_id = sync_db.get_profile_id(profile_name)?;

    // Count Parquet files
    let activities_path = storage_path.join("activities");
    let activity_files = if activities_path.exists() {
        std::fs::read_dir(&activities_path)
            .map(|rd| rd.filter(|e| e.is_ok()).count())
            .unwrap_or(0)
    } else {
        0
    };

    let health_path = storage_path.join("daily_health");
    let health_files = if health_path.exists() {
        std::fs::read_dir(&health_path)
            .map(|rd| rd.filter(|e| e.is_ok()).count())
            .unwrap_or(0)
    } else {
        0
    };

    let track_points_path = storage_path.join("track_points");
    let track_files = if track_points_path.exists() {
        std::fs::read_dir(&track_points_path)
            .map(|rd| rd.filter(|e| e.is_ok()).count())
            .unwrap_or(0)
    } else {
        0
    };

    // Get pending tasks
    let pending_count = if let Some(pid) = profile_id {
        sync_db.count_pending_tasks(pid, None)?
    } else {
        0
    };

    println!("Storage: {}", storage_path.display());
    println!("Profile: {}", profile_name);
    println!();
    println!("Parquet files:");
    println!("  Activity partitions:    {:>4}", activity_files);
    println!("  Health partitions:      {:>4}", health_files);
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
