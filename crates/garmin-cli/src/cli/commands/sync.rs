//! Sync commands for garmin-cli

use chrono::NaiveDate;

use crate::client::GarminClient;
use crate::config::CredentialStore;
use crate::db::default_db_path;
use crate::error::Result;
use crate::sync::{SyncEngine, SyncOptions, TaskQueue};
use crate::Database;

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
) -> Result<()> {
    let store = CredentialStore::new(profile.clone())?;
    let (oauth1, oauth2) = refresh_token(&store).await?;

    // Open database
    let db_path = db_path.unwrap_or_else(|| default_db_path().unwrap());
    println!("Using database: {}", db_path);

    let db = Database::open(&db_path)?;

    // Get or create profile
    let profile_id = get_or_create_profile(&db, profile.as_deref())?;

    // Build sync options
    let sync_all = !activities && !health && !performance;
    let opts = SyncOptions {
        sync_activities: activities || sync_all,
        sync_health: health || sync_all,
        sync_performance: performance || sync_all,
        from_date: from.as_ref().and_then(|s| parse_date(s)),
        to_date: to.as_ref().and_then(|s| parse_date(s)),
        dry_run,
        force: false,
        fancy_ui: !simple, // Use fancy TUI unless --simple is specified
        concurrency: 3,
    };

    if dry_run {
        println!("Dry run mode - no changes will be made");
    }

    // Create sync engine
    let client = GarminClient::new(&oauth1.domain);
    let mut engine = SyncEngine::new(db, client, oauth2, profile_id);

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
    let db_path = db_path.unwrap_or_else(|| default_db_path().unwrap());

    if !std::path::Path::new(&db_path).exists() {
        println!("No database found at: {}", db_path);
        println!("Run 'garmin sync' to create one.");
        return Ok(());
    }

    let db = Database::open(&db_path)?;
    let conn = db.connection();
    let conn = conn.lock().unwrap();

    // Get profile info
    let profile_name = profile.as_deref().unwrap_or("default");

    // Count activities
    let activity_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM activities", [], |row| row.get(0))
        .unwrap_or(0);

    // Count health days
    let health_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM daily_health", [], |row| row.get(0))
        .unwrap_or(0);

    // Count track points
    let trackpoint_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM track_points", [], |row| row.get(0))
        .unwrap_or(0);

    // Get pending tasks
    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sync_tasks WHERE status IN ('pending', 'failed')",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    println!("Database: {}", db_path);
    println!("Profile: {}", profile_name);
    println!();
    println!("Data stored:");
    println!("  Activities:    {:>8}", activity_count);
    println!("  Health days:   {:>8}", health_count);
    println!("  Track points:  {:>8}", trackpoint_count);
    println!();
    if pending_count > 0 {
        println!("Pending sync tasks: {}", pending_count);
    }

    Ok(())
}

/// Get or create profile in database
fn get_or_create_profile(db: &Database, profile_name: Option<&str>) -> Result<i32> {
    let conn = db.connection();
    let conn = conn.lock().unwrap();

    let name = profile_name.unwrap_or("default");

    // Try to get existing profile
    let existing = conn.query_row(
        "SELECT profile_id FROM profiles WHERE display_name = ?",
        duckdb::params![name],
        |row| row.get::<_, i32>(0),
    );

    match existing {
        Ok(id) => Ok(id),
        Err(duckdb::Error::QueryReturnedNoRows) => {
            // Create new profile with RETURNING to get the generated ID
            let id: i32 = conn
                .query_row(
                    "INSERT INTO profiles (display_name) VALUES (?) RETURNING profile_id",
                    duckdb::params![name],
                    |row| row.get(0),
                )
                .map_err(|e| crate::GarminError::Database(e.to_string()))?;

            Ok(id)
        }
        Err(e) => Err(crate::GarminError::Database(e.to_string())),
    }
}

/// Parse date string to NaiveDate
fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// Reset failed tasks to pending
pub async fn reset(db_path: Option<String>) -> Result<()> {
    let db_path = db_path.unwrap_or_else(|| default_db_path().unwrap());

    if !std::path::Path::new(&db_path).exists() {
        println!("No database found at: {}", db_path);
        return Ok(());
    }

    let db = Database::open(&db_path)?;
    let queue = TaskQueue::new(db);

    let reset_count = queue.reset_failed()?;
    println!("Reset {} failed tasks to pending", reset_count);

    Ok(())
}

/// Clear all pending tasks
pub async fn clear(db_path: Option<String>) -> Result<()> {
    let db_path = db_path.unwrap_or_else(|| default_db_path().unwrap());

    if !std::path::Path::new(&db_path).exists() {
        println!("No database found at: {}", db_path);
        return Ok(());
    }

    let db = Database::open(&db_path)?;
    let queue = TaskQueue::new(db);

    let cleared = queue.clear_pending()?;
    println!("Cleared {} pending tasks", cleared);

    Ok(())
}
