//! Database schema and migrations

use duckdb::Connection;

/// Current schema version
const SCHEMA_VERSION: i32 = 1;

/// Run all pending migrations
pub fn migrate(conn: &Connection) -> crate::Result<()> {
    // Create migrations table if not exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )
    .map_err(|e| crate::error::GarminError::Database(e.to_string()))?;

    // Get current version
    let current_version: i32 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    // Apply pending migrations
    if current_version < 1 {
        migration_v1(conn)?;
    }

    Ok(())
}

/// Migration v1: Initial schema
fn migration_v1(conn: &Connection) -> crate::Result<()> {
    let statements = [
        // Profiles for multi-account support
        "CREATE SEQUENCE IF NOT EXISTS profiles_seq",
        "CREATE TABLE IF NOT EXISTS profiles (
            profile_id INTEGER PRIMARY KEY DEFAULT nextval('profiles_seq'),
            display_name TEXT NOT NULL UNIQUE,
            user_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_sync_at TIMESTAMP
        )",
        // Activities (summary data from API)
        "CREATE TABLE IF NOT EXISTS activities (
            activity_id BIGINT PRIMARY KEY,
            profile_id INTEGER REFERENCES profiles(profile_id),
            activity_name TEXT,
            activity_type TEXT,
            start_time_local TIMESTAMP,
            start_time_gmt TIMESTAMP,
            duration_sec DOUBLE,
            distance_m DOUBLE,
            calories INTEGER,
            avg_hr INTEGER,
            max_hr INTEGER,
            avg_speed DOUBLE,
            max_speed DOUBLE,
            elevation_gain DOUBLE,
            elevation_loss DOUBLE,
            avg_cadence DOUBLE,
            avg_power INTEGER,
            normalized_power INTEGER,
            training_effect DOUBLE,
            training_load DOUBLE,
            start_lat DOUBLE,
            start_lon DOUBLE,
            end_lat DOUBLE,
            end_lon DOUBLE,
            ground_contact_time DOUBLE,
            vertical_oscillation DOUBLE,
            stride_length DOUBLE,
            location_name TEXT,
            raw_json JSON,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
        // Track points (time-series GPS + sensor data)
        "CREATE SEQUENCE IF NOT EXISTS track_points_seq",
        "CREATE TABLE IF NOT EXISTS track_points (
            id BIGINT PRIMARY KEY DEFAULT nextval('track_points_seq'),
            activity_id BIGINT REFERENCES activities(activity_id),
            timestamp TIMESTAMP NOT NULL,
            lat DOUBLE,
            lon DOUBLE,
            elevation DOUBLE,
            heart_rate INTEGER,
            cadence INTEGER,
            power INTEGER,
            speed DOUBLE
        )",
        "CREATE INDEX IF NOT EXISTS idx_trackpoints_activity ON track_points(activity_id, timestamp)",
        // Daily health metrics
        "CREATE SEQUENCE IF NOT EXISTS daily_health_seq",
        "CREATE TABLE IF NOT EXISTS daily_health (
            id BIGINT PRIMARY KEY DEFAULT nextval('daily_health_seq'),
            profile_id INTEGER REFERENCES profiles(profile_id),
            date DATE NOT NULL,
            steps INTEGER,
            step_goal INTEGER,
            total_calories INTEGER,
            active_calories INTEGER,
            bmr_calories INTEGER,
            resting_hr INTEGER,
            sleep_seconds INTEGER,
            deep_sleep_seconds INTEGER,
            light_sleep_seconds INTEGER,
            rem_sleep_seconds INTEGER,
            sleep_score INTEGER,
            avg_stress INTEGER,
            max_stress INTEGER,
            body_battery_start INTEGER,
            body_battery_end INTEGER,
            hrv_weekly_avg INTEGER,
            hrv_last_night INTEGER,
            hrv_status TEXT,
            avg_respiration DOUBLE,
            avg_spo2 INTEGER,
            lowest_spo2 INTEGER,
            hydration_ml INTEGER,
            moderate_intensity_min INTEGER,
            vigorous_intensity_min INTEGER,
            raw_json JSON,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(profile_id, date)
        )",
        "CREATE INDEX IF NOT EXISTS idx_health_date ON daily_health(profile_id, date)",
        // Performance metrics
        "CREATE SEQUENCE IF NOT EXISTS performance_metrics_seq",
        "CREATE TABLE IF NOT EXISTS performance_metrics (
            id BIGINT PRIMARY KEY DEFAULT nextval('performance_metrics_seq'),
            profile_id INTEGER REFERENCES profiles(profile_id),
            date DATE NOT NULL,
            vo2max DOUBLE,
            fitness_age INTEGER,
            training_readiness INTEGER,
            training_status TEXT,
            lactate_threshold_hr INTEGER,
            lactate_threshold_pace DOUBLE,
            race_5k_sec INTEGER,
            race_10k_sec INTEGER,
            race_half_sec INTEGER,
            race_marathon_sec INTEGER,
            endurance_score INTEGER,
            hill_score INTEGER,
            raw_json JSON,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(profile_id, date)
        )",
        // Weight entries
        "CREATE SEQUENCE IF NOT EXISTS weight_entries_seq",
        "CREATE TABLE IF NOT EXISTS weight_entries (
            id BIGINT PRIMARY KEY DEFAULT nextval('weight_entries_seq'),
            profile_id INTEGER REFERENCES profiles(profile_id),
            date DATE NOT NULL,
            weight_kg DOUBLE,
            bmi DOUBLE,
            body_fat_pct DOUBLE,
            muscle_mass_kg DOUBLE,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(profile_id, date)
        )",
        // Sync state tracking
        "CREATE TABLE IF NOT EXISTS sync_state (
            profile_id INTEGER REFERENCES profiles(profile_id),
            data_type TEXT NOT NULL,
            last_sync_date DATE,
            last_activity_id BIGINT,
            PRIMARY KEY (profile_id, data_type)
        )",
        // Sync tasks queue for recovery
        "CREATE SEQUENCE IF NOT EXISTS sync_tasks_seq",
        "CREATE TABLE IF NOT EXISTS sync_tasks (
            id BIGINT PRIMARY KEY DEFAULT nextval('sync_tasks_seq'),
            profile_id INTEGER,
            task_type TEXT NOT NULL,
            task_data JSON,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            next_retry_at TIMESTAMP,
            completed_at TIMESTAMP
        )",
        "CREATE INDEX IF NOT EXISTS idx_sync_tasks_status ON sync_tasks(status, next_retry_at)",
        // Record migration
        "INSERT INTO schema_migrations (version) VALUES (1)",
    ];

    for sql in statements {
        conn.execute(sql, [])
            .map_err(|e| crate::error::GarminError::Database(format!("{}: {}", sql.chars().take(50).collect::<String>(), e)))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_v1() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).expect("Migration failed");

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"profiles".to_string()));
        assert!(tables.contains(&"activities".to_string()));
        assert!(tables.contains(&"track_points".to_string()));
        assert!(tables.contains(&"daily_health".to_string()));
        assert!(tables.contains(&"sync_tasks".to_string()));
    }

    #[test]
    fn test_migration_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).expect("First migration failed");
        migrate(&conn).expect("Second migration should be idempotent");
    }
}
