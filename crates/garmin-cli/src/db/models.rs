//! Database models matching schema tables

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// Profile for multi-account support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub profile_id: i32,
    pub display_name: String,
    pub user_id: Option<i64>,
    pub created_at: Option<DateTime<Utc>>,
    pub last_sync_at: Option<DateTime<Utc>>,
}

/// Activity summary from Garmin API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub activity_id: i64,
    pub profile_id: i32,
    pub activity_name: Option<String>,
    pub activity_type: Option<String>,
    pub start_time_local: Option<DateTime<Utc>>,
    pub start_time_gmt: Option<DateTime<Utc>>,
    pub duration_sec: Option<f64>,
    pub distance_m: Option<f64>,
    pub calories: Option<i32>,
    pub avg_hr: Option<i32>,
    pub max_hr: Option<i32>,
    pub avg_speed: Option<f64>,
    pub max_speed: Option<f64>,
    pub elevation_gain: Option<f64>,
    pub elevation_loss: Option<f64>,
    pub avg_cadence: Option<f64>,
    pub avg_power: Option<i32>,
    pub normalized_power: Option<i32>,
    pub training_effect: Option<f64>,
    pub training_load: Option<f64>,
    pub start_lat: Option<f64>,
    pub start_lon: Option<f64>,
    pub end_lat: Option<f64>,
    pub end_lon: Option<f64>,
    pub ground_contact_time: Option<f64>,
    pub vertical_oscillation: Option<f64>,
    pub stride_length: Option<f64>,
    pub location_name: Option<String>,
    pub raw_json: Option<serde_json::Value>,
}

/// GPS track point with sensor data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackPoint {
    pub id: Option<i64>,
    pub activity_id: i64,
    pub timestamp: DateTime<Utc>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub elevation: Option<f64>,
    pub heart_rate: Option<i32>,
    pub cadence: Option<i32>,
    pub power: Option<i32>,
    pub speed: Option<f64>,
}

/// Daily health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyHealth {
    pub id: Option<i64>,
    pub profile_id: i32,
    pub date: NaiveDate,
    pub steps: Option<i32>,
    pub step_goal: Option<i32>,
    pub total_calories: Option<i32>,
    pub active_calories: Option<i32>,
    pub bmr_calories: Option<i32>,
    pub resting_hr: Option<i32>,
    pub sleep_seconds: Option<i32>,
    pub deep_sleep_seconds: Option<i32>,
    pub light_sleep_seconds: Option<i32>,
    pub rem_sleep_seconds: Option<i32>,
    pub sleep_score: Option<i32>,
    pub avg_stress: Option<i32>,
    pub max_stress: Option<i32>,
    pub body_battery_start: Option<i32>,
    pub body_battery_end: Option<i32>,
    pub hrv_weekly_avg: Option<i32>,
    pub hrv_last_night: Option<i32>,
    pub hrv_status: Option<String>,
    pub avg_respiration: Option<f64>,
    pub avg_spo2: Option<i32>,
    pub lowest_spo2: Option<i32>,
    pub hydration_ml: Option<i32>,
    pub moderate_intensity_min: Option<i32>,
    pub vigorous_intensity_min: Option<i32>,
    pub raw_json: Option<serde_json::Value>,
}

/// Performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub id: Option<i64>,
    pub profile_id: i32,
    pub date: NaiveDate,
    pub vo2max: Option<f64>,
    pub fitness_age: Option<i32>,
    pub training_readiness: Option<i32>,
    pub training_status: Option<String>,
    pub lactate_threshold_hr: Option<i32>,
    pub lactate_threshold_pace: Option<f64>,
    pub race_5k_sec: Option<i32>,
    pub race_10k_sec: Option<i32>,
    pub race_half_sec: Option<i32>,
    pub race_marathon_sec: Option<i32>,
    pub endurance_score: Option<i32>,
    pub hill_score: Option<i32>,
    pub raw_json: Option<serde_json::Value>,
}

/// Weight entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightEntry {
    pub id: Option<i64>,
    pub profile_id: i32,
    pub date: NaiveDate,
    pub weight_kg: Option<f64>,
    pub bmi: Option<f64>,
    pub body_fat_pct: Option<f64>,
    pub muscle_mass_kg: Option<f64>,
}

/// Sync state for incremental updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    pub profile_id: i32,
    pub data_type: String,
    pub last_sync_date: Option<NaiveDate>,
    pub last_activity_id: Option<i64>,
}

/// Sync task status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "pending"),
            TaskStatus::InProgress => write!(f, "in_progress"),
            TaskStatus::Completed => write!(f, "completed"),
            TaskStatus::Failed => write!(f, "failed"),
        }
    }
}

/// Sync task types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SyncTaskType {
    Activities {
        start: u32,
        limit: u32,
    },
    ActivityDetail {
        activity_id: i64,
    },
    DownloadGpx {
        activity_id: i64,
        #[serde(default)]
        activity_name: Option<String>,
        #[serde(default)]
        activity_date: Option<String>,
    },
    DailyHealth {
        date: NaiveDate,
    },
    Performance {
        date: NaiveDate,
    },
    Weight {
        from: NaiveDate,
        to: NaiveDate,
    },
    GenerateEmbeddings {
        activity_ids: Vec<i64>,
    },
}

/// Sync task for queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncTask {
    pub id: Option<i64>,
    pub profile_id: i32,
    pub task_type: SyncTaskType,
    pub status: TaskStatus,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl SyncTask {
    pub fn new(profile_id: i32, task_type: SyncTaskType) -> Self {
        Self {
            id: None,
            profile_id,
            task_type,
            status: TaskStatus::Pending,
            attempts: 0,
            last_error: None,
            created_at: None,
            next_retry_at: None,
            completed_at: None,
        }
    }
}
