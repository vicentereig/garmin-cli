//! Activity data models for Garmin Connect API
//!
//! These structures represent activities returned from the Garmin Connect API.

use serde::{Deserialize, Serialize};

/// Activity summary returned from the activity list endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivitySummary {
    /// Unique activity identifier
    pub activity_id: u64,

    /// User-provided or auto-generated activity name
    #[serde(default)]
    pub activity_name: Option<String>,

    /// Start time in local timezone (ISO 8601 format)
    #[serde(default)]
    pub start_time_local: Option<String>,

    /// Start time in GMT (ISO 8601 format)
    #[serde(default)]
    pub start_time_gmt: Option<String>,

    /// Activity type information
    #[serde(default)]
    pub activity_type: Option<ActivityType>,

    /// Distance in meters
    #[serde(default)]
    pub distance: Option<f64>,

    /// Duration in seconds
    #[serde(default)]
    pub duration: Option<f64>,

    /// Elapsed duration in seconds (including pauses)
    #[serde(default)]
    pub elapsed_duration: Option<f64>,

    /// Moving duration in seconds
    #[serde(default)]
    pub moving_duration: Option<f64>,

    /// Calories burned
    #[serde(default)]
    pub calories: Option<f64>,

    /// Average heart rate in bpm
    #[serde(default)]
    pub average_hr: Option<f64>,

    /// Maximum heart rate in bpm
    #[serde(default)]
    pub max_hr: Option<f64>,

    /// Average speed in m/s
    #[serde(default)]
    pub average_speed: Option<f64>,

    /// Maximum speed in m/s
    #[serde(default)]
    pub max_speed: Option<f64>,

    /// Total elevation gain in meters
    #[serde(default)]
    pub elevation_gain: Option<f64>,

    /// Total elevation loss in meters
    #[serde(default)]
    pub elevation_loss: Option<f64>,

    /// Average running cadence in steps per minute
    #[serde(default)]
    pub average_running_cadence_in_steps_per_minute: Option<f64>,

    /// Steps count
    #[serde(default)]
    pub steps: Option<u64>,

    /// Whether the activity has GPS data
    #[serde(default)]
    pub has_polyline: Option<bool>,

    /// Owner display name
    #[serde(default)]
    pub owner_display_name: Option<String>,
}

/// Activity type information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityType {
    /// Type key (e.g., "running", "cycling", "walking")
    pub type_key: String,

    /// Type ID
    #[serde(default)]
    pub type_id: Option<u64>,

    /// Parent type key
    #[serde(default)]
    pub parent_type_id: Option<u64>,

    /// Whether this is a custom activity type
    #[serde(default)]
    pub is_hidden: Option<bool>,
}

/// Full activity details (more comprehensive than summary)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityDetails {
    /// Unique activity identifier
    pub activity_id: u64,

    /// Activity name
    #[serde(default)]
    pub activity_name: Option<String>,

    /// Activity description
    #[serde(default)]
    pub description: Option<String>,

    /// Start time in local timezone
    #[serde(default)]
    pub start_time_local: Option<String>,

    /// Start time in GMT
    #[serde(default)]
    pub start_time_gmt: Option<String>,

    /// Activity type
    #[serde(default)]
    pub activity_type: Option<ActivityType>,

    /// Summary data (contains metrics like distance, duration, etc.)
    #[serde(default)]
    pub summary_dto: Option<serde_json::Value>,

    /// Location name
    #[serde(default)]
    pub location_name: Option<String>,

    /// Time zone unit
    #[serde(default)]
    pub time_zone_unit_dto: Option<serde_json::Value>,

    /// Metadata
    #[serde(default)]
    pub metadata_dto: Option<serde_json::Value>,

    /// Catch-all for unknown fields
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Response from activity upload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResult {
    /// Detailed import result
    pub detailed_import_result: DetailedImportResult,
}

/// Detailed import result from upload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailedImportResult {
    /// Upload ID
    pub upload_id: u64,

    /// Upload UUID
    #[serde(default)]
    pub upload_uuid: Option<UploadUuid>,

    /// Owner ID
    #[serde(default)]
    pub owner: Option<u64>,

    /// File size in bytes
    #[serde(default)]
    pub file_size: Option<u64>,

    /// Processing time in milliseconds
    #[serde(default)]
    pub processing_time: Option<u64>,

    /// Creation date
    #[serde(default)]
    pub creation_date: Option<String>,

    /// Original file name
    #[serde(default)]
    pub file_name: Option<String>,

    /// Successful imports
    #[serde(default)]
    pub successes: Vec<UploadSuccess>,

    /// Failed imports
    #[serde(default)]
    pub failures: Vec<serde_json::Value>,
}

/// Upload UUID
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadUuid {
    pub uuid: String,
}

/// Successful upload entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadSuccess {
    /// Created activity ID
    #[serde(default)]
    pub internal_id: Option<u64>,

    /// External ID
    #[serde(default)]
    pub external_id: Option<String>,

    /// Messages
    #[serde(default)]
    pub messages: Vec<serde_json::Value>,
}

impl ActivitySummary {
    /// Get a display-friendly name for the activity
    pub fn display_name(&self) -> String {
        self.activity_name
            .clone()
            .unwrap_or_else(|| "Unnamed Activity".to_string())
    }

    /// Get the activity type key
    pub fn type_key(&self) -> String {
        self.activity_type
            .as_ref()
            .map(|t| t.type_key.clone())
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Get distance in kilometers
    pub fn distance_km(&self) -> Option<f64> {
        self.distance.map(|d| d / 1000.0)
    }

    /// Get duration formatted as HH:MM:SS
    pub fn duration_formatted(&self) -> String {
        match self.duration {
            Some(secs) => {
                let total_secs = secs as u64;
                let hours = total_secs / 3600;
                let minutes = (total_secs % 3600) / 60;
                let seconds = total_secs % 60;
                if hours > 0 {
                    format!("{}:{:02}:{:02}", hours, minutes, seconds)
                } else {
                    format!("{}:{:02}", minutes, seconds)
                }
            }
            None => "-".to_string(),
        }
    }

    /// Get the date portion of start time
    pub fn date(&self) -> String {
        self.start_time_local
            .as_ref()
            .map(|s| {
                // Handle both ISO format (T separator) and space-separated format
                s.split(|c| c == 'T' || c == ' ')
                    .next()
                    .unwrap_or(s)
                    .to_string()
            })
            .unwrap_or_else(|| "-".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activity_summary_display_name() {
        let activity = ActivitySummary {
            activity_id: 123,
            activity_name: Some("Morning Run".to_string()),
            start_time_local: None,
            start_time_gmt: None,
            activity_type: None,
            distance: None,
            duration: None,
            elapsed_duration: None,
            moving_duration: None,
            calories: None,
            average_hr: None,
            max_hr: None,
            average_speed: None,
            max_speed: None,
            elevation_gain: None,
            elevation_loss: None,
            average_running_cadence_in_steps_per_minute: None,
            steps: None,
            has_polyline: None,
            owner_display_name: None,
        };
        assert_eq!(activity.display_name(), "Morning Run");
    }

    #[test]
    fn test_activity_summary_duration_formatted() {
        let mut activity = ActivitySummary {
            activity_id: 123,
            activity_name: None,
            start_time_local: None,
            start_time_gmt: None,
            activity_type: None,
            distance: None,
            duration: Some(3661.0), // 1h 1m 1s
            elapsed_duration: None,
            moving_duration: None,
            calories: None,
            average_hr: None,
            max_hr: None,
            average_speed: None,
            max_speed: None,
            elevation_gain: None,
            elevation_loss: None,
            average_running_cadence_in_steps_per_minute: None,
            steps: None,
            has_polyline: None,
            owner_display_name: None,
        };
        assert_eq!(activity.duration_formatted(), "1:01:01");

        activity.duration = Some(125.0); // 2m 5s
        assert_eq!(activity.duration_formatted(), "2:05");
    }

    #[test]
    fn test_activity_summary_distance_km() {
        let activity = ActivitySummary {
            activity_id: 123,
            activity_name: None,
            start_time_local: None,
            start_time_gmt: None,
            activity_type: None,
            distance: Some(10500.0), // 10.5 km
            duration: None,
            elapsed_duration: None,
            moving_duration: None,
            calories: None,
            average_hr: None,
            max_hr: None,
            average_speed: None,
            max_speed: None,
            elevation_gain: None,
            elevation_loss: None,
            average_running_cadence_in_steps_per_minute: None,
            steps: None,
            has_polyline: None,
            owner_display_name: None,
        };
        assert_eq!(activity.distance_km(), Some(10.5));
    }
}
