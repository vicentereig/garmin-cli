//! Partition key calculation for time-partitioned Parquet storage

use chrono::{Datelike, NaiveDate};

/// Entity types with their partition strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityType {
    /// Weekly partitions (YYYY-Www)
    Activities,
    /// Daily partitions (YYYY-MM-DD)
    TrackPoints,
    /// Monthly partitions (YYYY-MM)
    DailyHealth,
    /// Monthly partitions (YYYY-MM)
    PerformanceMetrics,
    /// Monthly partitions (YYYY-MM)
    Weight,
    /// Single file (profiles.parquet)
    Profiles,
}

impl EntityType {
    /// Get the directory name for this entity
    pub fn dir_name(&self) -> &'static str {
        match self {
            EntityType::Activities => "activities",
            EntityType::TrackPoints => "track_points",
            EntityType::DailyHealth => "daily_health",
            EntityType::PerformanceMetrics => "performance_metrics",
            EntityType::Weight => "weight",
            EntityType::Profiles => "", // Single file, no directory
        }
    }

    /// Calculate partition key for a given date
    pub fn partition_key(&self, date: NaiveDate) -> String {
        match self {
            EntityType::Activities => {
                // Weekly: YYYY-Www (ISO week)
                format!("{}-W{:02}", date.iso_week().year(), date.iso_week().week())
            }
            EntityType::TrackPoints => {
                // Daily: YYYY-MM-DD
                date.format("%Y-%m-%d").to_string()
            }
            EntityType::DailyHealth
            | EntityType::PerformanceMetrics
            | EntityType::Weight => {
                // Monthly: YYYY-MM
                date.format("%Y-%m").to_string()
            }
            EntityType::Profiles => {
                // Single file
                "profiles".to_string()
            }
        }
    }

    /// Get the glob pattern for querying all partitions
    pub fn glob_pattern(&self) -> String {
        match self {
            EntityType::Profiles => "profiles.parquet".to_string(),
            _ => format!("{}/*.parquet", self.dir_name()),
        }
    }

    /// Get the glob pattern for querying partitions in a date range
    pub fn date_range_pattern(&self, from: NaiveDate, to: NaiveDate) -> Vec<String> {
        let mut patterns = Vec::new();
        let mut current = from;

        while current <= to {
            let key = self.partition_key(current);
            let pattern = match self {
                EntityType::Profiles => "profiles.parquet".to_string(),
                _ => format!("{}/{}.parquet", self.dir_name(), key),
            };

            if !patterns.contains(&pattern) {
                patterns.push(pattern);
            }

            // Advance by appropriate interval
            current = match self {
                EntityType::TrackPoints => current.succ_opt().unwrap_or(current),
                EntityType::Activities => {
                    // Advance to next week
                    current + chrono::Duration::days(7)
                }
                _ => {
                    // Advance to next month
                    if current.month() == 12 {
                        NaiveDate::from_ymd_opt(current.year() + 1, 1, 1).unwrap()
                    } else {
                        NaiveDate::from_ymd_opt(current.year(), current.month() + 1, 1).unwrap()
                    }
                }
            };
        }

        patterns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weekly_partition_key() {
        let date = NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(); // Sunday of week 50
        assert_eq!(EntityType::Activities.partition_key(date), "2024-W50");
    }

    #[test]
    fn test_daily_partition_key() {
        let date = NaiveDate::from_ymd_opt(2024, 12, 15).unwrap();
        assert_eq!(EntityType::TrackPoints.partition_key(date), "2024-12-15");
    }

    #[test]
    fn test_monthly_partition_key() {
        let date = NaiveDate::from_ymd_opt(2024, 12, 15).unwrap();
        assert_eq!(EntityType::DailyHealth.partition_key(date), "2024-12");
    }

    #[test]
    fn test_glob_patterns() {
        assert_eq!(EntityType::Activities.glob_pattern(), "activities/*.parquet");
        assert_eq!(EntityType::Profiles.glob_pattern(), "profiles.parquet");
    }
}
