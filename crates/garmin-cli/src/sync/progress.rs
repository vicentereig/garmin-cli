//! Progress tracking for parallel sync with atomic counters

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Progress tracking for a single data stream
#[derive(Debug)]
pub struct StreamProgress {
    /// Display name for this stream
    pub name: &'static str,
    /// Total items to process
    pub total: AtomicU32,
    /// Completed items
    pub completed: AtomicU32,
    /// Failed items
    pub failed: AtomicU32,
    /// Last processed item description
    last_item: Mutex<String>,
}

impl StreamProgress {
    /// Create a new stream progress tracker
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            total: AtomicU32::new(0),
            completed: AtomicU32::new(0),
            failed: AtomicU32::new(0),
            last_item: Mutex::new(String::new()),
        }
    }

    /// Set the total count
    pub fn set_total(&self, total: u32) {
        self.total.store(total, Ordering::Relaxed);
    }

    /// Add to total (for dynamic task generation like activities -> GPX)
    pub fn add_total(&self, count: u32) {
        self.total.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment completed count
    pub fn complete_one(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment failed count
    pub fn fail_one(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the last processed item description
    pub fn set_last_item(&self, desc: String) {
        let mut last = self.last_item.lock().unwrap();
        *last = desc;
    }

    /// Get the last processed item description
    pub fn get_last_item(&self) -> String {
        self.last_item.lock().unwrap().clone()
    }

    /// Get completion percentage (0-100)
    pub fn percent(&self) -> u16 {
        let total = self.total.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        let completed = self.completed.load(Ordering::Relaxed);
        ((completed as f64 / total as f64) * 100.0) as u16
    }

    /// Check if this stream is complete
    pub fn is_complete(&self) -> bool {
        let total = self.total.load(Ordering::Relaxed);
        let completed = self.completed.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        total > 0 && (completed + failed) >= total
    }

    /// Get total count
    pub fn get_total(&self) -> u32 {
        self.total.load(Ordering::Relaxed)
    }

    /// Get completed count
    pub fn get_completed(&self) -> u32 {
        self.completed.load(Ordering::Relaxed)
    }

    /// Get failed count
    pub fn get_failed(&self) -> u32 {
        self.failed.load(Ordering::Relaxed)
    }
}

/// Overall sync progress across all streams
pub struct SyncProgress {
    /// Activities stream progress
    pub activities: StreamProgress,
    /// GPX downloads progress
    pub gpx: StreamProgress,
    /// Health data progress
    pub health: StreamProgress,
    /// Performance metrics progress
    pub performance: StreamProgress,
    /// Start time for ETA calculation
    pub start_time: Instant,
    /// User profile name
    pub profile_name: Mutex<String>,
    /// Date range being synced
    pub date_range: Mutex<String>,
    /// Request rate history (last 60 seconds)
    pub rate_history: Mutex<Vec<u32>>,
    /// Total requests made
    pub total_requests: AtomicU32,
}

impl SyncProgress {
    /// Create new sync progress tracker
    pub fn new() -> Self {
        Self {
            activities: StreamProgress::new("Activities"),
            gpx: StreamProgress::new("GPX Downloads"),
            health: StreamProgress::new("Health"),
            performance: StreamProgress::new("Performance"),
            start_time: Instant::now(),
            profile_name: Mutex::new(String::new()),
            date_range: Mutex::new(String::new()),
            rate_history: Mutex::new(vec![0; 60]),
            total_requests: AtomicU32::new(0),
        }
    }

    /// Set profile name
    pub fn set_profile(&self, name: &str) {
        let mut profile = self.profile_name.lock().unwrap();
        *profile = name.to_string();
    }

    /// Get profile name
    pub fn get_profile(&self) -> String {
        self.profile_name.lock().unwrap().clone()
    }

    /// Set date range
    pub fn set_date_range(&self, from: &str, to: &str) {
        let mut range = self.date_range.lock().unwrap();
        *range = format!("{} -> {}", from, to);
    }

    /// Get date range
    pub fn get_date_range(&self) -> String {
        self.date_range.lock().unwrap().clone()
    }

    /// Record a request for rate tracking
    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Update rate history (call once per second)
    pub fn update_rate_history(&self) {
        let current = self.total_requests.load(Ordering::Relaxed);
        let mut history = self.rate_history.lock().unwrap();

        // Shift history left, add current rate
        if history.len() >= 60 {
            history.remove(0);
        }
        history.push(current);
    }

    /// Get rate per minute (average over last minute)
    pub fn requests_per_minute(&self) -> u32 {
        let history = self.rate_history.lock().unwrap();
        if history.len() < 2 {
            return 0;
        }
        let start = history.first().copied().unwrap_or(0);
        let end = history.last().copied().unwrap_or(0);
        end.saturating_sub(start)
    }

    /// Get elapsed time as formatted string
    pub fn elapsed_str(&self) -> String {
        let elapsed = self.start_time.elapsed();
        let secs = elapsed.as_secs();
        let mins = secs / 60;
        let remaining_secs = secs % 60;

        if mins > 0 {
            format!("{}m {}s", mins, remaining_secs)
        } else {
            format!("{}s", secs)
        }
    }

    /// Estimate time remaining
    pub fn eta_str(&self) -> String {
        let total = self.total_remaining();
        let completed = self.total_completed();

        if completed == 0 {
            return "calculating...".to_string();
        }

        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rate = completed as f64 / elapsed;

        if rate < 0.01 {
            return "unknown".to_string();
        }

        let remaining = total.saturating_sub(completed);
        let eta_secs = (remaining as f64 / rate) as u64;

        if eta_secs > 3600 {
            let hours = eta_secs / 3600;
            let mins = (eta_secs % 3600) / 60;
            format!("~{}h {}m", hours, mins)
        } else if eta_secs > 60 {
            let mins = eta_secs / 60;
            format!("~{} minutes", mins)
        } else {
            format!("~{} seconds", eta_secs)
        }
    }

    /// Get total items remaining across all streams
    pub fn total_remaining(&self) -> u32 {
        self.activities.get_total()
            + self.gpx.get_total()
            + self.health.get_total()
            + self.performance.get_total()
    }

    /// Get total completed across all streams
    pub fn total_completed(&self) -> u32 {
        self.activities.get_completed()
            + self.gpx.get_completed()
            + self.health.get_completed()
            + self.performance.get_completed()
    }

    /// Get total failed across all streams
    pub fn total_failed(&self) -> u32 {
        self.activities.get_failed()
            + self.gpx.get_failed()
            + self.health.get_failed()
            + self.performance.get_failed()
    }

    /// Check if all streams are complete
    pub fn is_complete(&self) -> bool {
        (self.activities.get_total() == 0 || self.activities.is_complete())
            && (self.gpx.get_total() == 0 || self.gpx.is_complete())
            && (self.health.get_total() == 0 || self.health.is_complete())
            && (self.performance.get_total() == 0 || self.performance.is_complete())
    }

    /// Print simple status line (for --simple mode)
    pub fn print_simple_status(&self) {
        let act = &self.activities;
        let gpx = &self.gpx;
        let health = &self.health;
        let perf = &self.performance;

        print!(
            "\rAct: {}/{} | GPX: {}/{} | Health: {}/{} | Perf: {}/{} | {} ",
            act.get_completed(),
            act.get_total(),
            gpx.get_completed(),
            gpx.get_total(),
            health.get_completed(),
            health.get_total(),
            perf.get_completed(),
            perf.get_total(),
            self.elapsed_str(),
        );
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

impl Default for SyncProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared progress wrapped in Arc for parallel access
pub type SharedProgress = Arc<SyncProgress>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_progress() {
        let progress = StreamProgress::new("Test");

        progress.set_total(100);
        assert_eq!(progress.get_total(), 100);
        assert_eq!(progress.percent(), 0);

        progress.complete_one();
        assert_eq!(progress.get_completed(), 1);
        assert_eq!(progress.percent(), 1);

        for _ in 0..49 {
            progress.complete_one();
        }
        assert_eq!(progress.percent(), 50);
    }

    #[test]
    fn test_sync_progress() {
        let progress = SyncProgress::new();

        progress.activities.set_total(10);
        progress.health.set_total(20);

        assert_eq!(progress.total_remaining(), 30);
        assert_eq!(progress.total_completed(), 0);

        progress.activities.complete_one();
        progress.health.complete_one();

        assert_eq!(progress.total_completed(), 2);
    }
}
