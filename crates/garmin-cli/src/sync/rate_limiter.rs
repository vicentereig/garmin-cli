//! Shared rate limiter with semaphore-based concurrency control for parallel sync

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Thread-safe rate limiter for parallel API requests
#[derive(Clone)]
pub struct SharedRateLimiter {
    /// Semaphore limits concurrent requests
    semaphore: Arc<Semaphore>,
    /// Minimum delay between requests (2000ms = ~30 req/min)
    min_delay: Duration,
    /// Last request timestamp
    last_request: Arc<Mutex<Instant>>,
    /// Current backoff duration
    backoff: Arc<Mutex<Duration>>,
    /// Maximum backoff duration
    max_backoff: Duration,
    /// Consecutive rate limit hits (for pause detection)
    consecutive_429s: Arc<AtomicU32>,
}

impl Default for SharedRateLimiter {
    fn default() -> Self {
        Self::new(3) // 3 concurrent by default
    }
}

impl SharedRateLimiter {
    /// Create a new shared rate limiter with given concurrency
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            min_delay: Duration::from_millis(2000),
            last_request: Arc::new(Mutex::new(Instant::now() - Duration::from_secs(10))),
            backoff: Arc::new(Mutex::new(Duration::from_secs(0))),
            max_backoff: Duration::from_secs(300), // 5 minutes
            consecutive_429s: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Acquire a permit and wait for rate limit, returns a guard
    pub async fn acquire(&self) -> RateLimitGuard {
        // First acquire a permit from the semaphore
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();

        // Then ensure minimum delay since last request
        let required_delay = {
            let last = self.last_request.lock().unwrap();
            let backoff = self.backoff.lock().unwrap();
            let elapsed = last.elapsed();
            let required = self.min_delay + *backoff;

            if elapsed < required {
                required - elapsed
            } else {
                Duration::ZERO
            }
        };

        if !required_delay.is_zero() {
            tokio::time::sleep(required_delay).await;
        }

        // Update last request time
        {
            let mut last = self.last_request.lock().unwrap();
            *last = Instant::now();
        }

        RateLimitGuard { _permit: permit }
    }

    /// Handle a successful request - reset backoff
    pub fn on_success(&self) {
        let mut backoff = self.backoff.lock().unwrap();
        *backoff = Duration::ZERO;
        self.consecutive_429s.store(0, Ordering::Relaxed);
    }

    /// Handle a rate limit (429) response - increase backoff
    pub fn on_rate_limit(&self) {
        self.consecutive_429s.fetch_add(1, Ordering::Relaxed);
        let mut backoff = self.backoff.lock().unwrap();
        let new_backoff = (*backoff * 2).max(Duration::from_secs(1));
        *backoff = new_backoff.min(self.max_backoff);
    }

    /// Check if we should pause sync due to repeated rate limits
    pub fn should_pause(&self) -> bool {
        self.consecutive_429s.load(Ordering::Relaxed) >= 5
    }

    /// Get the current backoff duration
    pub fn current_backoff(&self) -> Duration {
        *self.backoff.lock().unwrap()
    }

    /// Get pause duration (30 minutes after 5 consecutive 429s)
    pub fn pause_duration(&self) -> Duration {
        Duration::from_secs(1800)
    }
}

/// Guard that holds a rate limit permit
pub struct RateLimitGuard {
    _permit: OwnedSemaphorePermit,
}

// Legacy single-threaded rate limiter for backward compatibility
/// Rate limiter configuration (legacy, use SharedRateLimiter for parallel sync)
pub struct RateLimiter {
    /// Minimum delay between requests
    min_delay: Duration,
    /// Current backoff delay
    backoff: Duration,
    /// Maximum backoff delay
    max_backoff: Duration,
    /// Backoff multiplier
    backoff_multiplier: f64,
    /// Last request time
    last_request: Option<Instant>,
    /// Consecutive rate limit hits
    consecutive_429s: u32,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    /// Create a new rate limiter with conservative defaults
    pub fn new() -> Self {
        Self {
            min_delay: Duration::from_millis(2000), // 30 req/min
            backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300), // 5 minutes
            backoff_multiplier: 2.0,
            last_request: None,
            consecutive_429s: 0,
        }
    }

    /// Wait before making the next request
    pub async fn wait(&mut self) {
        if let Some(last) = self.last_request {
            let elapsed = last.elapsed();
            let required_delay = self.min_delay + self.backoff;

            if elapsed < required_delay {
                let wait_time = required_delay - elapsed;
                tokio::time::sleep(wait_time).await;
            }
        }
        self.last_request = Some(Instant::now());
    }

    /// Handle a successful request
    pub fn on_success(&mut self) {
        self.backoff = Duration::from_secs(1);
        self.consecutive_429s = 0;
    }

    /// Handle a rate limit (HTTP 429) response
    pub fn on_rate_limit(&mut self) {
        self.consecutive_429s += 1;
        self.backoff = Duration::from_secs_f64(
            (self.backoff.as_secs_f64() * self.backoff_multiplier)
                .min(self.max_backoff.as_secs_f64()),
        );
    }

    /// Check if we should pause sync due to repeated rate limits
    pub fn should_pause(&self) -> bool {
        self.consecutive_429s >= 5
    }

    /// Get the current backoff duration
    pub fn current_backoff(&self) -> Duration {
        self.backoff
    }

    /// Get pause duration (30 minutes after 5 consecutive 429s)
    pub fn pause_duration(&self) -> Duration {
        Duration::from_secs(1800) // 30 minutes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_defaults() {
        let limiter = RateLimiter::new();
        assert_eq!(limiter.min_delay, Duration::from_millis(2000));
        assert_eq!(limiter.backoff, Duration::from_secs(1));
    }

    #[test]
    fn test_exponential_backoff() {
        let mut limiter = RateLimiter::new();

        limiter.on_rate_limit();
        assert_eq!(limiter.backoff, Duration::from_secs(2));

        limiter.on_rate_limit();
        assert_eq!(limiter.backoff, Duration::from_secs(4));

        limiter.on_rate_limit();
        assert_eq!(limiter.backoff, Duration::from_secs(8));
    }

    #[test]
    fn test_backoff_max() {
        let mut limiter = RateLimiter::new();

        // Hit rate limit many times
        for _ in 0..20 {
            limiter.on_rate_limit();
        }

        // Should not exceed max_backoff
        assert!(limiter.backoff <= limiter.max_backoff);
    }

    #[test]
    fn test_reset_on_success() {
        let mut limiter = RateLimiter::new();

        limiter.on_rate_limit();
        limiter.on_rate_limit();
        assert!(limiter.backoff > Duration::from_secs(1));

        limiter.on_success();
        assert_eq!(limiter.backoff, Duration::from_secs(1));
        assert_eq!(limiter.consecutive_429s, 0);
    }

    #[test]
    fn test_should_pause() {
        let mut limiter = RateLimiter::new();

        for _ in 0..4 {
            limiter.on_rate_limit();
            assert!(!limiter.should_pause());
        }

        limiter.on_rate_limit();
        assert!(limiter.should_pause());
    }

    #[test]
    fn test_shared_rate_limiter() {
        let limiter = SharedRateLimiter::new(3);

        limiter.on_rate_limit();
        assert!(!limiter.should_pause());

        for _ in 0..4 {
            limiter.on_rate_limit();
        }
        assert!(limiter.should_pause());

        limiter.on_success();
        assert!(!limiter.should_pause());
    }
}
