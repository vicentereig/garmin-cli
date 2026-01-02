//! Storage layer for Garmin data
//!
//! This module provides time-partitioned Parquet storage for Garmin data,
//! enabling concurrent read access during sync operations.
//!
//! ## Architecture
//!
//! - **Parquet files**: Time-partitioned storage for activities, health, performance, etc.
//! - **SQLite**: Sync state and task queue for operational data
//!
//! ## Storage Layout
//!
//! ```text
//! ~/.local/share/garmin/
//! ├── sync.db                      # SQLite for sync state + task queue
//! ├── profiles.parquet             # Single file (small, rarely changes)
//! ├── activities/
//! │   ├── 2024-W48.parquet        # Weekly partitions
//! │   └── ...
//! ├── track_points/
//! │   ├── 2024-12-01.parquet      # Daily partitions
//! │   └── ...
//! ├── daily_health/
//! │   ├── 2024-12.parquet         # Monthly partitions
//! │   └── ...
//! ├── performance_metrics/
//! │   ├── 2024-12.parquet         # Monthly partitions
//! │   └── ...
//! └── weight/
//!     ├── 2024-12.parquet         # Monthly partitions
//!     └── ...
//! ```
//!
//! ## Concurrent Access
//!
//! Parquet files are written atomically (temp file + rename), so readers always
//! see consistent data. External apps can query data using DuckDB:
//!
//! ```sql
//! SELECT * FROM 'activities/*.parquet' WHERE start_time_local > '2024-12-01';
//! ```

mod parquet;
mod partitions;
mod sync_db;

pub use parquet::ParquetStore;
pub use partitions::EntityType;
pub use sync_db::SyncDb;

use std::path::PathBuf;

use crate::error::Result;

/// Get the default storage path
pub fn default_storage_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("garmin")
}

/// Get the default sync database path
pub fn default_sync_db_path() -> PathBuf {
    default_storage_path().join("sync.db")
}

/// Storage manager combining Parquet store and sync database
pub struct Storage {
    pub parquet: ParquetStore,
    pub sync_db: SyncDb,
}

impl Storage {
    /// Open storage at the default location
    pub fn open_default() -> Result<Self> {
        let base_path = default_storage_path();
        Self::open(base_path)
    }

    /// Open storage at a custom location
    pub fn open(base_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_path).map_err(|e| {
            crate::error::GarminError::Database(format!(
                "Failed to create storage directory: {}",
                e
            ))
        })?;

        let parquet = ParquetStore::new(&base_path);
        let sync_db = SyncDb::open(base_path.join("sync.db"))?;

        Ok(Self { parquet, sync_db })
    }

    /// Open in-memory storage (for testing)
    pub fn open_in_memory(temp_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&temp_path).map_err(|e| {
            crate::error::GarminError::Database(format!(
                "Failed to create temp directory: {}",
                e
            ))
        })?;

        let parquet = ParquetStore::new(&temp_path);
        let sync_db = SyncDb::open_in_memory()?;

        Ok(Self { parquet, sync_db })
    }

    /// Create a new storage handle that shares the Parquet store but opens a new SyncDb connection.
    pub fn clone_with_new_db(&self) -> Result<Self> {
        let parquet = self.parquet.clone();
        let sync_db = SyncDb::open(self.base_path().join("sync.db"))?;
        Ok(Self { parquet, sync_db })
    }

    /// Get the base path for external readers
    pub fn base_path(&self) -> &std::path::Path {
        self.parquet.base_path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_storage_open() {
        let temp = TempDir::new().unwrap();
        let storage = Storage::open(temp.path().to_path_buf()).unwrap();
        assert!(storage.base_path().exists());
    }
}
