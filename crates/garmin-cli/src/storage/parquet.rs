//! Parquet read/write utilities for time-partitioned storage
//!
//! Uses Arrow record batches for efficient columnar storage.
//! Supports concurrent writes to different partitions via partition-level locks.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate};
use dashmap::DashMap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::sync::Mutex as TokioMutex;

use crate::db::models::{Activity, DailyHealth, PerformanceMetrics, Profile, TrackPoint, WeightEntry};
use crate::error::{GarminError, Result};

use super::partitions::EntityType;

/// Parquet storage for Garmin data
///
/// Supports concurrent writes to different partitions. Each partition has its own
/// lock to prevent lost updates when multiple workers write to the same partition.
#[derive(Clone)]
pub struct ParquetStore {
    base_path: PathBuf,
    /// Per-partition locks for concurrent write safety
    partition_locks: Arc<DashMap<String, Arc<TokioMutex<()>>>>,
}

impl ParquetStore {
    /// Create a new ParquetStore at the given base path
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
            partition_locks: Arc::new(DashMap::new()),
        }
    }

    /// Get or create a lock for a specific partition
    fn get_partition_lock(&self, partition_key: &str) -> Arc<TokioMutex<()>> {
        self.partition_locks
            .entry(partition_key.to_string())
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
    }

    /// Check if daily health data exists for a profile/date
    pub fn has_daily_health(&self, profile_id: i32, date: NaiveDate) -> Result<bool> {
        let key = EntityType::DailyHealth.partition_key(date);
        let path = self.partition_path(EntityType::DailyHealth, &key);
        if !path.exists() {
            return Ok(false);
        }
        let records = self.read_daily_health_from_path(&path)?;
        Ok(records
            .iter()
            .any(|r| r.profile_id == profile_id && r.date == date))
    }

    /// Check if performance metrics exist for a profile/date
    pub fn has_performance_metrics(&self, profile_id: i32, date: NaiveDate) -> Result<bool> {
        let key = EntityType::PerformanceMetrics.partition_key(date);
        let path = self.partition_path(EntityType::PerformanceMetrics, &key);
        if !path.exists() {
            return Ok(false);
        }
        let records = self.read_performance_metrics_from_path(&path)?;
        Ok(records
            .iter()
            .any(|r| r.profile_id == profile_id && r.date == date))
    }

    /// Check if track points exist for an activity/date partition
    pub fn has_track_points(&self, activity_id: i64, date: NaiveDate) -> Result<bool> {
        let key = EntityType::TrackPoints.partition_key(date);
        let path = self.partition_path(EntityType::TrackPoints, &key);
        if !path.exists() {
            return Ok(false);
        }
        let records = self.read_track_points_from_path(&path)?;
        Ok(records.iter().any(|r| r.activity_id == activity_id))
    }

    /// Get the full path for a partition file
    pub fn partition_path(&self, entity: EntityType, partition_key: &str) -> PathBuf {
        match entity {
            EntityType::Profiles => self.base_path.join("profiles.parquet"),
            _ => self
                .base_path
                .join(entity.dir_name())
                .join(format!("{}.parquet", partition_key)),
        }
    }

    /// Ensure the directory for an entity exists
    fn ensure_dir(&self, entity: EntityType) -> Result<()> {
        if entity != EntityType::Profiles {
            let dir = self.base_path.join(entity.dir_name());
            fs::create_dir_all(&dir).map_err(|e| {
                GarminError::Database(format!("Failed to create directory {:?}: {}", dir, e))
            })?;
        } else {
            fs::create_dir_all(&self.base_path).map_err(|e| {
                GarminError::Database(format!(
                    "Failed to create directory {:?}: {}",
                    self.base_path, e
                ))
            })?;
        }
        Ok(())
    }

    /// Write a record batch to a partition file atomically
    fn write_batch(&self, path: &Path, batch: &RecordBatch) -> Result<()> {
        // Write to temp file first
        let temp_path = path.with_extension("parquet.tmp");

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                GarminError::Database(format!("Failed to create directory: {}", e))
            })?;
        }

        let file = File::create(&temp_path)
            .map_err(|e| GarminError::Database(format!("Failed to create temp file: {}", e)))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .map_err(|e| GarminError::Database(format!("Failed to create Parquet writer: {}", e)))?;

        writer
            .write(batch)
            .map_err(|e| GarminError::Database(format!("Failed to write batch: {}", e)))?;

        writer
            .close()
            .map_err(|e| GarminError::Database(format!("Failed to close writer: {}", e)))?;

        // Atomic rename
        fs::rename(&temp_path, path)
            .map_err(|e| GarminError::Database(format!("Failed to rename temp file: {}", e)))?;

        Ok(())
    }

    /// Read all record batches from a partition file
    fn read_batches(&self, path: &Path) -> Result<Vec<RecordBatch>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)
            .map_err(|e| GarminError::Database(format!("Failed to open file: {}", e)))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| GarminError::Database(format!("Failed to create reader: {}", e)))?
            .build()
            .map_err(|e| GarminError::Database(format!("Failed to build reader: {}", e)))?;

        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| GarminError::Database(format!("Failed to read batches: {}", e)))
    }

    // =========================================================================
    // Activities
    // =========================================================================

    /// Write activities to weekly partitions
    pub fn write_activities(&self, activities: &[Activity]) -> Result<()> {
        self.ensure_dir(EntityType::Activities)?;

        // Group by partition key
        let mut partitions: std::collections::HashMap<String, Vec<&Activity>> =
            std::collections::HashMap::new();

        for activity in activities {
            if let Some(start_time) = activity.start_time_local {
                let key = EntityType::Activities.partition_key(start_time.date_naive());
                partitions.entry(key).or_default().push(activity);
            }
        }

        // Write each partition
        for (key, partition_activities) in partitions {
            let path = self.partition_path(EntityType::Activities, &key);
            let batch = Self::activities_to_batch(partition_activities)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    /// Upsert activities (read existing, merge, write)
    pub fn upsert_activities(&self, activities: &[Activity]) -> Result<()> {
        self.ensure_dir(EntityType::Activities)?;

        // Group new activities by partition key
        let mut partitions: std::collections::HashMap<String, Vec<Activity>> =
            std::collections::HashMap::new();

        for activity in activities {
            if let Some(start_time) = activity.start_time_local {
                let key = EntityType::Activities.partition_key(start_time.date_naive());
                partitions.entry(key).or_default().push(activity.clone());
            }
        }

        // For each partition, read existing and merge
        for (key, mut new_activities) in partitions {
            let path = self.partition_path(EntityType::Activities, &key);

            // Read existing
            let mut existing = self.read_activities_from_path(&path)?;

            // Create set of new activity IDs for fast lookup
            let new_ids: std::collections::HashSet<i64> =
                new_activities.iter().map(|a| a.activity_id).collect();

            // Keep existing activities that aren't being replaced
            existing.retain(|a| !new_ids.contains(&a.activity_id));

            // Merge
            existing.append(&mut new_activities);

            // Sort by activity_id for consistent ordering
            existing.sort_by_key(|a| a.activity_id);

            // Write merged
            let refs: Vec<&Activity> = existing.iter().collect();
            let batch = Self::activities_to_batch(refs)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    /// Upsert activities with partition-level locking for concurrent writes
    pub async fn upsert_activities_async(&self, activities: &[Activity]) -> Result<()> {
        self.ensure_dir(EntityType::Activities)?;

        // Group new activities by partition key
        let mut partitions: std::collections::HashMap<String, Vec<Activity>> =
            std::collections::HashMap::new();

        for activity in activities {
            if let Some(start_time) = activity.start_time_local {
                let key = EntityType::Activities.partition_key(start_time.date_naive());
                partitions.entry(key).or_default().push(activity.clone());
            }
        }

        // For each partition, acquire lock then read/merge/write
        for (key, mut new_activities) in partitions {
            // Acquire partition lock
            let lock = self.get_partition_lock(&key);
            let _guard = lock.lock().await;

            let path = self.partition_path(EntityType::Activities, &key);

            // Read existing
            let mut existing = self.read_activities_from_path(&path)?;

            // Create set of new activity IDs for fast lookup
            let new_ids: std::collections::HashSet<i64> =
                new_activities.iter().map(|a| a.activity_id).collect();

            // Keep existing activities that aren't being replaced
            existing.retain(|a| !new_ids.contains(&a.activity_id));

            // Merge
            existing.append(&mut new_activities);

            // Sort by activity_id for consistent ordering
            existing.sort_by_key(|a| a.activity_id);

            // Write merged
            let refs: Vec<&Activity> = existing.iter().collect();
            let batch = Self::activities_to_batch(refs)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    fn read_activities_from_path(&self, path: &Path) -> Result<Vec<Activity>> {
        let batches = self.read_batches(path)?;
        let mut activities = Vec::new();

        for batch in batches {
            activities.extend(Self::batch_to_activities(&batch)?);
        }

        Ok(activities)
    }

    fn activities_to_batch(activities: Vec<&Activity>) -> Result<RecordBatch> {
        let activity_id: Int64Array = activities.iter().map(|a| a.activity_id).collect();
        let profile_id: Int32Array = activities.iter().map(|a| a.profile_id).collect();
        let activity_name: StringArray = activities.iter().map(|a| a.activity_name.as_deref()).collect();
        let activity_type: StringArray = activities.iter().map(|a| a.activity_type.as_deref()).collect();
        let start_time_local: TimestampMicrosecondArray = activities
            .iter()
            .map(|a| a.start_time_local.map(|t| t.timestamp_micros()))
            .collect();
        let start_time_gmt: TimestampMicrosecondArray = activities
            .iter()
            .map(|a| a.start_time_gmt.map(|t| t.timestamp_micros()))
            .collect();
        let duration_sec: Float64Array = activities.iter().map(|a| a.duration_sec).collect();
        let distance_m: Float64Array = activities.iter().map(|a| a.distance_m).collect();
        let calories: Int32Array = activities.iter().map(|a| a.calories).collect();
        let avg_hr: Int32Array = activities.iter().map(|a| a.avg_hr).collect();
        let max_hr: Int32Array = activities.iter().map(|a| a.max_hr).collect();
        let avg_speed: Float64Array = activities.iter().map(|a| a.avg_speed).collect();
        let max_speed: Float64Array = activities.iter().map(|a| a.max_speed).collect();
        let elevation_gain: Float64Array = activities.iter().map(|a| a.elevation_gain).collect();
        let elevation_loss: Float64Array = activities.iter().map(|a| a.elevation_loss).collect();
        let avg_cadence: Float64Array = activities.iter().map(|a| a.avg_cadence).collect();
        let avg_power: Int32Array = activities.iter().map(|a| a.avg_power).collect();
        let normalized_power: Int32Array = activities.iter().map(|a| a.normalized_power).collect();
        let training_effect: Float64Array = activities.iter().map(|a| a.training_effect).collect();
        let training_load: Float64Array = activities.iter().map(|a| a.training_load).collect();
        let start_lat: Float64Array = activities.iter().map(|a| a.start_lat).collect();
        let start_lon: Float64Array = activities.iter().map(|a| a.start_lon).collect();
        let end_lat: Float64Array = activities.iter().map(|a| a.end_lat).collect();
        let end_lon: Float64Array = activities.iter().map(|a| a.end_lon).collect();
        let ground_contact_time: Float64Array = activities.iter().map(|a| a.ground_contact_time).collect();
        let vertical_oscillation: Float64Array = activities.iter().map(|a| a.vertical_oscillation).collect();
        let stride_length: Float64Array = activities.iter().map(|a| a.stride_length).collect();
        let location_name: StringArray = activities.iter().map(|a| a.location_name.as_deref()).collect();
        let raw_json: StringArray = activities
            .iter()
            .map(|a| a.raw_json.as_ref().map(|j| j.to_string()))
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("activity_id", DataType::Int64, false),
            Field::new("profile_id", DataType::Int32, false),
            Field::new("activity_name", DataType::Utf8, true),
            Field::new("activity_type", DataType::Utf8, true),
            Field::new("start_time_local", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), true),
            Field::new("start_time_gmt", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), true),
            Field::new("duration_sec", DataType::Float64, true),
            Field::new("distance_m", DataType::Float64, true),
            Field::new("calories", DataType::Int32, true),
            Field::new("avg_hr", DataType::Int32, true),
            Field::new("max_hr", DataType::Int32, true),
            Field::new("avg_speed", DataType::Float64, true),
            Field::new("max_speed", DataType::Float64, true),
            Field::new("elevation_gain", DataType::Float64, true),
            Field::new("elevation_loss", DataType::Float64, true),
            Field::new("avg_cadence", DataType::Float64, true),
            Field::new("avg_power", DataType::Int32, true),
            Field::new("normalized_power", DataType::Int32, true),
            Field::new("training_effect", DataType::Float64, true),
            Field::new("training_load", DataType::Float64, true),
            Field::new("start_lat", DataType::Float64, true),
            Field::new("start_lon", DataType::Float64, true),
            Field::new("end_lat", DataType::Float64, true),
            Field::new("end_lon", DataType::Float64, true),
            Field::new("ground_contact_time", DataType::Float64, true),
            Field::new("vertical_oscillation", DataType::Float64, true),
            Field::new("stride_length", DataType::Float64, true),
            Field::new("location_name", DataType::Utf8, true),
            Field::new("raw_json", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(activity_id),
                Arc::new(profile_id),
                Arc::new(activity_name),
                Arc::new(activity_type),
                Arc::new(start_time_local),
                Arc::new(start_time_gmt),
                Arc::new(duration_sec),
                Arc::new(distance_m),
                Arc::new(calories),
                Arc::new(avg_hr),
                Arc::new(max_hr),
                Arc::new(avg_speed),
                Arc::new(max_speed),
                Arc::new(elevation_gain),
                Arc::new(elevation_loss),
                Arc::new(avg_cadence),
                Arc::new(avg_power),
                Arc::new(normalized_power),
                Arc::new(training_effect),
                Arc::new(training_load),
                Arc::new(start_lat),
                Arc::new(start_lon),
                Arc::new(end_lat),
                Arc::new(end_lon),
                Arc::new(ground_contact_time),
                Arc::new(vertical_oscillation),
                Arc::new(stride_length),
                Arc::new(location_name),
                Arc::new(raw_json),
            ],
        )
        .map_err(|e| GarminError::Database(format!("Failed to create record batch: {}", e)))
    }

    fn batch_to_activities(batch: &RecordBatch) -> Result<Vec<Activity>> {
        let len = batch.num_rows();
        let mut activities = Vec::with_capacity(len);

        let activity_id = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let profile_id = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let activity_name = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let activity_type = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let start_time_local = batch.column(4).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let start_time_gmt = batch.column(5).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let duration_sec = batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
        let distance_m = batch.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        let calories = batch.column(8).as_any().downcast_ref::<Int32Array>().unwrap();
        let avg_hr = batch.column(9).as_any().downcast_ref::<Int32Array>().unwrap();
        let max_hr = batch.column(10).as_any().downcast_ref::<Int32Array>().unwrap();
        let avg_speed = batch.column(11).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_speed = batch.column(12).as_any().downcast_ref::<Float64Array>().unwrap();
        let elevation_gain = batch.column(13).as_any().downcast_ref::<Float64Array>().unwrap();
        let elevation_loss = batch.column(14).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_cadence = batch.column(15).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_power = batch.column(16).as_any().downcast_ref::<Int32Array>().unwrap();
        let normalized_power = batch.column(17).as_any().downcast_ref::<Int32Array>().unwrap();
        let training_effect = batch.column(18).as_any().downcast_ref::<Float64Array>().unwrap();
        let training_load = batch.column(19).as_any().downcast_ref::<Float64Array>().unwrap();
        let start_lat = batch.column(20).as_any().downcast_ref::<Float64Array>().unwrap();
        let start_lon = batch.column(21).as_any().downcast_ref::<Float64Array>().unwrap();
        let end_lat = batch.column(22).as_any().downcast_ref::<Float64Array>().unwrap();
        let end_lon = batch.column(23).as_any().downcast_ref::<Float64Array>().unwrap();
        let ground_contact_time = batch.column(24).as_any().downcast_ref::<Float64Array>().unwrap();
        let vertical_oscillation = batch.column(25).as_any().downcast_ref::<Float64Array>().unwrap();
        let stride_length = batch.column(26).as_any().downcast_ref::<Float64Array>().unwrap();
        let location_name = batch.column(27).as_any().downcast_ref::<StringArray>().unwrap();
        let raw_json = batch.column(28).as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..len {
            activities.push(Activity {
                activity_id: activity_id.value(i),
                profile_id: profile_id.value(i),
                activity_name: activity_name.is_valid(i).then(|| activity_name.value(i).to_string()),
                activity_type: activity_type.is_valid(i).then(|| activity_type.value(i).to_string()),
                start_time_local: start_time_local.is_valid(i).then(|| {
                    DateTime::from_timestamp_micros(start_time_local.value(i)).unwrap_or_default()
                }),
                start_time_gmt: start_time_gmt.is_valid(i).then(|| {
                    DateTime::from_timestamp_micros(start_time_gmt.value(i)).unwrap_or_default()
                }),
                duration_sec: duration_sec.is_valid(i).then(|| duration_sec.value(i)),
                distance_m: distance_m.is_valid(i).then(|| distance_m.value(i)),
                calories: calories.is_valid(i).then(|| calories.value(i)),
                avg_hr: avg_hr.is_valid(i).then(|| avg_hr.value(i)),
                max_hr: max_hr.is_valid(i).then(|| max_hr.value(i)),
                avg_speed: avg_speed.is_valid(i).then(|| avg_speed.value(i)),
                max_speed: max_speed.is_valid(i).then(|| max_speed.value(i)),
                elevation_gain: elevation_gain.is_valid(i).then(|| elevation_gain.value(i)),
                elevation_loss: elevation_loss.is_valid(i).then(|| elevation_loss.value(i)),
                avg_cadence: avg_cadence.is_valid(i).then(|| avg_cadence.value(i)),
                avg_power: avg_power.is_valid(i).then(|| avg_power.value(i)),
                normalized_power: normalized_power.is_valid(i).then(|| normalized_power.value(i)),
                training_effect: training_effect.is_valid(i).then(|| training_effect.value(i)),
                training_load: training_load.is_valid(i).then(|| training_load.value(i)),
                start_lat: start_lat.is_valid(i).then(|| start_lat.value(i)),
                start_lon: start_lon.is_valid(i).then(|| start_lon.value(i)),
                end_lat: end_lat.is_valid(i).then(|| end_lat.value(i)),
                end_lon: end_lon.is_valid(i).then(|| end_lon.value(i)),
                ground_contact_time: ground_contact_time.is_valid(i).then(|| ground_contact_time.value(i)),
                vertical_oscillation: vertical_oscillation.is_valid(i).then(|| vertical_oscillation.value(i)),
                stride_length: stride_length.is_valid(i).then(|| stride_length.value(i)),
                location_name: location_name.is_valid(i).then(|| location_name.value(i).to_string()),
                raw_json: raw_json.is_valid(i).then(|| {
                    serde_json::from_str(raw_json.value(i)).unwrap_or_default()
                }),
            });
        }

        Ok(activities)
    }

    // =========================================================================
    // Daily Health
    // =========================================================================

    /// Upsert daily health records
    pub fn upsert_daily_health(&self, records: &[DailyHealth]) -> Result<()> {
        self.ensure_dir(EntityType::DailyHealth)?;

        // Group by partition key
        let mut partitions: std::collections::HashMap<String, Vec<DailyHealth>> =
            std::collections::HashMap::new();

        for record in records {
            let key = EntityType::DailyHealth.partition_key(record.date);
            partitions.entry(key).or_default().push(record.clone());
        }

        // For each partition, read existing and merge
        for (key, mut new_records) in partitions {
            let path = self.partition_path(EntityType::DailyHealth, &key);

            // Read existing
            let mut existing = self.read_daily_health_from_path(&path)?;

            // Create set of new dates for fast lookup
            let new_dates: std::collections::HashSet<(i32, NaiveDate)> = new_records
                .iter()
                .map(|r| (r.profile_id, r.date))
                .collect();

            // Keep existing records that aren't being replaced
            existing.retain(|r| !new_dates.contains(&(r.profile_id, r.date)));

            // Merge
            existing.append(&mut new_records);

            // Sort by date for consistent ordering
            existing.sort_by_key(|r| (r.profile_id, r.date));

            // Write merged
            let refs: Vec<&DailyHealth> = existing.iter().collect();
            let batch = Self::daily_health_to_batch(refs)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    /// Upsert daily health records with partition-level locking for concurrent writes
    pub async fn upsert_daily_health_async(&self, records: &[DailyHealth]) -> Result<()> {
        self.ensure_dir(EntityType::DailyHealth)?;

        // Group by partition key
        let mut partitions: std::collections::HashMap<String, Vec<DailyHealth>> =
            std::collections::HashMap::new();

        for record in records {
            let key = EntityType::DailyHealth.partition_key(record.date);
            partitions.entry(key).or_default().push(record.clone());
        }

        // For each partition, acquire lock then read/merge/write
        for (key, mut new_records) in partitions {
            // Acquire partition lock
            let lock = self.get_partition_lock(&key);
            let _guard = lock.lock().await;

            let path = self.partition_path(EntityType::DailyHealth, &key);

            // Read existing
            let mut existing = self.read_daily_health_from_path(&path)?;

            // Create set of new dates for fast lookup
            let new_dates: std::collections::HashSet<(i32, NaiveDate)> = new_records
                .iter()
                .map(|r| (r.profile_id, r.date))
                .collect();

            // Keep existing records that aren't being replaced
            existing.retain(|r| !new_dates.contains(&(r.profile_id, r.date)));

            // Merge
            existing.append(&mut new_records);

            // Sort by date for consistent ordering
            existing.sort_by_key(|r| (r.profile_id, r.date));

            // Write merged
            let refs: Vec<&DailyHealth> = existing.iter().collect();
            let batch = Self::daily_health_to_batch(refs)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    fn read_daily_health_from_path(&self, path: &Path) -> Result<Vec<DailyHealth>> {
        let batches = self.read_batches(path)?;
        let mut records = Vec::new();

        for batch in batches {
            records.extend(Self::batch_to_daily_health(&batch)?);
        }

        Ok(records)
    }

    fn daily_health_to_batch(records: Vec<&DailyHealth>) -> Result<RecordBatch> {
        let id: Int64Array = records.iter().map(|r| r.id).collect();
        let profile_id: Int32Array = records.iter().map(|r| r.profile_id).collect();
        let date: Date32Array = records
            .iter()
            .map(|r| {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Some((r.date - epoch).num_days() as i32)
            })
            .collect();
        let steps: Int32Array = records.iter().map(|r| r.steps).collect();
        let step_goal: Int32Array = records.iter().map(|r| r.step_goal).collect();
        let total_calories: Int32Array = records.iter().map(|r| r.total_calories).collect();
        let active_calories: Int32Array = records.iter().map(|r| r.active_calories).collect();
        let bmr_calories: Int32Array = records.iter().map(|r| r.bmr_calories).collect();
        let resting_hr: Int32Array = records.iter().map(|r| r.resting_hr).collect();
        let sleep_seconds: Int32Array = records.iter().map(|r| r.sleep_seconds).collect();
        let deep_sleep_seconds: Int32Array = records.iter().map(|r| r.deep_sleep_seconds).collect();
        let light_sleep_seconds: Int32Array = records.iter().map(|r| r.light_sleep_seconds).collect();
        let rem_sleep_seconds: Int32Array = records.iter().map(|r| r.rem_sleep_seconds).collect();
        let sleep_score: Int32Array = records.iter().map(|r| r.sleep_score).collect();
        let avg_stress: Int32Array = records.iter().map(|r| r.avg_stress).collect();
        let max_stress: Int32Array = records.iter().map(|r| r.max_stress).collect();
        let body_battery_start: Int32Array = records.iter().map(|r| r.body_battery_start).collect();
        let body_battery_end: Int32Array = records.iter().map(|r| r.body_battery_end).collect();
        let hrv_weekly_avg: Int32Array = records.iter().map(|r| r.hrv_weekly_avg).collect();
        let hrv_last_night: Int32Array = records.iter().map(|r| r.hrv_last_night).collect();
        let hrv_status: StringArray = records.iter().map(|r| r.hrv_status.as_deref()).collect();
        let avg_respiration: Float64Array = records.iter().map(|r| r.avg_respiration).collect();
        let avg_spo2: Int32Array = records.iter().map(|r| r.avg_spo2).collect();
        let lowest_spo2: Int32Array = records.iter().map(|r| r.lowest_spo2).collect();
        let hydration_ml: Int32Array = records.iter().map(|r| r.hydration_ml).collect();
        let moderate_intensity_min: Int32Array = records.iter().map(|r| r.moderate_intensity_min).collect();
        let vigorous_intensity_min: Int32Array = records.iter().map(|r| r.vigorous_intensity_min).collect();
        let raw_json: StringArray = records
            .iter()
            .map(|r| r.raw_json.as_ref().map(|j| j.to_string()))
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("profile_id", DataType::Int32, false),
            Field::new("date", DataType::Date32, false),
            Field::new("steps", DataType::Int32, true),
            Field::new("step_goal", DataType::Int32, true),
            Field::new("total_calories", DataType::Int32, true),
            Field::new("active_calories", DataType::Int32, true),
            Field::new("bmr_calories", DataType::Int32, true),
            Field::new("resting_hr", DataType::Int32, true),
            Field::new("sleep_seconds", DataType::Int32, true),
            Field::new("deep_sleep_seconds", DataType::Int32, true),
            Field::new("light_sleep_seconds", DataType::Int32, true),
            Field::new("rem_sleep_seconds", DataType::Int32, true),
            Field::new("sleep_score", DataType::Int32, true),
            Field::new("avg_stress", DataType::Int32, true),
            Field::new("max_stress", DataType::Int32, true),
            Field::new("body_battery_start", DataType::Int32, true),
            Field::new("body_battery_end", DataType::Int32, true),
            Field::new("hrv_weekly_avg", DataType::Int32, true),
            Field::new("hrv_last_night", DataType::Int32, true),
            Field::new("hrv_status", DataType::Utf8, true),
            Field::new("avg_respiration", DataType::Float64, true),
            Field::new("avg_spo2", DataType::Int32, true),
            Field::new("lowest_spo2", DataType::Int32, true),
            Field::new("hydration_ml", DataType::Int32, true),
            Field::new("moderate_intensity_min", DataType::Int32, true),
            Field::new("vigorous_intensity_min", DataType::Int32, true),
            Field::new("raw_json", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id),
                Arc::new(profile_id),
                Arc::new(date),
                Arc::new(steps),
                Arc::new(step_goal),
                Arc::new(total_calories),
                Arc::new(active_calories),
                Arc::new(bmr_calories),
                Arc::new(resting_hr),
                Arc::new(sleep_seconds),
                Arc::new(deep_sleep_seconds),
                Arc::new(light_sleep_seconds),
                Arc::new(rem_sleep_seconds),
                Arc::new(sleep_score),
                Arc::new(avg_stress),
                Arc::new(max_stress),
                Arc::new(body_battery_start),
                Arc::new(body_battery_end),
                Arc::new(hrv_weekly_avg),
                Arc::new(hrv_last_night),
                Arc::new(hrv_status),
                Arc::new(avg_respiration),
                Arc::new(avg_spo2),
                Arc::new(lowest_spo2),
                Arc::new(hydration_ml),
                Arc::new(moderate_intensity_min),
                Arc::new(vigorous_intensity_min),
                Arc::new(raw_json),
            ],
        )
        .map_err(|e| GarminError::Database(format!("Failed to create record batch: {}", e)))
    }

    fn batch_to_daily_health(batch: &RecordBatch) -> Result<Vec<DailyHealth>> {
        let len = batch.num_rows();
        let mut records = Vec::with_capacity(len);

        let id = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let profile_id = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let date = batch.column(2).as_any().downcast_ref::<Date32Array>().unwrap();
        let steps = batch.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
        let step_goal = batch.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
        let total_calories = batch.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
        let active_calories = batch.column(6).as_any().downcast_ref::<Int32Array>().unwrap();
        let bmr_calories = batch.column(7).as_any().downcast_ref::<Int32Array>().unwrap();
        let resting_hr = batch.column(8).as_any().downcast_ref::<Int32Array>().unwrap();
        let sleep_seconds = batch.column(9).as_any().downcast_ref::<Int32Array>().unwrap();
        let deep_sleep_seconds = batch.column(10).as_any().downcast_ref::<Int32Array>().unwrap();
        let light_sleep_seconds = batch.column(11).as_any().downcast_ref::<Int32Array>().unwrap();
        let rem_sleep_seconds = batch.column(12).as_any().downcast_ref::<Int32Array>().unwrap();
        let sleep_score = batch.column(13).as_any().downcast_ref::<Int32Array>().unwrap();
        let avg_stress = batch.column(14).as_any().downcast_ref::<Int32Array>().unwrap();
        let max_stress = batch.column(15).as_any().downcast_ref::<Int32Array>().unwrap();
        let body_battery_start = batch.column(16).as_any().downcast_ref::<Int32Array>().unwrap();
        let body_battery_end = batch.column(17).as_any().downcast_ref::<Int32Array>().unwrap();
        let hrv_weekly_avg = batch.column(18).as_any().downcast_ref::<Int32Array>().unwrap();
        let hrv_last_night = batch.column(19).as_any().downcast_ref::<Int32Array>().unwrap();
        let hrv_status = batch.column(20).as_any().downcast_ref::<StringArray>().unwrap();
        let avg_respiration = batch.column(21).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_spo2 = batch.column(22).as_any().downcast_ref::<Int32Array>().unwrap();
        let lowest_spo2 = batch.column(23).as_any().downcast_ref::<Int32Array>().unwrap();
        let hydration_ml = batch.column(24).as_any().downcast_ref::<Int32Array>().unwrap();
        let moderate_intensity_min = batch.column(25).as_any().downcast_ref::<Int32Array>().unwrap();
        let vigorous_intensity_min = batch.column(26).as_any().downcast_ref::<Int32Array>().unwrap();
        let raw_json = batch.column(27).as_any().downcast_ref::<StringArray>().unwrap();

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        for i in 0..len {
            records.push(DailyHealth {
                id: id.is_valid(i).then(|| id.value(i)),
                profile_id: profile_id.value(i),
                date: epoch + chrono::Duration::days(date.value(i) as i64),
                steps: steps.is_valid(i).then(|| steps.value(i)),
                step_goal: step_goal.is_valid(i).then(|| step_goal.value(i)),
                total_calories: total_calories.is_valid(i).then(|| total_calories.value(i)),
                active_calories: active_calories.is_valid(i).then(|| active_calories.value(i)),
                bmr_calories: bmr_calories.is_valid(i).then(|| bmr_calories.value(i)),
                resting_hr: resting_hr.is_valid(i).then(|| resting_hr.value(i)),
                sleep_seconds: sleep_seconds.is_valid(i).then(|| sleep_seconds.value(i)),
                deep_sleep_seconds: deep_sleep_seconds.is_valid(i).then(|| deep_sleep_seconds.value(i)),
                light_sleep_seconds: light_sleep_seconds.is_valid(i).then(|| light_sleep_seconds.value(i)),
                rem_sleep_seconds: rem_sleep_seconds.is_valid(i).then(|| rem_sleep_seconds.value(i)),
                sleep_score: sleep_score.is_valid(i).then(|| sleep_score.value(i)),
                avg_stress: avg_stress.is_valid(i).then(|| avg_stress.value(i)),
                max_stress: max_stress.is_valid(i).then(|| max_stress.value(i)),
                body_battery_start: body_battery_start.is_valid(i).then(|| body_battery_start.value(i)),
                body_battery_end: body_battery_end.is_valid(i).then(|| body_battery_end.value(i)),
                hrv_weekly_avg: hrv_weekly_avg.is_valid(i).then(|| hrv_weekly_avg.value(i)),
                hrv_last_night: hrv_last_night.is_valid(i).then(|| hrv_last_night.value(i)),
                hrv_status: hrv_status.is_valid(i).then(|| hrv_status.value(i).to_string()),
                avg_respiration: avg_respiration.is_valid(i).then(|| avg_respiration.value(i)),
                avg_spo2: avg_spo2.is_valid(i).then(|| avg_spo2.value(i)),
                lowest_spo2: lowest_spo2.is_valid(i).then(|| lowest_spo2.value(i)),
                hydration_ml: hydration_ml.is_valid(i).then(|| hydration_ml.value(i)),
                moderate_intensity_min: moderate_intensity_min.is_valid(i).then(|| moderate_intensity_min.value(i)),
                vigorous_intensity_min: vigorous_intensity_min.is_valid(i).then(|| vigorous_intensity_min.value(i)),
                raw_json: raw_json.is_valid(i).then(|| {
                    serde_json::from_str(raw_json.value(i)).unwrap_or_default()
                }),
            });
        }

        Ok(records)
    }

    // =========================================================================
    // Track Points
    // =========================================================================

    /// Write track points for an activity
    pub fn write_track_points(&self, activity_date: NaiveDate, points: &[TrackPoint]) -> Result<()> {
        self.ensure_dir(EntityType::TrackPoints)?;

        let key = EntityType::TrackPoints.partition_key(activity_date);
        let path = self.partition_path(EntityType::TrackPoints, &key);

        // Read existing, filter out points for the same activity, add new
        let mut existing = self.read_track_points_from_path(&path)?;

        if let Some(first) = points.first() {
            existing.retain(|p| p.activity_id != first.activity_id);
        }

        existing.extend(points.iter().cloned());

        // Sort by activity_id, then timestamp
        existing.sort_by(|a, b| {
            a.activity_id
                .cmp(&b.activity_id)
                .then(a.timestamp.cmp(&b.timestamp))
        });

        let refs: Vec<&TrackPoint> = existing.iter().collect();
        let batch = Self::track_points_to_batch(refs)?;
        self.write_batch(&path, &batch)?;

        Ok(())
    }

    /// Write track points with partition-level locking for concurrent writes
    pub async fn write_track_points_async(
        &self,
        activity_date: NaiveDate,
        points: &[TrackPoint],
    ) -> Result<()> {
        self.ensure_dir(EntityType::TrackPoints)?;

        let key = EntityType::TrackPoints.partition_key(activity_date);

        // Acquire partition lock
        let lock = self.get_partition_lock(&key);
        let _guard = lock.lock().await;

        let path = self.partition_path(EntityType::TrackPoints, &key);

        // Read existing, filter out points for the same activity, add new
        let mut existing = self.read_track_points_from_path(&path)?;

        if let Some(first) = points.first() {
            existing.retain(|p| p.activity_id != first.activity_id);
        }

        existing.extend(points.iter().cloned());

        // Sort by activity_id, then timestamp
        existing.sort_by(|a, b| {
            a.activity_id
                .cmp(&b.activity_id)
                .then(a.timestamp.cmp(&b.timestamp))
        });

        let refs: Vec<&TrackPoint> = existing.iter().collect();
        let batch = Self::track_points_to_batch(refs)?;
        self.write_batch(&path, &batch)?;

        Ok(())
    }

    fn read_track_points_from_path(&self, path: &Path) -> Result<Vec<TrackPoint>> {
        let batches = self.read_batches(path)?;
        let mut points = Vec::new();

        for batch in batches {
            points.extend(Self::batch_to_track_points(&batch)?);
        }

        Ok(points)
    }

    fn track_points_to_batch(points: Vec<&TrackPoint>) -> Result<RecordBatch> {
        let id: Int64Array = points.iter().map(|p| p.id).collect();
        let activity_id: Int64Array = points.iter().map(|p| p.activity_id).collect();
        let timestamp: TimestampMicrosecondArray = points
            .iter()
            .map(|p| Some(p.timestamp.timestamp_micros()))
            .collect();
        let lat: Float64Array = points.iter().map(|p| p.lat).collect();
        let lon: Float64Array = points.iter().map(|p| p.lon).collect();
        let elevation: Float64Array = points.iter().map(|p| p.elevation).collect();
        let heart_rate: Int32Array = points.iter().map(|p| p.heart_rate).collect();
        let cadence: Int32Array = points.iter().map(|p| p.cadence).collect();
        let power: Int32Array = points.iter().map(|p| p.power).collect();
        let speed: Float64Array = points.iter().map(|p| p.speed).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("activity_id", DataType::Int64, false),
            Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
            Field::new("lat", DataType::Float64, true),
            Field::new("lon", DataType::Float64, true),
            Field::new("elevation", DataType::Float64, true),
            Field::new("heart_rate", DataType::Int32, true),
            Field::new("cadence", DataType::Int32, true),
            Field::new("power", DataType::Int32, true),
            Field::new("speed", DataType::Float64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id),
                Arc::new(activity_id),
                Arc::new(timestamp),
                Arc::new(lat),
                Arc::new(lon),
                Arc::new(elevation),
                Arc::new(heart_rate),
                Arc::new(cadence),
                Arc::new(power),
                Arc::new(speed),
            ],
        )
        .map_err(|e| GarminError::Database(format!("Failed to create record batch: {}", e)))
    }

    fn batch_to_track_points(batch: &RecordBatch) -> Result<Vec<TrackPoint>> {
        let len = batch.num_rows();
        let mut points = Vec::with_capacity(len);

        let id = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let activity_id = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let timestamp = batch.column(2).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let lat = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let lon = batch.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let elevation = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let heart_rate = batch.column(6).as_any().downcast_ref::<Int32Array>().unwrap();
        let cadence = batch.column(7).as_any().downcast_ref::<Int32Array>().unwrap();
        let power = batch.column(8).as_any().downcast_ref::<Int32Array>().unwrap();
        let speed = batch.column(9).as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..len {
            points.push(TrackPoint {
                id: id.is_valid(i).then(|| id.value(i)),
                activity_id: activity_id.value(i),
                timestamp: DateTime::from_timestamp_micros(timestamp.value(i)).unwrap_or_default(),
                lat: lat.is_valid(i).then(|| lat.value(i)),
                lon: lon.is_valid(i).then(|| lon.value(i)),
                elevation: elevation.is_valid(i).then(|| elevation.value(i)),
                heart_rate: heart_rate.is_valid(i).then(|| heart_rate.value(i)),
                cadence: cadence.is_valid(i).then(|| cadence.value(i)),
                power: power.is_valid(i).then(|| power.value(i)),
                speed: speed.is_valid(i).then(|| speed.value(i)),
            });
        }

        Ok(points)
    }

    // =========================================================================
    // Performance Metrics
    // =========================================================================

    /// Upsert performance metrics records
    pub fn upsert_performance_metrics(&self, records: &[PerformanceMetrics]) -> Result<()> {
        self.ensure_dir(EntityType::PerformanceMetrics)?;

        // Group by partition key
        let mut partitions: std::collections::HashMap<String, Vec<PerformanceMetrics>> =
            std::collections::HashMap::new();

        for record in records {
            let key = EntityType::PerformanceMetrics.partition_key(record.date);
            partitions.entry(key).or_default().push(record.clone());
        }

        // For each partition, read existing and merge
        for (key, mut new_records) in partitions {
            let path = self.partition_path(EntityType::PerformanceMetrics, &key);

            // Read existing
            let mut existing = self.read_performance_metrics_from_path(&path)?;

            // Create set of new dates for fast lookup
            let new_dates: std::collections::HashSet<(i32, NaiveDate)> = new_records
                .iter()
                .map(|r| (r.profile_id, r.date))
                .collect();

            // Keep existing records that aren't being replaced
            existing.retain(|r| !new_dates.contains(&(r.profile_id, r.date)));

            // Merge
            existing.append(&mut new_records);

            // Sort by date for consistent ordering
            existing.sort_by_key(|r| (r.profile_id, r.date));

            // Write merged
            let refs: Vec<&PerformanceMetrics> = existing.iter().collect();
            let batch = Self::performance_metrics_to_batch(refs)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    /// Upsert performance metrics records with partition-level locking for concurrent writes
    pub async fn upsert_performance_metrics_async(&self, records: &[PerformanceMetrics]) -> Result<()> {
        self.ensure_dir(EntityType::PerformanceMetrics)?;

        // Group by partition key
        let mut partitions: std::collections::HashMap<String, Vec<PerformanceMetrics>> =
            std::collections::HashMap::new();

        for record in records {
            let key = EntityType::PerformanceMetrics.partition_key(record.date);
            partitions.entry(key).or_default().push(record.clone());
        }

        // For each partition, acquire lock then read/merge/write
        for (key, mut new_records) in partitions {
            // Acquire partition lock
            let lock = self.get_partition_lock(&key);
            let _guard = lock.lock().await;

            let path = self.partition_path(EntityType::PerformanceMetrics, &key);

            // Read existing
            let mut existing = self.read_performance_metrics_from_path(&path)?;

            // Create set of new dates for fast lookup
            let new_dates: std::collections::HashSet<(i32, NaiveDate)> = new_records
                .iter()
                .map(|r| (r.profile_id, r.date))
                .collect();

            // Keep existing records that aren't being replaced
            existing.retain(|r| !new_dates.contains(&(r.profile_id, r.date)));

            // Merge
            existing.append(&mut new_records);

            // Sort by date for consistent ordering
            existing.sort_by_key(|r| (r.profile_id, r.date));

            // Write merged
            let refs: Vec<&PerformanceMetrics> = existing.iter().collect();
            let batch = Self::performance_metrics_to_batch(refs)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    fn read_performance_metrics_from_path(&self, path: &Path) -> Result<Vec<PerformanceMetrics>> {
        let batches = self.read_batches(path)?;
        let mut records = Vec::new();

        for batch in batches {
            records.extend(Self::batch_to_performance_metrics(&batch)?);
        }

        Ok(records)
    }

    fn performance_metrics_to_batch(records: Vec<&PerformanceMetrics>) -> Result<RecordBatch> {
        let id: Int64Array = records.iter().map(|r| r.id).collect();
        let profile_id: Int32Array = records.iter().map(|r| r.profile_id).collect();
        let date: Date32Array = records
            .iter()
            .map(|r| {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Some((r.date - epoch).num_days() as i32)
            })
            .collect();
        let vo2max: Float64Array = records.iter().map(|r| r.vo2max).collect();
        let fitness_age: Int32Array = records.iter().map(|r| r.fitness_age).collect();
        let training_readiness: Int32Array = records.iter().map(|r| r.training_readiness).collect();
        let training_status: StringArray = records.iter().map(|r| r.training_status.as_deref()).collect();
        let lactate_threshold_hr: Int32Array = records.iter().map(|r| r.lactate_threshold_hr).collect();
        let lactate_threshold_pace: Float64Array = records.iter().map(|r| r.lactate_threshold_pace).collect();
        let race_5k_sec: Int32Array = records.iter().map(|r| r.race_5k_sec).collect();
        let race_10k_sec: Int32Array = records.iter().map(|r| r.race_10k_sec).collect();
        let race_half_sec: Int32Array = records.iter().map(|r| r.race_half_sec).collect();
        let race_marathon_sec: Int32Array = records.iter().map(|r| r.race_marathon_sec).collect();
        let endurance_score: Int32Array = records.iter().map(|r| r.endurance_score).collect();
        let hill_score: Int32Array = records.iter().map(|r| r.hill_score).collect();
        let raw_json: StringArray = records
            .iter()
            .map(|r| r.raw_json.as_ref().map(|j| j.to_string()))
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("profile_id", DataType::Int32, false),
            Field::new("date", DataType::Date32, false),
            Field::new("vo2max", DataType::Float64, true),
            Field::new("fitness_age", DataType::Int32, true),
            Field::new("training_readiness", DataType::Int32, true),
            Field::new("training_status", DataType::Utf8, true),
            Field::new("lactate_threshold_hr", DataType::Int32, true),
            Field::new("lactate_threshold_pace", DataType::Float64, true),
            Field::new("race_5k_sec", DataType::Int32, true),
            Field::new("race_10k_sec", DataType::Int32, true),
            Field::new("race_half_sec", DataType::Int32, true),
            Field::new("race_marathon_sec", DataType::Int32, true),
            Field::new("endurance_score", DataType::Int32, true),
            Field::new("hill_score", DataType::Int32, true),
            Field::new("raw_json", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id),
                Arc::new(profile_id),
                Arc::new(date),
                Arc::new(vo2max),
                Arc::new(fitness_age),
                Arc::new(training_readiness),
                Arc::new(training_status),
                Arc::new(lactate_threshold_hr),
                Arc::new(lactate_threshold_pace),
                Arc::new(race_5k_sec),
                Arc::new(race_10k_sec),
                Arc::new(race_half_sec),
                Arc::new(race_marathon_sec),
                Arc::new(endurance_score),
                Arc::new(hill_score),
                Arc::new(raw_json),
            ],
        )
        .map_err(|e| GarminError::Database(format!("Failed to create record batch: {}", e)))
    }

    fn batch_to_performance_metrics(batch: &RecordBatch) -> Result<Vec<PerformanceMetrics>> {
        let len = batch.num_rows();
        let mut records = Vec::with_capacity(len);

        let id = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let profile_id = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let date = batch.column(2).as_any().downcast_ref::<Date32Array>().unwrap();
        let vo2max = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let fitness_age = batch.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
        let training_readiness = batch.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
        let training_status = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
        let lactate_threshold_hr = batch.column(7).as_any().downcast_ref::<Int32Array>().unwrap();
        let lactate_threshold_pace = batch.column(8).as_any().downcast_ref::<Float64Array>().unwrap();
        let race_5k_sec = batch.column(9).as_any().downcast_ref::<Int32Array>().unwrap();
        let race_10k_sec = batch.column(10).as_any().downcast_ref::<Int32Array>().unwrap();
        let race_half_sec = batch.column(11).as_any().downcast_ref::<Int32Array>().unwrap();
        let race_marathon_sec = batch.column(12).as_any().downcast_ref::<Int32Array>().unwrap();
        let endurance_score = batch.column(13).as_any().downcast_ref::<Int32Array>().unwrap();
        let hill_score = batch.column(14).as_any().downcast_ref::<Int32Array>().unwrap();
        let raw_json = batch.column(15).as_any().downcast_ref::<StringArray>().unwrap();

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        for i in 0..len {
            records.push(PerformanceMetrics {
                id: id.is_valid(i).then(|| id.value(i)),
                profile_id: profile_id.value(i),
                date: epoch + chrono::Duration::days(date.value(i) as i64),
                vo2max: vo2max.is_valid(i).then(|| vo2max.value(i)),
                fitness_age: fitness_age.is_valid(i).then(|| fitness_age.value(i)),
                training_readiness: training_readiness.is_valid(i).then(|| training_readiness.value(i)),
                training_status: training_status.is_valid(i).then(|| training_status.value(i).to_string()),
                lactate_threshold_hr: lactate_threshold_hr.is_valid(i).then(|| lactate_threshold_hr.value(i)),
                lactate_threshold_pace: lactate_threshold_pace.is_valid(i).then(|| lactate_threshold_pace.value(i)),
                race_5k_sec: race_5k_sec.is_valid(i).then(|| race_5k_sec.value(i)),
                race_10k_sec: race_10k_sec.is_valid(i).then(|| race_10k_sec.value(i)),
                race_half_sec: race_half_sec.is_valid(i).then(|| race_half_sec.value(i)),
                race_marathon_sec: race_marathon_sec.is_valid(i).then(|| race_marathon_sec.value(i)),
                endurance_score: endurance_score.is_valid(i).then(|| endurance_score.value(i)),
                hill_score: hill_score.is_valid(i).then(|| hill_score.value(i)),
                raw_json: raw_json.is_valid(i).then(|| {
                    serde_json::from_str(raw_json.value(i)).unwrap_or_default()
                }),
            });
        }

        Ok(records)
    }

    // =========================================================================
    // Weight
    // =========================================================================

    /// Upsert weight entries
    pub fn upsert_weight(&self, records: &[WeightEntry]) -> Result<()> {
        self.ensure_dir(EntityType::Weight)?;

        // Group by partition key
        let mut partitions: std::collections::HashMap<String, Vec<WeightEntry>> =
            std::collections::HashMap::new();

        for record in records {
            let key = EntityType::Weight.partition_key(record.date);
            partitions.entry(key).or_default().push(record.clone());
        }

        // For each partition, read existing and merge
        for (key, mut new_records) in partitions {
            let path = self.partition_path(EntityType::Weight, &key);

            // Read existing
            let mut existing = self.read_weight_from_path(&path)?;

            // Create set of new dates for fast lookup
            let new_dates: std::collections::HashSet<(i32, NaiveDate)> = new_records
                .iter()
                .map(|r| (r.profile_id, r.date))
                .collect();

            // Keep existing records that aren't being replaced
            existing.retain(|r| !new_dates.contains(&(r.profile_id, r.date)));

            // Merge
            existing.append(&mut new_records);

            // Sort by date for consistent ordering
            existing.sort_by_key(|r| (r.profile_id, r.date));

            // Write merged
            let refs: Vec<&WeightEntry> = existing.iter().collect();
            let batch = Self::weight_to_batch(refs)?;
            self.write_batch(&path, &batch)?;
        }

        Ok(())
    }

    fn read_weight_from_path(&self, path: &Path) -> Result<Vec<WeightEntry>> {
        let batches = self.read_batches(path)?;
        let mut records = Vec::new();

        for batch in batches {
            records.extend(Self::batch_to_weight(&batch)?);
        }

        Ok(records)
    }

    fn weight_to_batch(records: Vec<&WeightEntry>) -> Result<RecordBatch> {
        let id: Int64Array = records.iter().map(|r| r.id).collect();
        let profile_id: Int32Array = records.iter().map(|r| r.profile_id).collect();
        let date: Date32Array = records
            .iter()
            .map(|r| {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Some((r.date - epoch).num_days() as i32)
            })
            .collect();
        let weight_kg: Float64Array = records.iter().map(|r| r.weight_kg).collect();
        let bmi: Float64Array = records.iter().map(|r| r.bmi).collect();
        let body_fat_pct: Float64Array = records.iter().map(|r| r.body_fat_pct).collect();
        let muscle_mass_kg: Float64Array = records.iter().map(|r| r.muscle_mass_kg).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("profile_id", DataType::Int32, false),
            Field::new("date", DataType::Date32, false),
            Field::new("weight_kg", DataType::Float64, true),
            Field::new("bmi", DataType::Float64, true),
            Field::new("body_fat_pct", DataType::Float64, true),
            Field::new("muscle_mass_kg", DataType::Float64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id),
                Arc::new(profile_id),
                Arc::new(date),
                Arc::new(weight_kg),
                Arc::new(bmi),
                Arc::new(body_fat_pct),
                Arc::new(muscle_mass_kg),
            ],
        )
        .map_err(|e| GarminError::Database(format!("Failed to create record batch: {}", e)))
    }

    fn batch_to_weight(batch: &RecordBatch) -> Result<Vec<WeightEntry>> {
        let len = batch.num_rows();
        let mut records = Vec::with_capacity(len);

        let id = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let profile_id = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let date = batch.column(2).as_any().downcast_ref::<Date32Array>().unwrap();
        let weight_kg = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let bmi = batch.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let body_fat_pct = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let muscle_mass_kg = batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        for i in 0..len {
            records.push(WeightEntry {
                id: id.is_valid(i).then(|| id.value(i)),
                profile_id: profile_id.value(i),
                date: epoch + chrono::Duration::days(date.value(i) as i64),
                weight_kg: weight_kg.is_valid(i).then(|| weight_kg.value(i)),
                bmi: bmi.is_valid(i).then(|| bmi.value(i)),
                body_fat_pct: body_fat_pct.is_valid(i).then(|| body_fat_pct.value(i)),
                muscle_mass_kg: muscle_mass_kg.is_valid(i).then(|| muscle_mass_kg.value(i)),
            });
        }

        Ok(records)
    }

    // =========================================================================
    // Profiles
    // =========================================================================

    /// Write profiles (overwrites entire file)
    pub fn write_profiles(&self, profiles: &[Profile]) -> Result<()> {
        fs::create_dir_all(&self.base_path).map_err(|e| {
            GarminError::Database(format!("Failed to create directory: {}", e))
        })?;

        let path = self.partition_path(EntityType::Profiles, "");
        let refs: Vec<&Profile> = profiles.iter().collect();
        let batch = Self::profiles_to_batch(refs)?;
        self.write_batch(&path, &batch)?;

        Ok(())
    }

    /// Read all profiles
    pub fn read_profiles(&self) -> Result<Vec<Profile>> {
        let path = self.partition_path(EntityType::Profiles, "");
        let batches = self.read_batches(&path)?;
        let mut profiles = Vec::new();

        for batch in batches {
            profiles.extend(Self::batch_to_profiles(&batch)?);
        }

        Ok(profiles)
    }

    fn profiles_to_batch(profiles: Vec<&Profile>) -> Result<RecordBatch> {
        let profile_id: Int32Array = profiles.iter().map(|p| p.profile_id).collect();
        let display_name: StringArray = profiles.iter().map(|p| Some(p.display_name.as_str())).collect();
        let user_id: Int64Array = profiles.iter().map(|p| p.user_id).collect();
        let created_at: TimestampMicrosecondArray = profiles
            .iter()
            .map(|p| p.created_at.map(|t| t.timestamp_micros()))
            .collect();
        let last_sync_at: TimestampMicrosecondArray = profiles
            .iter()
            .map(|p| p.last_sync_at.map(|t| t.timestamp_micros()))
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("profile_id", DataType::Int32, false),
            Field::new("display_name", DataType::Utf8, false),
            Field::new("user_id", DataType::Int64, true),
            Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), true),
            Field::new("last_sync_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(profile_id),
                Arc::new(display_name),
                Arc::new(user_id),
                Arc::new(created_at),
                Arc::new(last_sync_at),
            ],
        )
        .map_err(|e| GarminError::Database(format!("Failed to create record batch: {}", e)))
    }

    fn batch_to_profiles(batch: &RecordBatch) -> Result<Vec<Profile>> {
        let len = batch.num_rows();
        let mut profiles = Vec::with_capacity(len);

        let profile_id = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let display_name = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let user_id = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let created_at = batch.column(3).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let last_sync_at = batch.column(4).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

        for i in 0..len {
            profiles.push(Profile {
                profile_id: profile_id.value(i),
                display_name: display_name.value(i).to_string(),
                user_id: user_id.is_valid(i).then(|| user_id.value(i)),
                created_at: created_at.is_valid(i).then(|| {
                    DateTime::from_timestamp_micros(created_at.value(i)).unwrap_or_default()
                }),
                last_sync_at: last_sync_at.is_valid(i).then(|| {
                    DateTime::from_timestamp_micros(last_sync_at.value(i)).unwrap_or_default()
                }),
            });
        }

        Ok(profiles)
    }

    /// Get the base path for external readers to use with DuckDB glob queries
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_activity_round_trip() {
        let temp = TempDir::new().unwrap();
        let store = ParquetStore::new(temp.path());

        let activities = vec![Activity {
            activity_id: 123,
            profile_id: 1,
            activity_name: Some("Morning Run".to_string()),
            activity_type: Some("running".to_string()),
            start_time_local: Some(DateTime::from_timestamp(1703001600, 0).unwrap()),
            start_time_gmt: Some(DateTime::from_timestamp(1703001600, 0).unwrap()),
            duration_sec: Some(3600.0),
            distance_m: Some(10000.0),
            calories: Some(500),
            avg_hr: Some(150),
            max_hr: Some(175),
            avg_speed: Some(2.78),
            max_speed: Some(3.5),
            elevation_gain: Some(100.0),
            elevation_loss: Some(100.0),
            avg_cadence: Some(180.0),
            avg_power: None,
            normalized_power: None,
            training_effect: Some(3.5),
            training_load: Some(120.0),
            start_lat: Some(37.7749),
            start_lon: Some(-122.4194),
            end_lat: Some(37.7849),
            end_lon: Some(-122.4094),
            ground_contact_time: Some(250.0),
            vertical_oscillation: Some(8.5),
            stride_length: Some(1.2),
            location_name: Some("San Francisco".to_string()),
            raw_json: None,
        }];

        store.upsert_activities(&activities).unwrap();

        // Read back
        let key = EntityType::Activities.partition_key(
            activities[0].start_time_local.unwrap().date_naive()
        );
        let path = store.partition_path(EntityType::Activities, &key);
        let read_back = store.read_activities_from_path(&path).unwrap();

        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].activity_id, 123);
        assert_eq!(read_back[0].activity_name, Some("Morning Run".to_string()));
    }

    #[test]
    fn test_daily_health_round_trip() {
        let temp = TempDir::new().unwrap();
        let store = ParquetStore::new(temp.path());

        let records = vec![DailyHealth {
            id: None,
            profile_id: 1,
            date: NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
            steps: Some(10000),
            step_goal: Some(8000),
            total_calories: Some(2500),
            active_calories: Some(500),
            bmr_calories: Some(2000),
            resting_hr: Some(55),
            sleep_seconds: Some(28800),
            deep_sleep_seconds: Some(7200),
            light_sleep_seconds: Some(14400),
            rem_sleep_seconds: Some(7200),
            sleep_score: Some(85),
            avg_stress: Some(30),
            max_stress: Some(75),
            body_battery_start: Some(95),
            body_battery_end: Some(45),
            hrv_weekly_avg: Some(45),
            hrv_last_night: Some(48),
            hrv_status: Some("balanced".to_string()),
            avg_respiration: Some(14.5),
            avg_spo2: Some(96),
            lowest_spo2: Some(92),
            hydration_ml: Some(2500),
            moderate_intensity_min: Some(30),
            vigorous_intensity_min: Some(20),
            raw_json: None,
        }];

        store.upsert_daily_health(&records).unwrap();

        // Read back
        let key = EntityType::DailyHealth.partition_key(records[0].date);
        let path = store.partition_path(EntityType::DailyHealth, &key);
        let read_back = store.read_daily_health_from_path(&path).unwrap();

        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].steps, Some(10000));
        assert_eq!(read_back[0].date, NaiveDate::from_ymd_opt(2024, 12, 15).unwrap());
    }

    #[test]
    fn test_has_daily_health() {
        let temp = TempDir::new().unwrap();
        let store = ParquetStore::new(temp.path());
        let date = NaiveDate::from_ymd_opt(2024, 12, 15).unwrap();

        assert!(!store.has_daily_health(1, date).unwrap());

        let records = vec![DailyHealth {
            id: None,
            profile_id: 1,
            date,
            steps: Some(10000),
            step_goal: None,
            total_calories: None,
            active_calories: None,
            bmr_calories: None,
            resting_hr: None,
            sleep_seconds: None,
            deep_sleep_seconds: None,
            light_sleep_seconds: None,
            rem_sleep_seconds: None,
            sleep_score: None,
            avg_stress: None,
            max_stress: None,
            body_battery_start: None,
            body_battery_end: None,
            hrv_weekly_avg: None,
            hrv_last_night: None,
            hrv_status: None,
            avg_respiration: None,
            avg_spo2: None,
            lowest_spo2: None,
            hydration_ml: None,
            moderate_intensity_min: None,
            vigorous_intensity_min: None,
            raw_json: None,
        }];

        store.upsert_daily_health(&records).unwrap();
        assert!(store.has_daily_health(1, date).unwrap());
    }

    #[test]
    fn test_has_track_points() {
        let temp = TempDir::new().unwrap();
        let store = ParquetStore::new(temp.path());
        let date = NaiveDate::from_ymd_opt(2024, 12, 15).unwrap();

        assert!(!store.has_track_points(42, date).unwrap());

        let points = vec![TrackPoint {
            id: None,
            activity_id: 42,
            timestamp: DateTime::from_timestamp(1703001600, 0).unwrap(),
            lat: Some(1.0),
            lon: Some(2.0),
            elevation: Some(3.0),
            heart_rate: None,
            cadence: None,
            power: None,
            speed: None,
        }];

        store.write_track_points(date, &points).unwrap();
        assert!(store.has_track_points(42, date).unwrap());
    }

    #[test]
    fn test_concurrent_read_write() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let temp = TempDir::new().unwrap();
        let store = Arc::new(ParquetStore::new(temp.path()));

        // Write initial data
        let initial_activities = vec![Activity {
            activity_id: 1,
            profile_id: 1,
            activity_name: Some("Initial Activity".to_string()),
            activity_type: Some("running".to_string()),
            start_time_local: Some(DateTime::from_timestamp(1703001600, 0).unwrap()),
            start_time_gmt: Some(DateTime::from_timestamp(1703001600, 0).unwrap()),
            duration_sec: Some(3600.0),
            distance_m: Some(10000.0),
            calories: None,
            avg_hr: None,
            max_hr: None,
            avg_speed: None,
            max_speed: None,
            elevation_gain: None,
            elevation_loss: None,
            avg_cadence: None,
            avg_power: None,
            normalized_power: None,
            training_effect: None,
            training_load: None,
            start_lat: None,
            start_lon: None,
            end_lat: None,
            end_lon: None,
            ground_contact_time: None,
            vertical_oscillation: None,
            stride_length: None,
            location_name: None,
            raw_json: None,
        }];
        store.upsert_activities(&initial_activities).unwrap();

        // Barrier to coordinate reader/writer threads
        let barrier = Arc::new(Barrier::new(2));
        let store_reader = Arc::clone(&store);
        let barrier_reader = Arc::clone(&barrier);

        // Reader thread: reads existing data while writer writes new data
        let reader_handle = thread::spawn(move || {
            barrier_reader.wait(); // Sync with writer

            // Read the existing file
            let key = EntityType::Activities.partition_key(
                NaiveDate::from_ymd_opt(2023, 12, 19).unwrap()
            );
            let path = store_reader.partition_path(EntityType::Activities, &key);
            let activities = store_reader.read_activities_from_path(&path).unwrap();

            // Should see at least the initial activity
            assert!(!activities.is_empty(), "Reader should see initial data");
            activities.len()
        });

        // Writer thread: writes new data to a DIFFERENT partition
        let writer_handle = thread::spawn(move || {
            barrier.wait(); // Sync with reader

            // Write to a different partition (different week)
            let new_activities = vec![Activity {
                activity_id: 2,
                profile_id: 1,
                activity_name: Some("Concurrent Activity".to_string()),
                activity_type: Some("cycling".to_string()),
                start_time_local: Some(DateTime::from_timestamp(1703606400, 0).unwrap()), // Different week
                start_time_gmt: Some(DateTime::from_timestamp(1703606400, 0).unwrap()),
                duration_sec: Some(7200.0),
                distance_m: Some(50000.0),
                calories: None,
                avg_hr: None,
                max_hr: None,
                avg_speed: None,
                max_speed: None,
                elevation_gain: None,
                elevation_loss: None,
                avg_cadence: None,
                avg_power: None,
                normalized_power: None,
                training_effect: None,
                training_load: None,
                start_lat: None,
                start_lon: None,
                end_lat: None,
                end_lon: None,
                ground_contact_time: None,
                vertical_oscillation: None,
                stride_length: None,
                location_name: None,
                raw_json: None,
            }];
            store.upsert_activities(&new_activities).unwrap();
            2 // Return activity id written
        });

        // Wait for both threads to complete
        let read_count = reader_handle.join().expect("Reader thread panicked");
        let written_id = writer_handle.join().expect("Writer thread panicked");

        assert_eq!(read_count, 1, "Reader should have read 1 activity");
        assert_eq!(written_id, 2, "Writer should have written activity 2");
    }

    #[test]
    fn test_duckdb_glob_query() {
        use duckdb::Connection;

        let temp = TempDir::new().unwrap();
        let store = ParquetStore::new(temp.path());

        // Write activities to multiple partitions
        let activities = vec![
            Activity {
                activity_id: 1,
                profile_id: 1,
                activity_name: Some("Week 51 Run".to_string()),
                activity_type: Some("running".to_string()),
                start_time_local: Some(DateTime::from_timestamp(1703001600, 0).unwrap()), // 2023-W51
                start_time_gmt: Some(DateTime::from_timestamp(1703001600, 0).unwrap()),
                duration_sec: Some(3600.0),
                distance_m: Some(10000.0),
                calories: None,
                avg_hr: None,
                max_hr: None,
                avg_speed: None,
                max_speed: None,
                elevation_gain: None,
                elevation_loss: None,
                avg_cadence: None,
                avg_power: None,
                normalized_power: None,
                training_effect: None,
                training_load: None,
                start_lat: None,
                start_lon: None,
                end_lat: None,
                end_lon: None,
                ground_contact_time: None,
                vertical_oscillation: None,
                stride_length: None,
                location_name: None,
                raw_json: None,
            },
            Activity {
                activity_id: 2,
                profile_id: 1,
                activity_name: Some("Week 52 Ride".to_string()),
                activity_type: Some("cycling".to_string()),
                start_time_local: Some(DateTime::from_timestamp(1703606400, 0).unwrap()), // 2023-W52
                start_time_gmt: Some(DateTime::from_timestamp(1703606400, 0).unwrap()),
                duration_sec: Some(7200.0),
                distance_m: Some(50000.0),
                calories: None,
                avg_hr: None,
                max_hr: None,
                avg_speed: None,
                max_speed: None,
                elevation_gain: None,
                elevation_loss: None,
                avg_cadence: None,
                avg_power: None,
                normalized_power: None,
                training_effect: None,
                training_load: None,
                start_lat: None,
                start_lon: None,
                end_lat: None,
                end_lon: None,
                ground_contact_time: None,
                vertical_oscillation: None,
                stride_length: None,
                location_name: None,
                raw_json: None,
            },
        ];
        store.upsert_activities(&activities).unwrap();

        // Use DuckDB to query all Parquet files with glob pattern
        let conn = Connection::open_in_memory().unwrap();
        let glob_pattern = format!("{}/*.parquet", temp.path().join("activities").display());

        let mut stmt = conn.prepare(&format!(
            "SELECT activity_id, activity_name FROM '{}' ORDER BY activity_id",
            glob_pattern
        )).unwrap();

        let results: Vec<(i64, String)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        // Should see both activities from different partitions
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (1, "Week 51 Run".to_string()));
        assert_eq!(results[1], (2, "Week 52 Ride".to_string()));
    }
}
