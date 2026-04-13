# garmin-cli

[![CI](https://github.com/grunt3714-lgtm/garmin-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/grunt3714-lgtm/garmin-cli/actions/workflows/ci.yml)
[![Release](https://github.com/grunt3714-lgtm/garmin-cli/actions/workflows/release.yml/badge.svg)](https://github.com/grunt3714-lgtm/garmin-cli/actions/workflows/release.yml)
[![Latest Release](https://img.shields.io/github/v/release/grunt3714-lgtm/garmin-cli?display_name=tag)](https://github.com/grunt3714-lgtm/garmin-cli/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Fast Garmin Connect CLI for Linux, with working auth, sync, and health/training commands.

## Install

One-liner:

```bash
curl -fsSL https://github.com/grunt3714-lgtm/garmin-cli/releases/latest/download/garmin-linux-amd64.tar.gz | tar -xz && install -Dm755 garmin-linux-amd64 ~/.local/bin/garmin
```

Build from source:

```bash
cargo build --release && install -Dm755 target/release/garmin ~/.local/bin/garmin
```

## Quick examples

```bash
garmin auth login
garmin health summary
garmin health sleep --days 7
garmin health training-readiness --days 14
garmin health training-status --days 14
garmin sync run --backfill
```

## Authentication

```bash
# Login (opens browser for SSO)
garmin auth login

# Check status
garmin auth status

# Logout
garmin auth logout
```

## More examples

```bash
# Today
garmin health summary

# Recovery trends
garmin health sleep --days 7
garmin health hrv --date 2025-12-13
garmin health training-readiness --days 14

# Training state
garmin health training-status --days 14
garmin health race-predictions --date 2025-12-13
garmin health performance-summary --date 2025-12-13

# Other useful commands
garmin health steps --days 28
garmin health weight --from 2025-01-01 --to 2025-12-31
garmin health insights --days 28
```

## Activity Commands

```bash
# List activities
garmin activities list
garmin activities list --limit 20

# Get activity details
garmin activities get 21247810009

# Download activity (FIT, GPX, TCX)
garmin activities download 21247810009 --type fit --output activity.fit
garmin activities download 21247810009 --type gpx --output activity.gpx

# Upload activity
garmin activities upload activity.fit
```

## Device Commands

```bash
# List devices
garmin devices list

# Get device details
garmin devices get 3442975663

# Show device history from synced data
garmin devices history
```

## Profile Commands

```bash
# Show profile
garmin profile show

# Show settings
garmin profile settings
```

## Sync Commands

Sync your Garmin data to local Parquet files for offline analysis. Sync runs use line-oriented terminal progress and a final summary.

```bash
# Sync recent data (latest mode)
garmin sync run

# Sync specific date range
garmin sync run --from 2025-01-01 --to 2025-12-31

# Backfill historical data
garmin sync run --backfill

# Sync only activities
garmin sync run --activities

# Sync only health data
garmin sync run --health

# Sync only performance metrics
garmin sync run --performance

# Dry run (preview what will be synced)
garmin sync run --dry-run

# Check sync status
garmin sync status

# Reset failed sync tasks
garmin sync reset

# Clear pending sync tasks
garmin sync clear
```

### Storage Architecture

Data is stored in time-partitioned Parquet files for efficient querying:

```
~/.local/share/garmin/           # Linux
~/Library/Application Support/garmin/  # macOS
├── sync.db                      # SQLite: sync state + task queue
├── profiles.parquet             # User profiles
├── activities/
│   ├── 2024-W48.parquet        # Weekly partitions
│   └── ...
├── track_points/
│   ├── 2024-12-01.parquet      # Daily partitions (GPS data)
│   └── ...
├── daily_health/
│   ├── 2024-12.parquet         # Monthly partitions
│   └── ...
└── performance_metrics/
    ├── 2024-12.parquet         # Monthly partitions
    └── ...
```

### Parallel Sync Pipeline

The sync uses a producer/consumer architecture with 4 concurrent workers:

- **4 Producers**: Fetch data from Garmin API (rate-limited)
- **Bounded channel**: Backpressure with 100-item buffer
- **4 Consumers**: Write to Parquet with partition-level locks
- **Crash recovery**: SQLite task queue persists progress

Different partitions can be written in parallel; writes to the same partition are serialized to prevent data loss.

### Querying with DuckDB

Query your synced Parquet files directly using DuckDB:

```bash
# Install DuckDB CLI
brew install duckdb  # macOS
# or download from https://duckdb.org

# Set your data path
# macOS: ~/Library/Application Support/garmin
# Linux: ~/.local/share/garmin
export GARMIN_DATA=~/Library/Application\ Support/garmin

# Query activities from all partitions
duckdb -c "SELECT * FROM '$GARMIN_DATA/activities/*.parquet' ORDER BY start_time_local DESC LIMIT 10"

# Query health data for a specific month
duckdb -c "SELECT * FROM '$GARMIN_DATA/daily_health/2025-01.parquet' ORDER BY date"

# Aggregate across all health data
duckdb -c "SELECT date, steps, resting_hr, sleep_seconds/3600.0 as sleep_hours FROM '$GARMIN_DATA/daily_health/*.parquet' WHERE date >= '2025-01-01' ORDER BY date"
```

### Parquet Schema

The sync creates several Parquet datasets:

- `activities/*.parquet` - Activity summaries (runs, rides, etc.) - weekly partitions
- `track_points/*.parquet` - GPS track points - daily partitions
- `daily_health/*.parquet` - Daily health metrics (sleep, stress, HRV, etc.) - monthly partitions
- `performance_metrics/*.parquet` - Training data (readiness, VO2 max, race predictions) - monthly partitions
- `profiles.parquet` - User profile data

Example query for training load trends:
```sql
-- macOS path shown; use ~/.local/share/garmin on Linux
SELECT date, training_status, training_readiness, vo2max
FROM '~/Library/Application Support/garmin/performance_metrics/*.parquet'
WHERE date >= '2025-12-01'
ORDER BY date DESC;
```

## Multiple Profiles

```bash
# Use a specific profile
garmin --profile work auth login
garmin --profile work health summary

# Or via environment variable
GARMIN_PROFILE=work garmin health summary
```

## Example Output

### Performance Summary

```
$ garmin health performance-summary

Performance Summary for 2025-12-19
==================================================
VO2 Max:             53.1 ml/kg/min
Fitness Age:         37 years (actual: 43)
Training Status:     strained 1
Training Load:       306 acute / 249 chronic (ratio: 1.20 OPTIMAL)
Load Focus:          anaerobic shortage
Training Readiness:  32 (LOW)

Lactate Threshold
------------------------------
  Heart Rate:        162 bpm
  Pace:              4:24/km
  Last Updated:      2025-11-01

Race Predictions
------------------------------
  5K                     21:13  (4:14/km)
  10K                    45:21  (4:32/km)
  Half Marathon        1:42:47  (4:52/km)
  Marathon             3:46:27  (5:22/km)
```

### Training Status History

```
$ garmin health training-status --days 7

Date           Status    Acute  Chronic        Ratio
------------------------------------------------------
2025-12-19 strained      306      249         1.20
2025-12-18 strained      212      232         0.90
2025-12-17 strained      146      223         0.60
2025-12-16 strained      171      231         0.70
2025-12-15 strained      221      235         0.90
2025-12-14 unproductive  268      240         1.10
2025-12-13 unproductive  314      244         1.20
```

### Health Insights

```
$ garmin health insights --days 28

╔══════════════════════════════════════════════════════════════════╗
║                     HEALTH INSIGHTS (28 days)                    ║
╚══════════════════════════════════════════════════════════════════╝

RESTORATIVE SLEEP RATIO
   Your avg: 38%  |  Target: >45%  |  Last night: 22%

STRESS CORRELATION
   Low restorative (<30%) -> avg next-day stress: 38
   High restorative (>45%) -> avg next-day stress: 24

TODAY'S PREDICTION
   Based on last night (50m restorative, 22%):
   Expected stress: HIGH (35-45 avg expected)
```

## License

MIT
