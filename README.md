# garmin-cli

Garmin Connect CLI built with Rust.

## Installation

```bash
cargo install --path crates/garmin-cli
```

Or from crates.io:
```bash
cargo install garmin-cli
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

## Health Commands

```bash
# Daily summary
garmin health summary
garmin health summary --date 2025-12-13

# Sleep
garmin health sleep
garmin health sleep --date 2025-12-13
garmin health sleep --days 7

# Stress
garmin health stress
garmin health stress --date 2025-12-13
garmin health stress --days 7

# Heart rate
garmin health heart-rate
garmin health heart-rate --date 2025-12-13

# Body battery
garmin health body-battery
garmin health body-battery --date 2025-12-13

# Steps (with date range)
garmin health steps
garmin health steps --days 28

# Calories
garmin health calories
garmin health calories --days 7

# Weight
garmin health weight
garmin health weight --from 2025-01-01 --to 2025-12-31
garmin health weight-add --weight 80.2 --unit kg

# VO2 Max
garmin health vo2max
garmin health vo2max --date 2025-12-10

# Training readiness
garmin health training-readiness
garmin health training-readiness --date 2025-12-13

# Training status
garmin health training-status

# HRV
garmin health hrv
garmin health hrv --date 2025-12-13

# Fitness age
garmin health fitness-age

# Performance metrics
garmin health lactate-threshold --days 90
garmin health race-predictions
garmin health endurance-score --days 30
garmin health hill-score --days 30
garmin health personal-records
garmin health performance-summary

# Additional health metrics
garmin health spo2
garmin health respiration
garmin health intensity-minutes
garmin health blood-pressure --from 2025-01-01 --to 2025-12-31
garmin health hydration

# Health insights (sleep/stress correlations)
garmin health insights
garmin health insights --days 28
```

## Activity Commands

```bash
# List activities
garmin activities list
garmin activities list --limit 20
garmin activities list --type running
garmin activities list --from 2025-12-01 --to 2025-12-31

# Get activity details
garmin activities get 21247810009

# Download activity (FIT, GPX, TCX)
garmin activities download 21247810009 --format fit --output activity.fit
garmin activities download 21247810009 --format gpx --output activity.gpx

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

```bash
# Sync activities to local database
garmin sync run
garmin sync run --days 30

# Check sync status
garmin sync status

# Reset failed sync tasks
garmin sync reset

# Clear pending sync tasks
garmin sync clear
```

## Output Formats

```bash
# Table (default)
garmin health summary

# JSON
garmin health summary --format json

# CSV
garmin health summary --format csv
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

```
╔══════════════════════════════════════════════════════════════════╗
║                     HEALTH INSIGHTS (28 days)                    ║
╚══════════════════════════════════════════════════════════════════╝

🧠 RESTORATIVE SLEEP RATIO
   Your avg: 38%  |  Target: >45%  |  Last night: 22% ⚠️

😰 STRESS CORRELATION
   Low restorative (<30%) → avg next-day stress: 38
   High restorative (>45%) → avg next-day stress: 24

🎯 TODAY'S PREDICTION
   Based on last night (50m restorative, 22%):
   Expected stress: HIGH (35-45 avg expected)

📊 SLEEP QUALITY RANKING (by restorative %)
   Best:  2025-12-14 (52% restorative, score 93)
   Worst: 2025-12-18 (22% restorative, score 50)
```

## License

MIT
