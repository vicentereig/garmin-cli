# garmin-cli

Garmin Connect CLI built with Rust.

## Installation

```bash
cargo install --path crates/garmin-cli
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
garmin health steps --days 30

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
```

## Profile Commands

```bash
# Show profile
garmin profile show

# Show settings
garmin profile settings
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
║                    GARMIN HEALTH SNAPSHOT                        ║
╚══════════════════════════════════════════════════════════════════╝

  ❤️  CARDIOVASCULAR                    🏃  FITNESS
  ─────────────────────                 ─────────────────────
  Resting HR      43 bpm                VO2 Max      53 ml/kg/min
  HRV             65 ms                 Fitness Age  37 yrs
  Status          BALANCED              Training     READY (69/100)

  📈  7-DAY STEPS
  ──────────────────────────────────────────────────────────
  Dec 13  ████████████████████████████████████████████  22,236
  Dec 12  ██████████████████████████████████            17,224
  Dec 11  ██████████████████████████████                15,086
```

## License

MIT
