---
name: garmin-health-analysis
description: Talk to your Garmin data naturally for health, recovery, sleep, workouts, performance, and training insights using the local `garmin` CLI.
---

# Garmin Health Analysis Skill

Use this skill when the user asks about:
- sleep, recovery, HRV, resting heart rate, stress, body battery
- workouts, runs, rides, recent activities, activity details
- training readiness, training status, VO2 max, race predictions, lactate threshold
- weight trends, hydration, steps, calories, respiration, SPO2
- syncing Garmin data locally for deeper analysis

## Source of truth
Use the locally installed CLI first:

```bash
garmin --help
```

Primary binary location on this machine:

```bash
/home/grunt/.local/bin/garmin
```

Fork/source repo on this machine:

```bash
/home/grunt/.openclaw/workspace/skills/garmin-cli
```

## Operating rules

- Prefer direct `garmin` CLI calls via `exec`.
- For simple questions, query only the needed command.
- For broader trend analysis, use `garmin sync run` first if local synced data is needed.
- Do not ask the user to manually interpret raw CLI output if you can summarize it.
- If auth is missing, use:

```bash
garmin auth status
garmin auth login
```

- If the user wants current-day status, start with:

```bash
garmin health summary
```

## Common command patterns

### Daily health
```bash
garmin health summary
garmin health sleep --days 7
garmin health stress --days 7
garmin health hrv
garmin health body-battery
garmin health heart-rate --date YYYY-MM-DD
garmin health steps --days 28
garmin health calories --days 7
```

### Performance and training
```bash
garmin health training-readiness --days 14
garmin health training-status --days 14
garmin health vo2max
garmin health race-predictions
garmin health endurance-score --days 30
garmin health hill-score --days 30
garmin health lactate-threshold --days 90
garmin health performance-summary
```

### Activities
```bash
garmin activities list --limit 20
garmin activities get <activity_id>
garmin activities download <activity_id> --type fit --output activity.fit
```

### Weight and related metrics
```bash
garmin health weight
garmin health hydration
garmin health blood-pressure --from YYYY-MM-DD --to YYYY-MM-DD
```

### Sync for offline analysis
```bash
garmin sync status
garmin sync run
garmin sync run --backfill
garmin sync run --health
garmin sync run --activities
```

## Response style

- Be practical and coaching-oriented.
- Highlight trends, outliers, and actionable next steps.
- For training questions, connect recovery metrics to workload when possible.
- Be careful not to overstate medical conclusions.
- Frame health insight as supportive guidance, not diagnosis.

## If something fails

- Show the actual Garmin CLI error briefly.
- If login fails, inspect whether auth expired:

```bash
garmin auth status
```

- If local analysis is requested but data is stale or missing, run sync first.
