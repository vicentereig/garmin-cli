# Backfill-Accurate Garmin Metrics (garmin-cli)

## Context & Problem
- a2ui-rails now queries Garmin data "as-of date", but historical rows often contain **today's values**.
- Root cause: garmin-cli sync writes **latest-only endpoints** into per-date parquet rows (performance_metrics).
- Result: visiting `/YYYY/MM/DD/briefings` can show current VO2/race predictions, not historical.

## Status (as of 2026-01-02)
### Completed
- Dual pipeline sync (frontier + backfill) with backfill window scheduling ✅

### Remaining
- Sync daily_health: fill sleep + HRV (TDD)
- Sync performance_metrics: use date-scoped endpoints (TDD)
- Guardrails for missing data (TDD)
- Backfill/migration tooling for date-range resync
- Restore TUI to show Frontier vs Backfill progress (TDD)

## Key Takeaways (from garmin-cli + python-garminconnect)
- Many metrics *are* backfillable via date-scoped endpoints.
- garmin-cli currently:
  - Uses `/metrics-service/metrics/maxmet/latest` for VO2 max (wrong for backfill).
  - Uses `/metrics-service/metrics/racepredictions/latest` for race predictions (wrong for backfill).
  - Uses date-scoped training readiness + training status endpoints, but extracts `mostRecentTrainingStatus`.
  - Leaves sleep stages/score + HRV fields **nil** even though endpoints exist.
- python-garminconnect confirms date-scoped endpoints for:
  - VO2 max, training readiness, training status, HRV, sleep, fitness age.
  - Endurance score and hill score (single-day + ranged stats).
  - Race predictions supports daily/monthly ranges (not just latest).

## Backfillable Matrix (field -> endpoint -> status)
Health (daily_health):
- sleep_seconds, deep/light/rem, sleep_score
  - `/wellness-service/wellness/dailySleepData/{display_name}?date=DATE`
  - Backfillable ✅ (not used in sync today)
- hrv_weekly_avg, hrv_last_night, hrv_status
  - `/hrv-service/hrv/DATE`
  - Backfillable ✅ (not used in sync today)
- steps, stress, calories, body battery, etc.
  - `/usersummary-service/usersummary/daily/{display_name}?calendarDate=DATE`
  - Backfillable ✅ (already used)

Performance (performance_metrics):
- vo2max, fitness_age
  - VO2: `/metrics-service/metrics/maxmet/daily/DATE/DATE`
  - Fitness age: `/fitnessage-service/fitnessage/DATE`
  - Backfillable ✅ (currently uses `latest`)
- training_readiness
  - `/metrics-service/metrics/trainingreadiness/DATE`
  - Backfillable ✅ (already date-scoped)
- training_status, training load (acute/chronic)
  - `/metrics-service/metrics/trainingstatus/aggregated/DATE`
  - Backfillable ✅ but must parse date-specific entry, not `mostRecentTrainingStatus`
- race_predictions (5K/10K/HM/Marathon)
  - `/metrics-service/metrics/racepredictions/daily/{display_name}?fromCalendarDate=DATE&toCalendarDate=DATE`
  - Backfillable ✅ (currently uses `latest`)
- endurance_score
  - `/metrics-service/metrics/endurancescore?calendarDate=DATE`
  - Backfillable ✅
- hill_score
  - `/metrics-service/metrics/hillscore?calendarDate=DATE`
  - Backfillable ✅
- lactate_threshold (optional)
  - endpoints present in CLI (range); can be added for backfill if useful

## Proposed Implementation (garmin-cli) — TDD Required
### 1) Sync daily_health: fill sleep + HRV (TDD)
- Write failing tests for new sleep/HRV fields before implementation.
- Fetch sleep DTO for date and map:
  - sleep_seconds (total), deep/light/rem seconds, sleep_score
- Fetch HRV summary and map:
  - hrv_weekly_avg, hrv_last_night, hrv_status
- Keep usersummary daily for steps/stress/body battery as-is.

### 2) Sync performance_metrics: use date-scoped endpoints (TDD)
- Write failing tests for VO2 max daily endpoint, race predictions daily range, and training status parsing.
- Replace VO2 max `latest` with daily endpoint and extract vo2MaxValue + fitnessAge.
- Replace race predictions `latest` with daily range endpoint for the same date.
- Training status: parse the entry that corresponds to the requested date, not "mostRecent".
  - If payload lacks a date-keyed entry, leave as nil and log.
- Add endurance_score + hill_score using per-day endpoints.

### 3) Add guardrails for missing data (TDD)
- Add tests to ensure nils are preserved when endpoints are missing/empty.
- For endpoints returning 404/empty, store nils (do not backfill with "latest").
- If Garmin returns partials, keep fields nil and note in logs.

## Persistence & Backfill Plan
### Persistence layer impact
- No schema changes needed: `daily_health` already stores sleep + HRV fields; `performance_metrics` already stores VO2, fitness_age, race predictions, endurance/hill.
- Update **sync ingestion** only: populate existing columns with date-scoped values instead of latest-only values.
- Ensure parquet upsert paths write updated fields for the same `date` (overwrite existing row for that date).

### Backfill / Migration Plan
1) Re-run sync for historical dates after updating sync endpoints (this overwrites parquet rows by date).
2) Add a CLI command to re-sync **daily_health** + **performance_metrics** for a date range (not optional).
3) For existing parquet rows, overwrite by date with newly fetched backfill data.

## Risks & Unknowns
- Training status payload may not provide per-date values (despite date-scoped endpoint).
- Race predictions daily endpoint is supported in python-garminconnect; confirm Garmin account permissions.
- Endurance/hill score endpoints may rate-limit; add throttling if needed.

## Acceptance Criteria
- Tests exist and pass for all newly backfilled fields (sleep, HRV, VO2, race predictions, endurance/hill, training status parsing).
- A date-specific briefing shows historical VO2, training readiness/status, race predictions, sleep, HRV.
- For dates with no data, briefing shows "No data" rather than today's values.
- Re-syncing a past date does not change "today" values or contaminate other dates.

## Next Steps (execution order)
1) Add sleep + HRV fetches to sync_daily_health.
2) Replace performance latest endpoints with date-scoped ones.
3) Add endurance/hill score pulls.
4) Implement training_status parsing for date-specific entries.
5) Add tests/fixtures for sync output (optional).

## Parallel Track: Restore TUI for Dual Pipelines
Goal: bring back the TUI and adapt it to represent **Frontier** vs **Backfill** progress.

### Scope
- Re-enable the TUI entrypoint for `sync` and optionally `watch`.
- Show two panels (or a split view) with:
  - Pipeline name (Frontier / Backfill)
  - Date range (latest window vs backfill window)
  - Queue counts by type + pending totals
  - Rate limit/backoff status per pipeline

### TDD Plan
1) Add view-model unit tests for pipeline-specific progress rendering.
2) Add tests for pipeline-aware queue counts and date ranges in UI state.
3) Smoke test to ensure TUI can render both pipelines without panics.

### Open Questions
- Should `watch` default to TUI when interactive, or keep TUI only for `sync`?
- If only `sync` uses TUI, what should `watch` show for progress (logs vs minimal status line)?
