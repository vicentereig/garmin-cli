# Complete Sync Contract for Garmin CLI

## Working Backwards Press Release

Garmin CLI now gives athletes a complete local copy of their Garmin history for any synced date range. A single `garmin sync run --from DATE --to DATE` reconciles activities, activity details, track data, daily health, recovery, stress, sleep, body battery, and training performance into the local data lake. Users no longer have to guess whether a live `garmin health ...` command has fresher data than the Parquet store.

Before this release, sync could report success while local files were still missing health rows, performance rows, activity detail fields, or newly discovered activities. Power users could work around it with combinations like `--health --performance --force`, `--activities --force`, or direct live commands, but that created a split-brain workflow: the CLI could show one answer live and another answer from synced storage.

With complete sync, the product promise is simple: if Garmin Connect has the metric for the requested period and Garmin CLI supports that domain, the local store has it too. Sync is no longer just a downloader. It is a coverage contract.

## Customer Promise

After a successful default sync for a date range:

- Activity lists, activity details, and local activity partitions agree for that range.
- Health commands and local health partitions agree for that range.
- Performance commands and local performance partitions agree for that range.
- Missing data is explicit, not silently skipped.
- `--force` is only needed to intentionally refresh existing records, not to fill obvious gaps.
- `sync status` can answer "what data is complete?" instead of only "how many partition files exist?"

## Customer FAQ

### What changes for normal users?

Users can run one command and trust the result:

```bash
garmin sync run --from 2026-06-15 --to 2026-06-19
```

That command should sync every first-class data domain for those dates by default: activities, daily health, and performance metrics. If the user later runs `garmin health sleep`, `garmin health body-battery`, `garmin health training-status`, or `garmin activities get` for the same period, the local store should already contain the corresponding supported data.

### What problem does this solve?

It removes the split-brain state where live commands can see data that sync did not persist. The June 2026 example exposed this clearly:

- `garmin activities list` showed June 15-18 activities while local activity Parquet initially stopped at June 14.
- `garmin health sleep --days 14`, `stress`, and `body-battery` showed June 15-19 live data while local `daily_health` initially stopped at June 14.
- `garmin health performance-summary -d 2026-06-19` showed current readiness, HRV, load, and race predictions while local `performance_metrics` only had weekly samples through June 15.
- Forcing individual domains fixed the local store, which means Garmin had the data and normal sync planning did not complete the local contract.

### Does complete sync mean downloading every possible Garmin file?

No. The default promise should cover queryable data that Garmin CLI exposes as commands and stores as structured Parquet. Large raw exports such as FIT, TCX, and KML archives should remain explicit downloads or an opt-in sync tier. GPX-derived track points are already part of sync and should stay in the default contract where available.

### Does complete sync mean every field becomes a typed column?

No. The first outcome is no missing records for supported domains. Typed columns should exist for high-value, commonly queried metrics. Rich or fast-moving payloads can be preserved as raw JSON first, then typed later when usage justifies it.

## Tenets

- One command should make the local store trustworthy for the requested period.
- Coverage should be measured by rows and dates, not by partition files.
- Live commands and synced data should use the same domain definitions.
- Missing Garmin data should be recorded as unavailable, not confused with unsynced data.
- `--force` should refresh stale or suspect rows, not act as a routine gap-filler.
- Raw JSON preservation is acceptable when typed schema expansion would slow delivery.

## Current State

### What sync already treats as first-class

- `activities/*.parquet`: weekly activity summary partitions.
- `track_points/*.parquet`: daily GPX-derived track points for activities with exportable GPS data.
- `daily_health/*.parquet`: monthly daily health rows.
- `performance_metrics/*.parquet`: monthly performance metric rows.
- `weight/*.parquet`: storage support exists, but sync does not currently make weight a first-class domain.

### What planning currently gets wrong

- Performance is planned weekly even though the CLI exposes date-specific performance endpoints.
- Health and activity partitions can exist while later rows in the same month or week are missing.
- Default sync can leave recent live data outside the local store until domain-specific `--force` runs are used.
- Activity sync stores list-summary payloads but not full `activities get` detail payloads.
- `sync status` counts files and pending tasks, but does not report date coverage, row gaps, or command-domain completeness.

## Outcome-Driven Requirements

### Outcome 1: Default Sync Completes the Requested Window

For `garmin sync run --from A --to B`, with no domain-narrowing flags:

- Activities are discovered for the full window.
- Activity details are reconciled for discovered activities.
- Track points are synced where Garmin provides exportable track data.
- Daily health is synced for every date in the window.
- Performance metrics are synced for every date in the window where Garmin provides date-specific values.
- Existing rows do not suppress missing rows in the same partition.
- The final summary reports coverage by domain.

Acceptance criteria:

- A dry run for a partially synced month reports missing dates, not "nothing to do" because `YYYY-MM.parquet` exists.
- A normal run fills those missing dates without `--force`.
- Running the equivalent live command after sync does not reveal supported data absent from local storage.

### Outcome 2: Sync Status Becomes a Coverage Report

`garmin sync status` should still show storage location and pending tasks, but it should also answer:

- Latest activity date in local storage.
- Latest health date in local storage.
- Latest performance date in local storage.
- Date gaps for the last N days.
- Whether the latest requested sync window is complete.
- Which domains have supported live commands but are not currently synced.

Acceptance criteria:

- `sync status` distinguishes "no Garmin data available" from "Garmin data not synced".
- Users can see that health is current through June 19 even if activity detail is current only through June 18.
- File counts remain secondary; coverage is primary.

### Outcome 3: Health Commands and Health Sync Share a Domain Contract

The following `garmin health` commands should have a clear sync story:

| Command domain | Current sync status | Desired outcome |
| --- | --- | --- |
| `summary`, `steps`, `calories` | Partially synced through daily usersummary | Preserve typed steps, calories, active calories, BMR, resting HR; add consumed calories, total distance, floors, active seconds where available |
| `sleep` | Synced for total/deep/light/REM/score/note | Preserve current fields; consider raw sleep DTO for awake/restless/components before adding more columns |
| `stress` | Only daily average/max via usersummary | Add detailed daily stress payload or typed stress time-series/summary so `stress` does not require live-only data |
| `body-battery` | Usersummary fields are mapped, but live command uses body battery reports | Sync body battery report data: charged, drained, start/end value and timestamps; clarify or rename existing columns if they represent charged/drained rather than start/end |
| `heart-rate` | Resting HR is synced; daily HR endpoint is live-only | Add daily heart-rate summary and zones; preserve raw daily HR payload for min/max/zones/time series |
| `hrv` | Synced for weekly average, last night, status | Keep as first-class daily health coverage |
| `spo2` | Partially represented by usersummary fields | Sync the dedicated SpO2 daily endpoint or preserve raw payload for average, lowest, latest |
| `respiration` | Only average respiration is partially represented | Sync dedicated respiration endpoint for waking/sleep/high/low values |
| `intensity-minutes` | Moderate/vigorous fields exist, but dedicated command has total/goal semantics | Add total and goal semantics or raw endpoint payload |
| `hydration` | Current typed field appears to map goal, not actual intake | Sync hydration value, goal, sweat loss, activity intake, and clarify typed field names |
| `blood-pressure` | Live-only | Add a first-class optional dataset for timestamped measurements |
| `weight` | Storage model exists; sync task exists, but sync does not plan/fetch it | Make weight/body composition syncable for date ranges, at least behind a `--body` or default health domain decision |
| `insights` | Live command refetches sleep/stress | Once sleep and stress coverage are complete, insights should be able to run from local synced data |

Acceptance criteria:

- Every health subcommand documents whether it is backed by sync.
- Every synced health row has coverage metadata for subdomains that may independently fail, such as sleep, HRV, stress detail, and body battery.
- Missing optional data is visible and does not block the rest of the daily row.

### Outcome 4: Performance Sync Matches Performance Commands

Performance is user-facing as daily data. Sync should not sample it weekly unless a specific metric is genuinely weekly.

The following domains should be covered:

| Command domain | Current sync status | Desired outcome |
| --- | --- | --- |
| `vo2max` | Synced, but typed model only has one VO2 value | Preserve run/cycling VO2 where available; preserve heat/altitude acclimation in raw payload or typed columns |
| `fitness-age` | Synced | Keep as daily performance coverage |
| `training-readiness` | Sync stores score only | Add level/status, sleep score, recovery time, HRV weekly average, acute load, and raw payload |
| `training-status` | Sync stores status phrase only | Add acute load, chronic load, load ratio, ACWR status, load focus, high/low aerobic and anaerobic load values/targets |
| `race-predictions` | Sync uses date-scoped daily endpoint, but live command still uses latest | Align live command and sync semantics; store date-specific 5K/10K/HM/marathon |
| `endurance-score` | Synced as overall score | Store classification and raw payload; keep daily coverage rather than weekly assumptions |
| `hill-score` | Synced as overall score | Store classification/factors and raw payload where useful |
| `lactate-threshold` | Live-only; sync fields are always null | Sync threshold HR and pace history into performance rows or a dedicated threshold dataset |
| `personal-records` | Live-only | Treat as snapshot/current-profile data or optional synced dataset; do not pretend it is date-range daily data |
| `performance-summary` | Live aggregation of several endpoints | Ensure every field shown by this command either exists locally after sync or is explicitly marked live-only |

Acceptance criteria:

- A date range sync creates performance rows for every requested date where Garmin returns performance data.
- `performance-summary -d DATE` has a local-data equivalent for all first-class fields.
- Weekly sampling is removed from the default performance contract unless documented as a deliberate rollup.

### Outcome 5: Activities Sync Includes the Data Users See in `activities get`

Activity list summaries are useful, but they are not enough to avoid split-brain. `activities get` exposes richer data than the list response.

Default sync should cover:

- Activity list discovery for the requested window.
- Full activity detail for each discovered activity.
- Summary DTO fields such as average HR, max HR, training load, training effect, anaerobic effect, training effect label/message, power, stamina, RPE/feel, body battery delta, and device/sensor metadata where available.
- Split/lap/interval summaries, at least as raw JSON if typed schema is too large for the first pass.
- Activity note/description, since the CLI now has note commands.
- GPX track points where exportable, including sensor extension streams already supported.

Opt-in or deferred:

- FIT archive mirroring.
- TCX/KML archive mirroring.
- Full chart/time-series payloads beyond GPX track point streams.

Acceptance criteria:

- If `garmin activities get ID` shows a supported summary field, sync stores it either typed or in raw activity-detail JSON.
- Local activity rows do not have missing HR/load fields when activity detail contains those values.
- The final sync report states how many activities were discovered, how many details were refreshed, and how many track files were available.

## Product Scope

### In Scope for Complete Sync v1

- Fix default date-range sync completeness for activities, health, and performance.
- Make row/date coverage checks authoritative.
- Fill the obvious live-vs-local gaps for current health and performance commands.
- Add activity-detail sync for discovered activities.
- Add coverage reporting in `sync status` and final sync summaries.
- Preserve raw payloads when typed schema expansion is not worth blocking the release.

### In Scope for Follow-Up

- New typed tables for detailed time-series health data: stress, heart rate, body battery.
- Weight/body composition sync as a normal health subdomain.
- Blood pressure sync for users who track it.
- Personal records/profile snapshots.
- Local-first implementations of `health insights` and `performance-summary`.

### Out of Scope for Complete Sync v1

- Mirroring every raw activity file format by default.
- Replacing the existing Parquet layout wholesale.
- Adding a new analytics engine.
- Guaranteeing data Garmin does not expose for the account, device, or date.

## Proposed User Experience

### Sync run

```bash
garmin sync run --from 2026-06-15 --to 2026-06-19
```

Expected final summary:

```text
Sync complete: Completed: 84, Failed: 0, Rate limited: 0

Coverage:
  Activities:     2026-06-15..2026-06-19 complete (10 activities, 10 details, 4 track files)
  Daily health:   2026-06-15..2026-06-19 complete (5 days)
  Performance:    2026-06-15..2026-06-19 complete (5 days)

Partial domains:
  Blood pressure: not enabled
  FIT archives: not enabled
```

### Sync status

```bash
garmin sync status
```

Expected useful readout:

```text
Storage: /Users/vicente/Library/Application Support/garmin
Profile: BITHENTE

Coverage:
  Activities:        latest 2026-06-18, no gaps in last 14 days
  Activity details:  latest 2026-06-18, no gaps in last 14 days
  Track points:      latest 2026-06-18, 6 activities without exportable GPS
  Daily health:      latest 2026-06-19, no gaps in last 14 days
  Performance:       latest 2026-06-19, no gaps in last 14 days

Pending tasks: 0
```

## Engineering Workstreams

### Workstream 1: Define the Sync Coverage Manifest

Create a small product-facing manifest of first-class sync domains and subdomains:

- Activities
- Activity details
- Track points
- Daily health
- Performance metrics
- Optional body/medical domains

The manifest should drive planning, status output, docs, and tests. It should make visible which live commands are fully backed by sync, partially backed by sync, or live-only.

### Workstream 2: Make Planning Row-Aware

Planning should inspect row-level coverage inside partition files:

- Activities: discovered activity IDs and dates in the requested window.
- Activity details: detail presence by activity ID.
- Track points: track availability by activity ID.
- Health: one row per date and profile, plus subdomain coverage.
- Performance: one row per date and profile, plus subdomain coverage.

Partition existence should never imply date-range completeness.

### Workstream 3: Promote Activity Details to First-Class Sync Data

Add a sync outcome for activity details:

- Fetch full details for discovered activity IDs.
- Store raw detail payload.
- Backfill key summary fields from detail payload into activity summary rows where the list response is sparse.
- Preserve split summaries and labels.

### Workstream 4: Complete Daily Health Coverage

Bring local sync into alignment with health commands:

- Keep usersummary as the base daily row.
- Add dedicated coverage for sleep, HRV, stress detail, body battery reports, heart rate, SpO2, respiration, intensity minutes, and hydration.
- Decide which values become typed columns now versus raw payloads.
- Add weight/body composition as a product decision: either include it in `--health` or expose a separate explicit domain.

### Workstream 5: Complete Daily Performance Coverage

Make performance daily by default:

- Sync every requested date, not every seventh date.
- Preserve raw endpoint payloads for debugging and future typed fields.
- Add typed fields for training load and load focus because those are central to user decisions.
- Populate lactate threshold fields from the existing live range endpoint or move them to a dedicated threshold dataset.
- Align `race-predictions` live command with date-scoped sync semantics.

### Workstream 6: Add Coverage-Aware Reporting

Improve sync output and status:

- Report requested window coverage by domain.
- Report gaps and partial subdomains.
- Report optional/live-only domains clearly.
- Keep file counts, but make them secondary.

### Workstream 7: Documentation and Trust Pass

Update README and release notes so users know:

- What default sync includes.
- What `--activities`, `--health`, and `--performance` narrow.
- What remains opt-in or live-only.
- How to verify local coverage.
- When to use `--force`.

## Launch Criteria

- `garmin sync run --from A --to B` fills missing rows for A..B without `--force`.
- `garmin sync run --dry-run --from A --to B` reports missing row counts by domain.
- `garmin sync status` reports latest dates and recent gaps for each first-class domain.
- Running live health/performance commands after sync does not reveal first-class data absent from local storage.
- Performance rows are daily for requested date ranges.
- Activity details are present for discovered activities.
- Existing Parquet readers remain compatible or have a documented migration path.
- Tests cover partially filled monthly and weekly partitions.

## Validation Scenarios

### Scenario 1: Partially synced current month

Given `daily_health/2026-06.parquet` exists with rows through June 14, when the user runs:

```bash
garmin sync run --from 2026-06-15 --to 2026-06-19
```

then health rows for June 15-19 are queued and written without `--force`.

### Scenario 2: Performance is available every day

Given Garmin returns readiness/status/race predictions for June 15-19, when the user syncs that range, then local `performance_metrics` contains one row for each available date, not only June 15.

### Scenario 3: Activity list is sparse but activity detail is rich

Given the activity list row has missing HR/load fields but `activities get ID` contains them, when sync completes, local storage has the richer supported values and raw detail payload.

### Scenario 4: Optional data is unavailable

Given no blood pressure or no SpO2 is available for a date, sync records that the subdomain was checked and unavailable. It does not keep retrying forever and does not block the rest of the date.

### Scenario 5: A downstream analysis uses only local data

Given a user asks for a two-week fitness assessment after sync, the analysis can use local Parquet data for activities, daily health, and performance without calling live health commands to patch missing rows.

## Risks and Tradeoffs

- More complete sync will make default runs slower and will hit more endpoints. The user outcome is worth it, but progress output and rate-limit behavior must stay clear.
- Some Garmin domains are sparse by device/account. Coverage reporting must make "not available" feel normal, not like a failure.
- Adding typed columns too aggressively can churn schema. Preserve raw payloads first where confidence is lower.
- Syncing activity details for every activity increases storage and API calls, but it closes a real trust gap exposed by recent runs.
- Date-scoped performance endpoints may still return latest-like values for some metrics. Tests should verify what the payload guarantees before making historical claims.

## Open Questions

- Should weight/body composition be part of default `--health`, or an explicit `--body` domain?
- Should blood pressure be default health sync, opt-in, or live-only until there is user demand?
- Should activity detail raw JSON live in `activities/*.parquet`, a separate `activity_details/*.parquet`, or both typed-backfilled summary plus detail table?
- Should `health insights` default to local synced data and offer a `--live` escape hatch?
- Should `performance-summary` gain a local mode, or should it remain live while sync catches up?
- How much raw payload should be kept by default before storage growth becomes a concern?

## Release Messaging

Headline: "Garmin CLI sync now means complete."

Supporting points:

- One command reconciles the full requested date range.
- Local Garmin data is finally trustworthy for analysis.
- Sync reports coverage, not just file counts.
- Health and performance metrics no longer require live command workarounds.
- Activity detail sync brings local training load, HR, labels, and splits closer to Garmin Connect.

## Success Metrics

- Fewer user workflows require `--force` after a normal date-range sync.
- `sync status` can explain local freshness without DuckDB inspection.
- Downstream personal analytics can run from local storage alone.
- New issues about "health command shows data but synced Parquet does not" trend toward zero.
- The README's sync promise matches actual shipped behavior.
