# Simplify Sync, CLI Surface, and Docs (garmin-cli)

## Context & Problem
- The project is functionally healthy, but the product surface has drifted.
- The README documents commands and flags that the binary does not support.
- The sync experience defaults to a TUI that auto-closes on completion, which makes successful runs feel abrupt and ambiguous.
- The sync feature set is split across `sync` and a separate top-level `watch` command, increasing conceptual and maintenance overhead.
- The global `--format` flag is exposed everywhere, but is not wired into command dispatch and appears to be dead surface area.

## Current State (as of 2026-03-08)
### Confirmed mismatches
- README documents unsupported activity filters:
  - `garmin activities list --type`
  - `garmin activities list --from ... --to ...`
- README documents unsupported flag shapes:
  - `garmin activities download --format fit` but CLI uses `--type`
  - `garmin health weight-add --weight 80.2` but CLI expects positional `weight`
- README states sync uses 3 concurrent workers, while implementation uses 4.
- README documents `--format json/csv` as a meaningful global feature, but command dispatch does not consume `cli.format`.

### Confirmed UX problems
- Default `garmin sync run` uses the TUI unless `--simple` is passed.
- The TUI exits automatically one second after completion.
- Fancy mode suppresses any post-run terminal summary after the TUI exits.
- `sync status` omits `performance_metrics`, despite performance sync being a first-class dataset.
- `sync status` tells users to run `garmin sync`, but the real command is `garmin sync run`.

### Confirmed surface complexity
- Sync-related entry points currently include:
  - `garmin sync run`
  - `garmin sync status`
  - `garmin sync reset`
  - `garmin sync clear`
  - `garmin watch`
- `watch` has overlapping scope with `sync`, but lives as a separate top-level concept.

## Goals
1. Make the docs fully match the shipped binary.
2. Reduce the sync experience to the smallest clear interface.
3. Remove or hide dead flags and stale concepts.
4. Make successful sync runs produce an unambiguous completion signal.
5. Decide whether the TUI earns its maintenance cost.

## Non-Goals
- Rewriting the sync engine internals unless required for the surface cleanup.
- Adding new Garmin data endpoints.
- Redesigning output formatting for every existing command in the same pass.

## Product Direction
### 1) Treat sync as a single feature area
- The primary user-facing concept should be `sync`.
- `watch` should not remain a separate top-level mental model unless it proves materially better than `sync watch`.

### 2) Prefer explicit, durable terminal output over transient UI
- Default behavior should optimize for clarity in normal shells.
- A TUI is acceptable only if it is clearly better than line-oriented progress output and does not hide completion state.

### 3) Docs should be generated from reality, not aspiration
- README examples should be validated against `--help` output and actual command behavior.
- Claims about concurrency, storage, and supported flags should come from the current implementation.

## Recommended Decisions
### Decision A: Simplify the sync UX now
- Change the default sync output to simple text mode.
- Keep the TUI only behind an explicit opt-in flag such as `--tui`, or remove it if the value proposition is weak.

### Decision B: Collapse sync-related commands under `sync`
- Preferred end state:
  - `garmin sync run`
  - `garmin sync status`
  - `garmin sync reset`
  - `garmin sync clear`
  - `garmin sync watch` if scheduled syncing is kept
- Alternative:
  - Remove scheduled syncing entirely for now if it is not actively used.

### Decision C: Remove dead `--format` surface unless it is implemented immediately
- Best option for this cleanup pass: remove global `--format`.
- If structured output is still desired, reintroduce it later as a real contract with tests.

## Execution Plan

### Phase 1: Documentation Truth Pass
Goal: eliminate trust gaps before deeper interface changes.

#### Tasks
- Audit README examples against compiled `--help` output.
- Remove unsupported examples and flags.
- Fix sync wording to match the current command names.
- Correct concurrency claims.
- Trim README sections that over-explain internal architecture if they are not part of the product promise.
- Decide whether the README should mention the TUI at all if it becomes opt-in or is removed.

#### Acceptance Criteria
- Every command shown in the README runs with the documented flags.
- README does not advertise unsupported output modes or stale sync behavior.
- `cargo run -q -- --help` and README tell the same story.

### Phase 2: CLI Surface Reduction
Goal: make the command tree smaller and clearer.

#### Tasks
- Remove the global `--format` flag and enum, unless full support is implemented in the same change.
- Review all command help text for stale descriptions inherited from removed features.
- Decide whether `watch` is:
  - moved to `sync watch`, or
  - removed entirely
- If `watch` is kept, make its scope and difference from `sync run` explicit in help text and docs.

#### Acceptance Criteria
- No global flags exist without runtime behavior.
- Sync-related help output fits a single coherent mental model.
- The top-level command list is smaller or clearer than it is today.

### Phase 3: Sync UX Simplification
Goal: make normal sync runs obvious and low-friction.

#### Tasks
- Change default sync execution to simple text output.
- Introduce explicit `--tui` only if the TUI is kept.
- Always print a final sync summary after completion, regardless of mode.
- Ensure dry-run and status flows produce direct, non-ambiguous summaries.
- Fix `sync status` to include `performance_metrics`.
- Fix `sync status` empty-state guidance to reference `garmin sync run`.

#### Acceptance Criteria
- `garmin sync run` is understandable without prior knowledge of the TUI.
- Completion always leaves a visible summary in the shell.
- `sync status` reports all first-class sync datasets.

### Phase 4: TUI Keep-or-Delete Decision
Goal: resolve whether the TUI remains part of the product.

#### Option 1: Keep the TUI as opt-in
- Rename the mode from implicit “fancy UI” to explicit TUI.
- Keep it behind `--tui`.
- Remove auto-close on completion.
- Require an explicit keypress to exit after final state is shown.
- Print a summary after restoring the terminal.

#### Option 2: Remove the TUI
- Delete the TUI code path and dependencies.
- Remove `ratatui` and `crossterm`.
- Replace with consistent progress lines and final summaries.

#### Decision Criteria
- Keep only if it provides clear value for long-running syncs.
- Remove if it mainly adds complexity, maintenance burden, or surprising terminal behavior.

#### Acceptance Criteria
- No remaining UI mode exits in a confusing way.
- The chosen output mode is consistent with the product’s likely usage pattern.

### Phase 5: Scheduled Sync Decision
Goal: resolve whether background-style syncing belongs in this CLI.

#### Tasks
- Evaluate whether `watch` is a real user need or leftover engineering ambition.
- If it stays:
  - move under `sync watch`
  - document one clear use case
  - keep output intentionally minimal
- If it goes:
  - remove command, docs, and tests
  - keep the sync model focused on explicit runs

#### Acceptance Criteria
- Scheduled sync is either clearly supported and easy to explain, or removed.
- There is no duplicate conceptual entry point for “syncing Garmin data locally.”

## Suggested Implementation Order
1. Remove dead global `--format` surface.
2. Fix `sync status` output and messages.
3. Flip sync default to simple output and add a guaranteed final summary.
4. Decide TUI fate:
   - keep as `--tui`, or
   - remove entirely
5. Decide `watch` fate:
   - migrate to `sync watch`, or
   - remove entirely
6. Rewrite README against the resulting command tree.
7. Regenerate or verify help text snapshots manually before release.

## Testing Plan
### CLI verification
- Run `cargo run -q -- --help`
- Run `cargo run -q -- sync run --help`
- Run `cargo run -q -- sync status --help`
- If kept, run `cargo run -q -- sync watch --help` or `cargo run -q -- watch --help`

### Regression tests
- Add tests for sync completion summaries in default mode.
- Add tests for `sync status` dataset reporting, including `performance_metrics`.
- Add tests for help output or argument parsing where surface changes are meaningful.
- Update or remove tests tied to deleted flags or commands.

### Validation
- `cargo test -q`
- Manual smoke test of `sync run --dry-run`
- Manual smoke test of any retained TUI path in an interactive terminal

## Risks & Tradeoffs
- Removing `--format` may disappoint users if they rely on undocumented behavior expectations, but the current surface is misleading and should not be preserved just because it exists in help output.
- Removing the TUI lowers complexity, but sacrifices a richer progress display for long-running syncs.
- Removing or relocating `watch` simplifies the CLI, but may affect any personal workflows already built around it.
- README reduction may feel less impressive, but it will become more trustworthy.

## Open Questions
- Is anyone actually using `watch` regularly?
- Is anyone relying on the TUI as the preferred sync experience?
- Is structured output a real product requirement, or just an unfinished idea?
- Should the README emphasize sync/parquet analysis at all, or should it lead with the direct API commands and treat sync as an advanced workflow?

## Success Criteria
- A new user can infer the correct sync flow from `garmin --help` alone.
- The README matches the executable with no known flag drift.
- `garmin sync run` ends with a clear, visible summary.
- The sync feature area has one obvious mental model.
- The codebase sheds at least one of:
  - dead global format support
  - implicit TUI default
  - separate top-level `watch`

## Recommended First Cut
- Make this a two-PR cleanup if you want to keep risk low.

### PR 1: Trust and clarity
- Remove dead `--format`
- Fix `sync status`
- Rewrite README to match reality
- Flip default sync output to simple mode with final summary

### PR 2: Surface reduction
- Decide TUI keep/remove
- Decide `watch` keep/move/remove
- Remove unused dependencies and tests
