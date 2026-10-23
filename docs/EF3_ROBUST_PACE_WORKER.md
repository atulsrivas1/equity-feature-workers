# EF3 robust history, volume pace and mean-ratio worker

This source change implements the worker for [#612](https://github.com/atulsrivas1/equity-features/issues/612) over the
supplied-input adapter (io main `86cbff2`, `equity_feature_duckdb.robust_pace`) and the pure library (core main
`ee92759`, `equity_features.ef3.compute_ef3_robust`). It consumes contract E: `ef3-robust1` (`ef3.robust_history`, which
also serves the cohort outputs), `ef3-pace1` (`ef3.volume_pace_robust`) and `ef3-mean1` (`ef3.volume_ratio_mean`). It
contains no median, MAD, quantile, rank, ratio, z-score or mean mathematics. Source preparation does not establish
story, release or acceptance completion.

## Public API

- `equity_feature_workers.equity_robust_pace`: `make_robust_history_work`, `make_volume_pace_work`,
  `make_volume_ratio_mean_work` (build one work item) and `compute_robust_history_inputs`,
  `compute_volume_pace_inputs`, `compute_volume_ratio_mean_inputs` (calculation).
- `equity_feature_workers.ef1_runner`: `run_ef3_robust_history`, `run_ef3_volume_pace`, `run_ef3_volume_ratio_mean`,
  thin wrappers over the shared sequential `_run_ef1` (unchanged). The caller owns the sink.
- `examples/ef3_robust_pace.py --case <id> --output-db <new .duckdb> --html <new .html> --run-id <id>` and
  `examples/ef3_robust_pace.py --refusals`.

## Deviations from the EF3-D template (recorded)

1. **No process-level grid registry.** EF3-D binds a daily grid object by digest in a process-level registry. Here the
   library carries the complete window, population and grid through the configuration and the supplied role batches
   (`grid`, `sessions`, `bars`, `population`, `cohort_values`), so nothing is registered and nothing outlives a work item.
2. **Fixed nullable role slots.** The library takes fixed slots (`series, sessions, population, cohort_values` for
   history and cohort; `grid, sessions, bars` for pace; `series, sessions` for mean). The worker binds each supplied
   batch to its slot by the manifest role and leaves absent roles `None`. Cohort-only selections bind only
   `population` and `cohort_values`; an unused `sessions` batch would break result identity, so none is supplied.
3. **Cohort roles hold several instruments.** The `population` and `cohort_values` batches carry the declared members,
   so the per-instrument check is skipped for them; the library admits membership and the target row. The task has one
   instrument: the target member.
4. **No ordered history, no initialization digest.** `ordered_history` is false and `initialization_sha256` is `None`.

## Example and fixtures

Each of the 124 contract result fixtures (44 history H01-H40 plus prefix and variants, 24 cohort C01-C20 with a, b and
10b variants, 37 pace, 19 mean) is built through the I/O adapter and run once per selected output plus one joint run when
several outputs are selected. Every run is compared with the frozen contract literal (value by the contract's acceptance
rule, status, reasons, counts, per-output witnesses for single-output runs), then SDK readback, idempotent replay and
close/reopen. Prefix fixtures (H25, R16) append later governed sessions; the market cutoff is set to the last appended
session's close because the batch validator refuses rows after the cutoff, while the target stays the configured
session. The result must equal the base fixture, and does.

An earlier author run had 26 failures (all cohort cases, H25, R16), all caused by the example, not the worker or library:
the cohort inputs were bound with a four-slot tuple against two role names, so a `sessions` batch was bound as
`cohort_values`, and the prefix cases used a cutoff before the appended sessions.

## Refusals

`--refusals` drives all 48 refusal fixtures (RF01-RF48): the adapter builds the batches (shared validators refuse some
there), the worker builds the work item (admission and manifest), then the calculation runs. For each fixture the stage,
the contract code, the library's own code for the same inputs and the worker's code are recorded. Result: all 48 match the
contract code via the library code. Nothing is published on any refusal.

| Stage | Count | Worker code |
| --- | --- | --- |
| Adapter build (batch or configuration refused while the adapter builds it) | 14 | worker never reached |
| Worker admission (work item build) | 16 | `INVALID_COMMAND` 13 (configuration, version, feature); `RESULT_IDENTITY_MISMATCH` 3 (RF09, RF11, RF42) |
| Compute (library) | 17 | `RESULT_IDENTITY_MISMATCH` 10 (all `INCONSISTENT_IDENTITY`); `INVALID_COMMAND` 7 |
| Library capability call only | 1 (RF31) | not a worker path: the worker exposes no update or merge |

Stage is not the same as who raises the code: RF17 is library-raised per the contract but refused at adapter build (the config builder), and RF06 is shared-validator-raised but refused at compute (the batch is valid on its own; the cutoff check runs inside the library). The worker collapses library codes into its own taxonomy, so the contract code is visible in the driver's record only.

## Not executed

mypy, unit tests (policy: none), CI, an independent oracle beyond the contract literals, installed-wheel qualification,
real or observed data, source-rights and release acceptance (EQ-157), and RF31 through the worker (no such path).
Library and adapter mathematics were not changed. Timing: the full results demo ran in 4 parallel shards (H, C, P, R) in roughly 50 minutes wall time, about 1.5 minutes per case per shard under that load (the reviewer saw about 10 seconds per case run alone).
