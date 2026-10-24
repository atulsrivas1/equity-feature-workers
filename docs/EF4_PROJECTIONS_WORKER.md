# EF4 statistical projections workers

This source change implements the workers for [EQ-172 #513](https://github.com/atulsrivas1/equity-features/issues/513)
(frozen contract I2) over the supplied-input adapter (io main `4ebfa30`, `equity_feature_duckdb.ef4_projections`,
`docs/EF4_PROJECTIONS_IO.md` in the io repository) and the pure library (core main `2517ce5`,
`equity_features.ef4.compute_ef4_path_projections`). It covers four producers and 26 outputs: `ef4.session_projection`
(7 `projection.*`), `ef4.bar_projection` (6 `projection.*`), `ef4.relative_excess` (6 `relative.session.*` and 2
`core.benchmark_*`) and `ef4.rate_projection` (5 `rate.window.*`). It contains no return, excess, drawdown, SMA,
dollar-volume, rate or comparison mathematics. Source preparation does not establish story, release or acceptance
completion.

## Public API

- `equity_feature_workers.equity_projections`: `make_session_projection_work`, `make_bar_projection_work` (take the library
  grid) and `make_relative_excess_work`, `make_rate_projection_work` (build one work item), and the matching
  `compute_*_inputs` calculations.
- `equity_feature_workers.ef1_runner`: `run_ef4_session_projection`, `run_ef4_bar_projection`, `run_ef4_relative_excess`,
  `run_ef4_rate_projection`, thin wrappers over the shared sequential `_run_ef1` (unchanged), additive to the existing
  runners. The caller owns the sink.
- `examples/ef4_projections.py --case <id> --output-db <new .duckdb> --html <new .html> --run-id <id> --harness <file>
  [--binding A|B]` and `examples/ef4_projections.py --refusals [--only SPR01,RXR05] --harness <file>`.
- Tools: 111 cases registered in `tools/ef1_results_demo.py`; `tools/ef1_readable_view.py` labels the projection, relative,
  core-benchmark and rate runs by selected output or `joint` (otherwise they would read as `prefix length N, series A`).

## Design

1. **Fixed nullable role slots.** The library takes fixed slots: session `(daily,)`, bar `(bars,)`, relative excess
   `(legs, membership)` and rate `(tape, legs)`. The worker binds each supplied batch to its slot by the manifest role and
   leaves a role that was not delivered `None`. Which roles an output needs is the library's decision.
2. **One batch per role (pinned ruling).** The worker admits at most one batch per role (distinct input ids). The epoch bars
   arrive as one batch with per-slot coverage `bar:<i>` (session projection `slot:<i>`) from the adapter; the worker does not
   concatenate chunks.
3. **Closed dispatch.** Calculation calls `compute_ef4_path_projections`, routed by the configuration identity. Grid
   producers pass their grid, fact producers the result entity (target instrument, configured session).
4. **Grid by digest.** The library carries no grid in the configuration, only `grid_sha256`. As the EF3 daily routes, the
   grid is registered under its own digest when the work item is built and calculation resolves it by the digest the
   configuration binds (the library verifies it again). The manifest carries the digest as initialization digest, the
   daily warmup sessions are the `window` sessions before the target, and the bar grid has no warmup. The registry is
   content-addressed and in-process, so the session and bar routes are for the sequential in-process runner only (process mode fails with `INVALID_COMMAND` for them because the child process has an empty grid registry; the parallel demo works because it runs one process per case); relative
   excess and rate need no registry.
5. **Headers from the library.** Feature headers come from `ef4_path_projections_definitions(config, grid=...)`, which
   describes only the outputs the configuration can execute (parameters are present exactly when their outputs are
   selected). Selecting an output the configuration cannot execute is refused at work-item build.
6. **Target instrument rule.** Every row of the daily, bars, membership, tape and rate-legs batches must carry the target
   instrument. The relative-excess legs batch also carries the benchmarks' legs, so the library alone decides on its
   instruments (undeclared instrument, self benchmark).
7. **Pinned rulings hold.** `core_benchmark_ids` is the adapter's canonical compact JSON text and the worker passes the
   configuration unchanged; REFERENCE evidence boundary is `ordinary` (the library's; observed on the stored relative-excess and
   rate results, while slot and bar witnesses are `completed_interval`); input ids are unique (refused otherwise); one batch per role.
8. **Nothing is hardcoded.** Entity, instrument, benchmark, session, calendar, grid and reference ids come from the task
   manifest, the configuration or the grid. The example binds the compact fixture names (`X`, `P`, `S1`, `D0`...) to two
   synthetic id sets (`--binding A` and `B`); the results do not depend on which is used.
9. **No ordered history for the fact producers**, no registry for them, and the one governed interval is the configured
   target session (daily: the grid sessions).

## Example and fixtures

Each of the 111 contract result fixtures (22 `SP`, 19 `BP`, 39 `RX` including the ten core-benchmark fixtures, 31 `RP`) is
built through the I/O adapter and run with all of its listed outputs (the joint run, the only selection valid for a
relative-excess or rate configuration whose optional parameters follow the selected outputs). Session and bar fixtures
with several outputs are also run once per selected output (their configuration has no per-output parameter), so a case
page can hold up to 8 runs. Every run is compared with the frozen contract literal (value by the contract's acceptance rule
`exact_bool`, `exact_integer` (int64 and decimal128 coefficient), `exact_float64`, `exact_plus_zero` or `tolerance`; status,
reasons, expected and observed counts; witnesses in order, except that a joint run whose result-wide evidence cap is below
the total witness count skips only the witness comparison), then SDK readback, idempotent replay and close/reopen.
Fixture mutations use the contract harness (`default_ctx`, `set_path`); the example needs
`--harness <core>/tools/contract_fixtures/ef4i_fixture_expand.py` (or `EF4I_HARNESS`; the results demo inherits the
environment variable). The harness used has sha256 `ffc65b2156acaadcfb1969612cbd88e4f628e025847dfd62e0585cf8a66150b8`
(the committed blob with LF endings; the example prints the sha256 of the file it loaded). `contract_document()` works only
from a source checkout of core; otherwise pass `--contract <PATH_PROJECTIONS_CONTRACT_PROPOSAL.json>`. Prefix pairs
(`SP01`/`SP17`, `BP01`/`BP16`, `SP02`/`SP12`, `BP01`/`BP12`, `RX01`/`RX18`) are separate fixtures with identical results.

## Refusals and where each is refused

`--refusals` drives all 114 refusal fixtures (22 `SPR`, 24 `BPR`, 37 `RXR`, 31 `RPR`). The adapter builds the batches and
the hand-built envelope, the caller builds the library grid, the worker builds the work item (admission), then the
calculation runs. The record carries the stage, the contract code, the contract's recorded layer, the library's own code
for the same inputs and the worker code. Build time means the work item is never created; compute time means the work item
exists and the library refuses it when the batches are processed.

| Stage | Count | Time | Worker code | Recipes |
| --- | --- | --- | --- | --- |
| Adapter build, shared batch rules or the adapter | 26 | build | worker never reached | SPR08 SPR09 SPR10 SPR11 SPR18 BPR05 BPR06 BPR07 BPR08 BPR09 BPR10 BPR11 BPR13 BPR14 BPR15 BPR20 RXR20 RXR22 RXR26 RXR27 RXR28 RXR33 RPR16 RPR17 RPR23 RPR27 |
| Adapter build, no explicit unit for the envelope | 4 | build | worker never reached | SPR17 BPR19 RXR32 RPR26 |
| Worker admission | 35 | build | `INVALID_COMMAND` (configuration parameters, version, unknown feature, an output the configuration cannot execute) | SPR01 SPR02 SPR03 SPR04 SPR05 SPR06 SPR07 SPR19 BPR01 BPR02 BPR03 BPR04 BPR21 RXR01 RXR02 RXR03 RXR04 RXR05 RXR08 RXR09 RXR10 RXR11 RXR12 RXR13 RXR14 RXR34 RPR01 RPR02 RPR03 RPR06 RPR07 RPR08 RPR09 RPR10 RPR28 |
| Worker admission, other-instrument row | 4 | build | `RESULT_IDENTITY_MISMATCH` | SPR14 RXR16 RXR17 RPR22 |
| Library compute | 27 | compute | `INVALID_COMMAND` | SPR12 SPR13 SPR15 SPR16 BPR17 BPR18 RXR06 RXR07 RXR19 RXR21 RXR23 RXR24 RXR25 RXR29 RXR30 RXR31 RPR04 RPR05 RPR11 RPR12 RPR13 RPR18 RPR19 RPR20 RPR21 RPR24 RPR25 |
| Library compute, overflow | 3 | compute | `CALCULATION_FAILED` | BPR12 RPR14 RPR15 |
| Library compute, inconsistent identity | 3 | compute | `RESULT_IDENTITY_MISMATCH` | BPR16 RXR15 RXR18 |
| Library capability call only (update, restore or merge) | 12 | **no worker path** | the worker exposes no such path | SPR20 SPR21 SPR22 BPR22 BPR23 BPR24 RXR35 RXR36 RXR37 RPR29 RPR30 RPR31 |

Totals: 30 refused while the adapter builds, 39 at work-item build, 33 at compute, 12 with no worker path (114). For every
fixture the contract code equals the code of the layer that refused (114 of 114), and every compute-time worker code
follows the translation of the shared worker layer. Results were identical under both id bindings.

Layer attribution against the contract's recorded `raised_by`. The 13 recipes the I/O record lists are unchanged: 11
recorded as producer admission are refused while the adapter builds the batch (SPR10 SPR11 SPR18 BPR10 BPR20 RXR22 RXR26
RXR27 RXR28 RXR33 RPR27), and SPR12 and SPR13, recorded as shared validator, are refused at compute after the batches
build. The worker adds these differences. Every config-tier and version-tier refusal of the library (INVALID_CONFIG,
INCOMPATIBLE_VERSION, UNKNOWN_FEATURE) is refused at work-item build, because the worker decodes the configuration to
build the headers, earlier than the library's compute call. Four recipes recorded as producer admission or library
identity (SPR14, RXR16, RXR17, RPR22) are refused at admission by the worker's target-instrument row check, with the same
code class `INCONSISTENT_IDENTITY` that the library gives for the same inputs. Four envelope recipes (SPR17, BPR19,
RXR32, RPR26: no explicit price unit) are refused by the adapter, which cannot hand-build a configuration without a unit.
The 12 capability recipes have no worker path: the worker exposes only the batch route. Nothing is published on any refusal.

## Profile and run shape

One worker case, uncontended, whole process (import, adapter, work items, publication, readback, replay, reopen, HTML):
`RX01` 3.5 s (1 run), `RP01` 4.1 s (1 run), `BP01` 10.7 s (7 runs), `SP01` 13.0 s (8 runs); about 1.5 s per run, dominated by
publication and full readback in the sink. The 111 cases were run as one subprocess per case, 28 in parallel on a 64-core
machine, for each of the two bindings (both sets concurrently): 159.5 s wall per set, mean 39 s per case under that
contention. The 114 refusals run in one process in a few seconds (no sink).

## Not executed

mypy, unit tests (policy: none), CI, an independent oracle beyond the contract literals, installed-wheel qualification, real
or observed data, source rights and release acceptance, performance claims, the 12 capability recipes through the worker (no
such path), and a separate-process runner for the grid routes (the registry is in-process). Library and adapter mathematics
were not changed. The full 111-case results demo is left to qualification; nine cases were produced through the demo for
the display check (`SP01`, `SP17`, `BP01`, `BP16`, `RX01`, `RX28`, `RX29`, `RP01`, `RP31`).
