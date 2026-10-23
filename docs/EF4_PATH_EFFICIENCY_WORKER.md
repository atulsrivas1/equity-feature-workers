# EF4 path efficiency and choppiness worker

This source change implements the worker for EQ-161 (path efficiency and choppiness, frozen contract `ef4-path1`,
config `ef4.path_efficiency`, algorithm `ef4-path-efficiency-v1`) over the supplied-input adapter (io main `19a5dde`,
`equity_feature_duckdb.path_efficiency`, io PR #40) and the pure library (core main `2517ce5` containing PR #708,
`equity_features_intraday.compute_path_efficiency`). It produces `path.choppiness_fraction`, `path.efficiency_ratio`,
`path.log_efficiency_ratio`, `path.net_displacement`, `path.path_length`, `path.true_range_sum` and
`path.window_range`. It contains no displacement, path length, ratio, logarithm, true range, window range or choppiness
mathematics. Source preparation does not establish story, release or acceptance completion.

## Public API

- `equity_feature_workers.equity_path_efficiency`: `make_ef4_path_efficiency_work` (builds one work item) and
  `compute_path_efficiency_inputs` (calculation).
- `equity_feature_workers.ef1_runner.run_ef4_path_efficiency`: thin wrapper over the shared sequential `_run_ef1`
  (unchanged); the caller owns the sink.
- `examples/ef4_path_efficiency.py --case <PE01..PE31> --output-db <new .duckdb> --html <new .html> --run-id <id>` and
  `examples/ef4_path_efficiency.py --refusals`.

## Design (recorded)

1. **Template.** The route follows the EF3 realized-variance route (the same R17 `IntradayContext` carried in the
   chunked configuration). No process-level registry: the configuration carries the whole governed context and
   nothing outlives a work item.
2. **One nullable `bars` role, one manifest per chunk.** The adapter delivers one BAR chunk per actual session; the
   library names them `bars:NNN`, and the worker requires one input manifest per chunk bound to exactly that batch
   (role-keyed admission is the shared `admit`). A missing delivery is a single manifest in state `missing` with no
   batch, and the library then reports `absent_input`.
3. **No hard-coded identities.** Instrument, session, calendar, grid, window, epoch profile and price unit are read
   from the configuration's decoded context or from the supplied batches. The worker owns no default.
4. **Headers from the library.** Feature ids, algorithm version and unit come from `describe_path_efficiency()`
   (dtype float64); the worker requires the `batch` capability for each selected output.
5. **Hosting.** Per the core implementation record the producer lives in `packages/intraday`; the worker imports
   `equity_features_intraday` (same package as the EF3 realized variance worker).
6. **No ordered history, no warm-up.** `ordered_history` is true, `warmup_sessions` is empty and
   `initialization_sha256` is the context digest (as the realized variance route).

## Refusals: where each one is raised

Stage definitions. **Adapter** (before the worker): the I/O builders or the shared batch validators refuse while the
adapter builds the batches or configuration, so no work item exists. **Build** (worker admission): the work item is
built; the library config decoder, the manifest binding and the feature checks run here; the worker raises
`INVALID_COMMAND` (configuration, version, feature) or `RESULT_IDENTITY_MISMATCH` (identity). **Compute**: the library
runs inside the worker calculation (`compute_path_efficiency_inputs`) and its typed refusal is collapsed into the
worker taxonomy. A refusal publishes nothing. The contract code is visible in the driver record (`library_code`);
the worker code is its own.

| Recipe | Contract code | Stage | Worker code |
| --- | --- | --- | --- |
| PR01 | DUPLICATE | adapter (shared validator) | worker not reached |
| PR02 | INVALID_ORDER | adapter | not reached |
| PR03 | INCONSISTENT_IDENTITY | compute | RESULT_IDENTITY_MISMATCH |
| PR04 | BOUNDS | adapter | not reached |
| PR05 | BOUNDS | compute (forming bar past the cutoff) | INVALID_COMMAND |
| PR06 | BOUNDS | adapter | not reached |
| PR07-PR11 | INVALID_SCHEMA | adapter | not reached |
| PR12-PR14 | INVALID_SCHEMA | adapter (wire constructor) | not reached |
| PR15 | OVERFLOW | adapter (wire constructor) | not reached |
| PR16 | INCONSISTENT_IDENTITY | compute (other session) | RESULT_IDENTITY_MISMATCH |
| PR17 | INCONSISTENT_IDENTITY | build (instrument differs, shared `admit`) | RESULT_IDENTITY_MISMATCH |
| PR18-PR25 | INVALID_CONFIG | adapter (config builder); as a raw configuration: build | INVALID_COMMAND |
| PR26-PR30 | INVALID_CONFIG | build | INVALID_COMMAND |
| PR31 | INCOMPATIBLE_VERSION | build | INVALID_COMMAND |
| PR32, PR33 | INVALID_UNIT | compute (batch unit versus config unit) | INVALID_COMMAND |
| PR34 | INVALID_UNIT | adapter (no price unit); as a raw configuration: build | INVALID_COMMAND |
| PR35 | UNSUPPORTED_ADJUSTMENT | adapter | not reached |
| PR36 | UNKNOWN_FEATURE | build | INVALID_COMMAND |
| PR37-PR39 | UNSUPPORTED_CAPABILITY | not a worker path | the worker exposes only `batch`; library code recorded |

Counts from the run: adapter 23 (of which 9 configuration refusals, PR18-PR25 and PR34, were also driven into the
worker as raw configurations and refused at build); build 8 (PR17, PR26-PR31, PR36); compute 5 (PR03, PR05, PR16,
PR32, PR33); not a worker path 3 (PR37-PR39). Stage is not the same as who raises the code: PR17 is library-raised per
the contract but the shared `admit` refuses it at build, and PR32, PR33 and PR05 are library codes that surface at
compute.

## Example and verification

Each contract result fixture is expanded from the compact notation to supplied rows (market cutoff C is the end of the
last delivered slot, K = C + 100 s, `ka` mapped to K - (100 - ka) s), built through the I/O adapter and run once per
selected output plus one joint run when several outputs are selected (8 runs for a 7-output fixture). Every run is
compared with the frozen contract literal (value by the contract acceptance rule: bit-exact `exact_float64`,
bit-exact `+0.0`, R17 tolerance; status, reasons, expected and observed counts, per-output witnesses; the joint run
compares the flat evidence list truncated at the result-wide `evidence_limit`), then SDK readback, idempotent replay and
close/reopen. Slots after an explicit target (prefix fixture PE25) are dropped because the R17 context ends at its
target; PE25 matches its own contract literal, which equals PE04.

Executed manually (Python 3.12, core main `2517ce5`, io main `19a5dde`, no unit tests, per owner policy): 19 of the 31
result fixtures end to end (adapter, worker, library, sink, HTML), spread across the outputs and groups (PE01-PE06, PE09,
PE12, PE13, PE16, PE20-PE23, PE25, PE27-PE29, PE31, including the prefix fixture PE25, the `governed_history` fixture
PE28, the close-only fixture PE20, the scale fixture PE21 and the evidence-limit fixtures PE22 and PE23): 136 runs, all
matching the contract literal, with verified readback, replay and reopen. All 39 refusal recipes reached the stage above
and the library code equals the contract code for all 39. The full 31-fixture results demo (registered in
`tools/ef1_results_demo.py`) is run once at qualification and was not run here.

Timing (one worker case, whole process, Windows): PE04 with 8 runs took 12.7 s alone, about 1.6 s per run; a one-run
case (PE22, PE23) took 3 to 4 s; under 6-way parallel load cases took 11 to 15 s; all 39 refusals together took 1.2 s.
The full demo is therefore about 6 minutes serial.

Readable view: `tools/ef1_readable_view.py` labels multi-run path cases by the selected output (`path.*`) or `joint`
instead of `prefix length N` (the label the EF3-D readable pages showed on non-prefix runs). The display check generated
readable pages for all 19 executed cases (all non-prefix except PE25), text-checked that no page contains `prefix
length` or `series`, that each run heading equals the output or `joint`, and that all 236 displayed values (repr and
stored hex text) equal the values in the canonical JSON.

## Not executed

mypy, unit tests (policy: none), CI, an independent oracle beyond the contract literals, installed-wheel qualification,
the remaining 12 result fixtures through the worker (PE07, PE08, PE10, PE11, PE14, PE15, PE17-PE19, PE24, PE26, PE30;
covered by the I/O-layer run and the full demo at qualification), windows near 4095 through the worker (the work-item
input limit is 1 MiB as for the other intraday route), real or observed data, source-rights and release acceptance
(EQ-163), and PR37-PR39 through the worker (no such path). Library and adapter mathematics were not changed.
