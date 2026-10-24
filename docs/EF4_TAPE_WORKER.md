# EF4 tape window and notional top-K workers

This source change implements the workers for [EQ-158 #492](https://github.com/atulsrivas1/equity-features/issues/492)
and [EQ-162 #496](https://github.com/atulsrivas1/equity-features/issues/496) (frozen contract G) over the supplied-input
adapter (io main `4ebfa30`, which contains the EF4-G merge `77fde51`; `equity_feature_duckdb.tape`) and the pure library (core
main `b6c5063`; `equity_features.ef4`: `compute_ef4_tape`, `merge_trade_top_k_notional`). It covers `ef4-tape1`
(`ef4.tape_window_sign`, nine `tape.window.*` and `tape.tick.*` outputs), `ef4-notional1` (`ef4.trade_top_k_notional`,
`tape.top_k_notional`) and the top-K partition merge. It contains no tick-rule, window-total, notional, rank or merge
mathematics. Source preparation does not establish story, release or acceptance completion.

## Public API

- `equity_feature_workers.equity_tape`: `make_tape_window_work`, `make_tape_top_k_work`, `make_tape_top_k_merge_work` (build
  one work item) and `compute_tape_window_inputs`, `compute_tape_top_k_inputs`, `compute_tape_top_k_merge_inputs`.
- `equity_feature_workers.ef1_runner`: `run_ef4_tape_window`, `run_ef4_tape_top_k`, `run_ef4_tape_top_k_merge`, thin wrappers
  over the shared sequential `_run_ef1` (unchanged), additive to the existing runners. The caller owns the sink.
- `examples/ef4_tape.py --case <id> --output-db <new .duckdb> --html <new .html> --run-id <id> --harness <file>` and
  `examples/ef4_tape.py --refusals [--only RF01,RF41] --harness <file>`; `--binding 0|1` selects one of the harness's two
  independent identity bindings.
- Tools: 82 cases registered in `tools/ef1_results_demo.py`. `tools/ef1_readable_view.py` labels `tape.` runs by output (or
  `joint`, or `(merge of N partition inputs)`) instead of `prefix length N, series A`, and shows a stored top-K cell as a table
  of its stored rows in stored (notional-rank) order.

## Design

1. **Fixed nullable role slots.** Window `(trades, seed)`, top-K `(trades,)`: the worker binds each supplied batch to its slot
   by the manifest role and leaves a role that was not delivered `None`. An absent trade batch and `seed_policy` handling are
   the library's decisions, not the worker's.
2. **Merge by role tree.** The merge work item takes one TRADE batch per partition. Its manifest roles spell the merge tree
   (`p0.trades`, `p1.trades` for a flat merge; `p0.p0.trades`, `p0.p1.trades`, `p1.trades` for `merge(merge(P1,P2),P3)`), the
   same `p<i>.` prefixing the library applies to the bindings of a merged result, so the task identity covers the tree. The
   worker computes one partial per leaf with the library top-K producer and merges along the tree with
   `merge_trade_top_k_notional`. All partitions share the one reviewed configuration of the task: a partition cannot carry its
   own K, evidence limit, policy version or knowledge cutoff (see the refusals without a worker path). What a batch itself
   carries (instrument, price unit, adjustment, scope policy and bounds) is checked by the library against that
   configuration, so a partition that differs there is refused at compute (or at admission for another instrument).
3. **Nothing is hardcoded.** Entity, instrument, session, role keys, seed id and snapshot come from the task manifest, the
   configuration or the batches. Headers come from the library's own `ef4_tape_definitions(config)`.
4. **Notional-ordered top-K cells.** The cell is stored and read back by the io-sdk codec fix (the decoder keeps the shared
   constructor check and reads the stored fields for `tape.top_k_notional` only). Every top-K and merge fixture is stored in the
   separate DuckDB, read back with full SDK verification, replayed and re-read after close/reopen; TK01 (notional order differs
   from size order) and TK15 (closing-auction boundary) pass this path. The worker never rebuilds a cell.
5. **Governed interval, no ordered history, no initialization digest, no process-level registry**, as the other EF3 and EF4
   workers.
6. **Evidence bound.** The library publishes the fixed bound `evidence_limit * 9` for a window result (library note 2); the
   example sizes the sink caps to the sink's own maxima.

## Example and fixtures

Each of the 82 contract result fixtures (50 window `W01-W22b`, 25 top-K `TK01-TK19`, 7 merge `PM01-PM06`) is built through the
I/O adapter and run: a window fixture once per selected output plus once jointly; a top-K fixture once; a merge fixture runs
every partition through the top-K worker (against the partition's literal partial) and the merge through the merge worker
(against the merged literal; PM06 also runs the inner merge). Every run is compared with the frozen contract literal (value by
the contract's acceptance rule, status, reasons, expected and observed counts, witnesses, and for the top-K the rows, K and
evidence boundaries), then SDK readback, idempotent replay and close/reopen. `--case G01|G02|G03` runs the three generated
differential fixtures (G01 20000 prints, G02 seeded, G03 single pass and the index-mod-4 merge for K in 1, 7, 64) through the
same path. The fixture notation is expanded by the contract fixture harness vendored in core at
`tools/contract_fixtures/ef4g_fixture_expand.py` (core `b6c5063`, #711; sha256 `d28f551c2ea21ea4454e6c81b329a52d4edcf06e36ec3e097ee80033f940fd7f`). Give it with `--harness` or `EF4G_HARNESS` (the
results demo inherits the variable). `contract_document()` works only from a source checkout of core; otherwise pass
`--contract <TAPE_CONTRACT_PROPOSAL.json>`. Prefix pairs (`TK01`/`TK10c`, `PM01`/`PM03`, `W07b`/`W07d`) are separate fixtures
with identical results.

## Refusals and where each is refused

`--refusals` drives all 75 refusal fixtures. The adapter builds the batches and the configuration (the shared validators
refuse some there), the worker builds the work item (admission), then the calculation runs. Build time means the work item is
never created; compute time means the work item exists and the library refuses it. Nothing is published on any refusal.

| Stage | Count | Time | Worker code |
| --- | --- | --- | --- |
| Adapter build (shared batch validator, shared constructor or the adapter) | 16 | build | worker never reached |
| Worker admission (work item build) | 27 | build | `INVALID_COMMAND` 22 (configuration parameters, version, unknown feature); `RESULT_IDENTITY_MISMATCH` 5 (RF11, RF37, RF64: a row, seed or partition of another instrument; RF15: a coverage count that differs from the rows; RF68: zero partitions) |
| Library compute | 23 | compute | `INVALID_COMMAND` 15; `RESULT_IDENTITY_MISMATCH` 5 (RF12, RF17, RF38, RF63, RF72: every library `INCONSISTENT_IDENTITY`); `CALCULATION_FAILED` 3 (RF39, RF40, RF74: `OVERFLOW`) |
| No worker path | 9 | not a worker path | RF41, RF42, RF43, RF54, RF55 (update, restore, window merge: capability calls the worker does not expose); RF69 (a supplied partial: the worker computes its own partials); RF60, RF65, RF66 (partials built with another K, knowledge cutoff or policy version: one reviewed configuration per merge task) |

For 73 of the 75 fixtures the contract code equals the code of the layer that refused; the counts above are those of the run. Two refusals carry the adapter codes the I/O record already
states: RF18 (`batch_kind = quote`: `UNSUPPORTED_SAMPLING` from the shared batch validator, the contract expects
`INVALID_SCHEMA`) and RF35 (two identical seed facts: `DUPLICATE` from the shared REFERENCE validator, the contract expects
`INVALID_SCHEMA`); both inputs are still refused at build. RF14, RF62 and RF71 (a non-raw adjustment basis) are refused at
adapter build with the contract code `UNSUPPORTED_ADJUSTMENT`. Stage is not who raises the code: for example RF01 and RF58
(`BOUNDS`, shared validator per the contract) are refused at compute, because the batch is valid on its own and the cutoff
check runs in the library. Compute-time worker codes follow the translation of the shared worker layer. The library's own
code for the same inputs (capability call, or the supplied-partial order check of RF69 reproduced by reordering a computed
partial's cell rows) is recorded by the example for reference.

| Id | Contract code | Refused at | Worker code | Note |
| --- | --- | --- | --- | --- |
| RF01 | BOUNDS | compute | INVALID_COMMAND |  |
| RF02 | BOUNDS | compute | INVALID_COMMAND |  |
| RF03 | INVALID_ORDER | build (adapter / shared validator) | - |  |
| RF04 | DUPLICATE | build (adapter / shared validator) | - |  |
| RF05 | DUPLICATE | build (adapter / shared validator) | - |  |
| RF06 | INVALID_SCHEMA | build (adapter / shared validator) | - |  |
| RF07 | INVALID_SCHEMA | build (adapter / shared validator) | - |  |
| RF08 | INVALID_SCHEMA | build (adapter / shared validator) | - |  |
| RF09 | OVERFLOW | build (adapter / shared validator) | - |  |
| RF10 | INVALID_SCHEMA | build (adapter / shared validator) | - |  |
| RF11 | INCONSISTENT_IDENTITY | build (worker admission) | RESULT_IDENTITY_MISMATCH |  |
| RF12 | INCONSISTENT_IDENTITY | compute | RESULT_IDENTITY_MISMATCH |  |
| RF13 | INVALID_UNIT | compute | INVALID_COMMAND |  |
| RF14 | UNSUPPORTED_ADJUSTMENT | build (adapter / shared validator) | - |  |
| RF15 | INCONSISTENT_IDENTITY | build (worker admission) | RESULT_IDENTITY_MISMATCH |  |
| RF16 | INVALID_CONFIG | compute | INVALID_COMMAND |  |
| RF17 | INCONSISTENT_IDENTITY | compute | RESULT_IDENTITY_MISMATCH |  |
| RF18 | INVALID_SCHEMA | build (adapter / shared validator) | - | adapter code UNSUPPORTED_SAMPLING |
| RF19 | BOUNDS | compute | INVALID_COMMAND |  |
| RF20 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF21 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF22 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF23 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF24 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF25 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF26 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF27 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF28 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF29 | INCOMPATIBLE_VERSION | build (worker admission) | INVALID_COMMAND |  |
| RF30 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF31 | INVALID_CONFIG | compute | INVALID_COMMAND |  |
| RF32 | INVALID_SCHEMA | compute | INVALID_COMMAND |  |
| RF33 | INVALID_SCHEMA | compute | INVALID_COMMAND |  |
| RF34 | BOUNDS | build (adapter / shared validator) | - |  |
| RF35 | INVALID_SCHEMA | build (adapter / shared validator) | - | adapter code DUPLICATE |
| RF36 | INVALID_SCHEMA | compute | INVALID_COMMAND |  |
| RF37 | INCONSISTENT_IDENTITY | build (worker admission) | RESULT_IDENTITY_MISMATCH |  |
| RF38 | INCONSISTENT_IDENTITY | compute | RESULT_IDENTITY_MISMATCH |  |
| RF70 | INVALID_UNIT | compute | INVALID_COMMAND |  |
| RF71 | UNSUPPORTED_ADJUSTMENT | build (adapter / shared validator) | - |  |
| RF72 | INCONSISTENT_IDENTITY | compute | RESULT_IDENTITY_MISMATCH |  |
| RF39 | OVERFLOW | compute | CALCULATION_FAILED |  |
| RF40 | OVERFLOW | compute | CALCULATION_FAILED |  |
| RF74 | OVERFLOW | compute | CALCULATION_FAILED |  |
| RF75 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF41 | UNSUPPORTED_CAPABILITY | none (no worker path) | - |  |
| RF42 | UNSUPPORTED_CAPABILITY | none (no worker path) | - |  |
| RF43 | UNSUPPORTED_CAPABILITY | none (no worker path) | - |  |
| RF44 | UNKNOWN_FEATURE | build (worker admission) | INVALID_COMMAND |  |
| RF45 | UNKNOWN_FEATURE | build (worker admission) | INVALID_COMMAND |  |
| RF46 | UNKNOWN_FEATURE | build (worker admission) | INVALID_COMMAND |  |
| RF47 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF48 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF49 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF50 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF51 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF52 | INVALID_CONFIG | build (worker admission) | INVALID_COMMAND |  |
| RF53 | INCOMPATIBLE_VERSION | build (worker admission) | INVALID_COMMAND |  |
| RF54 | UNSUPPORTED_CAPABILITY | none (no worker path) | - |  |
| RF55 | UNSUPPORTED_CAPABILITY | none (no worker path) | - |  |
| RF56 | DUPLICATE | build (adapter / shared validator) | - |  |
| RF57 | OVERFLOW | build (adapter / shared validator) | - |  |
| RF58 | BOUNDS | compute | INVALID_COMMAND |  |
| RF73 | BOUNDS | compute | INVALID_COMMAND |  |
| RF59 | DUPLICATE | compute | INVALID_COMMAND |  |
| RF60 | INVALID_CONFIG | none (no worker path) | - |  |
| RF61 | INVALID_UNIT | compute | INVALID_COMMAND |  |
| RF62 | UNSUPPORTED_ADJUSTMENT | build (adapter / shared validator) | - |  |
| RF63 | INCONSISTENT_IDENTITY | compute | RESULT_IDENTITY_MISMATCH |  |
| RF64 | INCONSISTENT_IDENTITY | build (worker admission) | RESULT_IDENTITY_MISMATCH |  |
| RF65 | INCONSISTENT_IDENTITY | none (no worker path) | - |  |
| RF66 | INCOMPATIBLE_VERSION | none (no worker path) | - |  |
| RF67 | INVALID_CONFIG | compute | INVALID_COMMAND |  |
| RF68 | INVALID_CONFIG | build (worker admission) | RESULT_IDENTITY_MISMATCH |  |
| RF69 | INVALID_ORDER | none (no worker path) | - |  |

## Not executed

mypy, unit tests (policy: none), CI, an independent oracle beyond the contract literals, installed-wheel qualification, real
or observed data, source rights and release acceptance, performance claims, RF41-RF43, RF54, RF55, RF60, RF65, RF66 and RF69
through the worker (no such path), the `bundles` package decoder (it rebuilds `TopKTrades` through the constructor and refuses
notional-ordered cells, so a Parquet or bundles round trip is not covered), and the full results demo (left to qualification).
Library and adapter mathematics were not changed.
