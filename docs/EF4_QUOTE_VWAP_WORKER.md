# EF4 quote dynamics and VWAP path workers

This source change implements the workers for [EQ-159 #493](https://github.com/atulsrivas1/equity-features/issues/493)
and [EQ-160 #494](https://github.com/atulsrivas1/equity-features/issues/494) (frozen contract H) over the supplied-input
adapter (io main `2f9c14c`, `equity_feature_duckdb.quote_vwap`) and the pure library (core main `2517ce5`,
`equity_features.ef4.compute_quote_vwap`). It covers `ef4-quote1` (`ef4.quote_dynamics`, 9 `quote.*` outputs) and
`ef4-vwap1` (`ef4.vwap_path`, 7 `vwap.*` outputs). It contains no spread, midpoint, microprice, VWAP, slope, extremum or
comparison mathematics. Source preparation does not establish story, release or acceptance completion.

## Public API

- `equity_feature_workers.equity_quote_vwap`: `make_quote_dynamics_work`, `make_vwap_path_work` (build one work item) and
  `compute_quote_dynamics_inputs`, `compute_vwap_path_inputs` (calculation).
- `equity_feature_workers.ef1_runner`: `run_ef4_quote_dynamics`, `run_ef4_vwap_path`, thin wrappers over the shared
  sequential `_run_ef1` (unchanged), additive to the existing runners. The caller owns the sink.
- `examples/ef4_quote_vwap.py --case <id> --output-db <new .duckdb> --html <new .html> --run-id <id> --harness <file>` and
  `examples/ef4_quote_vwap.py --refusals [--only RQ01,RV01] --harness <file>`.
- Tools: 69 cases registered in `tools/ef1_results_demo.py`; `tools/ef1_readable_view.py` labels the quote and VWAP runs by
  selected output or `joint` (otherwise they would read as `prefix length N`).

## Design

1. **Fixed nullable role slots.** The library takes fixed slots: quote `(bounds, quotes)` and VWAP
   `(bounds, grid, trades, bars, proof)`. The worker binds each supplied batch to its slot by the manifest role and leaves
   a role that was not delivered `None` (a prepared source has no trades, a bar source has no proof, an absent quote role
   is `None`). The worker never decides which roles a series source needs; the library refuses a wrong combination.
2. **Nothing is hardcoded.** Entity, instrument, session, role keys, snapshot, grid version and series policy come from the
   task manifest or the configuration. The role names are the library's own slot names.
3. **Headers from the library.** The feature headers are built from `quote_vwap_definitions(config)`, which describes only
   the outputs the configuration can execute (snapshot sampling has no `quote.change.*`). Selecting an output outside that
   set is refused at work-item build.
4. **Governed interval.** The one governed interval is the configured target session unless the caller declares another.
5. **Pinned adapter rulings** are the adapter's: coverage is always declared by the mapper (`complete`, `incomplete`,
   `absent`), bounds text is `snap`, and a bar is complete unless the caller sets `incomplete=True`. The worker reads the
   declaration from the batch metadata and records it in the input manifest state (`complete`, `partial`, `empty`).
6. **No ordered history, no initialization digest, no process-level registry.** The library carries the whole input through
   the batches and the configuration.

## Example and fixtures

Each of the 69 contract result fixtures (30 quote `Q01-Q30`, 39 VWAP `V01-V40` without `V27`) is built through the I/O
adapter and run once per selected output plus one joint run when several outputs are selected. Every run is compared with
the frozen contract literal (value by the contract's acceptance rule, status, reasons, expected and observed counts,
per-output witnesses for single-output runs), then SDK readback, idempotent replay and close/reopen. The fixture notation
is expanded by the contract fixture harness (`build_model`, `apply_ops`). The example needs `--harness <core>/tools/contract_fixtures/ef4h_fixture_expand.py`
(or `EF4H_HARNESS`; the results demo inherits the environment variable). The expander used for these runs has sha256
`0a91ccd55bccc170cdc778a28b1d4d8d8a9f1b0af838c8e32a3c895a0c45ebfd`. `contract_document()` works only from a source checkout of core (it locates `docs/ef4/` relative to the
installed `equity_features` package); otherwise pass `--contract <QUOTE_VWAP_CONTRACT_PROPOSAL.json>`. Prefix pairs (`Q01`/`Q20`,
`V02`/`V03`, `V02`/`V12`, `V02`/`V13`, `V18`/`V19`) are separate fixtures with identical results.

## Refusals and where each is refused

`--refusals` drives all 80 refusal fixtures (36 quote `RQ`, 44 VWAP `RV`). The adapter builds the batches and the
configuration (the shared validators refuse some there), the worker builds the work item (admission), then the calculation
runs. The record carries the stage, the contract code, the library's own code for the same inputs and the worker code.
Build time means the work item is never created; compute time means the work item exists and the library refuses it.

| Stage | Count | Time | Worker code |
| --- | --- | --- | --- |
| Adapter build (shared batch validator, shared constructor, or the adapter) | 29 | build | worker never reached |
| Worker admission (work item build) | 20 | build | `INVALID_COMMAND` 18 (configuration parameters, version, unknown feature, continuous output on snapshot sampling); `RESULT_IDENTITY_MISMATCH` 2 (RQ22, RV29: a row of another instrument) |
| Library compute | 29 | compute | `INVALID_COMMAND` 18; `RESULT_IDENTITY_MISMATCH` 11 (every library `INCONSISTENT_IDENTITY`) |
| Library capability call only (RQ36, RV41: update, restore or merge) | 2 | not a worker path | the worker exposes no such path |

For every fixture the contract code equals the code of the layer that refused (80 of 80). Compute-time worker codes follow
the translation of the shared worker layer. Stage is not who raises the code: RQ12 (`L.batch_kind`, library per the
contract) is refused while the adapter builds the batch, RQ09 (`S.cutoff`, shared validator per the contract) is refused at
compute because the batch is valid on its own and the cutoff check runs in the library, and RV43 (`S.bar_notional_bounds`)
is refused at adapter build. These are the layer differences the adapter record already states. Nothing is published on any
refusal.

## Not executed

mypy, unit tests (policy: none), CI, an independent oracle beyond the contract literals, installed-wheel qualification, real
or observed data, source rights and release acceptance, performance claims, and RQ36 and RV41 through the worker (no such
path). Library and adapter mathematics were not changed. The full 69-fixture results demo is left to qualification.
