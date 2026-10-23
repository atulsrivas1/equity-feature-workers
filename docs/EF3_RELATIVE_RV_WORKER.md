# EF3 relative statistics, log volatility and realized variance worker

This additive source implements the EF3 worker for [#612](https://github.com/atulsrivas1/equity-features/issues/612) over
the supplied-input adapter ([EF3_RELATIVE_RV_IO.md](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/EF3_RELATIVE_RV_IO.md),
io main `b8e09f4`) and the pure library (core `docs/ef3/RELATIVE_RV_CONTRACT_PROPOSAL.md` with Amendment 1, core main `d8ed913`).
It consumes contract D: `ef3-rel1` (`ef3.relative_stats`), `ef3-dlv1` (`ef3.daily_log_volatility`) and `ef3-rv1`
(`ef3.intraday_realized_variance`). Source preparation does not establish story, release or acceptance completion.

## Public API

- `equity_feature_workers.equity_relative_rv`:
  - `make_ef3_relative_work(config, *, grid, features, inputs, batches, instrument_id, job_id, generation_id, partition_id, destination_scope)`
    and `compute_relative_inputs`;
  - `make_ef3_log_volatility_work(...)` and `compute_log_volatility_inputs`;
  - `make_ef3_realized_variance_work(config, *, features, inputs, batches, ...)` and `compute_realized_variance_inputs`.
- `equity_feature_workers.ef1_runner`: `run_ef3_relative`, `run_ef3_log_volatility`, `run_ef3_realized_variance`, thin wrappers
  over the shared sequential `_run_ef1` (unchanged). The caller owns source and sink construction and lifetime.
- `examples/ef3_relative_rv.py --case <id> --output-db <new .duckdb> --html <new .html> --run-id <id> [--contract <json>]`.

The worker orchestrates only. It contains no return, beta, correlation, residual, logarithm, variance, volatility or
annualization logic, and it never infers, fills, sorts, rescales or defaults a role, slot, window, period, factor, grid
or benchmark. Admission is role-keyed: every bound batch must match its manifest binding, the configured namespace,
its declared coverage and the role's instrument (`instrument` for the instrument, the configured `benchmark_id` for the
benchmark, the grid instrument for the daily role, the context instrument for realized variance). The library owns
admission, mathematics, quality, witnesses and every typed refusal; a refusal surfaces as a worker failure and
publishes nothing.

## Routes

- Relative statistics: two fixed roles (`instrument`, `benchmark`), each nullable in the library; the worker passes the
  pair as `(instrument, benchmark)` and checks the task headers, governed sessions, warm-up sessions and grid digest against
  the library's own discovery (`ef3_definitions`).
- Daily log volatility: one fixed role (`daily`).
- Realized variance: the R17 context travels in the configuration as chunks (as in the EF2 channels route); the worker
  decodes it with the library codec and passes the BAR chunks to `compute_realized_variance`. Feature headers come from
  the library catalog.

The two daily producers bind a local `DailyGrid` by SHA-256 (the library carries no grid in the configuration). The worker
registers each grid under its own digest when the work item is built (`_register`, content-addressed; the configuration must
bind exactly that digest), and calculation resolves it by the digest in the configuration. The library verifies the digest
again. Because the work item callable must be a closure-free top-level function, this registry is the only process-level
state of the worker. It is therefore valid only for the sequential in-process runner used here.

## Example and fixtures

`examples/ef3_relative_rv.py` builds each contract fixture from the frozen contract JSON (never from expected values), with
the I/O adapter (`close_batch`, `intraday_batch`, the three config builders), then runs it through the worker. A case runs one
work item per selected output (one-output selection, Amendment 1 item 2) and one joint item when it selects several outputs.
For each run the example compares, against the contract literal:

- value (`bits` fixtures bit-exact including the sign of zero; `r17_tol` fixtures by `max(8 ulp, 1e-12 |r|)`), status,
  reasons, expected and observed counts;
- per-output witnesses for one-output runs (`instrument:slot:i`, `benchmark:slot:i`, `slot:i`, and the contract's
  `excluded:...:<reason>` form);
- the flat joint evidence list: `joint_selection_evidence` when the contract gives one, otherwise the per-output witness
  lists in lexical output order truncated to the result-wide `evidence_limit`.

Each run then goes through SDK readback, idempotent replay and a full close/reopen read of the stored result. The page holds
one canonical line per run, so the readable view reads it as a multi-run case.

Case set: 24 relative-statistics fixtures (RS01-RS24, 96 runs), 17 daily log-volatility fixtures (DV01-DV17, 17 runs),
21 realized-variance fixtures (RV01-RV21, 61 runs, with the annualization factor bound only when volatility is selected,
because the library refuses a factor without volatility and a volatility without a factor), and the prefix-property
fixtures PX01 (suffix slots after the target on RS02, 3 variants) and PX02 (on DV02, 3 variants), 6 runs. Total: 62 result
fixtures and 2 prefix fixtures, 180 worker runs.

## Executed

Executed on Windows, Python 3.12, `python -I -B` with every `packages/*/src` directory of the three repositories placed on
`sys.path` by a launcher (the same way the results demo starts its examples), against core `cf82122` (`origin/main`; the
library producers were added at `d8ed913`, and `6adf840` only adds the robust, pace and mean-ratio modules and exports, so
`_relative.py`, `_logvol.py`, `_common.py`, `_dispatch.py`, `_definitions.py` and `packages/intraday` are byte-identical across
both), I/O `b8e09f4`, and this branch:

- All 64 cases exited 0: 62 of 62 result fixtures and 2 of 2 prefix fixtures. Each run matched the contract literal in value,
  status, reasons, counts, per-output witnesses (one-output runs) and joint evidence (joint runs). 180 of 180 runs were
  read back through the SDK, replayed and reopened from a new separate DuckDB file.
- Negative control: the RS02 beta literal in a scratch copy of the contract was perturbed by 0.1 percent; the example
  rejected it with a literal mismatch. The comparison is live.
- HTML pages and DuckDB files were produced for every case.
- Results demo, filtered to the new cases (`tools/ef1_results_demo.py <new dir> --only ef3_relative_rv`): 64 runs, 0 failed, 64 readable views written (`view/`); the case-specific pages and DuckDB files are in the demo output directory, and DuckDB `.lock` sidecar files remain next to them after the run.
  The readable view is produced from the new pages through the unchanged `tools/ef1_readable_view.py`.
- Extraction check: `python -I tools/check_extraction.py --core-root C:\Atul\Claude\equity-features` run in the I/O repository
  at `b8e09f4` FAILS: `docs/EF1_SOURCE_EVOLUTION.json` declares `relative_rv.py` with SHA-256
  `b119cfecb3...`, while the file at `b8e09f4` hashes to `07e26cc2975d...`. In a scratch copy only, with the declaration
  corrected to `07e26cc2...`, the same check exits 0. The I/O repository was not changed; the declaration correction is
  a separate I/O change. This worker repository has no extraction check of its own, so its `EF1_SOURCE_EVOLUTION` entries are
  unchanged.

## Not executed

- No unit tests, no CI, no installed-wheel or package-manager qualification (policy: manual runs only).
- The 48 refusal fixtures and the full set of invalid-admission controls (role absence, instrument mismatch, benchmark
  mismatch, mismatched coverage, wrong governed sessions, wrong headers, forged context) are not run by the example. The
  admission code for them is present and reviewed by reading, not exercised.
- PX03 is not run as a separate case: the contract states it is not constructible with later bars under the R17 context and
  verifies it through RV21, which is run.
- The full results demo over all previously existing cases was not run in this slice; only the 64 new `ef3_relative_rv` cases
  were run through it. The `ef1_readable_view` module is unchanged.
- mypy and style checks were not run.
- Browser inspection of the HTML pages, original-source parity, source rights, performance, and EQ-157 release acceptance are
  not established.
- The I/O extraction check at `b8e09f4` is failing for the declaration reason above and has not been passed on the real
  I/O checkout.

## Deviations and choices

- The `b8e09f4` I/O commit is the merged adapter. Its earlier commits `97f1533` and `536e1a0` are on `origin/ef3d-io` and
  are now in `origin/main` history.
- A relative-statistics or daily log-volatility work item needs its `DailyGrid` registered in the process first (see above).
- Variance-only realized-variance runs build the configuration without an annualization factor; the library rule requires that.
  This is a caller configuration choice, not a change to any output.
- The example runs one item per output and one joint item for each multi-output fixture instead of one item per fixture, because
  the contract defines per-output witnesses by one-output selection and the joint list truncates in lexical order.
- No library, adapter or contract change was made. No version was bumped. No sink or SDK change was made.
