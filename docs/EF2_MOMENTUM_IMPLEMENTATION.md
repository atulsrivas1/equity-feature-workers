# EF2 momentum and trend worker

This additive source implements the bounded synthetic `momentum` route for EF2 worker [#610](https://github.com/atulsrivas1/equity-features/issues/610), consuming the coordinator-frozen contract A (`ef2-conv1`, config `intraday.momentum.v1`, `docs/ef2/MOMENTUM_TREND_CONTRACT_PROPOSAL.md` and `.json` in the core repo, with Amendment 1; fixtures: 97 single-target plus the prefix fixtures P01 to P03). Current GitHub Project status is authoritative. Source preparation does not establish story or release completion.

## Public API

- `equity_feature_workers.equity_momentum`: `make_ef2_momentum_work(config, *, features, ...)`, `compute_momentum_inputs`, `momentum_headers(config, features)`, `momentum_history(config, features)`, `required_momentum_inputs(config, features)`.
- `equity_feature_workers.ef1_runner`: `run_ef2_momentum`, a thin wrapper over the shared sequential `_run_ef1` (unchanged); no other runner changes. Caller owns source/sink construction and lifetime.

The worker orchestrates only. It contains no EMA, Wilder smoothing, directional movement, window, ratio, rounding, quality or evidence logic and never infers, fills or defaults a price, period or convention. The library owns admission, mathematics, quality, witnesses and every typed refusal; a refusal surfaces as a worker failure and publishes nothing.

## Route

Mirrors the accepted R17 supplied-intraday route and the money-flow worker. One config is one homogeneous family (`macd`, `dmi` or `stochastic`; periods have no defaults) and the caller selects any non-empty subset of that family's outputs (ten float64 outputs, header order lexicographic; units `currency/share`, `DMI points`, `stochastic points`, `Williams %R points`). The task decodes the chunked R17 context and the closed config through `decode_momentum_config` (identity `intraday.momentum.v1`, algorithm `intraday-momentum-v1`, schema `1`), requires the library headers, the governed sessions of the context and chunked `bars:000...` roles (or a single `bars` role declared `missing`) of kind BAR with the 1 MiB bound. Readiness is per output inside the library, so an unrelated gap, null or absent column never blocks another output. Warm-up sessions are those touched by the slot range: anchor through target for MACD and DMI, the widest selected rolling window for stochastic outputs (Williams k, `stoch_k` k + slowing - 1, `stoch_d` k + slowing + d - 2, as documented in the library API). Result identity (backend `intraday.python`), evidence limit, one value and one quality row per selected output, and the result-wide evidence bound are closed structurally.

## Synthetic example

`python examples/ef2_momentum.py --case M05 --output-db ABS_NEW_DB --html ABS_NEW_HTML --run-id demo-m` accepts the contract fixture ids read from the contract JSON (M, D, T and P series; E01 to E23 are refusals and are not run). Fixtures, configs and selected outputs come from `equity_feature_duckdb.momentum` (`synthetic_momentum_fixture(case_id, series=, length=)`, `synthetic_momentum_config(case_id, length=)`, `synthetic_momentum_features(case_id)`). Expected values are read by id from the contract JSON (default: the core repository copy; `--contract` overrides). Acceptance per output follows the fixture rule: `tolerance` (abs(y-r) <= max(8*ulp(r), 1e-12*abs(r)), r = the fixture float.hex) or `exact_plus_zero` (bit-exact +0.0); status, reasons, expected/observed counts and witnesses (slot and use, decoded through the packet's `slot:N` coverages) are compared exactly. Prefix fixtures run series A and B at each prefix length as separate items in one sink, are compared with the stated prefix outputs (which carry no witnesses) and A must equal B. Each case runs the worker, a NEW separate DuckDB sink, SDK readback, idempotent replay and close/reopen, then writes self-contained HTML.

Lessons carried from the money-flow slice: per-chunk `bars:NNN` roles come from the I/O `bars` tuple; the I/O synthetic config's evidence limit is replaced by the fixture's (default 10000, result-wide) through the library builder; the runner returns readbacks in its own order, so the example pairs them with items by task digest.

## Assumptions about sibling APIs

Core main `70e8bb2` (`build_momentum_config`, `decode_momentum_config` returning family/periods/evidence_limit, `compute_momentum`, `describe_momentum`); I/O main `5d075fb` (PR 34).

## Status

Executed on Windows Python 3.12 with `python -I` and sys.path set in code to every `packages/*/src` of the three repositories, against core main `70e8bb2` and I/O main `5d075fb`. `examples/ef2_momentum.py` ran all 100 contract result fixtures (97 single-target plus P01 to P03, 117 runs) into new scratch databases and HTML: 100 of 100 exited 0 with `verified_readback`, `verified_reopen` and `contract_literal_match` true, including idempotent replay, full reopen and the per-fixture acceptance rule, status, reasons, counts and slot/use witnesses. `tools/ef1_results_demo.py` ran all 230 cases (62 EF1, 68 EF2-B, 100 new): 230 of 230 ran, 0 failed.

Findings and fixes during the run: the runner returns readbacks in a different order from the items, so multi-run (prefix) cases paired results with the wrong packets until readbacks were matched by task digest; prefix cases state no witnesses, so witnesses are compared only where the contract gives `evidence_rows`. No contract defects found.

No tests, CI, installed-package, platform or release claim. HTML generation is not browser inspection. Original-source parity and source rights remain unqualified.
