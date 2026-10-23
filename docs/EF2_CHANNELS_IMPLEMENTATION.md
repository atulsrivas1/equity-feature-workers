# EF2 Ichimoku and price-channel worker

This additive source implements the bounded synthetic `channels` route for EF2 worker [#610](https://github.com/atulsrivas1/equity-features/issues/610), consuming the coordinator-frozen contract C (`ef2-v1`, algorithm `ef2-channels-v1`, config identity `ef2.<producer>`, `docs/ef2/ICHIMOKU_CHANNELS_CONTRACT_PROPOSAL.md` and `.json` in the core repo; fixtures: 74 result fixtures, 21 refusals, 7 invariants). Current GitHub Project status is authoritative. Source preparation does not establish story or release completion.

## Public API

- `equity_feature_workers.equity_channels`: `make_ef2_channels_work(config, *, features, ...)`, `compute_channels_inputs`, `channels_headers(config, features)`, `channels_history(config, features)`, `required_channels_inputs(config, features)`.
- `equity_feature_workers.ef1_runner`: `run_ef2_channels`, a thin wrapper over the shared sequential `_run_ef1` (unchanged); no other runner changes. Caller owns source/sink construction and lifetime.

The worker orchestrates only. It contains no extremum, midpoint, EMA, ATR, band, pivot, ranking, squeeze, rounding, quality or evidence logic and never infers, fills or defaults a price, period, multiplier or convention. The library owns admission, mathematics, quality, witnesses and every typed refusal; a refusal surfaces as a worker failure and publishes nothing.

## Route

Mirrors the accepted R17 supplied-intraday route and the momentum and money-flow workers. One config is one producer (`ichimoku`, `keltner`, `donchian`, `pivot_levels`, `band_width_percentile` or `bollinger_keltner_squeeze`; operation parameters have no defaults) and one WorkItem; the task family is the producer. The caller selects any non-empty subset of that producer's outputs (25 ids in all; header order lexicographic; units `currency/share`, `fraction`, `boolean`; all float64 except the squeeze flag, bool). The task decodes the chunked R17 context (reset epoch only) and the closed config through `decode_channels_config`, requires the library headers, the governed sessions of the context and chunked `bars:000...` roles, one per governed session, or a single `bars` role declared `missing`, of kind BAR with the 1 MiB bound. Readiness is per output inside the library. Warm-up sessions are those touched by the read range: the window reach for Donchian (`period - 1`) and Ichimoku (`displacement + max period - 1`, bounded by the anchor), anchor through target for Keltner, the percentile and the squeeze, and only the immediately preceding governed session for the pivots (none when the target is the first session; the library reports that as `missing_input`). Result identity (backend `intraday.python`), evidence limit, one value and one quality row per selected output, and the result-wide evidence bound are closed structurally.

## Synthetic example

`python examples/ef2_channels.py --case ICH-01 --output-db ABS_NEW_DB --html ABS_NEW_HTML --run-id demo-c` accepts the 74 contract result fixture ids read from the contract JSON (the refusals are not run). Fixtures, configs and selected outputs come from `equity_feature_duckdb.channels` (`synthetic_channels_fixture(case_id)`, `synthetic_channels_config(case_id)`, `synthetic_channels_features(case_id)`); the I/O config already carries the fixture's evidence limit. Expected values are read by id from the contract JSON (default: the core repository copy; `--contract` overrides). Acceptance per output follows the fixture: `R17 tolerance` (abs(y-r) <= max(8*ulp(r), 1e-12*abs(r)), r = the fixture float.hex), exactly zero reference (bit-exact +0.0), exact rank (bit-exact one conversion) and exact boolean; status, reasons, expected/observed counts and every evidence row (row id `slot:N`, use, knowledge reason, boundary, in order, after result-wide truncation) are compared exactly. Each case runs the worker, a NEW separate DuckDB sink, SDK readback, idempotent replay and close/reopen, then writes self-contained HTML.

The cross-fixture invariants and the 21 refusals belong to the library acceptance and are not run by the worker.

## Assumptions about sibling APIs

Core main `a53f440` (`build_channels_config`, `decode_channels_config` returning producer/values/evidence_limit with `param(name)`, `compute_channels(..., features=)`, `describe_channels`); I/O main `c15f540`.

## Status

Executed on Windows Python 3.12 with `python -I` and sys.path set in code to every `packages/*/src` of the three repositories, against core main `a53f440` and I/O main `c15f540`. `examples/ef2_channels.py` ran all 74 contract result fixtures into new scratch databases and HTML: 74 of 74 exited 0 with `verified_readback`, `verified_reopen` and `contract_literal_match` true, including idempotent replay, full reopen and the per-fixture acceptance rule, status, reasons, counts and evidence rows. A deliberately perturbed expected value (ICH-01 base line) was rejected by the comparison. `tools/ef1_results_demo.py` (run with `--core` and `--io`) ran all 304 cases (62 EF1, 68 EF2-B, 100 EF2-A, 74 new): 304 of 304 ran, 0 failed. The index shows a "readable view failed" note for the three pre-existing EF2-A prefix cases (P01 to P03, multi-result output); none for the channels cases.

Findings and fixes during the run: none needed; the lessons of the momentum slice (pair readbacks with items by task digest, per-chunk `bars:NNN` roles) were applied from the start. Witnesses are compared in full because every contract fixture states `evidence`. No contract defects found. The library's documented exact-zero limit (non-terminating EMA/ATR squeeze gaps) does not affect the 74 fixtures and remains a release-acceptance item.

No tests, CI, installed-package, platform or release claim. HTML generation is not browser inspection. Original-source parity and source rights remain unqualified.
