# EF2 money-flow and retracement-level workers

This additive source implements the bounded synthetic `money_flow` and `retracement_level` routes for EF2 worker [#610](https://github.com/atulsrivas1/equity-features/issues/610), consuming the coordinator-frozen contract B (`ef2-mf1` and `ef2-retrace1`, `docs/ef2/MONEYFLOW_RETRACEMENT_CONTRACT_PROPOSAL.md` and `.json` in the core repo; fixtures M01 to M36 and T01 to T32). Current GitHub Project status is authoritative. Source preparation does not establish story or release completion.

## Public API

- `equity_feature_workers.equity_money_flow`: `make_ef2_money_flow_work(config, *, features, ...)`, `compute_money_flow_inputs`, `money_flow_headers(config, features)`, `money_flow_history(config, features)`, `required_money_flow_inputs(config, features)`.
- `equity_feature_workers.equity_retracement`: `make_ef2_retracement_work(config, ...)`, `compute_retracement_inputs`, `retracement_headers(config)`, `required_retracement_inputs(config)`.
- `equity_feature_workers.ef1_runner`: `run_ef2_money_flow` and `run_ef2_retracement`, thin wrappers over the shared sequential `_run_ef1` (unchanged); no other runner changes. Caller owns source/sink construction and lifetime.

The workers orchestrate only. They contain no multiplier, running total, window, ratio, level, tie, rounding, quality or evidence logic and never infer, fill or default a volume, extremum, ratio, period or direction. The library owns admission, mathematics, quality, witnesses and every typed refusal (including the `OVERFLOW` refusal of a published OBV beyond int64); a refusal surfaces as a worker failure and publishes nothing.

## Money flow

Mirrors the accepted R17 supplied-intraday route. Config identity `ef2.money_flow`, algorithm `ef2-money-flow-v1`, schema `1`, parameters: the R17 chunked context, `policy_version = ef2-mf1`, `evidence_limit`, and `cmf_period` only when `intraday.cmf` is selected. The caller selects any non-empty subset of `intraday.ad_line`, `intraday.cmf`, `intraday.obv`; headers are lexicographic (`shares`, `fraction`, `shares`; OBV int64, the others float64). Readiness stays per output inside the library, so an unready slot outside an output's own dependency never blocks it (M24, M31, M34). The task rebuilds the config from the decoded context and compares it to the supplied one, requires the library headers, the governed sessions of the context, the warm-up sessions touched by the anchored (OBV, A/D) or window (CMF only) range, and chunked `bars:000...` roles (or a single `bars` role declared `missing`), all of kind BAR, with the pickle bound of 1 MiB. Result identity, evidence limit, one value and one quality row per selected output and at most two witnesses per output are closed structurally.

## Retracement level

Config identity `ef2.retracement_level`, algorithm `ef2-retracement-level-v1`, schema `1`, parameters `policy_version = ef2-retrace1`, `direction`, `ratio_num`, `ratio_den`, `evidence_limit`; ratio validity is the library's. Exactly two nullable REFERENCE roles in order `prior_high`, `prior_low`, one extremum row each (present-only batches are matched against the original role bindings, never inferred), one governed interval (the target session), one header `retracement.level` (float64, `currency/share`), no ordered history. A different ratio or direction is a different config and result.

## Synthetic examples

`python examples/ef2_money_flow.py --case M05 --output-db ABS_NEW_DB --html ABS_NEW_HTML --run-id demo-mf` accepts the 36 result fixtures M01 to M36; `examples/ef2_retracement.py` accepts T01 to T32 (MR01 to MR20 and TR01 to TR18 are refusals and are not run). Fixtures and configs come from `equity_feature_duckdb.money_flow` / `equity_feature_duckdb.retracement` (`synthetic_*_fixture(case_id=...)`, `synthetic_*_config(case_id=...)`). Expected values are read by id from the contract JSON (default: the core repository copy next to `equity_features`; `--contract` overrides). The contract states no numerical tolerance, so acceptance is bit-exact: OBV as an exact int, float64 values by `float.hex`, plus status, reasons, expected/observed counts and witnesses. Each case runs the worker, a NEW separate DuckDB sink, SDK readback, idempotent replay and close/reopen, then writes self-contained HTML. A fixture with a `selected` list (M34) runs only those outputs. `tools/ef1_results_demo.py` runs all cases, including the 68 new ones.

## Assumptions about sibling APIs (to confirm when they land)

- Core (confirmed against main 364fe61): `equity_features_intraday.build_money_flow_config`, `decode_money_flow_config`, `compute_money_flow` (one FeatureResult, lexical order, backend `intraday.python`) and `equity_features.ef2` (`build_retracement_config`, `decode_retracement_config`, `ef2_definitions`, `compute_retracement_level`, backend `python-exact`). Coverage-proven empty slots without a row are omitted from money-flow witnesses.
- I/O: `equity_feature_duckdb.money_flow` and `.retracement` as above; packets expose `.roles`, `.slots`, `.batches`.
- Witness identity: retracement witnesses are decoded to role through each batch's `source.input_id`. Money-flow witnesses are decoded to a slot index through declared `slot:N` interval coverages when present; otherwise the example checks count only and records that in the HTML and in the printed `witness_identity_checked` flag.

## Status

Authored, not executed: the library and I/O siblings were not available when this was written, so no import, example or demo has run. No tests, CI, installed-package, platform or release claim. Original-source parity and source rights remain unqualified.
