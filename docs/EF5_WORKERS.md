# EF5 price structures worker (candle, structure, technical)

Source for EQ-171 over the io adapter (`equity_feature_duckdb.price_structures`, io PR #58) and the pure library
(`equity_features_intraday.compute_ef5`). No mathematics here; the library owns admission, arithmetic, quality, witnesses and refusals.

## API

- `equity_feature_workers.equity_ef5`: `make_ef5_work(producer, config, ...)` (one work item; producer is `candle`, `structure` or `technical`),
  `compute_ef5_inputs` (calculation), families `ef5_candle`, `ef5_structure`, `ef5_technical`.
- `equity_feature_workers.ef1_runner.run_ef5`: thin wrapper over the shared sequential runner; the caller owns the sink.
- Header dtype follows the unit (`boolean` is bool, `windows` is int64, otherwise float64); the gate checks it against every library result.

## Gate

`python tools/verify_ef5_worker.py --contract <core docs/ef5 JSON>` (installed packages): 89 result fixtures (534 runs) = direct library call = stored
DuckDB read-back = contract literal, and 59 refusals: 31 are raised at the adapter with the recorded code, RF16 has no worker form, and of the 27 worker refusals 26 carry the recorded
`contract_code` (asserted) and RF46 is a documented known difference; the direct library call raises the recorded code for every recipe. RF16 (structural context not delivered) has no worker form because the worker takes its context from the
configuration; the direct call is checked. An unknown feature id surfaces the library's UNKNOWN_FEATURE from the worker. No new unit tests.

## Limits and follow-ups

- **`ef5.*` is not exposed through the service or MCP until the registration story lands.** Nothing in this change registers the producers there.
- **Recorded gap (RF16, PO-accepted)**: the worker has no context input (it reads the context from the configuration), so the recipe "structural context not
  delivered" cannot be run against the worker; the direct library call covers it. The gate fails if `make_ef5_work` or `compute_ef5_inputs` ever gains a
  parameter containing `context` without an RF16 worker test.
- No HTML example (the EF4 examples render one); the gate prints a summary only.
- A coordinated bump is owed before acceptance (core intraday, io duckdb, workers); see the plan document.

## Known difference (RF46)

A bar for another instrument is refused by the shared worker admission as worker code `RESULT` ("Session command failed: RESULT_IDENTITY_MISMATCH") with an empty `contract_code`, before the
library is reached; the recorded library code is INCONSISTENT_IDENTITY (the direct library call raises it). The gate pins this pair exactly, so any change
(a contract code appearing, or the worker code changing) fails it. The refusal is fail-closed either way; only the surfaced code differs from the recorded one.
