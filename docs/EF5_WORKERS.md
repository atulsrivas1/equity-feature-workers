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
DuckDB read-back = contract literal, and 59 refusals refused with the recorded code (31 at the adapter; for the rest the worker refuses and the direct
library call raises the recorded code). RF16 (structural context not delivered) has no worker form because the worker takes its context from the
configuration; the direct call is checked. An unknown feature id surfaces the library's UNKNOWN_FEATURE from the worker. No new unit tests.

## Limits and follow-ups

- Service and MCP catalogue registration of `ef5.*` is a separate story (not included).
- No HTML example (the EF4 examples render one); the gate prints a summary only.
- A coordinated bump is owed before acceptance (core intraday, io duckdb, workers); see the plan document.
