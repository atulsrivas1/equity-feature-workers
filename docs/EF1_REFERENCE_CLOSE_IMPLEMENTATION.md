# Reference close comparison worker

This additive source implements the bounded synthetic `reference_close` route for canonical [EQ213 #571](https://github.com/atulsrivas1/equity-features/issues/571), consuming the coordinator-frozen close-reconciliation contract (`ef1-close-compare1` v1, `docs/ef1/CLOSE_RECONCILIATION_CONTRACT_PROPOSAL.md` and `.json` in the core repo, decisions O1 to O11). Current GitHub Project status is authoritative. Source preparation does not establish story or release completion.

## Public API

Import `make_ef1_reference_close_work`, `compute_reference_close_inputs`, `reference_close_headers` and `required_reference_close_inputs` from `equity_feature_workers.equity_reference_close`. Import `run_ef1_reference_close` from `equity_feature_workers.ef1_runner`. The runner delegates to the existing shared sequential supervisor, serialized publisher and full receipt readback; no other runner changes. Caller owns source/sink construction and lifetime.

The route calls the pure public `compute_ef1` API with exactly four nullable slots in role order: `calendar_provenance` REFERENCE, `bars` BAR, `population_proof` REFERENCE, `authoritative_close` REFERENCE. Every role stays declared when absent; present-only batches are matched against the original role bindings and the three REFERENCE roles are never inferred from DataKind. The library owns the calculated-close selection, supplied-close admission, BPS, tolerance verdict, state, quality and witnesses. The worker reproduces none of it: no BPS derivation, no tolerance test, no timezone or default inference.

Config identity is `ef1.reference_close`, algorithm `v1`, schema `1`. The four headers, in lexicographic order, are `reference.close.absolute_difference_bps` (float64, `bps`), `reference.close.comparison_state` (string, `state`), `reference.close.supplied_price` (decimal128, `currency/share`) and `reference.close.within_tolerance` (bool, `boolean`). The calculated close is not emitted here; it stays in the separate `regular_bar_prices` result.

The task has one instrument, one governed target-session interval, no ordered history, warmup or initialization, merge policy none, serialized destination, exactly four maximum inputs and at most 1 MiB of present-input pickle.

Before any sink call the route checks family, config identity, roles, bindings, geometry, headers, backend, schema, math version, evidence limit 7, a single entity, four values and four quality rows, and that the state cell is one of the six frozen strings. Numerical equality is not intercepted by the worker.

## Synthetic example

Run `python examples/ef1_reference_close.py --case N02 --output-db ABSOLUTE_NEW_DB --html ABSOLUTE_NEW_HTML --run-id demo-close`. `--case` accepts any of the 47 contract result-fixture ids (N, Z, R, C, X, D groups); refusal fixtures E01 to E28 produce no result and are not cases. Parents must exist and outputs must be new, distinct absolute paths. `--contract` defaults to the core clone sibling of this repository. The example takes the input facts from the I/O repository's `equity_feature_duckdb.reference_close` synthetic fixture factory (assumed to be keyed by the contract fixture id), runs worker, a NEW separate DuckDB sink, compares value, status, reasons, counts and state to the contract's hand-derived literals (float64 by `float.hex`), then SDK readback, idempotent replay and close/reopen, and writes self-contained HTML. `tools/ef1_results_demo.py` includes a representative subset of cases.

## Status

Authored against the frozen contract and the first-trade template. Not executed: the library and I/O sibling layers were not available when written. No tests, CI, installed-package, platform or release claim. Original-source parity, source rights and official-price authenticity remain unqualified.
