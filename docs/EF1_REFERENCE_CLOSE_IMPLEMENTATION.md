# Reference close comparison worker

This additive source implements the bounded synthetic `reference_close` route for canonical [EQ213 #571](https://github.com/atulsrivas1/equity-features/issues/571), consuming the coordinator-frozen close-reconciliation contract (`ef1-close-compare1` v1, `docs/ef1/CLOSE_RECONCILIATION_CONTRACT_PROPOSAL.md` and `.json` in the core repo, decisions O1 to O11). Current GitHub Project status is authoritative. Source preparation does not establish story or release completion.

## Public API

Import `make_ef1_reference_close_work`, `compute_reference_close_inputs`, `reference_close_headers` and `required_reference_close_inputs` from `equity_feature_workers.equity_reference_close`. Import `run_ef1_reference_close` from `equity_feature_workers.ef1_runner`. The runner delegates to the existing shared sequential supervisor, serialized publisher and full receipt readback; no other runner changes. Caller owns source/sink construction and lifetime.

The route calls the pure public `compute_ef1` API with exactly four nullable slots in role order: `calendar_provenance` REFERENCE, `bars` BAR, `population_proof` REFERENCE, `authoritative_close` REFERENCE. Every role stays declared when absent; present-only batches are matched against the original role bindings and the three REFERENCE roles are never inferred from DataKind. The library owns the calculated-close selection, supplied-close admission, BPS, tolerance verdict, state, quality and witnesses. The worker reproduces none of it: no BPS derivation, no tolerance test, no timezone or default inference.

Config identity is `ef1.reference_close`, algorithm `v1`, schema `1`. The four headers, in lexicographic order, are `reference.close.absolute_difference_bps` (float64, `bps`), `reference.close.comparison_state` (string, `state`), `reference.close.supplied_price` (decimal128, `currency/share`) and `reference.close.within_tolerance` (bool, `boolean`). The calculated close is not emitted here; it stays in the separate `regular_bar_prices` result.

The task has one instrument, one governed target-session interval, no ordered history, warmup or initialization, merge policy none, serialized destination, exactly four maximum inputs and at most 1 MiB of present-input pickle.

Before any sink call the route checks family, config identity, roles, bindings, geometry, headers, backend, schema, math version, evidence limit 7, a single entity, four values and four quality rows, and that the state cell is one of the six frozen strings. Numerical equality is not intercepted by the worker.

## Synthetic example

Run `python examples/ef1_reference_close.py --case C01_within_tolerance --output-db ABSOLUTE_NEW_DB --html ABSOLUTE_NEW_HTML --run-id demo-close`. `--case` accepts the three I/O synthetic cases `C01_within_tolerance`, `C02_outside_tolerance` and `C03_absent_authoritative_close` from `equity_feature_duckdb.reference_close` (bars close 12.50, tolerance 10/1 BPS, scale 2). Parents must exist and outputs must be new, distinct absolute paths. The I/O layer carries no expected values, so the example hard-codes literals asserted from the contract rules: C01 mirrors contract fixture N01 (equal operands, within), C03 mirrors R01 (authoritative slot missing, `reference_unavailable`), and C02 (supplied 12.60, BPS 79.36507936507937, outside) has no contract twin and is derived by hand from the contract formula. It runs worker, a NEW separate DuckDB sink, compares value (float64 by `float.hex`), status, reasons, counts and state, then SDK readback, idempotent replay and close/reopen, and writes self-contained HTML. The 47 contract fixtures remain the library layer's check. `tools/ef1_results_demo.py` runs all three cases.

## Status

Executed on Windows Python 3.12.10 with PYTHONPATH of every `packages/*/src` of the three repositories, against core main `c754e5d` (library PR 639) and I/O main `1b46c1c` (PR 30). Imports of `equity_reference_close` and `run_ef1_reference_close` succeed.

One defect was found and fixed: the first run failed worker admission (`INVALID_COMMAND`) because the selection check required `dict`, while the decoded selection is another `Mapping`; it now accepts `Mapping`.

The example then ran for `C01_within_tolerance`, `C02_outside_tolerance` and `C03_absent_authoritative_close` into new scratch databases and HTML: each exited 0 with `verified_readback`, `verified_reopen` and `contract_literal_match` true (C01 within, BPS 0.0, supplied 1250; C02 outside, BPS 79.36507936507937, supplied 1260; C03 `reference_unavailable`, null/null), including idempotent replay. `tools/ef1_results_demo.py` ran all 20 cases (the 17 existing plus the 3 new) to completion. HTML generation is not browser inspection, and the demo records that each example ran, not independent review.

No tests, CI, installed-package, platform or release claim. Original-source parity, source rights and official-price authenticity remain unqualified.

## Contract catalogue

Internal, synthetic, pre-release. Generated input and output contract pages (in the io repository) for the producers this page covers; core wins on conflict, and the catalogue computes nothing and names no data supplier. Overview: [catalogue INDEX](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/INDEX.md); use: [CATALOGUE.md](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/CATALOGUE.md).

- `ef1.reference_close`: [input](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef1.reference_close/input.md), [output](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef1.reference_close/output.md)
