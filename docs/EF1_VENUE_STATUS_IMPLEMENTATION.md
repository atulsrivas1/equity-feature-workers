# Multi-venue status worker

This additive source implements the bounded synthetic `multivenue_status` route for canonical [EQ213 #571](https://github.com/atulsrivas1/equity-features/issues/571), consuming the coordinator-frozen multi-venue status contract (`ef1-multivenue-status1` v1, `docs/ef1/MULTI_VENUE_STATUS_CONTRACT_PROPOSAL.md` and `.json` in the core repo, decisions D01 to D16, R01). Current GitHub Project status is authoritative. Source preparation does not establish story or release completion.

## Public API

Import `make_ef1_venue_status_work`, `compute_venue_status_inputs`, `venue_status_headers` and `required_venue_status_inputs` from `equity_feature_workers.equity_venue_status`. Import `run_ef1_venue_status` from `equity_feature_workers.ef1_runner`. The runner delegates to the existing shared sequential supervisor, serialized publisher and full receipt readback; no other runner changes. Caller owns source/sink construction and lifetime.

The route calls the pure public `compute_ef1` API with exactly five nullable slots in role order: `venue_population`, `venue_seed`, `venue_states`, `venue_prefix`, `venue_validity`, all REFERENCE. Every role stays declared when absent; present-only batches are matched against the original role bindings and no role is inferred from DataKind or count. The library owns structural validation and typed refusals, the expected population, per-(publisher, dimension) facts, known/stale/unknown classification, the counts, the OR flags, the caller-supplied precedence, dominance, the latest event clock, quality, coverage and witnesses. The worker reproduces none of it: no counting, no precedence, no default order and no inference of a publisher, seed, certificate or state.

Config identity is `ef1.multivenue_status`, algorithm `v1`, schema `1`, one parameter `ef1_context_json` (calendar null). The seven headers, in lexicographic order, are `session.venue_status.aggregate_reason` (string, `reason`), `aggregate_state` (string, `state`), `known_nontrading_count` (int64, `publishers`), `known_trading_count` (int64, `publishers`), `latest_event_ns` (int64, `utc_ns`), `quoting_any` (bool, `boolean`) and `restriction_any` (bool, `boolean`). The single-publisher `status` producer is untouched.

The task has one instrument, one governed interval (the target session already on the config, because the context carries no calendar), no ordered history, warmup or initialization, merge policy none, serialized destination, exactly five maximum inputs and at most 1 MiB of present-input pickle.

Before any sink call the route checks family, config identity, roles, bindings, geometry, headers, backend, schema, math version, evidence limit 4, a single entity, seven values and seven quality rows, and that the reason cell is in the closed twelve-string vocabulary and the state cell is null or one of active, halted, closed. Numerical equality is not intercepted by the worker.

## Synthetic example

Run `python examples/ef1_venue_status.py --case MV05 --output-db ABSOLUTE_NEW_DB --html ABSOLUTE_NEW_HTML --run-id demo-venue`. `--case` accepts the 42 contract result fixtures MV01 to MV44 (there is no MV38 or MV39; RF01 to RF13 are refusals and are not run here). The I/O synthetic fixtures and config come from `equity_feature_duckdb.venue_status` (`synthetic_venue_status_fixture` and `synthetic_venue_status_config`, by `case_id`). Parents must exist and outputs must be new, distinct absolute paths. Expected values are not hard-coded: they are read by fixture id from the frozen contract JSON (default: the core repository copy next to the imported `equity_features`; `--contract` overrides) and compared in full: each of the seven values, status, reasons, expected/observed coverage, and the evidence order as (output, publisher, dimension), the publisher and dimension being decoded from the consumed evidence row identity. It runs worker, a NEW separate DuckDB sink, then SDK readback, idempotent replay and close/reopen, and writes self-contained HTML. `tools/ef1_results_demo.py` runs all 42 cases.

## Status

Authored, not executed: this source was written while the library and I/O layers were still being authored in parallel. Nothing in this document has been run; no import, example or demo result is claimed.

No tests, CI, installed-package, platform or release claim. Original-source parity and source rights remain unqualified.
