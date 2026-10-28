# EF5 workers layer: plan and blockers (EQ-171)

Status: plan only. No worker code in this change. Inputs: core EF5 library (merged, `equity_features_intraday.compute_ef5`), io adapter
(`equity_feature_duckdb.price_structures`, io PR #58, not yet merged), frozen contract and library errata in core `docs/ef5/`.

## Scope

Follow the EF4 path-efficiency worker (`_path_routes.py`, `equity_path_efficiency.py`, `docs/EF4_PATH_EFFICIENCY_WORKER.md`) for three producers.

1. `equity_feature_workers.equity_ef5`: `make_ef5_work(producer, ...)` and `compute_ef5_inputs`; headers for the 15 outputs with dtype and unit
   (float64, bool, int64; units `fraction`, `boolean`, `currency/share`, `fraction/s`, `fraction/s^2`, `windows`, slash spelling only).
2. Routes: one nullable `bars` role, one manifest per session chunk; exact window closure per selected output (span: candle 0 or 1, swing 2k,
   last swing L, velocity 1, acceleration 2, trend M*N; `session_return_fraction` needs slot 0); a bounded window never reads data after the
   target; correction/replay scope as in EF4. Result identity: `intraday.python` with the intraday version (the `INTRADAY_VERSION` constant).
3. Runner wrapper `run_ef5` over the shared sequential runner; caller owns the sink.
4. Examples `ef5_candle.py`, `ef5_structure.py`, `ef5_technical.py` with `--case <id> --output-db --html` and `--refusals`, driven by the core contract.
5. Docs `EF5_*_WORKER.md`; `tools/_versions.py` and `verify_versions.py` extended (workers 0.1.0a17 when merged).

## Gates (acceptance of the layer)

- Direct library result = adapter result = worker result = stored (DuckDB sink) result, canonical-encoding equal, for all 89 result fixtures
  (534 runs, outputs alone and jointly) including bool and int64 columns.
- All 59 refusal recipes through the worker path with the recorded stage and code; refusal class identical to the direct call.
- No default introduced by the worker (the brief lists none); known_at and coverage declarations pass through unchanged.

## Blockers and preconditions

1. **Remote client decode (PO ruling, E1/E2 follow-up item 5): the workers layer must not merge until the remote client decode path carries
   feature context**, or EF5 documents the same limit with an owner-visible note. The client conversion of remote results does not today
   receive the feature's contract context for cells whose decoding depends on it (the E1 notional top-K case is the known instance); EF5 outputs
   are plain float64, bool and int64 columns, so the gap is not expected to change values, but whether bool/int64 `ef5.*` columns convert
   without silent loss is unverified and must be tested before merge. Carry as C-item until the client change lands or the note is recorded.
2. io PR #58 (adapter) must merge first; the worker imports `price_structures`.
3. Versions: core intraday and io duckdb contents changed after the last coordinated bump, so a coordinated bump (intraday, duckdb, then workers) is
   owed before any acceptance of the EF5 layers; this plan assumes it follows the PO's bump procedure (one table, `verify_versions`).
4. Service/MCP catalogue: the `ef5.*` feature ids and backend identity must be registered where the service and MCP list producers; scope of
   that registration is outside the worker and needs its own story.
5. DuckDB sink: confirm bool and int64 value columns are stored and read back unchanged (the SDK codec round trip is verified in the io gate;
   the sink path is not yet exercised for these dtypes).

## Update: prerequisites implemented (this PR)

- Blocker 1 (remote client decode and feature context): `equity_feature_client.native._cell` now takes the column's feature context; a
  `tape.top_k_notional` column rebuilds its `TopKTrades` through the constructor with `ranking="notional"`, every other column as before. A
  notional-ordered cell decoded without the context is refused (fail-closed), so no cell type is silently mis-ranked. Regression gate
  `tools/verify_client_feature_context.py` (no unit tests): `ef5.*` float64, bool and int64 columns convert unchanged, the notional column
  converts with the context and not without, SDK round trips equal. client/mcp suites keep their main failure counts (5 and 9).
- Blocker 5 (DuckDB sink bool/int64): the io EF5 gate now publishes each of the 89 results to a fresh DuckDB sink and requires an equal
  read-back (io PR "EF5 gate adds DuckDB sink read-back"): all equal.
