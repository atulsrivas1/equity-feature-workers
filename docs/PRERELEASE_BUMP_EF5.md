# Coordinated pre-release bump for the EF5 layers - October 28, 2026

Paired with the core and io PRs of the same set. Pre-release label move only.

## Packages moved (this repository)

| Package | From | To | Why (content changed since the last bump) |
| --- | --- | --- | --- |
| equity-feature-workers | 0.1.0a16 | 0.1.0a17 | EF5 worker (`equity_ef5`, `run_ef5`); asserts intraday 0.1.0a5 |
| equity-feature-client | 0.1.0a0 | 0.1.0a1 | remote conversion now carries feature context (notional TopKTrades) |
| equity-feature-provenance | 0.1.0a3 | 0.1.0a4 | preflight admits duckdb adapter 0.1.0a11 (constant changed) |

Pin-only packages keep their versions and follow the pins: in workers, mcp (client pin a1) and service (extras duckdb a11, workers a17); in io the other
packages are unchanged. No tag, no publish, no release claim. Single source: each repository's `tools/_versions.py` (updated) checked by `tools/verify_versions.py`.

## Digests differ from the old versions (confirmed)

- Intraday catalogs (separate catalogs, from the installed a4 and a5 packages): EF5 catalog `0cc61c625779` to `ccb1cdaea7eb`, path-efficiency catalog
  `91ba7fb6a043` to `5f2a343faf87`, intraday (R17) catalog `848e8776ddb1` to `11e0c702d405` (each carries `package_version`).
- Result identity: the same EF5 library result (fixture CD07) encodes to a different SHA-256 under backend_version 0.1.0a5 (`c1f33a43416c...`) than under
  0.1.0a4 (`6b866b365e5c...`), so an a4 artifact and an a5 artifact cannot be confused.
- DuckDB: the receipt `adapter_version` default is now 0.1.0a11, so acquisition receipt identity digests differ from a10 by construction.

mcp (client pin) and service (extras) are pin-only and keep their versions, as in the previous bump. Goldens: none embed these labels (the gates and tests derive them).

**Still blocking any release claim:** the commit-pinned builders (`tools/build_*.py`, `qualify_*.py` with commit constants) still name pre-bump commits and need a re-pin after this bump merges (follow-up PR set); they are not executed end to end until then.
