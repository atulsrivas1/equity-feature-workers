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

## Builder re-pin after the EF5 bump (closed in part)

Commit pins now name the post-bump main commits (core 98778e7, io 94217cf, workers 99b5fed; core `tools/_versions.py` `SIBLING_COMMITS` and `SIBLINGS` carry them and the
sibling versions). Builders were run end to end with the pinned venv against detached checkouts of those commits; each builder builds its archives twice and asserts
repeat equality. Independently, a fresh double build of all 27 packages of the three repositories (SOURCE_DATE_EPOCH, sdists normalized as by
`tools/build_explanations.py`) is byte-equal for wheels and equal for sdists: 27 of 27. Wheel names confirm intraday 0.1.0a5, duckdb 0.1.0a11, workers 0.1.0a17,
client 0.1.0a1, provenance 0.1.0a4 and the earlier versions (contracts 0.0.4a5, features 0.0.4a6, evidence 0.1.0a3, io-sdk 0.1.0a3, mcp 0.1.0a2, duckdb-sink 0.1.0a1,
parquet 0.1.0a2, bundles 0.1.0a2); the conformance and r10 archives carry duckdb 0.1.0a11 and workers 0.1.0a17, the supplied-inputs archive carries intraday 0.1.0a5.

Also in this change: `tools/verify_versions.py` now gates the version pins stated in package READMEs (`name==V`, ``name`V`` or `shortV`; sections headed by an older
version are history and skipped). An altered README fails; stale statements in the workers README were corrected.

**Ran and passed (workers):** build_supplied_inputs.
**Ran and failed for reasons outside this change (workers), so still open:** build_foundation (its installed `mypy --strict` step reports errors in existing worker modules such as
`equity_breadth_adaptive`), build_client and build_mcp (their installed suites fail by the same test names as on main), build_service (installed environment check), build_mcp_evidence and
build_agent_research (`KeyError: 'evidence'` in the package table they borrow from core). build_mcp's metadata audit now includes the optional extras. **Not ported:** build_provenance (marked
NOT YET PORTED to this layout); build_resilience keeps its accepted historical baseline.
