# EQ213 EF1 supplied execution

Current additive clock source work is documented separately in [EF1_CLOCK_IMPLEMENTATION.md](EF1_CLOCK_IMPLEMENTATION.md). The anchor record below retains its original scope/evidence; clock source authoring is not an anchor release or qualification claim.

Status: source authoring in progress; unexecuted. Canonical story [EQ213 #571](https://github.com/atulsrivas1/equity-features/issues/571), EF1 epic569/milestone34. Owner authorized reviewed-subset parallel implementation; EQ164 full inventory audit remains an EF1 release gate.

Contract: ef1-contract1, ledger SHA256 `02940da0c4663bbced050ac551e7fbafe2a1da1534e4c6fa5c87eaf90d420076`. Canonical source-authoring freeze/admission is in equity-features `docs/ef1/`. This isolated worker branch starts at accepted `231349aaf2bf1b9ec21f45807382ca9c6522520c`. Frozen EQ142 sources/jobs are unchanged.

Initial owned slice: explicit supplied anchor-trades and actual-notional-bar callbacks, thin local runner, independent admission/result-parity tests and demonstration/readback documentation. Other EF1 producers remain unimplemented in this worker slice. No numerical formulas are implemented in workers.

No tests, builds, scans, benchmarks, fixture calculations, database reads/writes or browser demo have been executed. Source authoring only until the coordinator supplies an execution window independent of EQ142. Coordinator review precedes any commit/push; fresh independent final-head review and release gates remain mandatory.

Packaging amendment implemented in source: new workers0.1.0a15 exactly pins features0.0.4a5 and SDK0.1.0a2; contracts/wire0.0.4a4 and intraday0.1.0a3 remain unchanged. Supplied-session backend admission in this new artifact is exactly0.0.4a5; all other checks remain exact. Prior published workera14/featurea4 is unchanged. This resolves the source dependency mismatch; actual installation and legacy/full-result compatibility remain unexecuted gates.

## Authored public API

Import `equity_feature_workers.equity_sessions` explicitly:

- `ef1_headers(config)` returns the full fixed public-library anchor producer headers. No subset selection.
- `required_ef1_inputs(config)` deduplicates the producer's public InputRequirement records without introducing another input schema.
- `ef1_history(config, inputs, governed_sessions)` binds exact config/anchor and complete actual input declarations into an initialization digest, with target-only governed history and no incremental state.
- `compute_ef1_inputs(task, batches)` is the existing WorkItem callback signature. It supports only anchor_trades/anchor_bars in this slice; it returns exactly one unchanged complete FeatureResult and validates every header/binding/config/backend/entity identity.
- `make_ef1_anchor_work(...)` seals caller/adapter-owned InputManifest acquisition/revision identities and actual batches. It performs no acquisition, numerical calculation or publication. Present primary and anchor_provenance slots have an exact batch/declaration bijection; missing and observed-empty remain distinct.

`equity_feature_workers.ef1_runner.run_ef1_anchors(items, sink, requirements=...)` uses the unchanged sequential bounded supervisor and serialized publisher. Different configs require distinct job and generation identities. Return includes actual supervisor outcomes, explicit additional full-result readback outcomes and the live publisher for caller-controlled pending recovery. It does not automatically retry. Caller owns sink lifetime and must keep it open while draining/reconciling pending publications. A receipt retained beside a readback error is not a rollback claim. Cancellation can leave completed/committed tasks; kernels are not preempted.

The exact EF1 route admits distribution0.0.4a5/backend python-exact0.0.4a5 and contracts0.0.4a4. Under the scoped packaging amendment, this new worker artifact's supplied callbacks also require backend0.0.4a5, with continuous-quotes retaining python-exact-compensated and all other original checks preserved. Root-derived backend metadata evolves with the feature distribution; olda4 and newa5 result bytes are not claimed identical. Independent numerical/quality/evidence/input/config parity and expected version evolution must be qualified without metadata rewriting.

## Authored example and local viewer

`examples/ef1_anchors.py` accepts fixed A01/A02/A01-missing-notional synthetic packets and trades/bars_actual_notional modes. It calls `equity_feature_duckdb.ef1.synthetic_anchor_fixture` and `separate_output_path`, then the worker route and existing `equity_feature_duckdb_sink.DuckDBSink`. It has no arbitrary import path/provider/source database. CLI requires explicit absolute output DuckDB and new HTML paths plus persistent run ID. The caller executes this only in an admitted qualification window with compatible reviewed artifacts:

```text
python examples/ef1_anchors.py --output-db <absolute-new-output.duckdb> --html <absolute-new-export.html> --run-id fixture1 --mode trades --case A01
```

The HTML is generated from verified complete SDK readback, contains scalar values/units/status/reasons, publication/content/task identities and full canonical binding/evidence bytes, and is written exclusively after sink closure. It escapes text, includes no remote assets or JavaScript and preserves int64 values as text. Replaying an identical request preserves job/generation identity and uses a distinct new export filename; corrections change request/revision identity. A02 demonstrates covered-empty zero totals/null VWAP; missing-notional bars demonstrate independently ready volume. No browser demo or actual DuckDB readback is claimed yet.

## Authored validation and exact resume

`tests/ef1_anchor_fixture.py` independently supplies scale2 trades1000×2+1300×1 and a compatible actual-notional bar. Expected VWAP11.00, volume3 and notional coefficient3300 are authored mathematics, not values taken from the callback. `tests/test_ef1_anchors.py` covers full direct/worker parity, missing/covered-empty, missing notional, proof knowledge, role/header/history/byte/backend/config/binding rejection, serialized readback/replay, config scope separation, cancellation and factual postcommit readback failure. It reuses the accepted in-memory lifecycle fixture for worker unit tests; this does not establish DuckDB behavior.

Every test is **unexecuted**. Mandatory EF1 tests fail on missing API or wrong features0.0.4a5/contracts0.0.4a4/workers0.1.0a15; there is no optional version skip. After resource admission, install the exact reviewed candidates and run explicit EF1 tests with the expected ten method inventory and zero skips, actual DuckDB example/readback, required legacy/absence/version/import/typing/regression and declared artifact/platform qualification. Preserve all failure evidence. Independent review, coordinator integration, full EQ164 reconciliation and other EF1 producers remain release gates. [CI migration and qualification plan](EF1_QUALIFICATION_PLAN.md) records current foundation/supplied pin placeholders and preserves historical companion cohorts. Coordinator must replace placeholders with matching reviewed SHAs before push.

## Independent review correction W1

Separate automated reviewer `/root/ef1_worker_review` completed actual dirty-source review in `WORKER_IMPLEMENTATION_REVIEW.md` and found W1/P1: the old featurea4 dependency contradicted EF1's exacta5 requirement and the root-derived legacy backend version. The reviewer independently approved the scoped design in `PACKAGING_AMENDMENT.md` for source authoring. This is automated source/design review, not human review, final-head PR approval or execution evidence.

Correction authored: worker metadata/root versiona15, exact featurea5 dependency, supplied-session fixed backenda5, current-version expectations in supplied_original/acquisition_probe/probe_foundation/pilot_fixture, and strict mandatory EF1 dependency checks. Canonical manifest/wire versions, intraday backend and historical companion builder source pins/receipts were preserved. Subsequent coordinator review required current foundation/supplied workflow/build migration: current-only reviewed-pin placeholders and featurea5/workera15/adaptera9 assertions are now authored with unchanged gates. No numerical golden values changed. An initial source-search referenced the wrong probe path (`tools/probe_foundation.py`); the actual `tests/probe_foundation.py` was located and amended. Independent recheck of the corrected bytes remains pending; do not label W1 verified fixed until it occurs. All tests/builds/DB/benchmark operations remain unexecuted.
