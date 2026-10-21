# EF1 supplied current-state status orchestration

Source authoring only for [EQ165 #505](https://github.com/atulsrivas1/equity-features/issues/505) and supporting [EQ213 #571](https://github.com/atulsrivas1/equity-features/issues/571). This additive subset preserves anchor/clock behavior. It does not complete EQ165, the original full-scope EQ164 audit, EF1R04 transition-history/open diagnostics or EF1/EQ169 acceptance.

## Frozen contract and API

The coordinator's `ef1-contract1` fixture ledger SHA256 `02940da0c4663bbced050ac551e7fbafe2a1da1534e4c6fa5c87eaf90d420076` and independently reviewed `STATUS_INPUT_SLOTS.md` amendment SHA256 `bff0f3f110cd5b99950d94e0acc19acba0570f2e9eae727f7d88281f45ecd3e0` govern this implementation. The amendment changes local missing-role representation and deterministic proof/witness admission only; wire protocols and the original ledger bytes stay unchanged. The canonical coordinator freeze receipt preserves reviewer identity and source-authoring admission.

Explicitly import these functions from `equity_feature_workers.equity_status`:

| Function | Contract |
| --- | --- |
| `status_headers(config)` | Exact public full four-header set for the selected dimension, in lexical feature-ID order. |
| `required_status_inputs(config)` | Deduplicated actual pure-library requirements. |
| `compute_status_inputs(task, batches)` | One unchanged complete `FeatureResult` in a tuple; no output concatenation, relabelling or mathematics. |
| `make_ef1_status_work(config, *, inputs, batches, instrument_id, job_id, generation_id, partition_id, destination_scope, max_input_bytes=1048576)` | Sealed status `WorkItem` with four explicit manifests and present-only original batches. |

`config.identity` is exactly `ef1.status`; `task.family` is exactly `status`. Context `selection.dimension` is one of trading/quoting/ssr. A homogeneous selection yields that dimension's state and age_ns plus admission_state and admission_reason. Different dimensions, age policies and query clocks require distinct job/generation identities. Config decoding and reconstruction use the public pure builder/decoder, including actual session/window/availability/adjustment operands. No event values, times, sequence or knowledge facts are copied into context.

Exactly four `InputManifest` roles appear once, in this order:

1. `calendar_provenance`
2. `status_states`
3. `status_prefix`
4. `status_validity`

All actual inputs are canonical REFERENCE batches. Generic admission first checks actual metadata, namespace/instrument, unique original input IDs, coverage row counts and actual input byte limits. Then status admission checks the fixed producer/config/header/governed-session/history shape. The mechanical manifest walk emits None for a missing declaration or consumes the next original admitted batch for a present declaration; any remainder rejects. A covered-empty batch retains its original declared slot and metadata. Workers never infer a role from vocabulary, source ID, revision, row count or neighboring slots. The pure library owns role vocabulary and all causal proof mathematics.

Workers pass the concrete exact four slots directly to the public `compute_ef1`. Metadata input bindings retain the entire actual present metadata in canonical lexical role order, independently of slot order. The returned object's exact namespace/session/config/availability/input/backend/header identities and entity are checked; successful results are returned without reconstruction. Candidate versions remain features0.0.4a5, contracts0.0.4a4 and workers0.1.0a15; no accept-all dependency policy or new codec.

## Semantics preserved at the boundary

The pure library requires original state event<C, original known_at<=K in both availability modes, explicit original seed identity, correct dimension/epoch and reversible strict seq19/event IDs. Supplied complete-prefix proof and finite validity proof must individually cover the selected scope. No sequence generation, missing-message inference, interval stitching, certificate arbitration or last-observed fallback occurs in workers. Calendar uses the original explicit C01 schedule/c1 proof. No built-in maximum event age exists. An explicit maximum can reject the state while a proved descriptive age remains available. The two diagnostic outputs describe evaluation readiness; their availability does not establish factual readiness.

The actual selected original state and age witnesses remain untouched. Where current-event identity is unproved, no last-observed witness is substituted. Frozen S02 at C17 retains incomplete_coverage/governed_gap and incomplete_status_prefix until complete-prefix/transition prerequisites establish current identity. Its separately complete-prefix/expired-validity mutation yields missing_input/ineligible and outside_effective_interval.

Status input bytes are capped at1MiB; smaller caller task limits remain effective. The independent existing TaskManifest actual encoded JSON1MiB and context4096 ASCII/string limits remain unchanged. Those are separate from the pickle byte limit on actual input batches. Aggregate4096 REFERENCE rows (including calendar and both proofs), sink128 evidence limit and result/publication/supervisor limits remain mandatory. No general budget/scheduling/manifest change is made.

## Closed publication and demonstration

Explicit `equity_feature_workers.ef1_runner.run_ef1_statuses` reuses the existing private bounded sequential runner, serialized sink owner and full canonical SDK result readback. It admits only the status callback. Existing anchor/clock wrappers retain signatures and closure. Distinct-config job/generation guards, cancellation, pending publication state and factual postcommit receipts remain unchanged. A readback failure does not claim rollback.

`examples/ef1_status.py` uses actual I/O `synthetic_status_fixture(case=...)` and `synthetic_status_context(case=...)`, exact per-role provenance, the existing separate-output path guard and actual DuckDBSink. It reads the complete stored canonical result, verifies receipt/content against the execution output, closes the sink, and writes an exclusive escaped HTML export containing all values/quality/bindings/original witnesses and receipt hashes. No JavaScript, remote assets, source DB/private data or source-authentication claim is introduced.

After coordinator source/artifact/resource admission, intended commands use an absolute separate generated output path and a new HTML path:

```text
python examples/ef1_status.py --output-db <absolute-output.duckdb> --html <absolute-new-status.html> --run-id status-demo --case S01
```

The closed case choices S01/S02/S03 use C=K17/16/6 respectively, E20, and their original explicit seed/proofs. A replay preserves the original run/case identity and uses a new HTML filename. Exporting a replay does not create a new publication identity.

## Authored evidence and remaining gates

`tests/ef1_status_fixture.py` hand-authors original S01 trading, S02 SSR and S03 equal-time SSR rows, exact source metadata/c1 schedule, explicit selected seeds, proof rows and eight independent literal four-value/quality goldens. It does not import I/O factories or derive expected results from library outputs. Coverage counts and original selected witness literals are independently asserted. Every supported positive/negative case is mandatory with exact candidate versions; no missing-library skips.

`tests/test_ef1_status.py` authors complete SDK direct/worker parity; per-role/mixed/all missing and covered-empty identity; manifest order/shape/leftovers and contradictory populated vocabulary; binding/header/config/history/budget bounds; known-at proof/transition/calendars in both modes; original state/age witness and stale-state age; absent seed/no fallback; deterministic overlap rejection, disjoint proofs and rejected stitching; absent-state coherent proof targets; aggregate4096-row admission; whole unchanged result object and metadata identity; closed wrappers, full sink readback, replay, distinct config scopes and factual readback failure receipts. The companion I/O fixture remains a separate physical DuckDB demo input.

All new tests/demo are **authored and unexecuted**. No local imports, syntax checks, tests, builds, benchmarks, DB creation or N-drive reads occurred during the EQ142 resource reservation. No commit/push is authorized before coordinator review. Root owns cross-repository pins, qualification helper/census and workflows. Remaining gates include final source review, fresh independent final-head review, exact dependency qualification, current remote CI, numerical/typing/legacy/bothOS/four-form installed parity, physical DuckDB replay/readback, actual rendered browser demo and canonical issue/release acceptance. This document is no evidence of any passed runtime gate or released capability.
