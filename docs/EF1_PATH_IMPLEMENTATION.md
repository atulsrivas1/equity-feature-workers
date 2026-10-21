# EF1 sampled gap and opening-drive worker source

Source authoring only, October 20, 2026. Same selected [EQ167 #507](https://github.com/atulsrivas1/equity-features/issues/507), [EQ213 #571](https://github.com/atulsrivas1/equity-features/issues/571), EF1 epic569/milestone34. Frozen path amendment MD SHA256 `e55be33767184cafb971827f041c55933a41e7010e23a8b581f0aaa41d6667fe`, JSON `04c2743559de751d22225a2d521bd4f69eb75318f2f7e276ecde8c1bdf80a616`. The original ef1-contract1 ledger remains unchanged. No legacy/private numerical equivalence or complete EF1 release is claimed.

## Public APIs

Import `equity_feature_workers.equity_path_gap` explicitly for `path_gap_headers`, `required_path_gap_inputs`, `compute_path_gap_inputs`, `make_ef1_path_gap_work`; analogous names are in `equity_path_drive`. Each produces exactly one unchanged full pure-library FeatureResult: four gap or three drive outputs, exact features0.0.4a5/backend python-exact0.0.4a5 with canonical contracts/wire0.0.4a4. Root exports, package versions and historical companion constants remain unchanged.

Builders require caller-owned InputManifest declarations in exact order: gap calendar_provenance/path/prior_close; drive calendar_provenance/path. Missing manifest slots have binding=None and state=missing; actual batch tuples contain only the present corresponding CanonicalBatch subsequence. No None, fake empty batch, cloned identity, generated known-at timestamp or inferred completeness is supplied to the calculation. Present-empty and absent remain distinct. REFERENCE, selected TRADE/BAR and DAILY kinds are checked against exact roles. Original DAILY prior session, scope, fields, source identity and bindings are preserved; the pure library admits that preceding slot separately. Workers do not select prior rows or implement sampled-path mathematics.

Contexts are decoded and reconstructed through the public pure API, preserving the ASCII4096 context bound, exact config digest and original mathematical/session/calendar operands. Task families are exactly path_gap/path_drive. Governed intervals use the actual frozen calendar rows, including the explicit prior interval for gap. These routes have no recurrence/warmup/checkpoint state. Existing TaskManifest 1MiB serialization, input-byte, actual binding/entity/namespace admission, distinct input IDs and result identity checks remain in force. Full headers, original input bindings, backend/config/timing and target output entity are verified without metadata rewriting.

`equity_feature_workers.ef1_runner.run_ef1_path_gaps` and `run_ef1_path_drives` use the existing private `_run_ef1`, bounded sequential supervisor, serialized publication and complete SDK stored-result readback. Producer/config/sampling changes require distinct job and generation identities. Wrong callback/producer wrappers reject. Caller owns sink lifetime and pending recovery. A committed receipt remains visible when an additional readback fails; that failure does not assert rollback. Anchor/clock/status public APIs and execution behavior remain intact.

## Synthetic demonstration

`examples/ef1_path_gap.py` accepts G01/G02/G03; `examples/ef1_path_drive.py` accepts D01. Both require `--case`, `--output-db`, `--html`, `--run-id` (case has a documented default). They use the public synthetic I/O fixture/context helpers, closed worker routes, separate-output-path guard and existing DuckDB sink. They contain no source database discovery or private-source access.

Example, only in a coordinator-admitted execution window:

```text
python examples/ef1_path_gap.py --case G02 --output-db <absolute-new-output.duckdb> --html <absolute-new-export.html> --run-id fixture1
python examples/ef1_path_drive.py --case D01 --output-db <absolute-new-output.duckdb> --html <absolute-new-export.html> --run-id fixture1
```

The shared renderer exports actual verified complete SDK readback, all values/units/status/reasons, bounded original witnesses/bindings and task/publication/content identities. It escapes text, has no JavaScript/remote assets, renders int64 values as text and writes a new exclusive HTML export after sink closure. Provenance is explicitly synthetic. Selected witnesses are not an exhaustive path audit. These source-authored examples have not created a database, HTML artifact or browser demo.

## Authored checks and remaining gates

`tests/test_ef1_paths.py` has **23 authored test methods**, unexecuted. Its `ef1_path_oracle.json` is an exact raw copy of the independently reviewed frozen JSON: fourteen literal expected result populations, plus contract/control documentation. The independent test fixture constructs those supplied populations without I/O fixture factories and compares literal rational expectations, types, statuses/counts/reasons, evidence IDs/counts, prior/bar original bounds/timestamps, complete direct-worker SDK parity and codec roundtrip.

Additional source-authored controls cover each omitted role, original knowledge for each gap role in known_at/reconstruction modes, canonical errors before missing-role readiness, unit/scope/instrument/prior shape, endpoint trade versus completed-bar end, duplicates/reverse ordering, equal-time earliest ties, downward reversal, high/low-only bar crossings, first positive-volume open, zero/ineligible populations, null-volume/absent operands, null/zero prior, unknown population/count overflow, fixed headers/requirements, input/context bounds, closed wrappers, metadata rejection, memory-sink full readback/replay, distinct config identity, cancellation and retained receipt on readback failure. These controls are authored assertions, not executed numerical evidence or an exhaustive inventory claim.

No local project runtime, import, syntax checks, tests, build, benchmark, database operation or N: access was performed because the protected EQ142 reservation has no terminal exit receipt. Actual source/Git/text/hash inspection is the only current evidence. Owner policy allows exact-commit local validation/build evidence for merge; hosted CI completion is not a prerequisite. The resource hold still applies independently.

Coordinator must review actual changes before commit/push, reconcile source pins and qualification-helper test census, obtain a fresh independent review, then run exact-commit numerical/negative/legacy/installed/integration/readback checks when admitted. Test authoring is not Test status, qualification or release acceptance. Windows/Linux claims require their actual evidence; no platform inference. Full original inventory audit, remaining EF1 producers and browser demonstration remain release gates. No performance claim.
