# Outer provenance and actual generation readback

EQ103 adds experimental `equity-feature-provenance==0.1.0a1`. It explicitly
requires the pinned accepted DuckDB adapter/sink, worker and SDK packages,
including native DuckDB1.5.6 and NumPy2.2.6. This is an outer I/O composition;
pure calculations, receipts and diagnostics never import it. Initial support is
one entity, one `trades` input and the five unchanged `compute_trades` outputs.
Other families and native reference acquisition are unsupported.

`AcquiredInput(role, receipt, batch)` and `TaskEvidence(expected_task, acquired,
result, receipt, output=None)` detach the bounded borrowed record graph. Keep the
original expected task and original R9 execution receipt. The latter's retained
input fingerprints anchor normalized content; a new hash of observed data is
never substituted for the expected original.

`bind_supplied_provenance(evidence)` checks complete supplied records and returns
a frozen `ProvenanceReport`. Its mode is `supplied`, generation status
`not_checked`, and operational readback is false. A supplied SDK completion receipt
does not prove an actual backend read occurred. This function does not open files,
reacquire source data or discover changes on disk. Reacquisition with retained
original pins remains the accepted adapter's separate responsibility.

`verify_live_generation(store, spec, dependencies, evidence, cancellation=None)`
is explicit operational I/O. It accepts exact `GenerationStore` and exact
`DuckDBSink` dependencies only, in complete expected task order. All supplied
records are validated before store access. The accepted store owns its limits and
performs actual receipt lookup and full result readback. Missing completion returns
`GenerationProvenanceReport` with `live/waiting/False`; complete exact verified
inventory returns `live/complete/True`. Per-task reports still describe supplied
bindings and keep `not_checked/False`. Publicly constructible outcomes/reports are
not execution authorities; no API accepts a report as proof that execution ran.

Reports never authenticate source truth, historical acquisition, rights,
environment or suitability. Operational completion can coexist with unavailable
quality and a denied diagnostic policy. No input data, calculation result, quality
or original receipt is rewritten. Caller-forged originals and anchors can describe
a different internally consistent chain.

All errors carry fixed `ProvenanceError.code`: `invalid_schema`,
`unsupported_version`, `missing_provenance`, `inconsistent_binding`,
`limit_exceeded`, `readback_failed`, or `cancelled`. Provider payloads and exception
chains are not exposed. Cancellation remains cooperative and cannot interrupt an
active synchronous native operation. No physical quota or hard deadline is claimed.

The closed graph allows at most16tasks; one input and512rows/16columns/8192cells per
task; 32files;128missing dates;16price conversions;64unmapped labels;128governed
sessions;512result evidence rows. Ordinary labels are128codepoints/512UTF8bytes.
Accepted compound `SourceBinding.mapping_version/input_id` and retained
`InputFingerprint.input_id` alone allow256codepoints/1024UTF8bytes. Opaque file
paths are4096codepoints/16384UTF8bytes and never opened by supplied verification.
All strings refuse controls/surrogates. Whole-graph depth16/node32768 and fixed
work estimate1MiB/task run before hashing/encoding, followed by a separate actual
encoded-byte1MiB/task bound; aggregate16MiB/524288nodes/131072cells applies. Limits
are conjunctive and stricter schema/estimate limits may precede another maximum.
These are finite input-work limits, not memory reservation measurements.

The [plan](https://github.com/atulsrivas1/equity-features/blob/0fc675d/docs/stories/EQ-103_PLAN.md) and
[validation](https://github.com/atulsrivas1/equity-features/blob/0fc675d/docs/stories/EQ-103_VALIDATION.json) retain development failures,
independent controls and delivery status. The
[synthetic readback example](../../examples/provenance_readback.py) uses explicit
owned fixtures, actual public adapter/sink/store APIs and unavailable late data.
Wheel/source-distribution publication remains experimental, with no registry
installation promise before the story's actual acceptance.
