# Serialized publication and immutable operational generations

EQ061#69, experimental worker0.1.0a6. Calculations/core/IO/math/task/result/receipt codecs are unchanged. Publication and generation storage belong in the worker; backend conversion/layout/transaction/locks remain in the accepted sinks. No provider, task claim, supervisor, catalog selection or witness persistence is introduced.

## Bounded publisher

`SerialPublisher(sink, destination_scope, *, limits=PublicationLimits(...), requirements=...)` takes an explicitly supplied caller-owned sink. Supported visibility is manifest_last or transactional and writer mode serialized_writer. The creating Thread object owns publication; only it can call drain. Ownership does not transfer to a successor after the creator exits, even when Python recycles a numeric thread identifier. The caller closes the sink and must not concurrently use that instance elsewhere. Shared Parquet roots serialize whole attempts, including distinct partitions; shared DuckDB databases retain one writer. No root/database concurrency guarantee changes.

`submit(task, results)` accepts a sealed immutable TaskManifest and tuple of complete canonical FeatureResults. SDK preparation bounds result count/bytes/cells/evidence, then actual entity/feature inventory and existing OutputManifest enforce source/config/timing/header/destination identity. Submission does no source acquisition/calculation or sink callbacks. Pending task count and logical declaration/result bytes are bounded; identical pending submissions deduplicate, changed content for that task conflicts. Producers may submit from other threads. During a bounded drain the queue lock covers the whole pass and callbacks: producers get explicit BUSY backpressure and must retry under their own bounded policy. No hidden busy loop or blocked producer thread is introduced.

`drain(cancellation=...)` makes one bounded pass in insertion order. Each entry first performs live SDK lookup, receipt/content verification and logical readback. A valid existing commitment returns its original OutputManifest without another begin. Only factual ABSENT or ABORTED permits a new accepted SDK publish attempt; STAGING/UNKNOWN/corrupt or failed readback waits. SDK commit uncertainty remains conservative; unresolved entries stay pending with a fixed reason. A later pass looks up actual commitment before retry. Post-publish live receipt/readback is mandatory. Resolved receipt wire replaces declared wire within the shared pass budget, retaining logical result bytes and admission policy overhead. Complete entries are returned and removed; callers retain returned outputs and can explicitly resubmit to obtain verified replay. Cancellation/faults do not silently drop entries or imply completion. `pending` returns task identities or BUSY during a drain.

`PublicationProgress(task_sha256, output, reason)` is committed only when an actual verified output is present. Fault reasons are fixed SDK/worker codes, never backend exception text. These bounded operational observations are not mathematical availability statuses or exactly-once task claims. Source/result records and trusted synchronous callbacks are already caller-owned; logical byte bounds do not certify hard RSS, physical output size, timeout/preemption or performance.

## Completion-last generations

`GenerationSpec(namespace, job_id, generation_id, tasks)` declares a nonempty exact ordered immutable task set. Tasks must match namespace/job/generation and be unique. `key` binds operational namespace/job/generation; `identity` additionally binds ordered task digests. Changing tasks under a completed generation key conflicts.

`GenerationStore(root, *, limits=BarrierLimits(...), requirements=..., protected_sources=())` takes a trusted caller-owned local metadata directory. Supplied protected source paths must be disjoint from the root in both directions. Omitted source paths are not discovered/authenticated; callers must also configure result sinks separately from input stores. Constructors do not create storage. Supported qualification is local Windows/Linux; no UNC/network filesystem, distributed writer, power-loss or arbitrary filesystem adversary claim.

`publish(spec, dependencies)` requires exact expected task coverage and every dependency required. SDK logical output records and actual receipt/readback are reused through EQ060 barriers, including live-wire/shared byte quotas. Waiting returns `GenerationOutcome(complete=False, barrier=..., outputs=())`. No completion is written for missing/staged/corrupt/cancelled outputs. One stable local OS lock serializes store writers. Private bounded stage metadata is flushed before atomic publication of completion; completion is last. Existing completion is compared exactly and is never replaced. Retry reuses immutable committed outputs and completion. Source databases/rows are neither acquired nor modified by this store.

`read(spec, dependencies)` ignores all private stage files. Missing completion returns complete=False; malformed/unsupported/corrupt completion raises a fixed typed failure rather than becoming absence. A stored completion still requires current actual receipt/readback for every expected dependency. Caller receipt mismatch, backend unavailability/corruption or live-writer contention cannot yield a qualified complete generation. The returned ordered outputs match the immutable stored record exactly. It is an operational completion record, not an accepted-generation catalog pointer; EQ064 owns selection/history/read-only catalog access.

Storage belongs only in `<root>/.efworker-generations1/`: stable writer.lock, private stage-<opaque>.json and immutable <generation-key>.complete.json. Closed efworker-generation1 canonical inert metadata contains namespace/job/generation/identity and existing encoded OutputManifest records. There is no duplicate result/receipt/numerical witness schema or executable deserialization. Completion metadata and expected dependency/result declarations have explicit per-attempt byte/count caps. Interrupted private stages remain retained; the caller's retry count and stage lifetime govern cumulative retained disk space, not a hard aggregate storage quota. EQ062 owns executor/spill budgets. No automatic cleanup of unrelated paths occurs.

## Explicit example

```python
from pathlib import Path
from equity_feature_workers import (
    SerialPublisher, PublicationLimits, GenerationSpec, GenerationStore,
    Dependency, BarrierLimits,
)

# sink, requirements, sealed tasks and complete result tuples are caller-supplied.
# Results come from existing public calculations/commands; no acquisition here.
publisher = SerialPublisher(sink, destination_scope,
    limits=PublicationLimits(max_pending_tasks=64, max_pending_bytes=8388608),
    requirements=requirements)
for task, results in task_results:
    publisher.submit(task, results)
completed = {p.task_sha256: p.output for p in publisher.drain() if p.committed}
dependencies = tuple(
    Dependency(task.task_sha256, task, completed.get(task.task_sha256),
               sink if task.task_sha256 in completed else None)
    for task, results in task_results)
spec = GenerationSpec(namespace, job_id, generation_id,
                      tuple(task for task, results in task_results))
store = GenerationStore(Path(generation_root), limits=BarrierLimits(),
    requirements=requirements, protected_sources=(Path(original_input_path),))
outcome = store.publish(spec, dependencies)
# complete is true only after all declared committed outputs pass live readback.
```

Factories remain explicit per-run existing SinkRegistry registrations. Physical tests use accepted Parquet0.1.0a1/PyArrow20.0.0 and DuckDBsink0.1.0a0/DuckDB1.5.6. Worker mandatory dependencies remain only purefeatures0.0.4a4 and SDK0.1.0a2. Existing CLI commands are unchanged; no default filesystem discovery or backend loading is added. This story's interfaces are supplied local Python composition.

Twenty new physical/operational tests preserve the frozen two-member literal returns+0.2/-0.2, exact wire/receipt replay and ordering, real contention, concurrent producer/owner boundaries, count/byte/conflict/entity admission, cancellation, uncertain-commit lookup recovery, corruption/redaction, expected-generation completeness, interruption/last publication, immutable conflicts, malformed metadata, cross-process OS store ownership, protected original database/row preservation and explicit factories. See [qualification and actual delivery](EQ061_DELIVERY.md). Current installed/native/review/release evidence must be established there; no speed, private-source admission or historical generation acceptance follows from synthetic temporary-volume tests.
