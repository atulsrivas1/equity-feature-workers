# Supplied-input calculations

EF1 current-state status source candidate: explicitly import `status_headers`, `required_status_inputs`, `compute_status_inputs` and `make_ef1_status_work` from `equity_feature_workers.equity_status`; import closed `run_ef1_statuses` from `equity_feature_workers.ef1_runner`. Config identity `ef1.status`, task family `status`, one selected dimension/four output headers. Four exact ordered manifests calendar_provenance/status_states/status_prefix/status_validity retain present-only original wire batches; ordinary admission precedes mechanical reconstruction of four CanonicalBatch-or-None pure slots. No role inference or factual fallback. [Signatures, identity, original witnesses, budgets, synthetic example and unexecuted gates](../EF1_STATUS_IMPLEMENTATION.md). This is authored source, not qualified/released behavior; the earlier EQ140 surface below retains its original scope.

Experimental workers0.1.0a14 development under [EQ140](https://github.com/atulsrivas1/equity-features/issues/474). Implementation and initial source checks are distinct from unfinished final review, measured performance, installed/both-platform artifacts and release acceptance. The [accepted contract](https://github.com/atulsrivas1/equity-features/blob/77249cb18aea21d4b21eeb2b380b291ccae4d130/docs/stories/EQ-140_BINDING_CONTRACT.md) and canonical live Project own scope/status.

Install the normal worker distribution for existing core supplied APIs. Install its `intraday` extra with the exact experimental intraday0.1.0a3 producer for intraday callbacks. These are source/qualified-artifact development distributions; do not infer a stable registry release. Root imports and the default CLI/acquisition dispatcher do not import the optional producer or gain new command families.

Explicit imports:

```python
from equity_feature_workers.intraday import (
    compute_intraday_inputs, intraday_headers, intraday_history,
    required_intraday_inputs,
)
from equity_feature_workers.supplied_session import (
    compute_supplied_session_inputs, supplied_headers, supplied_history,
    required_supplied_inputs,
)
```

Use existing TaskManifest, InputManifest, WorkItem and BoundedSupervisor. `compute_intraday_inputs(task, batches)` and `compute_supplied_session_inputs(task, batches)` are top-level callbacks returning exactly one complete FeatureResult in the outer tuple. Pure calculations receive normalized immutable facts only; acquisition, task construction and publication remain outside kernels. No callback closure/prepared-object upload, new registry or task codec is introduced.

For intraday, choose a nonempty unique tuple of the eight public intraday IDs. `intraday_headers(features)` returns the sorted actual headers; `intraday_history(config, features)` supplies the existing ordered-history/warmup/initialization fields. Discovery returns existing InputRequirement records with actual per-output rules. Bands consume their selected N-close window; EMA/RSI own their anchored closes; ATR consumes H/L and prior-slot closes, independently of target close. Batch capability does not imply update/restore/merge support.

The task's config embeds its existing closed chunked context. Supplied chunks have actual compact bars:000, bars:001 roles in original order, full actual source metadata and a bijection with nonmissing manifests. Real empty chunks retain bindings. No chunks/no declarations means an observed empty tuple; a sole missing bars marker means absent delivery/None and skips preparation. Missing markers mixed into actual chunks reject. Preparation is local to the computing process and reused once for the selected operand families.

The core-only operation supports exactly structure, top_k, continuous_quotes and bars_prior_close. Use `supplied_headers(family)` without filtering the fixed result. Declare a primary bars/trades/quotes slot, actual or explicitly missing, and the admitted auxiliary slot as specified by the family. Batches follow primary then auxiliary with missing slots omitted. Seed mode declares quote_seed; inactive/unknown forbid it. Bars/prior-close always declares actual or missing prior_close. Real empty delivery is an actual empty canonical batch.

Quote seed is a real preopen target-session update with original causal expiry. Prior close is one real DAILY row matching the immediately preceding actual governed session interval, unit/action/knowledge/source scope. Missing/unavailable references preserve independent output quality. `supplied_history(config, family, inputs, governed_sessions)` returns existing ownership fields; the callback recomputes these identities even if callers bypass a helper. Structure/top-K have no ordered initialization; continuous has no invented prior-session warmup; supplied prior close owns only its actual consumed predecessor.

`python examples/supplied_intraday.py` runs an original six-slot, all-eight synthetic example through the actual sequential worker. Its expected EMA14.75 and original context are supplied locally. Call the example's typed `calculate()` with explicit mode/degree for other supported local modes. This demonstrates API behavior, not a speed/resource-utilization result. The bounded scheduler groups intersecting instruments and publishes after pool completion; it does not promise same-symbol parallelism, early-symbol delivery or persistent live workers.

## Publication limits

`PublicationLimits(max_receipt_wire_bytes=...)` optionally reserves the **escaped JSON string contribution** of the final SDK receipt, including quotes. Default None retains legacy behavior. Before queue mutation/sink I/O the publisher requires receipt-free output bytes minus JSON null's4 bytes plus this bound to fit the unchanged1,048,576-byte output codec. Result, input, queue, IPC and resident limits remain independently applicable.

The caller must establish a defensible receipt bound for the actual sink; generic ResultSink capabilities do not provide one. New synthetic qualification uses400000 bytes with its actual owner-checked serialized memory fixture. This is not a universal sink or slot limit. Actual installed sizing found a1536-slot receipt-free output fits but its committed output exceeds the cap;256/1024 fit in that fixture and2048/4096 reject. Direct4096 pure computation remains distinct from worker wire admissibility.

Every committed path checks the actual bound, including fresh return, verified lookup, idempotent replay and repeated drain. If a sink exceeds its declared bound after commit, RESOURCE_LIMIT leaves the task pending. Actual sink lookup/readback can still be committed; pending/unverified never means rollback or confirmed absence. Repeated drain does not bypass the violation. Reconcile actual state explicitly; do not publish a second identity to hide a committed result.

## Errors and compatibility

Known malformed schema/config/selection/order/sampling/adjustment maps to exact existing INVALID_COMMAND; actual binding/result/context/history mismatch to RESULT_IDENTITY_MISMATCH; actual byte/reservation admission to RESOURCE_LIMIT; overflow/unexpected faults to CALCULATION_FAILED. Ordinary missing/empty/partial/unknown/future data preserves the pure result's status/reasons/evidence. No private exception text is returned in worker diagnostics.

Core/contracts0.0.4a4, schema/math policy and core39 stay unchanged. Existing default acquisition/command, service/client/MCP APIs and their frozen dependency inputs are preserved. In particular the existing service jobs extra pins workers0.1.0a23 while its old builder snapshots worker0.1.0a13; this pre-existing constraint is preserved and **is not a claim that the service jobs extra installs or exposes a14/intraday**. This story's direct supplied worker surface is separately qualified. No service extra migration, new hosted endpoint or related compatibility success is inferred.

Independent original mathematical/status/quality/evidence fixtures, exact full direct/worker/SDK identity, sequential/thread/process parity, strict typing, absence/default regressions, closed diagnostic and receipt controls are mandatory. Performance adoption additionally needs the frozen repeated resource/E2E protocol; initial source tests alone do not establish it.

`python tools/build_supplied_inputs.py --core-root <accepted-core-checkout> --io-root <accepted-io-checkout>` prepares qualification from frozen Git blobs. Its separate workflow uses accepted core b1f5445 and I/O18d8676, without changing historical foundation/service pins. It checks six packages/twelve repeat archives, selected SDist rebuilt payload equality, fresh base/combined wheel/SDist forms, exact installed files/RECORD/console wrapper, existing core39 and typed consumers. A completed report describes its actual executed forms; a running or failed report is partial evidence. Native Windows Job tests establish accounting/cleanup controls only. Linux skips those four Windows controls explicitly, and base forms skip the optional intraday methods; combined forms must execute the intraday methods. No benchmark or release claim follows from these checks.
