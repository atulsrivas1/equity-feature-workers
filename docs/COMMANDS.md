# Bounded session commands — experimental0.1.0a3

Canonical [EQ058 #66](https://github.com/atulsrivas1/equity-features/issues/66), [pre-code plan](EQ058_PLAN.md). Public API: `SessionCommandSpec`, `CommandOutcome`, `run_session`, `run_registered`, `CommandError` and `CommandErrorCode`. Qualification/release remains pending until the issue's actual acceptance evidence.

Commands acquire one instrument/target session, invoke public `equity_features.session.compute_bars/compute_trades/compute_quotes`, admit results, publish through the existing SDK and verify sink readback. Event-weighted quotes remain distinct from continuous time weighting. Core numerical equations, schemas, nulls, quality, units, evidence and availability remain unchanged.

## Explicit invocation

Construct `SessionCommandSpec(job_id, generation_id, partition_id, family, request, config, features, governed_sessions, revision_id, destination_scope, max_input_bytes, ownership="serialized_destination")`. Families are bars/trades/quotes. Request is the canonical bounded historical AcquisitionRequest; configuration is canonical ConfigSpec. Headers must exactly equal the complete public calculation inventory: obtain `descriptor(compute_trades(None, config, entity=EntityKey(instrument, session))).features` for trades, and the corresponding public function for bars/quotes. This validates config through the same pure public calculation, without acquiring anything. Supply canonical IntervalSpecs in exactly the governed session order. Revision is caller-declared immutable source revision, not inferred truth.

Call `run_session(spec, source, sink, requirements=limits, cancellation=token)`. Source implements canonical HistoricalAdapter, sink implements existing ResultSink, limits is SDK SinkRequirements. Default cancellation never cancels; supplied token implements is_cancelled. A successful CommandOutcome holds the sealed OutputManifest and one complete FeatureResult. A source returns missing/unavailable via canonical terminal absent envelope, not None or an empty iterator. Observed empty is a supplied canonical zero-row batch with complete zero coverage. Partial source coverage remains partial.

For factories call `run_registered(spec, sources=..., sinks=..., source_id=..., source_config=..., sink_id=..., sink_config=..., credentials=..., requirements=..., cancellation=...)`. Registries are explicit per-run SDK SourceRegistry/SinkRegistry instances. Register trusted factories yourself. Credentials are a separate provider; never put credentials into spec/config/public JSON. No arbitrary import path, plugin scanning or backend selection from worker configuration.

The installed console command `equity-feature-worker --demo both` requires separately installed, already qualified `equity-feature-example-extensions==0.1.0a0`. It invokes the existing ExampleSource/ExampleSink directly and through their factories, computes independent bars500 shares/51200 actual notional/102.6 close-weighted price, and reports verified readback and the in-memory instance limitation. `python -m equity_feature_workers.cli --demo both` is equivalent.

Each real family is independently executable through an explicit trusted caller wrapper:

```python
from equity_feature_workers.cli import main
# Build spec and source/sink using the public contracts and your configured components.
# For explicit factories, resolve them through the SDK registries in this wrapper.
raise SystemExit(main(
    ["--session", spec.family],
    spec=spec, source=source, sink=sink, requirements=limits,
))
```

Without injected components --session fails with exit2. There is no automatic private source configuration. CLI JSON includes the exact existing SDK result wire string, task/content hashes and verified_readback. Structured quote values use the SDK codec; no alternate result serializer. Command errors exit1 with fixed safe messages. The tutorial additionally displays scalar bars values. The optional example package is verification/tutorial tooling, not a mandatory worker runtime dependency.

## Timing, admission and provenance

Before source/sink callbacks, the spec requires one instrument and target session, matching namespace/unit/adjustment/availability, request [session open, market cutoff), compatible kind/selection and full headers. Both auction-inclusion flags are rejected because the current acquisition request cannot certify the special auction populations. Bar intervals must be completed within the requested bounds; trade/quote events are half-open. No end+1 approximation. Governed intervals remain exact canonical UTCns and config constraints. No implicit prior close, reference lookup, seed, warm-up, wall-clock substitution or history readiness; ordered history is EQ059. Unknown known_at remains unknown; known_at causal C<=E/K<=E and reconstruction policies remain canonical.

Source/sink capability admission occurs before fetch. Each admitted source chunk is counted against request max_batch_rows, max_rows, max_batches and spec max_input_bytes. Bytes mean exact ASCII canonical-data JSON for delivery declarations plus supplied batch content; they are an admission accounting metric, not an RSS forecast. Encoder hashes/counts fragments without joining an entire batch wire copy and retains existing immutable column values. Cancellation is checked before/after each yielded chunk, before calculation/publication and before readback. Iterator close is called when supported. Exhaustion must follow final; extra data after final, missing final, bad ordinals and duplicate source input IDs are rejected. A final-exhaustion check may ask the trusted source for one additional item, which is never admitted.

Canonical validate_delivery validates bounded actual rows/schema/order/duplicates/request identity. Multiple chunks must also have identical scope, interval coverage, sampling, units, source/mapping/snapshot and coverage. Concatenation preserves declared global source coverage rather than replacing it with guessed completeness; trades/quotes require global observed count to match the assembled actual population. Single chunk retains original input ID; multiple chunks use worker-chunks-SHA256 over domain efworker-chunks1/NUL plus ordered original input IDs. Acquisition digest uses efworker-acquisition1/NUL plus canonical request and ordered delivery declarations. These bind declared revisions/identities, not independently authenticate row content or source truth. Revision/snapshot/input identities must change when a source changes their meaning; no cache or reuse execution is implemented here.

Trusted source callbacks can allocate internally, block or misbehave; workers cannot preempt synchronous callbacks. Retained immutable input, aggregate columns and conformance/calculation copies can coexist. Budget accounting does not imply a process-memory cap, timeout, file read cap or resource benchmark. EQ062/supervisor owns broader operational policies.

Observed bindings seal the EQ057 task only after acquisition. The planned command identity is not a durable claim. Exact actual result entities must equal the requested instrument/session for every feature, and headers must equal the admitted full inventory. OutputManifest checks actual metadata/config/input/availability identity. Existing SDK verifies content before publication; storage visibility/writer/reservation requirements are explicitly retained in publish, separate from envelope numeric limits.

Only a verified receipt matching envelope/results plus sink.read verification and exact encode_result logical parity returns success. Failed readback or cancellation after commit is not command success and does not imply commit rollback. The SDK's uncertainty/recovery rules still apply; EQ063 owns durable retry/recovery. This serial caller does not certify cross-process ownership or physical durability. Follow each chosen sink's qualified writer/storage rules; EQ061 coordinates publication. ExampleSink persists only in its caller-owned in-memory instance.

## Safe errors and validation

Fixed codes: INVALID_COMMAND, SOURCE_FAILED, RESOURCE_LIMIT, CANCELLED, CALCULATION_FAILED, RESULT_IDENTITY_MISMATCH, PUBLICATION_FAILED, READBACK_FAILED. Adapter/factory/sink exception text is discarded; callers inspect CommandError.code. No source credentials, SQL, paths or exception payload enter public error strings.

Independent development fixtures check bars500/51200/102.6, trades3/10/1011/101.1/10÷3, and normal/locked/crossed/invalid quotes mean spread1 and mean basis points10000÷101 with exact counts/evidence. Include direct/factory/CLI, all-family injected CLI, multi-chunk population/identity, missing/empty/partial/unknown/future knowledge, bounds/cancellation, bad request/inventory/entities, unsafe failures, extra final content, commit/readback failures and unsupported sink capabilities. Installed/native/form/review/actual-main evidence belongs [delivery](EQ058_DELIVERY.md); passing development checks alone is not release acceptance.

