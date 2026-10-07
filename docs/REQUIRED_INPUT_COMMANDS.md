# Required-input commands (experimental 0.1.0a4)

[EQ059 #67](https://github.com/atulsrivas1/equity-features/issues/67), [pre-code plan](EQ059_PLAN.md), [delivery evidence](EQ059_DELIVERY.md). Retains [session commands](COMMANDS.md) and [task protocol](MANIFESTS.md). Core contracts/features0.0.4a4, IOcontracts/SDK0.1.0a2, schema1 and mathematical algorithmsv1 are unchanged.

`RequiredCommandSpec` binds job/generation/partition, family, canonical config, exact governed interval grid, revision, destination, positive logical input-byte budget and serialized/independent destination ownership. `run_required(spec, source_or_none, sink, requirements=...)` returns `RequiredOutcome(command, witness)`. `command` is the existing verified `CommandOutcome`; its result/envelope/receipt/readback use the SDK codec and publication protocol. `requirements()` returns the selected public feature definitions' `InputRequirement` records. Closed family dispatch invokes these public functions:

| Family | Explicit inputs | Public computation | Owned witness returned |
|---|---|---|---|
| history | DAILY request/source or explicit absence, HistoryContext, feature IDs | compute_history | ReturnReference for exactly history.return; otherwise None |
| sma_reference | DAILY request/source or explicit absence, HistoryContext | compute_sma_reference | SMAReference |
| daily_baseline | DAILY request/source or explicit absence, HistoryContext | compute_daily_baseline | VolumeBaseline |
| interval_baseline | whole-bucket BAR request/source or explicit absence, BucketContext | compute_interval_baseline | IntervalBaseline |
| relative_volume | HistoryContext, supplied TargetVolume/VolumeBaseline or None | compute_relative_volume | None |
| interval_relative_volume | BucketContext, supplied BucketVolume/IntervalBaseline or None | compute_interval_relative_volume | None |
| relative_returns | RelativeSpec, selected IDs, supplied symbol/market/sector ReturnReference and ClassificationAdmission or None | compute_relative | None |

Every request is bounded, one instrument, exact namespace/unit/adjustment/availability/governed sessions and first governed open through market cutoff. Daily and bucket commands fetch their declared raw inputs directly: no previous derived worker job is required. Source instance is mandatory when a request is supplied. Request=None requires source=None and is a deliberate missing input, not factory fallback. The existing SDK validates source capability and each finite delivery. Historical envelopes preserve missing, observed empty, partial and complete states; rows are not sorted, filled or aggregated. A partial overall certificate can coexist with an available selected window when caller slot certificates/actual rows qualify it under the public mathematics.

Caller owns the canonical HistoryContext/BucketContext, all session bounds, one per-slot certificate, action admission and recursive anchor. Grid and target/config bounds must agree exactly. EMA/RSI/ATR need their accepted initialization and warm-up; ATR needs the supplied predecessor for its initial true range. Existing core functions admit unknown/future knowledge, missing slots, order, whole-bucket/early-close bounds, action/unit/adjustment and PIT policy. The worker does not manufacture references, calendar sessions, completeness or adjustments. Requests with auction inclusion are rejected before fetch because this acquisition protocol cannot certify that selection. Pure supplied-dependency families do not fetch sources. Incompatible witnesses are rejected before source/sink callbacks.

Daily/interval ratios require the baseline's exact config and context (including config ID), not merely equivalent parameters. Optional market/sector/membership absence produces canonical null/status/reason values for the selected feature inventory. Supplying unselected benchmark dependencies is rejected. Only selected missing roles enter the task; optional unrelated families do not block the command. Every task binds the ordered governed prefix and supplied dependency/initialization digest, raw acquisition identity, all public context/witness bindings, revision/config/cutoffs and explicit missing roles. Acquisition/dependency digests identify declarations; they are not provider row authentication.

Actual result entities, complete headers, config/availability and expected raw/context/dependent bindings are checked before publication. The existing SDK envelope/receipt content is checked and the sink's logical result is read back exactly before successful return. Safe error codes retain the session-command vocabulary: INVALID_COMMAND, SOURCE_FAILED, RESOURCE_LIMIT, CANCELLED, CALCULATION_FAILED, RESULT_IDENTITY_MISMATCH, PUBLICATION_FAILED, READBACK_FAILED; exceptions discard backend details. No task is declared complete from file presence. Logical publication/readback does not supply durable claims, retry/resume, crash recovery or independent physical storage certification.

Owned exact witnesses are returned by the Python API alongside their published FeatureResult. Never reconstruct a SMA/baseline's exact numerator/denominator or a return proof from the rounded scalar. Witnesses stay caller-owned/in-memory; persisted results alone cannot rebuild them. Resume needs qualified-source replay through the same public functions or separately supplied owned proofs. No competing witness codec/storage protocol is introduced.

`run_required_registered` uses caller-created SDK SourceRegistry/SinkRegistry, explicit source/sink IDs and public scalar config, with credentials separate. Source ID=None requires request=None and empty source config. Unknown/failing factories do not become absence. No dynamic module loading or built-in provider lookup.

The installed `equity-feature-worker --required FAMILY` accepts a caller wrapper injecting the spec, source/absence, sink and requirements via `equity_feature_workers.cli.main`. All seven families are selectable; no default adapter is inferred. The output uses the existing SDK result wire with `verified_readback`, task/content digests and an `owned_witness_type` diagnostic. CLI output is not a witness persistence/reconstruction format. Bare invocation exits2 for missing injected components.

The checked-in synthetic [fixtures](../tests/required_fixture.py) and independently frozen [literal oracle](../tests/required_oracle.json) exercise the API, including exact SMA230/2, prior baseline300/2, ratios4 and market return difference0.1. [Installed tests](../tests/test_required_inputs.py) cover direct/registered/CLI paths for every family. Example construction (the fixture import is test/tutorial code, not a runtime dependency):

```python
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_workers import RequiredCommandSpec, run_required
from required_fixture import SESSIONS, config, context, request, daily, LiteralSource
from equity_feature_example_extensions import ExampleSink
from equity_feature_example_extensions.sink import LIMITS

spec = RequiredCommandSpec(
    "job", "generation", "A-S3", "sma_reference", config(),
    tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in SESSIONS),
    "revision1", "synthetic-conformance", 65536,
    request=request(), context=context(),
)
outcome = run_required(spec, LiteralSource(daily(), spec.request), ExampleSink(), requirements=LIMITS)
assert (outcome.witness.numerator, outcome.witness.denominator) == (230, 2)
```

CPython3.12 x64 Windows/Linux qualification only. Budgets bound admitted request/dependency/acquisition JSON declarations, rows and batches, not hard process RSS. Hashing supplied canonical owned records may copy those records; caller must bound their allocation. Trusted callbacks can allocate or block before yielding; cancellation checks are cooperative, not preemption/timeouts. Serialization ownership is a declared requirement, not a lock manager. No performance/private-source/month/annual-generation readiness follows from synthetic checks; EQ060–066 carry barriers/publication/supervision/recovery/catalog/observability/pilot gates.
