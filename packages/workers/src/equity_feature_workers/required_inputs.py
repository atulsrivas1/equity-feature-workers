"""Independent raw-input and supplied-witness composition through public APIs."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TypeAlias

from equity_feature_contracts import (
    CanonicalBatch, ClassificationAdmission, ConfigSpec, DataKind, EntityKey, FeatureResult, InputBinding, builtin_registry,
)
from equity_feature_contracts.adapters import AcquisitionRequest, HistoricalAdapter
from equity_feature_contracts.buckets import BucketContext, BucketVolume, IntervalBaseline
from equity_feature_contracts.history import HistoryContext, SMAReference
from equity_feature_contracts.registry import InputRequirement
from equity_feature_contracts.relative import RelativeSpec, ReturnReference
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_contracts.volume import TargetVolume, VolumeBaseline
from equity_feature_io_contracts import CredentialProvider, FeatureHeader, PublicConfig, ResultSink, SinkRequirements
from equity_feature_io_sdk import Cancellation, SinkRegistry, SourceRegistry, admit_sink, admit_source, descriptor
from equity_features.buckets import compute_interval_baseline, compute_interval_relative_volume
from equity_features.history import compute_history, compute_sma_reference
from equity_features.relative import compute_relative
from equity_features.volume import compute_daily_baseline, compute_relative_volume

from .commands import (
    CommandError, CommandErrorCode, CommandOutcome, NeverCancelled, _cancel, _collect, _fail, _publish_verified, _record_hash,
)
from .manifests import InputManifest, TaskManifest, integer, label

Witness: TypeAlias = SMAReference | ReturnReference | VolumeBaseline | IntervalBaseline
Calculation: TypeAlias = FeatureResult | Witness
_RAW = {"history": (DataKind.DAILY, "daily_history"), "sma_reference": (DataKind.DAILY, "daily_history"),
        "daily_baseline": (DataKind.DAILY, "daily_history"), "interval_baseline": (DataKind.BAR, "bucket_history")}
_FAMILIES = tuple(_RAW) + ("relative_volume", "interval_relative_volume", "relative_returns")


def _result(calculation: Calculation) -> FeatureResult:
    return calculation if isinstance(calculation, FeatureResult) else calculation.result


@dataclass(frozen=True)
class RequiredCommandSpec:
    """Explicit raw acquisition or absence plus owned contexts/dependent witnesses."""
    job_id: str
    generation_id: str
    partition_id: str
    family: str
    config: ConfigSpec
    governed_sessions: tuple[IntervalSpec, ...]
    revision_id: str
    destination_scope: str
    max_input_bytes: int
    request: AcquisitionRequest | None = None
    context: HistoryContext | BucketContext | None = None
    feature_ids: tuple[str, ...] = ()
    relative_spec: RelativeSpec | None = None
    symbol: ReturnReference | None = None
    market: ReturnReference | None = None
    sector: ReturnReference | None = None
    membership: ClassificationAdmission | None = None
    target: TargetVolume | BucketVolume | None = None
    baseline: VolumeBaseline | IntervalBaseline | None = None
    ownership: str = "serialized_destination"
    _template: FeatureResult = field(init=False, repr=False, compare=False)
    dependency_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            if self.family not in _FAMILIES or type(self.config) is not ConfigSpec or type(self.feature_ids) is not tuple:
                _fail(CommandErrorCode.CONFIG)
            integer(self.max_input_bytes, 1); label(self.revision_id)
            self._admit_fields()
            # Validate exact governed bounds/config limits before hashing owned dependency records.
            template = _result(self.calculate(None))
            object.__setattr__(self, "_template", template)
            headers = descriptor(template).features
            TaskManifest(self.job_id, self.generation_id, self.partition_id, self.family, (self.entity.instrument_id,),
                         self.config, headers, (), self.governed_sessions, self.destination_scope, self.ownership,
                         self.request.max_batches if self.request is not None else 1, self.max_input_bytes)
            supplied = {name: {"type": type(value).__name__, "fields": asdict(value)} if value is not None else None
                        for name in ("context", "relative_spec", "symbol", "market", "sector", "membership", "target", "baseline")
                        if (value := getattr(self, name)) is not None}
            digest, _ = _record_hash({"family": self.family, "supplied": supplied, "config_digest": self.config.digest},
                                     b"efworker-required1\0", self.max_input_bytes)
            object.__setattr__(self, "dependency_sha256", digest)
            # Admit every already supplied auxiliary binding before adapter callbacks.
            auxiliary = tuple(InputManifest(b.role, digest, self.revision_id,
                              "partial" if not b.metadata.coverage.complete else "empty" if b.metadata.coverage.observed == 0 else "complete", b)
                              for b in template.metadata.inputs)
            self.task(auxiliary)
        except Exception:
            _fail(CommandErrorCode.CONFIG)

    def _admit_fields(self) -> None:
        family = self.family
        if family == "relative_returns":
            if self.context is not None or type(self.relative_spec) is not RelativeSpec or self.target is not None or self.baseline is not None:
                _fail(CommandErrorCode.CONFIG)
            for value in (self.symbol, self.market, self.sector):
                if value is not None and type(value) is not ReturnReference:
                    _fail(CommandErrorCode.CONFIG)
            if self.membership is not None and type(self.membership) is not ClassificationAdmission:
                _fail(CommandErrorCode.CONFIG)
            if "relative.market_return" not in self.feature_ids and self.market is not None:
                _fail(CommandErrorCode.CONFIG)
            if "relative.sector_return" not in self.feature_ids and (self.sector is not None or self.membership is not None):
                _fail(CommandErrorCode.CONFIG)
        else:
            expected = BucketContext if family in ("interval_baseline", "interval_relative_volume") else HistoryContext
            if type(self.context) is not expected or any(v is not None for v in (self.relative_spec, self.symbol, self.market, self.sector, self.membership)):
                _fail(CommandErrorCode.CONFIG)
            assert self.context is not None
            grid = tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in self.context.sessions)
            if grid != self.governed_sessions:
                _fail(CommandErrorCode.CONFIG)
            if family == "relative_volume":
                if (self.target is not None and type(self.target) is not TargetVolume) or (self.baseline is not None and type(self.baseline) is not VolumeBaseline):
                    _fail(CommandErrorCode.CONFIG)
            elif family == "interval_relative_volume":
                if (self.target is not None and type(self.target) is not BucketVolume) or (self.baseline is not None and type(self.baseline) is not IntervalBaseline):
                    _fail(CommandErrorCode.CONFIG)
            elif self.target is not None or self.baseline is not None:
                _fail(CommandErrorCode.CONFIG)
        if family not in ("history", "relative_returns") and self.feature_ids:
            _fail(CommandErrorCode.CONFIG)
        if family not in _RAW and self.request is not None:
            _fail(CommandErrorCode.CONFIG)
        if self.request is not None:
            if type(self.request) is not AcquisitionRequest:
                _fail(CommandErrorCode.CONFIG)
            r, c = self.request, self.config
            if (r.kind, r.namespace, r.instruments, r.sessions, r.price_unit, r.adjustment, r.availability) != (
                _RAW[family][0], c.session.namespace, (self.entity.instrument_id,), c.window.governed_sessions,
                c.price_unit, c.adjustment, c.availability,
            ) or r.start_ns != self.governed_sessions[0].start_ns or r.end_ns != c.availability.market_cutoff_ns:
                _fail(CommandErrorCode.CONFIG)
            assert self.context is not None
            if any(s.include_opening_auction or s.include_closing_auction for s in self.context.sessions):
                _fail(CommandErrorCode.CONFIG)
            _record_hash(asdict(r), b"", min(self.max_input_bytes, 1048576))

    @property
    def entity(self) -> EntityKey:
        if self.context is not None:
            return self.context.entity
        assert self.relative_spec is not None
        return self.relative_spec.entity

    @property
    def features(self) -> tuple[FeatureHeader, ...]:
        return descriptor(self._template).features

    def requirements(self) -> tuple[InputRequirement, ...]:
        """Return the actual public definition requirements, without a competing schema."""
        result: list[InputRequirement] = []
        for header in self.features:
            for requirement in builtin_registry().get(header.feature_id).requirements:
                if requirement not in result:
                    result.append(requirement)
        return tuple(result)

    def calculate(self, batch: CanonicalBatch | None) -> Calculation:
        family, c = self.family, self.config
        ctx = self.context
        if family in ("history", "sma_reference", "daily_baseline", "relative_volume"):
            assert isinstance(ctx, HistoryContext)
            if family == "history":
                result = compute_history(batch, c, context=ctx, feature_ids=self.feature_ids)
                return ReturnReference(result, c, ctx) if self.feature_ids == ("history.return",) else result
            if family == "sma_reference":
                return compute_sma_reference(batch, c, context=ctx)
            if family == "daily_baseline":
                return compute_daily_baseline(batch, c, context=ctx)
            assert self.target is None or isinstance(self.target, TargetVolume)
            assert self.baseline is None or isinstance(self.baseline, VolumeBaseline)
            return compute_relative_volume(self.target, self.baseline, c, context=ctx)
        if family in ("interval_baseline", "interval_relative_volume"):
            assert isinstance(ctx, BucketContext)
            if family == "interval_baseline":
                return compute_interval_baseline(batch, c, context=ctx)
            assert self.target is None or isinstance(self.target, BucketVolume)
            assert self.baseline is None or isinstance(self.baseline, IntervalBaseline)
            return compute_interval_relative_volume(self.target, self.baseline, c, context=ctx)
        assert self.relative_spec is not None
        return compute_relative(self.symbol, self.market, self.sector, self.membership, c,
                                spec=self.relative_spec, feature_ids=self.feature_ids)

    def task(self, inputs: tuple[InputManifest, ...]) -> TaskManifest:
        # Supplied return references also own ordered historical context. Bind the
        # governed prefix and dependency initialization for every composition family.
        warmup = tuple(s.name for s in self.governed_sessions[:self.config.window.governed_sessions.index(self.config.session.session_id)])
        return TaskManifest(self.job_id, self.generation_id, self.partition_id, self.family, (self.entity.instrument_id,),
                            self.config, self.features, inputs, self.governed_sessions, self.destination_scope, self.ownership,
                            self.request.max_batches if self.request is not None else 1, self.max_input_bytes,
                            ordered_history=True, warmup_sessions=warmup, initialization_sha256=self.dependency_sha256)


@dataclass(frozen=True)
class RequiredOutcome:
    command: CommandOutcome
    witness: Witness | None = None


def run_required(spec: RequiredCommandSpec, source: HistoricalAdapter | None, sink: ResultSink, *,
                 requirements: SinkRequirements, cancellation: Cancellation | None = None) -> RequiredOutcome:
    """Compute directly from raw requirements or explicit supplied dependencies."""
    if type(spec) is not RequiredCommandSpec or type(requirements) is not SinkRequirements:
        _fail(CommandErrorCode.CONFIG)
    token = cancellation if cancellation is not None else NeverCancelled()
    try:
        _cancel(token)
        admit_sink(sink, requirements)
        if spec.request is None:
            if source is not None:
                _fail(CommandErrorCode.CONFIG)
        else:
            if source is None:
                _fail(CommandErrorCode.CONFIG)
            admit_source(source, spec.request)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CONFIG)
    batch = None
    acquisition = spec.dependency_sha256
    if spec.request is not None:
        assert source is not None
        try:
            batch, acquisition = _collect(spec.request, spec.max_input_bytes, source, token)
        except CommandError:
            raise
        except Exception:
            _fail(CommandErrorCode.SOURCE)
    try:
        _cancel(token)
        calculation = spec.calculate(batch)
        result = _result(calculation)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CALCULATION)
    expected = list(spec._template.metadata.inputs)
    if batch is not None:
        expected.append(InputBinding(_RAW[spec.family][1], batch.kind, batch.metadata))
    expected.sort(key=lambda b: b.role)
    if (any(col.entities != (spec.entity,) for col in result.values) or descriptor(result).features != spec.features
        or result.metadata.inputs != tuple(expected) or result.metadata.config_digest != spec.config.digest
        or result.metadata.availability != spec.config.availability):
        _fail(CommandErrorCode.RESULT)
    inputs: list[InputManifest] = []
    try:
        for binding in expected:
            coverage = binding.metadata.coverage
            state = "partial" if not coverage.complete else "empty" if coverage.observed == 0 else "complete"
            digest = acquisition if spec.family in _RAW and binding.role == _RAW[spec.family][1] else spec.dependency_sha256
            inputs.append(InputManifest(binding.role, digest, spec.revision_id, state, binding))
        if spec.family in _RAW and batch is None:
            inputs.append(InputManifest(_RAW[spec.family][1], acquisition, spec.revision_id, "missing", None))
        absent: tuple[tuple[str, object | None], ...]
        if spec.family in ("relative_volume", "interval_relative_volume"):
            absent = (("absent.target", spec.target), ("absent.baseline", spec.baseline))
        elif spec.family == "relative_returns":
            selected: list[tuple[str, object | None]] = [("absent.symbol", spec.symbol)]
            if "relative.market_return" in spec.feature_ids:
                selected.append(("absent.market", spec.market))
            if "relative.sector_return" in spec.feature_ids:
                selected.extend((("absent.sector", spec.sector), ("absent.membership", spec.membership)))
            absent = tuple(selected)
        else:
            absent = ()
        for role, value in absent:
            if value is None:
                inputs.append(InputManifest(role, spec.dependency_sha256, spec.revision_id, "missing", None))
        task = spec.task(tuple(inputs))
    except Exception:
        _fail(CommandErrorCode.RESULT)
    command = _publish_verified(task, (result,), sink, requirements, token)
    witness = None if isinstance(calculation, FeatureResult) else calculation
    return RequiredOutcome(command, witness)


def run_required_registered(spec: RequiredCommandSpec, *, sources: SourceRegistry[HistoricalAdapter], sinks: SinkRegistry[ResultSink],
                            source_id: str | None, source_config: PublicConfig, sink_id: str, sink_config: PublicConfig,
                            credentials: CredentialProvider, requirements: SinkRequirements,
                            cancellation: Cancellation | None = None) -> RequiredOutcome:
    """Only explicitly supplied per-run registries; absence is a declared choice."""
    try:
        token = cancellation if cancellation is not None else NeverCancelled()
        _cancel(token)
        if (spec.request is None) != (source_id is None) or (source_id is None and source_config):
            _fail(CommandErrorCode.CONFIG)
        source = sources.resolve(source_id, source_config, credentials, spec.request) if source_id is not None and spec.request is not None else None
        sink = sinks.resolve(sink_id, sink_config, credentials, requirements)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CONFIG)
    return run_required(spec, source, sink, requirements=requirements, cancellation=token)
