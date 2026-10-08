"""Explicit installed capability planning and consent before component creation."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum
import math
import re
from types import MappingProxyType
from typing import TypeAlias, TypeVar

from equity_feature_contracts import builtin_registry
from equity_feature_contracts.adapters import AdapterCapabilities, HistoricalAdapter, require_adapter_capability
from equity_feature_contracts.registry import InputRequirement
from equity_feature_io_contracts import ConfigValue, CredentialProvider, PublicConfig, ResultSink, SinkRequirements
from equity_feature_io_sdk import Cancellation, SinkRegistry, SourceRegistry

from .commands import CommandError, CommandErrorCode, CommandOutcome, NeverCancelled, SessionCommandSpec, _record_hash, run_session
from .required_inputs import RequiredCommandSpec, RequiredOutcome, run_required

WorkerSpec: TypeAlias = SessionCommandSpec | RequiredCommandSpec
PlannedOutcome: TypeAlias = CommandOutcome | RequiredOutcome
_T = TypeVar("_T")


class PlanningErrorCode(StrEnum):
    INVALID = "INVALID_PLAN"
    UNSUPPORTED = "UNSUPPORTED_FEATURE_OR_ROLE"
    NO_CAPABILITY = "NO_INSTALLED_CAPABILITY"
    AMBIGUOUS = "AMBIGUOUS_SOURCE"
    STALE = "STALE_PLAN_OR_CAPABILITY"
    UNAUTHORIZED = "EXECUTION_NOT_AUTHORIZED"
    APPROVAL = "APPROVAL_FAILED"
    CANCELLED = "CANCELLED"
    SOURCE = "SOURCE_FAILED"
    SINK = "SINK_FAILED"
    COMMAND = "COMMAND_FAILED"
    LIMIT = "RESOURCE_LIMIT"


class PlanningError(ValueError):
    def __init__(self, code: PlanningErrorCode) -> None:
        if type(code) is not PlanningErrorCode:
            raise TypeError("typed planning code required")
        self.code = code
        super().__init__("Acquisition planning failed: " + code.value)


def _call(operation: Callable[[], _T], fallback: PlanningErrorCode) -> _T:
    # Raise outside exception handlers: original caller/provider exceptions are not chained.
    code = fallback
    try:
        return operation()
    except PlanningError as error:
        code = error.code
    except CommandError as error:
        code = {CommandErrorCode.CANCELLED: PlanningErrorCode.CANCELLED,
                CommandErrorCode.LIMIT: PlanningErrorCode.LIMIT,
                CommandErrorCode.SOURCE: PlanningErrorCode.SOURCE,
                CommandErrorCode.SINK: PlanningErrorCode.SINK,
                CommandErrorCode.READBACK: PlanningErrorCode.SINK}.get(error.code, fallback)
    except Exception:
        pass
    raise PlanningError(code)


def _identifier(value: str) -> None:
    if type(value) is not str or re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", value) is None:
        raise PlanningError(PlanningErrorCode.INVALID)


def _config(value: PublicConfig) -> tuple[tuple[str, ConfigValue], ...]:
    if not isinstance(value, Mapping) or len(value) > 64:
        raise PlanningError(PlanningErrorCode.INVALID)
    result: list[tuple[str, ConfigValue]] = []
    for key, cell in value.items():
        if (type(key) is not str or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key) is None
                or any(part in key for part in ("password", "secret", "credential", "token", "api_key"))
                or key in ("module", "import", "class_path", "callable")
                or type(cell) not in (str, int, float, bool, type(None))
                or (type(cell) is str and len(cell) > 65536)
                or (type(cell) is float and not math.isfinite(cell))):
            raise PlanningError(PlanningErrorCode.INVALID)
        result.append((key, cell))
    if len({key for key, _ in result}) != len(result):
        raise PlanningError(PlanningErrorCode.INVALID)
    return tuple(sorted(result))


@dataclass(frozen=True, repr=False, init=False)
class SourceOffer:
    """Caller-declared metadata for an explicitly registered installed factory."""
    source_id: str
    capabilities: AdapterCapabilities
    config_items: tuple[tuple[str, ConfigValue], ...]

    def __init__(self, source_id: str, capabilities: AdapterCapabilities, config: PublicConfig) -> None:
        _identifier(source_id)
        if type(capabilities) is not AdapterCapabilities:
            raise PlanningError(PlanningErrorCode.INVALID)
        items = _call(lambda: _config(config), PlanningErrorCode.INVALID)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(self, "config_items", items)

    @property
    def config(self) -> PublicConfig:
        return MappingProxyType(dict(self.config_items))

    def __repr__(self) -> str:
        return "SourceOffer(<explicit caller metadata>)"


@dataclass(frozen=True, repr=False)
class AcquisitionPlan:
    requested_features: tuple[str, ...]
    execution_features: tuple[str, ...]
    requirements: tuple[InputRequirement, ...]
    source: SourceOffer | None
    sink_id: str
    sink_config_items: tuple[tuple[str, ConfigValue], ...]
    sink_requirements: SinkRequirements
    command_sha256: str
    plan_sha256: str

    @property
    def sink_config(self) -> PublicConfig:
        return MappingProxyType(dict(self.sink_config_items))

    def __repr__(self) -> str:
        return "AcquisitionPlan(<scoped installed selection>)"


@dataclass(frozen=True)
class ExecutionApproval:
    """Trusted caller's approval for this exact runner plan, not provider billing consent."""
    plan_sha256: str

    def __post_init__(self) -> None:
        if type(self.plan_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", self.plan_sha256) is None:
            raise PlanningError(PlanningErrorCode.INVALID)


def _identity(value: object, prefix: bytes) -> str:
    return _call(lambda: _record_hash(value, prefix, 16777216)[0], PlanningErrorCode.LIMIT)


def plan_acquisition(spec: WorkerSpec, feature_ids: tuple[str, ...], *,
                     sources: SourceRegistry[HistoricalAdapter], offers: tuple[SourceOffer, ...],
                     sinks: SinkRegistry[ResultSink], sink_id: str, sink_config: PublicConfig,
                     requirements: SinkRequirements, source_id: str | None = None) -> AcquisitionPlan:
    """Inspect explicit registry IDs/metadata only; no construction or source/sink access."""
    return _call(lambda: _plan(spec, feature_ids, sources, offers, sinks, sink_id, sink_config,
                               requirements, source_id), PlanningErrorCode.INVALID)


def _plan(spec: WorkerSpec, feature_ids: tuple[str, ...], sources: SourceRegistry[HistoricalAdapter],
          offers: tuple[SourceOffer, ...], sinks: SinkRegistry[ResultSink], sink_id: str,
          sink_config: PublicConfig, requirements: SinkRequirements, source_id: str | None) -> AcquisitionPlan:
    if (type(spec) not in (SessionCommandSpec, RequiredCommandSpec)
            or type(feature_ids) is not tuple or not 1 <= len(feature_ids) <= 128
            or any(type(f) is not str for f in feature_ids) or len(set(feature_ids)) != len(feature_ids)
            or type(offers) is not tuple or len(offers) > 128 or any(type(o) is not SourceOffer for o in offers)
            or len({o.source_id for o in offers}) != len(offers) or type(requirements) is not SinkRequirements):
        raise PlanningError(PlanningErrorCode.INVALID)
    _identifier(sink_id)
    if sink_id not in sinks.ids or any(o.source_id not in sources.ids for o in offers):
        raise PlanningError(PlanningErrorCode.NO_CAPABILITY)
    if source_id is not None:
        _identifier(source_id)
    requested = tuple(sorted(feature_ids))
    execution = tuple(header.feature_id for header in spec.features)
    if any(f not in execution for f in requested):
        raise PlanningError(PlanningErrorCode.UNSUPPORTED)
    required: list[InputRequirement] = []
    registry = builtin_registry()
    for f in requested:
        for r in registry.get(f).requirements:
            if r not in required:
                required.append(r)
    request = spec.request
    selected: SourceOffer | None = None
    if request is not None:
        if any(r.schema_id == "canonical:1" and r.kind != request.kind for r in required):
            raise PlanningError(PlanningErrorCode.UNSUPPORTED)
        candidates: list[SourceOffer] = []
        for offer in offers:
            if source_id is not None and offer.source_id != source_id:
                continue
            compatible = True
            try:
                require_adapter_capability(offer.capabilities, request)
            except Exception:
                compatible = False
            if compatible:
                candidates.append(offer)
        if not candidates:
            raise PlanningError(PlanningErrorCode.NO_CAPABILITY)
        if len(candidates) != 1:
            raise PlanningError(PlanningErrorCode.AMBIGUOUS)
        selected = candidates[0]
    elif source_id is not None:
        raise PlanningError(PlanningErrorCode.INVALID)
    # None request is the existing explicit absence/supplied-witness route, never invented data.
    items = _config(sink_config)
    command = _identity(asdict(spec), b"efworker-acquisition-command1\0")
    content = {"command": command, "requested": requested, "execution": execution,
               "requirements": [asdict(r) for r in required], "source": asdict(selected) if selected else None,
               "sink_id": sink_id, "sink_config": items, "sink_requirements": asdict(requirements)}
    digest = _identity(content, b"efworker-acquisition-plan1\0")
    return AcquisitionPlan(requested, execution, tuple(required), selected, sink_id, items, requirements, command, digest)


def execute_plan(spec: WorkerSpec, plan: AcquisitionPlan, *, sources: SourceRegistry[HistoricalAdapter],
                 sinks: SinkRegistry[ResultSink], credentials: CredentialProvider,
                 authorize: Callable[[AcquisitionPlan], ExecutionApproval | None] | None = None,
                 cancellation: Cancellation | None = None) -> PlannedOutcome:
    """Default-denied exact consent; selected factories only; delegate accepted worker I/O."""
    token = cancellation if cancellation is not None else NeverCancelled()

    def cancelled() -> None:
        if _call(token.is_cancelled, PlanningErrorCode.CANCELLED):
            raise PlanningError(PlanningErrorCode.CANCELLED)

    def recheck() -> None:
        if type(plan) is not AcquisitionPlan:
            raise PlanningError(PlanningErrorCode.INVALID)
        renewed = plan_acquisition(spec, plan.requested_features, sources=sources,
            offers=(plan.source,) if plan.source is not None else (), sinks=sinks, sink_id=plan.sink_id,
            sink_config=plan.sink_config, requirements=plan.sink_requirements,
            source_id=plan.source.source_id if plan.source is not None else None)
        if renewed != plan:
            raise PlanningError(PlanningErrorCode.STALE)

    cancelled()
    _call(recheck, PlanningErrorCode.STALE)
    approval = _call(lambda: authorize(plan), PlanningErrorCode.APPROVAL) if authorize is not None else None
    if type(approval) is not ExecutionApproval or approval.plan_sha256 != plan.plan_sha256:
        raise PlanningError(PlanningErrorCode.UNAUTHORIZED)
    cancelled()
    _call(recheck, PlanningErrorCode.STALE)
    source: HistoricalAdapter | None = None
    if plan.source is not None:
        offer = plan.source
        request = spec.request
        if request is None:
            raise PlanningError(PlanningErrorCode.STALE)
        source = _call(lambda: sources.resolve(offer.source_id, offer.config, credentials, request), PlanningErrorCode.SOURCE)
        actual = _call(source.capabilities, PlanningErrorCode.SOURCE)
        if actual != offer.capabilities:
            raise PlanningError(PlanningErrorCode.STALE)
    cancelled()
    sink = _call(lambda: sinks.resolve(plan.sink_id, plan.sink_config, credentials, plan.sink_requirements), PlanningErrorCode.SINK)
    cancelled()
    if type(spec) is SessionCommandSpec:
        if source is None:
            raise PlanningError(PlanningErrorCode.STALE)
        return _call(lambda: run_session(spec, source, sink, requirements=plan.sink_requirements,
                                        cancellation=token), PlanningErrorCode.COMMAND)
    if not isinstance(spec, RequiredCommandSpec):
        raise PlanningError(PlanningErrorCode.INVALID)
    return _call(lambda: run_required(spec, source, sink, requirements=plan.sink_requirements,
                                     cancellation=token), PlanningErrorCode.COMMAND)
