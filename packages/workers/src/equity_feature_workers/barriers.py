"""Declared dependencies admitted from actual SDK lookup and logical readback."""
from __future__ import annotations

from dataclasses import dataclass, field

from equity_feature_contracts import FeatureResult
from equity_feature_contracts.composition import CompositionSpec, FamilyResult, FeatureBundle
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_contracts import PublicationState, PublicationStatus, ResultSink, SinkRequirements
from equity_feature_io_sdk import Cancellation, admit_sink, descriptor, encode_result, idempotency_key, verify_receipt
from equity_features.composition import compose_features

from .commands import CommandError, CommandErrorCode, CommandOutcome, NeverCancelled, _cancel, _fail, _record_hash
from .manifests import ManifestError, ManifestErrorCode, OutputManifest, TaskManifest, encode_output, encode_task, integer, label, sequence


@dataclass(frozen=True)
class BarrierLimits:
    max_dependencies: int = 64
    max_bytes: int = 8388608

    def __post_init__(self) -> None:
        try:
            integer(self.max_dependencies, 1); integer(self.max_bytes, 1)
            if self.max_dependencies > 4096:
                _fail(CommandErrorCode.CONFIG)
        except Exception:
            _fail(CommandErrorCode.CONFIG)


@dataclass(frozen=True)
class Dependency:
    instance_id: str
    task: TaskManifest
    output: OutputManifest | None = None
    sink: ResultSink | None = field(default=None, repr=False, compare=False)
    required: bool = True
    optional_policy: str = "wait"

    def __post_init__(self) -> None:
        try:
            label(self.instance_id)
            if (type(self.task) is not TaskManifest or type(self.required) is not bool
                or type(self.optional_policy) is not str
                or self.optional_policy not in ("wait", "allow_absent")
                or (self.required and self.optional_policy != "wait")):
                _fail(CommandErrorCode.CONFIG)
            if self.output is None:
                if self.sink is not None:
                    _fail(CommandErrorCode.CONFIG)
            elif type(self.output) is not OutputManifest or self.sink is None or self.output.task != self.task:
                _fail(CommandErrorCode.CONFIG)
        except Exception:
            _fail(CommandErrorCode.CONFIG)


@dataclass(frozen=True)
class WaitingDependency:
    instance_id: str
    task_sha256: str
    reason: str


@dataclass(frozen=True)
class VerifiedDependency:
    instance_id: str
    command: CommandOutcome


@dataclass(frozen=True)
class BarrierOutcome:
    dependencies: tuple[Dependency, ...]
    verified: tuple[VerifiedDependency, ...]
    waiting: tuple[WaitingDependency, ...]
    omitted: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.waiting


def _wire_size(value: TaskManifest | OutputManifest) -> int:
    try:
        return len(encode_task(value) if isinstance(value, TaskManifest) else encode_output(value))
    except ManifestError as error:
        _fail(CommandErrorCode.LIMIT if error.code is ManifestErrorCode.LIMIT else CommandErrorCode.CONFIG)


def _admit_dependencies(dependencies: tuple[Dependency, ...], limits: BarrierLimits,
                        requirements: SinkRequirements) -> int:
    if type(dependencies) is not tuple or type(limits) is not BarrierLimits or type(requirements) is not SinkRequirements:
        _fail(CommandErrorCode.CONFIG)
    if len(dependencies) > limits.max_dependencies:
        _fail(CommandErrorCode.LIMIT)
    if any(type(d) is not Dependency for d in dependencies):
        _fail(CommandErrorCode.CONFIG)
    if (len({d.instance_id for d in dependencies}) != len(dependencies)
        or len({d.task.task_sha256 for d in dependencies}) != len(dependencies)):
        _fail(CommandErrorCode.CONFIG)
    size = 0
    for d in dependencies:
        size += _wire_size(d.task if d.output is None else d.output)
        size += _record_hash({"instance_id": d.instance_id, "required": d.required, "optional_policy": d.optional_policy},
                             b"", limits.max_bytes)[1]
        if d.output is not None:
            e = d.output.envelope
            if (e.result_count > requirements.max_results or e.cell_count > requirements.max_result_cells
                or e.evidence_count > requirements.max_evidence_rows or e.content_bytes > requirements.max_total_bytes):
                _fail(CommandErrorCode.LIMIT)
            size += e.content_bytes
        if size > limits.max_bytes:
            _fail(CommandErrorCode.LIMIT)
    return size


def _admit_entities(task: TaskManifest, results: tuple[FeatureResult, ...]) -> None:
    expected = {(i, task.config.session.session_id, h.feature_id) for i in task.instruments for h in task.features}
    actual = [(e.instrument_id, e.session_id, col.feature_id) for result in results for col in result.values for e in col.entities]
    if len(set(actual)) != len(actual) or set(actual) != expected:
        _fail(CommandErrorCode.RESULT)


def _read_dependency(d: Dependency, requirements: SinkRequirements, token: Cancellation) -> VerifiedDependency | str:
    if d.output is None:
        return "EXPLICIT_ABSENCE"
    assert d.sink is not None
    try:
        admit_sink(d.sink, requirements)
        _cancel(token)
        status = d.sink.lookup(idempotency_key(d.output.envelope.identity))
        if type(status) is not PublicationStatus:
            _fail(CommandErrorCode.READBACK)
        _cancel(token)
        if status.state is not PublicationState.COMMITTED:
            return status.state.value
        assert status.receipt is not None
        if d.output.receipt is not None and d.output.receipt != status.receipt:
            _fail(CommandErrorCode.READBACK)
        verify_receipt(status.receipt, d.output.envelope)
        results = d.sink.read(status.receipt)
        _cancel(token)
        verify_receipt(status.receipt, d.output.envelope, results)
        if any(len(encode_result(r)) > requirements.max_chunk_bytes for r in results):
            _fail(CommandErrorCode.LIMIT)
        _admit_entities(d.task, results)
        output = OutputManifest(d.task, d.output.envelope, status.receipt)
        return VerifiedDependency(d.instance_id, CommandOutcome(output, results))
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.READBACK)


def inspect_barrier(dependencies: tuple[Dependency, ...], *, limits: BarrierLimits,
                    requirements: SinkRequirements, cancellation: Cancellation | None = None) -> BarrierOutcome:
    """No staged/structural receipt establishes completion without live readback."""
    _admit_dependencies(dependencies, limits, requirements)
    token = cancellation if cancellation is not None else NeverCancelled()
    verified: list[VerifiedDependency] = []
    waiting: list[WaitingDependency] = []
    omitted: list[str] = []
    for d in dependencies:
        _cancel(token)
        try:
            observed = _read_dependency(d, requirements, token)
        except CommandError as error:
            if error.code is CommandErrorCode.CANCELLED:
                raise
            # A fault remains a scoped waiting/fault reason; unrelated ready work continues.
            observed = error.code.value
        if isinstance(observed, VerifiedDependency):
            verified.append(observed)
            continue
        reason = observed
        if not d.required and d.optional_policy == "allow_absent" and reason in ("EXPLICIT_ABSENCE", "ABSENT"):
            omitted.append(d.instance_id)
        else:
            waiting.append(WaitingDependency(d.instance_id, d.task.task_sha256, reason))
    return BarrierOutcome(dependencies, tuple(verified), tuple(waiting), tuple(omitted))


@dataclass(frozen=True)
class TaskNode:
    task: TaskManifest
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    optional_policy: str = "wait"

    def __post_init__(self) -> None:
        try:
            if (type(self.task) is not TaskManifest or type(self.required) is not tuple or type(self.optional) is not tuple
                or type(self.optional_policy) is not str
                or self.optional_policy not in ("wait", "allow_absent")):
                _fail(CommandErrorCode.CONFIG)
            sequence(self.required, str); sequence(self.optional, str)
            if len(self.required) + len(self.optional) > 4096:
                _fail(CommandErrorCode.CONFIG)
            for key in self.required + self.optional:
                label(key)
            if len(set(self.required + self.optional)) != len(self.required + self.optional):
                _fail(CommandErrorCode.CONFIG)
        except Exception:
            _fail(CommandErrorCode.CONFIG)


@dataclass(frozen=True)
class ReadinessOutcome:
    barrier: BarrierOutcome
    ready_tasks: tuple[TaskManifest, ...]


def evaluate_readiness(nodes: tuple[TaskNode, ...], dependencies: tuple[Dependency, ...], *,
                       limits: BarrierLimits, requirements: SinkRequirements,
                       cancellation: Cancellation | None = None) -> ReadinessOutcome:
    """A waiting dependency blocks only its declared consumers, not unrelated roots."""
    dependency_bytes = _admit_dependencies(dependencies, limits, requirements)
    if type(nodes) is not tuple or any(type(n) is not TaskNode for n in nodes):
        _fail(CommandErrorCode.CONFIG)
    if len(nodes) + len(dependencies) > limits.max_dependencies:
        _fail(CommandErrorCode.LIMIT)
    by_id = {n.task.task_sha256: n for n in nodes}
    if len(by_id) != len(nodes):
        _fail(CommandErrorCode.CONFIG)
    known = set(by_id) | {d.task.task_sha256 for d in dependencies}
    if any(k not in known for n in nodes for k in n.required + n.optional):
        _fail(CommandErrorCode.CONFIG)
    node_bytes = sum(_wire_size(n.task) + _record_hash(
        {"required": n.required, "optional": n.optional, "optional_policy": n.optional_policy}, b"", limits.max_bytes)[1]
        for n in nodes)
    if dependency_bytes + node_bytes > limits.max_bytes:
        _fail(CommandErrorCode.LIMIT)
    # Iterative topological admission avoids recursion depth becoming an unbounded graph failure.
    remaining = set(by_id)
    while remaining:
        roots = {k for k in remaining if not (set(by_id[k].required + by_id[k].optional) & remaining)}
        if not roots:
            _fail(CommandErrorCode.CONFIG)
        remaining -= roots
    barrier = inspect_barrier(dependencies, limits=limits, requirements=requirements, cancellation=cancellation)
    committed = {v.command.output.task.task_sha256 for v in barrier.verified}
    omitted = {d.task.task_sha256 for d in dependencies if d.instance_id in barrier.omitted}
    ready = tuple(n.task for n in nodes if n.task.task_sha256 not in committed and set(n.required) <= committed
                  and set(n.optional) <= (committed | omitted if n.optional_policy == "allow_absent" else committed))
    return ReadinessOutcome(barrier, ready)


def admit_family(component: FamilyResult, task: TaskManifest) -> None:
    """Owned proofs must match full declared context bounds as well as values."""
    if (type(component) is not FamilyResult or component.config != task.config
        or descriptor(component.result).features != task.features):
        _fail(CommandErrorCode.RESULT)
    _admit_entities(task, (component.result,))
    ctx = component.owned_context
    if ctx is not None and tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in ctx.sessions) != task.governed_sessions:
        _fail(CommandErrorCode.RESULT)
    expected_inputs = tuple(sorted((i.binding for i in task.inputs if i.binding is not None), key=lambda b: b.role))
    if component.result.metadata.inputs != expected_inputs:
        _fail(CommandErrorCode.RESULT)


@dataclass(frozen=True)
class AssemblyOutcome:
    barrier: BarrierOutcome
    bundle: FeatureBundle | None


def run_assembly(spec: CompositionSpec, dependencies: tuple[Dependency, ...], components: tuple[FamilyResult, ...], *,
                 limits: BarrierLimits, requirements: SinkRequirements,
                 cancellation: Cancellation | None = None) -> AssemblyOutcome:
    _admit_dependencies(dependencies, limits, requirements)
    if (type(spec) is not CompositionSpec or type(components) is not tuple
        or any(type(c) is not FamilyResult for c in components)
        or tuple(d.instance_id for d in dependencies) != spec.instance_ids):
        _fail(CommandErrorCode.CONFIG)
    by_id = {d.instance_id: d for d in dependencies}
    if len({c.instance_id for c in components}) != len(components) or any(c.instance_id not in by_id for c in components):
        _fail(CommandErrorCode.CONFIG)
    for component in components:
        admit_family(component, by_id[component.instance_id].task)
    token = cancellation if cancellation is not None else NeverCancelled()
    barrier = inspect_barrier(dependencies, limits=limits, requirements=requirements, cancellation=token)
    if not barrier.ready:
        return AssemblyOutcome(barrier, None)
    actual = {v.instance_id: v.command.results for v in barrier.verified}
    if {c.instance_id for c in components} != set(actual):
        _fail(CommandErrorCode.RESULT)
    for component in components:
        if tuple(map(encode_result, actual[component.instance_id])) != (encode_result(component.result),):
            _fail(CommandErrorCode.RESULT)
    try:
        _cancel(token)
        bundle = compose_features(components, spec=spec)
        return AssemblyOutcome(barrier, bundle)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CALCULATION)
