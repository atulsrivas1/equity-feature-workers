"""Complete declared universe/shard receipts before pure supplied-proof breadth."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from equity_feature_contracts import ConfigSpec, FeatureResult
from equity_feature_contracts.breadth import BreadthResult, BreadthSpec, DeclaredUniverseSpec, MemberFeatures
from equity_feature_contracts.composition import FamilyResult
from equity_feature_contracts.history import SMAReference
from equity_feature_contracts.relative import ReturnReference
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_contracts import ResultSink, SinkRequirements
from equity_feature_io_sdk import Cancellation, admit_sink, descriptor, encode_result
from equity_features.breadth import compute_above_sma_breadth, compute_direction_breadth
from equity_features import __version__ as CORE_VERSION

from .barriers import (BarrierLimits, BarrierOutcome, Dependency, _admit_dependencies, admit_family, inspect_barrier)
from .commands import (CommandError, CommandErrorCode, CommandOutcome, NeverCancelled, _cancel, _fail,
                       _publish_verified, _record_hash)
from .manifests import InputManifest, TaskManifest, integer, label, sequence


@dataclass(frozen=True)
class UniverseShard:
    shard_id: str
    member_ids: tuple[str, ...]
    dependency_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        try:
            label(self.shard_id)
            sequence(self.member_ids, str, nonempty=True); sequence(self.dependency_ids, str, nonempty=True)
            for value in self.member_ids + self.dependency_ids:
                label(value)
            if len(set(self.member_ids)) != len(self.member_ids) or len(set(self.dependency_ids)) != len(self.dependency_ids):
                _fail(CommandErrorCode.CONFIG)
        except Exception:
            _fail(CommandErrorCode.CONFIG)


@dataclass(frozen=True)
class BreadthCommandSpec:
    job_id: str
    generation_id: str
    partition_id: str
    family: str
    config: ConfigSpec
    governed_sessions: tuple[IntervalSpec, ...]
    revision_id: str
    destination_scope: str
    max_input_bytes: int
    universe: DeclaredUniverseSpec
    aggregate: BreadthSpec
    shards: tuple[UniverseShard, ...]
    ownership: str = "serialized_destination"
    _template: FeatureResult = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            if (self.family not in ("direction_counts", "above_sma_fraction") or type(self.config) is not ConfigSpec
                or type(self.universe) is not DeclaredUniverseSpec or type(self.aggregate) is not BreadthSpec):
                _fail(CommandErrorCode.CONFIG)
            label(self.revision_id); integer(self.max_input_bytes, 1)
            sequence(self.shards, UniverseShard)
            for member in self.universe.members:
                label(member)
            if len({s.shard_id for s in self.shards}) != len(self.shards):
                _fail(CommandErrorCode.CONFIG)
            members = tuple(i for s in self.shards for i in s.member_ids)
            dependencies = tuple(i for s in self.shards for i in s.dependency_ids)
            if (len(set(members)) != len(members) or set(members) != set(self.universe.members)
                or len(set(dependencies)) != len(dependencies)):
                _fail(CommandErrorCode.CONFIG)
            object.__setattr__(self, "_template", self.calculate(None).result)
            digest, _ = _record_hash({"universe": asdict(self.universe), "shards": [asdict(s) for s in self.shards]},
                                     b"efworker-universe1\0", self.max_input_bytes)
            self.task((), digest)
        except Exception:
            _fail(CommandErrorCode.CONFIG)

    def calculate(self, members: tuple[MemberFeatures, ...] | None) -> BreadthResult:
        fn = compute_direction_breadth if self.family == "direction_counts" else compute_above_sma_breadth
        return fn(members, self.config, universe=self.universe, spec=self.aggregate)

    def task(self, inputs: tuple[InputManifest, ...], initialization_sha256: str) -> TaskManifest:
        prefix = self.config.window.governed_sessions.index(self.config.session.session_id)
        return TaskManifest(self.job_id, self.generation_id, self.partition_id, self.family,
                            (self.aggregate.entity.instrument_id,), self.config, descriptor(self._template).features,
                            inputs, self.governed_sessions, self.destination_scope, self.ownership, 1, self.max_input_bytes,
                            ordered_history=True, warmup_sessions=tuple(s.name for s in self.governed_sessions[:prefix]),
                            initialization_sha256=initialization_sha256)


@dataclass(frozen=True)
class BreadthOutcome:
    barrier: BarrierOutcome
    command: CommandOutcome | None
    breadth: BreadthResult | None


def _members(spec: BreadthCommandSpec, dependencies: tuple[Dependency, ...], members: tuple[MemberFeatures, ...]
             ) -> tuple[tuple[MemberFeatures, ...], dict[str, tuple[Dependency, ...]]]:
    if type(members) is not tuple or any(type(m) is not MemberFeatures for m in members):
        _fail(CommandErrorCode.CONFIG)
    if (len({m.entity.instrument_id for m in members}) != len(members)
        or set(m.entity.instrument_id for m in members) != set(spec.universe.members)):
        _fail(CommandErrorCode.CONFIG)
    by_id = {d.instance_id: d for d in dependencies}
    declared = {i for shard in spec.shards for i in shard.dependency_ids}
    if set(by_id) != declared or any(not d.required for d in dependencies):
        _fail(CommandErrorCode.CONFIG)
    per_member: dict[str, tuple[Dependency, ...]] = {}
    for shard in spec.shards:
        covered: set[str] = set()
        for key in shard.dependency_ids:
            task = by_id[key].task
            if (not set(task.instruments) <= set(shard.member_ids) or task.config.session != spec.config.session
                or task.config.availability != spec.config.availability or task.governed_sessions != spec.governed_sessions):
                _fail(CommandErrorCode.CONFIG)
            covered.update(task.instruments)
        if covered != set(shard.member_ids):
            _fail(CommandErrorCode.CONFIG)
        for name in shard.member_ids:
            per_member[name] = tuple(by_id[k] for k in shard.dependency_ids if name in by_id[k].task.instruments)
    selected: list[MemberFeatures] = []
    for member in members:
        if member.entity.session_id != spec.config.session.session_id:
            _fail(CommandErrorCode.CONFIG)
        if spec.family == "direction_counts":
            if member.return_reference is None:
                _fail(CommandErrorCode.CONFIG)
            selected.append(MemberFeatures(member.entity, return_reference=member.return_reference))
        else:
            if member.sma is None:
                _fail(CommandErrorCode.CONFIG)
            selected.append(MemberFeatures(member.entity, sma=member.sma, close=member.close))
    return tuple(selected), per_member


def _proofs(spec: BreadthCommandSpec, members: tuple[MemberFeatures, ...], per_member: dict[str, tuple[Dependency, ...]]) -> dict[str, str]:
    matched: dict[str, str] = {}
    selected_grid: object = None
    for member in members:
        deps = per_member[member.entity.instrument_id]
        companion: ReturnReference | SMAReference
        if spec.family == "direction_counts":
            ref = member.return_reference
            assert ref is not None
            result, config, context, companion = ref.result, ref.config, ref.context, ref
        else:
            sma = member.sma
            assert sma is not None
            result, config, context, companion = sma.reference.result, sma.config, sma.context, sma.reference
        parent = spec.config
        if (config.price_unit != parent.price_unit or config.window.selected_sessions() != parent.window.selected_sessions()
            or config.window.anchor != parent.window.anchor
            or dict((p.name, p.value) for p in config.parameters).get("period") != dict((p.name, p.value) for p in parent.parameters).get("period")
            or (config.adjustment.basis, config.adjustment.policy_version, config.adjustment.anchor)
            != (parent.adjustment.basis, parent.adjustment.policy_version, parent.adjustment.anchor)
            or result.metadata.backend_version != CORE_VERSION):
            _fail(CommandErrorCode.RESULT)
        grid = context.grid_version, tuple(s for s in context.sessions if s.session_id in parent.window.selected_sessions())
        if selected_grid is not None and grid != selected_grid:
            _fail(CommandErrorCode.RESULT)
        selected_grid = grid
        candidates = [d for d in deps if d.task.config == config and d.task.features == descriptor(result).features]
        if len(candidates) != 1:
            _fail(CommandErrorCode.RESULT)
        dep = candidates[0]
        try:
            admit_family(FamilyResult(dep.instance_id, result, config, context, companion), dep.task)
        except CommandError:
            raise
        except Exception:
            _fail(CommandErrorCode.RESULT)
        matched[member.entity.instrument_id] = dep.instance_id
        if member.close is not None and not any(
            i.binding is not None and (i.binding.kind, i.binding.metadata) == (member.close.source.kind, member.close.source.metadata)
            for d in deps for i in d.task.inputs
        ):
            _fail(CommandErrorCode.RESULT)
    return matched


def run_breadth(spec: BreadthCommandSpec, dependencies: tuple[Dependency, ...], members: tuple[MemberFeatures, ...],
                sink: ResultSink, *, limits: BarrierLimits, requirements: SinkRequirements,
                cancellation: Cancellation | None = None) -> BreadthOutcome:
    if type(spec) is not BreadthCommandSpec:
        _fail(CommandErrorCode.CONFIG)
    _admit_dependencies(dependencies, limits, requirements)
    selected, per_member = _members(spec, dependencies, members)
    matches = _proofs(spec, selected, per_member)
    # Digest only consumed owned proofs; never serialize sinks or unrequested member fields.
    digest, _ = _record_hash({"universe": asdict(spec.universe), "shards": [asdict(s) for s in spec.shards],
                             "members": [asdict(m) for m in selected], "tasks": [d.task.task_sha256 for d in dependencies]},
                            b"efworker-breadth1\0", spec.max_input_bytes)
    token = cancellation if cancellation is not None else NeverCancelled()
    try:
        _cancel(token); admit_sink(sink, requirements)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CONFIG)
    barrier = inspect_barrier(dependencies, limits=limits, requirements=requirements, cancellation=token)
    if not barrier.ready:
        return BreadthOutcome(barrier, None, None)
    actual = {v.instance_id: v.command.results for v in barrier.verified}
    for member in selected:
        result = member.return_reference.result if member.return_reference is not None else member.sma.reference.result if member.sma is not None else None
        assert result is not None
        if tuple(map(encode_result, actual[matches[member.entity.instrument_id]])) != (encode_result(result),):
            _fail(CommandErrorCode.RESULT)
    try:
        _cancel(token)
        breadth = spec.calculate(selected)
        receipt_digest, _ = _record_hash({"proofs": digest, "committed": [
            {"task": v.command.output.task.task_sha256, "content": v.command.output.receipt.content_sha256,
             "key": v.command.output.receipt.idempotency_key}
            for v in barrier.verified if v.command.output.receipt is not None]}, b"efworker-breadth-receipts1\0", spec.max_input_bytes)
        inputs = tuple(InputManifest(b.role, receipt_digest, spec.revision_id,
                       "partial" if not b.metadata.coverage.complete else "empty" if b.metadata.coverage.observed == 0 else "complete", b)
                       for b in breadth.result.metadata.inputs)
        task = spec.task(inputs, receipt_digest)
        command = _publish_verified(task, (breadth.result,), sink, requirements, token)
        return BreadthOutcome(barrier, command, breadth)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CALCULATION)
