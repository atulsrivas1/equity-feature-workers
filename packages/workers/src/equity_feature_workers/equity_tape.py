"""Closed EF4 tape window and notional top-K orchestration (ef4-tape1, ef4-notional1).

Three closed routes over explicitly supplied batches only: the tape window producer (``ef4.tape_window_sign``, nine
``tape.window.*`` and ``tape.tick.*`` outputs, role slots ``(trades, seed)``), the notional top-K producer
(``ef4.trade_top_k_notional``, role slot ``(trades,)``) and the top-K partition merge. The worker admits the supplied
batches against the reviewed configuration, binds them to the library's fixed role slots and calls the pure library
(``compute_ef4_tape``, ``merge_trade_top_k_notional``). It contains no tick-rule, window total, notional, rank or merge
mathematics and never infers, fills, sorts, rescales, imputes or defaults a role, slot, seed, partition or order. The library
owns admission, mathematics, quality, witnesses and every typed refusal; a refusal surfaces as a worker failure and
publishes nothing.

The merge route takes one batch per partition. Its manifest roles spell the merge tree: ``p<i>.trades`` is partition ``i`` of
a flat merge, ``p0.p1.trades`` is partition 1 of the inner merge that is itself partition 0 of the outer merge (the library
prefixes each level the same way, so the result binds exactly these roles). All partitions share the one reviewed
configuration of the task; a partition cannot carry its own K, policy version or cutoffs (those mismatches have no worker
path and are library-level refusals). No entity, instrument, session, role, seed or partition identifier is fixed here.
"""
from __future__ import annotations

import pickle
import re
from collections.abc import Callable, Iterable
from types import ModuleType
from typing import Any, cast

from equity_feature_contracts import CanonicalBatch, ConfigSpec, EntityKey, FeatureResult, IntervalSpec
from equity_feature_contracts.results import InputBinding
from equity_feature_io_contracts import FeatureHeader
from equity_feature_io_sdk import descriptor

from ._supplied import fail, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

WINDOW_FAMILY = 'tape_window'
TOP_K_FAMILY = 'tape_top_k'
MERGE_FAMILY = 'tape_top_k_merge'
MAX_INPUT_BYTES = 1048576
# Fixed library role slots (the library's own order).
WINDOW_ROLES = ('trades', 'seed')
TOP_K_ROLES = ('trades',)
_PRODUCERS = {
    WINDOW_FAMILY: ('ef4.tape_window_sign', 'ef4-tape-window-v1', 'TAPE_FEATURES', WINDOW_ROLES),
    TOP_K_FAMILY: ('ef4.trade_top_k_notional', 'ef4-top-k-notional-v1', 'TOP_K_FEATURES', TOP_K_ROLES),
    MERGE_FAMILY: ('ef4.trade_top_k_notional', 'ef4-top-k-notional-v1', 'TOP_K_FEATURES', TOP_K_ROLES),
}
_LEAF = re.compile(r'^(p(0|[1-9][0-9]*)\.)+trades$')


def _ef4() -> ModuleType:
    from equity_features import ef4
    return ef4


def _package_version() -> str:
    import equity_features
    return equity_features.__version__


def _selection(features: tuple[str, ...], allowed: tuple[str, ...]) -> tuple[str, ...]:
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in allowed for f in features)):
        fail()
    return tuple(sorted(features))


def _headers(config: ConfigSpec, selected: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    """Headers from the library's own discovery for the configuration (the worker owns no units or dtypes)."""
    definitions = {d.feature_id: d for d in _ef4().ef4_tape_definitions(config)}
    if set(selected) - set(definitions):
        fail()
    return tuple(FeatureHeader(f, definitions[f].algorithm_version, definitions[f].dtype, definitions[f].unit, '1')
                 for f in selected)


def _intervals(sessions: Iterable[Any]) -> tuple[IntervalSpec, ...]:
    return tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in sessions)


def _tree(roles: tuple[str, ...]) -> Any:
    """The merge tree spelled by leaf roles: a leaf is ``None``, a group is a tuple of children in index order."""
    def build(paths: list[tuple[str, ...]]) -> Any:
        if paths == [()]:
            return None
        groups: dict[int, list[tuple[str, ...]]] = {}
        for path in paths:
            if not path:
                fail(CommandErrorCode.RESULT)
            groups.setdefault(int(path[0][1:]), []).append(path[1:])
        if sorted(groups) != list(range(len(groups))):
            fail(CommandErrorCode.RESULT)
        return tuple(build(groups[i]) for i in range(len(groups)))
    if not roles or any(_LEAF.match(r) is None for r in roles) or len(set(roles)) != len(roles):
        fail(CommandErrorCode.RESULT)
    return build([tuple(r.split('.')[:-1]) for r in roles])


def _leaves(node: Any, prefix: str = '') -> list[str]:
    """Leaf roles in the library's own result order: depth first, partition index ascending."""
    if node is None:
        return [prefix + 'trades']
    return [role for i, child in enumerate(node) for role in _leaves(child, prefix + 'p%d.' % i)]


def _admit(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str) -> ModuleType:
    """Shared closed admission: family, identity, headers, role-keyed bindings and the exact target instrument."""
    with translated():
        identity, algorithm, allowed_name, roles = _PRODUCERS[family]
        library = _ef4()
        if (type(task) is not TaskManifest or type(batches) is not tuple or any(type(b) is not CanonicalBatch for b in batches)
                or task.family != family or task.ownership != 'serialized_destination' or task.math_policy_version != 'v1'
                or len(task.instruments) != 1):
            fail()
        config = task.config
        if type(config) is not ConfigSpec or (config.identity, config.algorithm_version, config.schema_version) != (
                identity, algorithm, '1'):
            fail(CommandErrorCode.RESULT)
        if task.max_input_bytes > MAX_INPUT_BYTES:
            fail(CommandErrorCode.LIMIT)
        if len(batches) > task.max_input_batches or len(pickle.dumps(batches, protocol=5)) > task.max_input_bytes:
            fail(CommandErrorCode.LIMIT)
        selected = _selection(tuple(h.feature_id for h in task.features), getattr(library, allowed_name))
        if task.features != _headers(config, selected):
            fail(CommandErrorCode.RESULT)
        bound = tuple(i for i in task.inputs if i.binding is not None)
        if family == MERGE_FAMILY:
            if not bound or any(i.binding is None for i in task.inputs):
                fail(CommandErrorCode.RESULT)
            _tree(tuple(i.role for i in bound))
        elif {i.role for i in task.inputs} - set(roles):
            fail(CommandErrorCode.RESULT)
        if (len(bound) != len(batches) or len({b.metadata.source.input_id for b in batches}) != len(batches)
                or len({i.role for i in bound}) != len(bound)):
            fail(CommandErrorCode.RESULT)
        for manifest, batch in zip(bound, batches):
            binding = cast(InputBinding, manifest.binding)
            if ((binding.role, binding.kind, binding.metadata) != (manifest.role, batch.kind, batch.metadata)
                    or batch.metadata.coverage.observed != batch.row_count
                    or batch.metadata.namespace != config.session.namespace):
                fail(CommandErrorCode.RESULT)
            column = batch.column('instrument_id')
            if column is None or any(value != task.instruments[0] for value in column.values):
                fail(CommandErrorCode.RESULT)
        if task.ordered_history is not False or task.warmup_sessions != () or task.initialization_sha256 is not None:
            fail(CommandErrorCode.RESULT)
        return library


def _by_role(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> dict[str, CanonicalBatch]:
    return {manifest.role: batch for manifest, batch in zip(
        tuple(i for i in task.inputs if i.binding is not None), batches)}


def _identity(task: TaskManifest, result: FeatureResult, expected_roles: list[str]) -> None:
    """The stored result must bind exactly the manifest inputs, the reviewed configuration and the task headers."""
    metadata = result.metadata
    bindings = {i.binding.role: i.binding for i in task.inputs if i.binding is not None}
    expected = tuple(bindings[role] for role in expected_roles)
    if (metadata.namespace, metadata.session_id, metadata.config_digest, metadata.availability,
        metadata.inputs, metadata.backend_id, metadata.backend_version, descriptor(result).features) != (
            task.config.session.namespace, task.config.session.session_id, task.config.digest,
            task.config.availability, expected, 'python-exact', _package_version(), task.features):
        fail(CommandErrorCode.RESULT)


def _compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit(task, batches, family)
        identity, algorithm, allowed_name, roles = _PRODUCERS[family]
        selected = tuple(h.feature_id for h in task.features)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        by_role = _by_role(task, batches)
        if family == MERGE_FAMILY:
            result = _merge(library, task, by_role, entity, selected)
            expected_roles = _leaves(_tree(tuple(by_role)))
        else:
            slots = tuple(by_role.get(role) for role in roles)
            result = library.compute_ef4_tape(slots, task.config, entity=entity, features=selected)
            expected_roles = sorted(by_role)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        _identity(task, result, expected_roles)
        return (result,)


def _merge(library: ModuleType, task: TaskManifest, by_role: dict[str, CanonicalBatch], entity: EntityKey,
        selected: tuple[str, ...]) -> FeatureResult:
    """Compute one partial per leaf partition with the shared configuration and merge them along the role tree."""
    def run(node: Any, prefix: str) -> FeatureResult:
        if node is None:
            batch = by_role[prefix + 'trades']
            return cast(FeatureResult, library.compute_ef4_tape((batch,), task.config, entity=entity, features=selected))
        parts = [library.NotionalPartial(task.config, entity, run(child, prefix + 'p%d.' % i))
                 for i, child in enumerate(node)]
        return cast(FeatureResult, library.merge_trade_top_k_notional(parts))
    tree = _tree(tuple(by_role))
    if tree is None:
        fail(CommandErrorCode.RESULT)
    return run(tree, '')


def compute_tape_window_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, WINDOW_FAMILY)


def compute_tape_top_k_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, TOP_K_FAMILY)


def compute_tape_top_k_merge_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, MERGE_FAMILY)


def _make(config: ConfigSpec, family: str, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple[Any, ...] | None, max_input_bytes: int,
        calculate: Callable[[TaskManifest, tuple[CanonicalBatch, ...]], tuple[FeatureResult, ...]]) -> WorkItem:
    with translated():
        identity, algorithm, allowed_name, roles = _PRODUCERS[family]
        if type(config) is not ConfigSpec:
            fail()
        library = _ef4()
        selected = _selection(features, getattr(library, allowed_name))
        # The governed interval is the configured target session unless the caller declares it explicitly.
        sessions = (config.session,) if governed is None else governed
        task = TaskManifest(job_id, generation_id, partition_id, family, (instrument_id,), config,
            _headers(config, selected), inputs, _intervals(sessions), destination_scope, 'serialized_destination',
            max(1, len(batches)), max_input_bytes, False, (), None)
        _admit(task, batches, family)
        return WorkItem(task, batches, calculate)


def make_tape_window_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple[Any, ...] | None = None, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`inputs` bind the delivered roles (`trades`, `seed`) to `batches` in the same order."""
    return _make(config, WINDOW_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_tape_window_inputs)


def make_tape_top_k_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple[Any, ...] | None = None, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`inputs` bind the delivered role (`trades`) to `batches` in the same order."""
    return _make(config, TOP_K_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_tape_top_k_inputs)


def make_tape_top_k_merge_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple[Any, ...] | None = None, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """One batch per partition; `inputs` roles spell the merge tree (`p0.trades`, `p1.trades`, `p0.p1.trades`, ...)."""
    return _make(config, MERGE_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_tape_top_k_merge_inputs)


__all__ = ['WINDOW_FAMILY', 'TOP_K_FAMILY', 'MERGE_FAMILY', 'make_tape_window_work', 'make_tape_top_k_work',
           'make_tape_top_k_merge_work', 'compute_tape_window_inputs', 'compute_tape_top_k_inputs',
           'compute_tape_top_k_merge_inputs']
