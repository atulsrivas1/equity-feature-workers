"""Closed EF3 breadth and adaptive-volume orchestration (ef3-breadth1, ef3-obsbreadth1, ef3-adaptive1).

Three producers over explicitly supplied REFERENCE batches only: session breadth (``ef3.session_breadth``), observed
breadth (``ef3.observed_breadth``) and adaptive volume (``ef3.adaptive_volume``). The worker admits the supplied batches
against the reviewed configuration and passes the nullable role slots to the pure library
(``equity_features.ef3.compute_ef3_breadth``). It contains no count, ratio, proximity, rank, rate, baseline or
acceleration mathematics and never infers, fills, sorts, rescales or defaults a role, slot, member, floor or epoch.

Admission is role-keyed: every manifest role is present, every bound batch matches its manifest binding, namespace and
declared coverage, and the target entity is the single governed session. Member instruments of the breadth producers are
not compared with the entity (the members are the population); adaptive observations must carry the entity instrument.
The library owns admission, mathematics, quality, witnesses and every typed refusal. A refusal raised while the work item
is built surfaces from the builder; a refusal raised while the result is calculated surfaces as a worker failure and
publishes nothing. Both are mapped by ``translated()``.
"""
from __future__ import annotations

import pickle
from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, EntityKey, FeatureResult, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import fail, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

SESSION_FAMILY = 'session_breadth'
OBSERVED_FAMILY = 'observed_breadth'
ADAPTIVE_FAMILY = 'adaptive_volume'
MAX_INPUT_BYTES = 1048576
BACKEND = 'python-exact'


def _ef3() -> ModuleType:
    from equity_features import ef3
    return ef3


def _package_version() -> str:
    import equity_features
    return equity_features.__version__


def _producer(config: object) -> tuple[str, tuple[str, ...]]:
    """Family and fixed role order of the configured producer (the library owns the identity check)."""
    library = _ef3()
    if type(config) is not ConfigSpec:
        fail()
    if config.identity == library.BREADTH_IDENTITY:
        return SESSION_FAMILY, _ROLES_SESSION
    if config.identity == library.OBSERVED_IDENTITY:
        return OBSERVED_FAMILY, _ROLES_OBSERVED
    if config.identity == library.ADAPTIVE_IDENTITY:
        return ADAPTIVE_FAMILY, _ROLES_ADAPTIVE
    fail(CommandErrorCode.CONFIG)


_ROLES_SESSION = ('population', 'prior_reference_availability', 'current_return', 'cumulative_volume', 'endpoint_price',
                  'vwap', 'momentum', 'selected_high', 'selected_low')
_ROLES_OBSERVED = ('members', 'change', 'qualifier_price', 'qualifier_volume')
_ROLES_ADAPTIVE = ('observations',)


def _definitions(config: ConfigSpec):
    return _ef3().ef3_breadth_definitions(config)


def _selection(features: tuple[str, ...], config: ConfigSpec) -> tuple[str, ...]:
    """Selected outputs: non-empty, unique, strings, each an executable output of this configuration (sorted)."""
    allowed = {d.feature_id for d in _definitions(config)}
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in allowed for f in features)):
        fail(CommandErrorCode.CONFIG)
    return tuple(sorted(features))


def _headers(config: ConfigSpec, selected: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    """Headers from the library's own definitions for this configuration (the worker owns no units or dtypes)."""
    definitions = {d.feature_id: d for d in _definitions(config)}
    return tuple(FeatureHeader(f, definitions[f].algorithm_version, definitions[f].dtype, definitions[f].unit, '1')
                 for f in selected)


def _intervals(config: ConfigSpec) -> tuple[IntervalSpec, ...]:
    session = config.session
    return (IntervalSpec(session.session_id, session.open_ns, session.close_ns),)


def _ordered(config: ConfigSpec) -> bool:
    """No ordered-history warmup or initialization is bound: membership intervals travel inside the supplied batches,
    and the adaptive stream is one epoch. The manifest therefore carries no warmup sessions and no initialization digest.
    """
    if type(config) is not ConfigSpec:
        fail()
    return False


def required_roles(config: ConfigSpec, features: tuple[str, ...]) -> tuple[str, ...]:
    """Roles the selected outputs declare in the library's own definitions, in the producer's fixed role order.

    The library validates and records only these roles, so the task binds exactly these roles.
    """
    with translated():
        selected = _selection(features, config)
        _, roles = _producer(config)
        needed = {r.role for d in _definitions(config) if d.feature_id in selected for r in d.requirements}
        return tuple(role for role in roles if role in needed)


def _admit(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    """Role-keyed admission of the supplied batches against the manifest (run before every calculation)."""
    with translated():
        library = _ef3()
        family, roles = _producer(task.config)
        if (type(task) is not TaskManifest or type(batches) is not tuple
                or any(type(b) is not CanonicalBatch for b in batches)
                or task.family != family or task.ownership != 'serialized_destination' or task.math_policy_version != 'v1'
                or len(task.instruments) != 1 or task.warmup_sessions != ()
                or task.initialization_sha256 is not None):
            fail()
        if task.max_input_bytes > MAX_INPUT_BYTES:
            fail(CommandErrorCode.LIMIT)
        if len(batches) > task.max_input_batches or len(pickle.dumps(batches, protocol=5)) > task.max_input_bytes:
            fail(CommandErrorCode.LIMIT)
        if {i.role for i in task.inputs} != set(required_roles(task.config, tuple(h.feature_id for h in task.features))):
            fail(CommandErrorCode.RESULT)
        bound = tuple(i for i in task.inputs if i.binding is not None)
        if len(bound) != len(batches) or len({b.metadata.source.input_id for b in batches}) != len(batches):
            fail(CommandErrorCode.RESULT)
        for manifest, batch in zip(bound, batches):
            binding = manifest.binding
            if ((binding.role, binding.kind, binding.metadata) != (manifest.role, batch.kind, batch.metadata)
                    or batch.metadata.coverage.observed != batch.row_count
                    or batch.metadata.namespace != task.config.session.namespace):
                fail(CommandErrorCode.RESULT)
            if manifest.role == 'observations':
                column = batch.column('instrument_id')
                if column is None or any(value != task.instruments[0] for value in column.values):
                    fail(CommandErrorCode.RESULT)
        if task.governed_sessions != _intervals(task.config) or task.ordered_history is not _ordered(task.config):
            fail(CommandErrorCode.RESULT)
        selected = _selection(tuple(h.feature_id for h in task.features), task.config)
        if task.features != _headers(task.config, selected):
            fail(CommandErrorCode.RESULT)
        return library


def admit_breadth_adaptive(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    """Admission used by the sequential runner (same checks as the work item builder)."""
    return _admit(task, batches)


def _identity(task: TaskManifest, result: FeatureResult, selected: tuple[str, ...]) -> None:
    """Result identity against the bound roles (exactly the roles the selected outputs declare)."""
    from equity_feature_io_sdk import descriptor
    required = set(required_roles(task.config, selected))
    metadata = result.metadata
    expected = tuple(sorted((i.binding for i in task.inputs if i.binding is not None and i.role in required),
                            key=lambda b: b.role))
    if (metadata.namespace, metadata.session_id, metadata.config_digest, metadata.availability,
        metadata.inputs, metadata.backend_id, metadata.backend_version, descriptor(result).features) != (
            task.config.session.namespace, task.config.session.session_id, task.config.digest,
            task.config.availability, expected, BACKEND, _package_version(), task.features):
        fail(CommandErrorCode.RESULT)


def compute_breadth_adaptive_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit(task, batches)
        family, roles = _producer(task.config)
        selected = _selection(tuple(h.feature_id for h in task.features), task.config)
        supplied = {manifest.role: batch for manifest, batch in zip(
            tuple(i for i in task.inputs if i.binding is not None), batches)}
        # Every nullable slot is passed in the library's fixed order; absent slots are None (the library's own rule).
        slots = tuple(supplied.get(role) for role in roles)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        # Counts, ratios, proximity, rates, baselines and accelerations are the library's; nothing is computed here.
        result = library.compute_ef3_breadth(slots, task.config, entity=entity, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        _identity(task, result, selected)
        return (result,)


def make_ef3_breadth_adaptive_work(config: ConfigSpec, *, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """Build one work item. Refusals raised by the library while the item is built surface here as worker failures."""
    with translated():
        family, roles = _producer(config)
        selected = _selection(features, config)
        if {i.role for i in inputs} != set(required_roles(config, selected)):
            fail(CommandErrorCode.RESULT)
        task = TaskManifest(job_id, generation_id, partition_id, family, (instrument_id,), config,
            _headers(config, selected), inputs, _intervals(config), destination_scope,
            'serialized_destination', max(1, len(batches)), max_input_bytes, _ordered(config), (), None)
        _admit(task, batches)
        return WorkItem(task, batches, compute_breadth_adaptive_inputs)


__all__ = ['required_roles', 'SESSION_FAMILY', 'OBSERVED_FAMILY', 'ADAPTIVE_FAMILY', 'admit_breadth_adaptive',
           'compute_breadth_adaptive_inputs', 'make_ef3_breadth_adaptive_work']
