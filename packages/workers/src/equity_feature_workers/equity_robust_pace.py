"""Closed EF3 robust history, robust volume pace and mean-ratio orchestration (ef3-robust1, ef3-pace1, ef3-mean1).

Three producers, one closed route each, over explicitly supplied batches only. The worker admits the supplied batches
against the reviewed configuration, binds them to the fixed library role slots and calls the pure library dispatch
(``equity_features.ef3.compute_ef3_robust``). It contains no median, MAD, quantile, rank, ratio, z-score or mean
mathematics and never infers, fills, sorts, rescales, imputes or defaults a role, slot, window, population, bucket,
grid or inclusion. The library owns admission, mathematics, quality, witnesses and every typed refusal; a refusal
surfaces as a worker failure and publishes nothing.

Unlike the EF3 daily-grid route, no process-level registry is needed: the library carries the complete window,
population and grid through the configuration and the supplied role batches, so no grid object is bound by digest and
nothing outlives a work item.
"""
from __future__ import annotations

import pickle
from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, EntityKey, FeatureResult, IntervalSpec
from equity_feature_io_contracts import FeatureHeader

from ._supplied import fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

HISTORY_FAMILY = 'robust_history'
PACE_FAMILY = 'volume_pace_robust'
MEAN_FAMILY = 'volume_ratio_mean'
MAX_INPUT_BYTES = 1048576
MULTI_MEMBER_ROLES = ('population', 'cohort_values')
# Fixed library role slots (the library's own ROLES order); a producer's absent role is None in its slot tuple.
HISTORY_ROLES = ('series', 'sessions', 'population', 'cohort_values')
PACE_ROLES = ('grid', 'sessions', 'bars')
MEAN_ROLES = ('series', 'sessions')
_PRODUCERS = {
    HISTORY_FAMILY: ('ef3.robust_history', 'ef3-robust-history-v1', 'ROBUST_FEATURES', HISTORY_ROLES),
    PACE_FAMILY: ('ef3.volume_pace_robust', 'ef3-volume-pace-robust-v1', 'PACE_FEATURES', PACE_ROLES),
    MEAN_FAMILY: ('ef3.volume_ratio_mean', 'ef3-volume-ratio-mean-v1', 'MEAN_FEATURES', MEAN_ROLES),
}


def _ef3() -> ModuleType:
    from equity_features import ef3
    return ef3


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
    definitions = {d.feature_id: d for d in _ef3().ef3_robust_definitions(config)}
    if set(selected) - set(definitions):
        fail(CommandErrorCode.RESULT)
    return tuple(FeatureHeader(f, definitions[f].algorithm_version, definitions[f].dtype, definitions[f].unit, '1')
                 for f in selected)


def _intervals(sessions) -> tuple[IntervalSpec, ...]:
    return tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in sessions)


def _admit(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str) -> ModuleType:
    """Shared closed admission: family, identity, headers, role-keyed bindings and the exact target entity."""
    with translated():
        identity, algorithm, allowed_name, roles = _PRODUCERS[family]
        library = _ef3()
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
        allowed = getattr(library, allowed_name)
        selected = _selection(tuple(h.feature_id for h in task.features), allowed)
        if task.features != _headers(config, selected):
            fail(CommandErrorCode.RESULT)
        if {i.role for i in task.inputs} - set(roles):
            fail(CommandErrorCode.RESULT)
        bound = tuple(i for i in task.inputs if i.binding is not None)
        if len(bound) != len(batches) or len({b.metadata.source.input_id for b in batches}) != len(batches):
            fail(CommandErrorCode.RESULT)
        for manifest, batch in zip(bound, batches):
            binding = manifest.binding
            if ((binding.role, binding.kind, binding.metadata) != (manifest.role, batch.kind, batch.metadata)
                    or batch.metadata.coverage.observed != batch.row_count
                    or batch.metadata.namespace != config.session.namespace or manifest.role not in roles):
                fail(CommandErrorCode.RESULT)
            if manifest.role in MULTI_MEMBER_ROLES:
                continue  # declared members of the cohort; the library admits membership and the target row
            column = batch.column('instrument_id')
            if column is None or any(value != task.instruments[0] for value in column.values):
                fail(CommandErrorCode.RESULT)
        if task.instruments != (task.instruments[0],) or task.ordered_history is not False or task.warmup_sessions != ():
            fail(CommandErrorCode.RESULT)
        if task.initialization_sha256 is not None:
            fail(CommandErrorCode.RESULT)
        return library


def _slots(task: TaskManifest, batches: tuple[CanonicalBatch, ...], roles: tuple[str, ...]) -> tuple:
    """Bind each supplied batch to its fixed library slot by its manifest role; absent roles stay None."""
    by_role = {manifest.role: batch for manifest, batch in zip(
        tuple(i for i in task.inputs if i.binding is not None), batches)}
    return tuple(by_role.get(role) for role in roles)


def _compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit(task, batches, family)
        identity, algorithm, allowed_name, roles = _PRODUCERS[family]
        selected = tuple(h.feature_id for h in task.features)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef3_robust(_slots(task, batches, roles), task.config, entity=entity, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', _package_version())
        return (result,)


def compute_robust_history_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, HISTORY_FAMILY)


def compute_volume_pace_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, PACE_FAMILY)


def compute_volume_ratio_mean_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, MEAN_FAMILY)


def _make(config: ConfigSpec, family: str, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple, max_input_bytes: int, calculate) -> WorkItem:
    with translated():
        identity, algorithm, allowed_name, roles = _PRODUCERS[family]
        if type(config) is not ConfigSpec:
            fail()
        library = _ef3()
        selected = _selection(features, getattr(library, allowed_name))
        task = TaskManifest(job_id, generation_id, partition_id, family, (instrument_id,), config,
            _headers(config, selected), inputs, _intervals(governed), destination_scope, 'serialized_destination',
            max(1, len(batches)), max_input_bytes, False, (), None)
        _admit(task, batches, family)
        return WorkItem(task, batches, calculate)


def make_robust_history_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, governed: tuple, job_id: str, generation_id: str,
        partition_id: str, destination_scope: str, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`governed` is the supplied sessions (session_id, open_ns, close_ns) declared for this item, in grid order."""
    return _make(config, HISTORY_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_robust_history_inputs)


def make_volume_pace_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, governed: tuple, job_id: str, generation_id: str,
        partition_id: str, destination_scope: str, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    return _make(config, PACE_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_volume_pace_inputs)


def make_volume_ratio_mean_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, governed: tuple, job_id: str, generation_id: str,
        partition_id: str, destination_scope: str, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    return _make(config, MEAN_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_volume_ratio_mean_inputs)


__all__ = ['HISTORY_FAMILY', 'PACE_FAMILY', 'MEAN_FAMILY', 'make_robust_history_work', 'make_volume_pace_work',
           'make_volume_ratio_mean_work', 'compute_robust_history_inputs', 'compute_volume_pace_inputs',
           'compute_volume_ratio_mean_inputs']
