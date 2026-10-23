"""Closed EF4 quote dynamics and VWAP path orchestration (ef4-quote1, ef4-vwap1).

Two producers, one closed route each, over explicitly supplied batches only. The worker admits the supplied batches
against the reviewed configuration, binds them to the fixed library role slots and calls the pure library dispatch
(``equity_features.ef4.compute_quote_vwap``). It contains no spread, midpoint, microprice, VWAP, slope, extremum or
comparison mathematics and never infers, fills, sorts, rescales, imputes or defaults a role, slot, grid, interval,
coverage or series source. The library owns admission, mathematics, quality, witnesses and every typed refusal; a refusal
surfaces as a worker failure and publishes nothing.

No entity, instrument, session, role, snapshot, grid or series identifier is fixed here: the target instrument and the
role names come from the task manifest, the session and grid version from the configuration. As in the other EF3 and EF4
routes no process-level registry is needed.
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

QUOTE_FAMILY = 'quote_dynamics'
VWAP_FAMILY = 'vwap_path'
MAX_INPUT_BYTES = 1048576
# Fixed library role slots (the library's own order); a role the producer does not read is None in its slot tuple.
QUOTE_ROLES = ('bounds', 'quotes')
VWAP_ROLES = ('bounds', 'grid', 'trades', 'bars', 'proof')
_PRODUCERS = {
    QUOTE_FAMILY: ('ef4.quote_dynamics', 'ef4-quote-dynamics-v1', 'QUOTE_FEATURES', QUOTE_ROLES),
    VWAP_FAMILY: ('ef4.vwap_path', 'ef4-vwap-path-v1', 'VWAP_FEATURES', VWAP_ROLES),
}


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
    definitions = {d.feature_id: d for d in _ef4().quote_vwap_definitions(config)}
    if set(selected) - set(definitions):
        fail()  # an output the configuration cannot execute (for example a continuous-only quote output on snapshots)
    return tuple(FeatureHeader(f, definitions[f].algorithm_version, definitions[f].dtype, definitions[f].unit, '1')
                 for f in selected)


def _intervals(sessions) -> tuple[IntervalSpec, ...]:
    return tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in sessions)


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
        allowed = getattr(library, allowed_name)
        selected = _selection(tuple(h.feature_id for h in task.features), allowed)
        if task.features != _headers(config, selected):
            fail(CommandErrorCode.RESULT)
        if {i.role for i in task.inputs} - set(roles):
            fail(CommandErrorCode.RESULT)
        bound = tuple(i for i in task.inputs if i.binding is not None)
        if (len(bound) != len(batches) or len({b.metadata.source.input_id for b in batches}) != len(batches)
                or len({i.role for i in bound}) != len(bound)):
            fail(CommandErrorCode.RESULT)
        for manifest, batch in zip(bound, batches):
            binding = manifest.binding
            if ((binding.role, binding.kind, binding.metadata) != (manifest.role, batch.kind, batch.metadata)
                    or batch.metadata.coverage.observed != batch.row_count
                    or batch.metadata.namespace != config.session.namespace or manifest.role not in roles):
                fail(CommandErrorCode.RESULT)
            column = batch.column('instrument_id')
            if column is None or any(value != task.instruments[0] for value in column.values):
                fail(CommandErrorCode.RESULT)
        if task.ordered_history is not False or task.warmup_sessions != () or task.initialization_sha256 is not None:
            fail(CommandErrorCode.RESULT)
        return library


def _slots(task: TaskManifest, batches: tuple[CanonicalBatch, ...], roles: tuple[str, ...]) -> tuple:
    """Bind each supplied batch to its fixed library slot by its manifest role; roles not delivered stay None."""
    by_role = {manifest.role: batch for manifest, batch in zip(
        tuple(i for i in task.inputs if i.binding is not None), batches)}
    return tuple(by_role.get(role) for role in roles)


def _compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit(task, batches, family)
        identity, algorithm, allowed_name, roles = _PRODUCERS[family]
        selected = tuple(h.feature_id for h in task.features)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_quote_vwap(_slots(task, batches, roles), task.config, entity=entity, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', _package_version())
        return (result,)


def compute_quote_dynamics_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, QUOTE_FAMILY)


def compute_vwap_path_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, VWAP_FAMILY)


def _make(config: ConfigSpec, family: str, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple | None, max_input_bytes: int, calculate) -> WorkItem:
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


def make_quote_dynamics_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple | None = None, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`inputs` bind the delivered roles (`bounds`, `quotes`) to `batches` in the same order."""
    return _make(config, QUOTE_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_quote_dynamics_inputs)


def make_vwap_path_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, governed: tuple | None = None, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`inputs` bind the delivered roles (`bounds`, `grid`, `trades`, `bars`, `proof`) to `batches` in the same order."""
    return _make(config, VWAP_FAMILY, features=features, inputs=inputs, batches=batches, instrument_id=instrument_id,
        job_id=job_id, generation_id=generation_id, partition_id=partition_id, destination_scope=destination_scope,
        governed=governed, max_input_bytes=max_input_bytes, calculate=compute_vwap_path_inputs)


__all__ = ['QUOTE_FAMILY', 'VWAP_FAMILY', 'make_quote_dynamics_work', 'make_vwap_path_work',
           'compute_quote_dynamics_inputs', 'compute_vwap_path_inputs']
