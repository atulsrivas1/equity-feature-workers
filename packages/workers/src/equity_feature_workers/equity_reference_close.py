"""Closed reference-close comparison orchestration; no numerical calculations."""
from __future__ import annotations

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement, ValueType
from equity_feature_io_contracts import FeatureHeader

from . import _path_routes as route
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

ROLES = ('calendar_provenance', 'bars', 'population_proof', 'authoritative_close')
PRODUCER = 'reference_close'
# Frozen closed vocabulary (ef1-close-compare1); the worker only checks membership, never derives a state.
STATES = frozenset(('calculated_unavailable', 'outside_tolerance', 'reference_future',
    'reference_invalid', 'reference_unavailable', 'within_tolerance'))


def reference_close_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    headers = route.headers(config, PRODUCER)
    if headers != (
            FeatureHeader('reference.close.absolute_difference_bps', 'v1', ValueType.FLOAT64, 'bps', '1'),
            FeatureHeader('reference.close.comparison_state', 'v1', ValueType.STRING, 'state', '1'),
            FeatureHeader('reference.close.supplied_price', 'v1', ValueType.DECIMAL128, 'currency/share', '1'),
            FeatureHeader('reference.close.within_tolerance', 'v1', ValueType.BOOL, 'boolean', '1')):
        fail(CommandErrorCode.RESULT)
    return headers


def required_reference_close_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, PRODUCER)


def _admit_reference_close(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 4:
        fail(CommandErrorCode.LIMIT)
    if task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    if (task.config.identity, task.config.algorithm_version, task.config.schema_version) != (
            'ef1.reference_close', 'v1', '1'):
        fail(CommandErrorCode.RESULT)
    library, decoded = route.context(task.config, PRODUCER)
    if task.family != PRODUCER or tuple(slot.role for slot in task.inputs) != ROLES:
        fail()
    if (task.features != reference_close_headers(task.config)
            or task.governed_sessions != route.governed(decoded)
            or len(task.governed_sessions) != 1
            or (task.governed_sessions[0].name, task.governed_sessions[0].start_ns,
                task.governed_sessions[0].end_ns) != (task.config.session.session_id,
                task.config.session.open_ns, task.config.session.close_ns)
            or task.ownership != 'serialized_destination'
            or (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None)):
        fail(CommandErrorCode.RESULT)
    selection = decoded.get('selection')
    if (not isinstance(selection, dict)
            or (selection.get('phase_profile'), selection.get('sampling')) != (
                'regular_no_auctions1', 'completed_bar_close')):
        fail()
    actual = iter(batches)
    for declaration, kind in zip(task.inputs, (DataKind.REFERENCE, DataKind.BAR, DataKind.REFERENCE, DataKind.REFERENCE)):
        if declaration.binding is not None and next(actual).kind is not kind:
            fail()
    return library


def compute_reference_close_inputs(task: TaskManifest,
        batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_reference_close(task, batches)
        present = iter(batches)
        slots = tuple(None if declaration.binding is None else next(present) for declaration in task.inputs)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', '0.0.4a5')
        # Generic result identity does not close math/evidence declarations.
        if (result.metadata.schema_version != '1' or result.metadata.math_policy_version != 'v1'
                or result.metadata.evidence_limit != 7
                or len(result.values) != 4 or len(result.quality) != 4 or len(result.evidence) > 7
                or any(column.entities != (entity,) for column in result.values)
                or any(row.entity != entity for row in result.quality)
                or any(row.entity != entity for row in result.evidence)):
            fail(CommandErrorCode.RESULT)
        # Close only structural shape; the verdict, state and BPS stay the library's.
        state = next((column for column in result.values
            if column.feature_id == 'reference.close.comparison_state'), None)
        if state is None or len(state.values) != 1 or state.values[0] not in STATES:
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_reference_close_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        _, decoded = route.context(config, PRODUCER)
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER,
            (instrument_id,), config, reference_close_headers(config), inputs,
            route.governed(decoded), destination_scope, 'serialized_destination', 4, max_input_bytes)
        _admit_reference_close(task, batches)
        return WorkItem(task, batches, compute_reference_close_inputs)


__all__ = ['reference_close_headers', 'required_reference_close_inputs',
    'compute_reference_close_inputs', 'make_ef1_reference_close_work']
