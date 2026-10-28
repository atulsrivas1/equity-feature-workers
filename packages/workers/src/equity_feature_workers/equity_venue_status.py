"""Closed multi-venue status reducer orchestration; no reduction, precedence or counting."""
from __future__ import annotations

from collections.abc import Mapping
from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from . import _path_routes as route
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem
from ._versions import FEATURES_VERSION

ROLES = ('venue_population', 'venue_seed', 'venue_states', 'venue_prefix', 'venue_validity')
PRODUCER = 'multivenue_status'
# Frozen closed vocabularies (ef1-multivenue-status1, D03/D11); the worker only checks membership,
# never derives a state or a reason.
STATES = frozenset(('active', 'halted', 'closed'))
REASONS = frozenset(('absent_population', 'incomplete_population', 'empty_population', 'absent_input',
    'incomplete_status_prefix', 'unknown_availability', 'future_knowledge', 'outside_effective_interval',
    'stale_state', 'unanimous', 'precedence_resolved', 'precedence_saturated'))


def venue_status_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    headers = route.headers(config, PRODUCER)
    if headers != (
            FeatureHeader('session.venue_status.aggregate_reason', 'v1', ValueType.STRING, 'reason', '1'),
            FeatureHeader('session.venue_status.aggregate_state', 'v1', ValueType.STRING, 'state', '1'),
            FeatureHeader('session.venue_status.known_nontrading_count', 'v1', ValueType.INT64, 'publishers', '1'),
            FeatureHeader('session.venue_status.known_trading_count', 'v1', ValueType.INT64, 'publishers', '1'),
            FeatureHeader('session.venue_status.latest_event_ns', 'v1', ValueType.INT64, 'utc_ns', '1'),
            FeatureHeader('session.venue_status.quoting_any', 'v1', ValueType.BOOL, 'boolean', '1'),
            FeatureHeader('session.venue_status.restriction_any', 'v1', ValueType.BOOL, 'boolean', '1')):
        fail(CommandErrorCode.RESULT)
    return headers


def required_venue_status_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, PRODUCER)


def _governed(config: ConfigSpec) -> tuple[IntervalSpec, ...]:
    # The contract context carries no calendar (D04); the single governed interval is the target
    # session already declared on the config. Nothing is generated or adjusted here.
    session = config.session
    return (IntervalSpec(session.session_id, session.open_ns, session.close_ns),)


def _admit_venue_status(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 5:
        fail(CommandErrorCode.LIMIT)
    if task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    if (task.config.identity, task.config.algorithm_version, task.config.schema_version) != (
            'ef1.multivenue_status', 'v1', '1'):
        fail(CommandErrorCode.RESULT)
    library, decoded = route.context(task.config, PRODUCER)
    if task.family != PRODUCER or tuple(slot.role for slot in task.inputs) != ROLES:
        fail()
    if (task.features != venue_status_headers(task.config)
            or task.governed_sessions != _governed(task.config)
            or task.ownership != 'serialized_destination'
            or (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None)):
        fail(CommandErrorCode.RESULT)
    if decoded.get('calendar') is not None or not isinstance(decoded.get('selection'), Mapping):
        fail()
    # Present-only batches are matched against the original role bindings; all five are REFERENCE
    # and no role is inferred from DataKind or count.
    actual = iter(batches)
    for declaration in task.inputs:
        if declaration.binding is not None and next(actual).kind is not DataKind.REFERENCE:
            fail()
    return library


def compute_venue_status_inputs(task: TaskManifest,
        batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_venue_status(task, batches)
        present = iter(batches)
        slots = tuple(None if declaration.binding is None else next(present) for declaration in task.inputs)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', FEATURES_VERSION)
        # Generic result identity does not close math/evidence declarations.
        if (result.metadata.schema_version != '1' or result.metadata.math_policy_version != 'v1'
                or result.metadata.evidence_limit != 4
                or len(result.values) != 7 or len(result.quality) != 7 or len(result.evidence) > 4
                or any(column.entities != (entity,) for column in result.values)
                or any(row.entity != entity for row in result.quality)
                or any(row.entity != entity for row in result.evidence)):
            fail(CommandErrorCode.RESULT)
        # Close only structural shape; every value, reason and witness stays the library's.
        cells = {column.feature_id.rsplit('.', 1)[1]: column.values for column in result.values}
        reason, state = cells.get('aggregate_reason', ()), cells.get('aggregate_state', ())
        if (len(reason) != 1 or reason[0] not in REASONS
                or len(state) != 1 or (state[0] is not None and state[0] not in STATES)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_venue_status_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        route.context(config, PRODUCER)
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER,
            (instrument_id,), config, venue_status_headers(config), inputs,
            _governed(config), destination_scope, 'serialized_destination', 5, max_input_bytes)
        _admit_venue_status(task, batches)
        return WorkItem(task, batches, compute_venue_status_inputs)


__all__ = ['venue_status_headers', 'required_venue_status_inputs',
    'compute_venue_status_inputs', 'make_ef1_venue_status_work']
