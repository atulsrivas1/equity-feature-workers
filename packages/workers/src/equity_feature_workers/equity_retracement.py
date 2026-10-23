"""Closed retracement-level orchestration (ef2-retrace1); no retracement mathematics."""
from __future__ import annotations

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

ROLES = ('prior_high', 'prior_low')
PRODUCER = 'retracement_level'
IDENTITY = ('ef2.retracement_level', 'ef2-retracement-level-v1', '1')
PARAMETERS = frozenset(('policy_version', 'direction', 'ratio_num', 'ratio_den', 'evidence_limit'))
DIRECTIONS = frozenset(('from_high', 'from_low'))


def _library() -> ModuleType:
    # Assumed sibling API (core ef2-b-library): the EF2 retracement module in packages/features.
    from equity_features import retracement
    return retracement


def retracement_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    """The single contract header; ratio and direction are config, never extra outputs."""
    with translated():
        headers = tuple(FeatureHeader(d.feature_id, d.algorithm_version, d.dtype, d.unit, d.schema_version)
            for d in _library().retracement_definitions(config))
        if headers != (FeatureHeader('retracement.level', IDENTITY[1], ValueType.FLOAT64, 'currency/share', '1'),):
            fail(CommandErrorCode.RESULT)
        return headers


def required_retracement_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    with translated():
        result: list[InputRequirement] = []
        for definition in _library().retracement_definitions(config):
            for requirement in definition.requirements:
                if requirement not in result:
                    result.append(requirement)
        return tuple(result)


def _evidence_limit(config: ConfigSpec) -> int:
    values = {p.name: p.value for p in config.parameters}
    limit = values.get('evidence_limit')
    if (set(values) != PARAMETERS or type(limit) is not int or not 0 <= limit <= 10000
            or values['policy_version'] != 'ef2-retrace1' or values['direction'] not in DIRECTIONS):
        fail()
    return limit


def _admit_retracement(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 2:
        fail(CommandErrorCode.LIMIT)
    if task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    if (task.config.identity, task.config.algorithm_version, task.config.schema_version) != IDENTITY:
        fail(CommandErrorCode.RESULT)
    _evidence_limit(task.config)
    if task.family != PRODUCER or tuple(slot.role for slot in task.inputs) != ROLES:
        fail()
    session = task.config.session
    if (task.features != retracement_headers(task.config)
            or len(task.governed_sessions) != 1
            or task.governed_sessions != (IntervalSpec(session.session_id, session.open_ns, session.close_ns),)
            or task.ownership != 'serialized_destination'
            or (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None)):
        fail(CommandErrorCode.RESULT)
    actual = iter(batches)
    for declaration in task.inputs:
        if declaration.binding is not None and next(actual).kind is not DataKind.REFERENCE:
            fail()
    return _library()


def compute_retracement_inputs(task: TaskManifest,
        batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_retracement(task, batches)
        present = iter(batches)
        slots = tuple(None if declaration.binding is None else next(present) for declaration in task.inputs)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_retracement_level(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', library.__version__)
        limit = _evidence_limit(task.config)
        # Close only structural shape; the level, status, reasons and witnesses stay the library's.
        if (result.metadata.schema_version != '1' or result.metadata.math_policy_version != 'v1'
                or result.metadata.evidence_limit != limit
                or len(result.values) != 1 or len(result.quality) != 1 or len(result.evidence) > min(limit, 2)
                or any(column.entities != (entity,) or len(column.values) != 1 for column in result.values)
                or any(row.entity != entity for row in result.quality)
                or any(row.entity != entity for row in result.evidence)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef2_retracement_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        session = config.session
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER,
            (instrument_id,), config, retracement_headers(config), inputs,
            (IntervalSpec(session.session_id, session.open_ns, session.close_ns),),
            destination_scope, 'serialized_destination', 2, max_input_bytes)
        _admit_retracement(task, batches)
        return WorkItem(task, batches, compute_retracement_inputs)


__all__ = ['retracement_headers', 'required_retracement_inputs',
    'compute_retracement_inputs', 'make_ef2_retracement_work']
