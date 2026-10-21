"""Closed regular completed-BAR price orchestration; no numerical calculations."""
from __future__ import annotations

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement, ValueType
from equity_feature_io_contracts import FeatureHeader

from . import _path_routes as route
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

ROLES = ('calendar_provenance', 'bars', 'population_proof')
PRODUCER = 'regular_bar_prices'


def regular_bar_price_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    headers = route.headers(config, PRODUCER)
    if headers != tuple(FeatureHeader(name, 'v1', ValueType.FLOAT64, 'currency/share', '1') for name in (
            'session.close.calculated_price', 'session.regular.first_completed_bar_open')):
        fail(CommandErrorCode.RESULT)
    return headers


def required_regular_bar_price_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, PRODUCER)


def _admit_regular_bar_prices(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 3:
        fail(CommandErrorCode.LIMIT)
    if task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    library, decoded = route.context(task.config, PRODUCER)
    if task.family != PRODUCER or tuple(slot.role for slot in task.inputs) != ROLES:
        fail()
    if (task.features != regular_bar_price_headers(task.config)
            or task.governed_sessions != route.governed(decoded)
            or len(task.governed_sessions) != 1
            or task.governed_sessions[0].name != task.config.session.session_id
            or task.ownership != 'serialized_destination'
            or (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None)):
        fail(CommandErrorCode.RESULT)
    if decoded.get('selection') != {'phase_profile': 'regular_no_auctions1', 'sampling': 'completed_bar_close'}:
        fail()
    actual = iter(batches)
    for declaration, kind in zip(task.inputs, (DataKind.REFERENCE, DataKind.BAR, DataKind.REFERENCE)):
        if declaration.binding is not None and next(actual).kind is not kind:
            fail()
    return library


def compute_regular_bar_price_inputs(task: TaskManifest,
        batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_regular_bar_prices(task, batches)
        present = iter(batches)
        slots = tuple(None if declaration.binding is None else next(present) for declaration in task.inputs)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', '0.0.4a5')
        # Generic result identity does not close math/evidence declarations.
        if (result.metadata.schema_version != '1' or result.metadata.math_policy_version != 'v1'
                or result.metadata.evidence_limit != 2
                or len(result.values) != 2 or len(result.quality) != 2 or len(result.evidence) > 2
                or any(column.entities != (entity,) for column in result.values)
                or any(row.entity != entity for row in result.quality)
                or any(row.entity != entity for row in result.evidence)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_regular_bar_price_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        _, decoded = route.context(config, PRODUCER)
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER,
            (instrument_id,), config, regular_bar_price_headers(config), inputs,
            route.governed(decoded), destination_scope, 'serialized_destination', 3, max_input_bytes)
        _admit_regular_bar_prices(task, batches)
        return WorkItem(task, batches, compute_regular_bar_price_inputs)


__all__ = ['regular_bar_price_headers', 'required_regular_bar_price_inputs',
    'compute_regular_bar_price_inputs', 'make_ef1_regular_bar_price_work']
