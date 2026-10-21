"""Closed four-role gap endpoint orchestration; no numerical calculations."""
from __future__ import annotations

from collections.abc import Mapping
from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader

from . import _path_routes as route
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

ROLES = ('calendar_provenance', 'path', 'prior_close', 'population_proof')
PRODUCER = 'path_gap_endpoint'


def gap_endpoint_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, PRODUCER)


def required_gap_endpoint_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, PRODUCER)


def _admit_gap_endpoint(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 4:
        fail(CommandErrorCode.LIMIT)
    if task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    library, decoded = route.context(task.config, PRODUCER)
    if task.family != PRODUCER or tuple(slot.role for slot in task.inputs) != ROLES:
        fail()
    if (task.features != gap_endpoint_headers(task.config)
            or task.governed_sessions != route.governed(decoded)
            or (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None)):
        fail(CommandErrorCode.RESULT)
    selection = decoded.get('selection')
    if not isinstance(selection, Mapping):
        fail()
    sampling = selection.get('sampling')
    if sampling not in ('eligible_trade', 'completed_bar_close'):
        fail()
    kinds = (DataKind.REFERENCE, DataKind.TRADE if sampling == 'eligible_trade' else DataKind.BAR,
        DataKind.DAILY, DataKind.REFERENCE)
    actual = iter(batches)
    for declaration, kind in zip(task.inputs, kinds):
        if declaration.binding is not None and next(actual).kind is not kind:
            fail()
    return library


def compute_gap_endpoint_inputs(task: TaskManifest,
        batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_gap_endpoint(task, batches)
        present = iter(batches)
        slots = tuple(None if declaration.binding is None else next(present) for declaration in task.inputs)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', '0.0.4a5')
        if (result.metadata.math_policy_version != 'v1' or result.metadata.evidence_limit != 5
                or len(result.values) != 2 or len(result.quality) != 2 or len(result.evidence) > 5
                or any(column.entities != (entity,) for column in result.values)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_gap_endpoint_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        _, decoded = route.context(config, PRODUCER)
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER,
            (instrument_id,), config, gap_endpoint_headers(config), inputs,
            route.governed(decoded), destination_scope, 'serialized_destination', 4, max_input_bytes)
        _admit_gap_endpoint(task, batches)
        return WorkItem(task, batches, compute_gap_endpoint_inputs)


__all__ = ['gap_endpoint_headers', 'required_gap_endpoint_inputs',
    'compute_gap_endpoint_inputs', 'make_ef1_gap_endpoint_work']
