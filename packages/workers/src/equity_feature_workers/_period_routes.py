"""Closed period/phase orchestration; all calendar mathematics stays in libraries."""
from __future__ import annotations

from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult
from ._path_routes import context, governed, headers as headers, requirements as requirements
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import Calculation, WorkItem


def roles(producer: str) -> tuple[str, ...]:
    if producer == 'period':
        return ('calendar_provenance', 'daily_history', 'bars')
    if producer == 'extended':
        return ('calendar_provenance', 'bars', 'prior_close')
    fail()


def admit_period(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 3:
        fail(CommandErrorCode.LIMIT)
    library, decoded = context(task.config, producer)
    if task.family != producer or tuple(slot.role for slot in task.inputs) != roles(producer):
        fail()
    if task.features != headers(task.config, producer) or task.governed_sessions != governed(decoded):
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None):
        fail(CommandErrorCode.RESULT)
    kinds = ((DataKind.REFERENCE, DataKind.DAILY, DataKind.BAR) if producer == 'period'
        else (DataKind.REFERENCE, DataKind.BAR, DataKind.DAILY))
    actual = iter(batches)
    for slot, kind in zip(task.inputs, kinds):
        if slot.binding is not None and next(actual).kind is not kind:
            fail()
    return library


def compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = admit_period(task, batches, producer)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        # Present-role subsequence, never invented batches or nullable arguments.
        result = library.compute_ef1(batches, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', '0.0.4a5')
        if any(column.entities != (entity,) for column in result.values):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make(config: ConfigSpec, *, producer: str, calculate: Calculation,
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...],
        instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, max_input_bytes: int) -> WorkItem:
    with translated():
        _, decoded = context(config, producer)
        task = TaskManifest(job_id, generation_id, partition_id, producer,
            (instrument_id,), config, headers(config, producer), inputs,
            governed(decoded), destination_scope, 'serialized_destination', 3, max_input_bytes)
        admit_period(task, batches, producer)
        return WorkItem(task, batches, calculate)
