"""Closed reference orchestration only; assessments remain in pure libraries."""
from __future__ import annotations

from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement, IntervalSpec
from equity_feature_io_contracts import FeatureHeader
from ._path_routes import context, headers, requirements
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import Calculation, WorkItem

CLASSIFICATION_ROLES = ('classification_facts', 'classification_history', 'classification_projection')
ACTION_ROLES = ('market', 'action_facts', 'action_history', 'action_projection', 'source_representation', 'source_quantity_basis')


def roles(producer: str) -> tuple[str, ...]:
    if producer == 'reference_classification':
        return CLASSIFICATION_ROLES
    if producer == 'reference_action':
        return ACTION_ROLES
    fail()


def governed(config: ConfigSpec) -> tuple[IntervalSpec, ...]:
    return (IntervalSpec(config.session.session_id, config.session.open_ns, config.session.close_ns),)


def alternatives(config: ConfigSpec, producer: str) -> tuple[tuple[InputRequirement, ...], ...]:
    """Complete OR variants, never flattened into simultaneous requirements."""
    with translated():
        library, _ = context(config, producer)
        variants: list[tuple[InputRequirement, ...]] = []
        for definition in library.ef1_definitions(config):
            for variant in definition.alternative_requirements:
                if variant not in variants:
                    variants.append(variant)
        return tuple(variants)


def slots(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[CanonicalBatch | None, ...]:
    """Mechanical reconstruction after ordinary actual binding admission."""
    result: list[CanonicalBatch | None] = []
    index = 0
    for declaration in task.inputs:
        if declaration.binding is None:
            result.append(None)
        else:
            if index >= len(batches):
                fail(CommandErrorCode.RESULT)
            result.append(batches[index])
            index += 1
    if index != len(batches):
        fail(CommandErrorCode.RESULT)
    return tuple(result)


def admit_reference(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576:
        fail(CommandErrorCode.LIMIT)
    library, _ = context(task.config, producer)
    if task.family != producer or tuple(item.role for item in task.inputs) != roles(producer):
        fail()
    if task.features != headers(task.config, producer) or task.governed_sessions != governed(task.config):
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None):
        fail(CommandErrorCode.RESULT)
    for role, batch in zip(roles(producer), slots(task, batches)):
        if batch is None:
            continue
        if role == 'market':
            if batch.kind not in (DataKind.TRADE, DataKind.BAR, DataKind.DAILY):
                fail()
        elif batch.kind is not DataKind.REFERENCE:
            fail()
    return library


def compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = admit_reference(task, batches, producer)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(slots(task, batches), task.config, entity=entity)
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
        context(config, producer)
        task = TaskManifest(job_id, generation_id, partition_id, producer,
            (instrument_id,), config, headers(config, producer), inputs,
            governed(config), destination_scope, 'serialized_destination',
            len(roles(producer)), max_input_bytes)
        admit_reference(task, batches, producer)
        return WorkItem(task, batches, calculate)
