"""Supplied current-state EF1 status orchestration; no event/proof inference."""
from __future__ import annotations

from collections.abc import Mapping
import json
from types import ModuleType

from equity_feature_contracts import (
    CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement,
)
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .equity_clock import _clock_governed
from .equity_sessions import _library
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem
from ._versions import FEATURES_VERSION


_ROLES = ('calendar_provenance', 'status_states', 'status_prefix', 'status_validity')


def _status_context(config: ConfigSpec) -> tuple[ModuleType, Mapping[str, object]]:
    if type(config) is not ConfigSpec or config.identity != 'ef1.status':
        fail()
    library = _library()
    context = library.decode_ef1_context(config)
    if not isinstance(context, Mapping) or context.get('producer') != 'status':
        fail()
    if len(config.parameters) != 1 or config.parameters[0].name != 'ef1_context_json':
        fail()
    text = config.parameters[0].value
    if type(text) is not str or not text.isascii() or not 1 <= len(text) <= 4096:
        fail()
    expected = library.build_ef1_config(json.loads(text), session=config.session,
        window=config.window, availability=config.availability,
        price_unit=config.price_unit, adjustment=config.adjustment)
    if expected != config or expected.digest != config.digest:
        fail(CommandErrorCode.RESULT)
    return library, context


def status_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    """Complete four-output set for exactly one selected status dimension."""
    with translated():
        library, _ = _status_context(config)
        return tuple(FeatureHeader(d.feature_id, d.algorithm_version, d.dtype,
            d.unit, d.schema_version) for d in library.ef1_definitions(config))


def required_status_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    """Expose pure-library requirements without a worker-owned source schema."""
    with translated():
        library, _ = _status_context(config)
        requirements: list[InputRequirement] = []
        for definition in library.ef1_definitions(config):
            for requirement in definition.requirements:
                if requirement not in requirements:
                    requirements.append(requirement)
        return tuple(requirements)


def _status_slots(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[CanonicalBatch | None, ...]:
    # Call only after ordinary actual-batch admission. Presence comes exclusively
    # from each manifest, never vocabulary/counts or a zip against shortened data.
    slots: list[CanonicalBatch | None] = []
    index = 0
    for declaration in task.inputs:
        if declaration.binding is None:
            slots.append(None)
        else:
            if index >= len(batches):
                fail(CommandErrorCode.RESULT)
            slots.append(batches[index])
            index += 1
    if index != len(batches):
        fail(CommandErrorCode.RESULT)
    return tuple(slots)


def _admit_status(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576:
        fail(CommandErrorCode.LIMIT)
    library, context = _status_context(task.config)
    if task.family != 'status' or tuple(slot.role for slot in task.inputs) != _ROLES:
        fail()
    if task.features != status_headers(task.config) or task.governed_sessions != _clock_governed(context):
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None):
        fail(CommandErrorCode.RESULT)
    if any(batch.kind is not DataKind.REFERENCE for batch in batches):
        fail()
    _status_slots(task, batches)
    return library


def compute_status_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    """Pass the exact original four slots and preserve the entire library result."""
    with translated():
        library = _admit_status(task, batches)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(_status_slots(task, batches), task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', FEATURES_VERSION)
        if any(column.entities != (entity,) for column in result.values):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_status_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    """Bind supplied declarations without creating sequence, coverage or facts."""
    with translated():
        _, context = _status_context(config)
        task = TaskManifest(job_id, generation_id, partition_id, 'status',
            (instrument_id,), config, status_headers(config), inputs,
            _clock_governed(context), destination_scope, 'serialized_destination',
            4, max_input_bytes)
        _admit_status(task, batches)
        return WorkItem(task, batches, compute_status_inputs)


__all__ = ['status_headers', 'required_status_inputs', 'compute_status_inputs', 'make_ef1_status_work']
