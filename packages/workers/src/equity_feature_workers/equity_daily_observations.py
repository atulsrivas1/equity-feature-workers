"""Closed daily-observation orchestration; numerical admission belongs to EF1."""
from __future__ import annotations

from collections.abc import Mapping
import json
from types import ModuleType

from equity_feature_contracts import (
    CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult,
    InputRequirement, IntervalSpec,
)
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .equity_sessions import _library
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

ROLES = ('calendar_manifest', 'calendar_slots', 'observation_coverage',
    'raw_observations', 'common_observations', 'basis_provenance')


def _context(config: ConfigSpec) -> ModuleType:
    if type(config) is not ConfigSpec:
        fail()
    library = _library()
    decoded = library.decode_ef1_context(config)
    if (not isinstance(decoded, Mapping) or decoded.get('producer') != 'daily_observations'
            or decoded.get('version') != 'ef1-daily-context2'
            or len(config.parameters) != 1 or config.parameters[0].name != 'ef1_context'):
        fail()
    text = config.parameters[0].value
    if type(text) is not str or not text.isascii() or not 1 <= len(text) <= 4096:
        fail()
    expected = library.build_ef1_config(json.loads(text), session=config.session,
        window=config.window, availability=config.availability,
        price_unit=config.price_unit, adjustment=config.adjustment)
    if expected != config or expected.digest != config.digest:
        fail(CommandErrorCode.RESULT)
    return library


def _governed(config: ConfigSpec) -> tuple[IntervalSpec, ...]:
    return (IntervalSpec(config.session.session_id, config.session.open_ns, config.session.close_ns),)


def daily_observations_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    with translated():
        library = _context(config)
        return tuple(FeatureHeader(d.feature_id, d.algorithm_version, d.dtype,
            d.unit, d.schema_version) for d in library.ef1_definitions(config))


def required_daily_observations_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    with translated():
        library = _context(config)
        result: list[InputRequirement] = []
        for definition in library.ef1_definitions(config):
            for requirement in definition.requirements:
                if requirement not in result:
                    result.append(requirement)
        return tuple(result)


def _admit_daily_observations(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 6:
        fail(CommandErrorCode.LIMIT)
    if task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    library = _context(task.config)
    if task.family != 'daily_observations' or tuple(slot.role for slot in task.inputs) != ROLES:
        fail()
    if (task.features != daily_observations_headers(task.config)
            or task.governed_sessions != _governed(task.config)
            or (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None)):
        fail(CommandErrorCode.RESULT)
    if any(batch.kind is not DataKind.REFERENCE for batch in batches):
        fail()
    return library


def compute_daily_observations_inputs(task: TaskManifest,
        batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_daily_observations(task, batches)
        actual = iter(batches)
        slots = tuple(None if slot.binding is None else next(actual) for slot in task.inputs)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'ef1', '0.0.4a5')
        if result.metadata.math_policy_version != 'v1' or result.metadata.evidence_limit != 28:
            fail(CommandErrorCode.RESULT)
        if (len(result.values) != 4 or len(result.quality) != 4 or len(result.evidence) > 28
                or any(column.entities != (entity,) for column in result.values)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_daily_observations_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        _context(config)
        task = TaskManifest(job_id, generation_id, partition_id, 'daily_observations',
            (instrument_id,), config, daily_observations_headers(config), inputs,
            _governed(config), destination_scope, 'serialized_destination', 6, max_input_bytes)
        _admit_daily_observations(task, batches)
        return WorkItem(task, batches, compute_daily_observations_inputs)


__all__ = ['daily_observations_headers', 'required_daily_observations_inputs',
    'compute_daily_observations_inputs', 'make_ef1_daily_observations_work']
