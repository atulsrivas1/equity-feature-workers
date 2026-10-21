"""Shared closed sampled-path admission; no acquisition, selection or mathematics."""
from __future__ import annotations

from collections.abc import Mapping
import json
from types import ModuleType
from typing import cast

from equity_feature_contracts import (
    CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult,
    InputRequirement, IntervalSpec,
)
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .equity_sessions import _library
from .manifests import InputManifest, TaskManifest
from .supervisor import Calculation, WorkItem


def context(config: ConfigSpec, producer: str) -> tuple[ModuleType, Mapping[str, object]]:
    if type(config) is not ConfigSpec:
        fail()
    library = _library()
    decoded = library.decode_ef1_context(config)
    if not isinstance(decoded, Mapping) or decoded.get('producer') != producer:
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
    return library, decoded


def governed(decoded: Mapping[str, object]) -> tuple[IntervalSpec, ...]:
    calendar = decoded.get('calendar')
    if not isinstance(calendar, Mapping):
        fail()
    rows = cast(tuple[tuple[str, str, int, int], ...], calendar['sessions'])
    return tuple(IntervalSpec(row[0], row[2], row[3]) for row in rows)


def headers(config: ConfigSpec, producer: str) -> tuple[FeatureHeader, ...]:
    with translated():
        library, _ = context(config, producer)
        return tuple(FeatureHeader(d.feature_id, d.algorithm_version, d.dtype,
            d.unit, d.schema_version) for d in library.ef1_definitions(config))


def requirements(config: ConfigSpec, producer: str) -> tuple[InputRequirement, ...]:
    with translated():
        library, _ = context(config, producer)
        result: list[InputRequirement] = []
        for definition in library.ef1_definitions(config):
            for requirement in definition.requirements:
                if requirement not in result:
                    result.append(requirement)
        return tuple(result)


def admit_path(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> ModuleType:
    admit(task, batches)
    library, decoded = context(task.config, producer)
    roles = ('calendar_provenance', 'path', 'prior_close') if producer == 'path_gap' else ('calendar_provenance', 'path')
    if task.family != producer or tuple(slot.role for slot in task.inputs) != roles:
        fail()
    if task.features != headers(task.config, producer) or task.governed_sessions != governed(decoded):
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None):
        fail(CommandErrorCode.RESULT)
    selection = decoded.get('selection')
    if not isinstance(selection, Mapping):
        fail()
    sampling = selection.get('sampling')
    if sampling not in ('eligible_trade', 'completed_bar_close'):
        fail()
    kinds = (DataKind.REFERENCE,
        DataKind.TRADE if sampling == 'eligible_trade' else DataKind.BAR, DataKind.DAILY)
    actual = iter(batches)
    for slot, kind in zip(task.inputs, kinds):
        if slot.binding is not None and next(actual).kind is not kind:
            fail()
    # Original prior DAILY session/scope is deliberately retained. The pure
    # library admits it against the explicit preceding calendar slot, never S.
    return library


def compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = admit_path(task, batches, producer)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        # Only actual batches, in declared present subsequence; no None/fakes.
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
            governed(decoded), destination_scope, 'serialized_destination',
            3 if producer == 'path_gap' else 2, max_input_bytes)
        admit_path(task, batches, producer)
        return WorkItem(task, batches, calculate)
