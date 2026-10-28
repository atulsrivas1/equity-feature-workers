"""Explicit supplied EF1 clock orchestration; no clock mathematics or acquisition."""
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
from .supervisor import WorkItem
from ._versions import FEATURES_VERSION


def _clock_context(config: ConfigSpec) -> tuple[ModuleType, Mapping[str, object]]:
    if type(config) is not ConfigSpec:
        fail()
    library = _library()  # Exact featurea5/contractsa4; existing anchor helper.
    context = library.decode_ef1_context(config)
    if not isinstance(context, Mapping) or context.get('producer') != 'clock':
        fail()
    if len(config.parameters) != 1 or config.parameters[0].name != 'ef1_context_json':
        fail()
    text = config.parameters[0].value
    if type(text) is not str or not text.isascii() or not 1 <= len(text) <= 4096:
        fail()
    # The public decoder owns closed mathematical context/canonical admission.
    # Reconstruct the whole original config, including schedule operands.
    expected = library.build_ef1_config(json.loads(text), session=config.session,
        window=config.window, availability=config.availability,
        price_unit=config.price_unit, adjustment=config.adjustment)
    if expected != config or expected.digest != config.digest:
        fail(CommandErrorCode.RESULT)
    return library, context


def _clock_governed(context: Mapping[str, object]) -> tuple[IntervalSpec, ...]:
    calendar = context.get('calendar')
    if not isinstance(calendar, Mapping):
        fail()
    # These exact row shapes/order/int64 operands were admitted by the public
    # decoder; workers neither generate dates nor calculate/adjust boundaries.
    rows = cast(tuple[tuple[str, str, int, int], ...], calendar['sessions'])
    return tuple(IntervalSpec(row[0], row[2], row[3]) for row in rows)


def clock_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    """Return all five public producer headers in their actual fixed order."""
    with translated():
        library, _ = _clock_context(config)
        return tuple(FeatureHeader(d.feature_id, d.algorithm_version, d.dtype,
            d.unit, d.schema_version) for d in library.ef1_definitions(config))


def required_clock_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    with translated():
        library, _ = _clock_context(config)
        requirements: list[InputRequirement] = []
        for definition in library.ef1_definitions(config):
            for requirement in definition.requirements:
                if requirement not in requirements:
                    requirements.append(requirement)
        return tuple(requirements)


def _admit_clock(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    library, context = _clock_context(task.config)
    if task.family != 'clock' or tuple(slot.role for slot in task.inputs) != ('calendar_provenance',):
        fail()
    if task.features != clock_headers(task.config) or task.governed_sessions != _clock_governed(context):
        fail(CommandErrorCode.RESULT)
    # Scheduled clock queries have no recurrence/warm-up/checkpoint state.
    # Full calendar selection/proof identity already lives in config and inputs.
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None):
        fail(CommandErrorCode.RESULT)
    if batches and batches[0].kind is not DataKind.REFERENCE:
        fail()
    return library


def compute_clock_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    """One clock config returns one complete unchanged public FeatureResult."""
    with translated():
        library = _admit_clock(task, batches)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(batches, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', FEATURES_VERSION)
        if any(column.entities != (entity,) for column in result.values):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_clock_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    """Bind actual adapter declarations; do not infer source knowledge or math."""
    with translated():
        _, context = _clock_context(config)
        task = TaskManifest(job_id, generation_id, partition_id, 'clock',
            (instrument_id,), config, clock_headers(config), inputs,
            _clock_governed(context), destination_scope, 'serialized_destination',
            1, max_input_bytes)
        _admit_clock(task, batches)
        return WorkItem(task, batches, compute_clock_inputs)


__all__ = ['clock_headers', 'required_clock_inputs', 'compute_clock_inputs', 'make_ef1_clock_work']
