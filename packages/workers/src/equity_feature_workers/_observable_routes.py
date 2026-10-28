"""Closed observable orchestration. Fixed nullable slots preserve role identity."""
from __future__ import annotations
from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult
from ._path_routes import context, governed, headers as headers, requirements as requirements
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import Calculation, WorkItem
from ._versions import FEATURES_VERSION


def roles(producer: str) -> tuple[str, ...]:
    if producer in ('opening_trade', 'opening_trade_seconds'):
        return ('calendar_provenance', 'market_prefix', 'source_capability', 'trades')
    if producer == 'status_history':
        return ('calendar_provenance', 'history_events', 'history_prefix', 'history_seed', 'source_capability')
    fail()


def admit_observable(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> ModuleType:
    admit(task, batches)
    expected_roles = roles(producer)
    if task.max_input_bytes > 1048576 or task.max_input_batches != len(expected_roles):
        fail(CommandErrorCode.LIMIT)
    library, decoded = context(task.config, producer)
    if task.family != producer or tuple(slot.role for slot in task.inputs) != expected_roles:
        fail()
    if task.config.algorithm_version != 'observable-v2' or task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    if task.features != headers(task.config, producer) or task.governed_sessions != governed(decoded):
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None):
        fail(CommandErrorCode.RESULT)
    actual = iter(batches)
    for slot in task.inputs:
        kind = DataKind.TRADE if slot.role == 'trades' else DataKind.REFERENCE
        if slot.binding is not None and next(actual).kind is not kind:
            fail()
    return library


def compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], producer: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = admit_observable(task, batches, producer)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        actual = iter(batches)
        slots = tuple(None if slot.binding is None else next(actual) for slot in task.inputs)
        result = library.compute_ef1(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'ef1', FEATURES_VERSION)
        if result.metadata.math_policy_version != 'v1' or result.metadata.evidence_limit != 32:
            fail(CommandErrorCode.RESULT)
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
            governed(decoded), destination_scope, 'serialized_destination', len(roles(producer)),
            max_input_bytes)
        admit_observable(task, batches, producer)
        return WorkItem(task, batches, calculate)
