"""Closed regular first-trade price orchestration; no numerical calculations."""
from __future__ import annotations

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement, ValueType
from equity_feature_io_contracts import FeatureHeader

from . import _path_routes as route
from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem
from ._versions import FEATURES_VERSION

ROLES = ('calendar_provenance', 'trades', 'population_proof')
PRODUCER = 'regular_first_trade_price'


def regular_first_trade_price_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    headers = route.headers(config, PRODUCER)
    if headers != tuple(FeatureHeader(name, 'v1', ValueType.FLOAT64, 'currency/share', '1') for name in ('session.regular.first_trade_price',)):
        fail(CommandErrorCode.RESULT)
    return headers


def required_regular_first_trade_price_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, PRODUCER)


def _admit_regular_first_trade_price(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.max_input_batches != 3:
        fail(CommandErrorCode.LIMIT)
    if task.math_policy_version != 'v1':
        fail(CommandErrorCode.RESULT)
    if (task.config.identity, task.config.algorithm_version, task.config.schema_version) != (
            'ef1.regular_first_trade_price', 'v1', '1'):
        fail(CommandErrorCode.RESULT)
    library, decoded = route.context(task.config, PRODUCER)
    if task.family != PRODUCER or tuple(slot.role for slot in task.inputs) != ROLES:
        fail()
    if (task.features != regular_first_trade_price_headers(task.config)
            or task.governed_sessions != route.governed(decoded)
            or len(task.governed_sessions) != 1
            or (task.governed_sessions[0].name, task.governed_sessions[0].start_ns,
                task.governed_sessions[0].end_ns) != (task.config.session.session_id,
                task.config.session.open_ns, task.config.session.close_ns)
            or task.ownership != 'serialized_destination'
            or (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != (False, (), None)):
        fail(CommandErrorCode.RESULT)
    if decoded.get('selection') != {'phase_profile': 'regular_no_auctions1', 'sampling': 'eligible_trade'}:
        fail()
    actual = iter(batches)
    for declaration, kind in zip(task.inputs, (DataKind.REFERENCE, DataKind.TRADE, DataKind.REFERENCE)):
        if declaration.binding is not None and next(actual).kind is not kind:
            fail()
    return library


def compute_regular_first_trade_price_inputs(task: TaskManifest,
        batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_regular_first_trade_price(task, batches)
        present = iter(batches)
        slots = tuple(None if declaration.binding is None else next(present) for declaration in task.inputs)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        result = library.compute_ef1(slots, task.config, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', FEATURES_VERSION)
        # Generic result identity does not close math/evidence declarations.
        if (result.metadata.schema_version != '1' or result.metadata.math_policy_version != 'v1'
                or result.metadata.evidence_limit != 1
                or len(result.values) != 1 or len(result.quality) != 1 or len(result.evidence) > 1
                or any(column.entities != (entity,) for column in result.values)
                or any(row.entity != entity for row in result.quality)
                or any(row.entity != entity for row in result.evidence)):
            fail(CommandErrorCode.RESULT)
        # Close structural quality/evidence semantics without recomputing firstness or price.
        column, = result.values
        quality, = result.quality
        trades = slots[1]
        if (len(column.values) != 1 or quality.feature_id != column.feature_id
                or (quality.expected, quality.observed) != (
                    None if trades is None else trades.metadata.coverage.expected,
                    0 if trades is None else trades.row_count)):
            fail(CommandErrorCode.RESULT)
        if (quality.status.value not in ('available', 'missing_input', 'incomplete_coverage', 'not_applicable')
                or tuple(reason.value for reason in quality.reasons) != tuple(sorted(reason.value for reason in quality.reasons))
                or (quality.status.value == 'available' and quality.reasons)):
            fail(CommandErrorCode.RESULT)
        available = quality.status.value == 'available'
        if ((column.values[0] is not None) != available or len(result.evidence) != int(available)):
            fail(CommandErrorCode.RESULT)
        if available:
            witness, = result.evidence
            if (trades is None or witness.feature_id != column.feature_id
                    or witness.input_id != trades.metadata.source.input_id
                    or witness.event_ns is None or witness.known_at_ns is None
                    or witness.effective_start_ns is not None or witness.effective_end_ns is not None
                    or witness.use != 'consumed' or witness.exclusion_reason is not None
                    or witness.boundary != 'ordinary'):
                fail(CommandErrorCode.RESULT)
            identities = trades.column('event_id')
            events = trades.column('event_ns')
            clocks = trades.column('known_at_ns')
            eligibility = trades.column('eligible')
            if identities is None or events is None or clocks is None or eligibility is None:
                fail(CommandErrorCode.RESULT)
            if not any((row_id, event, clock, eligible) == (
                    witness.row_id, witness.event_ns, witness.known_at_ns, True)
                    for row_id, event, clock, eligible in zip(identities.values, events.values,
                        clocks.values, eligibility.values)):
                fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_regular_first_trade_price_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        _, decoded = route.context(config, PRODUCER)
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER,
            (instrument_id,), config, regular_first_trade_price_headers(config), inputs,
            route.governed(decoded), destination_scope, 'serialized_destination', 3, max_input_bytes)
        _admit_regular_first_trade_price(task, batches)
        return WorkItem(task, batches, compute_regular_first_trade_price_inputs)


__all__ = ['regular_first_trade_price_headers', 'required_regular_first_trade_price_inputs',
    'compute_regular_first_trade_price_inputs', 'make_ef1_regular_first_trade_price_work']
