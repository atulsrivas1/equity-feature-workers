"""Closed EF5 price-structure orchestration (``ef5-candle1``, ``ef5-structure1``, ``ef5-technical1``).

Three producers (candle, structure, technical), one closed route each, over explicitly supplied inputs only. The worker admits
the supplied BAR chunks against the reviewed configuration and passes them to the pure library (``equity_features_intraday``).
It contains no candle geometry, swing, return, velocity, acceleration or trend-count mathematics, and never infers, fills,
sorts, rescales or defaults a role, slot, window, epoch profile, adjustment or grid. The library owns admission, mathematics,
quality, witnesses and every typed refusal; a refusal surfaces as a worker failure and publishes nothing. There is no
process-level registry: the configuration carries the whole governed context.
"""
from __future__ import annotations

from collections.abc import Iterable
from types import ModuleType
from typing import Any

from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

EF5_FAMILIES = {'candle': 'ef5_candle', 'structure': 'ef5_structure', 'technical': 'ef5_technical'}
_PRODUCER_OF = {family: producer for producer, family in EF5_FAMILIES.items()}
MAX_INPUT_BYTES = 1048576
# The unit fixes the value type of an EF5 header (contract headers table); the gate checks it against every library result.
_DTYPE = {'boolean': ValueType.BOOL, 'windows': ValueType.INT64}


def _intraday() -> ModuleType:
    import equity_features_intraday
    return equity_features_intraday


def _param(config: ConfigSpec, name: str) -> Any:
    return {p.name: p.value for p in config.parameters}.get(name)


def _catalog(producer: str) -> dict[str, Any]:
    return {e.feature_id: e for e in _intraday().describe_ef5() if e.feature_id.startswith(producer + '.')}


def _selection(producer: str, features: tuple[str, ...]) -> tuple[str, ...]:
    known = _catalog(producer)
    if type(features) is not tuple or not features or any(type(f) is not str for f in features) or len(set(features)) != len(features):
        fail()
    for feature in features:
        _intraday().require_ef5_capability(feature, 'batch')  # an unknown id is the library's UNKNOWN_FEATURE
        if feature not in known:
            fail()
    return tuple(sorted(features))


def _intervals(sessions: Iterable[Any]) -> tuple[IntervalSpec, ...]:
    return tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in sessions)


def _ef5_context(config: ConfigSpec, producer: str) -> Any:
    """Decode the chunked R17 context and require the exact reviewed envelope (the library decoder owns every rule)."""
    if type(config) is not ConfigSpec or producer not in EF5_FAMILIES:
        fail()
    library = _intraday()
    count = _param(config, 'context_chunks')
    if type(count) is not int or not 1 <= count <= 16:
        fail()
    chunks = tuple(_param(config, f'context_{n:03}') for n in range(count))
    if any(type(chunk) is not str or not 1 <= len(chunk) <= 4096 or (n < count - 1 and len(chunk) != 4096)
            for n, chunk in enumerate(chunks)):
        fail()
    context = library.IntradayContext.from_json(''.join(chunks))
    settings = library.decode_ef5_config(producer, context, config)
    spec = library.EF5_PRODUCERS[producer]
    if (config.identity, config.algorithm_version, config.schema_version) != (spec.identity, spec.algorithm, '1'):
        fail(CommandErrorCode.RESULT)
    return context, settings


def _headers(producer: str, selected: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    catalog = _catalog(producer)
    return tuple(FeatureHeader(f, catalog[f].algorithm_version, _DTYPE.get(catalog[f].unit, ValueType.FLOAT64),
                               catalog[f].unit, '1') for f in selected)


def _admit_ef5(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    with translated():
        library = _intraday()
        producer = _PRODUCER_OF.get(task.family)
        if producer is None:
            fail(CommandErrorCode.RESULT)
        context, settings = _ef5_context(task.config, producer)
        if task.instruments != (context.instrument_id,):
            fail(CommandErrorCode.RESULT)
        admit(task, batches)
        if (task.max_input_bytes > MAX_INPUT_BYTES or task.math_policy_version != 'v1'
                or task.ownership != 'serialized_destination'):
            fail(CommandErrorCode.LIMIT if task.max_input_bytes > MAX_INPUT_BYTES else CommandErrorCode.RESULT)
        selected = _selection(producer, tuple(h.feature_id for h in task.features))
        for feature in selected:
            library.require_ef5_capability(feature, 'batch')
        if task.features != _headers(producer, selected) or task.governed_sessions != _intervals(context.sessions):
            fail(CommandErrorCode.RESULT)
        if task.ordered_history is not True or task.warmup_sessions != () or task.initialization_sha256 != context.digest:
            fail(CommandErrorCode.RESULT)
        if not batches and not (len(task.inputs) == 1 and task.inputs[0].state == 'missing'):
            fail()
        return library


def compute_ef5_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_ef5(task, batches)
        producer = _PRODUCER_OF[task.family]
        context, settings = _ef5_context(task.config, producer)
        selected = tuple(h.feature_id for h in task.features)
        # Every value, status, reason, count and witness is the library's; nothing is computed here.
        result = library.compute_ef5(producer, batches or None, context, task.config, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'intraday.python', library.__version__)
        return (result,)


def make_ef5_work(producer: str, config: ConfigSpec, *, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    with translated():
        context, settings = _ef5_context(config, producer)
        if instrument_id != context.instrument_id:
            fail(CommandErrorCode.RESULT)
        selected = _selection(producer, features)
        task = TaskManifest(job_id, generation_id, partition_id, EF5_FAMILIES[producer], (instrument_id,), config,
            _headers(producer, selected), inputs, _intervals(context.sessions), destination_scope,
            'serialized_destination', max(1, len(batches)), max_input_bytes, True, (), context.digest)
        _admit_ef5(task, batches)
        return WorkItem(task, batches, compute_ef5_inputs)


__all__ = ['EF5_FAMILIES', 'make_ef5_work', 'compute_ef5_inputs']
