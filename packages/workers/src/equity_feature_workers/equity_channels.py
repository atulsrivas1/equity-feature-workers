"""Closed Ichimoku and price-channel orchestration (ef2-v1: ichimoku, keltner, donchian, pivot_levels, band_width_percentile, squeeze); no indicator mathematics.

Mirrors the accepted R17 supplied-intraday route (optional ``intraday`` extra): one BAR chunk per governed
session as ``bars:NNN`` roles, per-feature selection and independent readiness owned by the library.
One config is one producer (``ef2.<producer>``); the caller selects any non-empty subset of that producer's outputs.
"""
from __future__ import annotations

from typing import Any

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, FeatureResult, InputRequirement, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

ALGORITHM = 'ef2-channels-v1'
# Frozen header table (ef2-v1): producer -> output ids (lexical); units and dtypes by id. The worker only
# checks the library catalog against it.
PRODUCERS = {
    'ichimoku': tuple('trend.ichimoku.' + n for n in ('base_line', 'conversion_line', 'lagging_line_observed',
        'lagging_line_reference_close', 'leading_span_a_displayed', 'leading_span_a_observed',
        'leading_span_b_displayed', 'leading_span_b_observed')),
    'keltner': tuple('channel.keltner.' + n for n in ('lower', 'middle', 'upper', 'width_ratio')),
    'donchian': tuple('channel.donchian.' + n for n in ('lower', 'middle', 'upper', 'width_ratio')),
    'pivot_levels': tuple('channel.pivot.' + n for n in ('point', 'resistance_1', 'resistance_2', 'support_1', 'support_2')),
    'band_width_percentile': ('channel.band_width.percentile',),
    'bollinger_keltner_squeeze': ('channel.squeeze.bollinger_inside_keltner', 'channel.squeeze.lower_gap',
        'channel.squeeze.upper_gap'),
}
FEATURES = {f: p for p, ids in PRODUCERS.items() for f in ids}


def _unit(feature: str) -> str:
    return 'fraction' if feature.endswith(('width_ratio', 'percentile')) else (
        'boolean' if feature.endswith('bollinger_inside_keltner') else 'currency/share')


def _dtype(feature: str) -> ValueType:
    return ValueType.BOOL if feature.endswith('bollinger_inside_keltner') else ValueType.FLOAT64


def _library() -> ModuleType:
    import equity_features_intraday
    return equity_features_intraday


def _selection(features: tuple[str, ...]) -> tuple[str, ...]:
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in FEATURES for f in features)
            or len({FEATURES[f] for f in features}) != 1):
        fail()
    return tuple(sorted(features))


def _context(config: ConfigSpec, features: tuple[str, ...]) -> Any:
    """Decode the chunked R17 context and require the exact reviewed config (the library decoder)."""
    if type(config) is not ConfigSpec or config.price_unit is None:
        fail()
    library = _library()
    values = {p.name: p.value for p in config.parameters}
    count = values.get('context_chunks')
    if type(count) is not int or not 1 <= count <= 16:
        fail()
    chunks = tuple(values.get(f'context_{n:03}') for n in range(count))
    if any(type(chunk) is not str or not 1 <= len(chunk) <= 4096 or (n < count - 1 and len(chunk) != 4096)
            for n, chunk in enumerate(chunks)):
        fail()
    context = library.IntradayContext.from_json(''.join(chunk for chunk in chunks if isinstance(chunk, str)))
    # Closed envelope, producer identity, parameters, convention label and context transport are the library's decoder.
    settings = library.decode_channels_config(context, config)
    if (config.identity, config.algorithm_version, config.schema_version) != ('ef2.' + settings.producer, ALGORITHM, '1'):
        fail(CommandErrorCode.RESULT)
    if any(FEATURES[f] != settings.producer for f in features):
        fail()
    return context, settings


def channels_headers(config: ConfigSpec, features: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    with translated():
        selected = _selection(features)
        _context(config, selected)
        catalog = {entry.feature_id: entry for entry in _library().describe_channels()}
        headers = []
        for feature in selected:
            if feature not in catalog or catalog[feature].unit != _unit(feature):
                fail(CommandErrorCode.RESULT)
            headers.append(FeatureHeader(feature, catalog[feature].algorithm_version, _dtype(feature), _unit(feature), '1'))
        return tuple(headers)


def _lookback(settings: Any) -> int | None:
    """Slots read before the target for producers with a bounded rolling window (None: anchor through target)."""
    if settings.producer == 'donchian':
        return int(settings.param('period')) - 1
    if settings.producer == 'ichimoku':
        # Widest display read: the window ending at t - displacement.
        return int(settings.param('displacement') + max(settings.param(n) for n in
            ('conversion_period', 'base_period', 'leading_b_period')) - 1)
    return None


def channels_history(config: ConfigSpec, features: tuple[str, ...]) -> tuple[bool, tuple[str, ...], str]:
    """Warm-up sessions that the read range touches; slot arithmetic stays with the context."""
    with translated():
        selected = _selection(features)
        context, settings = _context(config, selected)
        slots = context.slots()
        target = slots[-1].session_id
        if settings.producer == 'pivot_levels':
            # Pivots read the whole immediately preceding governed session (none is missing input, ruling O8).
            order = [s.session_id for s in context.sessions]
            touched = {order[order.index(target) - 1]} if order.index(target) > 0 else set()
        else:
            back = _lookback(settings)
            start = context.anchor_index if back is None else max(context.anchor_index, context.target_index - back)
            touched = {slot.session_id for slot in slots[start:context.target_index + 1] if slot.session_id != target}
        return True, tuple(s.session_id for s in context.sessions if s.session_id in touched), context.digest


def required_channels_inputs(config: ConfigSpec, features: tuple[str, ...]) -> tuple[InputRequirement, ...]:
    with translated():
        selected = _selection(features)
        _, settings = _context(config, selected)
        producer = settings.producer
        fields = ['instrument_id', 'session_id', 'start_ns', 'end_ns', 'known_at_ns']
        fields += {'ichimoku': ['high', 'low', 'close'], 'keltner': ['high', 'low', 'close'],
            'donchian': ['high', 'low'], 'pivot_levels': ['high', 'low', 'close'],
            'band_width_percentile': ['close'], 'bollinger_keltner_squeeze': ['high', 'low', 'close']}[producer]
        rule = {'ichimoku': 'high and low of each output own window (displayed spans end at t - displacement); close only at t and t - displacement',
            'keltner': 'close of every slot from the epoch anchor and high and low from the second',
            'donchian': 'high of the upper window and low of the lower window',
            'pivot_levels': 'high and low of every slot of the immediately preceding governed session and its last close',
            'band_width_percentile': 'closes of the band period plus population bars',
            'bollinger_keltner_squeeze': 'close of every slot from the epoch anchor and high and low from the second'}[producer]
        return (
            InputRequirement('bars', 'canonical:1', tuple(fields), DataKind.BAR,
                rule=f'{producer}: {rule}; null/absent delivery remains per-feature quality, not global rejection'),
            InputRequirement('session', 'session:1', ('namespace', 'session_id', 'open_ns', 'close_ns'),
                rule='actual supplied governed sessions and decoded context'),
            InputRequirement('window', 'window:1', ('governed_sessions', 'target_session_id'),
                rule='actual ordered slots, selected anchor and independent closure'),
            InputRequirement('availability', 'availability:1', ('market_cutoff_ns', 'knowledge_cutoff_ns', 'evaluation_ns'),
                rule='actual consumed bounds/known-at; no unrelated-field readiness requirement'),
        )


def _admit_channels(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.math_policy_version != 'v1':
        fail(CommandErrorCode.LIMIT if task.max_input_bytes > 1048576 else CommandErrorCode.RESULT)
    if task.ownership != 'serialized_destination':
        fail()
    selected = _selection(tuple(h.feature_id for h in task.features))
    context, settings = _context(task.config, selected)
    if task.family != settings.producer:
        fail()
    if task.features != channels_headers(task.config, selected) or task.instruments != (context.instrument_id,):
        fail(CommandErrorCode.RESULT)
    sessions = tuple((s.session_id, s.open_ns, s.close_ns) for s in context.sessions)
    if tuple((s.name, s.start_ns, s.end_ns) for s in task.governed_sessions) != sessions:
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != channels_history(task.config, selected):
        fail(CommandErrorCode.RESULT)
    missing = not batches and len(task.inputs) == 1 and task.inputs[0].role == 'bars' and task.inputs[0].state == 'missing'
    if not missing:
        if (not batches or tuple(i.role for i in task.inputs) != tuple(f'bars:{n:03}' for n in range(len(batches)))
                or any(i.binding is None for i in task.inputs) or task.max_input_batches != len(batches)
                or any(batch.kind is not DataKind.BAR for batch in batches)):
            fail()
    elif task.max_input_batches != 1:
        fail(CommandErrorCode.LIMIT)
    return _library()


def compute_channels_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_channels(task, batches)
        selected = _selection(tuple(h.feature_id for h in task.features))
        context, settings = _context(task.config, selected)
        # Operands, recurrences, ranking, readiness and witnesses are the library's; nothing is computed here.
        result = library.compute_channels(batches or None, context, task.config, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'intraday.python', library.__version__)
        if (result.metadata.evidence_limit != settings.evidence_limit or len(result.values) != len(selected)
                or len(result.quality) != len(selected) or len(result.evidence) > settings.evidence_limit
                or any(len(column.values) != 1 for column in result.values)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef2_channels_work(config: ConfigSpec, *, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        selected = _selection(features)
        context, settings = _context(config, selected)
        history = channels_history(config, selected)
        task = TaskManifest(job_id, generation_id, partition_id, settings.producer, (instrument_id,), config,
            channels_headers(config, selected), inputs,
            tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in context.sessions),
            destination_scope, 'serialized_destination', max(1, len(batches)), max_input_bytes,
            history[0], history[1], history[2])
        _admit_channels(task, batches)
        return WorkItem(task, batches, compute_channels_inputs)


__all__ = ['channels_headers', 'channels_history', 'required_channels_inputs',
    'compute_channels_inputs', 'make_ef2_channels_work']
