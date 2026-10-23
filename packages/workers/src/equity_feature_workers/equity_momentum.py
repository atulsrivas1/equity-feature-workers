"""Closed momentum and trend orchestration (ef2-conv1: MACD, DMI/ADX, stochastic, Williams %R); no indicator mathematics.

Mirrors the accepted R17 supplied-intraday route (optional ``intraday`` extra): chunked ``bars:NNN``
roles on the governed grid, per-feature selection and independent readiness owned by the library.
One config is one homogeneous family; the caller selects any non-empty subset of that family's outputs.
"""
from __future__ import annotations

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, FeatureResult, InputRequirement, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

PRODUCER = 'momentum'
IDENTITY = ('intraday.momentum.v1', 'intraday-momentum-v1', '1')
# Frozen header table (ef2-conv1): feature -> (family, unit); all outputs are float64. The worker only
# checks the library catalog against it.
FEATURES = {'intraday.macd_histogram': ('macd', 'currency/share'), 'intraday.macd_line': ('macd', 'currency/share'),
    'intraday.macd_signal': ('macd', 'currency/share'),
    'intraday.adx': ('dmi', 'DMI points'), 'intraday.dx': ('dmi', 'DMI points'),
    'intraday.minus_di': ('dmi', 'DMI points'), 'intraday.plus_di': ('dmi', 'DMI points'),
    'intraday.stoch_d': ('stochastic', 'stochastic points'), 'intraday.stoch_k': ('stochastic', 'stochastic points'),
    'intraday.williams_r': ('stochastic', 'Williams %R points')}
ANCHORED = ('macd', 'dmi')


def _library() -> ModuleType:
    import equity_features_intraday
    return equity_features_intraday


def _selection(features: tuple[str, ...]) -> tuple[str, ...]:
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in FEATURES for f in features)
            or len({FEATURES[f][0] for f in features}) != 1):
        fail()
    return tuple(sorted(features))


def _context(config: ConfigSpec, features: tuple[str, ...]):
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
    # Closed envelope, family, periods, conventions and context transport are the library's decoder.
    settings = library.decode_momentum_config(context, config)
    if (config.identity, config.algorithm_version, config.schema_version) != IDENTITY:
        fail(CommandErrorCode.RESULT)
    if any(FEATURES[f][0] != settings.family for f in features):
        fail()
    return context, settings


def momentum_headers(config: ConfigSpec, features: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    with translated():
        selected = _selection(features)
        _context(config, selected)
        catalog = {entry.feature_id: entry for entry in _library().describe_momentum()}
        headers = []
        for feature in selected:
            if feature not in catalog or catalog[feature].unit != FEATURES[feature][1]:
                fail(CommandErrorCode.RESULT)
            headers.append(FeatureHeader(feature, catalog[feature].algorithm_version, ValueType.FLOAT64,
                FEATURES[feature][1], '1'))
        return tuple(headers)


def _span(feature: str, periods: tuple[int, ...]) -> int:
    """Rolling stochastic window length in slots (library-documented k/slowing/d arithmetic)."""
    k, slowing, d = periods
    return {'intraday.williams_r': k, 'intraday.stoch_k': k + slowing - 1, 'intraday.stoch_d': k + slowing + d - 2}[feature]


def momentum_history(config: ConfigSpec, features: tuple[str, ...]) -> tuple[bool, tuple[str, ...], str]:
    """Warm-up sessions that the slot range touches; slot arithmetic stays with the context."""
    with translated():
        selected = _selection(features)
        context, settings = _context(config, selected)
        if settings.family in ANCHORED:
            start = context.anchor_index
        else:
            start = max(context.anchor_index, context.target_index - max(_span(f, settings.periods) for f in selected) + 1)
        slots = context.slots()
        target = slots[-1].session_id
        touched = {slot.session_id for slot in slots[start:context.target_index + 1] if slot.session_id != target}
        return True, tuple(s.session_id for s in context.sessions if s.session_id in touched), context.digest


def required_momentum_inputs(config: ConfigSpec, features: tuple[str, ...]) -> tuple[InputRequirement, ...]:
    with translated():
        selected = _selection(features)
        _, settings = _context(config, selected)
        fields = ['instrument_id', 'session_id', 'start_ns', 'end_ns', 'known_at_ns', 'close']
        if settings.family != 'macd':
            fields[5:5] = ['high', 'low']
        rule = {'macd': 'close of every slot from the epoch anchor through target (empty slots are unready)',
            'dmi': 'high, low and close from the anchor through the prior slot and high and low at the target',
            'stochastic': 'high, low and close of the output own rolling window; close only on its last slots'}[settings.family]
        return (
            InputRequirement('bars', 'canonical:1', tuple(fields), DataKind.BAR,
                rule=f'{PRODUCER}: {settings.family}: {rule}; null/absent delivery remains per-feature quality, not global rejection'),
            InputRequirement('session', 'session:1', ('namespace', 'session_id', 'open_ns', 'close_ns'),
                rule='actual supplied governed sessions and decoded context'),
            InputRequirement('window', 'window:1', ('governed_sessions', 'target_session_id'),
                rule='actual ordered slots, selected anchor and independent closure'),
            InputRequirement('availability', 'availability:1', ('market_cutoff_ns', 'knowledge_cutoff_ns', 'evaluation_ns'),
                rule='actual consumed bounds/known-at; no unrelated-field readiness requirement'),
        )


def _admit_momentum(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.math_policy_version != 'v1':
        fail(CommandErrorCode.LIMIT if task.max_input_bytes > 1048576 else CommandErrorCode.RESULT)
    if task.family != PRODUCER or task.ownership != 'serialized_destination':
        fail()
    selected = _selection(tuple(h.feature_id for h in task.features))
    context, _ = _context(task.config, selected)
    if task.features != momentum_headers(task.config, selected) or task.instruments != (context.instrument_id,):
        fail(CommandErrorCode.RESULT)
    sessions = tuple((s.session_id, s.open_ns, s.close_ns) for s in context.sessions)
    if tuple((s.name, s.start_ns, s.end_ns) for s in task.governed_sessions) != sessions:
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != momentum_history(task.config, selected):
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


def compute_momentum_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_momentum(task, batches)
        selected = _selection(tuple(h.feature_id for h in task.features))
        context, settings = _context(task.config, selected)
        # Shared per-bar preparation, recurrences and independent readiness are the library's; nothing is computed here.
        result = library.compute_momentum(batches or None, context, task.config, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'intraday.python', library.__version__)
        if (result.metadata.evidence_limit != settings.evidence_limit or len(result.values) != len(selected)
                or len(result.quality) != len(selected) or len(result.evidence) > settings.evidence_limit
                or any(len(column.values) != 1 for column in result.values)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef2_momentum_work(config: ConfigSpec, *, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        selected = _selection(features)
        context, _ = _context(config, selected)
        history = momentum_history(config, selected)
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER, (instrument_id,), config,
            momentum_headers(config, selected), inputs,
            tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in context.sessions),
            destination_scope, 'serialized_destination', max(1, len(batches)), max_input_bytes,
            history[0], history[1], history[2])
        _admit_momentum(task, batches)
        return WorkItem(task, batches, compute_momentum_inputs)


__all__ = ['momentum_headers', 'momentum_history', 'required_momentum_inputs',
    'compute_momentum_inputs', 'make_ef2_momentum_work']
