"""Closed money-flow orchestration (ef2-mf1: OBV, A/D line, CMF); no money-flow mathematics.

Mirrors the accepted R17 supplied-intraday route (optional ``intraday`` extra): chunked ``bars:NNN``
roles on the governed grid, per-feature selection and independent readiness owned by the library.
"""
from __future__ import annotations

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, FeatureResult, InputRequirement, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

PRODUCER = 'money_flow'
IDENTITY = ('ef2.money_flow', 'ef2-money-flow-v1', '1')
# Frozen header table (ef2-mf1); the worker only checks the library against it.
FEATURES = {'intraday.ad_line': (ValueType.FLOAT64, 'shares'), 'intraday.cmf': (ValueType.FLOAT64, 'fraction'),
    'intraday.obv': (ValueType.INT64, 'shares')}
_FIXED = frozenset(('policy_version', 'evidence_limit', 'context_schema', 'context_sha256', 'context_chunks'))


def _library() -> ModuleType:
    # Assumed sibling API (core ef2-b-library): money-flow additions to equity_features_intraday.
    import equity_features_intraday
    return equity_features_intraday


def _selection(features: tuple[str, ...]) -> tuple[str, ...]:
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in FEATURES for f in features)):
        fail()
    return tuple(sorted(features))


def _context(config: ConfigSpec, features: tuple[str, ...]):
    """Decode the chunked R17 context and require the exact reviewed config (rebuild and compare)."""
    if type(config) is not ConfigSpec or config.price_unit is None:
        fail()
    library = _library()
    values = {p.name: p.value for p in config.parameters}
    count = values.get('context_chunks')
    if type(count) is not int or not 1 <= count <= 16:
        fail()
    names = set(_FIXED) | {f'context_{n:03}' for n in range(count)}
    wants_period = 'intraday.cmf' in features
    if wants_period:
        names.add('cmf_period')
    if set(values) != names:
        fail()
    chunks = tuple(values[f'context_{n:03}'] for n in range(count))
    if any(type(chunk) is not str or not 1 <= len(chunk) <= 4096 or (n < count - 1 and len(chunk) != 4096)
            for n, chunk in enumerate(chunks)):
        fail()
    context = library.IntradayContext.from_json(''.join(chunk for chunk in chunks if isinstance(chunk, str)))
    period, limit, policy = values.get('cmf_period'), values['evidence_limit'], values['policy_version']
    if (type(limit) is not int or not 0 <= limit <= 10000 or policy != 'ef2-mf1'
            or (wants_period and (type(period) is not int or not 1 <= period <= 4096))):
        fail()
    expected = library.build_money_flow_config(context, cmf_period=period if wants_period else None,
        availability=config.availability, price_unit=config.price_unit, adjustment=config.adjustment,
        evidence_limit=limit, policy_version=policy)
    if config != expected or (config.identity, config.algorithm_version, config.schema_version) != IDENTITY:
        fail(CommandErrorCode.RESULT)
    return context, (period if wants_period else None), limit


def money_flow_headers(config: ConfigSpec, features: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    with translated():
        selected = _selection(features)
        _context(config, selected)
        catalog = {entry.feature_id: entry for entry in _library().describe_money_flow()}
        headers = []
        for feature in selected:
            dtype, unit = FEATURES[feature]
            if feature not in catalog or catalog[feature].unit != unit:
                fail(CommandErrorCode.RESULT)
            headers.append(FeatureHeader(feature, catalog[feature].algorithm_version, dtype, unit, '1'))
        return tuple(headers)


def money_flow_history(config: ConfigSpec, features: tuple[str, ...]) -> tuple[bool, tuple[str, ...], str]:
    """Warm-up sessions that the slot range touches; slot arithmetic stays with the context."""
    with translated():
        selected = _selection(features)
        context, period, _ = _context(config, selected)
        anchored = any(f in ('intraday.obv', 'intraday.ad_line') for f in selected)
        start = context.anchor_index if anchored else max(context.anchor_index, context.target_index - period + 1)
        slots = context.slots()
        target = slots[-1].session_id
        touched = {slot.session_id for slot in slots[start:context.target_index + 1] if slot.session_id != target}
        return True, tuple(s.session_id for s in context.sessions if s.session_id in touched), context.digest


def required_money_flow_inputs(config: ConfigSpec, features: tuple[str, ...]) -> tuple[InputRequirement, ...]:
    with translated():
        selected = _selection(features)
        _, period, _ = _context(config, selected)
        fields = ['instrument_id', 'session_id', 'start_ns', 'end_ns', 'known_at_ns', 'close', 'volume']
        if any(f in ('intraday.ad_line', 'intraday.cmf') for f in selected):
            fields[5:5] = ['high', 'low']
        rules = []
        for feature in selected:
            rules.append({'intraday.obv': 'close and volume, anchor through target (reset epoch; empty slots add 0)',
                'intraday.ad_line': 'high, low, close and volume, anchor through target (empty slots add 0)',
                'intraday.cmf': f'high, low, close and volume of the last {period} governed slots inside the epoch'}[feature])
        return (
            InputRequirement('bars', 'canonical:1', tuple(fields), DataKind.BAR,
                rule=f'{PRODUCER}: ' + '; '.join(rules) + '; null/absent delivery remains per-feature quality, not global rejection'),
            InputRequirement('session', 'session:1', ('namespace', 'session_id', 'open_ns', 'close_ns'),
                rule='actual supplied governed sessions and decoded context'),
            InputRequirement('window', 'window:1', ('governed_sessions', 'target_session_id'),
                rule='actual ordered slots, selected anchor and independent closure'),
            InputRequirement('availability', 'availability:1', ('market_cutoff_ns', 'knowledge_cutoff_ns', 'evaluation_ns'),
                rule='actual consumed bounds/known-at; no unrelated-field readiness requirement'),
        )


def _admit_money_flow(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    if task.max_input_bytes > 1048576 or task.math_policy_version != 'v1':
        fail(CommandErrorCode.LIMIT if task.max_input_bytes > 1048576 else CommandErrorCode.RESULT)
    if task.family != PRODUCER or task.ownership != 'serialized_destination':
        fail()
    selected = _selection(tuple(h.feature_id for h in task.features))
    context, _, _ = _context(task.config, selected)
    if task.features != money_flow_headers(task.config, selected) or task.instruments != (context.instrument_id,):
        fail(CommandErrorCode.RESULT)
    sessions = tuple((s.session_id, s.open_ns, s.close_ns) for s in context.sessions)
    if tuple((s.name, s.start_ns, s.end_ns) for s in task.governed_sessions) != sessions:
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != money_flow_history(task.config, selected):
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


def compute_money_flow_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_money_flow(task, batches)
        selected = _selection(tuple(h.feature_id for h in task.features))
        context, _, limit = _context(task.config, selected)
        # Shared per-bar preparation and independent readiness are the library's; nothing is computed here.
        result = library.compute_money_flow(batches or None, context, task.config, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'intraday.python', library.__version__)
        if (result.metadata.evidence_limit != limit or len(result.values) != len(selected)
                or len(result.quality) != len(selected) or len(result.evidence) > 2 * len(selected)
                or any(len(column.values) != 1 for column in result.values)):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef2_money_flow_work(config: ConfigSpec, *, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    with translated():
        selected = _selection(features)
        context, _, _ = _context(config, selected)
        history = money_flow_history(config, selected)
        task = TaskManifest(job_id, generation_id, partition_id, PRODUCER, (instrument_id,), config,
            money_flow_headers(config, selected), inputs,
            tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in context.sessions),
            destination_scope, 'serialized_destination', max(1, len(batches)), max_input_bytes,
            history[0], history[1], history[2])
        _admit_money_flow(task, batches)
        return WorkItem(task, batches, compute_money_flow_inputs)


__all__ = ['money_flow_headers', 'money_flow_history', 'required_money_flow_inputs',
    'compute_money_flow_inputs', 'make_ef2_money_flow_work']
