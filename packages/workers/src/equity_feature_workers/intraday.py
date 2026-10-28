"""Optional supplied-only intraday operation; import explicitly with the extra."""
from __future__ import annotations

from fractions import Fraction

from equity_feature_contracts import CanonicalBatch, ConfigSpec, DataKind, FeatureResult, InputRequirement, ValueType
from equity_feature_io_contracts import FeatureHeader
from equity_features_intraday import (
    IntradayContext, build_intraday_config, compute_intraday, describe_intraday, prepare_intraday,
)

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import TaskManifest
from ._versions import INTRADAY_VERSION


def _selection(features: tuple[str, ...]) -> tuple[str, ...]:
    catalog = {entry.feature_id: entry for entry in describe_intraday()}
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in catalog for f in features)):
        fail()
    return tuple(sorted(features))


def _context(config: ConfigSpec, features: tuple[str, ...]) -> tuple[IntradayContext, int]:
    if type(config) is not ConfigSpec:
        fail()
    values = {p.name: p.value for p in config.parameters}
    count = values.get('context_chunks')
    if type(count) is not int or not 1 <= count <= 16:
        fail()
    names = {'period', 'k_num', 'k_den', 'evidence_limit', 'context_schema', 'context_sha256', 'context_chunks'}
    if set(values) != names | {f'context_{n:03}' for n in range(count)}:
        fail()
    chunks = tuple(values[f'context_{n:03}'] for n in range(count))
    if any(type(chunk) is not str or not 1 <= len(chunk) <= 4096 or (n < count - 1 and len(chunk) != 4096) for n, chunk in enumerate(chunks)):
        fail()
    # Type narrowing is deliberate: Config Parameter values are a closed union.
    text = ''.join(chunk for chunk in chunks if isinstance(chunk, str))
    context = IntradayContext.from_json(text)
    period, numerator, denominator, limit = (values[n] for n in ('period', 'k_num', 'k_den', 'evidence_limit'))
    if (type(period) is not int or type(numerator) is not int or type(denominator) is not int
            or type(limit) is not int or not 1 <= numerator <= 1000000 or not 1 <= denominator <= 1000000
            or not 0 <= limit <= 10000 or config.price_unit is None):
        fail()
    multiplier = Fraction(numerator, denominator)
    if (multiplier.numerator, multiplier.denominator) != (numerator, denominator):
        fail()
    catalog = {entry.feature_id: entry for entry in describe_intraday()}
    if any(not catalog[f].minimum_period <= period <= (4095 if f in ('intraday.rsi', 'intraday.atr') else 4096) for f in features):
        fail()
    expected = build_intraday_config(context, period=period, band_k=multiplier,
        availability=config.availability, price_unit=config.price_unit, adjustment=config.adjustment, evidence_limit=limit)
    if config != expected:
        fail(CommandErrorCode.RESULT)
    return context, period


def intraday_headers(features: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    with translated():
        selected = _selection(features)
        catalog = {entry.feature_id: entry for entry in describe_intraday()}
        return tuple(FeatureHeader(f, catalog[f].algorithm_version, ValueType.FLOAT64, catalog[f].unit, '1') for f in selected)


def _history(context: IntradayContext, period: int, features: tuple[str, ...]) -> tuple[bool, tuple[str, ...], str]:
    anchored = any(f in ('intraday.ema', 'intraday.rsi', 'intraday.atr') for f in features)
    start = context.anchor_index if anchored else max(context.anchor_index, context.target_index - period + 1)
    slots = context.slots()
    target = slots[-1].session_id
    touched = {slot.session_id for slot in slots[start:context.target_index + 1] if slot.session_id != target}
    return True, tuple(s.session_id for s in context.sessions if s.session_id in touched), context.digest


def intraday_history(config: ConfigSpec, features: tuple[str, ...]) -> tuple[bool, tuple[str, ...], str]:
    with translated():
        selected = _selection(features)
        context, period = _context(config, selected)
        return _history(context, period, selected)


def required_intraday_inputs(config: ConfigSpec, features: tuple[str, ...]) -> tuple[InputRequirement, ...]:
    with translated():
        selected = _selection(features)
        _, period = _context(config, selected)
        fields = ['instrument_id', 'session_id', 'start_ns', 'end_ns', 'known_at_ns']
        fields.append('close')  # ATR consumes prior closes, never requires target close.
        if 'intraday.atr' in selected:
            fields.extend(('high', 'low'))
        rules = []
        for feature in selected:
            rules.append(f'{feature}: ' + (
                f'last {period} closes within anchor through target only' if feature.startswith('intraday.band_') else
                f'{period} true ranges from current H/L and previous-slot close; target close unnecessary; anchor recurrence' if feature == 'intraday.atr' else
                f'{period + 1} seed closes and anchor-to-target recurrence' if feature == 'intraday.rsi' else
                f'{period} seed closes and anchor-to-target recurrence'))
        return (
            InputRequirement('bars', 'canonical:1', tuple(fields), DataKind.BAR, rule='; '.join(rules) + '; null/absent delivery remains per-feature quality, not global rejection'),
            InputRequirement('session', 'session:1', ('namespace', 'session_id', 'open_ns', 'close_ns'), rule='actual supplied governed sessions and decoded context'),
            InputRequirement('window', 'window:1', ('governed_sessions', 'target_session_id'), rule='actual ordered slots, selected anchor and independent closure'),
            InputRequirement('availability', 'availability:1', ('market_cutoff_ns', 'knowledge_cutoff_ns', 'evaluation_ns'), rule='actual consumed bounds/known-at; no unrelated-field readiness requirement'),
        )


def compute_intraday_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        admit(task, batches)
        if task.family != 'intraday':
            fail()
        selected = _selection(tuple(h.feature_id for h in task.features))
        context, period = _context(task.config, selected)
        if task.features != intraday_headers(selected) or task.instruments != (context.instrument_id,):
            fail(CommandErrorCode.RESULT)
        sessions = tuple((s.session_id, s.open_ns, s.close_ns) for s in context.sessions)
        if tuple((s.name, s.start_ns, s.end_ns) for s in task.governed_sessions) != sessions:
            fail(CommandErrorCode.RESULT)
        if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != _history(context, period, selected):
            fail(CommandErrorCode.RESULT)
        missing = not batches and len(task.inputs) == 1 and task.inputs[0].role == 'bars' and task.inputs[0].state == 'missing'
        if not missing:
            if tuple(i.role for i in task.inputs) != tuple(f'bars:{n:03}' for n in range(len(batches))) or any(i.binding is None for i in task.inputs):
                fail()
        if missing:
            result = compute_intraday(None, context, task.config, features=selected)
        else:
            requirements = tuple(sorted({'price_moments' if f.startswith('intraday.band_') else 'true_ranges' if f == 'intraday.atr' else 'anchored_closes' for f in selected}))
            preparation = prepare_intraday(batches, context, task.config, requirements=requirements)
            result = compute_intraday(batches, context, task.config, features=selected, preparation=preparation)
        result_identity(task, result, 'intraday.python', INTRADAY_VERSION)
        return (result,)


__all__ = ['compute_intraday_inputs', 'intraday_headers', 'required_intraday_inputs', 'intraday_history']
