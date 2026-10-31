"""Explicit supplied structure/top-K/continuous/prior-close operations."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json

from equity_feature_contracts import (
    BAR_IDS, CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult, InputRequirement,
    IntervalSpec, ValueType, builtin_registry,
)
from equity_feature_io_contracts import FeatureHeader
from equity_features.session import compute_bars, compute_structure, compute_time_weighted, compute_top_k

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from ._versions import FEATURES_VERSION

# The closed 13-id bars header set is core's BAR_IDS, imported and never retyped (RL-01-14; ADR-1 allows workers to import core).
_BARS = BAR_IDS
_FEATURES = {'structure': ('session.structure.interval_ohlcv', 'session.structure.interval_volume_share'),
    'top_k': ('session.trade.top_k',), 'continuous_quotes': ('session.quote.time_weighted_spread',), 'bars_prior_close': _BARS}


def _family(family: str) -> tuple[str, ...]:
    if type(family) is not str or family not in _FEATURES:
        fail()
    return _FEATURES[family]


def supplied_headers(family: str) -> tuple[FeatureHeader, ...]:
    with translated():
        registry = builtin_registry()
        return tuple(FeatureHeader(f, registry.get(f).algorithm_version,
            ValueType(registry.get(f).outputs[0].dtype), registry.get(f).outputs[0].unit,
            registry.get(f).schema_version) for f in _family(family))


def _roles(config: ConfigSpec, family: str) -> tuple[str, ...]:
    _family(family)
    if type(config) is not ConfigSpec:
        fail()
    if family == 'continuous_quotes':
        initial = next((p.value for p in config.parameters if p.name == 'initial_state'), None)
        if initial not in ('seed', 'inactive', 'unknown'):
            fail()
        return ('quotes', 'quote_seed') if initial == 'seed' else ('quotes',)
    return ('bars', 'prior_close') if family == 'bars_prior_close' else ('trades',) if family == 'top_k' else ('bars',)


def supplied_history(config: ConfigSpec, family: str, inputs: tuple[InputManifest, ...],
                     governed_sessions: tuple[IntervalSpec, ...]) -> tuple[bool, tuple[str, ...], str | None]:
    with translated():
        roles = _roles(config, family)
        if (type(inputs) is not tuple or any(type(i) is not InputManifest for i in inputs)
                or tuple(i.role for i in inputs) != roles or type(governed_sessions) is not tuple
                or any(type(s) is not IntervalSpec for s in governed_sessions)):
            fail()
        if tuple(s.name for s in governed_sessions) != config.window.governed_sessions:
            fail(CommandErrorCode.RESULT)
        if family in ('structure', 'top_k'):
            return False, (), None
        warmup: tuple[str, ...] = ()
        if family == 'bars_prior_close' and inputs[1].binding is not None:
            index = config.window.governed_sessions.index(config.session.session_id)
            if index == 0:
                fail()
            warmup = (config.window.governed_sessions[index - 1],)
        payload = {'family': family, 'config_digest': config.digest,
            'governed_sessions': [[s.name, s.start_ns, s.end_ns] for s in governed_sessions],
            'inputs': [asdict(i) for i in inputs]}
        wire = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
        initialization = hashlib.sha256(b'efworker-supplied-init1\0' + wire).hexdigest()
        return True, warmup, initialization


def required_supplied_inputs(config: ConfigSpec, family: str) -> tuple[InputRequirement, ...]:
    with translated():
        roles = _roles(config, family)
        identity = ('instrument_id', 'session_id', 'known_at_ns')
        requirements: list[InputRequirement] = []
        for role in roles:
            if role in ('bars', 'prior_close'):
                fields = identity + ('start_ns', 'end_ns') + (('close',) if role == 'prior_close' else
                    ('open', 'high', 'low', 'close', 'volume') + (('actual_notional',) if family == 'bars_prior_close' else ()))
                rule = ('one actual immediately preceding governed DAILY row; only overnight/close-close need it; absent reference preserves independent bars' if role == 'prior_close' else
                    'actual BAR intervals/coverage; independent fields and interval-local OHLCV vs whole-session share denominator; optional values may be null/absent')
                requirements.append(InputRequirement(role, 'canonical:1', fields, DataKind.DAILY if role == 'prior_close' else DataKind.BAR, rule=rule))
            elif role == 'trades':
                requirements.append(InputRequirement(role, 'canonical:1', identity + ('event_ns', 'order_key', 'event_id', 'price', 'size', 'eligible'), DataKind.TRADE,
                    rule='actual eligibility; rank (-size,event_ns,order_key,event_id); 1<=top_k<=evidence_limit<=10000; missing payload follows pure quality'))
            else:
                rule = ('one actual target-session preopen update, original event/known-at expiry; absent seed means unknown until an update' if role == 'quote_seed' else
                    'continuous updates only; explicit max_age_ns/initial_state, exact causal duration and null-category semantics')
                requirements.append(InputRequirement(role, 'canonical:1', identity + ('event_ns', 'order_key', 'event_id', 'bid', 'ask'), DataKind.QUOTE, 'continuous', rule))
        requirements.extend((
            InputRequirement('session', 'session:1', ('namespace', 'session_id', 'open_ns', 'close_ns', 'intervals'), rule='actual caller-governed target and structure interval bounds'),
            InputRequirement('window', 'window:1', ('governed_sessions', 'target_session_id'), rule='actual ordered sessions; no fabricated predecessor or seed session'),
            InputRequirement('availability', 'availability:1', ('market_cutoff_ns', 'knowledge_cutoff_ns', 'evaluation_ns'), rule='causal accepted input and reference knowledge; unavailable data remains result quality'),
        ))
        return tuple(requirements)


def compute_supplied_session_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        admit(task, batches)
        family = task.family
        roles = _roles(task.config, family)
        if tuple(i.role for i in task.inputs) != roles:
            fail()
        if task.features != supplied_headers(family):
            fail(CommandErrorCode.RESULT)
        history = supplied_history(task.config, family, task.inputs, task.governed_sessions)
        if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != history:
            fail(CommandErrorCode.RESULT)
        actual = iter(batches)
        by_role = {i.role: next(actual) if i.binding is not None else None for i in task.inputs}
        for role, source in by_role.items():
            expected = DataKind.DAILY if role == 'prior_close' else DataKind.BAR if role == 'bars' else DataKind.TRADE if role == 'trades' else DataKind.QUOTE
            if source is not None and source.kind is not expected:
                fail()
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        if family == 'structure':
            result = compute_structure(by_role['bars'], task.config, entity=entity)
        elif family == 'top_k':
            result = compute_top_k(by_role['trades'], task.config, entity=entity)
        elif family == 'continuous_quotes':
            result = compute_time_weighted(by_role['quotes'], task.config, entity=entity, seed=by_role.get('quote_seed'))
        else:
            prior = by_role['prior_close']
            if prior is not None:
                index = task.config.window.governed_sessions.index(entity.session_id)
                if index == 0 or prior.row_count != 1:
                    fail()
                governed = task.governed_sessions[index - 1]
                session = prior.column('session_id'); start = prior.column('start_ns'); end = prior.column('end_ns')
                if (session is None or start is None or end is None or prior.metadata.scope is None
                        or session.values != (governed.name,) or start.values != (governed.start_ns,)
                        or end.values != (governed.end_ns,) or (prior.metadata.scope.start_ns, prior.metadata.scope.end_ns) != (governed.start_ns, governed.end_ns)
                        or governed.end_ns > task.config.session.open_ns):
                    fail(CommandErrorCode.RESULT)
            result = compute_bars(by_role['bars'], task.config, entity=entity, prior_close=prior)
        result_identity(task, result, 'python-exact-compensated' if family == 'continuous_quotes' else 'python-exact', FEATURES_VERSION)
        return (result,)


__all__ = ['compute_supplied_session_inputs', 'supplied_headers', 'required_supplied_inputs', 'supplied_history']
