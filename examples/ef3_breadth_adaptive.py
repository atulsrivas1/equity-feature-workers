"""Synthetic contract fixtures -> EF3 breadth/adaptive worker -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture: 49 declared-universe breadth fixtures (ef3-breadth1), 20 observed-breadth
fixtures (ef3-obsbreadth1) and 30 adaptive-volume fixtures (ef3-adaptive1), 99 in total under their contract ids
(AD06 prefix, AD07a to AD07c included). Each fixture is run once per selected output (one-output selection) and once
jointly when it selects several outputs; every run goes through the I/O adapter, the worker, the pure library, a
separate DuckDB sink, SDK readback, idempotent replay and close/reopen. ``--refusals`` drives the 62 typed refusal fixtures (RB01-RB30,
RO01-RO15, RA01-RA17) through the same adapter and worker; the contract JSON stores only their error codes and
descriptions, so each violation is built here on a valid contract fixture input.

The adapter (``equity_feature_duckdb.breadth_adaptive``) maps the supplied fixture rows into REFERENCE batches and the
configuration envelopes; this script compares only against the frozen contract literal, never against the library.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import asdict
from html import escape
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_io_contracts import SinkRequirements
from equity_feature_workers import InputManifest

CONTRACT = 'BREADTH_ADAPTIVE_CONTRACT_PROPOSAL.json'
GROUPS = {'declared_fixtures': 'session_breadth', 'observed_fixtures': 'observed_breadth',
          'adaptive_fixtures': 'adaptive_volume'}
RESULT_COUNT = 99
ROLES = {
    'session_breadth': ('population', 'prior_reference_availability', 'current_return', 'cumulative_volume',
                        'endpoint_price', 'vwap', 'momentum', 'selected_high', 'selected_low'),
    'observed_breadth': ('members', 'change', 'qualifier_price', 'qualifier_volume'),
    'adaptive_volume': ('observations',),
}


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef3' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def fixtures_by_id(document: dict) -> dict:
    """All 99 result fixtures by contract id, each tagged with its producer."""
    rows = {}
    for group, producer in GROUPS.items():
        for row in document[group]:
            if row['id'] in rows:
                raise ValueError('duplicate contract id ' + row['id'])
            rows[row['id']] = dict(row, producer=producer, group_key=group)
    return rows


def accept(reference: dict, value) -> bool:
    """Value acceptance: a null literal matches null; a float literal is bit-exact (with its sign of zero)."""
    want = reference['value']
    if want is None or value is None:
        return want is None and value is None
    if isinstance(want, dict):
        r = float.fromhex(want['hex'])
        return type(value) is float and value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r)
    return type(value) is type(want) and value == want


def witness(row, producer: str) -> tuple:
    """Contract witness: (role, member) for breadth outputs and (sequence,) for adaptive observations."""
    if producer == 'adaptive_volume':
        # The library labels an observation witness 'seq:<sequence>'; the contract witness is the bare sequence.
        return (int(row.row_id[len('seq:'):]),)
    # The library labels a member witness as 'member:<id>'; the contract witness is the bare member id.
    member = row.row_id[len('member:'):] if row.row_id.startswith('member:') else row.row_id
    return (row.input_id, member)


def runs_for(fixture: dict) -> list[dict]:
    """Each run selects a set of outputs and carries the contract expectations for exactly those outputs."""
    expected = fixture['expected']
    runs = [dict(features=(feature,), expected={feature: expected[feature]}) for feature in sorted(expected)]
    if len(expected) > 1:
        runs.append(dict(features=tuple(sorted(expected)), expected=expected))
    return runs


def evidence_limit(fixture: dict) -> int:
    if fixture['producer'] == 'adaptive_volume':
        return fixture['input']['evidence_limit']
    return fixture['input']['params']['evidence_limit']


def case_for(fixture: dict):
    """The I/O adapter case of one contract fixture (declared, observed or adaptive)."""
    from equity_feature_duckdb import breadth_adaptive as adapter
    if fixture['producer'] == 'session_breadth':
        return adapter.declared_case(fixture['input'], entity_instrument='X')
    if fixture['producer'] == 'observed_breadth':
        return adapter.observed_case(fixture['input'], entity_instrument='X')
    return adapter.adaptive_case(fixture['input'], entity_instrument='X')


def declaration(role: str, batch) -> InputManifest:
    """Supplied-input manifest for one present role; an absent role is declared as missing with no binding."""
    raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
        InputBinding(role, batch.kind, batch.metadata))


def absent(role: str) -> InputManifest:
    return InputManifest(role, hashlib.sha256(('absent:' + role).encode('ascii')).hexdigest(), 'absent', 'missing', None)


def compare(result, fixture: dict, run: dict) -> list:
    """Values (acceptance rule), status, reasons, counts and witnesses against the contract literal for this run."""
    expected = run['expected']
    limit = evidence_limit(fixture)
    producer = fixture['producer']
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(expected)):
        problems.append(['selected outputs', [c.feature_id for c in result.values], sorted(expected)])
    found = {feature: [] for feature in expected}
    for row in result.evidence:
        found[row.feature_id].append(witness(row, producer))
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = expected[column.feature_id]
        stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['status'], want['reasons'], want['expected'], want['observed'])
        if not (accept(want, value) and stored == literal):
            problems.append([column.feature_id, 'value/quality', repr(value), stored, literal])
        if len(run['features']) == 1:
            want_w = [tuple(w) for w in want.get('evidence', [])]
            if found[column.feature_id] != want_w:
                problems.append([column.feature_id, 'witnesses', found[column.feature_id], want_w])
    if len(run['features']) > 1:
        want_joint = fixture.get('joint_selection_evidence') or [
            tuple(w) for feature in sorted(expected) for w in expected[feature].get('evidence', [])]
        want_joint = want_joint[:limit]
        flat = [witness(row, producer) for row in result.evidence]
        if flat != want_joint:
            problems.append(['joint evidence', flat, want_joint])
    return problems


def ordered(items, outcome) -> list:
    """Readbacks in item order, matched by task digest."""
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


def build_item(fixture: dict, run: dict, number: int, run_id: str, scope: str):
    """One worker item for one run. The task binds exactly the roles the selected outputs declare (library rule);
    supplied batches for other roles are not bound, because the library never reads them for this selection."""
    from equity_feature_workers.equity_breadth_adaptive import make_ef3_breadth_adaptive_work, required_roles
    label = '%s-%d' % (run_id, number)
    case = case_for(fixture)
    config = case.config(run['features'])
    roles = ROLES[fixture['producer']]
    needed = required_roles(config, run['features'])
    supplied = dict(zip(roles, case.slots))
    decls = tuple(declaration(role, supplied[role]) if supplied[role] is not None else absent(role) for role in needed)
    batches = tuple(supplied[role] for role in needed if supplied[role] is not None)
    return make_ef3_breadth_adaptive_work(config, features=run['features'], inputs=decls, batches=batches,
        instrument_id='X', job_id='job-' + label, generation_id='generation-' + label, partition_id='X-S',
        destination_scope=scope)


# ---- refusal fixtures RB01-RB30, RO01-RO15, RA01-RA17 through the worker path --------------------------------------
# The contract JSON stores only the error code and a description per refusal, so each case builds its violation on a
# valid contract fixture input (DB30 static declared, DB21 ordered-history declared, OB01 observed, AD06 adaptive).
# A case is driven through the I/O adapter (build), the worker (admission, then calculation) and the library directly;
# it matches only when the adapter or worker reports the contract code AND the library raises the same code. A case
# with several equivalent violations runs each as a variant and matches only when every variant matches. The cases that
# have no worker path (update/restore/merge: RB22, RO12, RA11; slot count: RB14) are checked against the library gate
# and reported with stage 'library_only'.

_REFUSAL_BASES = {'session_breadth': 'DB30', 'observed_breadth': 'OB01', 'adaptive_volume': 'AD06'}
_KEEP = object()


def _producer_of(rid: str) -> str:
    return {'RB': 'session_breadth', 'RO': 'observed_breadth', 'RA': 'adaptive_volume'}[rid[:2]]


def _facts(batch):
    from equity_feature_duckdb.breadth_adaptive import Fact
    c = {column.name: column.values for column in batch.columns}
    return [Fact(c['instrument_id'][i], c['reference_id'][i], c['fact_kind'][i], c['effective_start_ns'][i],
        c['effective_end_ns'][i], c['known_at_ns'][i], c['text'][i], c['price'][i], c['factor_num'][i],
        c['factor_den'][i]) for i in range(batch.row_count)]


def _remake(batch, facts, *, unit=_KEEP, snapshot=None):
    """The same role with explicit facts; the shared validators run inside, so this can refuse (adapter build)."""
    import dataclasses
    from equity_feature_duckdb import breadth_adaptive as ba
    meta = batch.metadata
    made = ba.reference_batch(facts, input_id=meta.source.input_id, complete=meta.coverage.complete,
        unit=meta.price_unit if unit is _KEEP else unit)
    if snapshot is not None:
        source = dataclasses.replace(made.metadata.source, snapshot_id=snapshot)
        made = dataclasses.replace(made, metadata=dataclasses.replace(made.metadata, source=source))
    return made


def _reversed(batch):
    """The same rows in reverse order, NOT validated here: the worker and the library must refuse the order."""
    import dataclasses
    from equity_feature_contracts import Column
    return dataclasses.replace(batch, columns=tuple(Column(c.name, c.values[::-1]) for c in batch.columns))


def _set_param(config, name, value):
    import dataclasses
    from equity_feature_contracts import Parameter
    params = tuple(Parameter(p.name, value) if p.name == name else p for p in config.parameters)
    return dataclasses.replace(config, parameters=params)


def _drop_param(config, name):
    import dataclasses
    return dataclasses.replace(config, parameters=tuple(p for p in config.parameters if p.name != name))


class _Base:
    """One valid request of a producer, mutable by the refusal builders (compact input, slots, config, features)."""

    def __init__(self, document: dict, producer: str, base: str | None = None, edit=None):
        import copy
        fixtures = fixtures_by_id(document)
        fixture = fixtures[base or _REFUSAL_BASES[producer]]
        inp = copy.deepcopy(fixture['input'])
        if edit is not None:
            edit(inp)
        self.producer = producer
        self.case = case_for(dict(fixture, input=inp))
        self.roles = ROLES[producer]
        self.slots = dict(zip(self.roles, self.case.slots))
        self.features = tuple(sorted(fixture['expected']))
        self.config = None
        self.capability = None
        self.library_only = None

    def with_slot(self, role, batch):
        self.slots[role] = batch
        return self

    def built_config(self):
        return self.config if self.config is not None else self.case.config(self.features)


def _declared_refusal(document: dict, rid: str) -> list:
    from equity_feature_duckdb import breadth_adaptive as ba
    from equity_feature_duckdb.breadth_adaptive import Fact

    def base(**kw):
        return _Base(document, 'session_breadth', **kw)

    def member_fact(batch, member):
        return next(f for f in _facts(batch) if f.instrument_id == member)

    def edited(role, change, **kw):
        spec = base(**kw)
        facts = _facts(spec.slots[role])
        change(facts)
        return spec.with_slot(role, _remake(spec.slots[role], facts))

    def param(name, value, **kw):
        spec = base(**kw)
        spec.config = _set_param(spec.built_config(), name, value)
        return spec

    def tol(which, pair):
        def edit(inp):
            inp['params'][which] = pair
        return base(edit=edit)

    if rid == 'RB01':
        return [edited('population', lambda f: f.append(f[0]))]
    if rid == 'RB02':
        return [edited('current_return', lambda f: f.append(f[0]))]
    if rid == 'RB03':
        def overlap(facts):
            facts.append(dataclasses_replace(facts[0], reference_id=ba.reference('bh1', ('t', 't', 'd6'),
                (ba.POPULATION_ID, ba.POPULATION_REVISION, 9))))
        return [edited('population', overlap, base='DB21')]
    if rid == 'RB04':
        return [edited('current_return', lambda f: f.append(dataclasses_replace(f[0], instrument_id='z')))]
    if rid == 'RB05':
        return [edited('current_return', lambda f: f.__setitem__(0, dataclasses_replace(f[0],
            reference_id=ba.reference('bf1', ('t',), ('OTHER-POP',)))))]
    if rid == 'RB06':
        spec = base()
        return [spec.with_slot('current_return', _remake(spec.slots['current_return'],
            _facts(spec.slots['current_return']), snapshot='snap-2'))]
    if rid == 'RB07':
        def contradict(inp):
            inp['roles']['pra']['rows']['a'] = {'k': 'unavailable'}
        return [base(edit=contradict)]
    if rid == 'RB08':
        spec = base(base='DB21')
        pop = _facts(spec.slots['population'])
        pop.append(dataclasses_replace(pop[1], reference_id=ba.reference('bh1', ('t', 't', 'd6'),
            (ba.POPULATION_ID, ba.POPULATION_REVISION, 1)), start_ns=pop[1].end_ns + 1 if pop[1].end_ns else 85,
            end_ns=None))
        return [spec.with_slot('population', _reversed(_remake(spec.slots['population'], pop)))]
    if rid == 'RB09':
        def negative(inp):
            inp['roles']['vol']['rows']['a'] = {'v': -5}
        return [base(edit=negative)]
    if rid == 'RB10':
        return [edited('vwap', lambda f: f.__setitem__(0, dataclasses_replace(f[0], factor_den=0)))]
    if rid == 'RB11':
        return [edited('population', lambda f: f.__setitem__(0, dataclasses_replace(f[0], end_ns=f[0].start_ns)),
            base='DB21')]
    if rid == 'RB12':
        spec = base()
        return [spec.with_slot('current_return', _remake(spec.slots['current_return'],
            _facts(spec.slots['current_return']), unit=ba.price_unit(3)))]
    if rid == 'RB13':
        return [edited('current_return', lambda f: f.__setitem__(0, dataclasses_replace(f[0],
            fact_kind=ba.CUMULATIVE_VOLUME_KIND)))]
    if rid == 'RB14':
        spec = base()
        spec.library_only = ('slot_count', [tuple(spec.slots.values())[:8], tuple(spec.slots.values()) + (None,)])
        return [spec]
    if rid == 'RB15':
        out = []
        for role, value in (('endpoint_price', 0), ('selected_high', -1), ('selected_low', 0)):
            short = {'endpoint_price': 'ep', 'selected_high': 'hi', 'selected_low': 'lo'}[role]
            def edit(inp, short=short, value=value):
                inp['roles'][short]['rows']['a'] = {'v': value}
            out.append(base(edit=edit))
        return out
    if rid == 'RB16':
        return [tol('near_high_tol', [5, 4])]
    if rid == 'RB17':
        return [tol('near_low_tol', [2, 4])]
    if rid == 'RB18':
        supplied = base()  # tolerances in the envelope, outputs not selected
        supplied.features = tuple(f for f in supplied.features if 'near_' not in f)
        supplied.config = supplied.case.config(_all_declared(document))
        missing = base()  # near_high selected, tolerance absent from the envelope
        missing.config = missing.case.config(tuple(f for f in missing.features if 'near_' not in f))
        return [supplied, missing]
    if rid == 'RB19':
        return [param('membership_profile', 'rolling')]
    if rid == 'RB20':
        return [param('policy_version', 'ef3-breadth2')]
    if rid == 'RB21':
        spec = base()
        spec.features = ('breadth.session.not_a_feature',)
        return [spec]
    if rid == 'RB22':
        spec = base()
        spec.capability = ('breadth.session.selected_member_count', 'update')
        return [spec]
    if rid in ('RB23', 'RB27'):
        sign = 1 if rid == 'RB23' else -1

        def big(inp):
            inp['roles']['ret']['rows'] = {'a': {'v': sign}, 'b': {'v': sign}, 'c': {'v': 0}, 'd': {'v': 0}}
            inp['roles']['vol']['rows'] = {'a': {'v': 2 ** 62}, 'b': {'v': 2 ** 62}, 'c': {'v': 1}, 'd': {'v': 1}}
        return [base(edit=big)]
    if rid == 'RB24':
        def many(inp):
            inp['roles']['population']['rows'] = [{'m': 'm%04d' % i, 'e': 'E', 's': 0, 'end': None, 'ka': 60, 'stint': 0}
                for i in range(4097)]
        return [base(edit=many)]
    if rid == 'RB25':
        return [edited('population', lambda f: f.__setitem__(0, dataclasses_replace(f[0], start_ns=5)))]
    if rid == 'RB26':
        return [edited('vwap', lambda f: f.__setitem__(0, dataclasses_replace(f[0], factor_num=None))),
            edited('vwap', lambda f: f.__setitem__(0, dataclasses_replace(f[0], factor_den=None)))]
    if rid == 'RB28':
        return [edited('vwap', lambda f: f.__setitem__(0, dataclasses_replace(f[0], price=0))),
            edited('vwap', lambda f: f.__setitem__(0, dataclasses_replace(f[0], price=-1)))]
    if rid == 'RB29':
        return [param('evidence_limit', 10001)]
    if rid == 'RB30':
        spec = base()
        import dataclasses
        population = spec.slots['population']  # the unit is dropped after the build, as a source would deliver it
        return [spec.with_slot('population', dataclasses.replace(population,
            metadata=dataclasses.replace(population.metadata, price_unit=None)))]
    raise KeyError(rid)


def dataclasses_replace(value, **changes):
    import dataclasses
    return dataclasses.replace(value, **changes)


def _all_declared(document: dict) -> tuple:
    return tuple(sorted(fixtures_by_id(document)['DB30']['expected']))


def _observed_refusal(document: dict, rid: str) -> list:
    from equity_feature_duckdb import breadth_adaptive as ba

    def base(**kw):
        return _Base(document, 'observed_breadth', **kw)

    def edited(role, change):
        spec = base()
        facts = _facts(spec.slots[role])
        change(facts)
        return spec.with_slot(role, _remake(spec.slots[role], facts))

    def param(name, value):
        spec = base()
        spec.config = _set_param(spec.built_config(), name, value)
        return spec

    def params(**values):
        def edit(inp):
            inp['params'].update(values)
        return base(edit=edit)

    if rid == 'RO01':
        return [edited('members', lambda f: f.append(f[0]))]
    if rid == 'RO02':
        def stray(inp):
            inp['roles']['chg']['rows']['z'] = {'v': 1}
        return [base(edit=stray)]
    if rid == 'RO03':
        return [params(scope='group')]
    if rid == 'RO04':
        return [params(group_label='g1')]
    if rid == 'RO05':
        return [params(floor_price=[4, 2]), params(floor_price=[-1, 1])]
    if rid == 'RO06':
        return [params(floor_volume=-1), params(floor_volume=1.5)]
    if rid == 'RO07':
        return [edited('members', lambda f: f.__setitem__(0, dataclasses_replace(f[0], fact_kind=ba.POPULATION_KIND)))]
    if rid == 'RO08':
        return [edited('members', lambda f: f.__setitem__(0, dataclasses_replace(f[0],
            reference_id=ba.reference('ob1', ('t',), ('OTHER-ROSTER',)))))]
    if rid == 'RO09':
        def negative(inp):
            inp['roles']['qv']['rows']['a'] = {'v': -1}
        return [base(edit=negative)]
    if rid == 'RO10':
        spec = base()
        return [spec.with_slot('qualifier_price', _remake(spec.slots['qualifier_price'],
            _facts(spec.slots['qualifier_price']), unit=ba.price_unit(3)))]
    if rid == 'RO11':
        return [param('policy_version', 'ef3-obsbreadth2')]
    if rid == 'RO12':
        spec = base()
        spec.capability = ('breadth.observed.observed_member_count', 'merge')
        return [spec]
    if rid == 'RO13':
        def many(inp):
            inp['roles']['members']['rows'] = {'m%04d' % i: {'g': 'g1'} for i in range(4097)}
        return [base(edit=many)]
    if rid == 'RO14':
        def zero(inp):
            inp['roles']['qp']['rows']['a'] = {'v': 0}
        return [base(edit=zero)]
    if rid == 'RO15':
        return [param('evidence_limit', 10001)]
    raise KeyError(rid)


def _adaptive_refusal(document: dict, rid: str) -> list:
    from equity_feature_duckdb import breadth_adaptive as ba

    def base(**kw):
        return _Base(document, 'adaptive_volume', **kw)

    def edited(change):
        spec = base()
        facts = _facts(spec.slots['observations'])
        change(facts)
        return spec.with_slot('observations', _remake(spec.slots['observations'], facts))

    def param(name, value):
        spec = base()
        spec.config = _set_param(spec.built_config(), name, value)
        return spec

    def rows(*specs):
        """Observation rows (sequence, time, rate) of the compact input."""
        def edit(inp):
            inp['obs']['rows'] = [{'q': q, 't': t, 'rate': rate} for q, t, rate in specs]
        return base(edit=edit)

    def ref(epoch, seq, event):
        return ba.reference('ao1', ('t', 'd19', 't'), (epoch, seq, event))

    if rid == 'RA01':
        return [edited(lambda f: f.append(f[0]))]
    if rid == 'RA02':
        return [edited(lambda f: f.__setitem__(slice(None), [
            dataclasses_replace(f[0], reference_id=ref('e1', 2, 'evA'), start_ns=1000000000),
            dataclasses_replace(f[0], reference_id=ref('e1', 2, 'evB'), start_ns=2000000000)]))]
    if rid == 'RA03':
        return [rows((3, 1000000000, [8, 1, 1]), (2, 2000000000, [8, 1, 1]))]
    if rid == 'RA04':
        return [edited(lambda f: f.__setitem__(0, dataclasses_replace(f[0], reference_id=ref('e2', 1, 'ev1'))))]
    if rid == 'RA05':
        return [rows((1, 1000000000, [-1, 1, 1]), (2, 2000000000, [8, 1, 1]))]
    if rid == 'RA06':
        return [rows((1, 1000000000, [8, 1, 0]), (2, 2000000000, [8, 1, 1]))]
    if rid == 'RA07':
        return [edited(lambda f: f.__setitem__(0, dataclasses_replace(f[0], price=8, factor_num=1, factor_den=None)))]
    if rid == 'RA08':
        spec = base(edit=lambda inp: inp.__setitem__('tau_ns', 0))
        return [spec]
    if rid == 'RA09':
        return [base(edit=lambda inp: inp.__setitem__('W', 0)), base(edit=lambda inp: inp.__setitem__('W', 4097))]
    if rid == 'RA10':
        return [param('policy_version', 'ef3-adaptive2')]
    if rid == 'RA11':
        spec = base()
        spec.capability = ('adaptive.volume.latest_event_ns', 'restore')
        return [spec]
    if rid == 'RA12':
        return [edited(lambda f: f.__setitem__(0, dataclasses_replace(f[0], fact_kind=ba.CURRENT_RETURN_KIND)))]
    if rid == 'RA13':
        return [edited(lambda f: f.__setitem__(0, dataclasses_replace(f[0], text='other-snapshot')))]
    if rid == 'RA14':
        spec = base()
        return [spec.with_slot('observations', _reversed(spec.slots['observations']))]
    if rid == 'RA15':
        def many(inp):
            inp['obs']['rows'] = [{'q': i + 1, 't': (i + 1) * 1000000, 'rate': [8, 1, 1]} for i in range(4097)]
        return [base(edit=many)]
    if rid == 'RA16':
        return [base(edit=lambda inp: inp.__setitem__('tau_ns', 2 ** 62 + 1))]
    if rid == 'RA17':
        return [base(edit=lambda inp: inp.__setitem__('evidence_limit', 10001))]
    raise KeyError(rid)


def build_refusal(document: dict, rid: str) -> list:
    return {'session_breadth': _declared_refusal, 'observed_breadth': _observed_refusal,
        'adaptive_volume': _adaptive_refusal}[_producer_of(rid)](document, rid)


def _library_slots(spec, needed):
    return tuple(spec.slots[role] if role in needed else None for role in spec.roles)


def _library_code(spec, needed) -> str | None:
    from equity_feature_contracts import ContractError, EntityKey
    from equity_features import ef3
    try:
        ef3.compute_ef3_breadth(_library_slots(spec, needed), spec.built_config(),
            entity=EntityKey('X', ba_session_id()), features=tuple(sorted(spec.features)))
        return None
    except ContractError as error:
        return error.code.value


def ba_session_id() -> str:
    from equity_feature_duckdb import breadth_adaptive as ba
    return ba.SESSION_ID


def _library_gate(spec) -> str | None:
    """Cases that have no worker path: the library capability gate or its fixed slot count."""
    from equity_feature_contracts import ContractError, EntityKey
    from equity_features import ef3
    try:
        if spec.capability is not None:
            ef3.require_ef3_breadth_capability(*spec.capability)
        else:
            for slots in spec.library_only[1]:
                ef3.compute_ef3_breadth(slots, spec.built_config(), entity=EntityKey('X', ba_session_id()),
                    features=tuple(sorted(spec.features)))
    except ContractError as error:
        return error.code.value
    return None


def _drive(spec, rid: str, want: str) -> dict:
    """One variant: adapter (already built), worker admission, worker calculation, library; the stage and codes."""
    from equity_feature_contracts import ContractError
    from equity_feature_workers.commands import CommandError
    from equity_feature_workers.equity_breadth_adaptive import make_ef3_breadth_adaptive_work, required_roles
    record = {'stage': None, 'adapter_code': None, 'worker_code': None, 'worker_contract_code': None,
        'library_code': None}
    if spec.capability is not None or spec.library_only is not None:
        record.update(stage='library_only', library_code=_library_gate(spec))
        record['matched'] = record['library_code'] == want
        return record
    config = spec.built_config()
    needed = ()
    try:
        needed = required_roles(config, spec.features)
        decls = tuple(declaration(role, spec.slots[role]) if spec.slots[role] is not None else absent(role)
            for role in needed)
        batches = tuple(spec.slots[role] for role in needed if spec.slots[role] is not None)
        item = make_ef3_breadth_adaptive_work(config, features=spec.features, inputs=decls, batches=batches,
            instrument_id='X', job_id='job-' + rid, generation_id='generation-' + rid, partition_id='refusal',
            destination_scope='ef3-refusal')
    except CommandError as error:
        record.update(stage='worker_admission', worker_code=error.code.value, worker_contract_code=error.contract_code,
            library_code=_library_code(spec, needed or spec.roles))
        record['matched'] = record['worker_contract_code'] == want == record['library_code']
        return record
    except ContractError as error:
        record.update(stage='worker_admission_untranslated', adapter_code=error.code.value)
        record['matched'] = False
        return record
    try:
        item.calculate(item.task, item.batches)
        record.update(stage='no_refusal', matched=False)
    except CommandError as error:
        record.update(stage='compute', worker_code=error.code.value, worker_contract_code=error.contract_code,
            library_code=_library_code(spec, needed))
        record['matched'] = record['worker_contract_code'] == want == record['library_code']
    return record


def _run_one(document: dict, rid: str, fixture: dict) -> dict:
    from equity_feature_contracts import ContractError
    want = fixture['error_code'].lower()
    record = {'id': rid, 'contract_code': fixture['error_code'], 'variants': []}
    try:
        specs = build_refusal(document, rid)
    except ContractError as error:
        record['variants'].append({'stage': 'adapter_build', 'adapter_code': error.code.value,
            'matched': error.code.value == want})
        specs = []
    for index, spec in enumerate(specs):
        record['variants'].append(_drive(spec, '%s-%d' % (rid, index), want))
    first = record['variants'][0]
    record.update(stage=first['stage'] if len({v['stage'] for v in record['variants']}) == 1 else 'mixed',
        matched=all(v['matched'] for v in record['variants']))
    return record


def run_refusals(document: dict) -> list:
    """All 62 refusal fixtures in contract order (RB, RO, RA); each row names the stage and the codes per variant."""
    fixtures = {row['id']: row for row in document['refusal_fixtures']}
    if len(fixtures) != 62:
        raise ValueError('Contract refusal fixtures differ from the example case list')
    order = sorted(fixtures, key=lambda key: ('RB', 'RO', 'RA').index(key[:2]) * 100 + int(key[2:]))
    return [_run_one(document, rid, fixtures[rid]) for rid in order]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', help='contract result fixture id, e.g. DB01, OB01, AD06')
    parser.add_argument('--refusals', action='store_true',
        help='drive the 62 refusal fixtures RB01-RB30, RO01-RO15, RA01-RA17 through the worker')
    parser.add_argument('--output-db', type=Path)
    parser.add_argument('--html', type=Path)
    parser.add_argument('--run-id')
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    args = parser.parse_args(argv)
    if args.refusals:
        rows = run_refusals(contract_document(args.contract))
        print(json.dumps(rows, indent=1, sort_keys=True))
        print('refusals driven %d, matched %d' % (len(rows), sum(row['matched'] for row in rows)), file=sys.stderr)
        return 0 if all(row['matched'] for row in rows) else 1
    if not (args.case and args.output_db and args.html and args.run_id):
        parser.error('--case, --output-db, --html and --run-id are required for a result case')
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require new distinct absolute output paths, existing parents and bounded ASCII run identity')
    if len(str(args.output_db)) + 32 >= 260:
        parser.error('Output path exceeds bounded Windows sidecar headroom')
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.ef1_runner import run_ef3_breadth_adaptive

    fixtures = fixtures_by_id(contract_document(args.contract))
    if len(fixtures) != RESULT_COUNT or args.case not in fixtures:
        parser.error('Contract fixtures differ from the example case list')
    fixture = fixtures[args.case]
    family = fixture['producer']
    scope = 'ef3-' + family.replace('_', '-') + '-synthetic'
    runs = runs_for(fixture)
    items = [build_item(fixture, run, number, args.run_id, scope) for number, run in enumerate(runs)]
    limit = evidence_limit(fixture)
    # The sink refuses any requirement above its default limits (100 results, 10 MiB total, 100000 cells and rows).
    limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576,
        max_total_bytes=min(10485760, 1048576 * len(items)), max_result_cells=min(100000, 4 * len(items)),
        max_evidence_rows=min(100000, max(1, min(limit, 10000)) * len(items)))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef3_breadth_adaptive(tuple(items), sink, requirements=limits)
        if len(outcome.readbacks) != len(items) or not all(row.verified for row in outcome.readbacks):
            print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]}, sort_keys=True))
            return 1
        rows = ordered(items, outcome)
        stored = [row.results[0] for row in rows]
        problems = []
        for run, result in zip(runs, stored):
            found = compare(result, fixture, run)
            if found:
                problems.append(found)
        if problems:
            raise AssertionError('Calculated result differs from the contract fixture literal: ' + json.dumps(problems, default=str))
        sdk = b'\n'.join(encode_result(result) for result in stored)
        replay = run_ef3_breadth_adaptive(tuple(items), sink, requirements=limits)
        if (len(replay.readbacks) != len(items) or not all(row.verified for row in replay.readbacks)
                or [row.output for row in ordered(items, replay)] != [row.output for row in rows]):
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        for row, result in zip(rows, stored):
            restored = reopened.read(row.output.receipt)
            verify_receipt(row.output.receipt, row.output.envelope, restored)
            if tuple(map(encode_result, restored)) != (encode_result(result),):
                raise AssertionError('Full stored-result reopen differs')
    outputs = ', '.join(sorted(fixture['expected']))
    document_html = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF3 ' + escape(family) + '</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>EF3 ' + escape(family.replace('_', ' ')) + ' (contract fixture ' + escape(args.case) + ')</h1>'
        '<p>' + escape(fixture['description']) + '. Supplied adapter  ->  worker  ->  pure library  ->  separate DuckDB sink. '
        'Contract-literal match (value by acceptance rule, status, reasons, counts, witnesses and joint evidence), '
        'SDK readback, idempotent replay and close/reopen verified. Runs: ' + str(len(runs)) + '.</p>'
        '<p>Selected outputs across runs: ' + escape(outputs) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(rows[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every count, ratio, proximity, rate, baseline and acceleration value is the '
        'library result. No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document_html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(runs), 'task_sha256': items[0].task.task_sha256,
        'content_sha256': rows[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
