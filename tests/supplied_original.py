"""Original fixture admission against installed pure APIs, before worker code."""
from dataclasses import asdict, replace
from fractions import Fraction
import hashlib
import sys
import argparse
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))  # tools/_versions.py
from _versions import FEATURES  # noqa: E402
import equity_feature_contracts as c
from equity_features.session import compute_bars, compute_structure, compute_top_k, compute_time_weighted
from equity_feature_io_sdk import encode_result, decode_result

ROOT = Path(__file__).resolve().parents[1]
SPEC = json.loads((ROOT / 'tests/supplied_original.json').read_text(encoding='utf-8'))
CHECKS = []
RESULTS = []


def check(label, condition):
    assert condition, label
    CHECKS.append(label)


def batch(kind, name, data, start, end, *, sampling='none', coverage=None, intervals=()):
    count = len(data['instrument_id'])
    return c.CanonicalBatch(kind, tuple(c.Column(k, tuple(v)) for k, v in data.items()),
        c.BatchMetadata('original', c.SourceBinding('synthetic', 'fixture1', 'map1', name),
            coverage or c.Coverage(count, count, True), c.PriceUnit(0, 'USD'),
            sampling=sampling, scope=c.InputScope(start, end, 'original-v1'),
            interval_coverage=intervals))


def config(end, *, intervals=(), parameters=()):
    return c.ConfigSpec('original-eq140', 'v1', (c.Parameter('eligibility_policy', 'original-v1'),) + parameters,
        c.SessionSpec('original', 'S', 0, end, 'supplied', intervals=intervals),
        c.WindowSpec(1, 'S', ('P', 'S')), c.AvailabilitySpec(end, end, end),
        price_unit=c.PriceUnit(0, 'USD'))


def record(name, result, cfg, inputs):
    check(name + ': SDK exact full-result roundtrip', decode_result(encode_result(result)) == result)
    check(name + ': exact config/availability', (result.metadata.config_digest, result.metadata.availability) == (cfg.digest, cfg.availability))
    check(name + ': exact actual bindings', result.metadata.inputs == tuple(sorted(inputs, key=lambda b: b.role)))
    check(name + ': actual core backend/version', (result.metadata.backend_id, result.metadata.backend_version) == ('python-exact-compensated' if name.startswith('continuous_') else 'python-exact', FEATURES))
    RESULTS.append({'case': name, 'result_sha256': hashlib.sha256(encode_result(result)).hexdigest(),
        'quality': [asdict(q) for q in result.quality], 'evidence': [asdict(e) for e in result.evidence]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    entity = c.EntityKey('A', 'S')
    base = SPEC['cases'][0]
    windows = tuple(c.IntervalSpec(k, *v) for k, v in base['intervals_ns'].items())
    rows = base['bars']
    data = dict(instrument_id=['A'] * 3, session_id=['S'] * 3,
        start_ns=[r['start'] for r in rows], end_ns=[r['end'] for r in rows],
        known_at_ns=[r['end'] for r in rows])
    data.update({key: [r[key] for r in rows] for key in ('open', 'high', 'low', 'close', 'volume', 'actual_notional')})
    bars = batch(c.DataKind.BAR, 'bars', data, 0, 30, intervals=tuple(
        c.IntervalCoverage(w.name, w.start_ns, w.end_ns, c.Coverage(n, n, True)) for w, n in zip(windows, (2, 1))))
    prior = batch(c.DataKind.DAILY, 'prior', dict(instrument_id=['A'], session_id=['P'],
        start_ns=[-30], end_ns=[0], close=[98], known_at_ns=[0]), -30, 0)
    cfg = config(30, intervals=windows)
    full = compute_bars(bars, cfg, entity=entity, prior_close=prior)
    expected = {'open': 100, 'high': 108, 'low': 99, 'close': 105, 'volume': 60,
        'notional': 6290, 'close_weighted_price': Fraction(631, 6), 'open_close_return': Fraction(1, 20),
        'range_fraction': Fraction(3, 35), 'close_location': Fraction(2, 3),
        'overnight_gap': Fraction(1, 49), 'close_close_return': Fraction(1, 14)}
    for col in full.values:
        name = col.feature_id.rsplit('.', 1)[1]
        check('bars: independent ' + name, col.values == (float(expected[name]),) if name not in ('volume', 'notional') else col.values == (expected[name],))
    check('bars: all twelve available', len(full.quality) == 12 and all(q.status == c.Status.AVAILABLE and q.reasons == () for q in full.quality))
    bindings = (c.InputBinding('bars', c.DataKind.BAR, bars.metadata), c.InputBinding('prior_close', c.DataKind.DAILY, prior.metadata))
    record('bars_prior_and_structure:bars', full, cfg, bindings)
    absent = compute_bars(bars, cfg, entity=entity)
    for col, q, original in zip(absent.values, absent.quality, full.values):
        is_prior = col.feature_id.rsplit('.', 1)[1] in ('overnight_gap', 'close_close_return')
        check('absent prior: ' + col.feature_id, (col.values == (None,) and q.status == c.Status.MISSING_INPUT and q.reasons == (c.Reason.ABSENT_INPUT,)) if is_prior else col == original and q.status == c.Status.AVAILABLE)
    record('bars_prior_absent', absent, cfg, bindings[:1])
    structure = compute_structure(bars, cfg, entity=entity)
    for row in structure.values[0].values[0].rows:
        check('structure: original ' + row.interval.name, [row.open_price, row.high_price, row.low_price, row.close_price, row.volume] == base['expected']['interval_ohlcv'][row.interval.name])
    check('structure: exact shares', [r.share for r in structure.values[1].values[0].rows] == [0.5, 0.5])
    record('bars_prior_and_structure:structure', structure, cfg, bindings[:1])
    partial = replace(bars, columns=tuple(c.Column(col.name, col.values[:2]) for col in bars.columns),
        metadata=replace(bars.metadata, coverage=c.Coverage(3, 2, False), interval_coverage=(
            bars.metadata.interval_coverage[0], c.IntervalCoverage('last', 20, 30, c.Coverage(1, 0, False)))))
    structure_gap = compute_structure(partial, cfg, entity=entity)
    first = structure_gap.values[0].values[0].rows[0]
    check('gap: independent first OHLCV remains available', (first.open_price, first.high_price, first.low_price, first.close_price, first.volume, first.quality.status) == (100, 108, 99, 106, 30, c.Status.AVAILABLE))
    check('gap: first share unavailable', structure_gap.values[1].values[0].rows[0].share is None)
    check('gap: first share exact cause', structure_gap.values[1].values[0].rows[0].quality.reasons == (c.Reason.GOVERNED_GAP,))
    record('structure_interval_independent_from_whole_session_gap', structure_gap, cfg, (c.InputBinding('bars', c.DataKind.BAR, partial.metadata),))
    trade_spec = SPEC['cases'][3]
    trades = trade_spec['trades']
    trade_data = dict(instrument_id=['A'] * 4, session_id=['S'] * 4,
        event_id=[r['id'] for r in trades], event_ns=[r['event'] for r in trades],
        order_key=[r['order'] for r in trades], known_at_ns=[r['event'] for r in trades])
    trade_data.update({key: [r[key] for r in trades] for key in ('price', 'size', 'eligible')})
    trade_batch = batch(c.DataKind.TRADE, 'trades', trade_data, 0, 30)
    top_cfg = config(30, parameters=(c.Parameter('top_k', 2), c.Parameter('evidence_limit', 2)))
    top = compute_top_k(trade_batch, top_cfg, entity=entity)
    check('topK: size/tie IDs', [r.event_id for r in top.values[0].values[0].rows] == trade_spec['expected_ids'])
    check('topK: exact evidence IDs', [r.row_id for r in top.evidence] == trade_spec['expected_evidence_ids'])
    check('topK: observed includes excluded row', (top.quality[0].expected, top.quality[0].observed, top.quality[0].status) == (4, 4, c.Status.AVAILABLE))
    record(trade_spec['id'], top, top_cfg, (c.InputBinding('trades', c.DataKind.TRADE, trade_batch.metadata),))
    quotes = batch(c.DataKind.QUOTE, 'quotes', dict(instrument_id=['A'] * 2, session_id=['S'] * 2,
        event_id=['Q1', 'Q2'], event_ns=[10, 50], order_key=[0, 0], known_at_ns=[10, 50],
        bid=[100, 102], ask=[104, 102]), 0, 100, sampling='continuous')
    seed = batch(c.DataKind.QUOTE, 'seed', dict(instrument_id=['A'], session_id=['S'],
        event_id=['SEED'], event_ns=[-10], order_key=[0], known_at_ns=[-10], bid=[99], ask=[101]), -10, 0, sampling='continuous')
    for spec in SPEC['cases'][4:]:
        initial = spec['initial_state']
        qcfg = config(100, parameters=(c.Parameter('max_age_ns', 30), c.Parameter('initial_state', initial)))
        result = compute_time_weighted(quotes, qcfg, entity=entity, seed=seed if initial == 'seed' else None)
        value = result.values[0].values[0]; oracle = spec['expected']
        check(initial + ': typed duration value retained', value is not None)
        check(initial + ': independent duration conservation', asdict(value.durations) == oracle['durations_ns'] and value.durations.total == 100)
        check(initial + ': exact status', result.quality[0].status.value == oracle['status'])
        for field, attr in (('spread', 'mean_spread'), ('bps', 'mean_bps')):
            expected_value = float(Fraction(oracle[field + '_fraction'])) if field + '_fraction' in oracle else None
            actual = getattr(value, attr)
            check(initial + ': independent ' + field, actual is None if expected_value is None else abs(actual - expected_value) <= 1e-12 + abs(expected_value) * 1e-12)
        if initial == 'seed':
            check('seed: original causal evidence', len(result.evidence) == 1 and result.evidence[0].row_id == 'SEED' and result.evidence[0].event_ns == -10 and result.evidence[0].use == 'consumed')
        elif initial == 'unknown':
            check('unknown: exact reasons', tuple(r.value for r in result.quality[0].reasons) == tuple(oracle['reasons']))
        qbindings = (c.InputBinding('quotes', c.DataKind.QUOTE, quotes.metadata),) + ((c.InputBinding('quote_seed', c.DataKind.QUOTE, seed.metadata),) if initial == 'seed' else ())
        record(spec['id'], result, qcfg, qbindings)
    report = {'scope': 'Original seven-profile pure-API/SDK fixture preflight only; no worker implementation, physical sink, performance or Ready acceptance.',
        'passed': True, 'assertions': len(CHECKS), 'checks': CHECKS, 'results': RESULTS}
    if args.output is not None:
        args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'passed': True, 'assertions': len(CHECKS), 'results': len(RESULTS)}))


if __name__ == '__main__':
    main()
