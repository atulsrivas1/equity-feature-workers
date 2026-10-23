"""Synthetic adapter -> momentum worker -> NEW separate DuckDB -> verified HTML."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
import math
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

# The contract result fixtures (ef2-conv1): 97 single-target and the three prefix fixtures P01 to P03;
# E01 to E23 are refusals and are not run here.
CONTRACT = 'MOMENTUM_TREND_CONTRACT_PROPOSAL.json'
FIXTURE_COUNT = 100
DEFAULT_EVIDENCE_LIMIT = 10000  # contract default; the limit is result-wide


def contract_fixtures(override: Path | None) -> dict:
    """Result fixtures by id from the frozen contract JSON in the core repository."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef2' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return {row['id']: row for row in json.load(source)['fixtures']}


def case_ids() -> tuple[str, ...]:
    """Case ids for --case, read from the contract JSON (the example never carries expected values)."""
    return tuple(contract_fixtures(None))


def variants(fixture: dict) -> list[tuple[dict, dict]]:
    """(I/O selector keywords, expected outputs/evidence) per run; a prefix fixture is series x prefix length.

    A single-target fixture is one variant. A prefix fixture runs series A and B at every listed prefix
    length; each prefix case states the outputs once, valid for both series (the prefix property).
    """
    if 'prefix_cases' not in fixture:
        return [({}, fixture['expected'])]
    return [(dict(series=series, length=case['prefix_length']), dict(outputs=case['outputs']))
        for case in fixture['prefix_cases'] for series in ('A', 'B')]


def accept(rule: str, value: float | None, reference: dict | None) -> bool:
    """Fixture acceptance: tolerance abs(y-r) <= max(8*ulp(r), 1e-12*abs(r)), or exact +0.0 (bit-exact)."""
    if reference is None or value is None:
        return reference is None and value is None
    r = float.fromhex(reference['hex'])
    if rule == 'exact_plus_zero':
        return value.hex() == r.hex() and math.copysign(1.0, value) == 1.0
    return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))


def compare(result, packet, expected: dict) -> tuple[bool, list, bool]:
    """Compare value (by acceptance rule), status, reasons, counts and witnesses with the contract literal.

    Witnesses are [slot, use] pairs decoded through the packet's declared ``slot:N`` interval coverages
    (end time); where the packet exposes no slot geometry only the counts are compared.
    """
    outputs = expected['outputs']
    ends = {}
    for batch in packet.batches:
        for interval in batch.metadata.interval_coverage:
            if interval.name.startswith('slot:'):
                ends[interval.end_ns] = int(interval.name[5:])
    found = {feature: [] for feature in outputs}
    for row in result.evidence:
        found[row.feature_id].append([ends.get(row.event_ns, row.event_ns), row.use])
    checked = all(isinstance(slot, int) and slot in ends.values() for rows in found.values() for slot, _ in rows)
    checked = checked and (bool(ends) or not any(found.values()))
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(outputs)):
        problems.append('selected outputs')
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = outputs[column.feature_id]
        rule = (want.get('acceptance') or {}).get('rule', 'tolerance')
        stored = (value, quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['value'], want['status'], want['reasons'], want['expected'], want['observed'])
        if not (accept(rule, stored[0], literal[0]) and stored[1:] == literal[1:]):
            problems.append([column.feature_id, rule, stored, literal])
        if 'evidence_rows' not in expected:
            continue
        want_rows = [list(row) for row in expected['evidence_rows'].get(column.feature_id, ())]
        if (found[column.feature_id] if checked else len(found[column.feature_id])) != (want_rows if checked else len(want_rows)):
            problems.append([column.feature_id, 'witnesses', found[column.feature_id], want_rows])
    return not problems, problems, checked


def ordered(items, outcome) -> list:
    """Readbacks in item order, matched by task digest."""
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', required=True, help='contract result fixture id, e.g. M01, D04, T30, P01')
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    args = parser.parse_args(argv)
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require new distinct absolute output paths, existing parents and bounded ASCII run identity')
    if len(str(args.output_db)) + 32 >= 260:
        parser.error('Output path exceeds bounded Windows sidecar headroom')
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb.momentum import synthetic_momentum_fixture, synthetic_momentum_config, synthetic_momentum_features
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_momentum import _context, make_ef2_momentum_work
    from equity_feature_workers.ef1_runner import run_ef2_momentum
    from equity_features_intraday import build_momentum_config

    fixtures = contract_fixtures(args.contract)
    if len(fixtures) != FIXTURE_COUNT or args.case not in fixtures:
        parser.error('Contract result fixtures differ from the example case list')
    fixture = fixtures[args.case]
    limit = fixture.get('evidence_limit', DEFAULT_EVIDENCE_LIMIT)
    runs = variants(fixture)
    scope = 'ef2-momentum-synthetic'
    items, packets, features_by_run = [], [], []
    for number, (selector, expected) in enumerate(runs):
        features = tuple(sorted(synthetic_momentum_features(args.case)))
        if features != tuple(sorted(expected['outputs'])) or features != tuple(sorted(fixture.get('features', features))):
            parser.error('I/O feature selection differs from the contract fixture')
        packet = synthetic_momentum_fixture(args.case, **selector)
        config = synthetic_momentum_config(args.case, length=selector.get('length'))
        # The I/O synthetic config may carry a different evidence limit; the limit is a caller parameter
        # (result-wide), so rebuild the same config through the library builder with the fixture limit.
        context, settings = _context(config, features)
        periods = dict(zip(PERIOD_KEYS[settings.family], settings.periods))
        config = build_momentum_config(context, family=settings.family, **periods,
            availability=config.availability, adjustment=config.adjustment, price_unit=config.price_unit, evidence_limit=limit)
        declarations = []
        if packet.bars is None:
            declarations.append(InputManifest('bars', hashlib.sha256(b'synthetic-absent:bars').hexdigest(),
                'synthetic-absent', 'missing', None))
        for index, batch in enumerate(packet.bars or ()):
            role = 'bars:%03d' % index
            raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
            coverage = batch.metadata.coverage
            state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
            declarations.append(InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id,
                state, InputBinding(role, batch.kind, batch.metadata)))
        label = '%s-%d' % (args.run_id, number)
        items.append(make_ef2_momentum_work(config, features=features, inputs=tuple(declarations), batches=packet.batches,
            instrument_id='X', job_id='job-' + label, generation_id='generation-' + label,
            partition_id='X-S', destination_scope=scope))
        packets.append(packet)
        features_by_run.append(features)
    limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576, max_total_bytes=1048576 * len(items),
        max_result_cells=4 * len(items), max_evidence_rows=max(1, min(limit, 40960)) * len(items))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef2_momentum(tuple(items), sink, requirements=limits)
        if len(outcome.readbacks) != len(items) or not all(row.verified for row in outcome.readbacks):
            print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]}, sort_keys=True))
            return 1
        # The runner returns readbacks in its own order; pair them with the items by task digest.
        rows = ordered(items, outcome)
        stored = [row.results[0] for row in rows]
        identity_checked = True
        for (selector, expected), packet, result in zip(runs, packets, stored):
            matched, problems, checked = compare(result, packet, expected)
            identity_checked = identity_checked and checked
            if not matched:
                raise AssertionError('Calculated result differs from the contract fixture literal: ' + json.dumps(problems, default=str))
        if len(runs) > 1:
            # Prefix property: series A and B at one prefix length publish identical values/status/counts.
            for first, second in zip(stored[0::2], stored[1::2]):
                if [(c.values, c.feature_id) for c in first.values] != [(c.values, c.feature_id) for c in second.values] or first.quality != second.quality:
                    raise AssertionError('Prefix property violated between series A and B')
        sdk = b'\n'.join(encode_result(result) for result in stored)
        receipts = [row.output.receipt for row in rows]
        replay = run_ef2_momentum(tuple(items), sink, requirements=limits)
        if (len(replay.readbacks) != len(items) or not all(row.verified for row in replay.readbacks)
                or [row.output for row in ordered(items, replay)] != [row.output for row in rows]):
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        for row, result in zip(rows, stored):
            restored = reopened.read(row.output.receipt)
            verify_receipt(row.output.receipt, row.output.envelope, restored)
            if tuple(map(encode_result, restored)) != (encode_result(result),):
                raise AssertionError('Full stored-result reopen differs')
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF2 momentum</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Momentum and trend (MACD, DMI/ADX, stochastic, Williams %R)</h1><p>Contract fixture ' + escape(args.case)
        + ': ' + escape(fixture['description'])
        + '. Actual input adapter  ->  worker  ->  pure calculation  ->  separate DuckDB sink. '
        'Contract-literal match (value by the fixture acceptance rule, status, reasons, coverage, witnesses), SDK readback, idempotent replay and close/reopen verified.</p>'
        '<p>Family ' + escape(fixture['family']) + '; runs ' + str(len(items)) + '; selected outputs: '
        + escape(', '.join(features_by_run[0])) + '. Witness identity was '
        + ('checked by slot and use' if identity_checked else 'checked by count and order only') + '.</p>'
        '<p>The worker orchestrates only; every recurrence, seed, ratio and rounding is the library result. '
        'Original-source parity remains unqualified.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(receipts[0].content_sha256)
        + '.</p><h2>Complete canonical result, original bindings and selected witnesses</h2><pre>'
        + escape(sdk.decode('ascii')) + '</pre><p>No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'witness_identity_checked': identity_checked, 'runs': len(items),
        'task_sha256': items[0].task.task_sha256, 'content_sha256': receipts[0].content_sha256}, sort_keys=True))
    return 0


# Period keyword names per family (the library builder's closed keywords, in decoded-settings order).
PERIOD_KEYS = {'macd': ('fast_period', 'slow_period', 'signal_period'), 'dmi': ('di_period', 'adx_period'),
    'stochastic': ('k_period', 'k_slowing', 'd_period')}

if __name__ == '__main__':
    raise SystemExit(main())
