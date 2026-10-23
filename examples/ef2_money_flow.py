"""Synthetic adapter -> money-flow worker -> NEW separate DuckDB -> verified HTML."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

# The contract result fixtures (ef2-mf1); MR01 to MR20 are refusals and are not run here.
CASES = tuple('M%02d' % number for number in range(1, 37))
CONTRACT = 'MONEYFLOW_RETRACEMENT_CONTRACT_PROPOSAL.json'
ALL_FEATURES = ('intraday.ad_line', 'intraday.cmf', 'intraday.obv')


def contract_fixtures(override: Path | None) -> dict:
    """Money-flow result fixtures by id from the frozen contract JSON in the core repository."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef2' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return {row['id']: row for row in json.load(source)['money_flow_fixtures']}


def expected_projection(fixture: dict) -> tuple[dict, dict]:
    """Contract-literal outputs and witness slot lists, unchanged from the JSON fixture.

    Acceptance is bit-exact: int64 OBV exactly, float64 by float.hex (the contract states no tolerance).
    """
    outputs, witnesses = {}, {}
    for feature, row in fixture['expected'].items():
        value = row['value']
        outputs[feature] = dict(value=value['hex'] if isinstance(value, dict) else value, status=row['status'],
            reasons=list(row['reasons']), expected=row['expected'], observed=row['observed'])
        witnesses[feature] = list(row.get('witness_slots', ()))
    return outputs, witnesses


def project(result) -> dict:
    """Compact comparable projection of a stored result (value, status, reasons, counts)."""
    outputs = {}
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        if type(value) is float:
            value = value.hex()
        outputs[column.feature_id] = dict(value=value, status=quality.status.value,
            reasons=[reason.value for reason in quality.reasons], expected=quality.expected, observed=quality.observed)
    return outputs


def witnesses(result, packet, expected: dict) -> tuple[dict, bool]:
    """Evidence per output as ordered slot indexes where the packet exposes slot geometry, else counts.

    The slot index is the position of the consumed row's event time among the packet's declared
    ``slot:N`` interval coverages (end time); returns (witnesses, identity_checked).
    """
    ends = {}
    for batch in packet.batches:
        for interval in batch.metadata.interval_coverage:
            if interval.name.startswith('slot:'):
                ends[interval.end_ns] = int(interval.name[5:])
    found = {feature: [] for feature in expected}
    for row in result.evidence:
        found[row.feature_id].append(ends.get(row.event_ns, row.event_ns))
    exposed = bool(ends) and all(isinstance(slot, int) and slot in ends.values() for rows in found.values() for slot in rows)
    if not exposed:
        return {feature: len(rows) for feature, rows in found.items()}, False
    # A single-slot window (anchor == target) cites that slot once.
    return {feature: rows for feature, rows in found.items()}, True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=CASES, default=CASES[0])
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
    from equity_feature_duckdb.money_flow import synthetic_money_flow_fixture, synthetic_money_flow_config
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_money_flow import make_ef2_money_flow_work
    from equity_feature_workers.ef1_runner import run_ef2_money_flow

    fixtures = contract_fixtures(args.contract)
    if tuple(sorted(row for row in fixtures if row.startswith('M'))) != CASES:
        parser.error('Contract result fixtures differ from the example case list')
    fixture = fixtures[args.case]
    features = tuple(sorted(fixture.get('selected', ALL_FEATURES)))
    expected, expected_witnesses = expected_projection(fixture)
    if tuple(sorted(expected)) != features:
        parser.error('Contract expected outputs differ from the selected features')
    packet = synthetic_money_flow_fixture(case_id=args.case)
    config = synthetic_money_flow_config(case_id=args.case)
    declarations = []
    for role, batch in zip(packet.roles, packet.slots):
        if batch is None:
            declarations.append(InputManifest(role, hashlib.sha256(('synthetic-absent:' + role).encode('ascii')).hexdigest(),
                'synthetic-absent', 'missing', None))
        else:
            raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
            coverage = batch.metadata.coverage
            state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
            declarations.append(InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id,
                state, InputBinding(role, batch.kind, batch.metadata)))
    scope = 'ef2-money-flow-synthetic'
    item = make_ef2_money_flow_work(config, features=features, inputs=tuple(declarations), batches=packet.batches,
        instrument_id='X', job_id='job-' + args.run_id, generation_id='generation-' + args.run_id,
        partition_id='X-S', destination_scope=scope)
    limits = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
        max_result_cells=len(features), max_evidence_rows=2 * len(features))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef2_money_flow((item,), sink, requirements=limits)
        row, = outcome.readbacks
        if not row.verified:
            print(json.dumps({'verified_readback': False, 'reason': row.reason}, sort_keys=True))
            return 1
        result, = row.results
        found, identity_checked = witnesses(result, packet, expected)
        want = expected_witnesses if identity_checked else {feature: len(slots) for feature, slots in expected_witnesses.items()}
        if project(result) != expected or found != want:
            raise AssertionError('Calculated result differs from the contract fixture literal')
        obv = next((column.values[0] for column in result.values if column.feature_id == 'intraday.obv'), None)
        if obv is not None and type(obv) is not int:
            raise AssertionError('Stored OBV is not an exact int64')
        sdk = encode_result(result)
        receipt = row.output.receipt
        replay = run_ef2_money_flow((item,), sink, requirements=limits)
        if not replay.readbacks[0].verified or replay.readbacks[0].output != row.output:
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        restored = reopened.read(receipt)
        verify_receipt(receipt, row.output.envelope, restored)
        if tuple(map(encode_result, restored)) != (sdk,):
            raise AssertionError('Full stored-result reopen differs')
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF2 money flow</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Money flow (OBV, accumulation/distribution, Chaikin money flow)</h1><p>Contract fixture ' + escape(args.case)
        + ': ' + escape(fixture['description'])
        + '. Actual input adapter  ->  worker  ->  pure calculation  ->  separate DuckDB sink. '
        'Contract-literal match (value bit-exact, status, reasons, coverage, witnesses), SDK readback, idempotent replay and close/reopen verified.</p>'
        '<p>Selected outputs: ' + escape(', '.join(features)) + '. Witness identity was '
        + ('checked by slot' if identity_checked else 'checked by count and order only') + '.</p>'
        '<p>The worker orchestrates only; every multiplier, running total and ratio is the library result. '
        'The A/D line is origin-dependent. Original-source parity remains unqualified.</p>'
        '<p>Task ' + escape(item.task.task_sha256) + '; content ' + escape(receipt.content_sha256)
        + '.</p><h2>Complete canonical result, original bindings and selected witnesses</h2><pre>'
        + escape(sdk.decode('ascii')) + '</pre><p>No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'witness_identity_checked': identity_checked,
        'task_sha256': item.task.task_sha256, 'content_sha256': receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
