"""Synthetic adapter -> retracement-level worker -> NEW separate DuckDB -> verified HTML."""
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

# The contract result fixtures (ef2-retrace1); TR01 to TR18 are refusals and are not run here.
CASES = tuple('T%02d' % number for number in range(1, 33))
CONTRACT = 'MONEYFLOW_RETRACEMENT_CONTRACT_PROPOSAL.json'
FEATURE = 'retracement.level'


def contract_fixtures(override: Path | None) -> dict:
    """Retracement result fixtures by id from the frozen contract JSON in the core repository."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef2' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return {row['id']: row for row in json.load(source)['retracement_fixtures']}


def expected_projection(fixture: dict) -> tuple[dict, list]:
    """Contract-literal output and witness roles, unchanged from the JSON fixture.

    Acceptance is bit-exact: float64 by float.hex (the contract states no tolerance).
    """
    row = fixture['expected'][FEATURE]
    value = row['value']
    return ({FEATURE: dict(value=value['hex'] if isinstance(value, dict) else value, status=row['status'],
        reasons=list(row['reasons']), expected=row['expected'], observed=row['observed'])},
        list(row.get('witness_rows', ())))


def project(result) -> dict:
    """Compact comparable projection of a stored result (value, status, reasons, counts)."""
    column, = result.values
    quality, = result.quality
    value, = column.values
    if type(value) is float:
        value = value.hex()
    return {column.feature_id: dict(value=value, status=quality.status.value,
        reasons=[reason.value for reason in quality.reasons], expected=quality.expected, observed=quality.observed)}


def witnesses(result, packet) -> list:
    """Evidence order as role names, decoded from each row's original input identity."""
    roles = {batch.metadata.source.input_id: role for role, batch in zip(packet.roles, packet.slots) if batch is not None}
    return [roles[row.input_id] for row in result.evidence]


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
    from equity_feature_duckdb.retracement import synthetic_retracement_fixture, synthetic_retracement_config
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_retracement import make_ef2_retracement_work
    from equity_feature_workers.ef1_runner import run_ef2_retracement

    fixtures = contract_fixtures(args.contract)
    if tuple(sorted(row for row in fixtures if row.startswith('T'))) != CASES:
        parser.error('Contract result fixtures differ from the example case list')
    fixture = fixtures[args.case]
    expected, expected_witnesses = expected_projection(fixture)
    packet = synthetic_retracement_fixture(case_id=args.case)
    config = synthetic_retracement_config(case_id=args.case)
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
    scope = 'ef2-retracement-synthetic'
    item = make_ef2_retracement_work(config, inputs=tuple(declarations), batches=packet.batches,
        instrument_id='X', job_id='job-' + args.run_id, generation_id='generation-' + args.run_id,
        partition_id='X-S', destination_scope=scope)
    limits = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
        max_result_cells=1, max_evidence_rows=2)
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef2_retracement((item,), sink, requirements=limits)
        row, = outcome.readbacks
        if not row.verified:
            print(json.dumps({'verified_readback': False, 'reason': row.reason}, sort_keys=True))
            return 1
        result, = row.results
        if project(result) != expected or witnesses(result, packet) != expected_witnesses:
            raise AssertionError('Calculated result differs from the contract fixture literal: ' + json.dumps([project(result), expected, witnesses(result, packet), expected_witnesses], default=str))
        sdk = encode_result(result)
        receipt = row.output.receipt
        replay = run_ef2_retracement((item,), sink, requirements=limits)
        if not replay.readbacks[0].verified or replay.readbacks[0].output != row.output:
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        restored = reopened.read(receipt)
        verify_receipt(receipt, row.output.envelope, restored)
        if tuple(map(encode_result, restored)) != (sdk,):
            raise AssertionError('Full stored-result reopen differs')
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF2 retracement level</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Retracement level</h1><p>Contract fixture ' + escape(args.case)
        + ': ' + escape(fixture['description'])
        + '. Actual input adapter  ->  worker  ->  pure calculation  ->  separate DuckDB sink. '
        'Contract-literal match (value bit-exact, status, reasons, coverage, witness roles), SDK readback, idempotent replay and close/reopen verified.</p>'
        '<p>The level uses the supplied exact integer prior-window extrema and the caller ratio and direction only; '
        'no close price enters. The worker orchestrates only and re-derives neither extremum. Original-source parity remains unqualified.</p>'
        '<p>Task ' + escape(item.task.task_sha256) + '; content ' + escape(receipt.content_sha256)
        + '.</p><h2>Complete canonical result, original bindings and selected witnesses</h2><pre>'
        + escape(sdk.decode('ascii')) + '</pre><p>No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'task_sha256': item.task.task_sha256, 'content_sha256': receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
