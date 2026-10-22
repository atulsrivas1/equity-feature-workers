"""Synthetic adapter -> reference-close worker -> NEW separate DuckDB -> verified HTML."""
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

# Contract result-fixture ids (docs/ef1/CLOSE_RECONCILIATION_CONTRACT_PROPOSAL.json in the core repo).
CASES = ('N01', 'N02', 'N03', 'N04', 'N05', 'N06', 'N07', 'N08', 'N09', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N16', 'N17', 'Z01', 'Z02', 'Z03', 'Z04', 'R01', 'R02', 'R03', 'R04', 'R05', 'R06', 'R07', 'R08', 'R09', 'R10', 'R11', 'R12', 'R13', 'R14', 'R15', 'C01', 'C02', 'C03', 'C04', 'C05', 'C06', 'X01', 'X02', 'X03', 'D01', 'D02')
DEFAULT_CONTRACT = Path(__file__).resolve().parents[2] / 'equity-features/docs/ef1/CLOSE_RECONCILIATION_CONTRACT_PROPOSAL.json'
SHORT = ('absolute_difference_bps', 'comparison_state', 'supplied_price', 'within_tolerance')


def project(result) -> dict:
    """Compact comparable projection of a stored result (value, status, reasons, counts)."""
    outputs = {}
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        outputs[column.feature_id.rsplit('.', 1)[1]] = dict(
            value=value.hex() if type(value) is float else value, status=quality.status.value,
            reasons=[reason.value for reason in quality.reasons], expected=quality.expected, observed=quality.observed)
    return outputs


def literal(fixture: dict) -> dict:
    """Independent expected literal from the contract fixture (hand-derived, not production output)."""
    outputs = {}
    for name in SHORT:
        row = fixture['expected']['outputs'][name]
        value = row.get('value_hex', row['value']) if name == 'absolute_difference_bps' and row['value'] is not None else row['value']
        outputs[name] = dict(value=value, status=row['status'], reasons=row['reasons'],
            expected=row['expected'], observed=row['observed'])
    return outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=CASES, default='N02')
    parser.add_argument('--contract', type=Path, default=DEFAULT_CONTRACT,
        help='frozen close-reconciliation contract JSON holding the independent expected literals')
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
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
    from equity_feature_duckdb.reference_close import synthetic_reference_close_fixture, synthetic_reference_close_config
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_reference_close import make_ef1_reference_close_work
    from equity_feature_workers.ef1_runner import run_ef1_reference_close

    packet = synthetic_reference_close_fixture(case_id=args.case)
    config = synthetic_reference_close_config(case_id=args.case)
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
    scope = 'ef1-reference-close-synthetic'
    item = make_ef1_reference_close_work(config, inputs=tuple(declarations), batches=packet.batches, instrument_id='X',
        job_id='job-' + args.run_id, generation_id='generation-' + args.run_id,
        partition_id='X-S', destination_scope=scope)
    limits = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
        max_result_cells=4, max_evidence_rows=7)
    contract = json.loads(args.contract.read_bytes())
    fixture = next(row for row in contract['fixtures'] if row['id'] == args.case)
    expected = literal(fixture)
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef1_reference_close((item,), sink, requirements=limits)
        row, = outcome.readbacks
        if not row.verified:
            print(json.dumps({'verified_readback': False, 'reason': row.reason}, sort_keys=True))
            return 1
        result, = row.results
        if project(result) != expected or result.values[1].values[0] != fixture['expected']['state']:
            raise AssertionError('Calculated result differs from the contract expected literal')
        sdk = encode_result(result)
        receipt = row.output.receipt
        replay = run_ef1_reference_close((item,), sink, requirements=limits)
        if not replay.readbacks[0].verified or replay.readbacks[0].output != row.output:
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        restored = reopened.read(receipt)
        verify_receipt(receipt, row.output.envelope, restored)
        if tuple(map(encode_result, restored)) != (sdk,):
            raise AssertionError('Full stored-result reopen differs')
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 reference close comparison</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Reference close comparison</h1><p>Explicit synthetic ' + escape(args.case)
        + '. Actual input adapter  ->  worker  ->  pure calculation  ->  separate DuckDB sink. '
        'Contract-literal match, SDK readback, idempotent replay and close/reopen verified.</p>'
        '<p>The calculated regular close and the supplied authoritative close stay separate values; only the supplied close is projected here. The comparison is arithmetic agreement under the caller tolerance and authenticates neither source rights nor an official price. Original-source parity remains unqualified.</p>'
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
