"""Synthetic adapter -> endpoint worker -> NEW separate DuckDB -> verified HTML."""
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=('E01_up_reversal', 'E26_bar_close_at_cutoff'), default='E01_up_reversal')
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
    from equity_feature_duckdb.ef1 import separate_output_path, synthetic_gap_endpoint_fixture, synthetic_gap_endpoint_config
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_gap_endpoint import make_ef1_gap_endpoint_work
    from equity_feature_workers.ef1_runner import run_ef1_gap_endpoints

    packet = synthetic_gap_endpoint_fixture(case_id=args.case)
    config = synthetic_gap_endpoint_config(case_id=args.case)
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
    scope = 'ef1-gap-endpoint-synthetic'
    item = make_ef1_gap_endpoint_work(config, inputs=tuple(declarations), batches=packet.batches, instrument_id='X',
        job_id='job-' + args.run_id, generation_id='generation-' + args.run_id,
        partition_id='X-S', destination_scope=scope)
    limits = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
        max_result_cells=2, max_evidence_rows=5)
    raw_oracle = (Path(__file__).resolve().parents[1] / 'tests/ef1_gap_endpoint_oracle_causal_v4.json').read_bytes()
    if hashlib.sha256(raw_oracle).hexdigest() != '698994acf9d324d3c94334b02f307b2997ce320eb397d7f7cb5f8ea359d4f239':
        raise AssertionError('Independent frozen oracle differs')
    expected = next(row['expected_result'] for row in json.loads(raw_oracle)['packets'] if row['id'] == args.case)
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef1_gap_endpoints((item,), sink, requirements=limits)
        row, = outcome.readbacks
        if not row.verified:
            print(json.dumps({'verified_readback': False, 'reason': row.reason}, sort_keys=True))
            return 1
        result, = row.results
        if json.loads(json.dumps(asdict(result))) != expected:
            raise AssertionError('Calculated full result differs from independent expected literal')
        sdk = encode_result(result)
        receipt = row.output.receipt
        replay = run_ef1_gap_endpoints((item,), sink, requirements=limits)
        if not replay.readbacks[0].verified or replay.readbacks[0].output != row.output:
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        restored = reopened.read(receipt)
        verify_receipt(receipt, row.output.envelope, restored)
        if tuple(map(encode_result, restored)) != (sdk,):
            raise AssertionError('Full stored-result reopen differs')
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 gap endpoint</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Supplied gap endpoint</h1><p>Explicit synthetic ' + escape(args.case)
        + '. Actual input adapter  ->  worker  ->  pure calculation  ->  separate DuckDB sink. '
        'Independent full literal match, SDK readback, idempotent replay and close/reopen verified.</p>'
        '<p>These endpoint values may decrease after reversal; original-source parity and observed-source capability remain unqualified.</p>'
        '<p>Task ' + escape(item.task.task_sha256) + '; content ' + escape(receipt.content_sha256)
        + '.</p><h2>Complete canonical result, original bindings and selected witnesses</h2><pre>'
        + escape(sdk.decode('ascii')) + '</pre><p>No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'independent_literal_match': True,
        'task_sha256': item.task.task_sha256, 'content_sha256': receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
