"""Synthetic adapter -> regular BAR worker -> NEW separate DuckDB -> verified HTML."""
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
    parser.add_argument('--case', choices=('R01_two_independent_endpoints', 'R08_unknown_before_first'), default='R01_two_independent_endpoints')
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
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb.regular_bar_prices import synthetic_regular_bar_price_fixture, synthetic_regular_bar_price_config
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_regular_bar_prices import make_ef1_regular_bar_price_work
    from equity_feature_workers.ef1_runner import run_ef1_regular_bar_prices

    packet = synthetic_regular_bar_price_fixture(case_id=args.case)
    config = synthetic_regular_bar_price_config(case_id=args.case)
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
    scope = 'ef1-regular-bar-synthetic'
    item = make_ef1_regular_bar_price_work(config, inputs=tuple(declarations), batches=packet.batches, instrument_id='X',
        job_id='job-' + args.run_id, generation_id='generation-' + args.run_id,
        partition_id='X-S', destination_scope=scope)
    limits = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
        max_result_cells=2, max_evidence_rows=2)
    raw_oracle = (Path(__file__).resolve().parents[1] / 'tests/ef1_regular_bar_prices_oracle.json').read_bytes()
    if hashlib.sha256(raw_oracle).hexdigest() != '05d5b4815df3fd621993037c84f9d897a32ee84c833715e6ca1f6c4e1473d030':
        raise AssertionError('Independent frozen oracle differs')
    expected = next(row['expected_result'] for row in json.loads(raw_oracle)['packets'] if row['id'] == args.case)
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef1_regular_bar_prices((item,), sink, requirements=limits)
        row, = outcome.readbacks
        if not row.verified:
            print(json.dumps({'verified_readback': False, 'reason': row.reason}, sort_keys=True))
            return 1
        result, = row.results
        if json.loads(json.dumps(asdict(result))) != expected:
            raise AssertionError('Calculated full result differs from independent expected literal')
        sdk = encode_result(result)
        receipt = row.output.receipt
        replay = run_ef1_regular_bar_prices((item,), sink, requirements=limits)
        if not replay.readbacks[0].verified or replay.readbacks[0].output != row.output:
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        restored = reopened.read(receipt)
        verify_receipt(receipt, row.output.envelope, restored)
        if tuple(map(encode_result, restored)) != (sdk,):
            raise AssertionError('Full stored-result reopen differs')
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 regular BAR prices</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Regular completed-BAR prices</h1><p>Explicit synthetic ' + escape(args.case)
        + '. Actual input adapter  ->  worker  ->  pure calculation  ->  separate DuckDB sink. '
        'Independent full literal match, SDK readback, idempotent replay and close/reopen verified.</p>'
        '<p>Calculated close is a supplied prefix endpoint, never proof of an official exchange close. Original-source parity and observed-source capability remain unqualified.</p>'
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
