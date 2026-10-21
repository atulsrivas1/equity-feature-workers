"""Explicit synthetic C01 clock -> bounded worker -> DuckDB -> local HTML.

Source-authored demonstration; run only in a coordinator-admitted resource window.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
from pathlib import Path

from equity_feature_contracts import AdjustmentSpec, AvailabilitySpec, InputBinding, SessionSpec, WindowSpec
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_feature_workers import InputManifest
from equity_feature_workers.equity_clock import make_ef1_clock_work
from equity_feature_workers.ef1_runner import run_ef1_clocks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--cutoff', type=int, choices=(10, 20, 30, 40), default=20)
    parser.add_argument('--proof', choices=('present', 'missing', 'unknown', 'future'), default='present')
    args = parser.parse_args(argv)
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.html.exists()
            or not args.html.parent.is_dir() or not 1 <= len(args.run_id) <= 128
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require distinct absolute paths, a new HTML export, and a bounded ASCII run identity')

    from equity_feature_duckdb.ef1 import (
        ClockInputs, separate_output_path, synthetic_clock_calendar, synthetic_clock_fixture,
    )
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_features.ef1 import build_ef1_config

    known = None if args.proof == 'unknown' else 41 if args.proof == 'future' else 5
    fixture = (ClockInputs(None, 'unavailable') if args.proof == 'missing'
        else synthetic_clock_fixture(known_at_ns=known))
    calendar = synthetic_clock_calendar()
    config = build_ef1_config(dict(version='ef1-context1', producer='clock',
        calendar=calendar, selection={}),
        session=SessionSpec('SYN', 'S', 10, 30, 'UTC', scheduled_close_ns=40, early_close=True),
        window=WindowSpec(1, 'S', ('S',), 'completed_eod'),
        availability=AvailabilitySpec(args.cutoff, args.cutoff, 40),
        price_unit=None, adjustment=AdjustmentSpec())
    batch = fixture.calendar_provenance
    if batch is None:
        declaration = InputManifest('calendar_provenance', hashlib.sha256(b'synthetic-missing-calendar').hexdigest(),
            'R1', 'missing', None)
    else:
        payload = json.dumps(asdict(batch), ensure_ascii=True, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('ascii')
        if len(payload) > 1048576:
            parser.error('Synthetic proof exceeds fixed input budget')
        declaration = InputManifest('calendar_provenance', hashlib.sha256(payload).hexdigest(),
            batch.metadata.source.snapshot_id, 'complete', InputBinding('calendar_provenance', batch.kind, batch.metadata))
    scope = 'ef1-synthetic-clock1'
    identity = args.run_id + '-C' + str(args.cutoff) + '-' + args.proof
    item = make_ef1_clock_work(config, inputs=(declaration,), batches=fixture.batches,
        instrument_id='X', job_id='job-' + identity, generation_id='generation-' + identity,
        partition_id='X-S', destination_scope=scope)
    requirements = SinkRequirements(max_results=1, max_chunk_bytes=262144,
        max_total_bytes=262144, max_result_cells=5, max_evidence_rows=128)
    output_path = separate_output_path(args.output_db, ())
    with DuckDBSink(output_path, scope, limits=requirements) as sink:
        outcome = run_ef1_clocks((item,), sink, requirements=requirements)
        readback = outcome.readbacks[0]
        if not readback.verified:
            print(json.dumps(dict(verified_readback=False, reason=readback.reason,
                has_receipt=readback.output is not None, pending=outcome.publisher.pending), sort_keys=True))
            return 1
        result, = readback.results
        sdk = encode_result(result)
        if len(sdk) > 262144:
            parser.error('Verified result export exceeds fixed budget')
        receipt = readback.output.receipt
        rows = ''.join('<tr><td>' + escape(column.feature_id) + '</td><td>'
            + escape(str(column.values[0])) + '</td><td>' + escape(column.unit) + '</td><td>'
            + escape(str(quality.status)) + '</td><td>'
            + escape(','.join(str(reason) for reason in quality.reasons)) + '</td></tr>'
            for column, quality in zip(result.values, result.quality))
        document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 C01 clock</title>'
            '<style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:1rem}'
            'td,th{border:1px solid #aaa;padding:.5rem}table{border-collapse:collapse}'
            'pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>Supplied session clock</h1>'
            '<p>C01 synthetic schedule and explicit ' + escape(args.proof) + ' proof. Source mode: '
            + escape(fixture.provenance) + '. No source authentication or inferred trading status.</p>'
            '<p>Requested C=K=' + escape(str(args.cutoff)) + ', E=40. Full SDK stored-result readback verified.</p>'
            '<p>Task: ' + escape(item.task.task_sha256) + '</p><p>Publication: '
            + escape(receipt.idempotency_key) + '</p><p>Content: ' + escape(receipt.content_sha256)
            + '</p><table><tr><th>Feature</th><th>Value</th><th>Unit</th><th>Status</th><th>Reasons</th></tr>'
            + rows + '</table><details><summary>Complete canonical result/bindings/evidence</summary><pre>'
            + escape(sdk.decode('ascii')) + '</pre></details><p>Integers are rendered as text; '
            'this viewer contains no JavaScript or remote assets.</p></html>')
    # The sink is closed before producing an exclusive, escaped local export.
    with args.html.open('x', encoding='utf-8', newline='\n') as destination:
        destination.write(document)
    print(json.dumps(dict(verified_readback=True, task_sha256=item.task.task_sha256,
        content_sha256=receipt.content_sha256, html=str(args.html)), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
