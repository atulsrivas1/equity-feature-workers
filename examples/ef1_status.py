"""Explicit synthetic S01/S02/S03 -> bounded status worker -> DuckDB -> HTML.

Run only in a coordinator-admitted resource window, with qualified candidates.
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
from equity_feature_workers.equity_status import make_ef1_status_work
from equity_feature_workers.ef1_runner import run_ef1_statuses


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--case', choices=('S01', 'S02', 'S03'), default='S01')
    args = parser.parse_args(argv)
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.html.exists()
            or not args.html.parent.is_dir() or not 1 <= len(args.run_id) <= 128
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require distinct absolute paths, a new HTML export, and a bounded ASCII run identity')

    from equity_feature_duckdb.ef1 import (
        separate_output_path, synthetic_status_context, synthetic_status_fixture,
    )
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_features.ef1 import build_ef1_config

    supplied = synthetic_status_fixture(case=args.case)
    context = synthetic_status_context(case=args.case)
    cutoff = {'S01': 17, 'S02': 16, 'S03': 6}[args.case]
    config = build_ef1_config(context,
        session=SessionSpec('SYN', 'S', 10, 30, 'UTC', scheduled_close_ns=40, early_close=True),
        window=WindowSpec(1, 'S', ('S',), 'completed_eod'),
        availability=AvailabilitySpec(cutoff, cutoff, 20),
        price_unit=None, adjustment=AdjustmentSpec())
    declarations = []
    for role, batch in zip(supplied.roles, supplied.slots, strict=True):
        if batch is None:
            declaration = InputManifest(role, hashlib.sha256(('synthetic-missing-' + role).encode('ascii')).hexdigest(),
                'R1', 'missing', None)
        else:
            payload = json.dumps(asdict(batch), ensure_ascii=True, sort_keys=True,
                separators=(',', ':'), allow_nan=False).encode('ascii')
            if len(payload) > 1048576:
                parser.error('Synthetic input exceeds fixed budget')
            coverage = batch.metadata.coverage
            state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
            declaration = InputManifest(role, hashlib.sha256(payload).hexdigest(),
                batch.metadata.source.snapshot_id, state, InputBinding(role, batch.kind, batch.metadata))
        declarations.append(declaration)
    scope = 'ef1-synthetic-status1'
    identity = args.run_id + '-' + args.case
    item = make_ef1_status_work(config, inputs=tuple(declarations), batches=supplied.batches,
        instrument_id='X', job_id='job-' + identity, generation_id='generation-' + identity,
        partition_id='X-S', destination_scope=scope)
    requirements = SinkRequirements(max_results=1, max_chunk_bytes=262144,
        max_total_bytes=262144, max_result_cells=4, max_evidence_rows=128)
    output_path = separate_output_path(args.output_db, ())
    with DuckDBSink(output_path, scope, limits=requirements) as sink:
        outcome = run_ef1_statuses((item,), sink, requirements=requirements)
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
            for column, quality in zip(result.values, result.quality, strict=True))
        origins = ', '.join(role + '=' + str(origin)
            for role, origin in zip(supplied.roles, supplied.provenance, strict=True))
        document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 supplied status</title>'
            '<style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:1rem}'
            'td,th{border:1px solid #aaa;padding:.5rem}table{border-collapse:collapse}'
            'pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>Supplied current-state status</h1>'
            '<p>Explicit synthetic ' + escape(args.case) + '. Source provenance: ' + escape(origins)
            + '. Supplied declarations do not authenticate a source or infer missing transitions.</p>'
            '<p>Requested C=K=' + str(cutoff) + ', E=20. Full SDK stored-result readback verified.</p>'
            '<p>Task: ' + escape(item.task.task_sha256) + '</p><p>Publication: '
            + escape(receipt.idempotency_key) + '</p><p>Content: ' + escape(receipt.content_sha256)
            + '</p><table><tr><th>Feature</th><th>Value</th><th>Unit</th><th>Status</th><th>Reasons</th></tr>'
            + rows + '</table><details><summary>Complete canonical result/bindings/original event witnesses</summary><pre>'
            + escape(sdk.decode('ascii')) + '</pre></details><p>Integer UTCns are rendered as text. '
            'This viewer contains no JavaScript or remote assets. Transition-history/open diagnostics remain unimplemented.</p></html>')
    # Exclusive escaped export follows full readback and sink closure.
    with args.html.open('x', encoding='utf-8', newline='\n') as destination:
        destination.write(document)
    print(json.dumps(dict(verified_readback=True, task_sha256=item.task.task_sha256,
        content_sha256=receipt.content_sha256, html=str(args.html)), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
