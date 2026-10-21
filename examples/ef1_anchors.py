"""Explicit synthetic EF1 anchor -> worker -> existing DuckDB sink demo.

Requires reviewed EF1 library/I/O artifacts. Not executed during source authoring.
No dynamic imports, source database, provider data or production deployment.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
from pathlib import Path

from equity_feature_contracts import (
    AdjustmentSpec, AvailabilitySpec, InputBinding, PriceUnit, SessionSpec, WindowSpec,
)
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_feature_workers import InputManifest
from equity_feature_workers.equity_sessions import make_ef1_anchor_work
from equity_feature_workers.ef1_runner import run_ef1_anchors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-db', type=Path, required=True, help='absolute separate generated-output DuckDB path')
    parser.add_argument('--html', type=Path, required=True, help='absolute new local HTML export path; never overwritten')
    parser.add_argument('--run-id', required=True, help='persistent identity; reuse unchanged for an intentional replay')
    parser.add_argument('--mode', choices=('trades', 'bars_actual_notional'), default='trades')
    parser.add_argument('--case', choices=('A01', 'A02', 'A01-missing-notional'), default='A01')
    args = parser.parse_args(argv)
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve()
            or args.html.exists() or not args.html.parent.is_dir()
            or not 1 <= len(args.run_id) <= 128
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)
            or (args.case == 'A01-missing-notional' and args.mode != 'bars_actual_notional')):
        parser.error('Require distinct absolute output paths, a new HTML file, a bounded ASCII run ID and compatible mode/case')

    # Fixed reviewed factories only; source fixtures contain no feature math.
    from equity_feature_duckdb.ef1 import separate_output_path, synthetic_anchor_fixture
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_features.ef1 import build_ef1_config

    fixture = synthetic_anchor_fixture(args.case, mode=args.mode)
    definition = dict(anchor_id='A', reset_id='R', start_ns=10,
        selection_policy_id='supplied-v1', revision='R1')
    context = dict(version='ef1-context1', producer='anchor', calendar=None,
        selection=dict(mode=args.mode, definition=definition))
    config = build_ef1_config(context, session=SessionSpec('SYN', 'S', 0, 30, 'supplied'),
        window=WindowSpec(1, 'S', ('S',), 'completed_eod'), availability=AvailabilitySpec(20, 20, 20),
        price_unit=PriceUnit(2, 'USD'), adjustment=AdjustmentSpec())
    declarations = []
    for role, batch in zip(fixture.roles, (fixture.market, fixture.anchor_provenance)):
        if batch is None:
            acquisition = hashlib.sha256(('synthetic-absent:' + role).encode('ascii')).hexdigest()
            declarations.append(InputManifest(role, acquisition, 'R1', 'missing', None))
            continue
        # These are tiny fixed synthetic packets. This acquisition identity names
        # their actual original payload, not a computed result or fake provider.
        encoded = json.dumps(asdict(batch), ensure_ascii=True, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('ascii')
        if len(encoded) > 1048576:
            parser.error('Synthetic packet exceeds fixed input byte budget')
        coverage = batch.metadata.coverage
        state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
        declarations.append(InputManifest(role, hashlib.sha256(encoded).hexdigest(),
            batch.metadata.source.snapshot_id, state, InputBinding(role, batch.kind, batch.metadata)))
    scope = 'ef1-synthetic-anchor1'
    identity = args.run_id + '-' + args.mode + '-' + args.case
    item = make_ef1_anchor_work(config, inputs=tuple(declarations), batches=fixture.batches,
        instrument_id='X', job_id='job-' + identity, generation_id='generation-' + identity,
        partition_id='X-S', destination_scope=scope)
    requirements = SinkRequirements(max_results=1, max_chunk_bytes=262144,
        max_total_bytes=262144, max_result_cells=3, max_evidence_rows=128)
    output = separate_output_path(args.output_db, ())
    # Readback completes while this one serialized writer owns the sink. Export
    # only verified data; browser rendering occurs after sink closure below.
    with DuckDBSink(output, scope, limits=requirements) as sink:
        outcome = run_ef1_anchors((item,), sink, requirements=requirements)
        readback = outcome.readbacks[0]
        if not readback.verified:
            print(json.dumps(dict(verified_readback=False, reason=readback.reason,
                has_receipt=readback.output is not None,
                pending_task_ids=outcome.publisher.pending), sort_keys=True))
            return 1
        result, = readback.results
        sdk = encode_result(result)
        if len(sdk) > 262144:
            parser.error('Verified export exceeds fixed result byte budget')
        receipt = readback.output.receipt
        rows = ''.join('<tr><td>' + escape(column.feature_id) + '</td><td>'
            + escape(str(column.values[0])) + '</td><td>' + escape(column.unit)
            + '</td><td>' + escape(str(quality.status)) + '</td><td>'
            + escape(','.join(str(reason) for reason in quality.reasons)) + '</td></tr>'
            for column, quality in zip(result.values, result.quality))
        report = ('<!doctype html><html lang="en"><meta charset="utf-8">'
            '<title>EF1 anchor results</title><style>body{font:16px system-ui;max-width:1100px;'
            'margin:2rem auto;padding:1rem}td,th{border:1px solid #aaa;padding:.5rem}'
            'table{border-collapse:collapse}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
            '<h1>EF1 supplied anchor results</h1><p>Original synthetic inputs; supplied anchor '
            'declaration. No source authentication or real-market coverage claim.</p><p>'
            + escape(args.mode + ' / ' + args.case) + '</p><p>Full SDK readback verified. '
            'All integers below are rendered as text; no JavaScript numeric conversion.</p>'
            '<p>Task: ' + escape(item.task.task_sha256) + '</p><p>Publication: '
            + escape(receipt.idempotency_key) + '</p><p>Content: ' + escape(receipt.content_sha256)
            + '</p><table><tr><th>Feature</th><th>Value</th><th>Unit</th><th>Status</th>'
            '<th>Reasons</th></tr>' + rows + '</table><details><summary>Complete canonical '
            'result, quality, bindings and evidence</summary><pre>'
            + escape(sdk.decode('ascii')) + '</pre></details></html>')
    # Exclusive file creation preserves existing exports; no remote assets/scripts.
    with args.html.open('x', encoding='utf-8', newline='\n') as destination:
        destination.write(report)
    print(json.dumps(dict(verified_readback=True, task_sha256=item.task.task_sha256,
        content_sha256=receipt.content_sha256, html=str(args.html)), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
