"""Closed synthetic period/phase adapter -> worker -> new DuckDB -> HTML demo."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_feature_workers import InputManifest


def main(producer: str, argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args(argv)
    if (producer not in ('period', 'extended') or not args.output_db.is_absolute()
            or not args.html.is_absolute() or args.output_db.resolve() == args.html.resolve()
            or args.html.exists() or not args.html.parent.is_dir()
            or not 1 <= len(args.run_id) <= 128
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require closed producer, distinct absolute paths, new HTML and bounded ASCII run identity')

    from equity_feature_duckdb.ef1 import (separate_output_path,
        synthetic_period_fixture, synthetic_period_config, synthetic_extended_fixture, synthetic_extended_config)
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_period import make_ef1_period_work
    from equity_feature_workers.equity_extended import make_ef1_extended_work
    from equity_feature_workers.ef1_runner import run_ef1_periods, run_ef1_extended

    period = producer == 'period'
    packet = (synthetic_period_fixture if period else synthetic_extended_fixture)()
    config = (synthetic_period_config if period else synthetic_extended_config)()
    declarations = []
    for role, batch in zip(packet.roles, packet.slots):
        if batch is None:
            declarations.append(InputManifest(role, hashlib.sha256(('synthetic-absent:' + role).encode('ascii')).hexdigest(),
                'R1', 'missing', None))
            continue
        wire = json.dumps(asdict(batch), ensure_ascii=True, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('ascii')
        if len(wire) > 1048576:
            parser.error('Synthetic input exceeds fixed input budget')
        coverage = batch.metadata.coverage
        state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
        declarations.append(InputManifest(role, hashlib.sha256(wire).hexdigest(),
            batch.metadata.source.snapshot_id, state, InputBinding(role, batch.kind, batch.metadata)))
    scope = 'ef1-synthetic-' + producer
    key = args.run_id + '-' + producer
    item = (make_ef1_period_work if period else make_ef1_extended_work)(config,
        inputs=tuple(declarations), batches=packet.batches, instrument_id='X',
        job_id='job-' + key, generation_id='generation-' + key, partition_id='X-S', destination_scope=scope)
    limits = SinkRequirements(max_results=1, max_chunk_bytes=262144,
        max_total_bytes=262144, max_result_cells=9, max_evidence_rows=72)
    database = separate_output_path(args.output_db, ())
    with DuckDBSink(database, scope, limits=limits) as sink:
        outcome = (run_ef1_periods if period else run_ef1_extended)((item,), sink, requirements=limits)
        readback, = outcome.readbacks
        if not readback.verified:
            print(json.dumps(dict(verified_readback=False, reason=readback.reason), sort_keys=True))
            return 1
        result, = readback.results
        sdk = encode_result(result)
        receipt = readback.output.receipt
        rows = ''.join('<tr><td>' + escape(column.feature_id) + '</td><td>' + escape(str(column.values[0]))
            + '</td><td>' + escape(column.unit) + '</td><td>' + escape(str(quality.status))
            + '</td><td>' + escape(','.join(str(reason) for reason in quality.reasons)) + '</td></tr>'
            for column, quality in zip(result.values, result.quality))
        document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 supplied period and phase</title>'
            '<style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:1rem}'
            'td,th{border:1px solid #aaa;padding:.5rem}table{border-collapse:collapse}'
            'pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>Supplied ' + escape(producer) + '</h1>'
            '<p>Explicit synthetic canonical fixture; provenance ' + escape(','.join(packet.provenance)) + '.</p>'
            '<p>Independent civil-calendar period or supplied BAR phase semantics. Extended prior-close denominator is the '
            'immediately preceding regular session in both phases. Original observed-count, regular-open, large-calendar '
            'and legacy source-parity obligations remain open. No private parity claim.</p>'
            '<p>Full SDK stored-result readback verified. Task ' + escape(item.task.task_sha256)
            + '; content ' + escape(receipt.content_sha256) + '.</p><table><tr><th>Feature</th><th>Value</th>'
            '<th>Unit</th><th>Status</th><th>Reasons</th></tr>' + rows + '</table>'
            '<details><summary>Complete canonical result/bindings/evidence</summary><pre>'
            + escape(sdk.decode('ascii')) + '</pre></details><p>Integers are text; no JavaScript or remote assets.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as destination:
        destination.write(document)
    print(json.dumps(dict(verified_readback=True, task_sha256=item.task.task_sha256,
        content_sha256=receipt.content_sha256, html=str(args.html)), sort_keys=True))
    return 0
