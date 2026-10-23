"""READ-ONLY stored 1-minute bars -> adapter -> regular BAR worker -> NEW separate DuckDB -> HTML.

Only the BAR role is a real observation. Calendar, population proof and known-at clocks are
explicit SYNTHETIC inputs. No rights or original-source parity claim is made.
"""
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


def _stat(path: Path) -> list[list[object]]:
    """Size and mtime of the source and its possible WAL sidecar (stat only; never opens for write)."""
    rows: list[list[object]] = []
    for item in (path, path.with_name(path.name + '.wal')):
        status = item.stat() if item.exists() else None
        rows.append([item.name, None if status is None else status.st_size, None if status is None else status.st_mtime_ns])
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-db', type=Path, required=True, help='existing DuckDB, opened read-only')
    parser.add_argument('--table', required=True, help='schema.table of the stored 1-minute bars')
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--date', required=True, help='ISO date')
    parser.add_argument('--row-cap', type=int, default=500)
    parser.add_argument('--price-scale', type=int, default=3, help='decimal scale; refuses if any stored price is not exact at it')
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args(argv)
    if (not args.source_db.is_absolute() or not args.source_db.is_file()
            or not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or args.source_db.resolve() in (args.output_db.resolve(), args.html.resolve())
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require existing absolute source and new distinct absolute output paths, existing parents and bounded ASCII run identity')
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb.real_bars_reader import read_real_regular_bars
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_regular_bar_prices import make_ef1_regular_bar_price_work
    from equity_feature_workers.ef1_runner import run_ef1_regular_bar_prices

    before = _stat(args.source_db)
    source = read_real_regular_bars(args.source_db, args.table, args.symbol, args.date, args.row_cap,
        price_scale=args.price_scale)
    packet, config = source.inputs, source.config
    declarations = []
    for role, batch in zip(packet.roles, packet.slots):
        if batch is None:
            declarations.append(InputManifest(role, hashlib.sha256(('absent:' + role).encode('ascii')).hexdigest(),
                'absent', 'missing', None))
        else:
            raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
            coverage = batch.metadata.coverage
            state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
            declarations.append(InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id,
                state, InputBinding(role, batch.kind, batch.metadata)))
    scope = 'ef1-real-regular-bar'
    item = make_ef1_regular_bar_price_work(config, inputs=tuple(declarations), batches=packet.batches,
        instrument_id=source.instrument_id, job_id='job-' + args.run_id, generation_id='generation-' + args.run_id,
        partition_id=source.instrument_id + '-S', destination_scope=scope)
    limits = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
        max_result_cells=2, max_evidence_rows=2)
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef1_regular_bar_prices((item,), sink, requirements=limits)
        row, = outcome.readbacks
        if not row.verified:
            print(json.dumps({'verified_readback': False, 'reason': row.reason}, sort_keys=True))
            return 1
        result, = row.results
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
    final = _stat(args.source_db)
    unchanged = before == final
    plain = json.loads(json.dumps(asdict(result)))
    qualities = {q['feature_id']: q for q in plain['quality']}
    features = [(c['feature_id'], c['unit'], c['values'], qualities[c['feature_id']]['status'],
                 qualities[c['feature_id']]['reasons']) for c in plain['values']]
    table = ''.join('<tr><td>' + escape(str(a)) + '</td><td>' + escape(str(b)) + '</td><td>' + escape(str(c))
        + '</td><td>' + escape(str(d)) + '</td><td>' + escape(str(e)) + '</td></tr>' for a, b, c, d, e in features)
    roles = ''.join('<tr><td>' + escape(role) + '</td><td>' + escape(source.provenance[role]) + '</td></tr>' for role in packet.roles)
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 real-source regular BAR prices</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}table{border-collapse:collapse}'
        'td,th{border:1px solid #888;padding:.3rem .6rem;text-align:left}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Regular completed-BAR prices from a bounded real-source slice</h1>'
        '<p>Bounded demonstration: one symbol, one date, ' + str(source.rows) + ' stored 1-minute bars read through a read-only DuckDB connection. '
        'Pipeline: stored source -&gt; read-only input adapter -&gt; worker -&gt; pure library -&gt; output sink -&gt; NEW separate DuckDB -&gt; this page.</p>'
        '<h2>Input provenance</h2><table><tr><th>Role</th><th>Origin</th></tr>' + roles + '</table>'
        '<p><b>Real observations</b> (label duckdb_backed_observed): ' + escape(', '.join(source.observed_fields)) + '.</p>'
        '<p><b>Synthetic inputs</b> (not present in the source, stipulated for this demonstration): ' + escape('; '.join(source.synthetic_labels)) + '.</p>'
        '<p>Symbol ' + escape(source.instrument_id) + ', date ' + escape(args.date) + '. Source unchanged by size and modification time: '
        + ('yes' if unchanged else 'NO') + '.</p>'
        '<p>Limit of the unchanged-source check: the catalog is view-only, so size and modification time of the .duckdb file cannot detect changes to the data behind the views. Stored rows whose timestamp has fractional seconds are refused, which narrows coverage.</p>'
        '<h2>Feature table</h2><table><tr><th>Feature</th><th>Unit</th><th>Value</th><th>Status</th><th>Reasons</th></tr>' + table + '</table>'
        '<p>Calculated close is the last completed regular-window bar close, not proof of an official exchange close. '
        'No dataset-rights claim and no original-source parity claim is made; both remain unqualified.</p>'
        '<p>Task ' + escape(item.task.task_sha256) + '; content ' + escape(receipt.content_sha256)
        + '.</p><h2>Complete canonical result</h2><pre>' + escape(sdk.decode('ascii'))
        + '</pre><p>No remote assets or JavaScript.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'replay_identical': True,
        'provenance': source.provenance, 'bar_rows': source.rows, 'source_unchanged': unchanged,
        'source_stat_before': before, 'source_stat_after': final,
        'task_sha256': item.task.task_sha256, 'content_sha256': receipt.content_sha256}, sort_keys=True))
    return 0 if unchanged else 2


if __name__ == '__main__':
    raise SystemExit(main())
