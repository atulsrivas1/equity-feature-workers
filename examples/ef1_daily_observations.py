"""Frozen synthetic adapter -> worker -> separate DuckDB -> reopen -> HTML."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
from pathlib import Path
import pickle

import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

CASES = ('V235-literal-last252', 'V201-observed-not-scheduled',
    'V223-prior-skips-invalid-populated-period', 'V240-full-leap-year-literal-history')
CORPUS_SHA256 = '479948037cd6d2fca3901cdb9cf3fcbba6256acbbcb9440bcd68e0362d7688c5'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--case', choices=CASES, default=CASES[0])
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args(argv)
    if (not args.output_db.is_absolute() or not args.html.is_absolute() or not args.corpus.is_absolute()
            or args.output_db.exists() or not args.output_db.parent.is_dir()
            or args.html.exists() or not args.html.parent.is_dir()
            or len({args.output_db.resolve(), args.html.resolve(), args.corpus.resolve()}) != 3
            or not 1 <= len(args.run_id) <= 64
            or any(ord(value) < 32 or ord(value) > 126 for value in args.run_id)):
        parser.error('Require distinct absolute new outputs and bounded ASCII run identity')
    # Read only the immutable source-safe derivative, never the private design pack.
    with args.corpus.open('rb') as source:
        raw = source.read(16 * 1048576 + 1)
    if len(raw) > 16 * 1048576 or hashlib.sha256(raw).hexdigest() != CORPUS_SHA256:
        parser.error('Require exact frozen public synthetic corpus')
    corpus = json.loads(raw)
    packet, = (row for row in corpus['packets'] if row['id'] == args.case)
    config = c.ConfigSpec.from_json(json.dumps(packet['config'], ensure_ascii=True,
        sort_keys=True, separators=(',', ':')))

    from equity_feature_duckdb.ef1 import (
        DailyObservationsInputs, make_daily_observations_batch, separate_output_path)
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_daily_observations import make_ef1_daily_observations_work
    from equity_feature_workers.ef1_runner import run_ef1_daily_observations

    slots = []
    for row in packet['role_slots']:
        value = row['batch']
        if value is None:
            slots.append(None)
            continue
        metadata = dict(value['metadata'])
        metadata['source'] = c.SourceBinding(**metadata['source'])
        metadata['coverage'] = c.Coverage(**metadata['coverage'])
        metadata['price_unit'] = None if metadata['price_unit'] is None else c.PriceUnit(**metadata['price_unit'])
        metadata['adjustment'] = c.AdjustmentSpec(**metadata['adjustment'])
        metadata['scope'] = c.InputScope(**metadata['scope'])
        metadata['interval_coverage'] = ()
        columns = value['columns']
        slots.append(make_daily_observations_batch(tuple(column['name'] for column in columns),
            tuple(zip(*(column['values'] for column in columns))), metadata=c.BatchMetadata(**metadata)))
    supplied = DailyObservationsInputs(*slots,
        provenance=tuple('unavailable' if slot is None else 'synthetic' for slot in slots))
    declarations = []
    for role, batch in zip(supplied.roles, supplied.slots):
        if batch is None:
            declarations.append(InputManifest(role, hashlib.sha256(('synthetic-absent:' + role).encode()).hexdigest(),
                'synthetic-missing', 'missing', None))
            continue
        coverage = batch.metadata.coverage
        state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
        declarations.append(InputManifest(role, hashlib.sha256(pickle.dumps(batch, protocol=5)).hexdigest(),
            batch.metadata.source.snapshot_id, state, c.InputBinding(role, batch.kind, batch.metadata)))
    scope = 'ef1-daily-synthetic'
    item = make_ef1_daily_observations_work(config, inputs=tuple(declarations), batches=supplied.batches,
        instrument_id=packet['entity']['instrument_id'], job_id='daily-job-' + args.run_id,
        generation_id='daily-generation-' + args.run_id, partition_id=args.case, destination_scope=scope)
    limits = SinkRequirements(**corpus['sink_requirements'])
    database = separate_output_path(args.output_db, (args.corpus,))
    with DuckDBSink(database, scope, limits=limits) as sink:
        outcome = run_ef1_daily_observations((item,), sink, requirements=limits)
        readback, = outcome.readbacks
        if not readback.verified:
            raise RuntimeError('Synthetic full readback failed: ' + str(readback.reason))
        result, = readback.results
        sdk = encode_result(result)
        receipt = readback.output.receipt
        if json.loads(json.dumps(asdict(result))) != packet['expected_result']:
            raise AssertionError('Actual synthetic result differs from frozen independent golden')
    # Read after physical connection close/reopen, without creating another write.
    with DuckDBSink(database, scope, limits=limits) as reopened:
        stored = reopened.read(receipt)
        verify_receipt(receipt, readback.output.envelope, stored)
        if tuple(map(encode_result, stored)) != (sdk,):
            raise AssertionError('Reopened complete canonical result differs')
    rows = ''.join('<tr><td>' + escape(column.feature_id) + '</td><td>' + escape(str(column.values[0]))
        + '</td><td>' + escape(column.unit) + '</td><td>' + escape(str(quality.status))
        + '</td><td>' + escape(','.join(str(reason) for reason in quality.reasons)) + '</td></tr>'
        for column, quality in zip(result.values, result.quality))
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 daily observations</title>'
        '<style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:1rem}'
        'td,th{border:1px solid #aaa;padding:.5rem}table{border-collapse:collapse}'
        'pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>Daily observations</h1>'
        '<p>Explicit synthetic canonical observations, calendars, coverage and basis proofs. Case '
        + escape(args.case) + '. No private source or original floating-point parity qualification.</p>'
        '<p>Full independent golden and SDK readback verified after database close/reopen. Task '
        + escape(item.task.task_sha256) + '; content ' + escape(receipt.content_sha256)
        + '.</p><table><tr><th>Feature</th><th>Value</th><th>Unit</th><th>Status</th><th>Reasons</th></tr>'
        + rows + '</table><details><summary>Complete canonical result/bindings/evidence</summary><pre>'
        + escape(sdk.decode('ascii')) + '</pre></details><p>No JavaScript or remote assets.</p></html>')
    if len(document.encode('utf-8')) > 262144:
        raise AssertionError('Synthetic HTML exceeds fixed display bound')
    with args.html.open('x', encoding='utf-8', newline='\n') as destination:
        destination.write(document)
    print(json.dumps(dict(verified_readback=True, reopened=True, case=args.case,
        task_sha256=item.task.task_sha256, content_sha256=receipt.content_sha256,
        html=str(args.html)), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
