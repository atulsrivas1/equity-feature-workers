"""Synthetic-only reference demo shared by the two explicitly closed CLI entrypoints."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
from pathlib import Path

from equity_feature_contracts import AdjustmentSpec, AvailabilitySpec, InputBinding, PriceUnit, SessionSpec, WindowSpec
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_feature_workers import InputManifest


def main(producer: str, argv: list[str] | None = None) -> int:
    classification = producer == 'reference_classification'
    parser = argparse.ArgumentParser(description='Explicit synthetic reference admission -> worker -> separate DuckDB -> HTML')
    parser.add_argument('--case', choices=('R01', 'R02') if classification else ('R03',), default='R01' if classification else 'R03')
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args(argv)
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.html.exists()
            or not args.html.parent.is_dir() or not 1 <= len(args.run_id) <= 128
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require distinct absolute paths, a new HTML export, and a bounded ASCII run identity')

    from equity_feature_duckdb.ef1 import (
        separate_output_path, synthetic_reference_classification_fixture, synthetic_reference_classification_context,
        synthetic_reference_action_fixture, synthetic_reference_action_context,
    )
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_features.ef1 import build_ef1_config
    from equity_feature_workers.equity_reference_classification import make_ef1_reference_classification_work
    from equity_feature_workers.equity_reference_action import make_ef1_reference_action_work
    from equity_feature_workers.ef1_runner import run_ef1_reference_classifications, run_ef1_reference_actions

    packet = (synthetic_reference_classification_fixture if classification else synthetic_reference_action_fixture)(case=args.case)
    context = (synthetic_reference_classification_context if classification else synthetic_reference_action_context)(case=args.case)
    cutoff = 20
    config = build_ef1_config(context,
        session=SessionSpec('fixture', 'S', 10, 30, 'UTC', scheduled_close_ns=30),
        window=WindowSpec(1, 'S', ('S',), 'completed_eod'),
        availability=AvailabilitySpec(cutoff, cutoff, cutoff),
        price_unit=None if classification else PriceUnit(0, 'USD'), adjustment=AdjustmentSpec() if classification else AdjustmentSpec('split', 'split-factors-v1', 'R1', 'anchor20'))
    declarations = []
    for role in packet.roles:
        batch = getattr(packet, role)
        if batch is None:
            declarations.append(InputManifest(role, hashlib.sha256(('synthetic-absent:' + role).encode('ascii')).hexdigest(),
                'R1', 'missing', None))
            continue
        payload = json.dumps(asdict(batch), ensure_ascii=True, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('ascii')
        if len(payload) > 1048576:
            parser.error('Synthetic input exceeds fixed input budget')
        coverage = batch.metadata.coverage
        state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
        declarations.append(InputManifest(role, hashlib.sha256(payload).hexdigest(),
            batch.metadata.source.snapshot_id, state, InputBinding(role, batch.kind, batch.metadata)))
    scope = 'ef1-synthetic-' + producer
    identity = args.run_id + '-' + producer + '-' + args.case
    builder = make_ef1_reference_classification_work if classification else make_ef1_reference_action_work
    item = builder(config, inputs=tuple(declarations), batches=packet.batches,
        instrument_id='X', job_id='job-' + identity, generation_id='generation-' + identity,
        partition_id='X-S', destination_scope=scope)
    requirements = SinkRequirements(max_results=1, max_chunk_bytes=262144,
        max_total_bytes=262144, max_result_cells=3 if classification else 2,
        max_evidence_rows=9 if classification else 12)
    output_path = separate_output_path(args.output_db, ())
    with DuckDBSink(output_path, scope, limits=requirements) as sink:
        runner = run_ef1_reference_classifications if classification else run_ef1_reference_actions
        outcome = runner((item,), sink, requirements=requirements)
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
        document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 reference admission</title>'
            '<style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:1rem}'
            'td,th{border:1px solid #aaa;padding:.5rem}table{border-collapse:collapse}'
            'pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>Supplied reference admission</h1>'
            '<p>Explicit synthetic case ' + escape(args.case) + '; producer ' + escape(producer)
            + '; provenance ' + escape(','.join(packet.provenance)) + '.</p>'
            '<p>Reference policy admission only; no price/factor transformation or numerical field readiness guarantee. Complete original bindings and selected bounded witnesses '
            'are retained; these witnesses are not an exhaustive reference audit. No private parity claim.</p>'
            '<p>Full SDK stored-result readback verified. C=K=E=' + escape(str(cutoff)) + '.</p>'
            '<p>Task: ' + escape(item.task.task_sha256) + '</p><p>Publication: '
            + escape(receipt.idempotency_key) + '</p><p>Content: ' + escape(receipt.content_sha256)
            + '</p><table><tr><th>Feature</th><th>Value</th><th>Unit</th><th>Status</th><th>Reasons</th></tr>'
            + rows + '</table><details><summary>Complete canonical result/bindings/evidence</summary><pre>'
            + escape(sdk.decode('ascii')) + '</pre></details><p>Integers are rendered as text; '
            'this viewer contains no JavaScript or remote assets.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as destination:
        destination.write(document)
    print(json.dumps(dict(verified_readback=True, task_sha256=item.task.task_sha256,
        content_sha256=receipt.content_sha256, html=str(args.html)), sort_keys=True))
    return 0
