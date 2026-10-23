"""Synthetic adapter -> channels worker -> NEW separate DuckDB -> verified HTML."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from html import escape
import json
import math
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

# The contract result fixtures (ef2-v1): 74 single-target fixtures (the prefix-property fixtures ICH-15,
# KEL-09, DON-09, PIV-10, PCT-14 and SQ-11 are plain stand-alone fixtures); the 21 refusals are not run here.
CONTRACT = 'ICHIMOKU_CHANNELS_CONTRACT_PROPOSAL.json'
FIXTURE_COUNT = 74


def contract_fixtures(override: Path | None) -> dict:
    """Result fixtures by id from the frozen contract JSON in the core repository."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef2' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return {row['id']: row for row in json.load(source)['fixtures']}


def case_ids() -> tuple[str, ...]:
    """Case ids for --case, read from the contract JSON (the example never carries expected values)."""
    return tuple(contract_fixtures(None))


def accept(acceptance: str | None, value, reference: dict) -> bool:
    """Fixture acceptance: R17 tolerance abs(y-r) <= max(8*ulp(r), 1e-12*abs(r)); bit-exact +0.0 or exact rank
    (one correctly rounded conversion); exact boolean."""
    want, hexed = reference['value'], reference.get('value_hex')
    if want is None or value is None:
        return want is None and value is None
    if isinstance(want, bool) or isinstance(value, bool):
        return type(want) is type(value) and want == value
    r = float.fromhex(hexed)
    rule = acceptance or ''
    if rule.startswith('reference is exactly 0'):
        return value.hex() == r.hex() and math.copysign(1.0, value) == 1.0
    if rule.startswith('exact'):
        return value.hex() == r.hex()
    return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))


def compare(result, expected: dict) -> list:
    """Compare value (by acceptance rule), status, reasons, counts and witnesses with the contract literal."""
    found = {feature: [] for feature in expected}
    for row in result.evidence:
        reason = row.exclusion_reason.value if row.exclusion_reason is not None else None
        found[row.feature_id].append([row.row_id, row.use, reason, row.boundary])
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(expected)):
        problems.append('selected outputs')
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = expected[column.feature_id]
        stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['status'], want['reasons'], want['expected'], want['observed'])
        if not (accept(want.get('acceptance'), value, want) and stored == literal):
            problems.append([column.feature_id, want.get('acceptance'), value, stored, literal])
        want_rows = [[row['row_id'], row['use'], row['reason'], row['boundary']] for row in want['evidence']]
        if found[column.feature_id] != want_rows:
            problems.append([column.feature_id, 'witnesses', found[column.feature_id], want_rows])
    return problems


def ordered(items, outcome) -> list:
    """Readbacks in item order, matched by task digest."""
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', required=True, help='contract result fixture id, e.g. ICH-01, KEL-04, PIV-03, SQ-12')
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
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
    from equity_feature_duckdb.channels import synthetic_channels_fixture, synthetic_channels_config, synthetic_channels_features
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.equity_channels import make_ef2_channels_work
    from equity_feature_workers.ef1_runner import run_ef2_channels

    fixtures = contract_fixtures(args.contract)
    if len(fixtures) != FIXTURE_COUNT or args.case not in fixtures:
        parser.error('Contract result fixtures differ from the example case list')
    fixture = fixtures[args.case]
    expected = fixture['expected']
    scope = 'ef2-channels-synthetic'
    features = tuple(sorted(synthetic_channels_features(args.case)))
    if features != tuple(sorted(expected)):
        parser.error('I/O feature selection differs from the contract fixture')
    packet = synthetic_channels_fixture(args.case)
    # The I/O synthetic config already carries the fixture evidence limit (result-wide, 0 allowed).
    config = synthetic_channels_config(args.case)
    declarations = []
    if packet.bars is None:
        declarations.append(InputManifest('bars', hashlib.sha256(b'synthetic-absent:bars').hexdigest(),
            'synthetic-absent', 'missing', None))
    for index, batch in enumerate(packet.bars or ()):
        role = 'bars:%03d' % index
        raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
        coverage = batch.metadata.coverage
        state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
        declarations.append(InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id,
            state, InputBinding(role, batch.kind, batch.metadata)))
    label = args.run_id
    items = [make_ef2_channels_work(config, features=features, inputs=tuple(declarations), batches=packet.batches,
        instrument_id='X', job_id='job-' + label, generation_id='generation-' + label,
        partition_id='X-S', destination_scope=scope)]
    limit = fixture['evidence_limit']
    limits = SinkRequirements(max_results=1, max_chunk_bytes=1048576, max_total_bytes=1048576,
        max_result_cells=len(features), max_evidence_rows=max(1, min(limit, 40960)))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef2_channels(tuple(items), sink, requirements=limits)
        if len(outcome.readbacks) != len(items) or not all(row.verified for row in outcome.readbacks):
            print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]}, sort_keys=True))
            return 1
        # The runner returns readbacks in its own order; pair them with the items by task digest.
        rows = ordered(items, outcome)
        result = rows[0].results[0]
        problems = compare(result, expected)
        if problems:
            raise AssertionError('Calculated result differs from the contract fixture literal: ' + json.dumps(problems, default=str))
        sdk = encode_result(result)
        receipt = rows[0].output.receipt
        replay = run_ef2_channels(tuple(items), sink, requirements=limits)
        if (len(replay.readbacks) != len(items) or not all(row.verified for row in replay.readbacks)
                or [row.output for row in ordered(items, replay)] != [row.output for row in rows]):
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        restored = reopened.read(receipt)
        verify_receipt(receipt, rows[0].output.envelope, restored)
        if tuple(map(encode_result, restored)) != (sdk,):
            raise AssertionError('Full stored-result reopen differs')
    document = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF2 channels</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>Ichimoku and price channels (Ichimoku, Keltner, Donchian, pivots, width percentile, squeeze)</h1><p>Contract fixture '
        + escape(args.case) + ': ' + escape(fixture['description'])
        + '. Actual input adapter  ->  worker  ->  pure calculation  ->  separate DuckDB sink. '
        'Contract-literal match (value by the fixture acceptance rule, status, reasons, coverage, witnesses), SDK readback, idempotent replay and close/reopen verified.</p>'
        '<p>Producer ' + escape(fixture['producer']) + '; selected outputs: ' + escape(', '.join(features))
        + '.</p><p>The worker orchestrates only; every window, recurrence, ranking and rounding is the library result. '
        'Original-source parity remains unqualified.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(receipt.content_sha256)
        + '.</p><h2>Complete canonical result, original bindings and selected witnesses</h2><pre>'
        + escape(sdk.decode('ascii')) + '</pre><p>No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'task_sha256': items[0].task.task_sha256, 'content_sha256': receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
