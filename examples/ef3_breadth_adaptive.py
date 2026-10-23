"""Synthetic contract fixtures -> EF3 breadth/adaptive worker -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture: 49 declared-universe breadth fixtures (ef3-breadth1), 20 observed-breadth
fixtures (ef3-obsbreadth1) and 30 adaptive-volume fixtures (ef3-adaptive1), 99 in total under their contract ids
(AD06 prefix, AD07a to AD07c included). Each fixture is run once per selected output (one-output selection) and once
jointly when it selects several outputs; every run goes through the I/O adapter, the worker, the pure library, a
separate DuckDB sink, SDK readback, idempotent replay and close/reopen. The 62 typed refusal fixtures are
not driven by this script: the contract JSON stores only their error codes and descriptions, not inputs, so they
are not executed in this change (see docs/EF3_BREADTH_ADAPTIVE_WORKER.md).

The adapter (``equity_feature_duckdb.breadth_adaptive``) maps the supplied fixture rows into REFERENCE batches and the
configuration envelopes; this script compares only against the frozen contract literal, never against the library.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict
from html import escape
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_io_contracts import SinkRequirements
from equity_feature_workers import InputManifest

CONTRACT = 'BREADTH_ADAPTIVE_CONTRACT_PROPOSAL.json'
GROUPS = {'declared_fixtures': 'session_breadth', 'observed_fixtures': 'observed_breadth',
          'adaptive_fixtures': 'adaptive_volume'}
RESULT_COUNT = 99
ROLES = {
    'session_breadth': ('population', 'prior_reference_availability', 'current_return', 'cumulative_volume',
                        'endpoint_price', 'vwap', 'momentum', 'selected_high', 'selected_low'),
    'observed_breadth': ('members', 'change', 'qualifier_price', 'qualifier_volume'),
    'adaptive_volume': ('observations',),
}


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef3' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def fixtures_by_id(document: dict) -> dict:
    """All 99 result fixtures by contract id, each tagged with its producer."""
    rows = {}
    for group, producer in GROUPS.items():
        for row in document[group]:
            if row['id'] in rows:
                raise ValueError('duplicate contract id ' + row['id'])
            rows[row['id']] = dict(row, producer=producer, group_key=group)
    return rows


def accept(reference: dict, value) -> bool:
    """Value acceptance: a null literal matches null; a float literal is bit-exact (with its sign of zero)."""
    want = reference['value']
    if want is None or value is None:
        return want is None and value is None
    if isinstance(want, dict):
        r = float.fromhex(want['hex'])
        return type(value) is float and value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r)
    return type(value) is type(want) and value == want


def witness(row, producer: str) -> tuple:
    """Contract witness: (role, member) for breadth outputs and (sequence,) for adaptive observations."""
    if producer == 'adaptive_volume':
        # The library labels an observation witness 'seq:<sequence>'; the contract witness is the bare sequence.
        return (int(row.row_id[len('seq:'):]),)
    # The library labels a member witness as 'member:<id>'; the contract witness is the bare member id.
    member = row.row_id[len('member:'):] if row.row_id.startswith('member:') else row.row_id
    return (row.input_id, member)


def runs_for(fixture: dict) -> list[dict]:
    """Each run selects a set of outputs and carries the contract expectations for exactly those outputs."""
    expected = fixture['expected']
    runs = [dict(features=(feature,), expected={feature: expected[feature]}) for feature in sorted(expected)]
    if len(expected) > 1:
        runs.append(dict(features=tuple(sorted(expected)), expected=expected))
    return runs


def evidence_limit(fixture: dict) -> int:
    if fixture['producer'] == 'adaptive_volume':
        return fixture['input']['evidence_limit']
    return fixture['input']['params']['evidence_limit']


def case_for(fixture: dict):
    """The I/O adapter case of one contract fixture (declared, observed or adaptive)."""
    from equity_feature_duckdb import breadth_adaptive as adapter
    if fixture['producer'] == 'session_breadth':
        return adapter.declared_case(fixture['input'], entity_instrument='X')
    if fixture['producer'] == 'observed_breadth':
        return adapter.observed_case(fixture['input'], entity_instrument='X')
    return adapter.adaptive_case(fixture['input'], entity_instrument='X')


def declaration(role: str, batch) -> InputManifest:
    """Supplied-input manifest for one present role; an absent role is declared as missing with no binding."""
    raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
        InputBinding(role, batch.kind, batch.metadata))


def absent(role: str) -> InputManifest:
    return InputManifest(role, hashlib.sha256(('absent:' + role).encode('ascii')).hexdigest(), 'absent', 'missing', None)


def compare(result, fixture: dict, run: dict) -> list:
    """Values (acceptance rule), status, reasons, counts and witnesses against the contract literal for this run."""
    expected = run['expected']
    limit = evidence_limit(fixture)
    producer = fixture['producer']
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(expected)):
        problems.append(['selected outputs', [c.feature_id for c in result.values], sorted(expected)])
    found = {feature: [] for feature in expected}
    for row in result.evidence:
        found[row.feature_id].append(witness(row, producer))
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = expected[column.feature_id]
        stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['status'], want['reasons'], want['expected'], want['observed'])
        if not (accept(want, value) and stored == literal):
            problems.append([column.feature_id, 'value/quality', repr(value), stored, literal])
        if len(run['features']) == 1:
            want_w = [tuple(w) for w in want.get('evidence', [])]
            if found[column.feature_id] != want_w:
                problems.append([column.feature_id, 'witnesses', found[column.feature_id], want_w])
    if len(run['features']) > 1:
        want_joint = fixture.get('joint_selection_evidence') or [
            tuple(w) for feature in sorted(expected) for w in expected[feature].get('evidence', [])]
        want_joint = want_joint[:limit]
        flat = [witness(row, producer) for row in result.evidence]
        if flat != want_joint:
            problems.append(['joint evidence', flat, want_joint])
    return problems


def ordered(items, outcome) -> list:
    """Readbacks in item order, matched by task digest."""
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


def build_item(fixture: dict, run: dict, number: int, run_id: str, scope: str):
    """One worker item for one run. The task binds exactly the roles the selected outputs declare (library rule);
    supplied batches for other roles are not bound, because the library never reads them for this selection."""
    from equity_feature_workers.equity_breadth_adaptive import make_ef3_breadth_adaptive_work, required_roles
    label = '%s-%d' % (run_id, number)
    case = case_for(fixture)
    config = case.config(run['features'])
    roles = ROLES[fixture['producer']]
    needed = required_roles(config, run['features'])
    supplied = dict(zip(roles, case.slots))
    decls = tuple(declaration(role, supplied[role]) if supplied[role] is not None else absent(role) for role in needed)
    batches = tuple(supplied[role] for role in needed if supplied[role] is not None)
    return make_ef3_breadth_adaptive_work(config, features=run['features'], inputs=decls, batches=batches,
        instrument_id='X', job_id='job-' + label, generation_id='generation-' + label, partition_id='X-S',
        destination_scope=scope)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', required=True, help='contract result fixture id, e.g. DB01, OB01, AD06')
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
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.ef1_runner import run_ef3_breadth_adaptive

    fixtures = fixtures_by_id(contract_document(args.contract))
    if len(fixtures) != RESULT_COUNT or args.case not in fixtures:
        parser.error('Contract fixtures differ from the example case list')
    fixture = fixtures[args.case]
    family = fixture['producer']
    scope = 'ef3-' + family.replace('_', '-') + '-synthetic'
    runs = runs_for(fixture)
    items = [build_item(fixture, run, number, args.run_id, scope) for number, run in enumerate(runs)]
    limit = evidence_limit(fixture)
    # The sink refuses any requirement above its default limits (100 results, 10 MiB total, 100000 cells and rows).
    limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576,
        max_total_bytes=min(10485760, 1048576 * len(items)), max_result_cells=min(100000, 4 * len(items)),
        max_evidence_rows=min(100000, max(1, min(limit, 10000)) * len(items)))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = run_ef3_breadth_adaptive(tuple(items), sink, requirements=limits)
        if len(outcome.readbacks) != len(items) or not all(row.verified for row in outcome.readbacks):
            print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]}, sort_keys=True))
            return 1
        rows = ordered(items, outcome)
        stored = [row.results[0] for row in rows]
        problems = []
        for run, result in zip(runs, stored):
            found = compare(result, fixture, run)
            if found:
                problems.append(found)
        if problems:
            raise AssertionError('Calculated result differs from the contract fixture literal: ' + json.dumps(problems, default=str))
        sdk = b'\n'.join(encode_result(result) for result in stored)
        replay = run_ef3_breadth_adaptive(tuple(items), sink, requirements=limits)
        if (len(replay.readbacks) != len(items) or not all(row.verified for row in replay.readbacks)
                or [row.output for row in ordered(items, replay)] != [row.output for row in rows]):
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        for row, result in zip(rows, stored):
            restored = reopened.read(row.output.receipt)
            verify_receipt(row.output.receipt, row.output.envelope, restored)
            if tuple(map(encode_result, restored)) != (encode_result(result),):
                raise AssertionError('Full stored-result reopen differs')
    outputs = ', '.join(sorted(fixture['expected']))
    document_html = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF3 ' + escape(family) + '</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>EF3 ' + escape(family.replace('_', ' ')) + ' (contract fixture ' + escape(args.case) + ')</h1>'
        '<p>' + escape(fixture['description']) + '. Supplied adapter  ->  worker  ->  pure library  ->  separate DuckDB sink. '
        'Contract-literal match (value by acceptance rule, status, reasons, counts, witnesses and joint evidence), '
        'SDK readback, idempotent replay and close/reopen verified. Runs: ' + str(len(runs)) + '.</p>'
        '<p>Selected outputs across runs: ' + escape(outputs) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(rows[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every count, ratio, proximity, rate, baseline and acceleration value is the '
        'library result. No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document_html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(runs), 'task_sha256': items[0].task.task_sha256,
        'content_sha256': rows[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
