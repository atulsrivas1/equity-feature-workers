"""Independent first-trade literals, exact stages and bounded route/sink closure."""
from collections import Counter
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import pickle
import tempfile
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import decode_result, encode_result, verify_receipt
from equity_feature_workers import CommandError, CommandErrorCode, WorkItem
from equity_feature_workers.manifests import decode_task, encode_task
from equity_feature_workers.equity_regular_first_trade_price import (
    ROLES, regular_first_trade_price_headers, required_regular_first_trade_price_inputs,
    make_ef1_regular_first_trade_price_work,
)
from equity_feature_workers.ef1_runner import run_ef1_regular_first_trade_price
from ef1_regular_first_trade_price_fixture import (
    CASES, SUCCESS, ERRORS, CONTROLS, fixture, request_fixture, literal_result, work, calculate,
)
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
    max_result_cells=1, max_evidence_rows=1)


def run(items, sink=None):
    return run_ef1_regular_first_trade_price(items,
        sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


def no_sink_calls(test, sink):
    test.assertFalse(any(call in sink.calls for call in ('begin', 'write', 'commit', 'read')))


class EF1RegularFirstTradePrice(unittest.TestCase):
    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def test_all_122_independent_whole_results_hex_sdk_and_separate_budgets(self):
        self.assertEqual((len(CASES), len(SUCCESS), len(ERRORS), len(CONTROLS)), (281, 122, 159, 17))
        for name, packet in SUCCESS.items():
            with self.subTest(case=name):
                item = work(name)
                actual = calculate(item)
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                value, = actual.values[0].values
                self.assertEqual(None if value is None else value.hex(), packet['expected_float_hex'])
                self.assertEqual(decode_result(encode_result(actual)), actual)
                self.assertLessEqual(len(encode_result(actual)), 262144)
                self.assertLessEqual(len(pickle.dumps(item.batches, protocol=5)), 1048576)
                self.assertLessEqual(len(encode_task(item.task)), 1048576)
                self.assertLessEqual(len(item.task.config.parameters[0].value.encode('ascii')), 4096)
                proof = fixture(name)[1][2]
                if proof is not None:
                    for value in proof.column('text').values:
                        self.assertLessEqual(len(value.encode('ascii')), 4096)

    def test_all_159_actual_earliest_refusals_including_n154_metadata(self):
        from equity_features import ef1
        observed = Counter()
        for name, packet in ERRORS.items():
            with self.subTest(case=name):
                marker = []
                try:
                    config, slots = request_fixture(packet['request'], marker)
                    marker[:] = ['canonical_validate_batch']
                    for batch in slots:
                        if batch is not None:
                            c.validate_batch(batch, required_fields=())
                    marker[:] = ['future_new_config_decode']
                    ef1.decode_ef1_context(config)
                    marker[:] = ['future_new_pure_semantic']
                    ef1.compute_ef1(slots, config, entity=c.EntityKey(**packet['request']['entity']))
                except c.ContractError as error:
                    self.assertEqual(error.code.value, packet['expected_error']['code'])
                    self.assertEqual(marker[0], packet['expected_error']['earliest_stage'])
                    observed[marker[0]] += 1
                else:
                    self.fail('Expected actual earliest refusal')
                if marker[0].endswith('_constructor') or marker[0] == 'transport_schema_admission':
                    continue
                expected = (CommandErrorCode.RESULT if packet['expected_error']['code'] == 'inconsistent_identity'
                    else CommandErrorCode.CALCULATION if packet['expected_error']['code'] == 'overflow'
                    else CommandErrorCode.CONFIG)
                self.rejects(lambda: calculate(work(name)), expected)
                sink = SerializedMemorySink()
                try:
                    outcome = run((work(name),), sink)
                except CommandError as error:
                    self.assertEqual(error.code, expected)
                else:
                    task, = outcome.execution.tasks
                    self.assertEqual(task.reason, expected.value)
                    self.assertIsNone(task.output)
                    self.assertIsNone(task.results)
                no_sink_calls(self, sink)
        self.assertEqual(observed, Counter(row['expected_error']['earliest_stage'] for row in ERRORS.values()))
        self.assertEqual(observed['BatchMetadata_constructor'], 1)

    def test_exact_roles_nullable_slots_and_target_only_manifest(self):
        from equity_features import ef1
        for name in SUCCESS:
            with self.subTest(case=name):
                item = work(name)
                self.assertEqual(tuple(slot.role for slot in item.task.inputs), ROLES)
                self.assertEqual(decode_task(encode_task(item.task)), item.task)
                interval, = item.task.governed_sessions
                session = item.task.config.session
                self.assertEqual((interval.name, interval.start_ns, interval.end_ns),
                    (session.session_id, session.open_ns, session.close_ns))
                self.assertEqual((item.task.ordered_history, item.task.warmup_sessions,
                    item.task.initialization_sha256, item.task.merge_policy, item.task.ownership),
                    (False, (), None, 'none', 'serialized_destination'))
                self.assertEqual(item.task.features, regular_first_trade_price_headers(item.task.config))
                self.assertTrue(required_regular_first_trade_price_inputs(item.task.config))
                with patch.object(ef1, 'compute_ef1', wraps=ef1.compute_ef1) as pure:
                    calculate(item)
                slots = pure.call_args.args[0]
                self.assertEqual(len(slots), 3)
                present = iter(item.batches)
                for declaration, value in zip(item.task.inputs, slots):
                    if declaration.binding is None:
                        self.assertIsNone(value)
                    else:
                        self.assertIs(value, next(present))

    def test_pickle5_budget_closed_family_config_history_binding_and_callback(self):
        item = work()
        size = len(pickle.dumps(item.batches, protocol=5))
        self.assertEqual(item.calculate(replace(item.task, max_input_bytes=size), item.batches), (calculate(item),))
        self.rejects(lambda: item.calculate(replace(item.task, max_input_bytes=size-1), item.batches), CommandErrorCode.LIMIT)
        for task in (replace(item.task, max_input_bytes=1048577), replace(item.task, max_input_batches=4)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.LIMIT)
        self.rejects(lambda: item.calculate(item.task, tuple(reversed(item.batches))), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(item.task, item.batches[:-1]), CommandErrorCode.RESULT)
        for task in (replace(item.task, family='regular_bar_prices'),
                replace(item.task, inputs=tuple(reversed(item.task.inputs))),
                replace(item.task, ordered_history=True, initialization_sha256='0'*64),
                replace(item.task, ownership='independent_destination')):
            self.rejects(lambda: item.calculate(task, item.batches))
        from equity_feature_workers.equity_regular_bar_prices import compute_regular_bar_price_inputs
        self.rejects(lambda: run((WorkItem(item.task, item.batches, compute_regular_bar_price_inputs),)))

    def test_all_17_controls_actual_dto_worker_and_oracle_layers_before_sink(self):
        from equity_features import ef1
        counts = Counter()
        for name, packet in CONTROLS.items():
            with self.subTest(case=name):
                stage = packet['expected_error']['earliest_stage']
                if stage.endswith('_constructor'):
                    with self.assertRaises(c.ContractError) as caught:
                        literal_result(packet['candidate_result'])
                    self.assertEqual(caught.exception.code.value, packet['expected_error']['code'])
                    counts['DTO'] += 1
                    continue
                config, slots = request_fixture(packet['request'])
                from ef1_anchor_fixture import declaration
                item = make_ef1_regular_first_trade_price_work(config,
                    inputs=tuple(declaration(role, value) for role, value in zip(ROLES, slots)),
                    batches=tuple(value for value in slots if value is not None), instrument_id='X',
                    job_id='control-'+name, generation_id='control-generation-'+name,
                    partition_id='X-S', destination_scope='first-trade-control')
                candidate = literal_result(packet['candidate_result'])
                self.assertEqual(decode_result(encode_result(candidate)), candidate)
                sink = SerializedMemorySink()
                if stage == 'integration_full_result_comparison':
                    expected_literal = next(case['expected_result'] for case in SUCCESS.values()
                        if case['request'] == packet['request'])
                    expected = calculate(item)
                    self.assertEqual(json.loads(json.dumps(asdict(expected))), expected_literal)
                    self.assertNotEqual(json.loads(json.dumps(asdict(candidate))), expected_literal)
                    with patch.object(ef1, 'compute_ef1', return_value=candidate):
                        self.assertEqual(calculate(item), candidate)
                    counts['integration'] += 1
                    continue
                with patch.object(ef1, 'compute_ef1', return_value=candidate):
                    self.rejects(lambda: calculate(item), CommandErrorCode.RESULT)
                    outcome = run((item,), sink)
                execution, = outcome.execution.tasks
                self.assertEqual(execution.reason, CommandErrorCode.RESULT.value)
                self.assertIsNone(execution.results)
                self.assertIsNone(execution.output)
                no_sink_calls(self, sink)
                counts['worker'] += 1
        self.assertEqual(counts, {'DTO': 3, 'worker': 13, 'integration': 1})

    def test_all_122_sdk_memory_sink_retry_full_literal_parity(self):
        for name, packet in SUCCESS.items():
            with self.subTest(case=name):
                item, sink = work(name), SerializedMemorySink()
                first, replay = run((item,), sink), run((item,), sink)
                self.assertTrue(first.readbacks[0].verified)
                self.assertTrue(replay.readbacks[0].verified)
                actual, = first.readbacks[0].results
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                self.assertEqual(first.readbacks[0].output, replay.readbacks[0].output)
                self.assertEqual(sink.calls.count('commit'), 1)

    def test_cancelled_no_sink_calls(self):
        class Cancelled:
            def is_cancelled(self):
                return True
        sink = SerializedMemorySink()
        outcome = run_ef1_regular_first_trade_price((work(),), sink,
            requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(outcome.execution.cancelled)
        no_sink_calls(self, sink)

    def test_all_122_lossless_adapter_worker_new_duckdb_retry_reopen(self):
        from equity_feature_duckdb.regular_first_trade_price import (
            RegularFirstTradePriceInputs, make_regular_first_trade_price_batch,
        )
        from equity_feature_duckdb_sink import DuckDBSink
        from ef1_anchor_fixture import declaration
        # An explicit owned output parent can retain every synthetic DB and lock for audit.
        parent = os.environ.get('EF1_FIRST_TRADE_REPORT_DIR')
        output = Path(tempfile.mkdtemp(prefix='ftdb-', dir=parent))
        self.assertLess(len(str(output / ('121.duckdb' + '.efio-duckdb1.lock'))), 260)
        for index, (name, packet) in enumerate(SUCCESS.items()):
            with self.subTest(case=name):
                config, original = fixture(name)
                slots = tuple(None if value is None else make_regular_first_trade_price_batch(value.kind,
                    tuple(column.name for column in value.columns),
                    tuple(tuple(column.values[row] for column in value.columns) for row in range(value.row_count)),
                    metadata=value.metadata) for value in original)
                supplied = RegularFirstTradePriceInputs(*slots,
                    provenance=tuple('unavailable' if value is None else 'synthetic' for value in slots))
                self.assertEqual(supplied.slots, original)
                item = make_ef1_regular_first_trade_price_work(config,
                    inputs=tuple(declaration(role, value) for role, value in zip(supplied.roles, supplied.slots)),
                    batches=supplied.batches, instrument_id=packet['request']['entity']['instrument_id'],
                    job_id='db-job-'+name, generation_id='db-generation-'+name,
                    partition_id='X-S', destination_scope='first-trade-test')
                database = output / f'{index:03}.duckdb'
                with DuckDBSink(database, 'first-trade-test', limits=LIMITS) as sink:
                    first = run((item,), sink)
                    row, = first.readbacks
                    self.assertTrue(row.verified)
                    actual, = row.results
                    self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                    replay = run((item,), sink)
                    self.assertTrue(replay.readbacks[0].verified)
                    self.assertEqual(replay.readbacks[0].output, row.output)
                    sdk = encode_result(actual)
                with DuckDBSink(database, 'first-trade-test', limits=LIMITS) as reopened:
                    restored = reopened.read(row.output.receipt)
                    verify_receipt(row.output.receipt, row.output.envelope, restored)
                    self.assertEqual(tuple(map(encode_result, restored)), (sdk,))
