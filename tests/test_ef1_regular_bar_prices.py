"""Independent full literal parity, actual refusal stages and pre-sink closure."""
from collections import Counter
from dataclasses import asdict, replace
import json
import pickle
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import decode_result, encode_result
from equity_feature_workers import CommandError, CommandErrorCode, WorkItem
from equity_feature_workers.manifests import decode_task, encode_task
from equity_feature_workers.equity_regular_bar_prices import ROLES, regular_bar_price_headers, required_regular_bar_price_inputs
from equity_feature_workers.ef1_runner import run_ef1_regular_bar_prices
from ef1_regular_bar_prices_fixture import CASES, SUCCESS, ERRORS, fixture, work, calculate
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
    max_result_cells=2, max_evidence_rows=2)


def run(items, sink=None):
    return run_ef1_regular_bar_prices(items, sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


class EF1RegularBarPrices(unittest.TestCase):
    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def test_all_100_packets_and_46_full_literal_results(self):
        self.assertEqual((len(CASES), len(SUCCESS), len(ERRORS)), (100, 46, 54))
        for name, packet in SUCCESS.items():
            with self.subTest(case=name):
                item = work(name)
                actual = calculate(item)
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                self.assertEqual(decode_result(encode_result(actual)), actual)
                self.assertLessEqual(len(encode_result(actual)), 262144)
                self.assertLessEqual(len(pickle.dumps(item.batches, protocol=5)), 1048576)
                self.assertLessEqual(len(encode_task(item.task)), 1048576)
                context = item.task.config.parameters[0].value
                self.assertTrue(context.isascii())
                self.assertLessEqual(len(context), 4096)
                proof = fixture(name)[1][2]
                if proof is not None:
                    for value in proof.column('text').values:
                        self.assertTrue(value.isascii())
                        self.assertLessEqual(len(value), 4096)

    def test_54_refusals_actual_constructor_canonical_config_and_pure_stages(self):
        from equity_features import ef1
        observed = Counter()
        for name, packet in ERRORS.items():
            with self.subTest(case=name):
                stage = 'Coverage_constructor' if name == 'N11_complete_unknown_expected' else 'CanonicalBatch_constructor'
                try:
                    config, slots = fixture(name)
                    stage = 'canonical_validate_batch'
                    for value in slots:
                        if value is not None:
                            c.validate_batch(value, required_fields=())
                    stage = 'endpoint_config_decode'
                    ef1.decode_ef1_context(config)
                    stage = 'endpoint_pure_semantic_or_geometry'
                    ef1.compute_ef1(slots, config, entity=c.EntityKey(**packet['entity']))
                except c.ContractError as error:
                    self.assertEqual(error.code.value, packet['expected_error']['code'])
                    self.assertEqual(stage, packet['expected_error']['expected_earliest_stage'])
                    observed[stage] += 1
                else:
                    self.fail('Expected refusal, not unsupported-route success')
                if stage.endswith('_constructor'):
                    continue  # Constructor refusal is not callback execution.
                expected = (CommandErrorCode.RESULT if packet['expected_error']['code'] == 'inconsistent_identity'
                    else CommandErrorCode.CALCULATION if packet['expected_error']['code'] == 'overflow' else CommandErrorCode.CONFIG)
                self.rejects(lambda: calculate(work(name)), expected)
        self.assertEqual(observed, {'Coverage_constructor': 1, 'CanonicalBatch_constructor': 3,
            'canonical_validate_batch': 10, 'endpoint_config_decode': 3, 'endpoint_pure_semantic_or_geometry': 37})

    def test_three_roles_original_present_subsequence_and_target_only_geometry(self):
        from equity_features import ef1
        for name in SUCCESS:
            with self.subTest(case=name):
                item = work(name)
                self.assertEqual(tuple(slot.role for slot in item.task.inputs), ROLES)
                self.assertEqual(decode_task(encode_task(item.task)), item.task)
                self.assertEqual(len(item.task.governed_sessions), 1)
                interval, = item.task.governed_sessions
                session = item.task.config.session
                self.assertEqual((interval.name, interval.start_ns, interval.end_ns),
                    (session.session_id, session.open_ns, session.close_ns))
                self.assertEqual(item.task.ownership, 'serialized_destination')
                self.assertEqual(item.task.features, regular_bar_price_headers(item.task.config))
                self.assertTrue(required_regular_bar_price_inputs(item.task.config))
                with patch.object(ef1, 'compute_ef1', wraps=ef1.compute_ef1) as pure:
                    calculate(item)
                slots = pure.call_args.args[0]
                self.assertEqual(len(slots), 3)
                actual = iter(item.batches)
                for declaration, value in zip(item.task.inputs, slots):
                    if declaration.binding is None:
                        self.assertIsNone(value)
                    else:
                        self.assertIs(value, next(actual))

    def test_pickle5_budget_separate_from_task_json(self):
        item = work()
        size = len(pickle.dumps(item.batches, protocol=5))
        self.assertEqual(item.calculate(replace(item.task, max_input_bytes=size), item.batches), (calculate(item),))
        self.rejects(lambda: item.calculate(replace(item.task, max_input_bytes=size-1), item.batches), CommandErrorCode.LIMIT)
        for task in (replace(item.task, max_input_bytes=1048577), replace(item.task, max_input_batches=4)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.LIMIT)

    def test_closed_callback_roles_bindings_family_history_and_ownership(self):
        item = work()
        self.rejects(lambda: item.calculate(item.task, tuple(reversed(item.batches))), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(item.task, item.batches[:-1]), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(replace(item.task, inputs=tuple(reversed(item.task.inputs))), item.batches))
        for task in (replace(item.task, family='path_gap_endpoint'), replace(item.task, features=item.task.features[:1]),
                replace(item.task, ordered_history=True, initialization_sha256='0'*64),
                replace(item.task, ownership='independent_destination')):
            self.rejects(lambda: item.calculate(task, item.batches))
        from equity_feature_workers.manifests import ManifestError, ManifestErrorCode
        for interval in (c.IntervalSpec('T', 10, 30), c.IntervalSpec('S', 9, 30)):
            with self.assertRaises(ManifestError) as caught:
                replace(item.task, governed_sessions=(interval,))
            self.assertEqual(caught.exception.code, ManifestErrorCode.BINDING)
        # The manifest constructor closes these identities before callback execution.
        from equity_feature_workers.equity_gap_endpoint import compute_gap_endpoint_inputs
        self.rejects(lambda: run((WorkItem(item.task, item.batches, compute_gap_endpoint_inputs),)))

    def test_complete_result_identity_and_explicit_math_evidence_closure_before_any_sink(self):
        from equity_features import ef1
        # R08 has one witness: evidence_limit=1 is a valid DTO and reaches the worker.
        name, = (name for name in SUCCESS if name.startswith('R08'))
        item = work(name)
        result = calculate(item)
        self.assertEqual(len(result.evidence), 1)
        metadata = result.metadata
        alternatives = [replace(metadata, **{field: value}) for field, value in (
            ('math_policy_version', 'unknown'), ('evidence_limit', 1), ('evidence_limit', 3),
            ('backend_id', 'ef1'), ('backend_version', '0.0.4a4'), ('config_digest', '0'*64))]
        alternatives.append(replace(metadata, namespace='OTHER', inputs=tuple(replace(binding,
            metadata=replace(binding.metadata, namespace='OTHER')) for binding in metadata.inputs)))
        alternatives.append(replace(metadata, availability=replace(metadata.availability, evaluation_ns=15)))
        alternatives.append(replace(metadata, inputs=tuple(replace(binding, metadata=replace(binding.metadata,
            source=replace(binding.metadata.source, snapshot_id='SUBSTITUTED'))) for binding in metadata.inputs)))
        changed = [replace(result, metadata=value) for value in alternatives]
        other = c.EntityKey('OTHER', item.task.config.session.session_id)
        changed.append(replace(result, values=tuple(replace(column, entities=(other,)) for column in result.values),
            quality=tuple(replace(row, entity=other) for row in result.quality), evidence=()))
        old_id, new_id = result.values[0].feature_id, 'session.close.substituted'
        changed.append(replace(result,
            values=tuple(replace(column, feature_id=new_id) if column.feature_id == old_id else column for column in result.values),
            quality=tuple(replace(row, feature_id=new_id) if row.feature_id == old_id else row for row in result.quality), evidence=()))
        for substituted in changed:
            with self.subTest(metadata=substituted.metadata):
                sink = SerializedMemorySink()
                with patch.object(ef1, 'compute_ef1', return_value=substituted) as pure:
                    self.rejects(lambda: calculate(item), CommandErrorCode.RESULT)
                    outcome = run((item,), sink)
                self.assertEqual(pure.call_count, 2)
                task, = outcome.execution.tasks
                self.assertEqual(task.reason, CommandErrorCode.RESULT.value)
                self.assertIsNone(task.results)
                self.assertIsNone(task.output)
                self.assertFalse(outcome.readbacks[0].verified)
                self.assertFalse(any(call in sink.calls for call in ('begin', 'write', 'commit', 'read')))

    def test_all_46_results_actual_sdk_publication_and_idempotent_full_readback(self):
        for name in SUCCESS:
            with self.subTest(case=name):
                item, sink = work(name), SerializedMemorySink()
                first, replay = run((item,), sink), run((item,), sink)
                self.assertTrue(first.readbacks[0].verified)
                self.assertTrue(replay.readbacks[0].verified)
                self.assertEqual(tuple(map(encode_result, first.readbacks[0].results)), (encode_result(calculate(item)),))
                self.assertEqual(first.readbacks[0].output, replay.readbacks[0].output)
                self.assertEqual(sink.calls.count('commit'), 1)

    def test_all_46_adapter_worker_duckdb_retry_close_reopen_full_literal_parity(self):
        from equity_feature_duckdb.regular_bar_prices import RegularBarPriceInputs, make_regular_bar_price_batch
        from equity_feature_duckdb_sink import DuckDBSink
        from equity_feature_io_sdk import verify_receipt
        from ef1_anchor_fixture import declaration
        from equity_feature_workers.equity_regular_bar_prices import make_ef1_regular_bar_price_work
        # Preserve this exclusive private output tree for review; no sidecar deletion.
        output = Path(tempfile.mkdtemp(prefix='ef1-regular-bar-worker-'))
        for index, (name, packet) in enumerate(SUCCESS.items()):
            with self.subTest(case=name):
                config, original = fixture(name)
                slots = tuple(None if value is None else make_regular_bar_price_batch(value.kind,
                    tuple(column.name for column in value.columns),
                    tuple(tuple(column.values[row] for column in value.columns) for row in range(value.row_count)),
                    metadata=value.metadata) for value in original)
                supplied = RegularBarPriceInputs(*slots,
                    provenance=tuple('unavailable' if value is None else 'synthetic' for value in slots))
                self.assertEqual(supplied.slots, original)
                item = make_ef1_regular_bar_price_work(config,
                    inputs=tuple(declaration(role, value) for role, value in zip(supplied.roles, supplied.slots)),
                    batches=supplied.batches, instrument_id='X', job_id='db-job-' + name,
                    generation_id='db-generation-' + name, partition_id='X-S', destination_scope='regular-bar-test')
                database = output / f'{index:02}.duckdb'
                with DuckDBSink(database, 'regular-bar-test', limits=LIMITS) as sink:
                    first = run((item,), sink)
                    row, = first.readbacks
                    self.assertTrue(row.verified)
                    actual, = row.results
                    self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                    replay = run((item,), sink)
                    self.assertTrue(replay.readbacks[0].verified)
                    self.assertEqual(replay.readbacks[0].output, row.output)
                    sdk = encode_result(actual)
                with DuckDBSink(database, 'regular-bar-test', limits=LIMITS) as reopened:
                    restored = reopened.read(row.output.receipt)
                    verify_receipt(row.output.receipt, row.output.envelope, restored)
                    self.assertEqual(tuple(map(encode_result, restored)), (sdk,))

    def test_cancelled_no_publication(self):
        class Cancelled:
            def is_cancelled(self):
                return True
        sink = SerializedMemorySink()
        result = run_ef1_regular_bar_prices((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(result.execution.cancelled)
        self.assertFalse(any(call in sink.calls for call in ('begin', 'write', 'commit', 'read')))
