"""Actual endpoint route, complete literal parity and pre-publication closure."""
from dataclasses import asdict, replace
import json
import pickle
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import decode_result, encode_result
from equity_feature_workers import CommandError, CommandErrorCode, WorkItem
from equity_feature_workers.manifests import decode_task, encode_task
from equity_feature_workers.equity_gap_endpoint import ROLES, gap_endpoint_headers, required_gap_endpoint_inputs
from equity_feature_workers.ef1_runner import run_ef1_gap_endpoints
from ef1_gap_endpoint_fixture import CASES, SUCCESS, ERRORS, fixture, work, calculate
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
    max_result_cells=2, max_evidence_rows=5)


def run(items, sink=None):
    return run_ef1_gap_endpoints(items, sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


class EF1GapEndpoint(unittest.TestCase):
    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def test_all_114_packets_and_71_full_literal_results(self):
        self.assertEqual((len(CASES), len(SUCCESS), len(ERRORS)), (114, 71, 43))
        for name, packet in SUCCESS.items():
            with self.subTest(case=name):
                actual = calculate(work(name))
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                self.assertEqual(decode_result(encode_result(actual)), actual)
                self.assertLessEqual(len(encode_result(actual)), 262144)
                item = work(name)
                self.assertLessEqual(len(pickle.dumps(item.batches, protocol=5)), 1048576)
                self.assertLessEqual(len(encode_task(item.task)), 1048576)
                context = item.task.config.parameters[0].value
                self.assertTrue(context.isascii())
                self.assertLessEqual(len(context), 4096)
                proof = next((batch for declaration, batch in zip(item.task.inputs, fixture(name)[1])
                    if declaration.role == 'population_proof'), None)
                if proof is not None:
                    text = proof.column('text')
                    if text is not None:
                        for value in text.values:
                            self.assertTrue(value.isascii())
                            self.assertLessEqual(len(value), 4096)

    def test_43_negative_packets_actual_constructor_canonical_pure_and_worker_stages(self):
        from equity_features import ef1
        for name, packet in ERRORS.items():
            with self.subTest(case=name):
                code = packet['expected_error']['code']
                if name == 'E30_unknown_expected':
                    with self.assertRaises(c.ContractError) as caught:
                        fixture(name)
                    self.assertEqual(caught.exception.code.value, code)
                    continue  # Constructor refusal is explicitly not pure coverage.
                config, slots = fixture(name)
                with self.assertRaises(c.ContractError) as caught:
                    ef1.compute_ef1(slots, config, entity=c.EntityKey(**packet['entity']))
                self.assertEqual(caught.exception.code.value, code)
                if packet['expected_error']['expected_earliest_stage'] == 'canonical_validate_batch':
                    codes = []
                    for value in slots:
                        if value is not None:
                            try:
                                c.validate_batch(value)
                            except c.ContractError as error:
                                codes.append(error.code.value)
                    self.assertIn(code, codes)
                if name == 'Q34_duplicate_actual_input_id':
                    self.rejects(lambda: work(name), CommandErrorCode.CONFIG)
                    continue  # Actual manifest binding refusal precedes callback.
                expected = (CommandErrorCode.RESULT if code == 'inconsistent_identity'
                    else CommandErrorCode.CALCULATION if code == 'overflow' else CommandErrorCode.CONFIG)
                self.rejects(lambda: calculate(work(name)), expected)

    def test_four_roles_present_subsequence_and_original_objects(self):
        from equity_features import ef1
        for name in SUCCESS:
            item = work(name)
            self.assertEqual(tuple(slot.role for slot in item.task.inputs), ROLES)
            self.assertEqual(decode_task(encode_task(item.task)), item.task)
            self.assertEqual(len(item.task.governed_sessions), 2)
            self.assertEqual(item.task.features, gap_endpoint_headers(item.task.config))
            self.assertTrue(required_gap_endpoint_inputs(item.task.config))
            with patch.object(ef1, 'compute_ef1', wraps=ef1.compute_ef1) as pure:
                calculate(item)
            slots = pure.call_args.args[0]
            self.assertEqual(len(slots), 4)
            actual = iter(item.batches)
            for declaration, value in zip(item.task.inputs, slots):
                if declaration.binding is None:
                    self.assertIsNone(value)
                else:
                    self.assertIs(value, next(actual))

    def test_actual_pickle5_budget_separate_from_task_json(self):
        item = work()
        size = len(pickle.dumps(item.batches, protocol=5))
        self.assertLessEqual(size, 1048576)
        self.assertLessEqual(len(encode_task(item.task)), 1048576)
        self.assertEqual(item.calculate(replace(item.task, max_input_bytes=size), item.batches), (calculate(item),))
        self.rejects(lambda: item.calculate(replace(item.task, max_input_bytes=size-1), item.batches), CommandErrorCode.LIMIT)
        for task in (replace(item.task, max_input_bytes=1048577), replace(item.task, max_input_batches=5)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.LIMIT)

    def test_closed_callback_role_order_binding_and_governed_geometry(self):
        item = work()
        self.rejects(lambda: item.calculate(item.task, tuple(reversed(item.batches))), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(item.task, item.batches[:-1]), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(replace(item.task, inputs=tuple(reversed(item.task.inputs))), item.batches))
        for task in (replace(item.task, family='path_gap'), replace(item.task, features=item.task.features[:1]),
                replace(item.task, ordered_history=True, initialization_sha256='0'*64)):
            self.rejects(lambda: item.calculate(task, item.batches))
        from equity_feature_workers.equity_path_gap import compute_path_gap_inputs
        self.rejects(lambda: run((WorkItem(item.task, item.batches, compute_path_gap_inputs),)))

    def test_complete_returned_metadata_identity_refuses_before_any_sink_publication(self):
        from equity_features import ef1
        item = work('E08_single_trade')
        result = calculate(item)
        self.assertLessEqual(len(result.evidence), 4)  # Both4 and6 substitutions are valid DTOs.
        metadata = result.metadata
        alternatives = [replace(metadata, **{field: value}) for field, value in (
            ('math_policy_version', 'unknown'), ('evidence_limit', 4), ('evidence_limit', 6),
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
        # A complete different header identity remains a valid DTO but is not this route.
        old_id = result.values[0].feature_id
        new_id = 'session.gap_endpoint.substituted'
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

    def test_all_71_real_results_publish_and_replay_full_sdk_readback(self):
        for name in SUCCESS:
            with self.subTest(case=name):
                item, sink = work(name), SerializedMemorySink()
                first, replay = run((item,), sink), run((item,), sink)
                self.assertTrue(first.readbacks[0].verified)
                self.assertTrue(replay.readbacks[0].verified)
                self.assertEqual(tuple(map(encode_result, first.readbacks[0].results)), (encode_result(calculate(item)),))
                self.assertEqual(first.readbacks[0].output, replay.readbacks[0].output)
                self.assertEqual(sink.calls.count('commit'), 1)

    def test_cancellation_no_publication(self):
        class Cancelled:
            def is_cancelled(self):
                return True
        sink = SerializedMemorySink()
        result = run_ef1_gap_endpoints((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(result.execution.cancelled)
        self.assertFalse(any(call in sink.calls for call in ('begin', 'write', 'commit', 'read')))
