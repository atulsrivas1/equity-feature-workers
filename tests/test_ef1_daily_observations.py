"""Actual daily producer, transport and serialized publication conformance."""
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, replace
import json
import multiprocessing
import pickle
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import decode_result, encode_result
from equity_feature_workers import CommandError, CommandErrorCode, WorkItem
from equity_feature_workers.manifests import decode_task, encode_task
from equity_feature_workers.equity_daily_observations import (
    ROLES, daily_observations_headers, required_daily_observations_inputs)
from equity_feature_workers.ef1_runner import run_ef1_daily_observations
from ef1_daily_observations_fixture import CASES, SUCCESS, ERRORS, ORACLE, fixture, work, calculate, spawned
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(**ORACLE['sink_requirements'])


def run(items, sink=None):
    return run_ef1_daily_observations(items,
        sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


class EF1DailyObservations(unittest.TestCase):
    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def test_all_119_packets_and_91_full_literal_results(self):
        self.assertEqual((len(CASES), len(SUCCESS), len(ERRORS)), (119, 91, 28))
        for name, packet in SUCCESS.items():
            with self.subTest(case=name):
                actual = calculate(work(name))
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                self.assertEqual(decode_result(encode_result(actual)), actual)

    def test_28_real_stage_refusals_without_mock_inventory(self):
        from equity_features import ef1
        for name, packet in ERRORS.items():
            with self.subTest(case=name, stage=packet['expected_error']['stage']):
                code = packet['expected_error']['code']
                config, slots = fixture(name)
                with self.assertRaises(c.ContractError) as pure:
                    ef1.compute_ef1(slots, config, entity=c.EntityKey(**packet['entity']))
                self.assertEqual(pure.exception.code.value, code)
                expected = (CommandErrorCode.CALCULATION if code == 'overflow' else
                    CommandErrorCode.RESULT if code == 'inconsistent_identity' else CommandErrorCode.CONFIG)
                if name == 'X212-duplicate-record':
                    # Actual row count disagrees with immutable delivery metadata.
                    # Generic worker admission must refuse before pure DUPLICATE.
                    expected = CommandErrorCode.RESULT
                self.rejects(lambda: calculate(work(name)), expected)

    def test_wire_discovery_original_bindings_and_target_only_history(self):
        for name in SUCCESS:
            item = work(name)
            self.assertEqual(decode_task(encode_task(item.task)), item.task)
            self.assertEqual(decode_task(encode_task(item.task)).task_sha256, item.task.task_sha256)
            self.assertEqual(tuple(slot.role for slot in item.task.inputs), ROLES)
            self.assertEqual(len(item.task.governed_sessions), 1)
            self.assertEqual(item.task.features, daily_observations_headers(item.task.config))
            self.assertTrue(required_daily_observations_inputs(item.task.config))
            result = calculate(item)
            self.assertEqual(result.metadata.inputs, tuple(sorted(
                (slot.binding for slot in item.task.inputs if slot.binding is not None), key=lambda value: value.role)))
            self.assertEqual(len(result.values), 4)
            self.assertLessEqual(len(result.evidence), 28)

    def test_six_nullable_slots_preserve_actual_objects_and_empty_batches(self):
        from equity_features import ef1
        for name in SUCCESS:
            item = work(name)
            with patch.object(ef1, 'compute_ef1', wraps=ef1.compute_ef1) as pure:
                calculate(item)
            forwarded = pure.call_args.args[0]
            self.assertEqual(len(forwarded), 6)
            present = iter(item.batches)
            for slot, value in zip(item.task.inputs, forwarded):
                if slot.binding is None:
                    self.assertIsNone(value)
                else:
                    self.assertIs(value, next(present))

    def test_actual_pickle5_payload_boundary_is_separate_from_task_json(self):
        item = work('V235-literal-last252')
        size = len(pickle.dumps(item.batches, protocol=5))
        self.assertGreater(size, len(encode_task(item.task)))
        self.assertEqual(item.calculate(replace(item.task, max_input_bytes=size), item.batches), (calculate(item),))
        self.rejects(lambda: item.calculate(replace(item.task, max_input_bytes=size - 1), item.batches), CommandErrorCode.LIMIT)

    def test_roles_order_metadata_and_missing_bijection_refuse(self):
        item = work()
        self.rejects(lambda: item.calculate(item.task, tuple(reversed(item.batches))), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(item.task, item.batches[:-1]), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(item.task, (None,) + item.batches[1:]))
        self.rejects(lambda: item.calculate(replace(item.task, inputs=tuple(reversed(item.task.inputs))), item.batches))
        changed = replace(item.batches[0], metadata=replace(item.batches[0].metadata,
            source=replace(item.batches[0].metadata.source, snapshot_id='SUBSTITUTED')))
        self.rejects(lambda: item.calculate(item.task, (changed,) + item.batches[1:]), CommandErrorCode.RESULT)

    def test_closed_task_budget_features_and_callback(self):
        item = work()
        for task in (replace(item.task, max_input_bytes=1048577), replace(item.task, max_input_batches=7)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.LIMIT)
        for task in (replace(item.task, features=item.task.features[:1]), replace(item.task, family='period'),
                replace(item.task, ordered_history=True, initialization_sha256='0' * 64)):
            self.rejects(lambda: item.calculate(task, item.batches))
        from equity_feature_workers.equity_period import compute_period_inputs
        self.rejects(lambda: run((WorkItem(item.task, item.batches, compute_period_inputs),)))

    def test_actual_spawn_returns_complete_sdk_bytes(self):
        items = tuple(work(name) for name in ('V201-observed-not-scheduled', 'V235-literal-last252',
            'V223-prior-skips-invalid-populated-period', 'V318-common_observations-none'))
        with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context('spawn')) as pool:
            actual = tuple(pool.map(spawned, items))
        self.assertEqual(actual, tuple(encode_result(calculate(item)) for item in items))

    def test_full_readback_and_replay_joined_by_task_sha(self):
        items = tuple(work(name) for name in ('V201-observed-not-scheduled', 'V235-literal-last252',
            'V223-prior-skips-invalid-populated-period'))
        sink = SerializedMemorySink()
        first, replay = run(items, sink), run(items, sink)
        expected = {item.task.task_sha256: encode_result(calculate(item)) for item in items}
        self.assertTrue(all(row.verified for row in first.readbacks + replay.readbacks))
        self.assertEqual({row.task_sha256 for row in first.readbacks}, set(expected))
        self.assertEqual(sink.calls.count('commit'), len(items))
        self.assertEqual({row.task_sha256: row.output for row in first.readbacks},
            {row.task_sha256: row.output for row in replay.readbacks})
        for row in first.readbacks:
            self.assertEqual(tuple(map(encode_result, row.results)), (expected[row.task_sha256],))

    def test_result_backend_config_inputs_and_entity_substitution_refuse(self):
        from equity_features import ef1
        item = work()
        result = calculate(item)
        for metadata in (replace(result.metadata, backend_id='python-exact'),
                replace(result.metadata, backend_version='0.0.4a4'),
                replace(result.metadata, config_digest='0' * 64), replace(result.metadata, inputs=tuple(
                    replace(binding, metadata=replace(binding.metadata,
                        source=replace(binding.metadata.source, snapshot_id='SUBSTITUTED')))
                    for binding in result.metadata.inputs))):
            with patch.object(ef1, 'compute_ef1', return_value=replace(result, metadata=metadata)):
                self.rejects(lambda: calculate(item), CommandErrorCode.RESULT)
        changed = replace(result, values=tuple(replace(column, entities=(c.EntityKey('OTHER',
            item.task.config.session.session_id),)) for column in result.values),
            quality=tuple(replace(row, entity=c.EntityKey('OTHER', item.task.config.session.session_id))
                for row in result.quality), evidence=())
        with patch.object(ef1, 'compute_ef1', return_value=changed):
            self.rejects(lambda: calculate(item), CommandErrorCode.RESULT)

    def test_exact_result_math_policy_and_declared_evidence_limit_before_publication(self):
        from equity_features import ef1
        item = work()
        actual = calculate(item)
        self.assertLessEqual(len(actual.evidence), 27)
        for field, value in (('math_policy_version', 'substituted'),
                ('evidence_limit', 27), ('evidence_limit', 29)):
            with self.subTest(field=field, value=value):
                # A genuine valid canonical DTO retaining the real computed
                # values, quality, evidence and all other metadata identities.
                substituted = replace(actual, metadata=replace(actual.metadata, **{field: value}))
                self.assertIs(type(substituted), c.FeatureResult)
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
                self.assertEqual(outcome.readbacks[0].reason, CommandErrorCode.RESULT.value)
                self.assertFalse(any(call in sink.calls for call in ('begin', 'write', 'commit', 'read')))

    def test_cancellation_prevents_publication(self):
        class Cancelled:
            def is_cancelled(self):
                return True
        sink = SerializedMemorySink()
        result = run_ef1_daily_observations((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(result.execution.cancelled)
        self.assertNotIn('begin', sink.calls)

    def test_full_readback_failure_keeps_committed_receipt(self):
        from equity_feature_workers.supervisor import BoundedSupervisor
        item, sink = work(), SerializedMemorySink()
        first = run((item,), sink)
        with patch.object(BoundedSupervisor, 'run', return_value=first.execution), \
                patch.object(sink, 'read', side_effect=ValueError('private detail')):
            failed = run((item,), sink)
        self.assertEqual(failed.readbacks[0].reason, 'READBACK_FAILED')
        self.assertEqual(failed.readbacks[0].output, first.readbacks[0].output)

    def test_same_job_or_generation_cannot_rebind_config(self):
        first, second = work(), work('V235-literal-last252')
        for key in ('job_id', 'generation_id'):
            changed = WorkItem(replace(second.task, **{key: getattr(first.task, key)}), second.batches, second.calculate)
            self.rejects(lambda: run((first, changed)), CommandErrorCode.RESULT)
