"""Independent full-result worker conformance for supplied periods and phases."""
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, replace
import json
import multiprocessing
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import decode_result, encode_result
from equity_feature_workers import CommandError, CommandErrorCode, WorkItem
from equity_feature_workers.manifests import decode_task, encode_task
from equity_feature_workers.equity_period import period_headers, required_period_inputs
from equity_feature_workers.equity_extended import extended_headers, required_extended_inputs
from equity_feature_workers.ef1_runner import run_ef1_periods, run_ef1_extended
from ef1_period_fixture import CASES, SUCCESS, ERRORS, fixture, work, calculate, spawned
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144,
    max_total_bytes=262144, max_result_cells=9, max_evidence_rows=72)


def run(items, sink=None):
    runner = run_ef1_periods if items[0].task.family == 'period' else run_ef1_extended
    return runner(items, sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


class EF1Periods(unittest.TestCase):
    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def test_all_sixty_two_frozen_packets_and_forty_two_full_success_results(self):
        self.assertEqual((len(CASES), len(SUCCESS), len(ERRORS)), (62, 42, 20))
        for name, packet in SUCCESS.items():
            with self.subTest(case=name):
                item = work(name)
                actual = calculate(item)
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                self.assertEqual(decode_result(encode_result(actual)), actual)

    def test_twenty_intentional_error_packets_translate_without_partial_result(self):
        for name, packet in ERRORS.items():
            with self.subTest(case=name):
                code = packet['expected_error']['code']
                expected = (CommandErrorCode.CALCULATION if code == 'overflow'
                    else CommandErrorCode.RESULT if code == 'inconsistent_identity' else CommandErrorCode.CONFIG)
                if name == 'CTRL_P_EMPTY_CANONICAL_CONTRADICTION':
                    # This intentionally impossible Coverage fails canonical DTO
                    # construction before there is a worker task to translate.
                    with self.assertRaises(c.ContractError) as caught:
                        fixture(name)
                    self.assertEqual(caught.exception.code.value, code)
                    continue
                self.rejects(lambda: calculate(work(name)), expected)

    def test_task_wire_sha_original_bindings_and_header_discovery(self):
        for name in SUCCESS:
            item = work(name)
            self.assertEqual(decode_task(encode_task(item.task)), item.task)
            self.assertEqual(decode_task(encode_task(item.task)).task_sha256, item.task.task_sha256)
            self.assertLessEqual(len(encode_task(item.task)), 1048576)
            result = calculate(item)
            self.assertEqual(result.metadata.inputs, tuple(sorted(
                (slot.binding for slot in item.task.inputs if slot.binding is not None), key=lambda value: value.role)))
            headers, requirements = ((period_headers, required_period_inputs) if item.task.family == 'period'
                else (extended_headers, required_extended_inputs))
            self.assertEqual(item.task.features, headers(item.task.config))
            self.assertTrue(requirements(item.task.config))
            self.assertEqual(len(result.values), 9 if item.task.family == 'period' else 8)

    def test_actual_spawn_matches_main_full_sdk_bytes(self):
        names = ('PERIOD_BASE', 'EXTENDED_BASE', 'PP06', 'EP04', 'EP05', 'EP06',
            'CTRL_EP09_RECONSTRUCTION_UNKNOWN')
        items = tuple(work(name) for name in names)
        with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context('spawn')) as pool:
            actual = tuple(pool.map(spawned, items))
        self.assertEqual(actual, tuple(encode_result(calculate(item)) for item in items))

    def test_present_original_batch_subsequence_reaches_pure_library(self):
        from equity_features import ef1
        for name in ('PERIOD_BASE', 'EXTENDED_BASE', 'CTRL_P_MISSING_PROOF',
                'CTRL_E_MISSING_PROOF', 'CTRL_P_MISSING_WHOLE_DAILY', 'CTRL_E_EMPTY_UNUSED_NULL_PRIOR'):
            item = work(name)
            with patch.object(ef1, 'compute_ef1', wraps=ef1.compute_ef1) as pure:
                calculate(item)
            forwarded = pure.call_args.args[0]
            self.assertEqual(forwarded, item.batches)
            self.assertTrue(all(left is right for left, right in zip(forwarded, item.batches)))
            self.assertTrue(all(type(batch) is c.CanonicalBatch for batch in forwarded))

    def test_role_order_missing_bijection_and_wrong_kind_are_refused(self):
        for name in ('PERIOD_BASE', 'EXTENDED_BASE'):
            item = work(name)
            self.rejects(lambda: item.calculate(item.task, tuple(reversed(item.batches))), CommandErrorCode.RESULT)
            self.rejects(lambda: item.calculate(item.task, item.batches[:-1]), CommandErrorCode.RESULT)
            self.rejects(lambda: item.calculate(item.task, (None,) + item.batches[1:]))
            self.rejects(lambda: item.calculate(replace(item.task, inputs=tuple(reversed(item.task.inputs))), item.batches))
            config, slots = fixture(name)
            self.rejects(lambda: work(name, slots=(slots[0], slots[2], slots[1])))

    def test_fixed_budget_history_and_full_features_cannot_be_relaxed(self):
        item = work()
        for task in (replace(item.task, features=item.task.features[:1]),
                replace(item.task, ordered_history=True, initialization_sha256='0' * 64)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.RESULT)
        for task in (replace(item.task, max_input_bytes=1), replace(item.task, max_input_bytes=1048577),
                replace(item.task, max_input_batches=4)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.LIMIT)

    def test_actual_source_metadata_substitution_refused(self):
        item = work()
        changed = replace(item.batches[0], metadata=replace(item.batches[0].metadata,
            source=replace(item.batches[0].metadata.source, snapshot_id='R2')))
        self.rejects(lambda: item.calculate(item.task, (changed,) + item.batches[1:]), CommandErrorCode.RESULT)

    def test_full_memory_readback_replay_joined_by_task_sha(self):
        for names in (('PERIOD_BASE', 'PP06', 'CTRL_P_MISSING_PROOF'),
                ('EXTENDED_BASE', 'EP04', 'EP05', 'EP06')):
            items, sink = tuple(work(name) for name in names), SerializedMemorySink()
            first, replay = run(items, sink), run(items, sink)
            expected = {item.task.task_sha256: encode_result(calculate(item)) for item in items}
            self.assertEqual({row.task_sha256 for row in first.readbacks}, set(expected))
            self.assertTrue(all(row.verified for row in first.readbacks + replay.readbacks))
            self.assertEqual(sink.calls.count('commit'), len(items))
            self.assertEqual({row.task_sha256: row.output for row in first.readbacks},
                {row.task_sha256: row.output for row in replay.readbacks})
            for row in first.readbacks:
                self.assertEqual(tuple(map(encode_result, row.results)), (expected[row.task_sha256],))

    def test_cross_producer_and_foreign_callback_refused(self):
        period, extended = work(), work('EXTENDED_BASE')
        self.rejects(lambda: run_ef1_periods((extended,), SerializedMemorySink(), requirements=LIMITS))
        self.rejects(lambda: run_ef1_extended((period,), SerializedMemorySink(), requirements=LIMITS))
        foreign = WorkItem(period.task, period.batches, extended.calculate)
        self.rejects(lambda: run_ef1_periods((foreign,), SerializedMemorySink(), requirements=LIMITS))
        self.rejects(lambda: period_headers(extended.task.config))
        self.rejects(lambda: extended_headers(period.task.config))

    def test_shared_job_or_generation_cannot_reuse_different_config(self):
        first, second = work(), work('PP06')
        self.assertNotEqual(first.task.config, second.task.config)
        for key in ('job_id', 'generation_id'):
            changed = WorkItem(replace(second.task, **{key: getattr(first.task, key)}), second.batches, second.calculate)
            self.rejects(lambda: run((first, changed)), CommandErrorCode.RESULT)

    def test_result_identity_rewrite_refused(self):
        from equity_features import ef1
        item, result = work(), calculate(work())
        for metadata in (replace(result.metadata, backend_version='0.0.4a4'),
                replace(result.metadata, config_digest='0' * 64), replace(result.metadata, inputs=tuple(
                    replace(binding, metadata=replace(binding.metadata,
                        source=replace(binding.metadata.source, snapshot_id='R9')))
                    for binding in result.metadata.inputs))):
            with patch.object(ef1, 'compute_ef1', return_value=replace(result, metadata=metadata)):
                self.rejects(lambda: calculate(item), CommandErrorCode.RESULT)

    def test_cancellation_prevents_publication(self):
        class Cancelled:
            def is_cancelled(self):
                return True
        sink = SerializedMemorySink()
        result = run_ef1_periods((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(result.execution.cancelled)
        self.assertNotIn('begin', sink.calls)

    def test_full_readback_failure_retains_committed_receipt(self):
        from equity_feature_workers.supervisor import BoundedSupervisor
        item, sink = work(), SerializedMemorySink()
        first = run((item,), sink)
        with patch.object(BoundedSupervisor, 'run', return_value=first.execution), \
                patch.object(sink, 'read', side_effect=ValueError('private detail')):
            failed = run((item,), sink)
        self.assertEqual(failed.readbacks[0].reason, 'READBACK_FAILED')
        self.assertEqual(failed.readbacks[0].output, first.readbacks[0].output)
        self.assertTrue(failed.readbacks[0].output.committed)

    def test_every_success_has_strict_original_known_at_witnesses(self):
        for name in SUCCESS:
            item, result = work(name), calculate(work(name))
            self.assertTrue(all(row.known_at_ns is not None and row.known_at_ns <= item.task.config.availability.knowledge_cutoff_ns
                and row.use == 'consumed' and row.exclusion_reason is None for row in result.evidence))

    def test_empty_phase_retains_predecessor_calendar_without_fake_zero_scope(self):
        for name in ('EP04', 'EP05', 'EP06', 'CTRL_E_EMPTY_UNUSED_NULL_PRIOR'):
            item = work(name)
            self.assertEqual(len(item.task.governed_sessions), 2)
            self.assertEqual(item.task.governed_sessions[-1].name, item.task.config.session.session_id)
            for batch in item.batches:
                if batch.metadata.scope is not None:
                    self.assertLess(batch.metadata.scope.start_ns, batch.metadata.scope.end_ns)
            self.assertEqual(json.loads(json.dumps(asdict(calculate(item)))), SUCCESS[name]['expected_result'])

    def test_synthetic_public_adapter_actual_duckdb_full_result_readback(self):
        from pathlib import Path
        import tempfile
        from equity_feature_duckdb.ef1 import synthetic_period_fixture, synthetic_extended_fixture
        from equity_feature_duckdb_sink import DuckDBSink
        for name, factory in (('PERIOD_BASE', synthetic_period_fixture),
                ('EXTENDED_BASE', synthetic_extended_fixture)):
            packet = factory()
            item = work(name, slots=packet.slots)
            self.assertTrue(all(mode == 'synthetic' for mode in packet.provenance))
            with tempfile.TemporaryDirectory(prefix='ef1-period-output-') as temporary:
                database = Path(temporary) / 'generated.duckdb'
                with DuckDBSink(database, item.task.destination_scope, limits=LIMITS) as sink:
                    outcome = run((item,), sink)
                    row, = outcome.readbacks
                    self.assertTrue(row.verified)
                    self.assertEqual(tuple(map(encode_result, row.results)), (encode_result(calculate(item)),))
                    self.assertEqual(json.loads(json.dumps(asdict(row.results[0]))), SUCCESS[name]['expected_result'])
                self.assertTrue(database.is_file())


if __name__ == '__main__':
    multiprocessing.freeze_support()
    unittest.main()
