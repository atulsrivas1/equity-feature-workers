"""Authored reference worker conformance; runtime validation remains pending."""
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
from equity_feature_workers.equity_reference_classification import (
    compute_reference_classification_inputs, reference_classification_headers,
    required_reference_classification_inputs, required_reference_classification_alternatives,
)
from equity_feature_workers.equity_reference_action import (
    compute_reference_action_inputs, reference_action_headers,
    required_reference_action_inputs, required_reference_action_alternatives,
)
from equity_feature_workers.ef1_runner import (
    run_ef1_reference_classifications, run_ef1_reference_actions, run_ef1_statuses,
)
from ef1_reference_fixture import CASES, fixture, work, calculate, replace_column, spawned
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144,
    max_total_bytes=262144, max_result_cells=3, max_evidence_rows=12)


def run(items, sink=None):
    runner = (run_ef1_reference_classifications if items[0].task.family == 'reference_classification'
        else run_ef1_reference_actions)
    return runner(items, sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


class EF1References(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import equity_features
        import equity_feature_workers
        if (equity_features.__version__, c.__version__, equity_feature_workers.__version__) != (
                '0.0.4a5', '0.0.4a4', '0.1.0a15'):
            raise AssertionError('Exact candidate versions required')

    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def test_all_fifty_five_independent_full_canonical_packets(self):
        self.assertEqual(len(CASES), 55)
        for name, packet in CASES.items():
            with self.subTest(case=name):
                item = work(name)
                actual = calculate(item)
                self.assertEqual(item.task.config.digest, packet['config_digest'])
                # JSON projects enum/tuple dataclass fields, never regenerates expected data.
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                self.assertEqual(decode_result(encode_result(actual)), actual)

    def test_exact_nullable_slots_are_original_objects_for_every_missing_role(self):
        from equity_features import ef1
        for name in ('R01-CANONICAL', 'R03-CANONICAL'):
            config, slots = fixture(name)
            for index in range(len(slots)):
                absent = tuple(None if i == index else batch for i, batch in enumerate(slots))
                item = work(name, slots=absent)
                with patch.object(ef1, 'compute_ef1', wraps=ef1.compute_ef1) as pure:
                    result = calculate(item)
                forwarded = pure.call_args.args[0]
                self.assertEqual(forwarded, absent)
                self.assertTrue(all(left is right for left, right in zip(forwarded, absent)))
                self.assertEqual(len(result.metadata.inputs), len(slots) - 1)
                self.assertIsNone(item.task.inputs[index].binding)

    def test_present_empty_is_not_missing(self):
        for name in ('R02-CANONICAL', 'RAW-ACTUAL-R1-empty-known_at'):
            item = work(name)
            self.assertTrue(any(batch.row_count == 0 for batch in item.batches))
            self.assertTrue(all(declaration.binding is not None for declaration in item.task.inputs))
            self.assertEqual(len(calculate(item).metadata.inputs), len(item.task.inputs))

    def test_manifest_task_wire_and_full_actual_binding_identity(self):
        for name in CASES:
            item = work(name)
            self.assertEqual(decode_task(encode_task(item.task)), item.task)
            self.assertLessEqual(len(encode_task(item.task)), 1048576)
            result = calculate(item)
            self.assertEqual(result.metadata.inputs, tuple(sorted(
                (slot.binding for slot in item.task.inputs if slot.binding is not None), key=lambda row: row.role)))
            self.assertEqual(result.metadata.config_digest, item.task.config.digest)
            self.assertEqual(result.metadata.backend_version, '0.0.4a5')

    def test_main_and_actual_spawn_full_result_bytes_match(self):
        items = tuple(work(name) for name in ('R01-CANONICAL', 'INDUSTRY-CANONICAL', 'R03-CANONICAL',
            'ACTION-late_proof-reconstruction', 'RAW-ACTUAL-R1-late_factor-reconstruction'))
        with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context('spawn')) as pool:
            actual = tuple(pool.map(spawned, items))
        self.assertEqual(actual, tuple(encode_result(calculate(item)) for item in items))

    def test_all_three_classification_kinds_preserve_exact_fixture_results(self):
        for name in ('R01-CANONICAL', 'INDUSTRY-CANONICAL', 'UNIVERSE-CANONICAL'):
            item = work(name)
            self.assertEqual(item.task.features, reference_classification_headers(item.task.config))
            self.assertEqual(len(required_reference_classification_inputs(item.task.config)), 3)
            self.assertEqual(required_reference_classification_alternatives(item.task.config), ())
            self.assertEqual(calculate(item).values[0].values, ('B',))

    def test_action_discovery_is_three_complete_or_variants(self):
        config, _ = fixture('R03-CANONICAL')
        self.assertEqual(len(required_reference_action_inputs(config)), 5)
        variants = required_reference_action_alternatives(config)
        self.assertEqual(len(variants), 3)
        self.assertTrue(all(len(variant) == 6 for variant in variants))
        self.assertEqual({variant[0].kind for variant in variants}, {c.DataKind.TRADE, c.DataKind.BAR, c.DataKind.DAILY})
        self.assertTrue(all(variant[1:] == required_reference_action_inputs(config) for variant in variants))

    def test_market_union_uses_actual_kind_without_context_selector(self):
        config, slots = fixture('R03-CANONICAL')
        market = slots[0]
        # Original TRADE fact population is supplied unchanged in the frozen packet.
        self.assertIs(market.kind, c.DataKind.TRADE)
        for kind in (c.DataKind.BAR, c.DataKind.DAILY):
            columns = tuple(c.Column(name, values) for name, values in {
                'instrument_id': ('X',), 'session_id': ('S',), 'start_ns': (10,),
                'end_ns': (15,), 'open': (100,), 'high': (100,), 'low': (100,),
                'close': (100,), 'volume': (10,), 'known_at_ns': (15,)}.items())
            changed = c.CanonicalBatch(kind, columns, market.metadata)
            item = work('R03-CANONICAL', slots=(changed,) + slots[1:])
            result = calculate(item)
            self.assertEqual(item.task.config, config)
            self.assertEqual(tuple(column.values[0] for column in result.values), ('none', 'passed'))
            self.assertIs(next(binding for binding in result.metadata.inputs if binding.role == 'market').kind, kind)

    def test_producer_role_order_present_only_bijection_and_no_fake_slots(self):
        for name in ('R01-CANONICAL', 'R03-CANONICAL'):
            item = work(name)
            compute = item.calculate
            self.rejects(lambda: compute(item.task, tuple(reversed(item.batches))), CommandErrorCode.RESULT)
            self.rejects(lambda: compute(item.task, (None,) + item.batches[1:]))
            self.rejects(lambda: compute(replace(item.task, inputs=tuple(reversed(item.task.inputs))), item.batches))
            self.rejects(lambda: compute(item.task, item.batches[:-1]), CommandErrorCode.RESULT)

    def test_headers_history_scope_and_input_byte_bounds_are_closed(self):
        item = work()
        for task in (replace(item.task, features=item.task.features[:1]),
                replace(item.task, ordered_history=True, initialization_sha256='0' * 64)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.RESULT)
        self.rejects(lambda: item.calculate(replace(item.task, max_input_bytes=1), item.batches), CommandErrorCode.LIMIT)
        self.rejects(lambda: work(config=replace(item.task.config, identity='ef1.reference_action')))
        self.rejects(lambda: reference_action_headers(item.task.config))

    def test_actual_source_metadata_cannot_be_replaced_after_task_binding(self):
        item = work()
        changed = replace(item.batches[0], metadata=replace(item.batches[0].metadata,
            source=replace(item.batches[0].metadata.source, snapshot_id='R2')))
        self.rejects(lambda: item.calculate(item.task, (changed,) + item.batches[1:]), CommandErrorCode.RESULT)

    def test_full_memory_readback_replay_is_bound_by_task_sha_not_order(self):
        for names in (('R01-CANONICAL', 'INDUSTRY-CANONICAL', 'UNIVERSE-CANONICAL'),
                ('R03-CANONICAL', 'RAW-ACTUAL-R1-empty-known_at')):
            items = tuple(work(name) for name in names)
            sink = SerializedMemorySink()
            first, replay = run(items, sink), run(items, sink)
            expected = {item.task.task_sha256: encode_result(calculate(item)) for item in items}
            self.assertEqual({row.task_sha256 for row in first.readbacks}, set(expected))
            self.assertTrue(all(row.verified for row in first.readbacks + replay.readbacks))
            self.assertEqual({row.task_sha256: row.output for row in first.readbacks},
                {row.task_sha256: row.output for row in replay.readbacks})
            self.assertEqual(sink.calls.count('commit'), len(items))
            for row in first.readbacks:
                self.assertEqual(tuple(map(encode_result, row.results)), (expected[row.task_sha256],))

    def test_shared_job_and_generation_cannot_reuse_different_config(self):
        first, second = work(), work('INDUSTRY-CANONICAL')
        for key in ('job_id', 'generation_id'):
            changed = WorkItem(replace(second.task, **{key: getattr(first.task, key)}), second.batches, second.calculate)
            self.rejects(lambda: run((first, changed)), CommandErrorCode.RESULT)

    def test_wrappers_reject_other_producers_and_foreign_callbacks(self):
        classification, action = work(), work('R03-CANONICAL')
        self.rejects(lambda: run_ef1_reference_classifications((action,), SerializedMemorySink(), requirements=LIMITS))
        self.rejects(lambda: run_ef1_reference_actions((classification,), SerializedMemorySink(), requirements=LIMITS))
        self.rejects(lambda: run_ef1_statuses((classification,), SerializedMemorySink(), requirements=LIMITS))
        foreign = WorkItem(classification.task, classification.batches, compute_reference_action_inputs)
        self.rejects(lambda: run_ef1_reference_classifications((foreign,), SerializedMemorySink(), requirements=LIMITS))

    def test_result_backend_config_and_full_inputs_cannot_be_rewritten(self):
        from equity_features import ef1
        item = work()
        result = calculate(item)
        for metadata in (replace(result.metadata, backend_version='0.0.4a4'),
                replace(result.metadata, config_digest='0' * 64), replace(result.metadata, inputs=tuple(replace(binding, metadata=replace(binding.metadata, source=replace(binding.metadata.source, snapshot_id='R9'))) for binding in result.metadata.inputs))):
            with patch.object(ef1, 'compute_ef1', return_value=replace(result, metadata=metadata)):
                self.rejects(lambda: calculate(item), CommandErrorCode.RESULT)

    def test_failure_readback_retains_actual_committed_receipt(self):
        from equity_feature_workers.supervisor import BoundedSupervisor
        item, sink = work(), SerializedMemorySink()
        first = run((item,), sink)
        with patch.object(BoundedSupervisor, 'run', return_value=first.execution), \
                patch.object(sink, 'read', side_effect=ValueError('private detail')):
            failed = run((item,), sink)
        self.assertEqual(failed.readbacks[0].reason, 'READBACK_FAILED')
        self.assertEqual(failed.readbacks[0].output, first.readbacks[0].output)
        self.assertTrue(failed.readbacks[0].output.committed)

    def test_causal_evidence_and_unavailable_value_no_evidence_in_both_modes(self):
        for name in CASES:
            item, result = work(name), calculate(work(name))
            self.assertTrue(all(row.known_at_ns is not None and row.known_at_ns <= item.task.config.availability.knowledge_cutoff_ns
                and row.use == 'consumed' and row.exclusion_reason is None for row in result.evidence))
            if item.task.family == 'reference_classification' and result.values[0].values == (None,):
                self.assertFalse(any(row.feature_id == result.values[0].feature_id for row in result.evidence))

    def test_action_gate_does_not_mutate_or_apply_source_factors(self):
        from equity_features import policies
        item = work('R03-CANONICAL')
        before = pickle.dumps(item.batches, protocol=5)
        with patch.object(policies, 'apply_action_policy', side_effect=AssertionError('Gate transformed input')):
            result = calculate(item)
        self.assertEqual(pickle.dumps(item.batches, protocol=5), before)
        self.assertEqual(tuple(column.values[0] for column in result.values), ('none', 'passed'))

    def test_canonical_structural_errors_win_even_with_missing_other_role(self):
        config, slots = fixture()
        bad = replace_column(slots[0], 'text', ('A', 'bad\x00text'))
        self.rejects(lambda: calculate(work(config=config, slots=(bad, None, slots[2]))), CommandErrorCode.CONFIG)
        foreign = replace_column(slots[0], 'instrument_id', ('X', 'Y'))
        self.rejects(lambda: work(config=config, slots=(foreign,) + slots[1:]), CommandErrorCode.RESULT)

    def test_cancellation_prevents_publication(self):
        class Cancelled:
            def is_cancelled(self):
                return True
        sink = SerializedMemorySink()
        outcome = run_ef1_reference_classifications((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(outcome.execution.cancelled)
        self.assertNotIn('begin', sink.calls)


    def test_actual_duckdb_sink_complete_sdk_readback_with_public_synthetic_adapter(self):
        from pathlib import Path
        import tempfile
        from equity_feature_duckdb.ef1 import synthetic_reference_classification_fixture, synthetic_reference_action_fixture
        from equity_feature_duckdb_sink import DuckDBSink
        for name, case, factory in (('R01-CANONICAL', 'R01', synthetic_reference_classification_fixture),
                ('R02-CANONICAL', 'R02', synthetic_reference_classification_fixture),
                ('R03-CANONICAL', 'R03', synthetic_reference_action_fixture)):
            packet = factory(case=case)
            item = work(name, slots=packet.slots)
            self.assertTrue(all(provenance == 'synthetic' for provenance in packet.provenance))
            with tempfile.TemporaryDirectory(prefix='ef1-reference-output-') as temporary:
                database = Path(temporary) / 'generated.duckdb'
                with DuckDBSink(database, item.task.destination_scope, limits=LIMITS) as sink:
                    outcome = run((item,), sink)
                    row, = outcome.readbacks
                    self.assertTrue(row.verified)
                    self.assertEqual(tuple(map(encode_result, row.results)), (encode_result(calculate(item)),))
                    self.assertEqual(json.loads(json.dumps(asdict(row.results[0]))), CASES[name]['expected_result'])
                self.assertTrue(database.is_file())

    def test_wrong_market_kind_and_relaxed_input_budget_are_rejected(self):
        config, slots = fixture('R03-CANONICAL')
        # A real REFERENCE object remains invalid in the union market slot.
        self.rejects(lambda: work('R03-CANONICAL', slots=(slots[1], None) + slots[2:]))
        item = work()
        self.rejects(lambda: item.calculate(replace(item.task, max_input_bytes=1048577), item.batches), CommandErrorCode.LIMIT)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    unittest.main()
