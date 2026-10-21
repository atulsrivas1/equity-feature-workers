"""Independent full-result worker conformance for supplied opening and physical status observables."""
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
from equity_feature_workers.ef1_runner import run_ef1_opening_trade, run_ef1_opening_trade_seconds, run_ef1_status_history
from ef1_observable_fixture import CASES, SUCCESS, ERRORS, fixture, work, calculate, spawned, literal_result, literal_candidate_wire
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144,
    max_total_bytes=262144, max_result_cells=6, max_evidence_rows=32)


def run(items, sink=None):
    runner = {'opening_trade':run_ef1_opening_trade, 'opening_trade_seconds':run_ef1_opening_trade_seconds, 'status_history':run_ef1_status_history}[items[0].task.family]
    return runner(items, sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


class EF1Observables(unittest.TestCase):
    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def test_eighty_nine_packets_and_fifty_eight_full_results(self):
        self.assertEqual((len(CASES), len(SUCCESS), len(ERRORS)), (89, 58, 31))
        for name, packet in SUCCESS.items():
            with self.subTest(case=name):
                item = work(name)
                actual = calculate(item)
                self.assertEqual(json.loads(json.dumps(asdict(actual))), packet['expected_result'])
                self.assertEqual(decode_result(encode_result(actual)), actual)

    def test_thirty_one_refusals_classified_by_actual_admission_stage(self):
        from equity_feature_workers.manifests import ManifestError
        stages = {}
        for name, packet in ERRORS.items():
            with self.subTest(case=name):
                if name == 'X15-evidence-bound':
                    # Candidate output is not a pure input. SDK DTO reconstruction
                    # must enforce its own original evidence_limit before publication.
                    with self.assertRaises(c.ContractError) as caught:
                        literal_result(packet['candidate_result'])
                    self.assertEqual(caught.exception.code.value, 'bounds')
                    from equity_feature_io_contracts import SinkError, SinkErrorCode
                    with self.assertRaises(SinkError) as wire_error:
                        decode_result(literal_candidate_wire(packet['candidate_result']))
                    self.assertEqual(wire_error.exception.code,SinkErrorCode.INVALID_CONTENT)
                    stages[name] = 'candidate-result-constructor-and-sdk-wire'
                    continue
                try:
                    fixture(name)
                except c.ContractError as error:
                    self.assertEqual(error.code.value, packet['expected_error']['code'])
                    stages[name] = 'canonical-constructor'
                    continue
                try:
                    item = work(name)
                except (CommandError,ManifestError) as error:
                    stages[name] = 'worker-task-admission'
                    actual_code = error.code
                else:
                    with self.assertRaises(CommandError) as caught:
                        calculate(item)
                    stages[name] = 'worker-pure-callback-translation'
                    actual_code = caught.exception.code
                if name not in ('X12-context-byte-bound', 'X13-task-byte-bound', 'X14-reference-row-bound'):
                    code = packet['expected_error']['code']
                    expected = CommandErrorCode.CALCULATION if code == 'overflow' else CommandErrorCode.RESULT if code == 'inconsistent_identity' else CommandErrorCode.CONFIG
                    self.assertEqual(actual_code, expected)
        self.assertEqual(len(stages), 31)
        print("OBSERVABLE_REFUSAL_STAGES="+json.dumps(stages,sort_keys=True))

    def test_task_wire_sha_original_bindings_and_header_discovery(self):
        import importlib
        for name in SUCCESS:
            item = work(name)
            self.assertEqual(decode_task(encode_task(item.task)), item.task)
            self.assertEqual(decode_task(encode_task(item.task)).task_sha256, item.task.task_sha256)
            self.assertLessEqual(len(encode_task(item.task)), 1048576)
            result = calculate(item)
            self.assertEqual(result.metadata.inputs, tuple(sorted((slot.binding for slot in item.task.inputs if slot.binding is not None), key=lambda value:value.role)))
            module = importlib.import_module('equity_feature_workers.equity_' + item.task.family)
            self.assertEqual(item.task.features, getattr(module,item.task.family+'_headers')(item.task.config))
            self.assertTrue(getattr(module,'required_'+item.task.family+'_inputs')(item.task.config))
            self.assertEqual(result.metadata.math_policy_version, 'v1')

    def test_actual_spawn_matches_main_full_sdk_bytes(self):
        items = tuple(work(name) for name in ('O01-first','O02-seconds','H01-one-record-three-dimensions','H05-active-missing-seed','T02-zero-prefix-empty-source-capability-retained'))
        with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context('spawn')) as pool:
            actual = tuple(pool.map(spawned, items))
        self.assertEqual(actual, tuple(encode_result(calculate(item)) for item in items))

    def test_original_nullable_slots_reach_pure_library_unchanged(self):
        from equity_features import ef1
        for name in ('O01-first','O07-zero-C10','H01-one-record-three-dimensions','H05-active-missing-seed','T02-zero-prefix-empty-source-capability-retained'):
            item = work(name)
            with patch.object(ef1,'compute_ef1',wraps=ef1.compute_ef1) as pure:
                calculate(item)
            forwarded = pure.call_args.args[0]
            self.assertEqual(forwarded, fixture(name)[1])
            self.assertEqual(len(forwarded),len(item.task.inputs))
            self.assertTrue(all(left is right for left,right in zip((value for value in forwarded if value is not None), item.batches)))

    def test_role_order_missing_bijection_and_wrong_kind_are_refused(self):
        for name in ('O01-first','H01-one-record-three-dimensions'):
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
                replace(item.task, max_input_batches=6)):
            self.rejects(lambda: item.calculate(task, item.batches), CommandErrorCode.LIMIT)

    def test_actual_source_metadata_substitution_refused(self):
        item = work()
        changed = replace(item.batches[0], metadata=replace(item.batches[0].metadata,
            source=replace(item.batches[0].metadata.source, snapshot_id='R2')))
        self.rejects(lambda: item.calculate(item.task, (changed,) + item.batches[1:]), CommandErrorCode.RESULT)

    def test_full_memory_readback_replay_joined_by_task_sha(self):
        for names in (('O01-first','O03-complete-empty','O04-partial-positive'), ('H01-one-record-three-dimensions','H05-active-missing-seed'), ('O02-seconds','O11-fractional-seconds')):
            items, sink = tuple(work(name) for name in names), SerializedMemorySink()
            first, replay = run(items,sink), run(items,sink)
            expected = {item.task.task_sha256:encode_result(calculate(item)) for item in items}
            self.assertEqual({row.task_sha256 for row in first.readbacks},set(expected))
            self.assertTrue(all(row.verified for row in first.readbacks+replay.readbacks))
            self.assertEqual(sink.calls.count('commit'),len(items))
            for row in first.readbacks:
                self.assertEqual(tuple(map(encode_result,row.results)),(expected[row.task_sha256],))

    def test_cross_producer_and_foreign_callback_refused(self):
        first, second = work(), work('H01-one-record-three-dimensions')
        self.rejects(lambda:run_ef1_opening_trade((second,),SerializedMemorySink(),requirements=LIMITS))
        self.rejects(lambda:run_ef1_status_history((first,),SerializedMemorySink(),requirements=LIMITS))
        self.rejects(lambda:run_ef1_opening_trade((WorkItem(first.task,first.batches,second.calculate),),SerializedMemorySink(),requirements=LIMITS))

    def test_shared_job_or_generation_cannot_reuse_different_config(self):
        first, second = work(), work('O08-closed-C30')
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
        result = run_ef1_opening_trade((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
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

    def test_zero_prefix_retains_exact_empty_capability_slot(self):
        item = work('T02-zero-prefix-empty-source-capability-retained')
        self.assertEqual(tuple(slot.role for slot in item.task.inputs), ('calendar_provenance','market_prefix','source_capability','trades'))
        self.assertIsNone(item.task.inputs[1].binding)
        self.assertEqual(item.task.inputs[2].state,'empty')
        self.assertIsNone(item.task.inputs[3].binding)
        self.assertEqual(json.loads(json.dumps(asdict(calculate(item)))),SUCCESS['T02-zero-prefix-empty-source-capability-retained']['expected_result'])

    def test_synthetic_public_adapter_actual_duckdb_full_result_readback(self):
        from pathlib import Path
        import tempfile
        from equity_feature_duckdb.ef1 import synthetic_opening_trade_fixture, synthetic_opening_trade_seconds_fixture, synthetic_status_history_fixture, synthetic_status_history_missing_capability_fixture
        from equity_feature_duckdb_sink import DuckDBSink
        for name,factory in (('O01-first',synthetic_opening_trade_fixture), ('O02-seconds',synthetic_opening_trade_seconds_fixture), ('H01-one-record-three-dimensions',synthetic_status_history_fixture), ('H11-missing-source-identity',synthetic_status_history_missing_capability_fixture)):
            packet = factory()
            item = work(name, slots=packet.slots)
            self.assertEqual(packet.provenance,tuple('unavailable' if slot is None else 'synthetic' for slot in packet.slots))
            with tempfile.TemporaryDirectory(prefix='ef1-observable-output-') as temporary:
                database = Path(temporary)/'generated.duckdb'
                with DuckDBSink(database,item.task.destination_scope,limits=LIMITS) as sink:
                    row, = run((item,),sink).readbacks
                    self.assertTrue(row.verified)
                    self.assertEqual(tuple(map(encode_result,row.results)),(encode_result(calculate(item)),))
                    self.assertEqual(json.loads(json.dumps(asdict(row.results[0]))),SUCCESS[name]['expected_result'])
                self.assertTrue(database.is_file())

    def test_result_backend_policy_and_algorithm_substitution_refused(self):
        from equity_features import ef1
        item, result = work(), calculate(work())
        for metadata in (replace(result.metadata,backend_id='python-exact'),
                replace(result.metadata,math_policy_version='observable-v2'),
                replace(result.metadata,evidence_limit=33)):
            with patch.object(ef1,'compute_ef1',return_value=replace(result,metadata=metadata)):
                self.rejects(lambda:calculate(item),CommandErrorCode.RESULT)
        columns = tuple(replace(column,algorithm_version='v1') for column in result.values)
        with patch.object(ef1,'compute_ef1',return_value=replace(result,values=columns)):
            self.rejects(lambda:calculate(item),CommandErrorCode.RESULT)

    def test_closed_math_policy_does_not_relax_v1_tasks(self):
        from equity_feature_workers.manifests import ManifestError
        from ef1_period_fixture import work as period_work
        item = work()
        with self.assertRaises(ManifestError):
            replace(item.task, math_policy_version='observable-v2')
        prior = period_work()
        with self.assertRaises(ManifestError):
            replace(prior.task, math_policy_version='observable-v2')
        with self.assertRaises(ManifestError):
            replace(item.task, math_policy_version='unknown')
        with self.assertRaises(ManifestError):
            replace(item.task, features=tuple(replace(header,algorithm_version='v1') for header in item.task.features), math_policy_version='unknown')

    def test_actual_coverage_observed_cannot_disagree_with_rows(self):
        config, slots = fixture()
        original = slots[-1]
        changed = replace(original,metadata=replace(original.metadata,coverage=c.Coverage(2,2,True)))
        self.rejects(lambda:work(slots=slots[:-1]+(changed,)),CommandErrorCode.RESULT)

    def test_wrong_family_proof_cannot_admit_identical_source_scope(self):
        from ef1_reference_fixture import replace_column
        config, slots = fixture('H01-one-record-three-dimensions')
        proof = slots[2]
        changed = replace_column(proof,'fact_kind',('ef1.observable.market_prefix',))
        self.rejects(lambda:calculate(work('H01-one-record-three-dimensions',slots=slots[:2]+(changed,)+slots[3:])))

    def test_history_original_knowledge_gates_both_modes(self):
        from ef1_reference_fixture import replace_column
        for mode in ('known_at','reconstruction'):
            config, slots = fixture('H01-one-record-three-dimensions')
            config = replace(config,availability=replace(config.availability,mode=mode,reconstruction_reason=None if mode=='known_at' else 'explicit-original-knowledge-check'))
            # Config descriptor has unchanged K; availability mode affects digest,
            # never the original incoming proof descriptor's exact K identity.
            events = replace_column(slots[1],'known_at_ns',(None,))
            result = calculate(work('H01-one-record-three-dimensions',config=config,slots=(slots[0],events)+slots[2:]))
            factual = {column.feature_id:column.values[0] for column in result.values if column.dtype is not c.ValueType.STRING}
            self.assertTrue(all(value is None for value in factual.values()))
            self.assertTrue(all(row.known_at_ns is not None and row.known_at_ns<=config.availability.knowledge_cutoff_ns for row in result.evidence))

    def test_oversize_outer_task_refuses_before_pure_callback(self):
        from equity_features import ef1
        with patch.object(ef1,'compute_ef1',side_effect=AssertionError('pure callback must not run')) as pure:
            with self.assertRaises(Exception):
                work('X13-task-byte-bound')
            pure.assert_not_called()

    def test_nullable_role_swap_is_not_recovered_from_source_names(self):
        item = work('T02-zero-prefix-empty-source-capability-retained')
        first,second,third,fourth = item.task.inputs
        moved = replace(item.task,inputs=(first,replace(third,role='market_prefix',binding=replace(third.binding,role='market_prefix')),replace(second,role='source_capability'),fourth))
        self.rejects(lambda:item.calculate(moved,item.batches))


if __name__ == '__main__':
    multiprocessing.freeze_support()
    unittest.main()
