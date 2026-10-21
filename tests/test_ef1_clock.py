"""Mandatory unexecuted C01 direct/worker/readback and independent boundary controls."""
from dataclasses import replace
import unittest
from unittest.mock import patch

import equity_features
import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_feature_workers import CommandError, CommandErrorCode, WorkItem
from equity_feature_workers.equity_clock import compute_clock_inputs, make_ef1_clock_work
from equity_feature_workers.ef1_runner import run_ef1_anchors, run_ef1_clocks
from ef1_clock_fixture import declaration, fixture, work
from supplied_memory import SerializedMemorySink


LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
    max_result_cells=5, max_evidence_rows=128)


class EF1Clock(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import equity_feature_workers
        from equity_features import ef1

        if (equity_features.__version__, c.__version__, equity_feature_workers.__version__) != (
                '0.0.4a5', '0.0.4a4', '0.1.0a15'):
            raise AssertionError('Clock tests require exact reviewed EF1 candidate versions')
        if not all(callable(getattr(ef1, name, None)) for name in (
                'build_ef1_config', 'decode_ef1_context', 'ef1_definitions', 'compute_ef1')):
            raise AssertionError('Mandatory EF1 public API absent')

    def reject(self, item, code, *, task=None, batches=None):
        with self.assertRaises(CommandError) as caught:
            compute_clock_inputs(task if task is not None else item.task,
                batches if batches is not None else item.batches)
        self.assertEqual(caught.exception.code, code)

    def test_C01_five_independent_values_and_complete_direct_parity(self):
        from equity_features.ef1 import compute_ef1

        expected = {
            10: ('regular', 0, 20, 0.0, True),
            20: ('regular', 10, 10, 0.5, True),
            30: ('postmarket', 20, 0, 1.0, True),
            40: ('closed', 20, 0, 1.0, True),
        }
        suffixes = ('phase', 'regular_elapsed_ns', 'regular_remaining_ns', 'regular_fraction', 'early_close')
        for cutoff, values in expected.items():
            item = work(cutoff=cutoff)
            result, = compute_clock_inputs(item.task, item.batches)
            self.assertEqual({column.feature_id: column.values[0] for column in result.values},
                dict(zip(('session.clock.' + suffix for suffix in suffixes), values)))
            self.assertTrue(all(q.status == c.Status.AVAILABLE and not q.reasons for q in result.quality))
            direct = compute_ef1(item.batches, item.task.config, entity=c.EntityKey('X', 'S'))
            self.assertEqual(encode_result(result), encode_result(direct))
            self.assertEqual(result.metadata.inputs, (item.task.inputs[0].binding,))
            self.assertEqual(result.metadata.config_digest, item.task.config.digest)
            self.assertEqual(result.metadata.backend_version, '0.0.4a5')

    def test_missing_unknown_future_and_equality_knowledge(self):
        for options, reason in ((dict(missing=True), c.Reason.ABSENT_INPUT),
                                (dict(proof_known=None), c.Reason.UNKNOWN_AVAILABILITY),
                                (dict(proof_known=21), c.Reason.FUTURE_KNOWLEDGE)):
            item = work(**options)
            result, = compute_clock_inputs(item.task, item.batches)
            self.assertTrue(all(column.values == (None,) for column in result.values))
            self.assertTrue(all(q.status == c.Status.MISSING_INPUT and reason in q.reasons for q in result.quality))
            self.assertEqual(result.metadata.inputs, () if options.get('missing') else (item.task.inputs[0].binding,))
        equality = work(proof_known=20)
        result, = compute_clock_inputs(equality.task, equality.batches)
        self.assertTrue(all(q.status == c.Status.AVAILABLE for q in result.quality))

    def test_wrong_proof_hash_revision_and_scope(self):
        config, proof = fixture()
        changed = [(replace(proof, columns=tuple(c.Column(column.name, ('c1:' + '0' * 64,))
            if column.name == 'reference_id' else column for column in proof.columns)), CommandErrorCode.RESULT),
            (replace(proof, metadata=replace(proof.metadata,
                source=replace(proof.metadata.source, snapshot_id='R2'))), CommandErrorCode.RESULT),
            (replace(proof, columns=tuple(c.Column(column.name, (35,))
                if column.name == 'effective_end_ns' else column for column in proof.columns)), CommandErrorCode.CONFIG)]
        for bad, code in changed:
            item = make_ef1_clock_work(config, inputs=(declaration(bad),), batches=(bad,),
                instrument_id='X', job_id='bad-proof', generation_id='bad-proof',
                partition_id='X-S', destination_scope='ef1-clock-independent')
            self.reject(item, code)

    def test_manifest_headers_history_role_and_actual_binding_reject(self):
        item = work()
        self.reject(item, CommandErrorCode.RESULT, task=replace(item.task, features=item.task.features[:1]))
        self.reject(item, CommandErrorCode.CONFIG, task=replace(item.task, family='anchor_trades'))
        self.reject(item, CommandErrorCode.CONFIG,
            task=replace(item.task, inputs=(replace(item.task.inputs[0], role='calendar',
                binding=replace(item.task.inputs[0].binding, role='calendar')),)))
        self.reject(item, CommandErrorCode.RESULT, batches=())
        self.reject(item, CommandErrorCode.LIMIT, task=replace(item.task, max_input_bytes=1))
        self.reject(item, CommandErrorCode.RESULT, task=replace(item.task,
            ordered_history=True, initialization_sha256='0' * 64))
        foreign = replace(item.batches[0], metadata=replace(item.batches[0].metadata,
            source=replace(item.batches[0].metadata.source, input_id='foreign')))
        self.reject(item, CommandErrorCode.RESULT, batches=(foreign,))

    def test_context_schedule_operands_are_not_repaired(self):
        from equity_features.ef1 import decode_ef1_context

        item = work()
        # Actual schedule and hashed mathematical operands must agree exactly.
        changed_session = replace(item.task.config.session, scheduled_close_ns=50)
        with self.assertRaises(c.ContractError) as caught:
            decode_ef1_context(replace(item.task.config, session=changed_session))
        self.assertEqual(caught.exception.code, c.ErrorCode.INCONSISTENT_IDENTITY)
        # Worker admission never rewrites the config to match a calculation.
        self.reject(item, CommandErrorCode.RESULT,
            task=replace(item.task, config=replace(item.task.config, session=changed_session)))

    def test_full_sink_readback_retry_and_config_scope(self):
        items = (work(cutoff=10, name='open'), work(cutoff=30, name='close'))
        sink = SerializedMemorySink()
        first = run_ef1_clocks(items, sink, requirements=LIMITS)
        replay = run_ef1_clocks(items, sink, requirements=LIMITS)
        self.assertTrue(all(row.verified for row in first.readbacks + replay.readbacks))
        self.assertEqual([row.output for row in first.readbacks], [row.output for row in replay.readbacks])
        self.assertEqual(sink.calls.count('commit'), 2)
        for row in first.readbacks:
            item = next(item for item in items if item.task.task_sha256 == row.task_sha256)
            self.assertEqual(tuple(map(encode_result, row.results)),
                tuple(map(encode_result, compute_clock_inputs(item.task, item.batches))))
        for field in ('job_id', 'generation_id'):
            merged = WorkItem(replace(items[1].task, **{field: getattr(items[0].task, field)}),
                items[1].batches, items[1].calculate)
            with self.assertRaises(CommandError):
                run_ef1_clocks((items[0], merged), SerializedMemorySink(), requirements=LIMITS)

    def test_cancelled_and_postcommit_readback_failure_stay_factual(self):
        class Cancelled:
            def is_cancelled(self):
                return True

        item = work()
        sink = SerializedMemorySink()
        stopped = run_ef1_clocks((item,), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(stopped.execution.cancelled)
        self.assertNotIn('begin', sink.calls)
        initial = run_ef1_clocks((item,), sink, requirements=LIMITS)
        from equity_feature_workers.supervisor import BoundedSupervisor
        with patch.object(BoundedSupervisor, 'run', return_value=initial.execution), \
             patch.object(sink, 'read', side_effect=ValueError('private source text')):
            failed = run_ef1_clocks((item,), sink, requirements=LIMITS)
        self.assertEqual(failed.readbacks[0].reason, 'READBACK_FAILED')
        self.assertEqual(failed.readbacks[0].output, initial.readbacks[0].output)
        self.assertTrue(failed.readbacks[0].output.committed)

    def test_anchor_wrapper_remains_closed_and_result_metadata_is_not_rewritten(self):
        from equity_feature_workers.equity_sessions import compute_ef1_inputs
        from equity_features import ef1

        clock = work()
        with self.assertRaises(CommandError):
            run_ef1_anchors((clock,), SerializedMemorySink(), requirements=LIMITS)
        with self.assertRaises(CommandError):
            run_ef1_clocks((WorkItem(clock.task, clock.batches, compute_ef1_inputs),),
                SerializedMemorySink(), requirements=LIMITS)
        direct = ef1.compute_ef1(clock.batches, clock.task.config, entity=c.EntityKey('X', 'S'))
        for metadata in (replace(direct.metadata, inputs=()),
                         replace(direct.metadata, config_digest='0' * 64),
                         replace(direct.metadata, backend_version='0.0.4a4')):
            with patch.object(ef1, 'compute_ef1', return_value=replace(direct, metadata=metadata)):
                self.reject(clock, CommandErrorCode.RESULT)


if __name__ == '__main__':
    unittest.main()
