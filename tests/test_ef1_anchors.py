"""Independent anchor arithmetic, admission and complete publication parity.

Mandatory candidate tests: a missing/wrong EF1 dependency is a failure, never a skip.
"""
from dataclasses import replace
import unittest
from unittest.mock import patch

import equity_features
import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_feature_workers import CommandError, CommandErrorCode
from equity_feature_workers.equity_sessions import compute_ef1_inputs
from equity_feature_workers.ef1_runner import run_ef1_anchors
from supplied_memory import SerializedMemorySink
from ef1_anchor_fixture import work
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))  # tools/_versions.py
from _versions import CONTRACTS, FEATURES, VERSIONS  # noqa: E402


LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
    max_result_cells=3, max_evidence_rows=128)


class EF1Anchors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import equity_feature_workers as worker
        from equity_features import ef1

        if (equity_features.__version__, c.__version__, worker.__version__) != (FEATURES, CONTRACTS, VERSIONS['equity-feature-workers']):
            raise AssertionError(f"EF1 requires exact features{FEATURES}/contracts{CONTRACTS}/workers{VERSIONS['equity-feature-workers']}")
        if not all(callable(getattr(ef1, name, None)) for name in (
                'build_ef1_config', 'decode_ef1_context', 'ef1_definitions', 'compute_ef1')):
            raise AssertionError('EF1 candidate public API is missing')

    def reject(self, item, code, *, task=None, batches=None):
        with self.assertRaises(CommandError) as caught:
            compute_ef1_inputs(task if task is not None else item.task,
                batches if batches is not None else item.batches)
        self.assertEqual(caught.exception.code, code)

    def test_independent_exact_arithmetic_and_full_direct_parity(self):
        from equity_features.ef1 import compute_ef1

        for mode in ('trades', 'bars_actual_notional'):
            item = work(mode)
            result, = compute_ef1_inputs(item.task, item.batches)
            values = {column.feature_id: column.values[0] for column in result.values}
            self.assertEqual(values, {'session.anchor.notional': 3300,
                'session.anchor.volume': 3, 'session.anchor.vwap': 11.0})
            self.assertTrue(all(q.status == c.Status.AVAILABLE for q in result.quality))
            direct = compute_ef1(item.batches, item.task.config, entity=c.EntityKey('X', 'S'))
            self.assertEqual(encode_result(result), encode_result(direct))
            self.assertEqual(result.metadata.config_digest, item.task.config.digest)
            self.assertEqual(result.metadata.inputs,
                tuple(sorted((slot.binding for slot in item.task.inputs), key=lambda b: b.role)))
            self.assertEqual(result.metadata.backend_version, FEATURES)

    def test_covered_empty_differs_from_missing_primary(self):
        empty = work(empty=True)
        result, = compute_ef1_inputs(empty.task, empty.batches)
        values = {column.feature_id: column.values[0] for column in result.values}
        self.assertEqual(values, {'session.anchor.notional': 0,
            'session.anchor.volume': 0, 'session.anchor.vwap': None})
        missing = work(market_missing=True)
        absent, = compute_ef1_inputs(missing.task, missing.batches)
        self.assertTrue(all(column.values == (None,) for column in absent.values))
        self.assertEqual([binding.role for binding in absent.metadata.inputs], ['anchor_provenance'])
        self.assertNotEqual(encode_result(result), encode_result(absent))

    def test_missing_notional_preserves_independent_volume(self):
        item = work('bars_actual_notional', notional=False)
        result, = compute_ef1_inputs(item.task, item.batches)
        self.assertEqual({column.feature_id: column.values[0] for column in result.values},
            {'session.anchor.notional': None, 'session.anchor.volume': 3, 'session.anchor.vwap': None})

    def test_exact_role_batch_bijection_headers_history_and_kind(self):
        item = work()
        self.reject(item, CommandErrorCode.RESULT, batches=item.batches[::-1])
        self.reject(item, CommandErrorCode.RESULT, batches=item.batches[:1])
        self.reject(item, CommandErrorCode.CONFIG,
            task=replace(item.task, inputs=item.task.inputs[::-1]), batches=item.batches[::-1])
        self.reject(item, CommandErrorCode.RESULT, task=replace(item.task, features=item.task.features[:1]))
        self.reject(item, CommandErrorCode.RESULT,
            task=replace(item.task, initialization_sha256='0' * 64))
        self.reject(item, CommandErrorCode.CONFIG, task=replace(item.task, family='anchor_bars'))
        self.reject(item, CommandErrorCode.LIMIT, task=replace(item.task, max_input_bytes=1))

    def test_unknown_future_missing_proof_does_not_become_available(self):
        for kwargs in ({'proof_missing': True}, {'proof_known': None}, {'proof_known': 21}):
            item = work(**kwargs)
            result, = compute_ef1_inputs(item.task, item.batches)
            self.assertTrue(all(column.values == (None,) for column in result.values))
            self.assertTrue(all(q.status == c.Status.MISSING_INPUT for q in result.quality))

    def test_backend_identity_and_future_package_reject_without_rewriting(self):
        from equity_features import ef1

        item = work()
        original = ef1.compute_ef1(item.batches, item.task.config, entity=c.EntityKey('X', 'S'))
        wrong = replace(original, metadata=replace(original.metadata, backend_version='0.0.4a4'))
        with patch.object(ef1, 'compute_ef1', return_value=wrong):
            self.reject(item, CommandErrorCode.RESULT)
        for metadata in (replace(original.metadata, inputs=original.metadata.inputs[:1]),
                         replace(original.metadata, config_digest='0' * 64)):
            with patch.object(ef1, 'compute_ef1', return_value=replace(original, metadata=metadata)):
                self.reject(item, CommandErrorCode.RESULT)
        with patch.object(equity_features, '__version__', '0.0.4a7'):
            self.reject(item, CommandErrorCode.CONFIG)
        with patch.object(c, '__version__', '0.0.4a6'):
            self.reject(item, CommandErrorCode.CONFIG)

    def test_serialized_readback_idempotent_replay_and_distinct_scopes(self):
        items = (work(name='trades'), work('bars_actual_notional', name='bars'))
        sink = SerializedMemorySink()
        first = run_ef1_anchors(items, sink, requirements=LIMITS)
        self.assertTrue(all(readback.verified for readback in first.readbacks))
        self.assertEqual(first.publisher.pending, ())
        replay = run_ef1_anchors(items, sink, requirements=LIMITS)
        self.assertEqual([r.output for r in first.readbacks], [r.output for r in replay.readbacks])
        for actual in first.readbacks:
            item = next(item for item in items if item.task.task_sha256 == actual.task_sha256)
            self.assertEqual(tuple(map(encode_result, actual.results)),
                tuple(map(encode_result, compute_ef1_inputs(item.task, item.batches))))
        self.assertEqual(sink.calls.count('commit'), 2)

    def test_shared_job_or_generation_cannot_publish_different_config(self):
        from equity_feature_workers import WorkItem

        trade = work(name='trade')
        bar = work('bars_actual_notional', name='bar')
        for field in ('job_id', 'generation_id'):
            changed = WorkItem(replace(bar.task, **{field: getattr(trade.task, field)}),
                bar.batches, bar.calculate)
            sink = SerializedMemorySink()
            with self.assertRaises(CommandError):
                run_ef1_anchors((trade, changed), sink, requirements=LIMITS)
            self.assertNotIn('begin', sink.calls)

    def test_readback_failure_retains_actual_committed_output(self):
        item = work()
        sink = SerializedMemorySink()
        # Capture a valid run first; then patch only the additional runner read.
        initial = run_ef1_anchors((item,), sink, requirements=LIMITS)
        self.assertTrue(initial.readbacks[0].verified)
        from equity_feature_workers.supervisor import BoundedSupervisor
        with patch.object(BoundedSupervisor, 'run', return_value=initial.execution), \
             patch.object(sink, 'read', side_effect=ValueError('private sink detail')):
            outcome = run_ef1_anchors((item,), sink, requirements=LIMITS)
        self.assertEqual(outcome.readbacks[0].reason, 'READBACK_FAILED')
        self.assertEqual(outcome.readbacks[0].output, initial.readbacks[0].output)
        self.assertTrue(outcome.readbacks[0].output.committed)

    def test_cancelled_before_work_never_publishes(self):
        class Cancelled:
            def is_cancelled(self):
                return True

        sink = SerializedMemorySink()
        outcome = run_ef1_anchors((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(outcome.execution.cancelled)
        self.assertTrue(all(not readback.verified for readback in outcome.readbacks))
        self.assertNotIn('begin', sink.calls)


if __name__ == '__main__':
    unittest.main()
