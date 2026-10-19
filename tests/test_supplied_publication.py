"""Actual serialized fixture, receipt headroom, committed lookup/replay bounds."""
from dataclasses import replace
import json
import threading
import unittest
from unittest.mock import patch

from equity_feature_io_contracts import SinkRequirements, SinkError, SinkErrorCode, PublicationState
from equity_feature_io_sdk import encode_receipt, encode_result, idempotency_key, prepare_publication, publish
from equity_feature_workers import PublicationLimits, SerialPublisher, OutputManifest, encode_output
from equity_feature_workers.manifests import MAX_MANIFEST_BYTES
from supplied_memory import SerializedMemorySink
from test_supplied_session import original_work

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=1048576, max_total_bytes=1048576,
    max_result_cells=100, max_evidence_rows=1000)


class SuppliedPublication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.items, cls.results = original_work()

    def publisher(self, sink, bound=400000):
        return SerialPublisher(sink, 'original-destination', limits=PublicationLimits(max_receipt_wire_bytes=bound), requirements=LIMITS)

    def test_all_original_full_results_commit_lookup_readback_replay(self):
        for item, result in zip(self.items, self.results):
            with self.subTest(partition=item.task.partition_id):
                sink = SerializedMemorySink(); publisher = self.publisher(sink)
                publisher.submit(item.task, (result,))
                outcome = publisher.drain()[0]
                self.assertTrue(outcome.committed)
                self.assertIsNone(outcome.reason)
                self.assertEqual(sink.read(outcome.output.receipt), (result,))
                self.assertEqual(publisher.pending, ())
                self.assertEqual(sink.lookup(outcome.output.receipt.idempotency_key).state, PublicationState.COMMITTED)
                publisher.submit(item.task, (result,))
                replay = publisher.drain()[0]
                self.assertEqual(replay.output, outcome.output)
                self.assertEqual(encode_result(sink.read(replay.output.receipt)[0]), encode_result(result))

    def test_exact_null_replacement_headroom_before_queue_or_sink_io(self):
        item, result = self.items[0], self.results[0]
        envelope = prepare_publication((result,), destination_scope=item.task.destination_scope,
            generation_id=item.task.generation_id, job_id=item.task.job_id, partition_id=item.task.partition_id, limits=LIMITS)
        declaration = OutputManifest(item.task, envelope)
        headroom = MAX_MANIFEST_BYTES - len(encode_output(declaration)) + 4
        sink = SerializedMemorySink()
        publisher = self.publisher(sink, headroom)
        publisher.submit(item.task, (result,))
        self.assertEqual(len(publisher.pending), 1)
        rejected = self.publisher(sink, headroom + 1)
        with self.assertRaises(SinkError) as caught:
            rejected.submit(item.task, (result,))
        self.assertEqual(caught.exception.code, SinkErrorCode.RESOURCE_LIMIT)
        self.assertEqual(rejected.pending, ())
        self.assertNotIn('begin', sink.calls)
        self.assertNotIn('lookup', sink.calls)

    def test_actual_bound_includes_escaped_receipt_string(self):
        item, result = self.items[0], self.results[0]
        sink = SerializedMemorySink(); publisher = self.publisher(sink)
        publisher.submit(item.task, (result,)); output = publisher.drain()[0].output
        escaped = len(json.dumps(encode_receipt(output.receipt).decode('ascii'), ensure_ascii=True,
            separators=(',', ':'), allow_nan=False).encode('ascii'))
        self.assertEqual(len(encode_output(output)), len(encode_output(replace(output, receipt=None))) - 4 + escaped)
        self.assertGreater(escaped, len(encode_receipt(output.receipt)))

    def test_postcommit_violation_retains_pending_and_cannot_retry_bypass(self):
        item, result = self.items[0], self.results[0]
        sink = SerializedMemorySink(); publisher = self.publisher(sink, 1)
        publisher.submit(item.task, (result,))
        for _ in range(3):
            outcome = publisher.drain()[0]
            self.assertFalse(outcome.committed)
            self.assertEqual(outcome.reason, 'RESOURCE_LIMIT')
            self.assertEqual(publisher.pending, (item.task.task_sha256,))
        envelope = prepare_publication((result,), destination_scope=item.task.destination_scope,
            generation_id=item.task.generation_id, job_id=item.task.job_id, partition_id=item.task.partition_id, limits=LIMITS)
        actual = sink.lookup(idempotency_key(envelope.identity))
        self.assertEqual(actual.state, PublicationState.COMMITTED)
        self.assertEqual(sink.read(actual.receipt), (result,))
        self.assertEqual(sink.calls.count('commit'), 1)

    def test_preexisting_lookup_and_uncertain_commit_paths_enforce_bound(self):
        item, result = self.items[0], self.results[0]
        for fault in (None, 'after_commit'):
            sink = SerializedMemorySink(fault)
            envelope = prepare_publication((result,), destination_scope=item.task.destination_scope,
                generation_id=item.task.generation_id, job_id=item.task.job_id, partition_id=item.task.partition_id, limits=LIMITS)
            receipt = publish(sink, envelope, (result,), requirements=LIMITS)
            publisher = self.publisher(sink, 1)
            publisher.submit(item.task, (result,))
            self.assertEqual(publisher.drain()[0].reason, 'RESOURCE_LIMIT')
            self.assertEqual(sink.read(receipt), (result,))
            self.assertEqual(len(publisher.pending), 1)

    def test_owner_guard_all_external_operations_and_reentry(self):
        sink = SerializedMemorySink(); item, result = self.items[0], self.results[0]
        publisher = self.publisher(sink); publisher.submit(item.task, (result,))
        output = publisher.drain()[0].output
        operations = [sink.capabilities, lambda: sink.lookup(output.receipt.idempotency_key),
            lambda: sink.read(output.receipt), lambda: sink.begin(output.envelope),
            lambda: sink.commit(None), lambda: sink.abort(None), lambda: sink.write(None, 0, result)]
        errors = []
        def foreign():
            for operation in operations:
                try:
                    operation()
                except SinkError as error:
                    errors.append(error.code)
        worker = threading.Thread(target=foreign); worker.start(); worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [SinkErrorCode.INVALID_SESSION] * len(operations))
        with patch.object(sink._inner, 'capabilities', side_effect=sink.capabilities):
            with self.assertRaises(SinkError) as caught:
                sink.capabilities()
            self.assertEqual(caught.exception.code, SinkErrorCode.BUSY)
        self.assertEqual(sink.capabilities().writer_mode, 'serialized_writer')

    def test_bound_validation_and_legacy_default(self):
        self.assertIsNone(PublicationLimits().max_receipt_wire_bytes)
        for bound in (0, -1, True, 1.0, MAX_MANIFEST_BYTES + 1):
            with self.assertRaises(SinkError):
                PublicationLimits(max_receipt_wire_bytes=bound)


if __name__ == '__main__':
    unittest.main()
