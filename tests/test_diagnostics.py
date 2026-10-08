"""Frozen literal arithmetic, bounded reporting and real end-to-end faults."""
from dataclasses import FrozenInstanceError, replace
import json
import os
import pickle
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from equity_feature_example_extensions import ExampleSink, SourceFactory, SinkFactory
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_io_contracts import SinkError, SinkErrorCode
from equity_feature_io_sdk import SourceRegistry, SinkRegistry, encode_result
from equity_feature_workers import (
    ProgressRecorder, StageTiming, DiagnosticStage as S, DiagnosticStatus as D,
    CommandError, run_session, run_registered, BoundedSupervisor, SerialPublisher,
    PublicationLimits, Dependency, GenerationSpec, TaskNode, evaluate_readiness, BarrierLimits,
)
from equity_feature_workers.diagnostics import interval
from equity_feature_workers.cli import NoCredentials
from test_commands import inputs, Source
from test_supervisor import work, budget, failed, wrong, counted, CALLS
from test_publication import sink_for, Proxy
from test_catalog import stores

ORACLE = json.loads(Path(__file__).with_name('diagnostic_oracle.json').read_text(encoding='utf-8'))


class Diagnostics(unittest.TestCase):
    def test_frozen_overlap_arithmetic_and_immutable_snapshot(self):
        tasks = tuple(work(member)[0].task for member in ('A', 'B'))
        recorder = ProgressRecorder()
        with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', side_effect=(0, 0, 136, 136)):
            with recorder.group(tasks, spans=5) as observations:
                for index, task in enumerate(tasks):
                    observation = observations[task.task_sha256]
                    for stage, durations in ORACLE['durations_ns'].items():
                        observation.add(interval(S(stage), 100, 100 + durations[index]))
                    observation.finish(D.COMPUTED)
        snapshot = recorder.snapshot()
        self.assertEqual([t.elapsed_ns for t in snapshot.tasks], [ORACLE['job_elapsed_ns']] * 2)
        self.assertEqual(sum(s.elapsed_ns for t in snapshot.tasks for s in t.timings), ORACLE['sum_stage_ns'])
        self.assertGreater(ORACLE['sum_stage_ns'], ORACLE['job_elapsed_ns'])
        with self.assertRaises(FrozenInstanceError):
            snapshot.tasks[0].status = D.VERIFIED
        before = snapshot.encode()
        with recorder.attempt('1' * 64, 'custom', 'another', spans=1) as observation:
            observation.finish(D.READY)
        self.assertEqual(snapshot.encode(), before)

    def test_exact_timing_admission_and_invalid_counters(self):
        for start, end in ((None, 9), (1, None), (True, 9), (9, 8), (-1, 9), (0, 1 << 63)):
            self.assertIsNone(interval(S.CALCULATION, start, end).elapsed_ns)
        for elapsed in (True, -1, 1 << 63):
            with self.assertRaises(ValueError):
                StageTiming(S.CALCULATION, elapsed)
        self.assertEqual(interval(S.QUEUE_WAIT, 7, 7).elapsed_ns, 0)

    def test_all_report_quotas_reject_before_source_or_clock(self):
        spec, _, source = inputs()
        for recorder in (ProgressRecorder(max_spans=3), ProgressRecorder(max_bytes=1024)):
            with patch('equity_feature_workers.diagnostics.time.perf_counter_ns') as clock:
                with self.assertRaises(SinkError):
                    run_session(spec, source, ExampleSink(), requirements=LIMITS, progress=recorder)
                clock.assert_not_called()
            self.assertEqual(source.called, 0)
            self.assertEqual(recorder.snapshot().tasks, ())
        for name in ('max_tasks', 'max_spans', 'max_bytes'):
            with self.assertRaises(ValueError):
                ProgressRecorder(**{name: True})

    def test_task_quota_and_owner_reentry_precede_hooks(self):
        recorder = ProgressRecorder(max_tasks=1)
        errors = []
        def foreign():
            try:
                with recorder.attempt('0' * 64, 'bars', 'P', spans=1):
                    self.fail('foreign owner entered')
            except SinkError as error:
                errors.append(error.code)
        with patch('equity_feature_workers.diagnostics.time.perf_counter_ns') as clock:
            thread = threading.Thread(target=foreign); thread.start(); thread.join()
            clock.assert_not_called()
        self.assertEqual(errors, [SinkErrorCode.INVALID_SESSION])
        with recorder.attempt('0' * 64, 'bars', 'P', spans=1):
            with patch('equity_feature_workers.diagnostics.time.perf_counter_ns') as clock:
                with self.assertRaises(SinkError):
                    with recorder.attempt('0' * 64, 'bars', 'P', spans=1): pass
                clock.assert_not_called()
        with self.assertRaises(SinkError):
            with recorder.attempt('0' * 64, 'bars', 'P', spans=1): pass

    def test_bad_clocks_preserve_actual_work_and_receipt(self):
        spec, batch, _ = inputs()
        reference = run_session(spec, Source(batch, spec.request), ExampleSink(), requirements=LIMITS)
        for clock in (lambda: True, lambda: -1, lambda: 1 << 63,
                      lambda: (_ for _ in ()).throw(ValueError('secret clock path'))):
            recorder = ProgressRecorder()
            with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', clock):
                outcome = run_session(spec, Source(batch, spec.request), ExampleSink(), requirements=LIMITS, progress=recorder)
            self.assertEqual(tuple(map(encode_result, outcome.results)), tuple(map(encode_result, reference.results)))
            self.assertTrue(outcome.output.committed)
            task = recorder.snapshot().tasks[0]
            self.assertEqual(task.status, D.VERIFIED)
            self.assertTrue(all(t.elapsed_ns is None for t in task.timings))
            self.assertIsNone(task.elapsed_ns)
            self.assertNotIn(b'secret', recorder.snapshot().encode())

    def test_disabled_telemetry_never_reads_clock(self):
        spec, _, source = inputs()
        with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', side_effect=AssertionError('clock')) as clock:
            self.assertTrue(run_session(spec, source, ExampleSink(), requirements=LIMITS).output.committed)
            BoundedSupervisor().run((work()[0],))
            clock.assert_not_called()

    def test_custom_labels_are_hashes_and_unknown_error_redacted(self):
        task = replace(work()[0].task, family='secret/private/family', partition_id='secret/private/path')
        recorder = ProgressRecorder()
        with self.assertRaisesRegex(ValueError, 'private'):
            with recorder.group((task,), spans=1):
                raise ValueError('private credential payload')
        observation = recorder.snapshot().tasks[0]
        self.assertEqual((observation.family, observation.reason, observation.status), ('custom', 'UNAVAILABLE', D.FAILED))
        self.assertEqual(observation.task_sha256, task.task_sha256)
        self.assertNotIn(b'private', recorder.snapshot().encode())

    def test_revision_intent_and_sealed_identity_one_slot_per_attempt(self):
        spec, batch, _ = inputs()
        recorder = ProgressRecorder(max_tasks=2)
        for revision in ('r1', 'r2'):
            outcome = run_session(replace(spec, revision_id=revision), Source(batch, spec.request),
                                  ExampleSink(), requirements=LIMITS, progress=recorder)
            self.assertEqual(recorder.snapshot().tasks[-1].task_sha256, outcome.output.task.task_sha256)
        rows = recorder.snapshot().tasks
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0].intent_sha256, rows[1].intent_sha256)
        self.assertNotEqual(rows[0].task_sha256, rows[1].task_sha256)

    def test_registered_factories_and_fixed_factory_failure(self):
        sources, sinks = SourceRegistry(), SinkRegistry()
        sources.register('custom.source', SourceFactory()); sinks.register('custom.sink', SinkFactory())
        spec, _, _ = inputs()
        recorder = ProgressRecorder()
        kwargs = dict(sources=sources, sinks=sinks, source_id='custom.source', source_config={'namespace': 'demo'},
            sink_id='custom.sink', sink_config={'destination_scope': 'synthetic-conformance'},
            credentials=NoCredentials(), requirements=LIMITS, progress=recorder)
        outcome = run_registered(spec, **kwargs)
        self.assertEqual(recorder.snapshot().tasks[0].status, D.VERIFIED)
        self.assertEqual(recorder.snapshot().tasks[0].timings[0].stage, S.FACTORY)
        values = {column.feature_id: column.values[0] for column in outcome.results[0].values}
        self.assertEqual(values['session.bar.volume'], ORACLE['bar_goldens']['volume'])
        with patch.object(sources, 'resolve', side_effect=ValueError('secret factory path')):
            with self.assertRaises(CommandError): run_registered(spec, **kwargs)
        row = recorder.snapshot().tasks[-1]
        self.assertEqual((row.status, row.reason), (D.FAILED, 'INVALID_COMMAND'))
        self.assertIsNone(row.task_sha256)
        self.assertNotIn(b'secret', recorder.snapshot().encode())

    def test_invalid_partial_source_and_cancellation(self):
        spec, batch, _ = inputs('trades')
        class Broken(Source):
            def iter_batches(self, request, token): raise ValueError('secret source path')
        recorder = ProgressRecorder()
        with self.assertRaises(CommandError):
            run_session(spec, Broken(batch, spec.request), ExampleSink(), requirements=LIMITS, progress=recorder)
        self.assertEqual(recorder.snapshot().tasks[-1].reason, 'SOURCE_FAILED')
        partial = replace(batch, metadata=replace(batch.metadata, coverage=replace(batch.metadata.coverage, expected=4, complete=False)))
        outcome = run_session(spec, Source(partial, spec.request), ExampleSink(), requirements=LIMITS, progress=recorder)
        self.assertEqual(outcome.output.task.inputs[0].state, 'partial')
        token = type('Stop', (), {'is_cancelled': lambda self: True})()
        with self.assertRaises(CommandError):
            run_session(spec, Source(batch, spec.request), ExampleSink(), requirements=LIMITS, cancellation=token, progress=recorder)
        self.assertEqual(recorder.snapshot().tasks[-1].status, D.CANCELLED)

    def test_supervisor_all_modes_full_wire_parity_and_scoped_failure(self):
        items = tuple(work(member)[0] for member in ('A', 'B'))
        reference = BoundedSupervisor().run(items)
        for mode in ('sequential', 'thread', 'process'):
            recorder = ProgressRecorder()
            outcome = BoundedSupervisor(mode=mode, budget=budget()).run(items, progress=recorder)
            self.assertEqual(tuple(tuple(map(encode_result, e.results)) for e in outcome.tasks),
                             tuple(tuple(map(encode_result, e.results)) for e in reference.tasks))
            rows = recorder.snapshot().tasks
            self.assertTrue(all(t.status is D.COMPUTED for t in rows))
            self.assertTrue(all(tuple(s.stage for s in t.timings) == (S.QUEUE_WAIT, S.CALCULATION, S.SERIALIZATION) for t in rows))
        recorder = ProgressRecorder()
        outcome = BoundedSupervisor().run((work('A', failed)[0], work('B')[0], work('C', wrong)[0]), progress=recorder)
        self.assertEqual(sorted(t.status.value for t in recorder.snapshot().tasks), ['computed', 'failed', 'failed'])
        self.assertNotIn(b'secret', recorder.snapshot().encode())
        self.assertEqual(sum(e.results is not None for e in outcome.tasks), 1)

    def test_supervisor_reservation_precedes_calculation(self):
        CALLS.clear()
        with self.assertRaises(SinkError):
            BoundedSupervisor().run((work('A', counted)[0],), progress=ProgressRecorder(max_spans=2))
        self.assertEqual(CALLS, [])

    def test_real_both_sink_pipeline_generation_catalog_and_recovery(self):
        for kind in ('parquet', 'duckdb'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory, sink_for(kind, Path(directory)) as inner:
                sink = Proxy(inner); recorder = ProgressRecorder()
                item, _ = work()
                publisher = SerialPublisher(sink, item.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
                outcome = BoundedSupervisor().run((item,), publisher=publisher, progress=recorder)
                output = outcome.tasks[0].output
                self.assertIsNotNone(output)
                self.assertEqual(recorder.snapshot().tasks[-1].status, D.VERIFIED)
                begins = sink.begins
                publisher.submit(item.task, outcome.tasks[0].results, progress=recorder)
                replay = publisher.drain(progress=recorder)[0]
                self.assertEqual(replay.output, output)
                self.assertEqual(sink.begins - begins, ORACLE['invariants']['committed_recovery_new_begin_count'])
                catalog, generations = stores(directory)
                spec = GenerationSpec(item.task.config.session.namespace, item.task.job_id, item.task.generation_id, (item.task,))
                dependencies = (Dependency(item.task.task_sha256, item.task, output, sink),)
                self.assertTrue(generations.publish(spec, dependencies, progress=recorder).complete)
                self.assertEqual(recorder.snapshot().tasks[-1].status, D.GENERATION_COMPLETE)
                self.assertIsNone(catalog.snapshot(spec.namespace, spec.job_id))
                selected = catalog.select(spec, generations, dependencies, expected_sequence=0, progress=recorder)
                self.assertTrue(selected.accepted)
                self.assertEqual(recorder.snapshot().tasks[-1].status, D.CATALOG_ACCEPTED)
                self.assertEqual(sink.begins, begins)

    def test_uncertain_commit_explicit_retry_keeps_original_receipt(self):
        for kind in ('parquet', 'duckdb'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory, sink_for(kind, Path(directory)) as inner:
                class Uncertain(Proxy):
                    hidden = False
                    def commit(self, session):
                        self.receipt = self.inner.commit(session); self.hidden = True
                        raise SinkError(SinkErrorCode.COMMIT_UNKNOWN)
                    def lookup(self, key):
                        if self.hidden: raise SinkError(SinkErrorCode.UNAVAILABLE)
                        return self.inner.lookup(key)
                sink = Uncertain(inner); item, _ = work(); recorder = ProgressRecorder()
                result = BoundedSupervisor().run((item,)).tasks[0].results
                publisher = SerialPublisher(sink, item.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
                publisher.submit(item.task, result, progress=recorder)
                self.assertIsNone(publisher.drain(progress=recorder)[0].output)
                self.assertEqual(recorder.snapshot().tasks[-1].reason, 'COMMIT_UNKNOWN')
                sink.hidden = False
                recovered = publisher.drain(progress=recorder)[0]
                self.assertEqual(recovered.output.receipt, sink.receipt)
                self.assertEqual(sink.begins, 1)
                self.assertEqual(recorder.snapshot().tasks[-1].status, D.VERIFIED)

    def test_backend_busy_is_distinct_from_queue_probe(self):
        item, _ = work(); result = BoundedSupervisor().run((item,)).tasks[0].results
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as inner:
            class Busy(Proxy):
                def begin(self, envelope): raise SinkError(SinkErrorCode.BUSY)
            recorder = ProgressRecorder()
            publisher = SerialPublisher(Busy(inner), item.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
            publisher.submit(item.task, result, progress=recorder)
            self.assertEqual(publisher.drain(progress=recorder)[0].reason, 'BUSY')
            rows = recorder.snapshot().tasks
            self.assertEqual([t.stage for t in rows[0].timings], [S.SERIALIZATION, S.QUEUE_PROBE])
            self.assertEqual(rows[0].timings[-1].probes, 1)
            self.assertEqual(rows[1].timings[-1].stage, S.BACKEND_PROBE)
            self.assertEqual(rows[1].timings[-1].probes, 1)
            publisher._lock.acquire()
            try:
                with self.assertRaises(SinkError): publisher.submit(item.task, result, progress=recorder)
            finally: publisher._lock.release()
            self.assertEqual(recorder.snapshot().tasks[-1].reason, 'BUSY')
            self.assertEqual(recorder.snapshot().tasks[-1].timings[-1].stage, S.QUEUE_PROBE)

    def test_dependency_waiting_is_scoped_and_report_does_not_certify(self):
        first, _ = work('A'); unrelated, _ = work('B')
        nodes = (TaskNode(first.task, required=(unrelated.task.task_sha256,)), TaskNode(unrelated.task))
        recorder = ProgressRecorder()
        outcome = evaluate_readiness(nodes, (), limits=BarrierLimits(), requirements=LIMITS, progress=recorder)
        self.assertEqual(outcome.ready_tasks, (unrelated.task,))
        self.assertEqual([t.status for t in recorder.snapshot().tasks], [D.WAITING, D.READY])

    def test_source_to_both_physical_sinks_all_session_families(self):
        for kind in ('parquet', 'duckdb'):
            for family in ('bars', 'trades', 'quotes'):
                with self.subTest(kind=kind, family=family), tempfile.TemporaryDirectory() as directory, sink_for(kind, Path(directory)) as sink:
                    spec, batch, source = inputs(family)
                    recorder = ProgressRecorder()
                    outcome = run_session(spec, source, sink, requirements=LIMITS, progress=recorder)
                    reference = run_session(spec, Source(batch, spec.request), ExampleSink(), requirements=LIMITS)
                    self.assertEqual(tuple(map(encode_result, outcome.results)), tuple(map(encode_result, reference.results)))
                    self.assertEqual(tuple(map(encode_result, sink.read(outcome.output.receipt))), tuple(map(encode_result, outcome.results)))
                    row = recorder.snapshot().tasks[0]
                    self.assertEqual((row.family, row.task_sha256, row.status), (family, outcome.output.task.task_sha256, D.VERIFIED))
                    self.assertEqual(tuple(t.stage for t in row.timings), (S.ACQUISITION, S.CALCULATION, S.SERIALIZATION, S.PUBLICATION))
                    self.assertEqual(source.called, 1)

    def test_partial_write_cancel_retains_task_and_explicit_retry(self):
        for kind in ('parquet', 'duckdb'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory, sink_for(kind, Path(directory)) as inner:
                token = type('Token', (), {'stop': False, 'is_cancelled': lambda self: self.stop})()
                class CancelWrite(Proxy):
                    def write(self, session, ordinal, result):
                        self.inner.write(session, ordinal, result); token.stop = True
                sink = CancelWrite(inner); item, _ = work(); recorder = ProgressRecorder()
                result = BoundedSupervisor().run((item,)).tasks[0].results
                publisher = SerialPublisher(sink, item.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
                publisher.submit(item.task, result)
                first = publisher.drain(cancellation=token, progress=recorder)[0]
                self.assertIsNone(first.output)
                self.assertEqual(recorder.snapshot().tasks[-1].status, D.CANCELLED)
                self.assertEqual(publisher.pending, (item.task.task_sha256,))
                catalog, _ = stores(directory)
                self.assertIsNone(catalog.snapshot(item.task.config.session.namespace, item.task.job_id))
                token.stop = False
                second = publisher.drain(progress=recorder)[0]
                self.assertIsNotNone(second.output)
                self.assertEqual(recorder.snapshot().tasks[-1].status, D.VERIFIED)
                self.assertEqual(len(recorder.snapshot().tasks), 2)

    def test_corrupt_readback_preserves_commit_and_no_catalog_acceptance(self):
        for kind in ('parquet', 'duckdb'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory, sink_for(kind, Path(directory)) as inner:
                class Corrupt(Proxy):
                    broken = True
                    def read(self, receipt):
                        if self.broken: raise ValueError('secret private backend path')
                        return self.inner.read(receipt)
                sink = Corrupt(inner); item, _ = work(); recorder = ProgressRecorder()
                result = BoundedSupervisor().run((item,)).tasks[0].results
                publisher = SerialPublisher(sink, item.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
                publisher.submit(item.task, result)
                self.assertIsNone(publisher.drain(progress=recorder)[0].output)
                self.assertEqual(recorder.snapshot().tasks[-1].reason, 'READBACK_FAILED')
                self.assertNotIn(b'secret', recorder.snapshot().encode())
                catalog, _ = stores(directory)
                self.assertIsNone(catalog.snapshot(item.task.config.session.namespace, item.task.job_id))
                sink.broken = False
                self.assertIsNotNone(publisher.drain(progress=recorder)[0].output)
                self.assertEqual(sink.begins, 1)

    def test_child_sidecar_fault_is_unavailable_and_preserves_verified_result(self):
        from equity_feature_workers.supervisor import _compute_partition
        item, _ = work()
        def malformed(*args, **kwargs):
            execution = _compute_partition(*args, **kwargs)[0]
            timing = StageTiming(S.CALCULATION, 1)
            object.__setattr__(timing, 'elapsed_ns', 'secret telemetry payload')
            return (replace(execution, timings=(timing,)),)
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as sink:
            publisher = SerialPublisher(sink, item.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
            recorder = ProgressRecorder()
            with patch('equity_feature_workers.supervisor._compute_partition', malformed):
                outcome = BoundedSupervisor(mode='thread').run((item,), publisher=publisher, progress=recorder)
            self.assertIsNotNone(outcome.tasks[0].output)
            row = recorder.snapshot().tasks[0]
            self.assertEqual(row.status, D.VERIFIED)
            self.assertTrue(all(t.elapsed_ns is None for t in row.timings[-3:]))
            self.assertNotIn(b'secret', recorder.snapshot().encode())

    def test_process_sidecar_transport_and_resident_reserve_precede_pool(self):
        from equity_feature_workers import TaskExecution
        item, _ = work()
        legacy_bound = max(len(pickle.dumps(TaskExecution(item.task, None, None, None, code.value), protocol=5)) for code in SinkErrorCode)
        with patch('equity_feature_workers.supervisor.ProcessPoolExecutor') as pool:
            with self.assertRaises(SinkError):
                BoundedSupervisor(mode='process', budget=budget(max_task_transport_bytes=legacy_bound)).run((item,), progress=ProgressRecorder())
            pool.assert_not_called()
        baseline = BoundedSupervisor().run((item,))
        recorder = ProgressRecorder()
        observed = BoundedSupervisor().run((item,), progress=recorder)
        self.assertEqual(observed.reserved_resident_bytes - baseline.reserved_resident_bytes, 512 + 2048 + 3 * 256)
        with self.assertRaises(SinkError):
            BoundedSupervisor(budget=budget(max_resident_bytes=observed.reserved_resident_bytes - 1)).run((item,), progress=ProgressRecorder())

    def test_reentry_guard_precedes_supervisor_clock_hooks(self):
        item, _ = work(); supervisor = BoundedSupervisor(); recorder = ProgressRecorder()
        calls = []
        def clock():
            with self.assertRaises(SinkError) as caught:
                supervisor.run((item,))
            calls.append(caught.exception.code)
            return len(calls)
        with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', clock):
            outcome = supervisor.run((item,), progress=recorder)
        self.assertIsNotNone(outcome.tasks[0].results)
        self.assertTrue(calls)
        self.assertTrue(all(code is SinkErrorCode.INVALID_SESSION for code in calls))

    def test_budgeted_wire_and_failure_terminal_slot(self):
        recorder = ProgressRecorder(max_tasks=1, max_spans=1, max_bytes=512 + 2048 + 256)
        with self.assertRaises(SinkError):
            with recorder.attempt('f' * 64, 'secret/path', 'secret/partition', spans=1) as observation:
                with observation.stage(S.CALCULATION):
                    with observation.stage(S.PUBLICATION): self.fail('over quota work ran')
        row = recorder.snapshot().tasks[0]
        self.assertEqual((row.status, row.reason), (D.FAILED, 'RESOURCE_LIMIT'))
        self.assertEqual(len(row.timings), 1)
        self.assertLessEqual(len(recorder.snapshot().encode()), recorder.max_bytes)
        self.assertNotIn(b'secret', recorder.snapshot().encode())

    def test_actual_work_interruption_is_not_bad_clock(self):
        recorder = ProgressRecorder()
        with self.assertRaises(KeyboardInterrupt):
            with recorder.attempt('f' * 64, 'bars', 'P', spans=1) as observation:
                with observation.stage(S.CALCULATION): raise KeyboardInterrupt
        self.assertEqual((recorder.snapshot().tasks[0].status, recorder.snapshot().tasks[0].reason), (D.FAILED, 'INTERRUPTED'))

    def test_catalog_reentry_before_clock_preserves_acceptance(self):
        from test_catalog import publish
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as sink:
            spec, dependencies, _ = publish(directory, sink, 'A')
            catalog, generations = stores(directory)
            recorder = ProgressRecorder(); errors = []
            def clock():
                with self.assertRaises(SinkError) as caught:
                    catalog.select(spec, generations, dependencies, expected_sequence=0)
                errors.append(caught.exception.code)
                return len(errors)
            with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', clock):
                accepted = catalog.select(spec, generations, dependencies, expected_sequence=0, progress=recorder)
            self.assertTrue(accepted.accepted)
            self.assertTrue(errors)
            self.assertTrue(all(code is SinkErrorCode.BUSY for code in errors))
            self.assertTrue(all(t.status is D.CATALOG_ACCEPTED for t in recorder.snapshot().tasks))

    def test_generation_and_catalog_failure_retain_timing_and_fixed_reason(self):
        from test_catalog import publish
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as sink:
            spec, dependencies, _ = publish(directory, sink, 'A')
            catalog, generations = stores(directory)
            recorder = ProgressRecorder()
            with patch.object(generations, '_load', side_effect=SinkError(SinkErrorCode.CORRUPTION)):
                with self.assertRaises(SinkError): generations.publish(spec, dependencies, progress=recorder)
            self.assertTrue(all(t.reason == 'CORRUPTION' and t.timings[0].stage is S.GENERATION for t in recorder.snapshot().tasks))
            with patch.object(catalog, '_load', side_effect=SinkError(SinkErrorCode.CORRUPTION)):
                with self.assertRaises(SinkError): catalog.select(spec, generations, dependencies, expected_sequence=0, progress=recorder)
            self.assertTrue(all(t.reason == 'CORRUPTION' and t.timings[0].stage is S.CATALOG for t in recorder.snapshot().tasks[-3:]))
            self.assertIsNone(catalog.snapshot(spec.namespace, spec.job_id))

    def test_unknown_error_code_hook_is_never_evaluated(self):
        class Hostile(ValueError):
            @property
            def code(self): raise AssertionError('secret code getter ran')
        recorder = ProgressRecorder()
        with self.assertRaises(Hostile):
            with recorder.attempt('f' * 64, 'bars', 'P', spans=1): raise Hostile('secret')
        self.assertEqual(recorder.snapshot().tasks[0].reason, 'UNAVAILABLE')
        self.assertNotIn(b'secret', recorder.snapshot().encode())

    def test_readiness_already_verified_is_not_waiting(self):
        spec, _, source = inputs()
        sink = ExampleSink()
        command = run_session(spec, source, sink, requirements=LIMITS)
        task = command.output.task
        recorder = ProgressRecorder()
        outcome = evaluate_readiness((TaskNode(task),), (Dependency(task.task_sha256, task, command.output, sink),),
            limits=BarrierLimits(), requirements=LIMITS, progress=recorder)
        self.assertEqual(outcome.ready_tasks, ())
        self.assertEqual((recorder.snapshot().tasks[0].status, recorder.snapshot().tasks[0].reason), (D.VERIFIED, None))

    def test_publisher_fence_blocks_default_operations_through_all_clock_hooks(self):
        for operation in ('submit', 'drain'):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as inner:
                sink = Proxy(inner); first, _ = work('A'); second, _ = work('B')
                results = BoundedSupervisor().run((first, second)).tasks
                bykey = {e.task.task_sha256: e.results for e in results}
                publisher = SerialPublisher(sink, first.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
                publisher.submit(first.task, bykey[first.task.task_sha256])
                recorder = ProgressRecorder(); rejected = []
                def clock():
                    for call in (lambda: publisher.drain(), lambda: publisher.submit(second.task, bykey[second.task.task_sha256])):
                        with self.assertRaises(SinkError) as caught: call()
                        rejected.append(caught.exception.code)
                    return len(rejected)
                with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', clock):
                    if operation == 'submit':
                        publisher.submit(second.task, bykey[second.task.task_sha256], progress=recorder)
                        self.assertEqual(sink.begins, 0)
                        self.assertEqual(set(publisher.pending), {first.task.task_sha256, second.task.task_sha256})
                    else:
                        outcomes = publisher.drain(progress=recorder)
                        self.assertEqual(len(outcomes), 1)
                        self.assertIsNotNone(outcomes[0].output)
                        self.assertEqual(sink.begins, 1)
                self.assertGreaterEqual(len(rejected), 4)
                self.assertTrue(all(reason is SinkErrorCode.BUSY for reason in rejected))
                self.assertFalse(publisher._observing)
                publisher.drain()

    def test_foreign_default_producer_remains_allowed_during_observed_submit(self):
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as sink:
            first, _ = work('A'); second, _ = work('B')
            bykey = {e.task.task_sha256: e.results for e in BoundedSupervisor().run((first, second)).tasks}
            publisher = SerialPublisher(sink, first.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
            called = []; returned = []; errors = []
            def foreign():
                try: returned.append(publisher.submit(second.task, bykey[second.task.task_sha256]))
                except Exception as error: errors.append(error)
            def clock():
                if not called:
                    called.append(True)
                    thread = threading.Thread(target=foreign); thread.start(); thread.join(5)
                    self.assertFalse(thread.is_alive())
                return 1
            with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', clock):
                publisher.submit(first.task, bykey[first.task.task_sha256], progress=ProgressRecorder())
            self.assertEqual(errors, [])
            self.assertEqual(returned, [second.task.task_sha256])
            self.assertEqual(len(publisher.drain()), 2)

    def test_supervisor_publisher_fence_precedes_initial_and_final_clocks(self):
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as inner:
            sink = Proxy(inner); first, _ = work('A'); second, _ = work('B')
            result = BoundedSupervisor().run((first,)).tasks[0].results
            publisher = SerialPublisher(sink, first.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
            publisher.submit(first.task, result)
            rejected = []
            def clock():
                with self.assertRaises(SinkError) as caught: publisher.drain()
                rejected.append(caught.exception.code)
                return len(rejected)
            with patch('equity_feature_workers.diagnostics.time.perf_counter_ns', clock):
                with self.assertRaises(SinkError) as caught:
                    BoundedSupervisor().run((second,), publisher=publisher, progress=ProgressRecorder())
            self.assertEqual(caught.exception.code, SinkErrorCode.BUSY)
            self.assertEqual(sink.begins, 0)
            self.assertEqual(publisher.pending, (first.task.task_sha256,))
            self.assertTrue(rejected)
            self.assertTrue(all(code is SinkErrorCode.BUSY for code in rejected))
            self.assertFalse(publisher._observing)
            self.assertIsNotNone(publisher.drain()[0].output)

    def test_empty_readiness_owner_and_active_preflight_before_dependency_hooks(self):
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as inner:
            class Reads(Proxy):
                reads = 0
                def read(self, receipt):
                    self.reads += 1
                    return self.inner.read(receipt)
            sink = Reads(inner); spec, _, source = inputs()
            command = run_session(spec, source, sink, requirements=LIMITS)
            dependency = Dependency(command.output.task.task_sha256, command.output.task, command.output, sink)
            recorder = ProgressRecorder(); polls = []; errors = []
            token = type('Token', (), {'is_cancelled': lambda self: polls.append(1) or False})()
            def call():
                return evaluate_readiness((), (dependency,), limits=BarrierLimits(), requirements=LIMITS,
                                          cancellation=token, progress=recorder)
            def foreign():
                try: call()
                except SinkError as error: errors.append(error.code)
            baseline = sink.reads
            thread = threading.Thread(target=foreign); thread.start(); thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [SinkErrorCode.INVALID_SESSION])
            self.assertEqual((polls, sink.reads), ([], baseline))
            with recorder.attempt('f' * 64, 'bars', 'P', spans=1):
                with self.assertRaises(SinkError) as caught: call()
                self.assertEqual(caught.exception.code, SinkErrorCode.INVALID_SESSION)
                self.assertEqual((polls, sink.reads), ([], baseline))
            self.assertTrue(call().barrier.ready)
            self.assertGreater(sink.reads, baseline)

    def test_malformed_clock_metadata_is_unavailable_and_snapshot_valid(self):
        class ClockName(str): pass
        infos = (
            SimpleNamespace(implementation=ClockName('QueryPerformanceCounter()'), monotonic=True, resolution=1e-9),
            SimpleNamespace(implementation='QueryPerformanceCounter()', monotonic=1, resolution=1e-9),
            SimpleNamespace(implementation='QueryPerformanceCounter()', monotonic=True, resolution=True),
            SimpleNamespace(implementation='QueryPerformanceCounter()', monotonic=True, resolution=float('nan')),
            SimpleNamespace(implementation='QueryPerformanceCounter()', monotonic=True, resolution=-1),
            SimpleNamespace(implementation='secret private implementation', monotonic=True, resolution=1e-9),
        )
        spec, batch, _ = inputs()
        for info in infos:
            recorder = ProgressRecorder()
            with patch('equity_feature_workers.diagnostics.time.get_clock_info', return_value=info):
                command = run_session(spec, Source(batch, spec.request), ExampleSink(), requirements=LIMITS, progress=recorder)
            snapshot = recorder.snapshot()
            self.assertEqual((snapshot.clock, snapshot.resolution_ns), ('unavailable', None))
            self.assertTrue(command.output.committed)
            self.assertEqual(snapshot.tasks[0].status, D.VERIFIED)
            self.assertNotIn(b'secret', snapshot.encode())

    def test_observed_supervisor_retains_foreign_queued_work_for_explicit_drain(self):
        from equity_feature_workers.supervisor import _compute_partition
        with tempfile.TemporaryDirectory() as directory, sink_for('parquet', Path(directory)) as inner:
            sink = Proxy(inner); item, _ = work('A'); foreign, _ = work('B')
            result = BoundedSupervisor().run((foreign,)).tasks[0].results
            publisher = SerialPublisher(sink, item.task.destination_scope, limits=PublicationLimits(), requirements=LIMITS)
            submitted = []; errors = []
            def producer():
                try: submitted.append(publisher.submit(foreign.task, result))
                except Exception as error: errors.append(error)
            def calculating(*args, **kwargs):
                thread = threading.Thread(target=producer); thread.start(); thread.join(5)
                self.assertFalse(thread.is_alive())
                return _compute_partition(*args, **kwargs)
            recorder = ProgressRecorder()
            with patch('equity_feature_workers.supervisor._compute_partition', calculating):
                outcome = BoundedSupervisor().run((item,), publisher=publisher, progress=recorder)
            self.assertEqual(errors, [])
            self.assertEqual(submitted, [foreign.task.task_sha256])
            self.assertIsNotNone(outcome.tasks[0].output)
            self.assertEqual(publisher.pending, (foreign.task.task_sha256,))
            self.assertEqual(sink.begins, 1)
            self.assertEqual([t.task_sha256 for t in recorder.snapshot().tasks], [item.task.task_sha256])
            self.assertIsNotNone(publisher.drain()[0].output)
            self.assertEqual(sink.begins, 2)

    def test_clock_metadata_properties_are_copied_once_before_admission(self):
        class ClockName(str): pass
        class Changing:
            def __init__(self): self.reads = {'implementation': 0, 'monotonic': 0, 'resolution': 0}
            @property
            def implementation(self):
                self.reads['implementation'] += 1
                return 'QueryPerformanceCounter()' if self.reads['implementation'] == 1 else ClockName('QueryPerformanceCounter()')
            @property
            def monotonic(self):
                self.reads['monotonic'] += 1
                return True if self.reads['monotonic'] == 1 else 'secret malformed monotonic'
            @property
            def resolution(self):
                self.reads['resolution'] += 1
                return 1e-9 if self.reads['resolution'] == 1 else float('nan')
        info = Changing(); spec, _, source = inputs(); recorder = ProgressRecorder()
        with patch('equity_feature_workers.diagnostics.time.get_clock_info', return_value=info):
            command = run_session(spec, source, ExampleSink(), requirements=LIMITS, progress=recorder)
        self.assertEqual(info.reads, {'implementation': 1, 'monotonic': 1, 'resolution': 1})
        self.assertTrue(command.output.committed)
        self.assertEqual((recorder.snapshot().clock, recorder.snapshot().resolution_ns), ('QueryPerformanceCounter()', 1))
        self.assertEqual(recorder.snapshot().tasks[0].status, D.VERIFIED)
        self.assertNotIn(b'secret', recorder.snapshot().encode())

    def test_retained_report_history_is_in_combined_resident_gate_before_work(self):
        for mode in ('sequential', 'process'):
            with self.subTest(mode=mode):
                item, _ = work('A', counted)
                fresh = BoundedSupervisor(mode=mode).run((item,), progress=ProgressRecorder())
                recorder = ProgressRecorder()
                for index in range(20):
                    with recorder.attempt(f'{index:064x}', 'bars', 'P', spans=1) as observation:
                        observation.add(StageTiming(S.CALCULATION, 1))
                        observation.finish(D.COMPUTED)
                prior = recorder.reserved_bytes
                self.assertGreater(prior, 512)
                CALLS.clear()
                supervisor = BoundedSupervisor(mode=mode, budget=budget(max_resident_bytes=fresh.reserved_resident_bytes))
                with patch('equity_feature_workers.supervisor.ProcessPoolExecutor', side_effect=AssertionError('pool started before retained report admission')) as pool:
                    with self.assertRaises(SinkError) as caught:
                        supervisor.run((item,), progress=recorder)
                    self.assertEqual(caught.exception.code, SinkErrorCode.RESOURCE_LIMIT)
                    pool.assert_not_called()
                self.assertEqual(CALLS, [])
                self.assertGreater(recorder.reserved_bytes, prior)
                self.assertEqual(recorder.snapshot().tasks[-1].reason, 'RESOURCE_LIMIT')


if __name__ == '__main__': unittest.main()
