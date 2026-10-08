"""Whole-task local execution; caller-owned calculations and serialized I/O."""
from __future__ import annotations

from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED
from contextlib import nullcontext
from dataclasses import dataclass, replace
import hashlib
import multiprocessing
import os
from pathlib import Path
import pickle
import threading
from types import FunctionType
from typing import Callable, NoReturn
import uuid

from equity_feature_contracts import CanonicalBatch, FeatureResult
from equity_feature_io_contracts import SinkError, SinkErrorCode, SinkRequirements
from equity_feature_io_sdk import Cancellation, decode_result, encode_result, prepare_publication
from .diagnostics import (ProgressRecorder, DiagnosticStage, DiagnosticStatus, StageTiming,
                          _Attempt, counter, interval, fixed_reason)
from .barriers import _admit_entities
from .commands import CommandError, CommandErrorCode, NeverCancelled, _cancel, compute_session_inputs
from .manifests import OutputManifest, TaskManifest, decode_task, encode_output, encode_task, integer
from .publication import SerialPublisher

Calculation = Callable[[TaskManifest, tuple[CanonicalBatch, ...]], tuple[FeatureResult, ...]]


def _fail(code: SinkErrorCode) -> NoReturn:
    raise SinkError(code) from None


def _size(value: object) -> int:
    try:
        return len(pickle.dumps(value, protocol=5))
    except Exception:
        _fail(SinkErrorCode.INVALID_CONFIG)


def _inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> int:
    if type(task) is not TaskManifest or type(batches) is not tuple or any(type(b) is not CanonicalBatch for b in batches):
        _fail(SinkErrorCode.INVALID_CONFIG)
    if len(batches) > task.max_input_batches or len({b.metadata.source.input_id for b in batches}) != len(batches):
        _fail(SinkErrorCode.INVALID_CONTENT)
    if any(b.metadata.coverage.observed != b.row_count for b in batches):
        _fail(SinkErrorCode.INVALID_CONTENT)
    bindings = tuple(i.binding for i in task.inputs if i.binding is not None)
    if any(not any((b.kind, b.metadata) == (x.kind, x.metadata) for x in bindings) for b in batches):
        _fail(SinkErrorCode.INVALID_CONTENT)
    size = _size(batches)
    if size > task.max_input_bytes:
        _fail(SinkErrorCode.RESOURCE_LIMIT)
    return size


@dataclass(frozen=True)
class WorkItem:
    task: TaskManifest
    batches: tuple[CanonicalBatch, ...]
    calculate: Calculation = compute_session_inputs

    def __post_init__(self) -> None:
        _inputs(self.task, self.batches)
        # Pure top-level local functions: do not serialize callback closures/handles.
        if type(self.calculate) is not FunctionType or self.calculate.__closure__ is not None or '<locals>' in self.calculate.__qualname__:
            _fail(SinkErrorCode.INVALID_CONFIG)
        _size(self.calculate)

    @property
    def rows(self) -> int:
        return sum(b.row_count for b in self.batches)


class InputReuseCache:
    """Exact sealed scope cache. No acquisition, reload or implicit eviction."""
    def __init__(self, *, max_entries: int = 64, max_bytes: int = 8388608) -> None:
        try:
            integer(max_entries, 1); integer(max_bytes, 1)
            if max_entries > 4096:
                raise ValueError
        except Exception:
            _fail(SinkErrorCode.INVALID_CONFIG)
        self.max_entries, self.max_bytes = max_entries, max_bytes
        self._owner = threading.current_thread()
        self._entries: dict[str, tuple[CanonicalBatch, ...]] = {}
        self._bytes = 0

    def _own(self) -> None:
        if threading.current_thread() is not self._owner:
            _fail(SinkErrorCode.INVALID_SESSION)

    def put(self, task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> None:
        self._own(); size = _inputs(task, batches); key = task.reuse_sha256
        prior = self._entries.get(key)
        if prior is not None:
            if prior != batches:
                _fail(SinkErrorCode.CONFLICT)
            return
        if len(self._entries) >= self.max_entries or self._bytes + size > self.max_bytes:
            _fail(SinkErrorCode.RESOURCE_LIMIT)
        self._entries[key] = batches
        self._bytes += size

    def get(self, task: TaskManifest) -> tuple[CanonicalBatch, ...] | None:
        self._own()
        if type(task) is not TaskManifest:
            _fail(SinkErrorCode.INVALID_CONFIG)
        return self._entries.get(task.reuse_sha256)

    def clear(self) -> None:
        self._own(); self._entries.clear(); self._bytes = 0


@dataclass(frozen=True)
class ResourceBudget:
    max_tasks: int = 64
    workers: int = 1
    max_cpu_slots: int = 2
    compute_threads: int = 1
    backend_threads: int = 1
    max_in_flight: int = 1
    io_slots: int = 1
    max_input_bytes: int = 16777216
    max_resident_bytes: int = 67108864
    max_task_result_bytes: int = 262144
    max_task_transport_bytes: int = 1048576
    max_total_result_bytes: int = 16777216
    max_result_cells: int = 100000
    max_evidence_rows: int = 10000

    def __post_init__(self) -> None:
        try:
            for value in self.__dict__.values():
                integer(value, 1)
            if self.max_tasks > 4096 or self.workers > 32 or self.max_in_flight > self.workers or self.io_slots != 1:
                raise ValueError
            if self.workers * self.compute_threads + self.backend_threads > self.max_cpu_slots:
                raise ValueError
            if self.max_task_result_bytes > self.max_total_result_bytes:
                raise ValueError
        except Exception:
            _fail(SinkErrorCode.INVALID_CONFIG)


@dataclass(frozen=True)
class Partition:
    ordinal: int
    items: tuple[WorkItem, ...]


def partition_tasks(items: tuple[WorkItem, ...], workers: int = 1) -> tuple[Partition, ...]:
    try:
        integer(workers, 1)
        if workers > 32 or type(items) is not tuple or not items or len(items) > 4096 or any(type(i) is not WorkItem for i in items):
            raise ValueError
        if len({i.task.task_sha256 for i in items}) != len(items):
            _fail(SinkErrorCode.CONFLICT)
    except SinkError:
        raise
    except Exception:
        _fail(SinkErrorCode.INVALID_CONFIG)
    # Connected components keep intersecting instrument populations together.
    groups: list[tuple[set[str], list[WorkItem]]] = []
    for item in sorted(items, key=lambda i: i.task.task_sha256):
        population = set(item.task.instruments); members = [item]
        remaining = []
        for instruments, group in groups:
            if population.intersection(instruments):
                population.update(instruments); members.extend(group)
            else:
                remaining.append((instruments, group))
        # A newly combined population can connect a previously examined component.
        while True:
            linked = [g for g in remaining if population.intersection(g[0])]
            if not linked:
                break
            for instruments, group in linked:
                population.update(instruments); members.extend(group); remaining.remove((instruments, group))
        groups = remaining + [(population, members)]
    groups.sort(key=lambda g: tuple(sorted(g[0])))
    shards: list[list[WorkItem]] = [[] for _ in range(min(workers, len(groups)))]
    weights = [0] * len(shards)
    for _, members in groups:
        members.sort(key=lambda i: (i.task.config.session.open_ns, i.task.task_sha256))
        index = min(range(len(shards)), key=lambda n: (weights[n], n))
        shards[index].extend(members); weights[index] += sum(max(i.rows, 1) for i in members)
    result = tuple(Partition(n, tuple(shard)) for n, shard in enumerate(shards))
    assert {i.task.task_sha256 for p in result for i in p.items} == {i.task.task_sha256 for i in items}
    return result


def _validate_results(task: TaskManifest, results: tuple[FeatureResult, ...], budget: ResourceBudget) -> int:
    if type(budget) is not ResourceBudget:
        _fail(SinkErrorCode.INVALID_CONFIG)
    if type(results) is not tuple or any(type(r) is not FeatureResult for r in results):
        _fail(SinkErrorCode.INVALID_CONTENT)
    try:
        envelope = prepare_publication(results, destination_scope=task.destination_scope, generation_id=task.generation_id,
            job_id=task.job_id, partition_id=task.partition_id,
            limits=SinkRequirements(max_results=64, max_chunk_bytes=budget.max_task_result_bytes,
                max_total_bytes=budget.max_task_result_bytes, max_result_cells=budget.max_result_cells,
                max_evidence_rows=budget.max_evidence_rows))
        _admit_entities(task, results)
        OutputManifest(task, envelope)
        return envelope.content_bytes
    except SinkError:
        raise
    except Exception:
        _fail(SinkErrorCode.INVALID_CONTENT)


@dataclass(frozen=True)
class SpillReference:
    run_id: str
    task_sha256: str
    files: tuple[tuple[str, int, str], ...]


class ResultSpill:
    """Exclusively owned local run directory; existing task/result codecs only."""
    def __init__(self, root: Path, *, max_bytes: int = 33554432, max_tasks: int = 64,
                 protected_sources: tuple[Path, ...] = ()) -> None:
        try:
            if os.name not in ('nt', 'posix') or type(root) not in (Path, type(Path())) or str(root).startswith('\\\\') or root.is_symlink():
                raise ValueError
            integer(max_bytes, 1); integer(max_tasks, 1)
            if max_tasks > 4096 or type(protected_sources) is not tuple or len(protected_sources) > 4096:
                raise ValueError
            target = root.resolve()
            for source in protected_sources:
                if not isinstance(source, Path) or source.resolve().is_relative_to(target) or target.is_relative_to(source.resolve()):
                    raise ValueError
        except Exception:
            _fail(SinkErrorCode.INVALID_CONFIG)
        self.root, self.max_bytes, self.max_tasks = target, max_bytes, max_tasks
        self.run_id = uuid.uuid4().hex
        self.directory = target / ('efworker-spill-' + self.run_id)
        self._owner = threading.current_thread()
        self._used = 0
        self._tasks = 0
        self._owned: dict[str, tuple[int, str]] = {}
        self._references: set[SpillReference] = set()
        self._created = False
        self._closed = False

    def _own(self) -> None:
        if self._closed or threading.current_thread() is not self._owner:
            _fail(SinkErrorCode.INVALID_SESSION)
        if self.root.is_symlink() or self.directory.is_symlink():
            _fail(SinkErrorCode.CORRUPTION)

    @property
    def remaining_bytes(self) -> int:
        self._own(); return self.max_bytes - self._used

    def write(self, task: TaskManifest, results: tuple[FeatureResult, ...], *, budget: ResourceBudget) -> SpillReference:
        self._own(); _validate_results(task, results, budget)
        payloads = [('task.json', encode_task(task))] + [(f'result-{n}.json', encode_result(r)) for n, r in enumerate(results)]
        size = sum(len(b) for _, b in payloads)
        if self._tasks >= self.max_tasks or self._used + size > self.max_bytes:
            _fail(SinkErrorCode.RESOURCE_LIMIT)
        # Reserve the whole attempt, retaining reservation after partial failure.
        self._tasks += 1; self._used += size
        prefix = task.task_sha256 + '-' + uuid.uuid4().hex
        files = []
        try:
            if not self._created:
                self.root.mkdir(parents=True, exist_ok=True)
                self.directory.mkdir(exist_ok=False)
                self._created = True
            for name, data in payloads:
                name = prefix + '-' + name
                digest = hashlib.sha256(data).hexdigest()
                with (self.directory / name).open('xb') as stream:
                    self._owned[name] = (len(data), digest)
                    stream.write(data); stream.flush(); os.fsync(stream.fileno())
                files.append((name, len(data), digest))
        except Exception:
            _fail(SinkErrorCode.UNAVAILABLE)
        reference = SpillReference(self.run_id, task.task_sha256, tuple(files))
        self._references.add(reference)
        return reference

    def read(self, reference: SpillReference, *, budget: ResourceBudget) -> tuple[TaskManifest, tuple[FeatureResult, ...]]:
        self._own()
        if type(reference) is not SpillReference or reference.run_id != self.run_id or reference not in self._references:
            _fail(SinkErrorCode.INVALID_CONTENT)
        data = []
        try:
            for name, size, digest in reference.files:
                if self._owned.get(name) != (size, digest) or Path(name).name != name:
                    _fail(SinkErrorCode.CORRUPTION)
                path = self.directory / name
                if path.is_symlink():
                    _fail(SinkErrorCode.CORRUPTION)
                with path.open('rb') as stream:
                    wire = stream.read(size + 1)
                if len(wire) != size or hashlib.sha256(wire).hexdigest() != digest:
                    _fail(SinkErrorCode.CORRUPTION)
                data.append(wire)
            task = decode_task(data[0]); results = tuple(decode_result(b) for b in data[1:])
            if task.task_sha256 != reference.task_sha256:
                _fail(SinkErrorCode.CORRUPTION)
            _validate_results(task, results, budget)
            return task, results
        except SinkError:
            raise
        except Exception:
            _fail(SinkErrorCode.CORRUPTION)

    def close(self, *, remove: bool = False) -> None:
        self._own()
        if type(remove) is not bool:
            _fail(SinkErrorCode.INVALID_CONFIG)
        try:
            if remove and self._created and self.directory.exists():
                # No recursive delete; preserve unexpected/unrelated entries.
                for name in self._owned:
                    path = self.directory / name
                    if path.is_symlink():
                        _fail(SinkErrorCode.CORRUPTION)
                    path.unlink(missing_ok=True)
                if not any(self.directory.iterdir()):
                    self.directory.rmdir()
            self._closed = True
        except SinkError:
            raise
        except Exception:
            _fail(SinkErrorCode.UNAVAILABLE)


@dataclass(frozen=True)
class TaskExecution:
    task: TaskManifest
    results: tuple[FeatureResult, ...] | None
    spill: SpillReference | None
    output: OutputManifest | None
    reason: str | None
    timings: tuple[StageTiming, ...] = ()


@dataclass(frozen=True)
class SupervisorOutcome:
    tasks: tuple[TaskExecution, ...]
    row_ledger: tuple[tuple[str, int], ...]
    distinct_reuse_rows: int
    admitted_input_bytes: int
    reserved_resident_bytes: int
    result_bytes: int
    mode: str
    cancelled: bool


def _reason(error: Exception) -> str:
    if isinstance(error, (SinkError, CommandError)) and type(error) in (SinkError, CommandError) and type(error.code) in (SinkErrorCode, CommandErrorCode):
        return error.code.value
    return 'CALCULATION_FAILED'


def _compute_partition(partition: Partition, budget: ResourceBudget, process_transport: bool = False,
                       telemetry: bool = False, submitted_ns: int | None = None) -> tuple[TaskExecution, ...]:
    outcomes = []
    for item in partition.items:
        timings: list[StageTiming] = []
        started = counter() if telemetry else None
        if telemetry:
            timings.append(interval(DiagnosticStage.QUEUE_WAIT, submitted_ns, started))
        try:
            try:
                results = item.calculate(item.task, item.batches)
            finally:
                if telemetry:
                    timings.append(interval(DiagnosticStage.CALCULATION, started, counter()))
            validation = counter() if telemetry else None
            try:
                _validate_results(item.task, results, budget)
            finally:
                if telemetry:
                    timings.append(interval(DiagnosticStage.SERIALIZATION, validation, counter()))
            execution = TaskExecution(item.task, results, None, None, None, tuple(timings))
            if process_transport and _size(execution) > budget.max_task_transport_bytes:
                _fail(SinkErrorCode.RESOURCE_LIMIT)
            outcomes.append(execution)
        except Exception as error:
            outcomes.append(TaskExecution(item.task, None, None, None, _reason(error), tuple(timings)))
    return tuple(outcomes)


class BoundedSupervisor:
    def __init__(self, *, budget: ResourceBudget = ResourceBudget(), mode: str = 'sequential') -> None:
        if type(budget) is not ResourceBudget or mode not in ('sequential', 'thread', 'process'):
            _fail(SinkErrorCode.INVALID_CONFIG)
        if mode == 'sequential' and budget.workers != 1:
            _fail(SinkErrorCode.INVALID_CONFIG)
        if budget.max_cpu_slots > (os.cpu_count() or 1):
            _fail(SinkErrorCode.RESOURCE_LIMIT)
        self.budget, self.mode = budget, mode
        self._owner = threading.current_thread()
        self._running = False

    def run(self, items: tuple[WorkItem, ...], *, cancellation: Cancellation | None = None,
            spill: ResultSpill | None = None, publisher: SerialPublisher | None = None,
            progress: ProgressRecorder | None = None) -> SupervisorOutcome:
        if threading.current_thread() is not self._owner or self._running:
            _fail(SinkErrorCode.INVALID_SESSION)
        if type(items) is not tuple or not items or len(items) > self.budget.max_tasks or any(type(i) is not WorkItem for i in items):
            _fail(SinkErrorCode.RESOURCE_LIMIT)
        self._running = True
        try:
            if progress is None:
                return self._run(items, cancellation=cancellation, spill=spill, publisher=publisher)
            if type(progress) is not ProgressRecorder:
                _fail(SinkErrorCode.INVALID_CONFIG)
            spans = 5 + 3 * len(items) if publisher is not None else 3
            if publisher is not None and type(publisher) is not SerialPublisher:
                _fail(SinkErrorCode.INVALID_CONFIG)
            with publisher._observation_scope() if publisher is not None else nullcontext():
                with progress.group(tuple(i.task for i in items), spans=spans) as observations:
                    outcome = self._run(items, cancellation=cancellation, spill=spill, publisher=publisher,
                                        _observations=observations)
                    for execution in outcome.tasks:
                        observation = observations[execution.task.task_sha256]
                        for timing in execution.timings:
                            observation.add(timing)
                        status = (DiagnosticStatus.VERIFIED if execution.output is not None else
                                  DiagnosticStatus.CANCELLED if execution.reason == 'CANCELLED' else
                                  DiagnosticStatus.FAILED if execution.reason is not None else DiagnosticStatus.COMPUTED)
                        observation.finish(status, fixed_reason(execution.reason) if execution.reason is not None else None)
                    return outcome
        finally:
            self._running = False

    def _run(self, items: tuple[WorkItem, ...], *, cancellation: Cancellation | None = None,
             spill: ResultSpill | None = None, publisher: SerialPublisher | None = None,
             _observations: dict[str, _Attempt] | None = None) -> SupervisorOutcome:
        if spill is not None and type(spill) is not ResultSpill or publisher is not None and type(publisher) is not SerialPublisher:
            _fail(SinkErrorCode.INVALID_CONFIG)
        budget = self.budget
        if publisher is not None:
            publisher.admit_owner()
        reused_inputs: dict[str, tuple[CanonicalBatch, ...]] = {}
        for item in items:
            key = item.task.reuse_sha256
            if key in reused_inputs and reused_inputs[key] != item.batches:
                _fail(SinkErrorCode.CONFLICT)
            reused_inputs[key] = item.batches
        if publisher is not None and publisher.pending:
            _fail(SinkErrorCode.BUSY)
        if publisher is not None and any(i.task.destination_scope != publisher.destination_scope for i in items):
            _fail(SinkErrorCode.INVALID_CONFIG)
        partitions = partition_tasks(items, budget.workers)
        sizes = [_size(item) for item in items]
        input_bytes = sum(sizes)
        result_reservation = len(items) * budget.max_task_result_bytes
        if input_bytes > budget.max_input_bytes or result_reservation > budget.max_total_result_bytes:
            _fail(SinkErrorCode.RESOURCE_LIMIT)
        # Pool results return a whole coarse shard, so reserve every shard's full result population.
        copies = 0
        if self.mode == 'process':
            # Failure records also cross IPC; reject oversized task metadata before callbacks.
            if any(max(_size(TaskExecution(i.task, None, None, None, reason,
                           (StageTiming(DiagnosticStage.QUEUE_WAIT, (1 << 63) - 1),
                            StageTiming(DiagnosticStage.CALCULATION, (1 << 63) - 1),
                            StageTiming(DiagnosticStage.SERIALIZATION, (1 << 63) - 1)) if _observations else ()))
                       for reason in tuple(code.value for code in SinkErrorCode) + tuple(code.value for code in CommandErrorCode))
                   > budget.max_task_transport_bytes for i in items):
                _fail(SinkErrorCode.RESOURCE_LIMIT)
            # Serialized + child-owned inputs; all-shard child/IPC/parent records.
            # Per-record transport bounds are enforced before child results are returned.
            copies = 2 * sum(sorted((_size(p) for p in partitions), reverse=True)[:budget.max_in_flight])
            copies += 3 * (len(items) * budget.max_task_transport_bytes + _size((None,) * len(items)))
        pending = publisher.limits.max_pending_bytes if publisher is not None else 0
        resident = input_bytes + copies + result_reservation + pending + budget.max_task_result_bytes + max(len(encode_task(i.task)) for i in items)
        if _observations:
            # Report wire reservations coexist with returned results in the coordinator.
            # Python/container overhead remains outside this logical (not hard RSS) bound.
            resident += next(iter(_observations.values())).recorder.reserved_bytes
        if resident > budget.max_resident_bytes:
            _fail(SinkErrorCode.RESOURCE_LIMIT)
        if spill is not None:
            if len(items) > spill.max_tasks - spill._tasks or sum(len(encode_task(i.task)) for i in items) + result_reservation > spill.remaining_bytes:
                _fail(SinkErrorCode.RESOURCE_LIMIT)
        token = cancellation if cancellation is not None else NeverCancelled()
        cancelled = False
        def check() -> bool:
            nonlocal cancelled
            try:
                _cancel(token)
                return True
            except CommandError as error:
                if error.code.value == 'CANCELLED':
                    cancelled = True; return False
                _fail(SinkErrorCode.INVALID_CONFIG)
        executions: dict[str, TaskExecution] = {}
        if self.mode == 'sequential':
            for item in partitions[0].items:
                if not check():
                    break
                produced_one = _compute_partition(Partition(0, (item,)), budget, telemetry=_observations is not None,
                    submitted_ns=counter() if _observations else None)[0]
                executions[item.task.task_sha256] = produced_one
        else:
            pool = ThreadPoolExecutor(max_workers=budget.workers) if self.mode == 'thread' else ProcessPoolExecutor(
                max_workers=budget.workers, mp_context=multiprocessing.get_context('spawn'))
            futures: dict[Future[tuple[TaskExecution, ...]], Partition] = {}
            iterator = iter(partitions)
            exhausted = False
            try:
                while futures or not exhausted:
                    while not exhausted and len(futures) < budget.max_in_flight and check():
                        partition = next(iterator, None)
                        if partition is None:
                            exhausted = True; break
                        futures[pool.submit(_compute_partition, partition, budget, self.mode == 'process',
                            _observations is not None, counter() if _observations else None)] = partition
                    if cancelled:
                        exhausted = True
                        for future in futures:
                            future.cancel()
                    if not futures:
                        break
                    completed, _ = wait(futures, timeout=0.05, return_when=FIRST_COMPLETED)
                    for future in completed:
                        partition = futures.pop(future)
                        if future.cancelled():
                            continue
                        try:
                            produced = future.result()
                        except Exception:
                            produced = tuple(TaskExecution(i.task, None, None, None, 'CALCULATION_FAILED') for i in partition.items)
                        executions.update((e.task.task_sha256, e) for e in produced)
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
        if _observations:
            for identity, sidecar_execution in tuple(executions.items()):
                try:
                    if (type(sidecar_execution.timings) is not tuple or len(sidecar_execution.timings) > 3
                            or any(type(t) is not StageTiming for t in sidecar_execution.timings)
                            or tuple(t.stage for t in sidecar_execution.timings) not in (
                                (DiagnosticStage.QUEUE_WAIT, DiagnosticStage.CALCULATION),
                                (DiagnosticStage.QUEUE_WAIT, DiagnosticStage.CALCULATION, DiagnosticStage.SERIALIZATION))):
                        raise ValueError
                    copied = tuple(StageTiming(t.stage, t.elapsed_ns, t.probes) for t in sidecar_execution.timings)
                except Exception:
                    copied = (StageTiming(DiagnosticStage.QUEUE_WAIT, None),
                              StageTiming(DiagnosticStage.CALCULATION, None),
                              StageTiming(DiagnosticStage.SERIALIZATION, None))
                executions[identity] = replace(sidecar_execution, timings=copied)
        # Publication is always coordinator-owned, after pure worker completion.
        total_results = 0
        output_metadata = 0
        for item in sorted(items, key=lambda i: i.task.task_sha256):
            identity = item.task.task_sha256
            execution = executions.get(identity)
            if execution is None:
                executions[identity] = TaskExecution(item.task, None, None, None, 'CANCELLED' if cancelled else 'CALCULATION_FAILED')
                continue
            if execution.results is None:
                continue
            results = execution.results
            total_results += _validate_results(item.task, results, budget)
            reference = None
            if spill is not None:
                try:
                    reference = spill.write(item.task, results, budget=budget)
                except SinkError as error:
                    executions[identity] = replace(execution, results=results, spill=None, output=None, reason=error.code.value)
                    continue
            output = None; reason = execution.reason
            if publisher is not None and check():
                try:
                    observation = _observations[identity] if _observations else None
                    if observation is not None:
                        publisher._submit(item.task, results, _observation=observation)
                        publication_progress = publisher._drain(cancellation=token, _observations=_observations)
                    else:
                        publisher.submit(item.task, results)
                        publication_progress = publisher.drain(cancellation=token)
                    for p in publication_progress:
                        prior = executions[p.task_sha256]
                        accepted = p.output; diagnosis = p.reason
                        if accepted is not None:
                            size = len(encode_output(accepted))
                            if resident + output_metadata + size > budget.max_resident_bytes:
                                accepted = None; diagnosis = SinkErrorCode.RESOURCE_LIMIT.value
                            else:
                                output_metadata += size
                        if p.task_sha256 == identity:
                            output, reason = accepted, diagnosis
                        else:
                            executions[p.task_sha256] = replace(prior, output=accepted, reason=diagnosis)
                except Exception as error:
                    reason = _reason(error)
            elif not check():
                reason = 'CANCELLED'
            executions[identity] = replace(execution, results=None if reference is not None else results, spill=reference, output=output, reason=reason)
        ordered = tuple(executions[i.task.task_sha256] for i in sorted(items, key=lambda i: i.task.task_sha256))
        reused: dict[str, int] = {}
        for item in items:
            reused.setdefault(item.task.reuse_sha256, item.rows)
        return SupervisorOutcome(ordered, tuple((i.task.task_sha256, i.rows) for i in sorted(items, key=lambda i: i.task.task_sha256)),
            sum(reused.values()), input_bytes, resident, total_results, self.mode, cancelled)
