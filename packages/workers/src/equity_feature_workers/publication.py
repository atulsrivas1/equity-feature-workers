"""Bounded serialized publication through the accepted SDK and caller-owned sinks."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
import threading
from typing import Iterator

from equity_feature_contracts import FeatureResult
from equity_feature_io_contracts import PublicationState, ResultSink, SinkError, SinkErrorCode, SinkRequirements
from equity_feature_io_sdk import Cancellation, admit_sink, prepare_publication, publish

from .barriers import BarrierLimits, Dependency, _admit_dependencies, _admit_entities, inspect_barrier
from .commands import CommandError, NeverCancelled
from .diagnostics import (ProgressRecorder, DiagnosticStage, DiagnosticStatus, _Attempt,
                          counter, interval, fixed_reason)
from .manifests import OutputManifest, TaskManifest, integer, label


@dataclass(frozen=True)
class PublicationLimits:
    max_pending_tasks: int = 64
    max_pending_bytes: int = 8388608

    def __post_init__(self) -> None:
        try:
            integer(self.max_pending_tasks, 1); integer(self.max_pending_bytes, 1)
            if self.max_pending_tasks > 4096:
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None


@dataclass(frozen=True)
class PublicationProgress:
    task_sha256: str
    output: OutputManifest | None
    reason: str | None

    @property
    def committed(self) -> bool:
        return self.output is not None


@dataclass(frozen=True)
class _Pending:
    declaration: OutputManifest
    results: tuple[FeatureResult, ...]
    size: int
    queued_ns: int | None = None


class SerialPublisher:
    """One owner thread drains a bounded queue; producers receive explicit backpressure.

    The caller retains sink lifetime. No other code may share that sink instance.
    Unresolved entries remain pending; returned committed outputs remain caller-owned.
    """

    def __init__(self, sink: ResultSink, destination_scope: str, *, limits: PublicationLimits,
                 requirements: SinkRequirements) -> None:
        try:
            label(destination_scope)
            if type(limits) is not PublicationLimits or type(requirements) is not SinkRequirements:
                raise ValueError
            admit_sink(sink, requirements)
            capability = sink.capabilities()
            if capability.writer_mode != "serialized_writer" or capability.visibility not in ("manifest_last", "transactional"):
                raise SinkError(SinkErrorCode.UNSUPPORTED_CAPABILITY)
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None
        self._sink, self._scope = sink, destination_scope
        self._limits, self._requirements = limits, requirements
        self._owner = threading.current_thread()
        self._lock = threading.Lock()
        self._pending: dict[str, _Pending] = {}
        self._observing = False

    @property
    def limits(self) -> PublicationLimits:
        return self._limits

    @property
    def destination_scope(self) -> str:
        return self._scope

    def admit_owner(self) -> None:
        """Check coordinator ownership without calculation, queue mutation or I/O."""
        if threading.current_thread() is not self._owner:
            raise SinkError(SinkErrorCode.INVALID_SESSION)

    def _bounds(self, count: int | None = None, size: int | None = None) -> BarrierLimits:
        return BarrierLimits(self._limits.max_pending_tasks if count is None else count,
                             self._limits.max_pending_bytes if size is None else size)

    def _dependency(self, entry: _Pending, output: OutputManifest | None = None) -> Dependency:
        value = entry.declaration if output is None else output
        return Dependency(value.task.task_sha256, value.task, value, self._sink)

    def _admit_operation(self) -> None:
        # Default foreign producers remain supported. The creating owner cannot
        # reenter through a telemetry hook and consume somebody else's output.
        if threading.current_thread() is self._owner and self._observing:
            raise SinkError(SinkErrorCode.BUSY)

    @contextmanager
    def _observation_scope(self) -> Iterator[None]:
        self.admit_owner()
        self._admit_operation()
        self._observing = True
        try:
            yield
        finally:
            self._observing = False

    def submit(self, task: TaskManifest, results: tuple[FeatureResult, ...], *,
               progress: ProgressRecorder | None = None, _observation: _Attempt | None = None) -> str:
        self._admit_operation()
        if progress is None and _observation is None:
            return self._submit(task, results)
        self.admit_owner()
        if (progress is not None and (type(progress) is not ProgressRecorder or _observation is not None)
                or _observation is not None and type(_observation) is not _Attempt
                or type(task) is not TaskManifest or task.destination_scope != self._scope):
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        self._observing = True
        try:
            if progress is not None:
                with progress.attempt(task.task_sha256, task.family, task.partition_id, spans=2) as observation:
                    observation.bind(task)
                    key = self._submit(task, results, _observation=observation)
                    observation.finish(DiagnosticStatus.WAITING)
                    return key
            assert _observation is not None
            _observation.recorder._own(active=True)
            return self._submit(task, results, _observation=_observation)
        finally:
            self._observing = False

    def _submit(self, task: TaskManifest, results: tuple[FeatureResult, ...], *,
                _observation: _Attempt | None = None) -> str:
        if type(task) is not TaskManifest or task.destination_scope != self._scope:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        requirements = replace(self._requirements, visibility=None, writer_mode=None, reservation_retention_ns=1)
        with _observation.stage(DiagnosticStage.SERIALIZATION) if _observation else nullcontext():
            envelope = prepare_publication(results, destination_scope=self._scope, generation_id=task.generation_id,
                                           job_id=task.job_id, partition_id=task.partition_id, limits=requirements)
            _admit_entities(task, results)
            declaration = OutputManifest(task, envelope)
            provisional = _Pending(declaration, results, 0)
            size = _admit_dependencies((self._dependency(provisional),), self._bounds(), self._requirements)
            entry = _Pending(declaration, results, size, counter() if _observation else None)
        with _observation.stage(DiagnosticStage.QUEUE_PROBE, probes=1) if _observation else nullcontext():
            acquired = self._lock.acquire(blocking=False)
        if not acquired:
            raise SinkError(SinkErrorCode.BUSY)
        try:
            key = task.task_sha256
            existing = self._pending.get(key)
            if existing is not None:
                if existing.declaration != declaration:
                    raise SinkError(SinkErrorCode.CONFLICT)
                return key
            if (len(self._pending) >= self._limits.max_pending_tasks
                or sum(p.size for p in self._pending.values()) + size > self._limits.max_pending_bytes):
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            self._pending[key] = entry
            return key
        finally:
            self._lock.release()

    @property
    def pending(self) -> tuple[str, ...]:
        if not self._lock.acquire(blocking=False):
            raise SinkError(SinkErrorCode.BUSY)
        try:
            return tuple(self._pending)
        finally:
            self._lock.release()

    def _one(self, entry: _Pending, available: int, token: Cancellation) -> PublicationProgress:
        dependency = self._dependency(entry)
        barrier = inspect_barrier((dependency,), limits=self._bounds(1, available),
                                  requirements=self._requirements, cancellation=token)
        if barrier.verified:
            return PublicationProgress(dependency.task.task_sha256, barrier.verified[0].command.output, None)
        reason = barrier.waiting[0].reason
        if reason not in (PublicationState.ABSENT.value, PublicationState.ABORTED.value):
            return PublicationProgress(dependency.task.task_sha256, None, reason)
        # Accepted SDK performs conservative lookup/abort handling on uncertain commits.
        receipt = publish(self._sink, entry.declaration.envelope, entry.results,
                          requirements=self._requirements, cancellation=token)
        declared = OutputManifest(entry.declaration.task, entry.declaration.envelope, receipt)
        barrier = inspect_barrier((self._dependency(entry, declared),), limits=self._bounds(1, available),
                                  requirements=self._requirements, cancellation=token)
        if barrier.verified:
            return PublicationProgress(dependency.task.task_sha256, barrier.verified[0].command.output, None)
        return PublicationProgress(dependency.task.task_sha256, None, barrier.waiting[0].reason)

    def drain(self, *, cancellation: Cancellation | None = None, progress: ProgressRecorder | None = None,
              _observations: dict[str, _Attempt] | None = None) -> tuple[PublicationProgress, ...]:
        """One bounded pass; telemetry cannot reenter through default operations."""
        self.admit_owner()
        self._admit_operation()
        if progress is None and _observations is None:
            return self._drain(cancellation=cancellation)
        if progress is not None and (type(progress) is not ProgressRecorder or _observations is not None):
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        self._observing = True
        try:
            if progress is not None:
                progress._own()
                if not self._lock.acquire(blocking=False):
                    raise SinkError(SinkErrorCode.BUSY)
                try:
                    tasks = tuple(p.declaration.task for p in self._pending.values())
                    if not tasks:
                        return ()
                    with progress.group(tasks, spans=3) as observations:
                        return self._drain(cancellation=cancellation, _observations=observations, _locked=True)
                finally:
                    self._lock.release()
            return self._drain(cancellation=cancellation, _observations=_observations)
        finally:
            self._observing = False

    def _drain(self, *, cancellation: Cancellation | None = None,
               _observations: dict[str, _Attempt] | None = None,
               _locked: bool = False) -> tuple[PublicationProgress, ...]:
        if not _locked and not self._lock.acquire(blocking=False):
            raise SinkError(SinkErrorCode.BUSY)
        token = cancellation if cancellation is not None else NeverCancelled()
        try:
            used = sum(p.size for p in self._pending.values())
            outcomes: list[PublicationProgress] = []
            completed: list[str] = []
            for key, entry in self._pending.items():
                # A supervised observed pass can spend only its reserved task
                # population. Foreign producers retain their entries for an
                # explicit caller drain; never consume unreserved work here.
                if _observations is not None and key not in _observations:
                    continue
                available = self._limits.max_pending_bytes - used + entry.size
                observation = _observations[key] if _observations else None
                started = counter() if observation else None
                if observation:
                    observation.add(interval(DiagnosticStage.QUEUE_WAIT, entry.queued_ns, started))
                try:
                    outcome = self._one(entry, available, token)
                except SinkError as error:
                    outcome = PublicationProgress(key, None, error.code.value)
                except CommandError as error:
                    outcome = PublicationProgress(key, None, error.code.value)
                except Exception:
                    outcome = PublicationProgress(key, None, SinkErrorCode.UNAVAILABLE.value)
                if observation:
                    timing = interval(DiagnosticStage.PUBLICATION, started, counter())
                    observation.add(timing)
                    if outcome.reason == SinkErrorCode.BUSY.value:
                        # The actual backend/barrier call returned BUSY. This combined
                        # probe includes lookup/conversion; it is not OS lock wait.
                        observation.add(interval(DiagnosticStage.BACKEND_PROBE, 0, timing.elapsed_ns, probes=1))
                    observation.finish(DiagnosticStatus.VERIFIED if outcome.output is not None else
                        DiagnosticStatus.CANCELLED if outcome.reason == 'CANCELLED' else DiagnosticStatus.WAITING,
                        fixed_reason(outcome.reason) if outcome.reason is not None else None)
                if outcome.output is not None:
                    size = _admit_dependencies((self._dependency(entry, outcome.output),),
                                              self._bounds(1, available), self._requirements)
                    used += size - entry.size
                    completed.append(key)
                outcomes.append(outcome)
            for key in completed:
                del self._pending[key]
            return tuple(outcomes)
        finally:
            if not _locked:
                self._lock.release()
