"""Bounded, local observational sidecars. Never a publication authority."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from enum import StrEnum
import hashlib
import json
import math
import threading
import time
from typing import Iterator, cast

from equity_feature_io_contracts import SinkError, SinkErrorCode, PublicationState
from .manifests import TaskManifest, digest, integer


class DiagnosticStage(StrEnum):
    FACTORY = 'factory'
    ACQUISITION = 'acquisition'
    CALCULATION = 'calculation'
    SERIALIZATION = 'serialization'
    PUBLICATION = 'publication'
    QUEUE_WAIT = 'queue_wait'
    BACKEND_PROBE = 'backend_probe'
    QUEUE_PROBE = 'queue_probe'
    GENERATION = 'generation'
    CATALOG = 'catalog'
    DEPENDENCY = 'dependency'


class DiagnosticStatus(StrEnum):
    READY = 'ready'
    WAITING = 'waiting'
    COMPUTED = 'computed'
    VERIFIED = 'verified'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    GENERATION_COMPLETE = 'generation_complete'
    CATALOG_ACCEPTED = 'catalog_accepted'


_REASONS = frozenset(c.value for c in PublicationState) | frozenset(c.value for c in SinkErrorCode) | frozenset((
    'INVALID_COMMAND', 'SOURCE_FAILED', 'CALCULATION_FAILED', 'RESULT_IDENTITY_MISMATCH',
    'PUBLICATION_FAILED', 'READBACK_FAILED', 'CANCELLED', 'WAITING_DEPENDENCY', 'INTERRUPTED'))
_FAMILIES = ('bars', 'trades', 'quotes', 'history', 'sma_reference', 'daily_baseline',
             'interval_baseline', 'relative_volume', 'interval_relative_volume', 'relative_returns',
             'assembly', 'direction_counts', 'above_sma_fraction')
_MAX = (1 << 63) - 1
_CLOCKS = ('unavailable', 'QueryPerformanceCounter()', 'clock_gettime(CLOCK_MONOTONIC)',
           'clock_gettime(CLOCK_HIGHRES)', 'mach_absolute_time()')


def fixed_reason(value: object, fallback: str = 'UNAVAILABLE') -> str:
    """Do not stringify caller errors, labels, or objects."""
    return value if type(value) is str and value in _REASONS else fallback


def counter() -> int | None:
    try:
        value = time.perf_counter_ns()
        return value if type(value) is int and 0 <= value <= _MAX else None
    except BaseException:
        return None


def _failure_reason(error: BaseException) -> str:
    from .commands import CommandError, CommandErrorCode
    if type(error) in (KeyboardInterrupt, SystemExit):
        return 'INTERRUPTED'
    if type(error) in (SinkError, CommandError):
        code = cast(SinkError, error).code
        if type(code) in (SinkErrorCode, CommandErrorCode):
            return fixed_reason(code.value)
    return 'UNAVAILABLE'


@dataclass(frozen=True)
class StageTiming:
    stage: DiagnosticStage
    elapsed_ns: int | None
    probes: int = 0

    def __post_init__(self) -> None:
        if type(self.stage) is not DiagnosticStage:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        if self.elapsed_ns is not None:
            integer(self.elapsed_ns, 0)
        integer(self.probes, 0)

    @property
    def available(self) -> bool:
        return self.elapsed_ns is not None


def interval(stage: DiagnosticStage, start: int | None, end: int | None, *, probes: int = 0) -> StageTiming:
    valid = type(start) is int and type(end) is int and 0 <= start <= end <= _MAX
    return StageTiming(stage, end - start if valid else None, probes)  # type: ignore[operator]


@dataclass(frozen=True)
class TaskDiagnostic:
    attempt: int
    intent_sha256: str
    task_sha256: str | None
    family: str
    family_sha256: str
    partition_sha256: str
    status: DiagnosticStatus
    reason: str | None
    timings: tuple[StageTiming, ...]
    elapsed_ns: int | None

    def __post_init__(self) -> None:
        integer(self.attempt, 1)
        for value in (self.intent_sha256, self.family_sha256, self.partition_sha256):
            digest(value)
        if self.task_sha256 is not None:
            digest(self.task_sha256)
        if (type(self.family) is not str or self.family not in _FAMILIES + ('custom',) or type(self.status) is not DiagnosticStatus
                or self.reason is not None and (type(self.reason) is not str or self.reason not in _REASONS)
                or type(self.timings) is not tuple or any(type(t) is not StageTiming for t in self.timings)):
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        if self.elapsed_ns is not None:
            integer(self.elapsed_ns, 0)


@dataclass(frozen=True)
class DiagnosticSnapshot:
    tasks: tuple[TaskDiagnostic, ...]
    clock: str
    resolution_ns: int | None

    def __post_init__(self) -> None:
        if (type(self.tasks) is not tuple or any(type(t) is not TaskDiagnostic for t in self.tasks)
                or type(self.clock) is not str or self.clock not in _CLOCKS):
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        if self.resolution_ns is not None:
            integer(self.resolution_ns, 1)

    def encode(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=True, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode('ascii')


class _Attempt:
    def __init__(self, recorder: ProgressRecorder, intent: str, family: str, partition: str, spans: int) -> None:
        self.recorder, self.intent, self.capacity = recorder, intent, spans
        self.family = family if family in _FAMILIES else 'custom'
        self.family_hash = hashlib.sha256(family.encode('utf-8')).hexdigest()
        self.partition_hash = hashlib.sha256(partition.encode('utf-8')).hexdigest()
        self.task: str | None = None
        self.timings: list[StageTiming] = []
        self._reserved = 0
        self.status = DiagnosticStatus.READY
        self.reason: str | None = None
        self.start = counter()

    def bind(self, task: TaskManifest) -> None:
        self.recorder._own(active=True)
        if type(task) is not TaskManifest or self.task is not None:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        self.task = task.task_sha256

    def add(self, timing: StageTiming) -> None:
        self.recorder._own(active=True)
        if type(timing) is not StageTiming or self._reserved >= self.capacity:
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        self._reserved += 1
        self.timings.append(StageTiming(timing.stage, timing.elapsed_ns, timing.probes))

    @contextmanager
    def stage(self, stage: DiagnosticStage, *, probes: int = 0) -> Iterator[None]:
        self.recorder._own(active=True)
        if type(stage) is not DiagnosticStage or self._reserved >= self.capacity:
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        integer(probes, 0)
        self._reserved += 1
        start = counter()
        try:
            yield
        finally:
            self.timings.append(interval(stage, start, counter(), probes=probes))

    def finish(self, status: DiagnosticStatus, reason: str | None = None) -> None:
        self.recorder._own(active=True)
        if type(status) is not DiagnosticStatus:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        self.status = status
        self.reason = fixed_reason(reason) if reason is not None else None


class ProgressRecorder:
    """Creating-thread owned; reserve the entire attempt before invoking work.

    Each reserved span costs 256 ASCII bytes, each attempt 2048 bytes and the
    snapshot header 512 bytes. These conservative fixed bounds cover every
    closed field, int64 value and hash. Unused reservations are not recycled.
    """
    def __init__(self, *, max_tasks: int = 64, max_spans: int = 4096, max_bytes: int = 4194304) -> None:
        for value in (max_tasks, max_spans, max_bytes):
            integer(value, 1)
        if max_bytes < 512:
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        self.max_tasks, self.max_spans, self.max_bytes = max_tasks, max_spans, max_bytes
        self._owner = threading.current_thread()
        self._active = False
        self._tasks: list[TaskDiagnostic] = []
        self._spans, self._bytes = 0, 512
        self._clock = 'unavailable'
        self._resolution: int | None = None

    def _own(self, *, active: bool = False) -> None:
        if threading.current_thread() is not self._owner or self._active != active:
            raise SinkError(SinkErrorCode.INVALID_SESSION)

    @contextmanager
    def attempt(self, intent_sha256: str, family: str, partition: str, *, spans: int) -> Iterator[_Attempt]:
        self._own()
        digest(intent_sha256)
        integer(spans, 1)
        # Validate labels without retaining their contents in a report.
        from .manifests import label
        label(family); label(partition)
        reserved = 2048 + spans * 256
        if (len(self._tasks) >= self.max_tasks or self._spans + spans > self.max_spans
                or self._bytes + reserved > self.max_bytes):
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        self._spans += spans; self._bytes += reserved; self._active = True
        try:
            self._clock_metadata()
            observation = _Attempt(self, intent_sha256, family, partition, spans)
            try:
                yield observation
            except BaseException as error:
                reason = _failure_reason(error)
                observation.finish(DiagnosticStatus.CANCELLED if reason == 'CANCELLED' else DiagnosticStatus.FAILED, reason)
                raise
            finally:
                elapsed = interval(DiagnosticStage.QUEUE_WAIT, observation.start, counter()).elapsed_ns
                self._tasks.append(TaskDiagnostic(len(self._tasks) + 1, observation.intent, observation.task,
                    observation.family, observation.family_hash, observation.partition_hash,
                    observation.status, observation.reason, tuple(observation.timings), elapsed))
        finally:
            self._active = False

    def _clock_metadata(self) -> None:
        try:
            info = time.get_clock_info('perf_counter')
            implementation, monotonic, resolution = info.implementation, info.monotonic, info.resolution
            if (type(implementation) is not str or implementation not in _CLOCKS[1:]
                    or type(monotonic) is not bool or not monotonic
                    or type(resolution) not in (int, float) or not math.isfinite(resolution)
                    or resolution <= 0):
                self._clock, self._resolution = 'unavailable', None
                return
            resolution_ns = math.ceil(resolution * 1_000_000_000)
            if not 0 < resolution_ns <= _MAX:
                self._clock, self._resolution = 'unavailable', None
                return
            self._clock, self._resolution = implementation, resolution_ns
        except BaseException:
            self._clock, self._resolution = 'unavailable', None

    def snapshot(self) -> DiagnosticSnapshot:
        self._own()
        return DiagnosticSnapshot(tuple(self._tasks), self._clock, self._resolution)

    @property
    def reserved_bytes(self) -> int:
        """Entire retained report reservation, including the current group."""
        self._own(active=self._active)
        return self._bytes

    @contextmanager
    def group(self, tasks: tuple[TaskManifest, ...], *, spans: int, intent_sha256: str | None = None) -> Iterator[dict[str, _Attempt]]:
        """Reserve all prepared supervisor tasks before any child submission.

        Per-task elapsed covers the containing supervisor invocation, including
        scheduling and final publication. Child stage timings remain distinct.
        """
        self._own()
        integer(spans, 1)
        if (type(tasks) is not tuple or not tasks or any(type(t) is not TaskManifest for t in tasks)
                or len({t.task_sha256 for t in tasks}) != len(tasks)):
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        if intent_sha256 is not None:
            digest(intent_sha256)
        count = len(tasks)
        reserved = count * (2048 + spans * 256)
        if (len(self._tasks) + count > self.max_tasks or self._spans + count * spans > self.max_spans
                or self._bytes + reserved > self.max_bytes):
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        self._spans += count * spans; self._bytes += reserved; self._active = True
        observations: dict[str, _Attempt] = {}
        try:
            self._clock_metadata()
            for task in tasks:
                observation = _Attempt(self, intent_sha256 or task.task_sha256, task.family, task.partition_id, spans)
                observation.bind(task)
                observations[task.task_sha256] = observation
            try:
                yield observations
            except BaseException as error:
                for observation in observations.values():
                    if observation.status is not DiagnosticStatus.VERIFIED:
                        observation.finish(DiagnosticStatus.CANCELLED if _failure_reason(error) == 'CANCELLED' else DiagnosticStatus.FAILED, _failure_reason(error))
                raise
        finally:
            for observation in observations.values():
                elapsed = interval(DiagnosticStage.QUEUE_WAIT, observation.start, counter()).elapsed_ns
                self._tasks.append(TaskDiagnostic(len(self._tasks) + 1, observation.intent, observation.task,
                    observation.family, observation.family_hash, observation.partition_hash,
                    observation.status, observation.reason, tuple(observation.timings), elapsed))
            self._active = False
