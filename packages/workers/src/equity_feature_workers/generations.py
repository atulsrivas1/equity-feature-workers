"""Private local staging and immutable completion-last operational generations."""
from __future__ import annotations

from dataclasses import dataclass
import errno
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import BinaryIO
import uuid

from equity_feature_io_contracts import SinkError, SinkErrorCode, SinkRequirements
from equity_feature_io_sdk import Cancellation

from .barriers import BarrierLimits, BarrierOutcome, Dependency, _admit_dependencies, inspect_barrier
from .diagnostics import ProgressRecorder, DiagnosticStage, DiagnosticStatus, counter, interval
from .commands import CommandError, CommandErrorCode, NeverCancelled, _cancel
from .manifests import ManifestError, ManifestErrorCode, OutputManifest, TaskManifest, decode_output, encode_output, label, _pairs


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def _check_cancel(token: Cancellation) -> None:
    try:
        _cancel(token)
    except CommandError as error:
        code = SinkErrorCode.CANCELLED if error.code is CommandErrorCode.CANCELLED else SinkErrorCode.INVALID_CONFIG
        raise SinkError(code) from None


@dataclass(frozen=True)
class GenerationSpec:
    namespace: str
    job_id: str
    generation_id: str
    tasks: tuple[TaskManifest, ...]

    def __post_init__(self) -> None:
        try:
            for value in (self.namespace, self.job_id, self.generation_id):
                label(value)
            if (type(self.tasks) is not tuple or not 1 <= len(self.tasks) <= 4096
                or any(type(t) is not TaskManifest for t in self.tasks)
                or len({t.task_sha256 for t in self.tasks}) != len(self.tasks)
                or any((t.config.session.namespace, t.job_id, t.generation_id) !=
                       (self.namespace, self.job_id, self.generation_id) for t in self.tasks)):
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None

    @property
    def key(self) -> str:
        return hashlib.sha256(b"efworker-generation-key1\0" + _json(
            [self.namespace, self.job_id, self.generation_id])).hexdigest()

    @property
    def identity(self) -> str:
        return hashlib.sha256(b"efworker-generation1\0" + _json(
            [self.key, [t.task_sha256 for t in self.tasks]])).hexdigest()


@dataclass(frozen=True)
class GenerationOutcome:
    complete: bool
    barrier: BarrierOutcome | None
    outputs: tuple[OutputManifest, ...]


class _StoreLock:
    """Cooperating local writers keep one stable inode; never delete a lock file."""

    def __init__(self, path: Path) -> None:
        self.file: BinaryIO | None = None
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\0")
            os.lseek(fd, 0, os.SEEK_SET)
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.file = os.fdopen(fd, "r+b", buffering=0)
        except OSError as error:
            os.close(fd)
            code = SinkErrorCode.BUSY if error.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK) else SinkErrorCode.UNAVAILABLE
            raise SinkError(code) from None
        except Exception:
            os.close(fd)
            raise

    def close(self) -> None:
        if self.file is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
        finally:
            self.file.close()
            self.file = None


class GenerationStore:
    """Caller-owned trusted local metadata root, distinct from result storage/catalog.

    No source discovery, backend layout, destructive cleanup or accepted pointer.
    Stale private stage files are retained; only immutable complete records are read.
    """

    def __init__(self, root: Path, *, limits: BarrierLimits, requirements: SinkRequirements,
                 protected_sources: tuple[Path, ...] = ()) -> None:
        if sys.platform not in ("win32", "linux"):
            raise SinkError(SinkErrorCode.UNSUPPORTED_CAPABILITY)
        try:
            if (not isinstance(root, Path) or type(limits) is not BarrierLimits
                or type(requirements) is not SinkRequirements or type(protected_sources) is not tuple
                or len(protected_sources) > 4096 or any(not isinstance(p, Path) for p in protected_sources)):
                raise ValueError
            root = root.resolve()
            if str(root).startswith("\\\\"):
                raise ValueError
            for source in protected_sources:
                path = source.resolve()
                if path.is_relative_to(root) or root.is_relative_to(path):
                    raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None
        self._root, self._limits, self._requirements = root / ".efworker-generations1", limits, requirements

    def _dependencies(self, spec: GenerationSpec, dependencies: tuple[Dependency, ...]) -> tuple[Dependency, ...]:
        if type(spec) is not GenerationSpec:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        _admit_dependencies(dependencies, self._limits, self._requirements)
        by_id = {d.task.task_sha256: d for d in dependencies}
        if (set(by_id) != {t.task_sha256 for t in spec.tasks} or any(not d.required for d in dependencies)):
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        return tuple(by_id[t.task_sha256] for t in spec.tasks)

    def _record(self, spec: GenerationSpec, outputs: tuple[OutputManifest, ...]) -> bytes:
        try:
            data = _json({"protocol": "efworker-generation1", "namespace": spec.namespace, "job_id": spec.job_id,
                          "generation_id": spec.generation_id, "identity": spec.identity,
                          "outputs": [encode_output(o).decode("ascii") for o in outputs]})
        except ManifestError as error:
            code = SinkErrorCode.RESOURCE_LIMIT if error.code is ManifestErrorCode.LIMIT else SinkErrorCode.CORRUPTION
            raise SinkError(code) from None
        if len(data) > self._limits.max_bytes:
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        return data

    def _path(self, spec: GenerationSpec) -> Path:
        return self._root / (spec.key + ".complete.json")

    def _load(self, spec: GenerationSpec) -> tuple[OutputManifest, ...] | None:
        path = self._path(spec)
        try:
            if path.is_symlink() or self._root.is_symlink():
                raise SinkError(SinkErrorCode.CORRUPTION)
            with path.open("rb") as stream:
                data = stream.read(self._limits.max_bytes + 1)
            if len(data) > self._limits.max_bytes:
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            value = json.loads(data.decode("ascii"), object_pairs_hook=_pairs)
            if (type(value) is not dict or set(value) != {"protocol", "namespace", "job_id", "generation_id", "identity", "outputs"}
                or value["protocol"] != "efworker-generation1" or type(value["outputs"]) is not list
                or not 1 <= len(value["outputs"]) <= self._limits.max_dependencies
                or any(type(v) is not str for v in value["outputs"])):
                raise ValueError
            outputs = tuple(decode_output(v.encode("ascii")) for v in value["outputs"])
            if any(o.receipt is None for o in outputs):
                raise ValueError
            stored = GenerationSpec(value["namespace"], value["job_id"], value["generation_id"], tuple(o.task for o in outputs))
            if data != self._record(stored, outputs):
                raise ValueError
            if stored != spec:
                raise SinkError(SinkErrorCode.CONFLICT)
            return outputs
        except FileNotFoundError:
            return None
        except SinkError as error:
            code = error.code if error.code in (SinkErrorCode.RESOURCE_LIMIT, SinkErrorCode.CONFLICT) else SinkErrorCode.CORRUPTION
            raise SinkError(code) from None
        except Exception:
            raise SinkError(SinkErrorCode.CORRUPTION) from None

    def read(self, spec: GenerationSpec, dependencies: tuple[Dependency, ...], *,
             cancellation: Cancellation | None = None) -> GenerationOutcome:
        ordered = self._dependencies(spec, dependencies)
        _check_cancel(cancellation if cancellation is not None else NeverCancelled())
        stored = self._load(spec)
        if stored is None:
            return GenerationOutcome(False, None, ())
        barrier = inspect_barrier(ordered, limits=self._limits, requirements=self._requirements, cancellation=cancellation)
        if not barrier.ready:
            return GenerationOutcome(False, barrier, ())
        outputs = tuple(v.command.output for v in barrier.verified)
        if outputs != stored:
            raise SinkError(SinkErrorCode.CONFLICT)
        return GenerationOutcome(True, barrier, outputs)

    def publish(self, spec: GenerationSpec, dependencies: tuple[Dependency, ...], *,
                cancellation: Cancellation | None = None, progress: ProgressRecorder | None = None) -> GenerationOutcome:
        if progress is not None:
            if type(progress) is not ProgressRecorder or type(spec) is not GenerationSpec:
                raise SinkError(SinkErrorCode.INVALID_CONFIG)
            with progress.group(spec.tasks, spans=1, intent_sha256=spec.identity) as observations:
                start = counter()
                try:
                    outcome = self.publish(spec, dependencies, cancellation=cancellation)
                finally:
                    timing = interval(DiagnosticStage.GENERATION, start, counter())
                    for observation in observations.values():
                        observation.add(timing)
                for observation in observations.values():
                    observation.finish(DiagnosticStatus.GENERATION_COMPLETE if outcome.complete else DiagnosticStatus.WAITING)
                return outcome
        ordered = self._dependencies(spec, dependencies)
        token = cancellation if cancellation is not None else NeverCancelled()
        _check_cancel(token)
        try:
            if self._root.is_symlink():
                raise SinkError(SinkErrorCode.INVALID_CONFIG)
            self._root.mkdir(parents=True, exist_ok=True)
            if (self._root / "writer.lock").is_symlink():
                raise SinkError(SinkErrorCode.INVALID_CONFIG)
            lock = _StoreLock(self._root / "writer.lock")
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None
        try:
            existing = self._load(spec)
            barrier = inspect_barrier(ordered, limits=self._limits, requirements=self._requirements, cancellation=token)
            if not barrier.ready:
                return GenerationOutcome(False, barrier, ())
            outputs = tuple(v.command.output for v in barrier.verified)
            data = self._record(spec, outputs)
            if existing is not None:
                if outputs != existing:
                    raise SinkError(SinkErrorCode.CONFLICT)
                return GenerationOutcome(True, barrier, outputs)
            stage = self._root / ("stage-" + uuid.uuid4().hex + ".json")
            with stage.open("xb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            _check_cancel(token)
            os.replace(stage, self._path(spec))
            if self._load(spec) != outputs:
                raise SinkError(SinkErrorCode.CORRUPTION)
            return GenerationOutcome(True, barrier, outputs)
        except (SinkError, CommandError):
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None
        finally:
            try:
                lock.close()
            except Exception:
                raise SinkError(SinkErrorCode.UNAVAILABLE) from None
