"""Bounded local operational claims; OS ownership spans receipt-first publication."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import json
import os
from pathlib import Path
import re
import sys
import threading
from typing import Callable, Any, cast
import uuid

from equity_feature_contracts import FeatureResult
from equity_feature_io_contracts import FactoryError, ResultSink, SinkError, SinkErrorCode, SinkRequirements
from equity_feature_io_sdk import Cancellation, admit_sink, prepare_publication

from .barriers import BarrierLimits, Dependency, _admit_dependencies, _admit_entities, inspect_barrier
from .commands import CommandError, NeverCancelled, _publish_verified
from .generations import _StoreLock
from .manifests import (ClaimIdentity, OutputManifest, TaskManifest, decode_output, decode_task,
                        encode_output, encode_task, integer, _pairs)


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


@dataclass(frozen=True)
class ClaimLimits:
    max_tasks: int = 64
    max_total_bytes: int = 8388608
    max_record_bytes: int = 1048576
    max_attempts: int = 3

    def __post_init__(self) -> None:
        try:
            for value in asdict(self).values():
                integer(value, 1)
            if self.max_tasks > 4096 or self.max_attempts > 4096 or self.max_record_bytes > 16777216:
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None


@dataclass(frozen=True)
class ClaimProgress:
    identity: ClaimIdentity
    attempts: int
    output: OutputManifest | None
    results: tuple[FeatureResult, ...]
    reason: str | None

    @property
    def committed(self) -> bool:
        """Only a live verified sink readback yields output here, never a JSON flag."""
        return self.output is not None


class ClaimStore:
    """Trusted local caller root; no discovery, cleanup, scheduler or catalog pointer.

    All participating callers must retain the stable lock files and use this API.
    Network/mounted remote filesystems, adversarial root mutation and power loss are
    outside qualification. Interrupted temporary files are deliberately retained.
    """

    def __init__(self, root: Path, *, limits: ClaimLimits, requirements: SinkRequirements,
                 protected_sources: tuple[Path, ...] = ()) -> None:
        if sys.platform not in ("win32", "linux"):
            raise SinkError(SinkErrorCode.UNSUPPORTED_CAPABILITY)
        try:
            if (not isinstance(root, Path) or root.is_symlink() or type(limits) is not ClaimLimits
                or type(requirements) is not SinkRequirements or type(protected_sources) is not tuple
                or len(protected_sources) > 4096 or any(not isinstance(p, Path) for p in protected_sources)):
                raise ValueError
            resolved = root.resolve()
            if str(resolved).startswith("\\\\"):
                raise ValueError
            for source in protected_sources:
                path = source.resolve()
                if path.is_relative_to(resolved) or resolved.is_relative_to(path):
                    raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None
        self._root = resolved / ".efworker-claims1"
        self._limits, self._requirements = limits, requirements
        self._owner = threading.current_thread()

    def _admit_owner(self) -> None:
        if threading.current_thread() is not self._owner:
            raise SinkError(SinkErrorCode.INVALID_SESSION)

    def _lock(self) -> _StoreLock:
        self._admit_owner()
        try:
            if self._root.is_symlink():
                raise SinkError(SinkErrorCode.CORRUPTION)
            self._root.mkdir(parents=True, exist_ok=True)
            if (self._root / "writer.lock").is_symlink():
                raise SinkError(SinkErrorCode.CORRUPTION)
            return _StoreLock(self._root / "writer.lock")
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None

    def _inventory(self) -> tuple[set[str], int]:
        try:
            return self._scan()
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None

    def _scan(self) -> tuple[set[str], int]:
        keys: set[str] = set()
        total = 0
        # scandir is incremental: reject excess task entries rather than materialize a corpus.
        with os.scandir(self._root) as entries:
            for entry in entries:
                name = entry.name
                if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                    raise SinkError(SinkErrorCode.CORRUPTION)
                if name == "writer.lock":
                    continue
                if re.fullmatch(r"stage-[0-9a-f]{32}\.tmp", name):
                    continue
                match = re.fullmatch(r"([0-9a-f]{64})\.(json|lock)", name)
                if match is None:
                    raise SinkError(SinkErrorCode.CORRUPTION)
                keys.add(match[1])
                if len(keys) > self._limits.max_tasks:
                    raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
                if match[2] == "json":
                    size = entry.stat(follow_symlinks=False).st_size
                    if size > self._limits.max_record_bytes:
                        raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
                    total += size
                    if total > self._limits.max_total_bytes:
                        raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        return keys, total

    def _read(self, task: TaskManifest) -> dict[str, Any] | None:
        path = self._root / (task.task_sha256 + ".json")
        if not path.exists():
            return None
        try:
            if path.is_symlink():
                raise ValueError
            with path.open("rb") as stream:
                raw = stream.read(self._limits.max_record_bytes + 1)
            if len(raw) > self._limits.max_record_bytes:
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            record = json.loads(raw, object_pairs_hook=_pairs)
            if (type(record) is not dict or set(record) !=
                {"protocol", "task", "identity", "attempts", "cancelled", "state", "intent", "reason"}
                or _json(record) != raw or record["protocol"] != "efworker-claim1"
                or decode_task(record["task"].encode("ascii")) != task
                or type(record["identity"]) is not dict):
                raise ValueError
            identity = ClaimIdentity(**record["identity"])
            if identity.task_sha256 != task.task_sha256:
                raise ValueError
            integer(record["attempts"], 0)
            if record["attempts"] > 4096 or type(record["cancelled"]) is not bool:
                raise ValueError
            if record["state"] not in ("CLAIMED", "FAILED", "CANCELLED", "INTENT", "COMMITTED"):
                raise ValueError
            reason = record["reason"]
            known = {c.value for c in SinkErrorCode} | {"RETRY_EXHAUSTED"}
            if reason is not None and (type(reason) is not str or reason not in known):
                raise ValueError
            intent = record["intent"]
            if intent is not None:
                output = decode_output(intent.encode("ascii"))
                if output.task != task or record["state"] not in ("INTENT", "COMMITTED"):
                    raise ValueError
                if record["state"] == "COMMITTED" and output.receipt is None:
                    raise ValueError
            elif record["state"] in ("INTENT", "COMMITTED"):
                raise ValueError
            return cast(dict[str, Any], record)
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.CORRUPTION) from None

    def _write(self, task: TaskManifest, record: dict[str, Any]) -> None:
        try:
            self._write_record(task, record)
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None

    def _write_record(self, task: TaskManifest, record: dict[str, Any]) -> None:
        keys, total = self._inventory()
        key = task.task_sha256
        if key not in keys and len(keys) >= self._limits.max_tasks:
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        data = _json(record)
        path = self._root / (key + ".json")
        previous = path.stat().st_size if path.exists() else 0
        if len(data) > self._limits.max_record_bytes or total - previous + len(data) > self._limits.max_total_bytes:
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        temporary = self._root / ("stage-" + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("xb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None

    def acquire(self, task: TaskManifest, *, owner_id: str, attempt_id: str,
                issued_at_ns: int, expires_at_ns: int, now_ns: int,
                clear_cancel: bool = False) -> TaskClaim:
        """Hold one exact task OS lock until close/exit; live expiry never steals it."""
        self._admit_owner()
        try:
            if type(task) is not TaskManifest or type(clear_cancel) is not bool:
                raise ValueError
            identity = ClaimIdentity(task.task_sha256, owner_id, attempt_id, issued_at_ns, expires_at_ns)
            integer(now_ns)
            if not issued_at_ns <= now_ns < expires_at_ns:
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None
        lock = self._lock()
        owned: _StoreLock | None = None
        try:
            keys, _ = self._inventory()
            if task.task_sha256 not in keys and len(keys) >= self._limits.max_tasks:
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            owned = _StoreLock(self._root / (task.task_sha256 + ".lock"))
            record = self._read(task)
            if record is None:
                record = {"protocol": "efworker-claim1", "task": encode_task(task).decode("ascii"),
                          "identity": asdict(identity), "attempts": 0, "cancelled": False,
                          "state": "CLAIMED", "intent": None, "reason": None}
            else:
                record["identity"] = asdict(identity)
                if record["intent"] is None:
                    record["state"] = "CLAIMED"
            if clear_cancel:
                record["cancelled"] = False
            self._write(task, record)
            handle = TaskClaim(self, task, identity, owned)
            owned = None
            return handle
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None
        finally:
            if owned is not None:
                owned.close()
            lock.close()

    def request_cancel(self, task: TaskManifest) -> None:
        """Durable cooperative flag, independent of a currently held task lock."""
        if type(task) is not TaskManifest:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        lock = self._lock()
        try:
            self._inventory()
            record = self._read(task)
            if record is None:
                raise SinkError(SinkErrorCode.INVALID_SESSION)
            record["cancelled"] = True
            self._write(task, record)
        finally:
            lock.close()


class _ClaimCancellation:
    def __init__(self, claim: TaskClaim, token: Cancellation) -> None:
        self.claim, self.token = claim, token

    def is_cancelled(self) -> bool:
        try:
            observed = self.token.is_cancelled()
            if type(observed) is not bool:
                raise ValueError
            return observed or bool(self.claim._record()["cancelled"])
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None


class TaskClaim:
    """Creating-thread-owned context manager; caller supplies calculation and sink."""

    def __init__(self, store: ClaimStore, task: TaskManifest, identity: ClaimIdentity, lock: _StoreLock) -> None:
        self._store, self.task, self.identity, self._lock = store, task, identity, lock
        self._closed = False
        self._running = False

    def _admit(self) -> None:
        self._store._admit_owner()
        if self._closed:
            raise SinkError(SinkErrorCode.INVALID_SESSION)

    def _record(self) -> dict[str, Any]:
        self._admit()
        lock = self._store._lock()
        try:
            self._store._inventory()
            record = self._store._read(self.task)
            if record is None or record["identity"] != asdict(self.identity):
                raise SinkError(SinkErrorCode.INVALID_SESSION)
            return record
        finally:
            lock.close()

    def _update(self, **changes: Any) -> dict[str, Any]:
        self._admit()
        lock = self._store._lock()
        try:
            record = self._store._read(self.task)
            if record is None or record["identity"] != asdict(self.identity):
                raise SinkError(SinkErrorCode.INVALID_SESSION)
            record.update(changes)
            self._store._write(self.task, record)
            return record
        finally:
            lock.close()

    def _progress(self, record: dict[str, Any], reason: str | None,
                  output: OutputManifest | None = None,
                  results: tuple[FeatureResult, ...] = ()) -> ClaimProgress:
        return ClaimProgress(self.identity, record["attempts"], output, results, reason)

    def run(self, calculate: Callable[[], tuple[FeatureResult, ...]], sink: ResultSink, *, now_ns: int,
            cancellation: Cancellation | None = None) -> ClaimProgress:
        """One pass: recover actual receipt first; retry only proven absence/abort."""
        self._admit()
        if self._running:
            raise SinkError(SinkErrorCode.BUSY)
        try:
            integer(now_ns)
            if not self.identity.issued_at_ns <= now_ns < self.identity.expires_at_ns or not callable(calculate):
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None
        self._running = True
        try:
            record = self._record()
            bounds = BarrierLimits(1, self._store._limits.max_record_bytes)
            requirements = self._store._requirements
            try:
                admit_sink(sink, requirements)
            except FactoryError:
                raise SinkError(SinkErrorCode.UNSUPPORTED_CAPABILITY) from None
            intent = None if record["intent"] is None else decode_output(record["intent"].encode("ascii"))
            if intent is not None:
                barrier = inspect_barrier((Dependency("claim", self.task, intent, sink),),
                                          limits=bounds, requirements=requirements)
                if barrier.verified:
                    command = barrier.verified[0].command
                    record = self._update(state="COMMITTED", intent=encode_output(command.output).decode("ascii"), reason=None)
                    return self._progress(record, None, command.output, command.results)
                reason = barrier.waiting[0].reason
                if reason not in ("ABSENT", "ABORTED"):
                    return self._progress(record, reason)
            token = _ClaimCancellation(self, cancellation if cancellation is not None else NeverCancelled())
            if token.is_cancelled():
                record = self._update(state="INTENT" if intent is not None else "CANCELLED", reason="CANCELLED")
                return self._progress(record, "CANCELLED")
            if record["attempts"] >= self._store._limits.max_attempts:
                return self._progress(record, "RETRY_EXHAUSTED")
            record = self._update(attempts=record["attempts"] + 1, reason=None)
            retained: tuple[FeatureResult, ...] = ()
            try:
                results = calculate()
                limits = replace(requirements, visibility=None, writer_mode=None, reservation_retention_ns=1)
                envelope = prepare_publication(results, destination_scope=self.task.destination_scope,
                    generation_id=self.task.generation_id, job_id=self.task.job_id,
                    partition_id=self.task.partition_id, limits=limits)
                _admit_entities(self.task, results)
                output = OutputManifest(self.task, envelope)
                _admit_dependencies((Dependency("claim", self.task, output, sink),), bounds, requirements)
                retained = results
                if intent is not None and intent.envelope != envelope:
                    raise SinkError(SinkErrorCode.CONFLICT)
                if token.is_cancelled():
                    raise SinkError(SinkErrorCode.CANCELLED)
                record = self._update(state="INTENT", intent=encode_output(output).decode("ascii"), reason=None)
                command = _publish_verified(self.task, results, sink, requirements, token)
                record = self._update(state="COMMITTED", intent=encode_output(command.output).decode("ascii"), reason=None)
                return self._progress(record, None, command.output, command.results)
            except (SinkError, CommandError) as error:
                reason = error.code.value
                if reason not in {c.value for c in SinkErrorCode}:
                    reason = "UNAVAILABLE"
            except Exception:
                reason = "UNAVAILABLE"
            current = self._record()
            record = self._update(state="INTENT" if current["intent"] is not None else
                                 ("CANCELLED" if reason == "CANCELLED" else "FAILED"), reason=reason)
            return self._progress(record, reason, results=retained)
        finally:
            self._running = False

    def close(self) -> None:
        self._store._admit_owner()
        if self._running:
            raise SinkError(SinkErrorCode.BUSY)
        if not self._closed:
            self._lock.close()
            self._closed = True

    def __enter__(self) -> TaskClaim:
        self._admit()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
