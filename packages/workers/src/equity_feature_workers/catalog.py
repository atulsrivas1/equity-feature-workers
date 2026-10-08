"""Serialized local catalog acceptance; immutable snapshots require live verification."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import uuid

from equity_feature_io_contracts import SinkError, SinkErrorCode, SinkRequirements
from equity_feature_io_sdk import Cancellation

from .diagnostics import ProgressRecorder, DiagnosticStage, DiagnosticStatus, counter, interval
from .barriers import Dependency
from .commands import CommandError, CommandErrorCode, NeverCancelled
from .generations import GenerationSpec, GenerationStore, GenerationOutcome, _StoreLock
from .manifests import OutputManifest, decode_output, encode_output, integer, label, _pairs


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")



class _CheckedCancellation:
    def __init__(self, original: Cancellation) -> None:
        self.original = original

    def is_cancelled(self) -> bool:
        try:
            value = self.original.is_cancelled()
            if type(value) is not bool:
                raise ValueError
            return value
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None


def _check_cancel(token: Cancellation) -> None:
    if token.is_cancelled():
        raise SinkError(SinkErrorCode.CANCELLED)


def _close(lock: _StoreLock) -> None:
    try:
        lock.close()
    except Exception:
        raise SinkError(SinkErrorCode.UNAVAILABLE) from None


def _key(namespace: str, job_id: str) -> str:
    try:
        label(namespace); label(job_id)
        return hashlib.sha256(b"efworker-catalog-key1\0" + _json([namespace, job_id])).hexdigest()
    except Exception:
        raise SinkError(SinkErrorCode.INVALID_CONFIG) from None


@dataclass(frozen=True)
class CatalogLimits:
    max_catalogs: int = 64
    max_history: int = 64
    max_total_bytes: int = 8388608
    max_record_bytes: int = 4194304

    def __post_init__(self) -> None:
        try:
            for value in asdict(self).values():
                integer(value, 1)
            if self.max_catalogs > 4096 or self.max_history > 4096 or self.max_record_bytes > 16777216:
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None


@dataclass(frozen=True)
class CatalogEntry:
    spec: GenerationSpec
    outputs: tuple[OutputManifest, ...]

    def __post_init__(self) -> None:
        try:
            if (type(self.spec) is not GenerationSpec or type(self.outputs) is not tuple
                or len(self.outputs) != len(self.spec.tasks)
                or any(type(o) is not OutputManifest or o.receipt is None for o in self.outputs)
                or tuple(o.task for o in self.outputs) != self.spec.tasks):
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None


@dataclass(frozen=True)
class CatalogSnapshot:
    namespace: str
    job_id: str
    history: tuple[CatalogEntry, ...]

    def __post_init__(self) -> None:
        _key(self.namespace, self.job_id)
        try:
            if (type(self.history) is not tuple or not 1 <= len(self.history) <= 4096
                or any(type(e) is not CatalogEntry or (e.spec.namespace, e.spec.job_id) !=
                       (self.namespace, self.job_id) for e in self.history)
                or len({e.spec.generation_id for e in self.history}) != len(self.history)):
                raise ValueError
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None

    @property
    def sequence(self) -> int:
        return len(self.history)

    @property
    def selected(self) -> CatalogEntry:
        return self.history[-1]

    @property
    def identity(self) -> str:
        return hashlib.sha256(b"efworker-catalog-snapshot1\0" + _payload(self, 16777216)).hexdigest()


def _payload(snapshot: CatalogSnapshot, limit: int) -> bytes:
    history: list[dict[str, object]] = []
    used = 0
    for entry in snapshot.history:
        outputs: list[str] = []
        for output in entry.outputs:
            wire = encode_output(output).decode("ascii")
            used += len(_json(wire)) + 1
            if used > limit:
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            outputs.append(wire)
        history.append({"generation_id": entry.spec.generation_id,
                        "generation_identity": entry.spec.identity, "outputs": outputs})
    data = _json({"protocol": "efworker-catalog1", "namespace": snapshot.namespace,
                  "job_id": snapshot.job_id, "sequence": snapshot.sequence, "history": history})
    if len(data) > limit:
        raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
    return data


@dataclass(frozen=True)
class CatalogOutcome:
    snapshot: CatalogSnapshot | None
    generation: GenerationOutcome

    @property
    def accepted(self) -> bool:
        return self.snapshot is not None and self.generation.complete


class CatalogStore:
    """Cooperating local metadata only; no sink writes, discovery or destructive cleanup.

    Snapshot reads never create files. Retained old snapshots require recorded lineage
    and live generation verification before their outputs can be consumed as accepted.
    """

    def __init__(self, root: Path, *, limits: CatalogLimits, requirements: SinkRequirements,
                 protected_sources: tuple[Path, ...] = ()) -> None:
        if sys.platform not in ("win32", "linux"):
            raise SinkError(SinkErrorCode.UNSUPPORTED_CAPABILITY)
        try:
            if (not isinstance(root, Path) or root.is_symlink() or type(limits) is not CatalogLimits
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
        self._root = resolved / ".efworker-catalog1"
        self._limits, self._requirements = limits, requirements
        self._owner = threading.current_thread()
        self._entered = False
        self._observing = False

    def _inventory(self) -> tuple[int, int]:
        count = total = 0
        try:
            if self._root.is_symlink():
                raise SinkError(SinkErrorCode.CORRUPTION)
            with os.scandir(self._root) as entries:
                for entry in entries:
                    if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                        raise SinkError(SinkErrorCode.CORRUPTION)
                    if re.fullmatch(r"[0-9a-f]{64}\.catalog\.json", entry.name):
                        size = entry.stat(follow_symlinks=False).st_size
                        count += 1; total += size
                        if size > self._limits.max_record_bytes:
                            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
                    elif entry.name != "writer.lock" and not re.fullmatch(r"stage-[0-9a-f]{32}\.tmp", entry.name):
                        raise SinkError(SinkErrorCode.CORRUPTION)
                    if count > self._limits.max_catalogs or total > self._limits.max_total_bytes:
                        raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            return count, total
        except FileNotFoundError:
            return 0, 0
        except SinkError:
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None

    def _path(self, namespace: str, job_id: str) -> Path:
        return self._root / (_key(namespace, job_id) + ".catalog.json")

    def _load(self, namespace: str, job_id: str) -> CatalogSnapshot | None:
        path = self._path(namespace, job_id)
        try:
            if self._root.is_symlink() or path.is_symlink():
                raise SinkError(SinkErrorCode.CORRUPTION)
            with path.open("rb") as stream:
                data = stream.read(self._limits.max_record_bytes + 1)
            if len(data) > self._limits.max_record_bytes:
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            value = json.loads(data.decode("ascii"), object_pairs_hook=_pairs)
            if (type(value) is not dict or set(value) != {"protocol", "namespace", "job_id", "sequence", "history"}
                or value["protocol"] != "efworker-catalog1" or value["namespace"] != namespace
                or value["job_id"] != job_id or type(value["history"]) is not list
                or not 1 <= len(value["history"]) <= self._limits.max_history
                or type(value["sequence"]) is not int or value["sequence"] != len(value["history"])):
                raise ValueError
            history = []
            for entry in value["history"]:
                if (type(entry) is not dict or set(entry) != {"generation_id", "generation_identity", "outputs"}
                    or type(entry["outputs"]) is not list or not 1 <= len(entry["outputs"]) <= 4096
                    or any(type(v) is not str for v in entry["outputs"])):
                    raise ValueError
                outputs = tuple(decode_output(v.encode("ascii")) for v in entry["outputs"])
                spec = GenerationSpec(namespace, job_id, entry["generation_id"], tuple(o.task for o in outputs))
                if entry["generation_identity"] != spec.identity:
                    raise ValueError
                history.append(CatalogEntry(spec, outputs))
            snapshot = CatalogSnapshot(namespace, job_id, tuple(history))
            if data != _payload(snapshot, self._limits.max_record_bytes):
                raise ValueError
            return snapshot
        except FileNotFoundError:
            return None
        except SinkError as error:
            if error.code is SinkErrorCode.RESOURCE_LIMIT:
                raise
            raise SinkError(SinkErrorCode.CORRUPTION) from None
        except OSError:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None
        except Exception:
            raise SinkError(SinkErrorCode.CORRUPTION) from None

    def snapshot(self, namespace: str, job_id: str) -> CatalogSnapshot | None:
        _key(namespace, job_id)
        self._inventory()
        return self._load(namespace, job_id)

    def _admit_generation(self, spec: GenerationSpec, generations: GenerationStore,
                          dependencies: tuple[Dependency, ...]) -> None:
        try:
            if (type(generations) is not GenerationStore or generations._requirements != self._requirements
                or generations._root.is_relative_to(self._root) or self._root.is_relative_to(generations._root)):
                raise SinkError(SinkErrorCode.INVALID_CONFIG)
            generations._dependencies(spec, dependencies)
        except SinkError:
            raise
        except CommandError as error:
            code = SinkErrorCode.RESOURCE_LIMIT if error.code is CommandErrorCode.LIMIT else SinkErrorCode.INVALID_CONFIG
            raise SinkError(code) from None
        except Exception:
            raise SinkError(SinkErrorCode.INVALID_CONFIG) from None

    def _generation(self, spec: GenerationSpec, generations: GenerationStore,
                    dependencies: tuple[Dependency, ...], token: Cancellation) -> GenerationOutcome:
        self._admit_generation(spec, generations, dependencies)
        try:
            return generations.read(spec, dependencies, cancellation=token)
        except SinkError:
            raise
        except CommandError as error:
            code = (SinkErrorCode.RESOURCE_LIMIT if error.code is CommandErrorCode.LIMIT else
                    SinkErrorCode.CANCELLED if error.code is CommandErrorCode.CANCELLED else SinkErrorCode.INVALID_CONFIG)
            raise SinkError(code) from None
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None

    def verify(self, snapshot: CatalogSnapshot, generations: GenerationStore,
               dependencies: tuple[Dependency, ...], *, cancellation: Cancellation | None = None) -> CatalogOutcome:
        if type(snapshot) is not CatalogSnapshot:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        token = _CheckedCancellation(cancellation if cancellation is not None else NeverCancelled())
        _check_cancel(token)
        _payload(snapshot, self._limits.max_record_bytes)
        current = self.snapshot(snapshot.namespace, snapshot.job_id)
        if current is None or current.history[:snapshot.sequence] != snapshot.history:
            raise SinkError(SinkErrorCode.CONFLICT)
        generation = self._generation(snapshot.selected.spec, generations, dependencies, token)
        if generation.complete and generation.outputs != snapshot.selected.outputs:
            raise SinkError(SinkErrorCode.CONFLICT)
        return CatalogOutcome(snapshot if generation.complete else None, generation)

    def select(self, spec: GenerationSpec, generations: GenerationStore,
               dependencies: tuple[Dependency, ...], *, expected_sequence: int,
               cancellation: Cancellation | None = None, progress: ProgressRecorder | None = None) -> CatalogOutcome:
        if threading.current_thread() is not self._owner:
            raise SinkError(SinkErrorCode.INVALID_SESSION)
        if self._entered or self._observing:
            raise SinkError(SinkErrorCode.BUSY)
        if progress is None:
            return self._select(spec, generations, dependencies, expected_sequence=expected_sequence,
                                cancellation=cancellation)
        self._observing = True
        try:
            if type(progress) is not ProgressRecorder or type(spec) is not GenerationSpec:
                raise SinkError(SinkErrorCode.INVALID_CONFIG)
            integer(expected_sequence, 0)
            intent = hashlib.sha256(b'efworker-catalog-attempt1\0' + _json([spec.identity, expected_sequence])).hexdigest()
            with progress.group(spec.tasks, spans=1, intent_sha256=intent) as observations:
                start = counter()
                try:
                    outcome = self._select(spec, generations, dependencies, expected_sequence=expected_sequence,
                                          cancellation=cancellation)
                finally:
                    timing = interval(DiagnosticStage.CATALOG, start, counter())
                    for observation in observations.values():
                        observation.add(timing)
                for observation in observations.values():
                    observation.finish(DiagnosticStatus.CATALOG_ACCEPTED if outcome.accepted else DiagnosticStatus.WAITING)
                return outcome
        finally:
            self._observing = False

    def _select(self, spec: GenerationSpec, generations: GenerationStore,
                dependencies: tuple[Dependency, ...], *, expected_sequence: int,
                cancellation: Cancellation | None = None) -> CatalogOutcome:
        if threading.current_thread() is not self._owner:
            raise SinkError(SinkErrorCode.INVALID_SESSION)
        if self._entered:
            raise SinkError(SinkErrorCode.BUSY)
        lock = None
        self._entered = True
        try:
            try:
                if type(spec) is not GenerationSpec:
                    raise ValueError
                integer(expected_sequence, 0)
            except Exception:
                raise SinkError(SinkErrorCode.INVALID_CONFIG) from None
            token = _CheckedCancellation(cancellation if cancellation is not None else NeverCancelled())
            _check_cancel(token)
            self._admit_generation(spec, generations, dependencies)
            if self._root.is_symlink():
                raise SinkError(SinkErrorCode.INVALID_CONFIG)
            self._root.mkdir(parents=True, exist_ok=True)
            if (self._root / "writer.lock").is_symlink():
                raise SinkError(SinkErrorCode.INVALID_CONFIG)
            lock = _StoreLock(self._root / "writer.lock")
            count, total = self._inventory()
            current = self._load(spec.namespace, spec.job_id)
            replay = current is not None and current.selected.spec == spec
            if not replay:
                if expected_sequence != (current.sequence if current is not None else 0):
                    raise SinkError(SinkErrorCode.CONFLICT)
                if current is not None and any(e.spec.generation_id == spec.generation_id for e in current.history):
                    raise SinkError(SinkErrorCode.CONFLICT)
            generation = self._generation(spec, generations, dependencies, token)
            if not generation.complete:
                return CatalogOutcome(None, generation)
            entry = CatalogEntry(spec, generation.outputs)
            if replay:
                assert current is not None
                if current.selected != entry:
                    raise SinkError(SinkErrorCode.CONFLICT)
                return CatalogOutcome(current, generation)
            history = current.history if current is not None else ()
            if len(history) >= self._limits.max_history or (current is None and count >= self._limits.max_catalogs):
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            result = CatalogSnapshot(spec.namespace, spec.job_id, history + (entry,))
            data = _payload(result, self._limits.max_record_bytes)
            prior_size = len(_payload(current, self._limits.max_record_bytes)) if current is not None else 0
            if total - prior_size + len(data) > self._limits.max_total_bytes:
                raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
            stage = self._root / ("stage-" + uuid.uuid4().hex + ".tmp")
            with stage.open("xb") as stream:
                stream.write(data); stream.flush(); os.fsync(stream.fileno())
            _check_cancel(token)
            try:
                os.replace(stage, self._path(spec.namespace, spec.job_id))
            except OSError as error:
                if sys.platform == "win32" and getattr(error, "winerror", None) in (5, 32, 33):
                    raise SinkError(SinkErrorCode.BUSY) from None
                raise SinkError(SinkErrorCode.UNAVAILABLE) from None
            if self._load(spec.namespace, spec.job_id) != result:
                raise SinkError(SinkErrorCode.CORRUPTION)
            return CatalogOutcome(result, generation)
        except (SinkError, CommandError):
            raise
        except Exception:
            raise SinkError(SinkErrorCode.UNAVAILABLE) from None
        finally:
            try:
                if lock is not None:
                    _close(lock)
            finally:
                self._entered = False
