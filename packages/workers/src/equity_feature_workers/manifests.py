"""Immutable worker identities; acquisition, claims and publication remain separate."""
from __future__ import annotations

from dataclasses import dataclass, fields
from enum import StrEnum
import hashlib
import json
import re
from typing import Any, NoReturn, cast

from equity_feature_contracts import inputs as i, specs as s
from equity_feature_contracts.results import InputBinding
from equity_feature_io_contracts.publication import CompletionReceipt, FeatureHeader, PublicationEnvelope
from equity_feature_io_sdk import decode_envelope, decode_receipt, encode_envelope, encode_receipt, verify_receipt


class ManifestErrorCode(StrEnum):
    INVALID = "INVALID_MANIFEST"
    VERSION = "INCOMPATIBLE_VERSION"
    LIMIT = "RESOURCE_LIMIT"
    BINDING = "INCONSISTENT_IDENTITY"
    UNSUPPORTED = "UNSUPPORTED_CAPABILITY"


class ManifestError(ValueError):
    def __init__(self, code: ManifestErrorCode) -> None:
        self.code = code
        super().__init__("Worker manifest failed: " + code.value)


def fail(code: ManifestErrorCode = ManifestErrorCode.INVALID) -> NoReturn:
    raise ManifestError(code)


def label(value: object) -> None:
    if type(value) is not str or not value.strip() or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in value):
        fail()


def digest(value: object) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        fail()


def integer(value: object, minimum: int = i.I64_MIN) -> None:
    if type(value) is not int or not minimum <= value <= i.I64_MAX:
        fail()


def sequence(value: object, cls: type[Any], *, nonempty: bool = False) -> None:
    if type(value) is not tuple or (nonempty and not value) or any(type(v) is not cls for v in value):
        fail()


@dataclass(frozen=True)
class InputManifest:
    """Expected canonical binding and immutable acquisition identity, including absence."""
    role: str
    acquisition_sha256: str
    revision_id: str
    state: str
    binding: InputBinding | None
    known_at_ns: int | None = None

    def __post_init__(self) -> None:
        label(self.role); digest(self.acquisition_sha256); label(self.revision_id)
        if self.known_at_ns is not None:
            integer(self.known_at_ns)
        if self.state not in ("missing", "empty", "partial", "complete"):
            fail()
        if self.state == "missing":
            if self.binding is not None or self.known_at_ns is not None:
                fail(ManifestErrorCode.BINDING)
        else:
            if type(self.binding) is not InputBinding or self.binding.role != self.role:
                fail(ManifestErrorCode.BINDING)
            coverage = self.binding.metadata.coverage
            if self.state == "empty" and (coverage.observed != 0 or coverage.expected != 0 or not coverage.complete):
                fail(ManifestErrorCode.BINDING)
            if self.state == "complete" and (not coverage.complete or coverage.observed == 0):
                fail(ManifestErrorCode.BINDING)
            if self.state == "partial" and coverage.complete:
                fail(ManifestErrorCode.BINDING)
        # Also rejects unsafe labels embedded in otherwise canonical records.
        _wire(self)


@dataclass(frozen=True)
class TaskManifest:
    job_id: str
    generation_id: str
    partition_id: str
    family: str
    instruments: tuple[str, ...]
    config: s.ConfigSpec
    features: tuple[FeatureHeader, ...]
    inputs: tuple[InputManifest, ...]
    governed_sessions: tuple[s.IntervalSpec, ...]
    destination_scope: str
    ownership: str
    max_input_batches: int
    max_input_bytes: int
    ordered_history: bool = False
    warmup_sessions: tuple[str, ...] = ()
    initialization_sha256: str | None = None
    merge_policy: str = "none"
    protocol_version: str = "efworker-task1"
    canonical_package_version: str = "0.0.4a4"
    canonical_schema_version: str = "1"
    math_policy_version: str = "v1"

    def __post_init__(self) -> None:
        if (self.protocol_version, self.canonical_package_version, self.canonical_schema_version, self.math_policy_version) != ("efworker-task1", "0.0.4a4", "1", "v1"):
            fail(ManifestErrorCode.VERSION)
        for v in (self.job_id, self.generation_id, self.partition_id, self.family, self.destination_scope):
            label(v)
        sequence(self.instruments, str, nonempty=True)
        for v in self.instruments:
            label(v)
        if len(set(self.instruments)) != len(self.instruments):
            fail()
        if type(self.config) is not s.ConfigSpec:
            fail()
        sequence(self.features, FeatureHeader, nonempty=True)
        sequence(self.inputs, InputManifest)
        sequence(self.governed_sessions, s.IntervalSpec, nonempty=True)
        sequence(self.warmup_sessions, str)
        for v in self.warmup_sessions:
            label(v)
        if len({h.feature_id for h in self.features}) != len(self.features) or len({x.role for x in self.inputs}) != len(self.inputs):
            fail()
        if any(h.schema_version != "1" for h in self.features):
            fail(ManifestErrorCode.VERSION)
        bindings = tuple(x.binding for x in self.inputs if x.binding is not None)
        if len({b.metadata.source.input_id for b in bindings}) != len(bindings):
            fail()
        if any(b.metadata.namespace != self.config.session.namespace for b in bindings):
            fail(ManifestErrorCode.BINDING)
        ids = tuple(v.name for v in self.governed_sessions)
        if ids != self.config.window.governed_sessions or len(set(ids)) != len(ids):
            fail(ManifestErrorCode.BINDING)
        if any(a.end_ns > b.start_ns for a, b in zip(self.governed_sessions, self.governed_sessions[1:])):
            fail(ManifestErrorCode.BINDING)
        target = self.governed_sessions[ids.index(self.config.session.session_id)]
        if (target.start_ns, target.end_ns) != (self.config.session.open_ns, self.config.session.close_ns):
            fail(ManifestErrorCode.BINDING)
        if type(self.ordered_history) is not bool or len(set(self.warmup_sessions)) != len(self.warmup_sessions):
            fail()
        target_index = ids.index(self.config.session.session_id)
        if any(v not in ids[:target_index] for v in self.warmup_sessions) or tuple(v for v in ids[:target_index] if v in self.warmup_sessions) != self.warmup_sessions:
            fail(ManifestErrorCode.BINDING)
        if self.initialization_sha256 is not None:
            digest(self.initialization_sha256)
        if self.ordered_history and not self.warmup_sessions and self.initialization_sha256 is None:
            fail(ManifestErrorCode.BINDING)
        if not self.ordered_history and (self.warmup_sessions or self.initialization_sha256 is not None):
            fail(ManifestErrorCode.BINDING)
        # Mathematical splitting is not qualified by this version; across-instrument tasks remain independent.
        if self.merge_policy != "none":
            fail(ManifestErrorCode.UNSUPPORTED)
        if self.ownership not in ("serialized_destination", "independent_destination"):
            fail(ManifestErrorCode.UNSUPPORTED)
        integer(self.max_input_batches, 1); integer(self.max_input_bytes, 1)
        encoded = _canonical(_wire(self))
        if len(encoded) > MAX_MANIFEST_BYTES:
            fail(ManifestErrorCode.LIMIT)

    @property
    def task_sha256(self) -> str:
        return hashlib.sha256(b"efworker-task1\0" + encode_task(self)).hexdigest()

    @property
    def reuse_sha256(self) -> str:
        """Exact compatible input/config/initialization scope, not an actual cache."""
        return hashlib.sha256(b"efworker-reuse1\0" + _canonical(_wire((self.config, self.inputs, self.instruments, self.governed_sessions, self.warmup_sessions, self.initialization_sha256)))).hexdigest()


@dataclass(frozen=True)
class ClaimIdentity:
    task_sha256: str
    owner_id: str
    attempt_id: str
    issued_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        digest(self.task_sha256); label(self.owner_id); label(self.attempt_id)
        integer(self.issued_at_ns); integer(self.expires_at_ns)
        if self.issued_at_ns >= self.expires_at_ns:
            fail()


@dataclass(frozen=True)
class OutputManifest:
    task: TaskManifest
    envelope: PublicationEnvelope
    receipt: CompletionReceipt | None = None

    def __post_init__(self) -> None:
        if type(self.task) is not TaskManifest or type(self.envelope) is not PublicationEnvelope:
            fail()
        t, e = self.task, self.envelope
        # Reuse actual SDK version admission/closed envelope codec.
        try:
            decode_envelope(encode_envelope(e))
        except Exception:
            fail(ManifestErrorCode.VERSION)
        if (e.job_id, e.generation_id, e.partition_id, e.destination_scope, e.canonical_package_version, e.canonical_schema_version, e.math_policy_version) != (t.job_id, t.generation_id, t.partition_id, t.destination_scope, t.canonical_package_version, t.canonical_schema_version, t.math_policy_version):
            fail(ManifestErrorCode.BINDING)
        if not e.result_descriptors:
            fail(ManifestErrorCode.BINDING)
        expected_inputs = tuple(sorted((v.binding for v in t.inputs if v.binding is not None), key=lambda b: b.role))
        for d in e.result_descriptors:
            m = d.metadata
            if (m.namespace, m.session_id, m.config_digest, m.availability, m.inputs, m.math_policy_version, m.schema_version, d.features) != (t.config.session.namespace, t.config.session.session_id, t.config.digest, t.config.availability, expected_inputs, t.math_policy_version, t.canonical_schema_version, t.features):
                fail(ManifestErrorCode.BINDING)
        if self.receipt is not None:
            try:
                verify_receipt(self.receipt, e)
            except Exception:
                fail(ManifestErrorCode.BINDING)

    @property
    def committed(self) -> bool:
        """A structurally verified supplied receipt; physical lookup/readback is separate."""
        return self.receipt is not None


MAX_MANIFEST_BYTES = 1_048_576
# Static input/config closure. Output envelopes/receipts use the existing SDK codec only.
_RECORDS: dict[str, type[Any]] = {v.__name__: v for v in (
    InputManifest, TaskManifest, ClaimIdentity, InputBinding, FeatureHeader,
    i.BatchMetadata, i.SourceBinding, i.Coverage, i.PriceUnit, i.AdjustmentSpec, i.InputScope, i.IntervalCoverage,
    s.AvailabilitySpec, s.IntervalSpec,
)}
from equity_feature_contracts.results import ValueType
_ENUMS: dict[str, type[StrEnum]] = {"DataKind": i.DataKind, "ValueType": ValueType}


def _wire(value: object) -> Any:
    cls = type(value)
    if value is None or cls is bool:
        return value
    if cls is str:
        label(value)
        return value
    if cls is int:
        integer(value)
        return {"int": str(value)}
    if cls in _ENUMS.values():
        return {"enum": cls.__name__, "value": value.value}  # type: ignore[attr-defined]
    if cls is tuple:
        return [_wire(v) for v in cast(tuple[object, ...], value)]
    if cls is s.ConfigSpec:
        return {"config": value.to_json()}  # type: ignore[attr-defined]
    if cls in _RECORDS.values():
        return {"record": cls.__name__, "fields": {f.name: _wire(getattr(value, f.name)) for f in fields(value)}}  # type: ignore[arg-type]
    fail()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for k, v in pairs:
        if k in result:
            fail()
        result[k] = v
    return result


def _decode(value: Any) -> Any:
    if value is None or type(value) is bool:
        return value
    if type(value) is str:
        label(value)
        return value
    if type(value) is list:
        return tuple(_decode(v) for v in value)
    if type(value) is not dict:
        fail()
    if set(value) == {"int"} and type(value["int"]) is str and re.fullmatch(r"0|-?[1-9][0-9]*", value["int"]):
        number = int(value["int"]); integer(number)
        return number
    if set(value) == {"config"} and type(value["config"]) is str:
        config = s.ConfigSpec.from_json(value["config"])
        if config.to_json() != value["config"]:
            fail()
        return config
    if set(value) == {"enum", "value"} and type(value["enum"]) is str and value["enum"] in _ENUMS:
        return _ENUMS[value["enum"]](value["value"])
    if set(value) == {"record", "fields"} and type(value["record"]) is str and value["record"] in _RECORDS:
        cls = _RECORDS[value["record"]]
        if type(value["fields"]) is not dict or set(value["fields"]) != {f.name for f in fields(cls)}:
            fail()
        return cls(**{k: _decode(v) for k, v in value["fields"].items()})
    fail()


def encode_task(task: TaskManifest) -> bytes:
    if type(task) is not TaskManifest:
        fail()
    return _canonical(_wire(task))


def decode_task(data: bytes) -> TaskManifest:
    if type(data) is not bytes or len(data) > MAX_MANIFEST_BYTES:
        fail(ManifestErrorCode.LIMIT)
    try:
        result = _decode(json.loads(data.decode("ascii"), object_pairs_hook=_pairs))
        if type(result) is not TaskManifest or encode_task(result) != data:
            fail()
        return result
    except ManifestError:
        raise
    except Exception:
        fail()


def encode_output(output: OutputManifest) -> bytes:
    if type(output) is not OutputManifest:
        fail()
    value = {"protocol": "efworker-output1", "task": encode_task(output.task).decode("ascii"), "envelope": encode_envelope(output.envelope).decode("ascii"), "receipt": None if output.receipt is None else encode_receipt(output.receipt).decode("ascii")}
    data = _canonical(value)
    if len(data) > MAX_MANIFEST_BYTES:
        fail(ManifestErrorCode.LIMIT)
    return data


def decode_output(data: bytes) -> OutputManifest:
    if type(data) is not bytes or len(data) > MAX_MANIFEST_BYTES:
        fail(ManifestErrorCode.LIMIT)
    try:
        v = json.loads(data.decode("ascii"), object_pairs_hook=_pairs)
        if type(v) is not dict or set(v) != {"protocol", "task", "envelope", "receipt"} or v["protocol"] != "efworker-output1":
            fail()
        result = OutputManifest(decode_task(v["task"].encode("ascii")), decode_envelope(v["envelope"].encode("ascii")), None if v["receipt"] is None else decode_receipt(v["receipt"].encode("ascii")))
        if encode_output(result) != data:
            fail()
        return result
    except ManifestError:
        raise
    except Exception:
        fail()
