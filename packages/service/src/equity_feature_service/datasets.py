"""Operator-admitted immutable sources; no remote imports or source factories."""
from __future__ import annotations

from dataclasses import dataclass, fields
import hashlib
import json
import re
from typing import Any, Callable
from equity_feature_contracts.adapters import Cancellation

from equity_feature_contracts import inputs as i, results as r
from equity_feature_contracts.validation import validate_batch
from equity_feature_io_sdk import encode_result, decode_result

from . import codec


@dataclass(frozen=True)
class Scope:
    instrument_id: str
    session_id: str
    start_ns: int
    end_ns: int

    def __post_init__(self) -> None:
        for x in (self.instrument_id, self.session_id):
            if type(x) is not str or not x or len(x) > 256:
                raise ValueError("invalid_scope")
        for time_ns in (self.start_ns, self.end_ns):
            if type(time_ns) is not int:
                raise ValueError("invalid_scope")
            codec.integer(str(time_ns))
        if self.start_ns >= self.end_ns:
            raise ValueError("invalid_scope")

    def wire(self) -> codec.Json:
        return {"instrument_id": self.instrument_id, "session_id": self.session_id,
                "start_ns": str(self.start_ns), "end_ns": str(self.end_ns)}


@dataclass(frozen=True)
class DatasetIdentity:
    dataset_id: str
    revision: str
    snapshot_id: str
    mapping_version: str
    source_id: str
    input_id: str

    def wire(self) -> codec.Json:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_source(cls, dataset_id: str, revision: str, source: i.SourceBinding) -> DatasetIdentity:
        return cls(dataset_id, revision, source.snapshot_id, source.mapping_version,
                   source.source_id, source.input_id)


def _fresh(value: Any) -> Any:
    if type(value) in codec._TYPES:
        return type(value)(**{f.name: _fresh(getattr(value, f.name)) for f in fields(value)})
    if type(value) in (tuple, list):
        return tuple(_fresh(v) for v in value)
    return value


def _batch(value: i.CanonicalBatch, scope: Scope) -> i.CanonicalBatch:
    if type(value) is not i.CanonicalBatch or value.row_count > 100:
        raise codec.WireError("bounds")
    if len(value.columns) > 64:
        raise codec.WireError("bounds")
    # Reapply native constructors even to forged or mutated frozen instances.
    batch = i.CanonicalBatch(value.kind, tuple(i.Column(c.name, c.values) for c in value.columns), _fresh(value.metadata))
    validate_batch(batch)
    if batch.metadata.ordering != "declared" or batch.metadata.scope is None:
        raise codec.WireError("inconsistent_identity")
    if (batch.metadata.scope.start_ns, batch.metadata.scope.end_ns) != (scope.start_ns, scope.end_ns):
        raise codec.WireError("inconsistent_identity")
    for name, expected in (("instrument_id", scope.instrument_id), ("session_id", scope.session_id)):
        col = batch.column(name)
        if col is None or any(x != expected for x in col.values):
            raise codec.WireError("inconsistent_identity")
    for name in ("event_ns", "start_ns", "effective_start_ns"):
        col = batch.column(name)
        if col is not None and any(type(x) is not int or not scope.start_ns <= x < scope.end_ns for x in col.values):
            raise codec.WireError("inconsistent_identity")
    col = batch.column("end_ns")
    if col is not None and any(type(x) is not int or not scope.start_ns < x <= scope.end_ns for x in col.values):
        raise codec.WireError("inconsistent_identity")
    return batch


def _batch_bytes(batch: i.CanonicalBatch) -> bytes:
    data = codec.canonical({"kind": batch.kind.value, "metadata": codec.cell(batch.metadata),
                            "columns": [{"name": c.name, "values": [codec.cell(v, decimal=i.schema_for(batch.kind).field(c.name).dtype == i.DType.DECIMAL128) for v in c.values]} for c in batch.columns]})
    if len(data) > 1_048_576:
        raise codec.WireError("bounds")
    return data


@dataclass(frozen=True)
class RawRead:
    batch: i.CanonicalBatch
    receipt_fingerprint: str


class RawDataset:
    kind = "raw"

    def __init__(self, dataset_id: str, revision: str, scope: Scope, baseline: RawRead,
                 read: Callable[[Cancellation | None], RawRead], *, columns: tuple[str, ...], rights: frozenset[str],
                 rights_owner: str, rights_evidence: str, valid_from_ns: int, expires_at_ns: int) -> None:
        batch = _batch(baseline.batch, scope)
        if re.fullmatch(r"[0-9a-f]{64}", baseline.receipt_fingerprint) is None:
            raise ValueError("invalid_receipt")
        self.identity = DatasetIdentity.from_source(dataset_id, revision, batch.metadata.source)
        self.scope, self.columns = scope, tuple(columns)
        if not columns or len(set(columns)) != len(columns) or len(columns) > 64:
            raise ValueError("invalid_columns")
        for name in columns:
            i.schema_for(batch.kind).field(name)
            if batch.column(name) is None:
                raise ValueError("absent_native_column")
        self._fingerprint = hashlib.sha256(_batch_bytes(batch)).digest()
        self._receipt = baseline.receipt_fingerprint
        self._read = read
        self.rights, self.rights_owner, self.rights_evidence = rights, rights_owner, rights_evidence
        self.valid_from_ns, self.expires_at_ns = valid_from_ns, expires_at_ns
        self.features: tuple[tuple[str, str], ...] = ()
        self._metadata = codec.canonical(codec.cell(batch.metadata))
        self._kind = batch.kind
        self._value_sizes: dict[str, int] = {}
        for name in columns:
            field = i.schema_for(batch.kind).field(name)
            col = batch.column(name)
            values = col.values if col is not None else (None,) * batch.row_count
            self._value_sizes[name] = len(codec.canonical([codec.cell(v, decimal=field.dtype == i.DType.DECIMAL128) for v in values])) - 2

    @property
    def acquisition_commitment(self) -> tuple[str, str]:
        """Complete admitted batch and receipt hashes, without another read."""
        return self._fingerprint.hex(),self._receipt

    def _shell(self, name: str) -> codec.Json:
        field = i.schema_for(self._kind).field(name)
        dtype = "int64" if field.dtype == i.DType.UTC_NS else field.dtype.value
        unit = "UTCns" if field.dtype == i.DType.UTC_NS else "scaled_price" if name in ("price", "bid", "ask", "open", "high", "low", "close") else "shares" if name in ("size", "volume", "bid_size", "ask_size") else "dimensionless"
        return {"name": name, "dtype": dtype, "unit": unit, "values": []}

    def size_hint(self, selected: tuple[str, ...], version: str, request_id: str) -> int:
        body = {"dataset": self.identity.wire(), "scope": self.scope.wire(),
                "columns": [self._shell(name) for name in selected], "metadata": json.loads(self._metadata), "next_cursor": None}
        envelope = {"schema": "equity.remote", "version": version, "kind": "slice", "request_id": request_id, "payload": body}
        return len(codec.canonical(envelope)) + sum(self._value_sizes[name] for name in selected)

    def produce(self, selected: tuple[str, ...], version: str, cancellation: Cancellation | None = None) -> tuple[str, codec.Json]:
        if cancellation is not None and cancellation.is_cancelled():
            raise codec.WireError("not_permitted")
        read = self._read(cancellation)
        if type(read) is not RawRead:
            raise codec.WireError("inconsistent_identity")
        batch = _batch(read.batch, self.scope)
        if read.receipt_fingerprint != self._receipt or hashlib.sha256(_batch_bytes(batch)).digest() != self._fingerprint:
            raise codec.WireError("inconsistent_identity")
        columns: list[codec.Json] = []
        for name in selected:
            field = i.schema_for(batch.kind).field(name)
            col = batch.column(name)
            values = col.values if col is not None else (None,) * batch.row_count
            column = self._shell(name)
            column["values"] = [codec.cell(v, decimal=field.dtype == i.DType.DECIMAL128) for v in values]
            columns.append(column)
        return "slice", {"dataset": self.identity.wire(), "scope": self.scope.wire(), "columns": columns,
                         "metadata": codec.cell(batch.metadata), "next_cursor": None}


class FeatureDataset:
    kind = "feature"

    def __init__(self, identity: DatasetIdentity, scope: Scope, result: r.FeatureResult,
                 producer_context: codec.Json, producer_command_digest: str, *, rights: frozenset[str],
                 rights_owner: str, rights_evidence: str, valid_from_ns: int, expires_at_ns: int) -> None:
        if type(result) is not r.FeatureResult or len(result.values) > 39 or len(result.evidence) > 100:
            raise codec.WireError("bounds")
        if any(len(c.entities) > 100 for c in result.values) or sum(b.metadata.coverage.observed for b in result.metadata.inputs) > 100:
            raise codec.WireError("bounds")
        for c in result.values:
            if any(e.instrument_id != scope.instrument_id or e.session_id != scope.session_id for e in c.entities):
                raise codec.WireError("inconsistent_identity")
        data = encode_result(result)  # Public SDK reapplies full native semantic constructors.
        if len(data) > 1_048_576:
            raise codec.WireError("bounds")
        native = decode_result(data)
        self.identity, self.scope = identity, scope
        self.columns = tuple(c.feature_id for c in native.values)
        self.features = tuple((c.feature_id, c.algorithm_version) for c in native.values)
        self.rights, self.rights_owner, self.rights_evidence = rights, rights_owner, rights_evidence
        self.valid_from_ns, self.expires_at_ns = valid_from_ns, expires_at_ns
        context = json.loads(codec.canonical(producer_context))
        self.registry_snapshot = context["registry_snapshot"]
        md = native.metadata
        expected = {"namespace": md.namespace, "math_policy_version": md.math_policy_version}
        if any(context.get(k) != v for k, v in expected.items()) or context["config"]["digest"] != md.config_digest:
            raise codec.WireError("inconsistent_identity")
        availability = {f.name: str(getattr(md.availability, f.name)) if f.name.endswith("_ns") else getattr(md.availability, f.name) for f in fields(md.availability)}
        if context.get("availability") != availability or md.session_id != scope.session_id:
            raise codec.WireError("inconsistent_identity")
        actual_bindings = [b.metadata.source for b in md.inputs]
        claimed = context["dataset"]
        if not any(all(claimed[k] == getattr(source, k) for k in ("source_id", "snapshot_id", "mapping_version", "input_id")) for source in actual_bindings):
            raise codec.WireError("inconsistent_identity")
        if any(pair["feature_id"] not in self.columns or (pair["feature_id"], pair["algorithm_version"]) not in self.features for pair in context["features"]):
            raise codec.WireError("inconsistent_identity")
        if re.fullmatch(r"[0-9a-f]{64}", producer_command_digest) is None:
            raise codec.WireError("invalid_schema")
        original_command = {"operation": "calculate", "context": context, "scope": scope.wire()}
        actual_digest = hashlib.sha256(codec.canonical(original_command)).hexdigest()
        if producer_command_digest != actual_digest:
            raise codec.WireError("inconsistent_identity")
        # Pin source aggregate scope rather than relabeling numerical values.
        if not md.inputs or any(b.metadata.scope is None or (b.metadata.scope.start_ns, b.metadata.scope.end_ns) != (scope.start_ns, scope.end_ns) for b in md.inputs):
            raise codec.WireError("inconsistent_identity")
        self._result: codec.Json = {
            "context": context, "command_digest": producer_command_digest,
            "backend_id": md.backend_id, "backend_version": md.backend_version,
            "metadata": codec.cell(md), "columns": [{"feature_id": c.feature_id,
                "algorithm_version": c.algorithm_version, "schema_version": c.schema_version,
                "dtype": c.dtype.value, "unit": c.unit,
                "entities": [{"instrument_id": e.instrument_id, "session_id": e.session_id} for e in c.entities],
                "values": [codec.cell(v, decimal=c.dtype == r.ValueType.DECIMAL128) for v in c.values]} for c in native.values],
            "quality": [{"entity": {"instrument_id": q.entity.instrument_id, "session_id": q.entity.session_id},
                "feature_id": q.feature_id, "status": q.status.value, "expected": None if q.expected is None else str(q.expected),
                "observed": str(q.observed), "reasons": [reason.value for reason in q.reasons]} for q in native.quality],
            "evidence": [codec.cell(e) for e in native.evidence], "next_cursor": None,
            "executed_features": [{"feature_id": f, "algorithm_version": a} for f, a in self.features]}
        self._result_bytes = codec.canonical(self._result)
        del self._result

    def size_hint(self, selected: tuple[str, ...], version: str, request_id: str) -> int:
        kind, body = self.produce(selected, version)
        return len(codec.canonical({"schema": "equity.remote", "version": version, "kind": kind, "request_id": request_id, "payload": body}))

    def produce(self, selected: tuple[str, ...], version: str, cancellation: Cancellation | None = None) -> tuple[str, codec.Json]:
        if version != "1.1":
            raise codec.WireError("incompatible_version")
        result = json.loads(self._result_bytes)
        result["columns"] = [c for c in result["columns"] if c["feature_id"] in selected]
        result["quality"] = [q for q in result["quality"] if q["feature_id"] in selected]
        result["evidence"] = [e for e in result["evidence"] if e["fields"]["feature_id"]["value"] in selected]
        return "feature_slice", {"dataset": self.identity.wire(), "scope": self.scope.wire(),
            "selected_features": [{"feature_id": f, "algorithm_version": a} for f, a in self.features if f in selected],
            "feature_result": result}


Dataset = RawDataset | FeatureDataset
