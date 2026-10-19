"""Independent in-memory lifecycle fixture; no process/storage durability claim."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib

from equity_feature_contracts.results import FeatureResult
from equity_feature_io_contracts.publication import (
    AbortOutcome, ArtifactReference, CompletionReceipt, FeatureHeader, PublicationEnvelope,
    PublicationState, PublicationStatus, ResultDescriptor, SinkCapabilities, SinkError,
    SinkErrorCode, WriteSession, digest,
)
from equity_feature_io_sdk.codec import decode_result, encode_result, idempotency_key


@dataclass(frozen=True)
class Session:
    attempt_id: str
    key: str


@dataclass
class Reservation:
    envelope: PublicationEnvelope
    state: PublicationState
    session: Session
    data: list[bytes]
    receipt: CompletionReceipt | None = None


def _descriptor(result: FeatureResult) -> ResultDescriptor:
    return ResultDescriptor(result.metadata, tuple(FeatureHeader(c.feature_id, c.algorithm_version, c.dtype, c.unit, c.schema_version) for c in result.values))


class MemorySink:
    """Single Python instance only. Restart/faults are simulations, not durable recovery."""
    def __init__(self, fault: str | None = None) -> None:
        if fault not in (None, "before_write", "after_write", "before_commit", "after_commit", "unknown_lookup"):
            raise ValueError("unknown synthetic fault")
        self.fault = fault
        self._reservations: dict[str, Reservation] = {}
        self._attempt = 0

    def capabilities(self) -> SinkCapabilities:
        return SinkCapabilities(("1",), ("efio-json1",), ("efio-key1",), ("efio-content1",),
                                ("0.0.4a4",), ("1",), ("v1",), "transactional", "single_writer",
                                True, True, True, 1, 100, 1048576, 10485760, 100000, 100000)

    def begin(self, envelope: PublicationEnvelope) -> Session | CompletionReceipt:
        if type(envelope) is not PublicationEnvelope:
            raise SinkError(SinkErrorCode.INVALID_CONFIG)
        if (envelope.protocol_version, envelope.codec_version, envelope.identity_version, envelope.digest_version,
            envelope.canonical_package_version, envelope.canonical_schema_version, envelope.math_policy_version) != ("1", "efio-json1", "efio-key1", "efio-content1", "0.0.4a4", "1", "v1"):
            raise SinkError(SinkErrorCode.INCOMPATIBLE_VERSION)
        caps = self.capabilities()
        if any(d.metadata.schema_version != envelope.canonical_schema_version or d.metadata.math_policy_version != envelope.math_policy_version or
               any(h.schema_version != envelope.canonical_schema_version for h in d.features) for d in envelope.result_descriptors):
            raise SinkError(SinkErrorCode.INCOMPATIBLE_VERSION)
        if envelope.result_count > min(envelope.max_results, caps.max_results) or envelope.content_bytes > min(envelope.max_total_bytes, caps.max_total_bytes) or envelope.cell_count > min(envelope.max_result_cells, caps.max_result_cells) or envelope.evidence_count > min(envelope.max_evidence_rows, caps.max_evidence_rows):
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        key = idempotency_key(envelope.identity)
        previous = self._reservations.get(key)
        if previous is not None:
            e = previous.envelope
            if (e.expected_content_sha256, e.result_count, e.cell_count, e.evidence_count, e.content_bytes) != (envelope.expected_content_sha256, envelope.result_count, envelope.cell_count, envelope.evidence_count, envelope.content_bytes):
                raise SinkError(SinkErrorCode.CONFLICT)
            if previous.state is PublicationState.COMMITTED:
                if any(len(data) > min(envelope.max_chunk_bytes, caps.max_chunk_bytes) for data in previous.data):
                    raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
                assert previous.receipt is not None
                self.read(previous.receipt)
                return previous.receipt
            if previous.state is PublicationState.STAGING:
                raise SinkError(SinkErrorCode.BUSY)
        self._attempt += 1
        session = Session(str(self._attempt), key)
        self._reservations[key] = Reservation(envelope, PublicationState.STAGING, session, [])
        return session

    def _owned(self, session: WriteSession, *, staging: bool = True) -> Reservation:
        if type(session) is not Session:
            raise SinkError(SinkErrorCode.INVALID_SESSION)
        assert isinstance(session, Session)
        reservation = self._reservations.get(session.key)
        if reservation is None or reservation.session is not session or (staging and reservation.state is not PublicationState.STAGING):
            raise SinkError(SinkErrorCode.INVALID_SESSION)
        return reservation

    def write(self, session: WriteSession, ordinal: int, result: FeatureResult) -> None:
        reservation = self._owned(session)
        e = reservation.envelope
        if type(ordinal) is not int or ordinal != len(reservation.data) or ordinal >= e.result_count:
            raise SinkError(SinkErrorCode.INVALID_CONTENT)
        if self.fault == "before_write":
            raise RuntimeError("synthetic_sensitive_fixture_marker")
        data = encode_result(result)
        if _descriptor(result) != e.result_descriptors[ordinal]:
            raise SinkError(SinkErrorCode.INVALID_CONTENT)
        if len(data) > min(e.max_chunk_bytes, self.capabilities().max_chunk_bytes) or sum(map(len, reservation.data)) + len(data) > min(e.max_total_bytes, self.capabilities().max_total_bytes):
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        delivered = tuple(decode_result(data) for data in reservation.data) + (result,)
        cells = sum(len(c.entities) for r in delivered for c in r.values)
        evidence = sum(len(r.evidence) for r in delivered)
        if cells > min(e.max_result_cells, self.capabilities().max_result_cells) or evidence > min(e.max_evidence_rows, self.capabilities().max_evidence_rows):
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        keys = [(r.metadata.namespace, entity.instrument_id, entity.session_id, c.feature_id) for r in delivered for c in r.values for entity in c.entities]
        if len(keys) != len(set(keys)):
            raise SinkError(SinkErrorCode.INVALID_CONTENT)
        reservation.data.append(data)
        if self.fault == "after_write":
            raise RuntimeError("synthetic_sensitive_fixture_marker")

    def commit(self, session: WriteSession) -> CompletionReceipt:
        reservation = self._owned(session)
        e = reservation.envelope
        if self.fault == "before_commit":
            raise SinkError(SinkErrorCode.RETRYABLE_FAILURE)
        result = tuple(decode_result(data) for data in reservation.data)
        hasher = hashlib.sha256(b"efio-content1\0")
        for data in reservation.data:
            hasher.update(len(data).to_bytes(8, "big"))
            hasher.update(data)
        cells = sum(len(c.entities) for r in result for c in r.values)
        evidence = sum(len(r.evidence) for r in result)
        if (len(result), cells, evidence, sum(map(len, reservation.data)), hasher.hexdigest()) != (e.result_count, e.cell_count, e.evidence_count, e.content_bytes, e.expected_content_sha256):
            raise SinkError(SinkErrorCode.CONFLICT)
        keys = [(r.metadata.namespace, entity.instrument_id, entity.session_id, c.feature_id) for r in result for c in r.values for entity in c.entities]
        if len(keys) != len(set(keys)):
            raise SinkError(SinkErrorCode.INVALID_CONTENT)
        artifacts = tuple(ArtifactReference(f"result-{index}.json", hashlib.sha256(data).hexdigest(), len(data)) for index, data in enumerate(reservation.data))
        receipt = CompletionReceipt(e.identity, idempotency_key(e.identity), hasher.hexdigest(), len(result), cells, evidence,
                                    sum(map(len, reservation.data)), artifacts, 10)
        reservation.receipt = receipt
        reservation.state = PublicationState.COMMITTED
        if self.fault in ("after_commit", "unknown_lookup"):
            raise RuntimeError("synthetic_sensitive_fixture_marker")
        return receipt

    def abort(self, session: WriteSession) -> AbortOutcome:
        reservation = self._owned(session, staging=False)
        if reservation.state is PublicationState.COMMITTED:
            assert reservation.receipt is not None
            self.read(reservation.receipt)
            return AbortOutcome(PublicationState.COMMITTED, reservation.receipt)
        reservation.state = PublicationState.ABORTED
        reservation.data.clear()
        return AbortOutcome(PublicationState.ABORTED, None)

    def lookup(self, idempotency_key: str) -> PublicationStatus:
        digest(idempotency_key)
        if self.fault == "unknown_lookup":
            return PublicationStatus(PublicationState.UNKNOWN, None)
        reservation = self._reservations.get(idempotency_key)
        if reservation is not None and reservation.state is PublicationState.COMMITTED:
            assert reservation.receipt is not None
            self.read(reservation.receipt)
        return PublicationStatus(PublicationState.ABSENT, None) if reservation is None else PublicationStatus(reservation.state, reservation.receipt)

    def read(self, receipt: CompletionReceipt) -> tuple[FeatureResult, ...]:
        reservation = self._reservations.get(receipt.idempotency_key)
        if reservation is None or reservation.state is not PublicationState.COMMITTED:
            raise SinkError(SinkErrorCode.UNAVAILABLE)
        if reservation.receipt != receipt or len(receipt.artifacts) != len(reservation.data):
            raise SinkError(SinkErrorCode.CORRUPTION)
        for artifact, data in zip(receipt.artifacts, reservation.data, strict=True):
            if artifact.byte_length != len(data) or artifact.byte_sha256 != hashlib.sha256(data).hexdigest():
                raise SinkError(SinkErrorCode.CORRUPTION)
        try:
            results = tuple(decode_result(data) for data in reservation.data)
            hasher = hashlib.sha256(b"efio-content1\0")
            for data in reservation.data:
                hasher.update(len(data).to_bytes(8, "big"))
                hasher.update(data)
            cells = sum(len(c.entities) for r in results for c in r.values)
            evidence = sum(len(r.evidence) for r in results)
            if receipt.identity != reservation.envelope.identity or receipt.idempotency_key != idempotency_key(receipt.identity) or tuple(_descriptor(r) for r in results) != receipt.identity.result_descriptors:
                raise SinkError(SinkErrorCode.CORRUPTION)
            if (len(results), cells, evidence, sum(map(len, reservation.data)), hasher.hexdigest()) != (receipt.result_count, receipt.cell_count, receipt.evidence_count, receipt.content_bytes, receipt.content_sha256):
                raise SinkError(SinkErrorCode.CORRUPTION)
            return results
        except Exception:
            raise SinkError(SinkErrorCode.CORRUPTION) from None

    def simulate_restart(self) -> None:
        """Invalidate staging attempts while retaining reservations in this Python object."""
        for reservation in self._reservations.values():
            if reservation.state is PublicationState.STAGING:
                reservation.state = PublicationState.ABORTED
                reservation.data.clear()

    def corrupt(self, key: str, ordinal: int, replacement: bytes) -> None:
        """Explicit synthetic tamper hook, not an extension protocol method."""
        self._reservations[key].data[ordinal] = replacement
