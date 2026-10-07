"""Bounded single-entity session composition through public contracts only."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from enum import StrEnum
import hashlib
import json
from typing import Callable, NoReturn

from equity_feature_contracts import CanonicalBatch, Column, ConfigSpec, DataKind, EntityKey, FeatureResult, InputBinding
from equity_feature_contracts.adapters import AcquisitionRequest, AdapterBatch, HistoricalAdapter, validate_delivery
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_contracts import CredentialProvider, FeatureHeader, PublicConfig, ResultSink, SinkRequirements
from equity_feature_io_sdk import (
    Cancellation, SinkRegistry, SourceRegistry, admit_sink, admit_source, descriptor,
    encode_result, prepare_publication, publish, verify_content, verify_receipt,
)
from equity_features.session import compute_bars, compute_quotes, compute_trades
from .manifests import InputManifest, OutputManifest, TaskManifest, integer, label


class CommandErrorCode(StrEnum):
    CONFIG = "INVALID_COMMAND"
    SOURCE = "SOURCE_FAILED"
    LIMIT = "RESOURCE_LIMIT"
    CANCELLED = "CANCELLED"
    CALCULATION = "CALCULATION_FAILED"
    RESULT = "RESULT_IDENTITY_MISMATCH"
    SINK = "PUBLICATION_FAILED"
    READBACK = "READBACK_FAILED"


class CommandError(ValueError):
    def __init__(self, code: CommandErrorCode) -> None:
        self.code = code
        super().__init__("Session command failed: " + code.value)


def _fail(code: CommandErrorCode) -> NoReturn:
    raise CommandError(code) from None


class NeverCancelled:
    def is_cancelled(self) -> bool:
        return False


_CALCULATORS: dict[str, Callable[..., FeatureResult]] = {
    "bars": compute_bars, "trades": compute_trades, "quotes": compute_quotes,
}
_KINDS = {"bars": DataKind.BAR, "trades": DataKind.TRADE, "quotes": DataKind.QUOTE}


@dataclass(frozen=True)
class SessionCommandSpec:
    """Requested work; observed bindings are sealed only after acquisition."""
    job_id: str
    generation_id: str
    partition_id: str
    family: str
    request: AcquisitionRequest
    config: ConfigSpec
    features: tuple[FeatureHeader, ...]
    governed_sessions: tuple[IntervalSpec, ...]
    revision_id: str
    destination_scope: str
    max_input_bytes: int
    ownership: str = "serialized_destination"

    def __post_init__(self) -> None:
        try:
            if type(self.request) is not AcquisitionRequest or type(self.config) is not ConfigSpec:
                _fail(CommandErrorCode.CONFIG)
            r, c = self.request, self.config
            if self.family not in _KINDS or len(r.instruments) != 1 or r.sessions != (c.session.session_id,):
                _fail(CommandErrorCode.CONFIG)
            if (r.kind, r.namespace, r.price_unit, r.adjustment, r.availability, r.start_ns, r.end_ns) != (
                _KINDS[self.family], c.session.namespace, c.price_unit, c.adjustment,
                c.availability, c.session.open_ns, c.availability.market_cutoff_ns,
            ) or c.session.include_closing_auction or c.session.include_opening_auction:
                _fail(CommandErrorCode.CONFIG)
            label(self.revision_id)
            integer(self.max_input_bytes, 1)
            _record_hash(asdict(r), b"", min(self.max_input_bytes, 1048576))
            self.task(())  # Reuse the manifest's exact config/interval/version admission.
            # Public calculation admission and inventory, without source or sink access.
            expected = descriptor(_CALCULATORS[self.family](None, c, entity=self.entity)).features
            if expected != self.features:
                _fail(CommandErrorCode.CONFIG)
        except Exception:
            _fail(CommandErrorCode.CONFIG)

    @property
    def entity(self) -> EntityKey:
        return EntityKey(self.request.instruments[0], self.config.session.session_id)

    def task(self, inputs: tuple[InputManifest, ...]) -> TaskManifest:
        return TaskManifest(self.job_id, self.generation_id, self.partition_id, self.family,
                            self.request.instruments, self.config, self.features, inputs,
                            self.governed_sessions, self.destination_scope, self.ownership,
                            self.request.max_batches, self.max_input_bytes)


@dataclass(frozen=True)
class CommandOutcome:
    output: OutputManifest
    results: tuple[FeatureResult, ...]


def _cancel(cancellation: Cancellation) -> None:
    if cancellation.is_cancelled():
        _fail(CommandErrorCode.CANCELLED)


def _record_hash(value: object, prefix: bytes, budget: int) -> tuple[str, int]:
    """Incrementally count exact ASCII JSON content; no unbounded joined wire copy."""
    hasher = hashlib.sha256(prefix)
    count = 0
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    for fragment in encoder.iterencode(value):
        count += len(fragment)
        if count > budget:
            _fail(CommandErrorCode.LIMIT)
        hasher.update(fragment.encode("ascii"))
    return hasher.hexdigest(), count


def _delivery_record(delivery: AdapterBatch, *, content: bool = False) -> dict[str, object]:
    # Do not dataclasses.asdict the entire batch: it recursively copies every cell.
    record: dict[str, object] = {
        "request_id": delivery.request_id, "ordinal": delivery.ordinal, "final": delivery.final,
        "source": asdict(delivery.source), "source_coverage": asdict(delivery.source_coverage),
        "delivery_coverage": asdict(delivery.delivery_coverage), "disposition": delivery.disposition,
        "reason": delivery.reason,
    }
    batch = delivery.batch
    if content and batch is not None:
        record["batch"] = {"kind": batch.kind, "metadata": asdict(batch.metadata),
                           "columns": [{"name": c.name, "values": c.values} for c in batch.columns]}
    return record


def _collect(spec: SessionCommandSpec, source: HistoricalAdapter, cancellation: Cancellation) -> tuple[CanonicalBatch | None, str]:
    request = spec.request
    deliveries: list[AdapterBatch] = []
    used = 0
    rows = 0
    iterator = iter(source.iter_batches(request, cancellation))
    try:
        while True:
            _cancel(cancellation)
            try:
                delivery = next(iterator)
            except StopIteration:
                break
            _cancel(cancellation)
            if len(deliveries) >= request.max_batches or type(delivery) is not AdapterBatch:
                _fail(CommandErrorCode.LIMIT if type(delivery) is AdapterBatch else CommandErrorCode.SOURCE)
            batch = delivery.batch
            if batch is not None:
                rows += batch.row_count
                if batch.row_count > request.max_batch_rows or rows > request.max_rows:
                    _fail(CommandErrorCode.LIMIT)
            _, size = _record_hash(_delivery_record(delivery, content=True), b"", spec.max_input_bytes - used)
            used += size
            deliveries.append(delivery)
            if delivery.final:
                # Require true exhaustion, not silently truncated content after a final marker.
                _cancel(cancellation)
                try:
                    next(iterator)
                except StopIteration:
                    break
                _fail(CommandErrorCode.SOURCE)
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
    validate_delivery(request, source.capabilities(), tuple(deliveries))
    acquisition, _ = _record_hash({"request": asdict(request), "deliveries": [
        _delivery_record(d) for d in deliveries
    ]}, b"efworker-acquisition1\0", spec.max_input_bytes + 1048576)
    batches = tuple(d.batch for d in deliveries if d.batch is not None)
    if not batches:
        return None, acquisition
    first = batches[0]
    if any(replace(b.metadata, source=first.metadata.source) != first.metadata for b in batches):
        _fail(CommandErrorCode.SOURCE)
    if len(batches) == 1:
        return first, acquisition
    aggregate, _ = _record_hash([b.metadata.source.input_id for b in batches], b"efworker-chunks1\0", spec.max_input_bytes)
    metadata = replace(first.metadata, source=replace(first.metadata.source, input_id="worker-chunks-" + aggregate))
    columns = tuple(Column(c.name, tuple(v for b in batches for v in b.columns[index].values))
                    for index, c in enumerate(first.columns))
    return CanonicalBatch(first.kind, columns, metadata), acquisition


def run_session(spec: SessionCommandSpec, source: HistoricalAdapter, sink: ResultSink, *,
                requirements: SinkRequirements, cancellation: Cancellation | None = None) -> CommandOutcome:
    """Return success only after actual entity admission, commit and verified readback."""
    if type(spec) is not SessionCommandSpec or type(requirements) is not SinkRequirements:
        _fail(CommandErrorCode.CONFIG)
    token = cancellation if cancellation is not None else NeverCancelled()
    try:
        _cancel(token)
        admit_source(source, spec.request)
        admit_sink(sink, requirements)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CONFIG)
    try:
        batch, acquisition = _collect(spec, source, token)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.SOURCE)
    try:
        _cancel(token)
        binding = InputBinding(spec.family, _KINDS[spec.family], batch.metadata) if batch is not None else None
        coverage = batch.metadata.coverage if batch is not None else None
        state = "missing" if coverage is None else "partial" if not coverage.complete else "empty" if coverage.observed == 0 else "complete"
        task = spec.task((InputManifest(spec.family, acquisition, spec.revision_id, state, binding),))
        result = _CALCULATORS[spec.family](batch, spec.config, entity=spec.entity)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CALCULATION)
    if any(c.entities != (spec.entity,) for c in result.values) or descriptor(result).features != task.features:
        _fail(CommandErrorCode.RESULT)
    results = (result,)
    try:
        limits = replace(requirements, visibility=None, writer_mode=None, reservation_retention_ns=1)
        envelope = prepare_publication(results, destination_scope=task.destination_scope, generation_id=task.generation_id,
                                       job_id=task.job_id, partition_id=task.partition_id, limits=limits)
        OutputManifest(task, envelope)
        verify_content(envelope, results)
        _cancel(token)
        receipt = publish(sink, envelope, results, requirements=requirements, cancellation=token)
        verify_receipt(receipt, envelope, results)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.SINK)
    try:
        _cancel(token)
        readback = sink.read(receipt)
        verify_receipt(receipt, envelope, readback)
        if tuple(map(encode_result, readback)) != tuple(map(encode_result, results)):
            _fail(CommandErrorCode.READBACK)
        return CommandOutcome(OutputManifest(task, envelope, receipt), results)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.READBACK)


def run_registered(spec: SessionCommandSpec, *, sources: SourceRegistry[HistoricalAdapter], sinks: SinkRegistry[ResultSink],
                   source_id: str, source_config: PublicConfig, sink_id: str, sink_config: PublicConfig,
                   credentials: CredentialProvider, requirements: SinkRequirements,
                   cancellation: Cancellation | None = None) -> CommandOutcome:
    """Use only caller-supplied explicit registries; credentials stay outside manifests."""
    try:
        token = cancellation if cancellation is not None else NeverCancelled()
        _cancel(token)
        source = sources.resolve(source_id, source_config, credentials, spec.request)
        sink = sinks.resolve(sink_id, sink_config, credentials, requirements)
    except CommandError:
        raise
    except Exception:
        _fail(CommandErrorCode.CONFIG)
    return run_session(spec, source, sink, requirements=requirements, cancellation=token)
