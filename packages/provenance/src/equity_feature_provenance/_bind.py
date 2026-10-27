"""Supplied content consistency and a separate fixed operational readback path."""
from __future__ import annotations
from dataclasses import asdict
import json
from typing import Callable, TypeVar
from equity_feature_contracts import inputs as i, results as r, builtin_registry
from equity_feature_evidence import canonical_content, encode_receipt, verify_receipt_content
from equity_feature_io_contracts import FeatureHeader, SinkError, SinkErrorCode
from equity_feature_io_sdk import Cancellation, descriptor, verify_receipt
from equity_feature_duckdb_sink import DuckDBSink
from equity_feature_workers import (GenerationStore, GenerationSpec, Dependency,
    GenerationOutcome, BarrierOutcome, VerifiedDependency, WaitingDependency,
    CommandOutcome, CommandError, CommandErrorCode, encode_task, encode_output)
from ._model import (TaskEvidence, InputLink, ProvenanceReport, GenerationProvenanceReport,
    ProvenanceError, require)
from ._preflight import preflight, _Budget

_T = TypeVar('_T')
_IDS = ('session.trade.count','session.trade.volume','session.trade.notional',
        'session.trade.vwap','session.trade.mean_size')
_REGISTRY = builtin_registry()
_HEADERS = tuple(FeatureHeader(name, 'v1', r.ValueType(_REGISTRY.get(name).outputs[0].dtype),
    _REGISTRY.get(name).outputs[0].unit, '1') for name in _IDS)


def _guard(operation: Callable[[], _T], code: str = 'inconsistent_binding') -> _T:
    try: return operation()
    except ProvenanceError: raise
    except Exception: pass
    # Raised after the handler so the caller's payload is not chained.
    raise ProvenanceError(code)


def _size(value: TaskEvidence) -> int:
    task = value.expected_task
    parts = [encode_task(task), canonical_content(task.config), canonical_content(value.result),
        encode_receipt(value.receipt)]
    for acquired in value.acquired:
        parts.append(json.dumps(asdict(acquired.receipt), sort_keys=True, separators=(',',':'),
            ensure_ascii=True, allow_nan=False).encode('utf-8'))
        if acquired.batch is not None: parts.append(canonical_content(acquired.batch))
    if value.output is not None: parts.append(encode_output(value.output))
    size = sum(len(part) for part in parts)
    require(size <= 1048576, 'limit_exceeded')
    return size


def _bind(value: TaskEvidence) -> ProvenanceReport:
    task = value.expected_task
    require(task.features == _HEADERS, 'unsupported_version')
    require(task.config.availability.mode == 'known_at', 'inconsistent_binding')
    require(task.config.adjustment == i.AdjustmentSpec(), 'unsupported_version')
    require(len(task.inputs) == len(value.acquired) == 1, 'missing_provenance')
    manifest, acquired = task.inputs[0], value.acquired[0]
    require(manifest.role == acquired.role == 'trades', 'unsupported_version')
    acquisition = acquired.receipt
    require(acquisition.identity_digest == manifest.acquisition_sha256, 'inconsistent_binding')
    require(acquisition.availability == task.config.availability, 'inconsistent_binding')
    require(type(manifest.revision_id) is str and bool(manifest.revision_id.strip()))
    require(descriptor(value.result).features == task.features, 'inconsistent_binding')
    expected_entity = r.EntityKey(task.instruments[0], task.config.session.session_id)
    require(all(c.entities == (expected_entity,) for c in value.result.values), 'inconsistent_binding')
    batch = acquired.batch
    if manifest.state == 'missing':
        require(batch is None and manifest.binding is None and acquisition.disposition == 'missing'
            and acquisition.rows == 0 and acquisition.normalization is None, 'inconsistent_binding')
        supplied: tuple[tuple[str, i.CanonicalBatch], ...] = ()
        fingerprint = None
        require(not value.receipt.inputs, 'inconsistent_binding')
    else:
        require(batch is not None and manifest.binding is not None, 'missing_provenance')
        assert batch is not None and manifest.binding is not None
        require(acquisition.disposition == 'data' and acquisition.normalization is not None, 'inconsistent_binding')
        assert acquisition.normalization is not None
        require(batch.metadata == manifest.binding.metadata and batch.kind == manifest.binding.kind
            and batch.metadata.namespace == task.config.session.namespace, 'inconsistent_binding')
        require(acquisition.source == batch.metadata.source and acquisition.coverage == batch.metadata.coverage
            and acquisition.rows == batch.row_count == batch.metadata.coverage.observed
            and acquisition.normalization.rows == batch.row_count, 'inconsistent_binding')
        require(batch.metadata.source.mapping_version.endswith(':retained-map1:'+acquisition.normalization.digest)
            and batch.metadata.source.input_id.endswith(':mapped:'+acquisition.normalization.digest), 'inconsistent_binding')
        require(len(value.receipt.inputs) == 1 and value.receipt.inputs[0].role == manifest.role
            and value.receipt.inputs[0].input_id == batch.metadata.source.input_id, 'inconsistent_binding')
        fingerprint = value.receipt.inputs[0].content  # Retained expected anchor; never replaced by observed rehash.
        supplied = ((manifest.role, batch),)
        require(len(canonical_content(batch)) <= task.max_input_bytes and task.max_input_batches >= 1, 'limit_exceeded')
    require(verify_receipt_content(value.receipt, task.config, supplied, value.result), 'inconsistent_binding')
    if value.output is not None:
        require(value.output.task == task, 'inconsistent_binding')
        require(value.output.envelope.result_descriptors == (descriptor(value.result),), 'inconsistent_binding')
        if value.output.receipt is not None:
            verify_receipt(value.output.receipt, value.output.envelope, (value.result,))
    link = InputLink(manifest.role, manifest.acquisition_sha256, manifest.revision_id,
        manifest.state, fingerprint, tuple(acquisition.missing_sessions))
    return ProvenanceReport(task.task_sha256, (link,), value.receipt.configuration, value.receipt.output)


def bind_supplied_provenance(evidence: TaskEvidence) -> ProvenanceReport:
    """Compare complete supplied originals against the caller's retained anchors."""
    preflight((evidence,))
    evidence = TaskEvidence(evidence.expected_task, evidence.acquired, evidence.result,
        evidence.receipt, evidence.output)
    _guard(lambda: _size(evidence))
    return _guard(lambda: _bind(evidence))


def verify_live_generation(store: GenerationStore, spec: GenerationSpec,
        dependencies: tuple[Dependency, ...], evidence: tuple[TaskEvidence, ...], *,
        cancellation: Cancellation | None = None) -> GenerationProvenanceReport:
    """Explicit outer I/O; only actual accepted full readback can mark completion."""
    require(type(store) is GenerationStore and type(spec) is GenerationSpec)
    require(type(dependencies) in (tuple, list) and type(evidence) in (tuple, list))
    require(type(spec.tasks) is tuple)
    require(1 <= len(spec.tasks) <= 16 and len(dependencies) <= 16 and len(evidence) <= 16, 'limit_exceeded')
    require(len(spec.tasks) == len(dependencies) == len(evidence), 'inconsistent_binding')
    require(all(type(d) is Dependency for d in dependencies))
    for dependency in dependencies:
        require(type(dependency.required) is bool and dependency.required
            and type(dependency.optional_policy) is str and dependency.optional_policy == 'wait', 'unsupported_version')
        if dependency.output is None: require(dependency.sink is None, 'unsupported_version')
        else: require(type(dependency.sink) is DuckDBSink, 'unsupported_version')
    # Also bound declared spec/dependency tasks and output graphs before equality or hashes.
    extras = tuple((task, dependency.task, dependency.output, dependency.instance_id)
                   for task, dependency in zip(spec.tasks, dependencies, strict=True))
    preflight(evidence, extras)
    evidence = tuple(TaskEvidence(item.expected_task, item.acquired, item.result,
        item.receipt, item.output) for item in evidence)
    labels = _Budget()
    for text in (spec.namespace, spec.job_id, spec.generation_id):
        require(type(text) is str); labels.walk(text)
    require(len({d.instance_id for d in dependencies}) == len(dependencies), 'inconsistent_binding')
    require(all(task == observed.expected_task == dependency.task
        for task, observed, dependency in zip(spec.tasks, evidence, dependencies, strict=True)), 'inconsistent_binding')
    partitions = tuple(item.expected_task.partition_id for item in evidence)
    entities = tuple((item.expected_task.config.session.namespace, item.expected_task.instruments[0],
        item.expected_task.config.session.session_id) for item in evidence)
    require(len(set(partitions)) == len(partitions) and len(set(entities)) == len(entities))
    require(all(dependency.output == observed.output for dependency, observed in zip(dependencies, evidence, strict=True)), 'inconsistent_binding')
    sizes = tuple(_guard(lambda: _size(item)) for item in evidence)
    reports = tuple(_guard(lambda: _bind(item)) for item in evidence)
    task_ids = tuple(report.task_sha256 for report in reports)
    require(len(set(task_ids)) == len(task_ids), 'inconsistent_binding')
    require(all((t.config.session.namespace,t.job_id,t.generation_id) == (spec.namespace,spec.job_id,spec.generation_id) for t in spec.tasks), 'inconsistent_binding')
    generation_bytes = json.dumps([spec.key,spec.identity,task_ids,
        tuple(d.instance_id for d in dependencies)], separators=(',',':')).encode('utf-8')
    require(sum(sizes) + len(generation_bytes) <= 16777216, 'limit_exceeded')
    outcome = None; failure = 'readback_failed'
    try:
        outcome = store.read(spec, tuple(dependencies), cancellation=cancellation)
    except SinkError as error:
        if error.code is SinkErrorCode.CANCELLED: failure = 'cancelled'
    except CommandError as error:
        if error.code is CommandErrorCode.CANCELLED: failure = 'cancelled'
    except Exception: pass
    require(outcome is not None, failure)
    assert outcome is not None
    require(type(outcome) is GenerationOutcome and type(outcome.complete) is bool, 'readback_failed')
    result = GenerationProvenanceReport(spec.key, spec.identity, task_ids, reports)
    if not outcome.complete:
        if outcome.barrier is not None:
            require(type(outcome.barrier) is BarrierOutcome and type(outcome.barrier.waiting) is tuple
                and len(outcome.barrier.waiting) <= 16, 'readback_failed')
            for waiting in outcome.barrier.waiting:
                require(type(waiting) is WaitingDependency and type(waiting.reason) is str, 'readback_failed')
                require(waiting.reason in ('EXPLICIT_ABSENCE','ABSENT','STAGING','ABORTED','UNKNOWN'), 'readback_failed')
        return result
    require(type(outcome.barrier) is BarrierOutcome, 'readback_failed')
    assert outcome.barrier is not None
    require(type(outcome.barrier.verified) is tuple and type(outcome.outputs) is tuple
        and type(outcome.barrier.waiting) is tuple and not outcome.barrier.waiting
        and type(outcome.barrier.omitted) is tuple and not outcome.barrier.omitted
        and len(outcome.barrier.verified) == len(task_ids)
        and len(outcome.outputs) == len(task_ids), 'readback_failed')
    for verified in outcome.barrier.verified:
        require(type(verified) is VerifiedDependency and type(verified.command) is CommandOutcome
            and type(verified.command.results) is tuple and len(verified.command.results) == 1, 'readback_failed')
    preflight(evidence, tuple((output, verified.command.output, verified.command.results[0])
        for output, verified in zip(outcome.outputs, outcome.barrier.verified, strict=True)))
    require(tuple(v.instance_id for v in outcome.barrier.verified) == tuple(d.instance_id for d in dependencies), 'inconsistent_binding')
    for expected, verified, output in zip(evidence, outcome.barrier.verified, outcome.outputs, strict=True):
        require(output.task == expected.expected_task and verified.command.output == output, 'inconsistent_binding')
        require(verified.command.results == (expected.result,) and output == expected.output, 'inconsistent_binding')
        require(output.receipt is not None, 'readback_failed')
        receipt = output.receipt
        assert receipt is not None
        _guard(lambda: verify_receipt(receipt, output.envelope, verified.command.results))
    object.__setattr__(result, 'generation_status', 'complete')
    object.__setattr__(result, 'operational_readback', True)
    return result
