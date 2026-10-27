"""Closed borrowed-record traversal before encoders, identity hashes or I/O."""
from __future__ import annotations
from datetime import date
import math
from typing import Callable, cast
from equity_feature_contracts import inputs as i, results as r, specs as s
import equity_feature_evidence as e
from equity_feature_io_contracts import publication as pub
import equity_feature_duckdb as a
import equity_feature_workers as w
from ._model import AcquiredInput, TaskEvidence, ProvenanceError, require

# Static public pinned record/field closure. No caller-supplied type registry.
_FIELDS: dict[type[object], tuple[str, ...]] = {
    i.PriceUnit: ('scale', 'currency'),
    i.SourceBinding: ('source_id', 'snapshot_id', 'mapping_version', 'input_id'),
    i.Coverage: ('expected', 'observed', 'complete'),
    i.AdjustmentSpec: ('basis', 'policy_version', 'action_snapshot', 'anchor'),
    i.InputScope: ('start_ns', 'end_ns', 'eligibility_policy', 'include_opening_auction', 'include_closing_auction'),
    i.IntervalCoverage: ('name', 'start_ns', 'end_ns', 'coverage'),
    i.BatchMetadata: ('namespace', 'source', 'coverage', 'price_unit', 'adjustment', 'sampling', 'quantity_unit', 'ordering', 'duplicate_policy', 'scope', 'interval_coverage'),
    i.Column: ('name', 'values'),
    i.CanonicalBatch: ('kind', 'columns', 'metadata'),
    s.IntervalSpec: ('name', 'start_ns', 'end_ns'),
    s.SessionSpec: ('namespace', 'session_id', 'open_ns', 'close_ns', 'timezone_label', 'intervals', 'scheduled_close_ns', 'early_close', 'include_opening_auction', 'include_closing_auction'),
    s.AvailabilitySpec: ('market_cutoff_ns', 'knowledge_cutoff_ns', 'evaluation_ns', 'mode', 'reconstruction_reason'),
    s.WindowSpec: ('count', 'target_session_id', 'governed_sessions', 'anchor'),
    s.Parameter: ('name', 'value'),
    s.ConfigSpec: ('identity', 'algorithm_version', 'parameters', 'session', 'window', 'availability', 'adjustment', 'price_unit', 'schema_version'),
    r.EntityKey: ('instrument_id', 'session_id'),
    r.FeatureColumn: ('feature_id', 'algorithm_version', 'dtype', 'unit', 'entities', 'values', 'schema_version'),
    r.QualityRow: ('entity', 'feature_id', 'status', 'expected', 'observed', 'reasons'),
    r.InputBinding: ('role', 'kind', 'metadata', 'schema_version'),
    r.ResultMetadata: ('namespace', 'session_id', 'availability', 'config_digest', 'inputs', 'backend_id', 'backend_version', 'evidence_limit', 'schema_version', 'math_policy_version'),
    r.EvidenceRow: ('entity', 'feature_id', 'input_id', 'row_id', 'event_ns', 'known_at_ns', 'effective_start_ns', 'effective_end_ns', 'use', 'exclusion_reason', 'boundary'),
    r.FeatureResult: ('values', 'quality', 'metadata', 'evidence'),
    e.ContentFingerprint: ('domain', 'digest', 'byte_length', 'schema_version', 'canonicalization', 'algorithm'),
    e.InputFingerprint: ('role', 'input_id', 'content'),
    e.FormulaBinding: ('feature_id', 'formula_version'),
    e.PackageVersion: ('name', 'version'),
    e.EnvironmentDeclaration: ('python_implementation', 'python_version', 'platform', 'packages'),
    e.ExecutionClaim: ('executed_at_ns', 'clock_source', 'uncertainty_ns'),
    e.EvidenceDisclosure: ('state', 'retained_count', 'total_count'),
    e.ExecutionReceipt: ('metadata', 'configuration', 'inputs', 'output', 'formulas', 'environment', 'execution', 'evidence', 'schema_version', 'canonicalization', 'domain'),
    pub.FeatureHeader: ('feature_id', 'algorithm_version', 'dtype', 'unit', 'schema_version'),
    pub.ResultDescriptor: ('metadata', 'features'),
    pub.PublicationIdentity: ('protocol_version', 'codec_version', 'identity_version', 'digest_version', 'canonical_package_version', 'canonical_schema_version', 'math_policy_version', 'destination_scope', 'generation_id', 'job_id', 'partition_id', 'result_descriptors', 'supersedes_key'),
    pub.PublicationEnvelope: ('protocol_version', 'codec_version', 'identity_version', 'digest_version', 'canonical_package_version', 'canonical_schema_version', 'math_policy_version', 'destination_scope', 'generation_id', 'job_id', 'partition_id', 'result_descriptors', 'supersedes_key', 'expected_content_sha256', 'result_count', 'cell_count', 'evidence_count', 'content_bytes', 'max_results', 'max_chunk_bytes', 'max_total_bytes', 'max_result_cells', 'max_evidence_rows', 'caller_created_at_ns'),
    pub.ArtifactReference: ('artifact_id', 'byte_sha256', 'byte_length'),
    pub.CompletionReceipt: ('identity', 'idempotency_key', 'content_sha256', 'result_count', 'cell_count', 'evidence_count', 'content_bytes', 'artifacts', 'caller_committed_at_ns'),
    a.FileEvidence: ('partition', 'observed_original_sha256', 'verified_optimized_sha256'),
    a.AcquisitionReceipt: ('request_digest', 'configuration_digest', 'resolved_digest', 'input_evidence_digest', 'catalog_sha256', 'files', 'missing_sessions', 'source', 'normalization', 'coverage', 'availability', 'calendar_version', 'disposition', 'rows', 'pin_strength', 'hash_bytes', 'schema', 'adapter_version', 'canonical_schema'),
    a.PriceConversion: ('field', 'interpretation', 'rounding', 'rounded_cells'),
    a.MappingReport: ('version', 'digest', 'rows', 'price_conversions', 'unmapped_fields', 'interval_clock', 'eligibility_policy', 'eligibility_source'),
    a.ResolvedPartition: ('session', 'original_path', 'optimized_path', 'original_bytes', 'optimized_bytes', 'declared_rows', 'schema_sha256', 'admission', 'original_dataset', 'substituted_dataset', 'original_sha256', 'optimized_sha256', 'receipt_id'),
    w.InputManifest: ('role', 'acquisition_sha256', 'revision_id', 'state', 'binding', 'known_at_ns'),
    w.TaskManifest: ('job_id', 'generation_id', 'partition_id', 'family', 'instruments', 'config', 'features', 'inputs', 'governed_sessions', 'destination_scope', 'ownership', 'max_input_batches', 'max_input_bytes', 'ordered_history', 'warmup_sessions', 'initialization_sha256', 'merge_policy', 'protocol_version', 'canonical_package_version', 'canonical_schema_version', 'math_policy_version'),
    w.OutputManifest: ('task', 'envelope', 'receipt'),
    AcquiredInput: ('role', 'receipt', 'batch'),
    TaskEvidence: ('expected_task', 'acquired', 'result', 'receipt', 'output'),
}

_ENUMS = (i.DataKind, i.DType, r.Status, r.Reason, r.ValueType)


def _seq(value: object, maximum: int) -> None:
    require(type(value) in (tuple, list))
    assert isinstance(value, (tuple, list))
    require(len(value) <= maximum, 'limit_exceeded')


def _count(value: object) -> None:
    require(type(value) is int)
    assert isinstance(value, int)
    require(0 <= value <= i.I64_MAX)


def _digest(value: object, optional: bool = False) -> None:
    if optional and value is None: return
    require(type(value) is str)
    assert isinstance(value, str)
    require(len(value) == 64 and all(c in '0123456789abcdef' for c in value))


def _date(value: object) -> None:
    require(type(value) is str)
    assert isinstance(value, str)
    require(len(value) == 10)
    valid = False
    try: valid = date.fromisoformat(value).isoformat() == value
    except ValueError: pass
    require(valid)


def _text(value: object, path: bool = False, optional: bool = False) -> None:
    if optional and value is None: return
    require(type(value) is str)
    assert isinstance(value, str)
    require(len(value) <= (4096 if path else 128), 'limit_exceeded')
    require(bool(value.strip()))


def _fields(value: object) -> tuple[tuple[str, object], ...]:
    """Only literal keys of an exact pinned record may be reflected."""
    names = _FIELDS.get(type(value))
    require(names is not None)
    assert names is not None
    return tuple((name, getattr(value, name)) for name in names)


def _caps(value: object) -> None:
    """Field cardinalities precede traversal, copying and property hashing."""
    if type(value) is a.AcquisitionReceipt:
        _seq(value.files, 32); _seq(value.missing_sessions, 128)
        require(type(value.schema) is str and value.schema == 'acquisition1', 'unsupported_version')
        require(type(value.adapter_version) is str and value.adapter_version == '0.1.0a9', 'unsupported_version')
        require(type(value.canonical_schema) is int and value.canonical_schema == 1, 'unsupported_version')
        for digest in (value.request_digest, value.configuration_digest, value.resolved_digest,
                value.input_evidence_digest, value.catalog_sha256): _digest(digest)
        _count(value.rows); _count(value.hash_bytes)
        require(type(value.source) is i.SourceBinding and type(value.coverage) is i.Coverage and type(value.availability) is s.AvailabilitySpec)
        require(value.normalization is None or type(value.normalization) is a.MappingReport)
        require(type(value.disposition) is str and value.disposition in ('data','missing'))
        require(type(value.pin_strength) is str and value.pin_strength in ('no_selected_files','all_selected_originals_pinned','observed_without_all_original_pins'))
        require(all(type(f) is a.FileEvidence for f in value.files))
        # Fixed primitive partition identity, validated before set/hash use.
        identities = []
        for item in value.files:
            require(type(item.partition) is a.ResolvedPartition)
            partition = item.partition
            _date(partition.session); _text(partition.original_path, True)
            _text(partition.optimized_path, True)
            identities.append((partition.session, partition.original_path, partition.optimized_path))
        require(len(set(identities)) == len(identities))
        for session in value.missing_sessions: _date(session)
        require(len(set(value.missing_sessions)) == len(value.missing_sessions))
    elif type(value) is a.FileEvidence:
        require(type(value.partition) is a.ResolvedPartition)
        _digest(value.observed_original_sha256); _digest(value.verified_optimized_sha256, True)
    elif type(value) is a.ResolvedPartition:
        _date(value.session)
        _text(value.original_path, True); _text(value.optimized_path, True)
        _text(value.admission); _text(value.original_dataset)
        _text(value.substituted_dataset, optional=True); _text(value.receipt_id, optional=True)
        for n in (value.original_bytes, value.optimized_bytes, value.declared_rows): _count(n)
        _digest(value.schema_sha256); _digest(value.original_sha256, True); _digest(value.optimized_sha256, True)
    elif type(value) is a.MappingReport:
        require(type(value.version) is str and value.version == 'retained-map1', 'unsupported_version')
        _digest(value.digest); _count(value.rows)
        _seq(value.price_conversions, 16); _seq(value.unmapped_fields, 64)
        require(all(type(v) is a.PriceConversion for v in value.price_conversions))
        for text in value.unmapped_fields: _text(text)
        _text(value.interval_clock); _text(value.eligibility_source)
        _text(value.eligibility_policy, optional=True)
    elif type(value) is a.PriceConversion:
        _count(value.rounded_cells)
        _text(value.field); _text(value.interpretation); _text(value.rounding)
    elif type(value) is i.CanonicalBatch:
        require(type(value.kind) is i.DataKind and value.kind is i.DataKind.TRADE, 'unsupported_version')
        _seq(value.columns, 16)
        require(all(type(c) is i.Column for c in value.columns))
        for column in value.columns: _seq(column.values, 512)
        require(sum(len(c.values) for c in value.columns) <= 8192, 'limit_exceeded')
    elif type(value) is w.TaskManifest:
        require(type(value.family) is str and value.family == 'trades', 'unsupported_version')
        require(all(type(v) is str for v in (value.protocol_version, value.canonical_package_version,
            value.canonical_schema_version, value.math_policy_version)), 'unsupported_version')
        require((value.protocol_version, value.canonical_package_version, value.canonical_schema_version,
            value.math_policy_version) == ('efworker-task1','0.0.4a4','1','v1'), 'unsupported_version')
        _seq(value.features, 5); _seq(value.inputs, 1); _seq(value.instruments, 1)
        require(len(value.features) == 5 and len(value.inputs) == 1 and len(value.instruments) == 1, 'unsupported_version')
        _seq(value.governed_sessions, 128); _seq(value.warmup_sessions, 128)
    elif type(value) is s.ConfigSpec:
        _seq(value.parameters, 64)
        require(all(type(p) is s.Parameter for p in value.parameters))
        names = tuple(p.name for p in value.parameters)
        require(all(type(n) is str for n in names))
        require(names == tuple(sorted(set(names))))
    elif type(value) is s.SessionSpec: _seq(value.intervals, 128)
    elif type(value) is s.WindowSpec: _seq(value.governed_sessions, 128)
    elif type(value) is r.FeatureResult:
        _seq(value.values, 5); _seq(value.quality, 5); _seq(value.evidence, 512)
        require(len(value.values) == 5 and len(value.quality) == 5, 'unsupported_version')
    elif type(value) is r.FeatureColumn:
        _seq(value.entities, 1); _seq(value.values, 1)
        require(len(value.entities) == len(value.values) == 1)
    elif type(value) is r.ResultMetadata: _seq(value.inputs, 1)
    elif type(value) is i.BatchMetadata: _seq(value.interval_coverage, 128)
    elif type(value) is e.ExecutionReceipt:
        _seq(value.inputs, 1); _seq(value.formulas, 5)
    elif type(value) is e.EnvironmentDeclaration: _seq(value.packages, 64)
    elif type(value) is pub.PublicationEnvelope:
        _seq(value.result_descriptors, 1)
        _count(value.result_count); _count(value.cell_count); _count(value.evidence_count)
        require(value.result_count == 1, 'unsupported_version')
        _count(value.content_bytes)
        require(value.content_bytes <= 1048576, 'limit_exceeded')
        require(value.cell_count <= 8192 and value.evidence_count <= 512, 'limit_exceeded')
    elif type(value) is pub.ResultDescriptor: _seq(value.features, 5)
    elif type(value) is pub.CompletionReceipt: _seq(value.artifacts, 16)
    elif type(value) is TaskEvidence: _seq(value.acquired, 1)


class _Budget:
    def __init__(self) -> None:
        self.nodes = 0
        self.units = 0

    def walk(self, value: object, depth: int = 0, *, path: bool = False, generated_id: bool = False) -> None:
        self.nodes += 1; self.units += 64
        require(depth <= 16 and self.nodes <= 32768 and self.units <= 1048576, 'limit_exceeded')
        if value is None or type(value) is bool: return
        if type(value) is int:
            assert isinstance(value, int)
            require(-(10**38) < value < 10**38)
            return
        if type(value) is float:
            assert isinstance(value, float)
            require(math.isfinite(value))
            return
        if type(value) in _ENUMS:
            assert isinstance(value, (i.DataKind, i.DType, r.Status, r.Reason, r.ValueType))
            self.walk(value.value, depth + 1); return
        if type(value) is str:
            assert isinstance(value, str)
            require(len(value) <= (4096 if path else 256 if generated_id else 128), 'limit_exceeded')
            require(not any(ord(c) < 32 or 127 <= ord(c) <= 159 or 0xD800 <= ord(c) <= 0xDFFF for c in value))
            require(len(value.encode('utf-8')) <= (16384 if path else 1024 if generated_id else 512), 'limit_exceeded')
            self.units += 12 * len(value)
            require(self.units <= 1048576, 'limit_exceeded')
            return
        if type(value) in (tuple, list):
            assert isinstance(value, (tuple, list))
            require(len(value) <= 4096, 'limit_exceeded')
            for child in value: self.walk(child, depth + 1)
            return
        _caps(value)
        for name, child in _fields(value):
            self.walk(name, depth + 1)
            self.walk(child, depth + 1,
                path=type(value) is a.ResolvedPartition and name in ('original_path','optimized_path'),
                generated_id=(type(value) is i.SourceBinding and name in ('mapping_version','input_id'))
                    or (type(value) is e.InputFingerprint and name == 'input_id'))


def preflight(evidence: tuple[TaskEvidence, ...], extras: tuple[tuple[object, ...], ...] = ()) -> None:
    require(type(evidence) in (tuple, list))
    require(1 <= len(evidence) <= 16, 'limit_exceeded')
    require(not extras or len(extras) == len(evidence))
    total_nodes = total_units = total_cells = 0
    for index, value in enumerate(evidence):
        require(type(value) is TaskEvidence)
        budget = _Budget(); budget.walk(value)
        if extras:
            for extra in extras[index]: budget.walk(extra)
        total_nodes += budget.nodes; total_units += budget.units
        total_cells += sum(len(c.values) for acquired in value.acquired if acquired.batch is not None for c in acquired.batch.columns)
        require(total_nodes <= 524288 and total_units <= 16777216 and total_cells <= 131072, 'limit_exceeded')


def _clone(value: object) -> object:
    if type(value) in (tuple, list):
        assert isinstance(value, (tuple, list))
        return tuple(_clone(v) for v in value)
    if type(value) not in _FIELDS: return value
    skip = ('schema','adapter_version','canonical_schema') if type(value) is a.AcquisitionReceipt else ()
    values = {name:_clone(child) for name,child in _fields(value) if name not in skip}
    factory = cast(Callable[..., object], type(value))
    return factory(**values)


def freeze_acquired(receipt: a.AcquisitionReceipt, batch: i.CanonicalBatch | None) -> tuple[a.AcquisitionReceipt, i.CanonicalBatch | None]:
    budget = _Budget(); budget.walk(receipt); budget.walk(batch)
    try:
        return cast(a.AcquisitionReceipt, _clone(receipt)), cast(i.CanonicalBatch | None, _clone(batch))
    except ProvenanceError: raise
    except Exception: pass
    raise ProvenanceError()


def freeze_task(value: TaskEvidence) -> tuple[w.TaskManifest, tuple[AcquiredInput, ...], r.FeatureResult, e.ExecutionReceipt, w.OutputManifest | None]:
    preflight((value,))
    try:
        return (cast(w.TaskManifest, _clone(value.expected_task)),
            tuple(AcquiredInput(a.role, a.receipt, a.batch) for a in value.acquired),
            cast(r.FeatureResult, _clone(value.result)), cast(e.ExecutionReceipt, _clone(value.receipt)),
            cast(w.OutputManifest | None, _clone(value.output)))
    except ProvenanceError: raise
    except Exception: pass
    raise ProvenanceError()
