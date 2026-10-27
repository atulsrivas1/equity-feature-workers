"""Outer provenance values; supplied records never attest their own origin."""
from __future__ import annotations
from dataclasses import dataclass, field
from equity_feature_contracts import FeatureResult, CanonicalBatch
from equity_feature_evidence import ExecutionReceipt, ContentFingerprint
from equity_feature_duckdb import AcquisitionReceipt
from equity_feature_workers import TaskManifest, OutputManifest


class ProvenanceError(ValueError):
    """Closed payload-free errors, including explicit cooperative cancellation."""
    def __init__(self, code: str = 'invalid_schema') -> None:
        if type(code) is not str or code not in ('invalid_schema','unsupported_version',
                'missing_provenance','inconsistent_binding','limit_exceeded',
                'readback_failed','cancelled'):
            code = 'invalid_schema'
        self.code = code
        super().__init__('Provenance rejected: ' + code)


def require(ok: bool, code: str = 'invalid_schema') -> None:
    if not ok: raise ProvenanceError(code)


def _label(value: str) -> None:
    require(type(value) is str)
    require(len(value) <= 128, 'limit_exceeded')
    require(bool(value.strip()) and not any(ord(c) < 32 or 127 <= ord(c) <= 159 or 0xD800 <= ord(c) <= 0xDFFF for c in value))
    require(len(value.encode('utf-8')) <= 512, 'limit_exceeded')


def _hex(value: str) -> None:
    require(type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value))


def _fingerprint(value: ContentFingerprint, domain: str) -> None:
    require(type(value) is ContentFingerprint)
    require(type(value.domain) is str and value.domain == domain)
    _hex(value.digest)
    require(type(value.byte_length) is int and 0 <= value.byte_length <= 2**63-1)
    for v, expected in ((value.schema_version,'1'),(value.algorithm,'sha256'),(value.canonicalization,'typed-json-v1')):
        require(type(v) is str and v == expected, 'unsupported_version')


@dataclass(frozen=True)
class AcquiredInput:
    role: str
    receipt: AcquisitionReceipt
    batch: CanonicalBatch | None
    def __post_init__(self) -> None:
        require(type(self.role) is str and self.role == 'trades', 'unsupported_version')
        require(type(self.receipt) is AcquisitionReceipt, 'missing_provenance')
        require(self.batch is None or type(self.batch) is CanonicalBatch)
        # Bound the complete borrowed graph before any constructor/encoder clone.
        from ._preflight import freeze_acquired
        receipt, batch = freeze_acquired(self.receipt, self.batch)
        object.__setattr__(self, 'receipt', receipt)
        object.__setattr__(self, 'batch', batch)


@dataclass(frozen=True)
class TaskEvidence:
    expected_task: TaskManifest
    acquired: tuple[AcquiredInput, ...]
    result: FeatureResult
    receipt: ExecutionReceipt
    output: OutputManifest | None = None
    def __post_init__(self) -> None:
        require(type(self.expected_task) is TaskManifest and type(self.result) is FeatureResult)
        require(type(self.receipt) is ExecutionReceipt, 'missing_provenance')
        require(type(self.acquired) in (tuple, list))
        require(len(self.acquired) > 0, 'missing_provenance')
        require(len(self.acquired) == 1)
        require(all(type(a) is AcquiredInput for a in self.acquired))
        require(self.output is None or type(self.output) is OutputManifest)
        from ._preflight import freeze_task
        task, acquired, result, receipt, output = freeze_task(self)
        object.__setattr__(self, 'expected_task', task)
        object.__setattr__(self, 'acquired', acquired)
        object.__setattr__(self, 'result', result)
        object.__setattr__(self, 'receipt', receipt)
        object.__setattr__(self, 'output', output)


@dataclass(frozen=True)
class InputLink:
    role: str
    acquisition_sha256: str
    revision_id: str
    state: str
    content: ContentFingerprint | None
    missing_sessions: tuple[str, ...]
    def __post_init__(self) -> None:
        require(type(self.role) is str and self.role == 'trades', 'unsupported_version')
        _hex(self.acquisition_sha256); _label(self.revision_id)
        require(type(self.state) is str and self.state in ('missing','empty','partial','complete'))
        if self.content is not None: _fingerprint(self.content, 'canonical_batch')
        require((self.content is None) == (self.state == 'missing'))
        require(type(self.missing_sessions) in (tuple, list))
        require(len(self.missing_sessions) <= 128, 'limit_exceeded')
        for value in self.missing_sessions: _label(value)
        object.__setattr__(self, 'missing_sessions', tuple(self.missing_sessions))


@dataclass(frozen=True)
class ProvenanceReport:
    task_sha256: str
    input_links: tuple[InputLink, ...]
    configuration: ContentFingerprint
    output: ContentFingerprint
    verification_mode: str = field(default='supplied', init=False)
    generation_key: str | None = field(default=None, init=False)
    generation_identity: str | None = field(default=None, init=False)
    generation_status: str = field(default='not_checked', init=False)
    operational_readback: bool = field(default=False, init=False)
    source_authenticated: bool = field(default=False, init=False)
    historical_acquisition_verified: bool = field(default=False, init=False)
    def __post_init__(self) -> None:
        _hex(self.task_sha256)
        require(type(self.input_links) in (tuple, list) and len(self.input_links) == 1)
        require(all(type(link) is InputLink for link in self.input_links))
        _fingerprint(self.configuration, 'configuration'); _fingerprint(self.output, 'feature_result')
        object.__setattr__(self, 'input_links', tuple(self.input_links))


@dataclass(frozen=True)
class GenerationProvenanceReport:
    generation_key: str
    generation_identity: str
    expected_tasks: tuple[str, ...]
    task_reports: tuple[ProvenanceReport, ...]
    verification_mode: str = field(default='live', init=False)
    generation_status: str = field(default='waiting', init=False)
    operational_readback: bool = field(default=False, init=False)
    source_authenticated: bool = field(default=False, init=False)
    historical_acquisition_verified: bool = field(default=False, init=False)
    def __post_init__(self) -> None:
        _hex(self.generation_key); _hex(self.generation_identity)
        require(type(self.expected_tasks) in (tuple, list) and type(self.task_reports) in (tuple, list))
        require(1 <= len(self.expected_tasks) <= 16 and len(self.task_reports) <= 16, 'limit_exceeded')
        require(len(self.expected_tasks) == len(self.task_reports))
        for task in self.expected_tasks: _hex(task)
        require(len(set(self.expected_tasks)) == len(self.expected_tasks))
        require(all(type(report) is ProvenanceReport for report in self.task_reports))
        require(tuple(report.task_sha256 for report in self.task_reports) == tuple(self.expected_tasks))
        object.__setattr__(self, 'expected_tasks', tuple(self.expected_tasks))
        object.__setattr__(self, 'task_reports', tuple(self.task_reports))
