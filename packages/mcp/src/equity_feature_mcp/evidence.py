"""Bounded owner-prepared evidence, separate from remote dispatch and calculations.

Importing this module requires no optional evidence/native packages. Their public
APIs are loaded only while an owner constructs a catalog, never while serving it.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields
import hashlib
import importlib
import json
import re
from typing import Any, NoReturn, cast

from equity_feature_client import ResultView

from .models import MCPProfile

KINDS = ('producer', 'receipt', 'explanation', 'diagnostics')
STATUS = ('available', 'insufficient_history', 'missing_input', 'incomplete_coverage', 'not_applicable')
REASONS = ('absent_input', 'null_field', 'observed_empty', 'no_eligible_observations',
           'missing_action_evidence', 'insufficient_history', 'governed_gap', 'unknown_availability',
           'future_knowledge', 'partial_universe', 'zero_denominator', 'empty_universe', 'ineligible', 'future_market')
LIMITS = {'producer': 131072, 'receipt': 1048576, 'explanation': 262144, 'diagnostics': 262144}
REPRESENTATIONS = {'producer': 'public_producer_json', 'receipt': 'execution_receipt',
                   'explanation': 'feature_explanation', 'diagnostics': 'equity.mcp.diagnostics.v1'}
CHUNK = 4096


def _fail(code: str = 'evidence_invalid') -> NoReturn:
    raise ValueError(code) from None


def _require(condition: bool, code: str = 'evidence_invalid') -> None:
    if not condition:
        _fail(code)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')


def _label(value: object) -> None:
    _require(type(value) is str)
    text = cast(str, value)
    _require(bool(text.strip()) and len(text) <= 128)
    _require(not any(ord(c) < 32 or 127 <= ord(c) <= 159 or 0xD800 <= ord(c) <= 0xDFFF for c in text))
    _require(len(text.encode('utf-8')) <= 512)


def _tree(value: object, depth: int = 0) -> int:
    _require(depth <= 20, 'evidence_bounds')
    count = 1
    if type(value) is dict:
        for key, child in cast(dict[str, object], value).items():
            count += _tree(key, depth + 1) + _tree(child, depth + 1)
            _require(count <= 32768, 'evidence_bounds')
    elif type(value) is list:
        for child in value:
            count += _tree(child, depth + 1)
            _require(count <= 32768, 'evidence_bounds')
    else:
        _require(value is None or type(value) in (str, int, float, bool))
    return count


def _bounded(raw: bytes, limit: int) -> bytes:
    _require(type(raw) is bytes)
    _require(0 < len(raw) <= limit, 'evidence_bounds')
    _require(all(32 <= c <= 126 for c in raw))
    value = json.loads(raw)
    _tree(value)
    _require(_canonical(value) == raw)
    return raw


def _dependencies() -> dict[str, Any]:
    try:
        contracts = importlib.import_module('equity_feature_contracts')
        results = importlib.import_module('equity_feature_contracts.results')
        convert_result = importlib.import_module('equity_feature_client.native').convert_result
        receipts = importlib.import_module('equity_feature_evidence')
        explanations = importlib.import_module('equity_feature_explanations')
        diagnostics = importlib.import_module('equity_feature_diagnostics')
    except ImportError:
        _fail('evidence_dependency')
    return dict(contracts=contracts, results=results, convert_result=convert_result,
                receipts=receipts, explanations=explanations, diagnostics=diagnostics)


def _configuration(value: object, deps: dict[str, Any]) -> Any:
    """Own and validate every public component before digest/conversion work."""
    c = deps['contracts']
    _require(type(value) is c.ConfigSpec)
    v = cast(Any, value)
    text_bytes = 0

    def text(raw: object) -> str:
        nonlocal text_bytes
        _require(type(raw) is str)
        s = cast(str, raw)
        _require(len(s) <= 65536, 'evidence_bounds')
        text_bytes += len(s.encode('utf-8'))
        _require(text_bytes <= 65536, 'evidence_bounds')
        return s

    def scalar(raw: object) -> Any:
        _require(raw is None or type(raw) in (str, int, float, bool))
        if type(raw) is str:
            return text(raw)
        if type(raw) is int:
            _require(raw.bit_length() <= 4096, 'evidence_bounds')
        return raw

    _require(type(v.parameters) in (tuple, list))
    _require(len(v.parameters) <= 256, 'evidence_bounds')
    _require(type(v.session) is c.SessionSpec and type(v.window) is c.WindowSpec
             and type(v.availability) is c.AvailabilitySpec and type(v.adjustment) is c.AdjustmentSpec
             and (v.price_unit is None or type(v.price_unit) is c.PriceUnit))
    s = v.session
    w = v.window
    _require(type(s.intervals) in (tuple, list) and type(w.governed_sessions) in (tuple, list))
    _require(len(s.intervals) <= 128 and len(w.governed_sessions) <= 256, 'evidence_bounds')
    parameters = []
    for p in v.parameters:
        _require(type(p) is c.Parameter)
        parameters.append(c.Parameter(text(p.name), scalar(p.value)))
    intervals = []
    for interval in s.intervals:
        _require(type(interval) is c.IntervalSpec)
        intervals.append(c.IntervalSpec(text(interval.name), interval.start_ns, interval.end_ns))
    session = c.SessionSpec(text(s.namespace), text(s.session_id), s.open_ns, s.close_ns, text(s.timezone_label),
        tuple(intervals), s.scheduled_close_ns, s.early_close, s.include_opening_auction, s.include_closing_auction)
    window = c.WindowSpec(w.count, text(w.target_session_id), tuple(text(slot) for slot in w.governed_sessions), text(w.anchor))
    a = v.availability
    availability = c.AvailabilitySpec(a.market_cutoff_ns, a.knowledge_cutoff_ns, a.evaluation_ns,
        text(a.mode), None if a.reconstruction_reason is None else text(a.reconstruction_reason))
    a = v.adjustment
    adjustment = c.AdjustmentSpec(text(a.basis), text(a.policy_version), text(a.action_snapshot), text(a.anchor))
    unit = None if v.price_unit is None else c.PriceUnit(v.price_unit.scale, text(v.price_unit.currency))
    copied = c.ConfigSpec(text(v.identity), text(v.algorithm_version), tuple(parameters), session, window,
        availability, adjustment, unit, text(v.schema_version))
    _bounded(copied.to_json().encode('ascii'), 65536)
    return copied


@dataclass(frozen=True, slots=True)
class DiagnosticAssociation:
    check_id: str
    role: str
    input_role: str | None = None
    input_id: str | None = None

    def __post_init__(self) -> None:
        _label(self.check_id)
        _require(type(self.role) is str and self.role in ('configuration', 'output', 'input'))
        if self.role == 'input':
            _label(self.input_role)
            _label(self.input_id)
        else:
            _require(self.input_role is None and self.input_id is None)


@dataclass(frozen=True, slots=True)
class EvidenceRegistration:
    """Trusted owner declarations; catalog construction performs public preparation.

The object annotations keep the default installation independent of optional
typing/runtime imports. Catalog admission requires the exact public record types.
"""
    profile_id: str
    job_id: str
    result_id: str
    view: object
    configuration: object
    receipt: object = None
    explanation: object = None
    diagnostics: object = None
    associations: tuple[DiagnosticAssociation, ...] = ()
    report_sharing_authorized: bool = False

    def __post_init__(self) -> None:
        _require(type(self.profile_id) is str and re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', self.profile_id) is not None)
        for value in (self.job_id, self.result_id):
            _require(type(value) is str and re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value) is not None)
        _require(type(self.report_sharing_authorized) is bool and self.report_sharing_authorized)
        _require(type(self.view) is ResultView and type(self.view.encoded) is bytes)
        _require(type(self.associations) is tuple)
        _require(len(self.associations) <= 256, 'evidence_bounds')
        for association in self.associations:
            _require(type(association) is DiagnosticAssociation)
            DiagnosticAssociation(association.check_id, association.role, association.input_role, association.input_id)


@dataclass(frozen=True, slots=True)
class _Entry:
    profile_id: str
    job_id: str
    result_id: str
    command_digest: str
    producer: bytes
    quality: bytes
    artifacts: tuple[tuple[str, bytes], ...]
    manifest: bytes


def _fingerprint(value: Any, deps: dict[str, Any]) -> Any:
    _require(type(value) is deps['receipts'].ContentFingerprint)
    return deps['receipts'].ContentFingerprint(value.domain, value.digest, value.byte_length,
                                             value.schema_version, value.canonicalization, value.algorithm)


def _fingerprint_json(value: Any) -> dict[str, object]:
    return dict(domain=value.domain, digest=value.digest, byte_length=str(value.byte_length),
                schema_version=value.schema_version, canonicalization=value.canonicalization, algorithm=value.algorithm)


def _diagnostic(value: Any, associations: tuple[DiagnosticAssociation, ...], result: Any,
                receipt: Any, config_fp: Any, output_fp: Any, deps: dict[str, Any]) -> bytes:
    d = deps['diagnostics']
    c = deps['contracts']
    r = deps['results']
    _require(type(value) is d.DiagnosticReport)
    _require(type(value.context) is d.DecisionContext and type(value.policy) is d.DiagnosticPolicy)
    context = value.context
    _require(type(context.entity) is r.EntityKey and type(context.availability) is c.AvailabilitySpec)
    a = context.availability
    availability = c.AvailabilitySpec(a.market_cutoff_ns, a.knowledge_cutoff_ns, a.evaluation_ns, a.mode, a.reconstruction_reason)
    copied_context = d.DecisionContext(context.namespace, r.EntityKey(context.entity.instrument_id, context.entity.session_id), availability)
    _require(copied_context.namespace == result.metadata.namespace and availability == result.metadata.availability)
    _require(any(copied_context.entity in column.entities for column in result.values))
    copied_policy = d.DiagnosticPolicy(value.policy.policy_id, value.policy.mode, value.policy.unknown_action)
    _require(type(value.findings) in (tuple, list))
    _require(1 <= len(value.findings) <= 256, 'evidence_bounds')
    copied_findings = []
    for f in value.findings:
        _require(type(f) is d.CheckFinding)
        content = None if f.content is None else _fingerprint(f.content, deps)
        copied_findings.append(d.CheckFinding(f.check_id, f.subject_id, f.kind, content, f.status,
                                             f.severity, f.reasons, f.assessed, f.unknown_prerequisites, f.age_ns))
    report = d.DiagnosticReport(copied_context, copied_policy, tuple(copied_findings), value.schema_version)
    for field in fields(report):
        if not field.init:
            _require(type(getattr(value, field.name)) is type(getattr(report, field.name))
                     and getattr(value, field.name) == getattr(report, field.name))
    by_id: dict[str, DiagnosticAssociation] = {}
    for association in associations:
        _require(association.check_id not in by_id)
        by_id[association.check_id] = association
    used: set[str] = set()
    findings = []
    for f in report.findings:
        selected = by_id.get(f.check_id)
        if f.content is None:
            _require(selected is None)
        else:
            _require(selected is not None)
            assert selected is not None
            if selected.role == 'configuration':
                expected = config_fp
            elif selected.role == 'output':
                expected = output_fp
            else:
                _require(receipt is not None)
                matches = [item for item in receipt.inputs if item.role == selected.input_role and item.input_id == selected.input_id]
                _require(len(matches) == 1)
                expected = matches[0].content
            _require(f.content == expected)
            used.add(f.check_id)
        findings.append(dict(check_id=f.check_id, subject_id=f.subject_id, kind=f.kind,
                             content=None if f.content is None else _fingerprint_json(f.content),
                             status=f.status, severity=f.severity, reasons=list(f.reasons), assessed=f.assessed,
                             unknown_prerequisites=str(f.unknown_prerequisites),
                             age_ns=None if f.age_ns is None else str(f.age_ns)))
    _require(used == set(by_id))
    projected = dict(schema_version=report.schema_version,
        context=dict(namespace=context.namespace, entity=asdict(copied_context.entity),
                     availability=dict(market_cutoff_ns=str(availability.market_cutoff_ns),
                         knowledge_cutoff_ns=str(availability.knowledge_cutoff_ns), evaluation_ns=str(availability.evaluation_ns),
                         mode=availability.mode, reconstruction_reason=availability.reconstruction_reason)),
        policy=asdict(copied_policy), findings=findings,
        **{name: getattr(report, name) for name in ('requested_count', 'passed_count', 'failed_count', 'unknown_count', 'assessed_count')},
        unknown_prerequisite_count=str(report.unknown_prerequisite_count), not_requested=list(report.not_requested),
        admission=report.admission, source_authenticated=False, historical_acquisition_verified=False)
    return _canonical(dict(representation='equity.mcp.diagnostics.v1', schema_version='1', report=projected,
                           associations=[asdict(item) for item in associations], input_content_verified=False))


def _quality(result: Any) -> bytes:
    status = dict.fromkeys(STATUS, 0)
    reasons = dict.fromkeys(REASONS, 0)
    unknown = 0
    for row in result.quality:
        _require(row.status.value in status)
        status[row.status.value] += 1
        unknown += row.expected is None
        for reason in row.reasons:
            _require(reason.value in reasons)
            reasons[reason.value] += 1
    return _canonical(dict(row_count=len(result.quality), expected_unknown_count=unknown, statuses=status, reasons=reasons))


def _descriptor(kind: str, raw: bytes) -> dict[str, object]:
    return dict(representation=REPRESENTATIONS[kind], schema_version='1', byte_length=len(raw),
                sha256=hashlib.sha256(raw).hexdigest(), input_content_verified=False,
                source_authenticated=False, execution_verified=False)


def _prepare(registration: EvidenceRegistration, profile: MCPProfile, deps: dict[str, Any]) -> _Entry:
    v = registration
    EvidenceRegistration(v.profile_id, v.job_id, v.result_id, v.view, v.configuration, v.receipt,
                         v.explanation, v.diagnostics, v.associations, v.report_sharing_authorized)
    commands = [command for command in profile.commands if command.profile_id == v.profile_id]
    _require(len(commands) == 1)
    command = commands[0]
    e = deps['receipts']
    x = deps['explanations']
    configuration = _configuration(v.configuration, deps)
    context = command.expectation.context_json()
    identity = context['config']
    availability = configuration.availability
    scope = command.expectation.scope
    _require(identity['config_id'] == configuration.identity
        and identity['schema_version'] == configuration.schema_version
        and identity['algorithm_version'] == configuration.algorithm_version
        and identity['digest'] == configuration.digest)
    _require(context['namespace'] == configuration.session.namespace
        and scope.session_id == configuration.session.session_id
        and scope.start_ns == configuration.session.open_ns
        and scope.end_ns == availability.market_cutoff_ns)
    _require(context['availability'] == dict(market_cutoff_ns=str(availability.market_cutoff_ns),
        knowledge_cutoff_ns=str(availability.knowledge_cutoff_ns), evaluation_ns=str(availability.evaluation_ns),
        mode=availability.mode, reconstruction_reason=availability.reconstruction_reason))
    original_view = cast(ResultView, v.view)
    _require(len(original_view.encoded) <= 262144, 'evidence_bounds')
    view = ResultView(original_view.encoded, original_view.filename)
    outcome = deps['convert_result'](view, command.expectation, config=configuration)
    _require(outcome.failure is None and outcome.view is not None)
    result = outcome.view
    _require(result.metadata.availability == availability)
    config_fp = e.fingerprint_config(configuration)
    output_fp = e.fingerprint_result(result)
    receipt = None
    artifacts = {'producer': _canonical(view.payload_json())}
    focus = None
    if v.receipt is not None:
        _require(type(v.receipt) is e.ExecutionReceipt)
        receipt = e.decode_receipt(e.encode_receipt(v.receipt))
        _require(receipt.configuration == config_fp and receipt.output == output_fp)
        expected = e.make_receipt(result, configuration=config_fp, inputs=receipt.inputs, output=output_fp,
            formulas=receipt.formulas, environment=receipt.environment, execution=receipt.execution, evidence=receipt.evidence)
        _require(receipt == expected)
        artifacts['receipt'] = e.encode_receipt(receipt)
    if v.explanation is not None:
        _require(receipt is not None and type(v.explanation) is x.Explanation)
        supplied = cast(Any, v.explanation)
        expected = x.explain_result(result, receipt, configuration, entity=supplied.entity,
                                    feature_id=supplied.feature_id, evidence_limit=supplied.display_limit)
        _require(x.encode_explanation(supplied) == x.encode_explanation(expected))
        artifacts['explanation'] = x.encode_explanation(expected)
        focus = dict(entity=asdict(expected.entity), feature_id=expected.feature_id, display_limit=expected.display_limit,
                     selected_retained_count=expected.selected_retained_count)
    if v.diagnostics is not None:
        artifacts['diagnostics'] = _diagnostic(v.diagnostics, v.associations, result, receipt, config_fp, output_fp, deps)
    else:
        _require(not v.associations)
    for kind, raw in artifacts.items():
        _bounded(raw, LIMITS[kind])
    quality = _quality(result)
    manifest = dict(profile_id=v.profile_id, job_id=v.job_id, result_id=v.result_id,
                    command_digest=command.expectation.command_digest, report_sharing_authorized=True,
                    artifacts={kind: _descriptor(kind, artifacts[kind]) if kind in artifacts else None for kind in KINDS},
                    quality=json.loads(quality), explanation_focus=focus)
    return _Entry(v.profile_id, v.job_id, v.result_id, command.expectation.command_digest,
                  artifacts['producer'], quality, tuple((kind, artifacts[kind]) for kind in KINDS if kind in artifacts), _canonical(manifest))


@dataclass(frozen=True, slots=True, init=False)
class EvidenceCatalog:
    """Immutable prepared bytes and deterministic logical admission, not a sandbox."""
    profile_snapshot: bytes
    snapshot: bytes
    snapshot_sha256: str
    _entries: tuple[_Entry, ...]

    def __init__(self, profile: MCPProfile, registrations: tuple[EvidenceRegistration, ...]) -> None:
        try:
            _require(type(profile) is MCPProfile and type(registrations) is tuple)
            _require(len(registrations) <= 8, 'evidence_bounds')
            _require(all(type(v) is EvidenceRegistration for v in registrations))
            rebuilt = MCPProfile(profile.owner_context_id, profile.slices, profile.commands, profile.reference_ttl_seconds)
            _require(rebuilt.snapshot == profile.snapshot and rebuilt.snapshot_sha256 == profile.snapshot_sha256)
            deps = _dependencies()
            entries = tuple(sorted((_prepare(v, rebuilt, deps) for v in registrations), key=lambda v: (v.profile_id, v.result_id, v.job_id)))
            snapshot = _canonical(dict(representation='equity.mcp.evidence.catalog.v1', schema_version='1',
                owner_context_id=rebuilt.owner_context_id, profile_sha256=rebuilt.snapshot_sha256,
                entries=[json.loads(v.manifest) for v in entries]))
            _bounded(snapshot, 65536)
            _require(len({(v.profile_id, v.result_id) for v in entries}) == len(entries)
                     and len({(v.profile_id, v.job_id) for v in entries}) == len(entries))
            total = len(snapshot) + sum(len(raw) for entry in entries for _, raw in entry.artifacts)
            _require(total <= 4194304, 'evidence_bounds')
            object.__setattr__(self, 'profile_snapshot', rebuilt.snapshot)
            object.__setattr__(self, 'snapshot', snapshot)
            object.__setattr__(self, 'snapshot_sha256', hashlib.sha256(snapshot).hexdigest())
            object.__setattr__(self, '_entries', entries)
        except (MemoryError, KeyboardInterrupt, SystemExit):
            raise
        except Exception as error:
            code = getattr(error, 'code', None)
            if code == 'limit_exceeded':
                _fail('evidence_bounds')
            if type(error) is ValueError and error.args in (('evidence_invalid',), ('evidence_bounds',), ('evidence_dependency',)):
                _fail(cast(str, error.args[0]))
            _fail()

    def payload_json(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self.snapshot))

    def _lookup(self, profile_id: str, result_id: str) -> _Entry | None:
        return next((v for v in self._entries if v.profile_id == profile_id and v.result_id == result_id), None)


def _capabilities(enabled: bool) -> dict[str, Any]:
    return dict(representation='equity.mcp.evidence.capabilities.v1', enabled=enabled,
                modes=['local_owner_prepared'], kinds=list(KINDS), chunk_bytes=CHUNK,
                automatic_reports=False, input_content_verified=False, source_authenticated=False, hosted_qualified=False)


def _missing(reference: str, kind: str, presence: str) -> dict[str, Any]:
    return dict(representation='equity.mcp.evidence.v1', reference=reference, kind=kind, presence=presence,
                quality=None, artifact=None, chunk=None, offset=0, next_offset=None, complete=False)


def _read(catalog: EvidenceCatalog | None, *, reference: str, profile_id: str, job_id: str,
          result_id: str, command_digest: str, producer: bytes, kind: str, offset: int) -> tuple[dict[str, Any] | None, str | None]:
    """Only called AFTER real native artifact authorization; no optional API work."""
    if catalog is None:
        return (_missing(reference, kind, 'not_enabled'), None) if offset == 0 else (None, 'evidence_bounds')
    entry = catalog._lookup(profile_id, result_id)
    if entry is None:
        return (_missing(reference, kind, 'not_supplied'), None) if offset == 0 else (None, 'evidence_bounds')
    if entry.job_id != job_id or entry.command_digest != command_digest or entry.producer != producer:
        return None, 'evidence_binding'
    raw = dict(entry.artifacts).get(kind)
    if raw is None:
        return (_missing(reference, kind, 'not_supplied'), None) if offset == 0 else (None, 'evidence_bounds')
    if offset >= len(raw):
        return None, 'evidence_bounds'
    stop = min(offset + CHUNK, len(raw))
    payload = dict(representation='equity.mcp.evidence.v1', reference=reference, kind=kind, presence='supplied',
                   quality=json.loads(entry.quality), artifact=json.loads(entry.manifest)['artifacts'][kind], chunk=raw[offset:stop].decode('ascii'),
                   offset=offset, next_offset=None if stop == len(raw) else stop, complete=stop == len(raw))
    return payload, None


__all__ = ['DiagnosticAssociation', 'EvidenceRegistration', 'EvidenceCatalog']
