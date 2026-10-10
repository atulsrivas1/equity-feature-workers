"""Optional trusted native worker registrations; one shared bounded job owner."""
from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field, replace
import hashlib
import json
import re
import secrets
import threading
import time
from types import MappingProxyType
from typing import Any

from equity_feature_contracts import CanonicalBatch
from equity_feature_contracts.adapters import AdapterBatch, HistoricalAdapter
from equity_feature_io_contracts import CredentialProvider, ResultSink, SinkRequirements
from equity_feature_io_contracts.publication import (SinkError, SinkErrorCode, CompletionReceipt,
    ArtifactReference, PublicationState)
from equity_feature_io_sdk import (SourceRegistry, SinkRegistry, encode_result, encode_envelope, encode_receipt,
    decode_result, decode_envelope, decode_receipt, verify_content, verify_receipt)
from equity_feature_workers import (SessionCommandSpec, SourceOffer, ExecutionApproval, CommandOutcome,
    PlanningError, PlanningErrorCode, plan_acquisition, execute_plan)

from . import codec
from ._audit import AuditReservation, AuditDenied
from ._cache import ResultCache, fingerprint, identity_key
from .datasets import DatasetIdentity, FeatureDataset, RawDataset, Scope, _batch_bytes
from .service import Credential, Ledger, _Prepared, _error


def _bounded(value: object, limit: int) -> bytes:
    fragments: list[str] = []
    used = 0
    for fragment in json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).iterencode(value):
        used += len(fragment)
        if used > limit:
            raise codec.WireError("bounds")
        fragments.append(fragment)
    return ''.join(fragments).encode('ascii')


@dataclass(frozen=True)
class JobGrant:
    """Additional server-owned config/complete executed-feature permission."""
    grant_id: str
    config_digest: str
    features: frozenset[tuple[str, str]]

    def __post_init__(self) -> None:
        if (type(self.grant_id) is not str or not 0 < len(self.grant_id) <= 256
                or type(self.config_digest) is not str or re.fullmatch(r'[0-9a-f]{64}',self.config_digest) is None
                or type(self.features) is not frozenset or not 1 <= len(self.features) <= 39
                or any(type(pair) is not tuple or len(pair) != 2
                       or any(type(v) is not str or not 0 < len(v) <= 256 for v in pair) for pair in self.features)):
            raise ValueError('invalid_job_permission')


@dataclass(frozen=True)
class OwnedReceiptProfile:
    """Operator attestation: one artifact, printable ASCII ID at most128 chars."""
    profile: str = 'owned-one-artifact-ascii128-v1'

    def __post_init__(self) -> None:
        if self.profile != 'owned-one-artifact-ascii128-v1':
            raise ValueError('unadmitted_receipt_profile')


class JobRegistration:
    """Freeze native request/config and a complete observed owned delivery."""
    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self,'_sealed',False):
            raise AttributeError('immutable_job_registration')
        object.__setattr__(self,name,value)

    def __delattr__(self, name: str) -> None:
        raise AttributeError('immutable_job_registration')

    @property
    def sink_config(self) -> dict[str, Any]:
        return json.loads(self.sink_config_bytes)  # type: ignore[no-any-return]

    def __init__(self, request: codec.Json, spec: SessionCommandSpec, delivery: AdapterBatch, *,
                 sources: SourceRegistry[HistoricalAdapter], source: SourceOffer,
                 sinks: SinkRegistry[ResultSink], sink_id: str, sink_config: dict[str, Any],
                 credentials: CredentialProvider, receipt_profile: OwnedReceiptProfile,
                 acquisition_receipt_fingerprint: str) -> None:
        codec.validate(request)
        if type(receipt_profile) is not OwnedReceiptProfile:
            raise ValueError('unadmitted_receipt_profile')
        if (type(acquisition_receipt_fingerprint) is not str
                or re.fullmatch(r'[0-9a-f]{64}',acquisition_receipt_fingerprint) is None):
            raise ValueError('invalid_acquisition_receipt')
        if (request['kind'] != 'request' or request['payload']['operation'] != 'calculate'
                or type(spec) is not SessionCommandSpec or spec.family not in ('trades', 'bars', 'quotes')
                or spec.request.max_batches != 1 or spec.request.max_rows > 100
                or spec.max_input_bytes > 1_048_576 or len(spec.features) > 39
                or type(delivery) is not AdapterBatch or type(delivery.batch) is not CanonicalBatch
                or not delivery.final or delivery.ordinal != 0 or delivery.request_id != spec.request.request_id
                or delivery.batch.row_count > 100):
            raise ValueError('invalid_job_registration')
        payload = request['payload']
        context = payload['context']
        actual_digest = hashlib.sha256(codec.canonical({k:v for k,v in payload.items() if k not in ('command_digest','idempotency_key')})).hexdigest()
        if payload['command_digest'] != actual_digest:
            raise ValueError('invalid_job_command')
        scope = Scope(spec.entity.instrument_id, spec.entity.session_id, spec.request.start_ns, spec.request.end_ns)
        batch = delivery.batch
        identity = DatasetIdentity.from_source(context['dataset']['dataset_id'], context['dataset']['revision'], batch.metadata.source)
        # Reuse native producer context/quality validation before any remote submission.
        from equity_feature_workers import compute_session_inputs, prepare_session
        class Baseline:
            def capabilities(self) -> Any:
                return source.capabilities
            def iter_batches(self, acquisition: Any, cancellation: Any) -> Iterator[AdapterBatch]:
                if acquisition != spec.request:
                    raise ValueError('changed_source_request')
                yield delivery
        prepared = prepare_session(spec, Baseline())
        result = compute_session_inputs(prepared.task, (batch,))[0]
        FeatureDataset(identity, scope, result, context, actual_digest, rights=frozenset(('retain','derived_read')),
                       rights_owner='owned-registration', rights_evidence='explicit-server-registration',
                       valid_from_ns=0, expires_at_ns=2**63-1)
        self.context_bytes = codec.canonical(context)
        self.payload_bytes = codec.canonical({k:v for k,v in payload.items() if k not in ('command_digest','idempotency_key')})
        self.spec_bytes = _bounded(asdict(spec), 16_384)
        self.delivery_hash = hashlib.sha256(_bounded(asdict(delivery), 1_048_576)).hexdigest()
        self.acquisition_commitment = hashlib.sha256(_batch_bytes(batch)).hexdigest(),acquisition_receipt_fingerprint
        self.identity, self.scope = identity, scope
        self.spec, self.sources, self.source = spec, sources, source
        self.sinks, self.sink_id, self.credentials = sinks, sink_id, credentials
        self.sink_config_bytes = _bounded(sink_config,8192)
        self.receipt_profile = receipt_profile
        self.columns = tuple(column.name for column in batch.columns)
        self.execution_features = frozenset((f.feature_id,f.algorithm_version) for f in spec.features)
        self.command_digest = actual_digest
        self.requirements = SinkRequirements(max_results=1,max_chunk_bytes=65_536,max_total_bytes=65_536,
                                             max_result_cells=3900,max_evidence_rows=100)
        self.registration_digest = hashlib.sha256(_bounded({'spec':asdict(spec),'delivery':self.delivery_hash,
            'context':context,'source':asdict(source),'sink_id':sink_id,'sink_config':self.sink_config,
            'receipt_profile':asdict(receipt_profile),'acquisition_commitment':self.acquisition_commitment}, 65_536)).hexdigest()
        self._sealed = True


@dataclass
class _Job:
    job_id: str
    principal: str
    credential_digest: str
    grant_id: str
    dataset_id: str
    key_digest: str
    identity_digest: str
    command_digest: str
    grant_expires_ns: int
    state: str = 'queued'
    result_id: str | None = None
    error: codec.Json | None = None
    cancelled: bool = False
    deadline_ns: int = 0
    expires_ns: int = 0
    held: int = 262_144
    native: bytes = field(default=b'', repr=False)
    envelope: bytes = field(default=b'', repr=False)
    receipt: bytes = field(default=b'', repr=False)
    wire: bytes = field(default=b'', repr=False)
    committed_receipt_sha256: str | None = None
    receipt_profile_failed: bool = False
    audit_reservation: AuditReservation | None = field(default=None,repr=False)


class JobScheduler:
    """Single execution thread; ledger lock linearizes admission and visibility."""
    def __init__(self, ledger: Ledger, registrations: tuple[JobRegistration, ...], grants: tuple[JobGrant, ...], *,
                 monotonic: Callable[[], int] = time.monotonic_ns) -> None:
        if type(ledger) is not Ledger or not callable(monotonic) or not 1 <= len(registrations) <= 16 or len(grants) > 64:
            raise ValueError('invalid_job_startup')
        if any(type(r) is not JobRegistration for r in registrations) or any(type(g) is not JobGrant for g in grants):
            raise ValueError('invalid_job_startup')
        self.ledger, self.monotonic = ledger, monotonic
        self.registrations = MappingProxyType({r.identity.dataset_id:r for r in registrations})
        self.grants = MappingProxyType({g.grant_id:g for g in grants})
        if len(self.registrations) != len(registrations) or len(self.grants) != len(grants):
            raise ValueError('duplicate_job_registration')
        for r in registrations:
            dataset = ledger.datasets.get(r.identity.dataset_id)
            if (type(dataset) is not RawDataset or dataset.identity != r.identity or dataset.scope != r.scope
                    or dataset.acquisition_commitment != r.acquisition_commitment
                    or r.context_bytes != codec.canonical(json.loads(r.context_bytes))
                    or json.loads(r.context_bytes)['registry_snapshot'] != ledger.registry_snapshot):
                raise ValueError('unadmitted_job_dataset')
        for g in grants:
            underlying = next((v for v in ledger.grants if v.grant_id == g.grant_id), None)
            admitted_registration = self.registrations.get(underlying.dataset_id) if underlying else None
            if (underlying is None or admitted_registration is None or not {'calculate','job_manage','retain'} <= underlying.actions
                    or not 0 < underlying.expires_at_ns-underlying.valid_from_ns <= 3_600_000_000_000
                    or g.config_digest != admitted_registration.spec.config.digest or not admitted_registration.execution_features <= g.features):
                raise ValueError('invalid_job_grant')
        self.jobs: dict[str,_Job] = {}
        self.keys: dict[tuple[str,str],str] = {}
        self.queue: deque[str] = deque()
        self.retained: dict[str,int] = {}
        self.total_retained = 0
        self.cache = ResultCache()
        self._running: str | None = None
        self._last_mono = 0
        self._last_wall = ledger.now()
        self._closed = False
        self.epoch = secrets.token_hex(16)
        self._mono()
        with ledger.lock:
            if getattr(ledger,'_job_scheduler',None) is not None:
                raise ValueError('shared_job_scheduler_required')
            setattr(ledger,'_job_scheduler',self)
        self.condition = threading.Condition(ledger.lock)
        self.thread = threading.Thread(target=self._run,name='owned-native-jobs',daemon=True)
        self.maintenance = threading.Thread(target=self._maintain,name='owned-job-expiry',daemon=True)
        self.thread.start()
        self.maintenance.start()

    def _mono(self) -> int:
        n = self.monotonic()
        if type(n) is not int or not self._last_mono <= n <= 2**63-1-30_000_000_000:
            raise codec.WireError('not_permitted')
        self._last_mono = n
        return n

    def _now(self) -> int:
        self._last_wall = self.ledger.now()
        return self._last_wall

    def _authorized(self, credential: Credential, registration: JobRegistration, grant_id: str, action: str) -> bool:
        dataset = self.ledger.datasets.get(registration.identity.dataset_id)
        if type(dataset) is not RawDataset or dataset.acquisition_commitment != registration.acquisition_commitment:
            return False
        grant = self.ledger.authorized(credential,dataset,registration.columns,action,self._now())
        rule = self.grants.get(grant_id)
        return (grant is not None and grant.grant_id == grant_id and rule is not None
                and rule.config_digest == registration.spec.config.digest and registration.execution_features <= rule.features)

    def _credential(self, job: _Job) -> Credential:
        credential = next((c for c in self.ledger.credentials if c.principal == job.principal and c.digest == job.credential_digest), None)
        if credential is None:
            raise codec.WireError('not_permitted')
        return credential

    def _retention_allowed(self, job: _Job) -> bool:
        try:
            return self._authorized(self._credential(job),self.registrations[job.dataset_id],job.grant_id,'retain')
        except ValueError:
            return False

    def _check(self, job: _Job, action: str = 'calculate', *, cancel: bool = True) -> None:
        with self.ledger.lock:
            r = self.registrations[job.dataset_id]
            required = ('calculate','job_manage','retain') if action == 'calculate' else (action,)
            if (not all(self._authorized(self._credential(job),r,job.grant_id,a) for a in required)
                    or (cancel and (job.cancelled or (job.deadline_ns and self._mono() >= job.deadline_ns)))):
                raise SinkError(SinkErrorCode.CANCELLED)

    def _release(self, job: _Job, amount: int) -> None:
        job.held -= amount
        self.total_retained -= amount
        self.retained[job.principal] -= amount

    def _clear(self, job: _Job) -> None:
        if job.result_id is not None:
            self.cache.purge(0,job.result_id)
        job.native = job.envelope = job.receipt = job.wire = b''
        if job.held:
            self._release(job,job.held)

    def _audit_exit(self, job: _Job, decision: str, *, never_started: bool = False) -> bool:
        reservation = job.audit_reservation
        if reservation is None:
            return True
        try:
            if reservation.remaining == 2:
                self.ledger.audit_finish(reservation,'native_start','cancelled' if never_started else 'failed',exited=True)
            self.ledger.audit_finish(reservation,'native_finish',decision,exited=True)
            return True
        except Exception:
            if reservation.remaining and self.ledger.audit is not None:
                self.ledger.audit.abandon_after_exit(reservation)
            return False
        finally:
            job.audit_reservation = None

    def _sweep(self) -> None:
        n = self._now()
        self.cache.purge(n)
        if self.ledger.audit is not None:
            self.ledger.audit.purge(n)
        for job in tuple(self.jobs.values()):
            if job.state == 'queued':
                try:
                    self._check(job)
                except Exception:
                    job.cancelled, job.state = True,'cancelled'
                    self.queue.remove(job.job_id)
                    job.expires_ns = min(n+300_000_000_000,job.grant_expires_ns)
                    self._clear(job)
                    self._audit_exit(job,'cancelled',never_started=True)
            if job.state in ('succeeded','failed','cancelled','expired'):
                if (job.expires_ns and n >= job.expires_ns
                        or job.held and not self._retention_allowed(job)):
                    self._clear(job)
                    job.state, job.result_id, job.error = 'expired',None,None
                if n >= job.grant_expires_ns:
                    self._clear(job)
                    self.keys.pop((job.principal,job.key_digest),None)
                    del self.jobs[job.job_id]

    def _snapshot(self, job: _Job, version: str, rid: str) -> codec.Json:
        return {'schema':'equity.remote','version':version,'kind':'job','request_id':rid,
                'payload':{'job_id':job.job_id,'command_digest':job.command_digest,'state':job.state,
                           'result_id':job.result_id,'error':job.error}}

    def visible(self, credential: Credential, job_id: str, version: str, rid: str) -> codec.Json:
        with self.ledger.lock:
            self._sweep()
            job = self.jobs.get(job_id)
            if job is None or job.principal != credential.principal or not self._authorized(credential,self.registrations[job.dataset_id],job.grant_id,'job_manage'):
                raise codec.WireError('not_permitted')
            return self._snapshot(job,version,rid)

    def _result_job(self, credential: Credential, result_id: str, operation: str) -> _Job:
        """Lightweight current authorization/expiry for preparation and final visibility."""
        with self.ledger.lock:
            self._sweep()
            job = next((j for j in self.jobs.values() if j.result_id == result_id), None)
            if (job is None or job.state != 'succeeded' or job.principal != credential.principal
                    or credential not in self.ledger.credentials or not self._retention_allowed(job)):
                raise codec.WireError('not_permitted')
            registration = self.registrations[job.dataset_id]
            dataset_now = self.ledger.datasets.get(job.dataset_id)
            admission = hashlib.sha256(_bounded({'principal':job.principal,'grant':job.grant_id,
                'policy':self.ledger.policy_revision,'registration':registration.registration_digest},8192)).hexdigest()
            if (admission != job.identity_digest or dataset_now is None
                    or dataset_now.identity != registration.identity or dataset_now.scope != registration.scope
                    or self.ledger.registry_snapshot != json.loads(registration.context_bytes)['registry_snapshot']):
                raise codec.WireError('not_permitted')
            actions = ('derived_read','retain','export') if operation == 'artifact_read' else ('derived_read','retain')
            if not all(self._authorized(credential,registration,job.grant_id,a) for a in actions):
                raise codec.WireError('not_permitted')
            return job

    def result_current(self, credential: Credential, result_id: str, operation: str) -> None:
        self._result_job(credential,result_id,operation)

    def result_visible(self, credential: Credential, result_id: str, version: str, rid: str,
                       operation: str) -> codec.Json:
        """Pure retained-form verification; never accesses native source/sink objects."""
        with self.ledger.lock:
            job = self._result_job(credential,result_id,operation)
            registration = self.registrations[job.dataset_id]
            actions = ('derived_read','retain','export') if operation == 'artifact_read' else ('derived_read','retain')
            if (len(job.native) > 65_536 or len(job.wire) > 131_072
                    or len(job.envelope)+len(job.receipt) > 32_768
                    or len(job.native)+len(job.wire)+len(job.envelope)+len(job.receipt) != job.held
                    or hashlib.sha256(job.receipt).hexdigest() != job.committed_receipt_sha256):
                raise codec.WireError('not_permitted')
            native, envelope, receipt = decode_result(job.native),decode_envelope(job.envelope),decode_receipt(job.receipt)
            verify_content(envelope,(native,))
            verify_receipt(receipt,envelope,(native,))
            dataset = FeatureDataset(registration.identity,registration.scope,native,
                json.loads(registration.context_bytes),registration.command_digest,
                rights=frozenset(actions),rights_owner='owned-registration',rights_evidence='verified-native-receipt',
                valid_from_ns=0,expires_at_ns=job.grant_expires_ns)
            _, body = dataset.produce(dataset.columns,'1.1')
            if codec.canonical(body) != job.wire:
                raise codec.WireError('not_permitted')
            # Only original producer representation is cached. Native content,
            # receipt and complete reconstructed wire were verified above on
            # every lookup; final authority and transfer remain controller work.
            producer_payload = codec.canonical(body['feature_result'])
            original_grant = next(g for g in self.ledger.grants if g.grant_id == job.grant_id)
            current_dataset = self.ledger.datasets[job.dataset_id]
            try:
                self._result_job(credential,result_id,operation)
                key = identity_key({'schema':'cache.identity.v1','principal':job.principal,
                    'original_credential_digest':job.credential_digest,
                    'grant_sha256':fingerprint(asdict(original_grant)),
                    'job_grant_sha256':fingerprint(asdict(self.grants[job.grant_id])),
                    'rights_sha256':fingerprint({'rights_owner':current_dataset.rights_owner,
                        'rights_evidence':current_dataset.rights_evidence,'valid_from_ns':current_dataset.valid_from_ns,
                        'expires_at_ns':current_dataset.expires_at_ns,'rights':current_dataset.rights}),
                    'dataset_identity':current_dataset.identity.wire(),
                    'acquisition_commitment':{'content_sha256':registration.acquisition_commitment[0],
                        'receipt_sha256':registration.acquisition_commitment[1]},
                    'context_sha256':hashlib.sha256(registration.context_bytes).hexdigest(),
                    'registration_sha256':registration.registration_digest,'result_id':result_id,'epoch':self.epoch,
                    'native_content_sha256':hashlib.sha256(job.native).hexdigest(),
                    'committed_receipt_sha256':job.committed_receipt_sha256,
                    'producer_wire_sha256':hashlib.sha256(job.wire).hexdigest(),
                    'operation':operation,'projection':list(dataset.columns),'transport_version':version})
                producer_payload = self.cache.representation(key,job.principal,result_id,producer_payload,
                    job.expires_ns,self._now(),self.retained.get(job.principal,0),self.total_retained)
            except ValueError:
                # Complete oversized server records are never truncated. Cache
                # refusal preserves ordinary independently verified delivery.
                pass
            return {'schema':'equity.remote','version':version,'kind':'result','request_id':rid,
                    'payload':json.loads(producer_payload)}

    def prepare_result(self, credential: Credential, request: codec.Json) -> _Prepared:
        version,rid,payload = request['version'],request['request_id'],request['payload']
        if version != '1.1':
            return _error(400,'transport','incompatible_version',version,rid)
        if payload['cursor'] is not None:
            return _error(400,'transport','invalid_schema',version,rid)
        result_id,operation = payload['result_id'],payload['operation']
        with self.ledger.lock:
            try:
                amount = len(codec.encode(self.result_visible(credential,result_id,version,rid,operation)))
            except Exception:
                return _error(403,'authorization','not_permitted',version,rid)
            if not self.ledger.reserve(credential.principal,amount):
                return _error(429,'quota','quota_exceeded',version,rid)
            # Retain only bounded opaque/auth/correlation metadata between prepare and emission.
            placeholder = _error(403,'authorization','not_permitted',version,rid).envelope
            attachment = ('result-'+hashlib.sha256(result_id.encode('ascii')).hexdigest()+'.json'
                          if operation == 'artifact_read' else None)
            return _Prepared(200,placeholder,credential,action=operation,reserved_bytes=amount,
                finalize=lambda: self.result_visible(credential,result_id,version,rid,operation),attachment=attachment,
                before_emit=lambda: self.result_current(credential,result_id,operation))

    def discovery_features(self, credential: Credential) -> set[tuple[str,str]]:
        with self.ledger.lock:
            features: set[tuple[str,str]] = set()
            for registration in self.registrations.values():
                if any(self._authorized(credential,registration,g,'calculate')
                       and self._authorized(credential,registration,g,'discover') for g in self.grants):
                    features.update((p['feature_id'],p['algorithm_version']) for p in json.loads(registration.context_bytes)['features'])
            return features

    def handle(self, credential: Credential, request: codec.Json) -> _Prepared:
        version,rid,payload = request['version'],request['request_id'],request['payload']
        with self.ledger.lock:
            self._sweep()
            if self._closed:
                return _error(403,'authorization','not_permitted',version,rid)
            if payload['operation'] == 'calculate':
                registration = self.registrations.get(payload['context']['dataset']['dataset_id'])
                original = {k:v for k,v in payload.items() if k not in ('command_digest','idempotency_key')}
                if (registration is None or payload['context']['dataset'] != registration.identity.wire()
                        or payload['scope'] != registration.scope.wire()):
                    return _error(403,'authorization','not_permitted',version,rid)
                grant_id = next((g for g in self.grants if self._authorized(credential,registration,g,'calculate')
                                 and self._authorized(credential,registration,g,'retain')
                                 and self._authorized(credential,registration,g,'job_manage')), None)
                if grant_id is None:
                    return _error(403,'authorization','not_permitted',version,rid)
                if codec.canonical(original) != registration.payload_bytes or payload['command_digest'] != registration.command_digest:
                    return _error(422,'contract','inconsistent_identity',version,rid)
                key = hashlib.sha256(payload['idempotency_key'].encode('utf-8')).hexdigest()
                identity = hashlib.sha256(_bounded({'principal':credential.principal,'grant':grant_id,
                    'policy':self.ledger.policy_revision,'registration':registration.registration_digest},8192)).hexdigest()
                existing_id = self.keys.get((credential.principal,key))
                if existing_id:
                    job = self.jobs[existing_id]
                    if job.identity_digest != identity or job.command_digest != payload['command_digest']:
                        return _error(422,'contract','inconsistent_identity',version,rid)
                else:
                    if (len(self.jobs) >= 16 or sum(j.principal == credential.principal for j in self.jobs.values()) >= 8
                            or len(self.queue) >= 2 or any(self.jobs[j].principal == credential.principal for j in self.queue)
                            or self.total_retained+self.cache.total+262_144 > 2_097_152
                            or self.retained.get(credential.principal,0)+self.cache.principal_bytes(credential.principal)+262_144 > 1_048_576):
                        return _error(429,'quota','quota_exceeded',version,rid)
                    job = _Job(self.epoch+'-'+secrets.token_hex(16),credential.principal,credential.digest,grant_id,registration.identity.dataset_id,key,identity,payload['command_digest'],
                        next(g.expires_at_ns for g in self.ledger.grants if g.grant_id == grant_id))
                    _bounded({k:v for k,v in asdict(job).items() if k not in ('native','wire','envelope','receipt')},8192)
                    if self.ledger.audit is not None:
                        try:
                            job.audit_reservation = self.ledger.audit.reserve(self._now(),actor=job.principal,
                                command=job.command_digest,grant=job.grant_id,policy=self.ledger.policy_revision,action='calculate')
                        except AuditDenied:
                            return _error(429,'quota','quota_exceeded',version,rid)
                    self.jobs[job.job_id] = job
                    self.keys[(job.principal,key)] = job.job_id
                    self.queue.append(job.job_id)
                    self.total_retained += job.held
                    self.retained[job.principal] = self.retained.get(job.principal,0)+job.held
                    self.condition.notify()
            else:
                observed_job = self.jobs.get(payload['job_id'])
                if observed_job is None or observed_job.principal != credential.principal or not self._authorized(credential,self.registrations[observed_job.dataset_id],observed_job.grant_id,'job_manage'):
                    return _error(403,'authorization','not_permitted',version,rid)
                job = observed_job
                if payload['operation'] == 'job_cancel' and job.state in ('queued','running','cancel_requested'):
                    job.cancelled = True
                    if job.state == 'queued':
                        self.queue.remove(job.job_id)
                        job.state = 'cancelled'
                        job.expires_ns = min(self._now()+300_000_000_000,job.grant_expires_ns)
                        self._clear(job)
                        self._audit_exit(job,'cancelled',never_started=True)
                    else:
                        job.state = 'cancel_requested'
                    self.condition.notify()
            return _Prepared(200,self._snapshot(job,version,rid),credential,
                             finalize=lambda: self.visible(credential,job.job_id,version,rid))

    def _run(self) -> None:
        while True:
            with self.condition:
                if self._closed and not self.queue:
                    return
                try:
                    self._sweep()
                except ValueError:
                    # Fail closed without losing the one process-owned executor.
                    self.condition.wait(0.1)
                    continue
                if not self.queue and not self._closed:
                    self.condition.wait(0.1)
                    continue
                if self._closed and not self.queue:
                    return
                job = self.jobs[self.queue.popleft()]
                self._running,job.state = job.job_id,'running'
            try:
                with self.ledger.lock:
                    job.deadline_ns = self._mono()+30_000_000_000
                    if job.audit_reservation is not None:
                        self.ledger.audit_finish(job.audit_reservation,'native_start','started',exited=False)
                self._execute(job)
                with self.ledger.lock:
                    self._check(job)
                    job.state,job.result_id = 'succeeded',self.epoch+'-'+secrets.token_hex(16)
            except Exception as execution_error:
                with self.ledger.lock:
                    try:
                        self._check(job)
                        cancelled = False
                    except Exception:
                        cancelled = True
                    job.state = 'cancelled' if cancelled else 'failed'
                    bounded_failure = (job.receipt_profile_failed
                        or isinstance(execution_error,SinkError) and execution_error.code is SinkErrorCode.RESOURCE_LIMIT
                        or isinstance(execution_error,PlanningError) and execution_error.code is PlanningErrorCode.LIMIT)
                    job.error = None if cancelled else ({'category':'contract','code':'bounds','retryable':False}
                        if bounded_failure else {'category':'internal','code':'internal_error','retryable':False})
            finally:
                with self.condition:
                    try:
                        completed_ns = self._now()
                    except ValueError:
                        completed_ns = self._last_wall
                    job.expires_ns = min(completed_ns+300_000_000_000,job.grant_expires_ns)
                    try:
                        keep_receipt = self._retention_allowed(job)
                    except ValueError:
                        keep_receipt = False
                    if job.state in ('failed','cancelled') and (not job.receipt or not keep_receipt):
                        self._clear(job)
                    actual = len(job.native)+len(job.wire)+len(job.envelope)+len(job.receipt)
                    self._release(job,job.held-actual)
                    decision = ('historical_commit' if job.committed_receipt_sha256 and job.state != 'succeeded'
                        else job.state if job.state in ('succeeded','failed','cancelled','expired') else 'failed')
                    if not self._audit_exit(job,decision):
                        job.state,job.result_id = 'failed',None
                        job.error = {'category':'internal','code':'internal_error','retryable':False}
                        self._clear(job)
                    self._running = None
                    self.condition.notify_all()

    def _maintain(self) -> None:
        while True:
            with self.condition:
                if self._closed and self._running is None:
                    return
                try:
                    self._sweep()
                except ValueError:
                    pass
                self.condition.wait(0.05)

    def _execute(self, job: _Job) -> None:
        registration = self.registrations[job.dataset_id]
        spec = replace(registration.spec,job_id=job.job_id,generation_id=job.job_id,partition_id=job.job_id)
        scheduler = self
        class Cancellation:
            def is_cancelled(self) -> bool:
                try:
                    scheduler._check(job)
                    return False
                except Exception:
                    return True
        class Source:
            def __init__(self, original: HistoricalAdapter) -> None:
                self.original = original
            def capabilities(self) -> Any:
                return self.original.capabilities()
            def iter_batches(self, request: Any, cancellation: Any) -> Iterator[AdapterBatch]:
                scheduler._check(job)
                if request != registration.spec.request:
                    raise codec.WireError('inconsistent_identity')
                iterator = iter(self.original.iter_batches(request,cancellation))
                try:
                    for delivery in iterator:
                        scheduler._check(job)
                        if hashlib.sha256(_bounded(asdict(delivery),1_048_576)).hexdigest() != registration.delivery_hash:
                            raise codec.WireError('inconsistent_identity')
                        yield delivery
                        scheduler._check(job)
                finally:
                    close = getattr(iterator,'close',None)
                    if close is not None:
                        close()
        class Sink:
            def __init__(self, original: ResultSink) -> None:
                self.original = original
                self.owner = threading.current_thread()
            def capture(self, receipt: Any) -> None:
                data = encode_receipt(receipt)
                job.committed_receipt_sha256 = hashlib.sha256(data).hexdigest()
                valid = (type(receipt) is CompletionReceipt and len(receipt.artifacts) == 1
                         and len(receipt.artifacts[0].artifact_id) <= 128
                         and all(32 <= ord(c) < 127 for c in receipt.artifacts[0].artifact_id)
                         and len(job.envelope)+len(data) <= 32_768)
                if not valid:
                    job.receipt_profile_failed = True
                    raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
                job.receipt = data
            def check(self, *, cleanup: bool = False) -> None:
                if self.owner is not threading.current_thread():
                    raise SinkError(SinkErrorCode.INVALID_SESSION)
                if not cleanup:
                    scheduler._check(job)
            def capabilities(self) -> Any:
                return self.original.capabilities()
            def begin(self, envelope: Any) -> Any:
                with scheduler.ledger.lock:
                    self.check()
                    data = encode_envelope(envelope)
                    worst = CompletionReceipt(envelope.identity,'f'*64,'f'*64,1,2**63-1,2**63-1,
                        2**63-1,(ArtifactReference('"'*128,'f'*64,2**63-1),),-2**63)
                    if len(data)+len(encode_receipt(worst)) > 32_768:
                        raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
                    job.envelope = data
                    session = self.original.begin(envelope)
                    if type(session) is CompletionReceipt:
                        self.capture(session)
                    return session
            def write(self, session: Any, ordinal: int, result: Any) -> None:
                with scheduler.ledger.lock:
                    self.check()
                    data = encode_result(result)
                    if ordinal != 0 or len(data) > 65_536:
                        raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
                    self.original.write(session,ordinal,result)
                    job.native = data
            def commit(self, session: Any) -> Any:
                with scheduler.ledger.lock:
                    self.check()
                    receipt = self.original.commit(session)
                    self.capture(receipt)
                    return receipt
            def read(self, receipt: Any) -> Any:
                with scheduler.ledger.lock:
                    self.check()
                    return self.original.read(receipt)
            def lookup(self, key: str) -> Any:
                with scheduler.ledger.lock:
                    self.check()
                    status = self.original.lookup(key)
                    if status.state is PublicationState.COMMITTED:
                        self.capture(status.receipt)
                    return status
            def abort(self, session: Any) -> Any:
                with scheduler.ledger.lock:
                    self.check(cleanup=True)
                    status = self.original.abort(session)
                    if status.state is PublicationState.COMMITTED:
                        self.capture(status.receipt)
                    return status
        class Factory:
            protocol_version = '1'
            def __init__(self, kind: str) -> None:
                self.kind = kind
            def validate_config(self, config: Any) -> Any:
                if config:
                    raise ValueError('unexpected_job_config')
                return {}
            def create(self, config: Any, credentials: Any) -> Any:
                scheduler._check(job)
                if self.kind == 'source':
                    return Source(registration.sources.resolve(registration.source.source_id,registration.source.config,credentials,spec.request))
                return Sink(registration.sinks.resolve(registration.sink_id,registration.sink_config,credentials,registration.requirements))
        sources: SourceRegistry[HistoricalAdapter] = SourceRegistry()
        sinks: SinkRegistry[ResultSink] = SinkRegistry()
        sources.register('owned.source',Factory('source'))
        sinks.register('owned.sink',Factory('sink'))
        offer = SourceOffer('owned.source',registration.source.capabilities,{})
        context = json.loads(registration.context_bytes)
        selected = tuple(pair['feature_id'] for pair in context['features'])
        plan = plan_acquisition(spec,selected,sources=sources,offers=(offer,),sinks=sinks,
                                sink_id='owned.sink',sink_config={},requirements=registration.requirements)
        def approve(candidate: Any) -> ExecutionApproval:
            self._check(job)
            return ExecutionApproval(candidate.plan_sha256)
        outcome = execute_plan(spec,plan,sources=sources,sinks=sinks,credentials=registration.credentials,
                               authorize=approve,cancellation=Cancellation())
        if type(outcome) is not CommandOutcome:
            raise codec.WireError('inconsistent_identity')
        if (outcome.output.receipt is None or job.receipt_profile_failed or job.envelope != encode_envelope(outcome.output.envelope)
                or job.receipt != encode_receipt(outcome.output.receipt)):
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        self._check(job)
        dataset = FeatureDataset(registration.identity,registration.scope,outcome.results[0],context,registration.command_digest,
                                 rights=frozenset(('retain','derived_read')),rights_owner='owned-job',rights_evidence='server-approved-job',
                                 valid_from_ns=0,expires_at_ns=2**63-1)
        _, wire = dataset.produce(dataset.columns,'1.1')
        wire_data = codec.canonical(wire)
        if len(wire_data) > 131_072 or len(wire_data)+len(job.native)+len(job.envelope)+len(job.receipt) > 262_144:
            raise SinkError(SinkErrorCode.RESOURCE_LIMIT)
        with self.ledger.lock:
            self._check(job)
            job.wire = wire_data

    def close(self, timeout: float = 1.0) -> bool:
        if type(timeout) not in (int,float) or not 0 <= timeout <= 30:
            raise ValueError('invalid_job_join')
        with self.condition:
            self._closed = True
            for job in self.jobs.values():
                if job.state in ('queued','running','cancel_requested'):
                    job.cancelled = True
                if job.state == 'queued':
                    self.queue.remove(job.job_id)
                    job.state = 'cancelled'
                    job.expires_ns = min(self._last_wall+300_000_000_000,
                        job.grant_expires_ns)
                    self._clear(job)
                    self._audit_exit(job,'cancelled',never_started=True)
            try:
                self._sweep()
            except ValueError:
                pass
            self.condition.notify_all()
        end = time.monotonic()+timeout
        self.thread.join(timeout)
        self.maintenance.join(max(0.0,end-time.monotonic()))
        return not self.thread.is_alive() and not self.maintenance.is_alive()
