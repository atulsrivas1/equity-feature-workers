"""Single-process loopback qualification, shared admission and atomic visibility."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
import hashlib
import hmac
import ipaddress
import re
from threading import RLock
from typing import Any

from . import codec
from .datasets import Dataset, Scope, FeatureDataset
from equity_feature_contracts import ContractError

StartResponse = Callable[..., Any]
_ACTIONS = frozenset(("discover", "raw_read", "derived_read", "retain"))


@dataclass(frozen=True)
class Limits:
    requests: int
    principal_transfer: int
    global_transfer: int
    interval_ns: int

    def __post_init__(self) -> None:
        if type(self.interval_ns) is not int or self.interval_ns != 60_000_000_000:
            raise ValueError("invalid_policy_interval")
        for value, maximum in ((self.requests, 60), (self.principal_transfer, 1_048_576),
                               (self.global_transfer, 2_097_152), (self.interval_ns, 60_000_000_000)):
            if type(value) is not int or not 0 < value <= maximum:
                raise ValueError("invalid_limits")


@dataclass(frozen=True)
class Credential:
    principal: str
    digest: str = field(repr=False)
    valid_from_ns: int = 0
    expires_at_ns: int = 0

    @classmethod
    def provision(cls, principal: str, token: str, valid_from_ns: int, expires_at_ns: int) -> Credential:
        if type(token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43,256}", token) is None:
            raise ValueError("invalid_credential")
        return cls(principal, hashlib.sha256(token.encode("ascii")).hexdigest(), valid_from_ns, expires_at_ns)


@dataclass(frozen=True)
class Grant:
    grant_id: str
    principal: str
    dataset_id: str
    dataset_revision: str
    policy_revision: str
    scope: Scope
    columns: frozenset[str]
    actions: frozenset[str]
    valid_from_ns: int
    expires_at_ns: int


def _interval(start: int, end: int) -> None:
    if type(start) is not int or type(end) is not int or not 0 <= start < end <= 2**63 - 1:
        raise ValueError("invalid_validity")


class Ledger:
    """Share this exact instance across every controller in one process."""
    def __init__(self, *, clock: Callable[[], int], limits: Limits,
                 credentials: tuple[Credential, ...], grants: tuple[Grant, ...],
                 datasets: tuple[Dataset, ...], policy_revision: str, registry_snapshot: str) -> None:
        if type(limits) is not Limits or not callable(clock) or not policy_revision:
            raise ValueError("invalid_startup")
        if not 1 <= len(credentials) <= 16 or len(grants) > 64 or not 1 <= len(datasets) <= 16:
            raise ValueError("invalid_startup")
        if re.fullmatch(r"[0-9a-f]{64}", registry_snapshot) is None:
            raise ValueError("invalid_registry")
        if len({c.digest for c in credentials}) != len(credentials) or len({g.grant_id for g in grants}) != len(grants):
            raise ValueError("duplicate_identity")
        if len({d.identity.dataset_id for d in datasets}) != len(datasets):
            raise ValueError("duplicate_dataset")
        for c in credentials:
            _interval(c.valid_from_ns, c.expires_at_ns)
            if not c.principal or len(c.principal) > 256 or re.fullmatch(r"[0-9a-f]{64}", c.digest) is None:
                raise ValueError("invalid_credential")
        for d in datasets:
            _interval(d.valid_from_ns, d.expires_at_ns)
            if not d.rights_owner or not d.rights_evidence or not d.rights <= _ACTIONS:
                raise ValueError("invalid_rights")
            # Structural registration is admitted before any request can select it.
            req = {"schema": "equity.remote", "version": "1.0", "kind": "request", "request_id": "admission",
                   "payload": {"operation": "slice", "dataset": d.identity.wire(), "scope": d.scope.wire(),
                               "columns": list(d.columns), "cursor": None}}
            codec.validate(req)
        for d in datasets:
            if isinstance(d, FeatureDataset):
                if d.registry_snapshot != registry_snapshot or "retain" not in d.rights:
                    raise ValueError("invalid_feature_admission")
                kind, payload = d.produce(d.columns, "1.1")
                codec.encode({"schema": "equity.remote", "version": "1.1", "kind": kind, "request_id": "admission", "payload": payload})
        if len({f for d in datasets for f in d.features}) > 39:
            raise ValueError("invalid_feature_count")
        for g in grants:
            _interval(g.valid_from_ns, g.expires_at_ns)
            if not g.grant_id or g.principal not in {c.principal for c in credentials} or not g.actions <= _ACTIONS:
                raise ValueError("invalid_grant")
            if type(g.columns) is not frozenset or type(g.actions) is not frozenset:
                raise ValueError("invalid_grant")
        self.clock, self.limits = clock, limits
        self.credentials, self.grants = tuple(credentials), tuple(grants)
        self.datasets = {d.identity.dataset_id: d for d in datasets}
        self.policy_revision, self.registry_snapshot = policy_revision, registry_snapshot
        self.lock = RLock()
        self._revoked: set[str] = set()
        self._window: int | None = None
        self._last_ns: int | None = None
        self._requests = 0
        self._principal_requests: dict[str, int] = {}
        self._transfer = 0
        self._reserved = 0
        self._principal_reserved: dict[str, int] = {}
        self._principal_transfer: dict[str, int] = {}

    def now(self) -> int:
        n = self.clock()
        if type(n) is not int or not 0 <= n <= 2**63 - 1 or (self._last_ns is not None and n < self._last_ns):
            raise ValueError("invalid_clock")
        self._last_ns = n
        window = n // self.limits.interval_ns
        if window != self._window:
            self._window = window
            self._requests = self._transfer = 0
            self._principal_requests.clear()
            self._principal_transfer.clear()
        return n

    def revoke(self, grant_id: str) -> None:
        with self.lock:
            if grant_id not in {g.grant_id for g in self.grants}:
                raise ValueError("unknown_grant")
            self._revoked.add(grant_id)

    def authenticate(self, header: str, n: int) -> Credential | None:
        match = re.fullmatch(r"(?i:Bearer) ([A-Za-z0-9_-]{43,256})", header)
        digest = hashlib.sha256((match[1] if match else "").encode("ascii")).hexdigest()
        found = None
        for c in self.credentials:
            equal = hmac.compare_digest(c.digest, digest)
            if equal and match and c.valid_from_ns <= n < c.expires_at_ns:
                found = c
        return found

    def admit_attempt(self, principal: str | None) -> bool:
        self._requests += 1
        if principal is not None:
            self._principal_requests[principal] = self._principal_requests.get(principal, 0) + 1
        return self._requests <= self.limits.requests and (principal is None or self._principal_requests[principal] <= self.limits.requests)

    def authorized(self, credential: Credential, dataset: Dataset, columns: tuple[str, ...], action: str, n: int) -> Grant | None:
        if not credential.valid_from_ns <= n < credential.expires_at_ns or not dataset.valid_from_ns <= n < dataset.expires_at_ns:
            return None
        if action not in dataset.rights:
            return None
        for g in self.grants:
            if (g.principal == credential.principal and g.dataset_id == dataset.identity.dataset_id
                    and g.dataset_revision == dataset.identity.revision and g.policy_revision == self.policy_revision
                    and g.grant_id not in self._revoked and g.valid_from_ns <= n < g.expires_at_ns
                    and action in g.actions and g.scope == dataset.scope and set(columns) <= g.columns):
                # Complete feature producer context/metadata is exposed; its whole approved feature vocabulary also needs scope.
                if dataset.kind == "feature" and not set(dataset.columns) <= g.columns:
                    continue
                return g
        return None

    def transfer(self, principal: str | None, size: int) -> bool:
        used = self._principal_transfer.get(principal, 0) if principal is not None else 0
        if self._transfer + self._reserved + size > self.limits.global_transfer or (principal is not None and used + self._principal_reserved.get(principal, 0) + size > self.limits.principal_transfer):
            return False
        self._transfer += size
        if principal is not None:
            self._principal_transfer[principal] = used + size
        return True

    def reserve(self, principal: str, size: int) -> bool:
        if not 0 <= size <= codec.MAX_RESPONSE:
            return False
        if (self._transfer + self._reserved + size > self.limits.global_transfer
                or self._principal_transfer.get(principal, 0) + self._principal_reserved.get(principal, 0) + size > self.limits.principal_transfer):
            return False
        self._reserved += size
        self._principal_reserved[principal] = self._principal_reserved.get(principal, 0) + size
        return True

    def release(self, principal: str, size: int) -> None:
        self._reserved -= size
        self._principal_reserved[principal] -= size


@dataclass
class _Prepared:
    status: int
    envelope: codec.Json
    credential: Credential | None = None
    dataset: Dataset | None = None
    columns: tuple[str, ...] = ()
    action: str = ""
    reserved_bytes: int = 0


def _error(status: int, category: str, code: str, version: str = "1.0", request_id: str | None = None) -> _Prepared:
    return _Prepared(status, {"schema": "equity.remote", "version": version, "kind": "error", "request_id": request_id,
                             "payload": {"category": category, "code": code, "retryable": False}})


class _CurrentGrant:
    """Cooperative native cancellation; no uploaded callback or hard preemption."""
    def __init__(self, ledger: Ledger, credential: Credential, dataset: Dataset,
                 columns: tuple[str, ...], action: str) -> None:
        self.ledger, self.credential, self.dataset = ledger, credential, dataset
        self.columns, self.action = columns, action

    def is_cancelled(self) -> bool:
        with self.ledger.lock:
            try:
                return self.ledger.authorized(self.credential, self.dataset, self.columns,
                                              self.action, self.ledger.now()) is None
            except Exception:
                return True


class Service:
    def __init__(self, ledger: Ledger) -> None:
        if type(ledger) is not Ledger:
            raise ValueError("shared_ledger_required")
        self.ledger = ledger

    def _prepare(self, env: dict[str, Any]) -> _Prepared:
        ledger = self.ledger
        credential = None
        try:
            with ledger.lock:
                n = ledger.now()
                credential = ledger.authenticate(env.get("HTTP_AUTHORIZATION", ""), n)
                if not ledger.admit_attempt(credential.principal if credential else None):
                    result = _error(429, "quota", "quota_exceeded")
                    result.credential = credential
                    return result
            if credential is None:
                return _error(401, "authentication", "unauthenticated")
            if env.get("wsgi.multiprocess", True) or not ipaddress.ip_address(env.get("REMOTE_ADDR", "")).is_loopback:
                raise codec.WireError("invalid_schema")
            if env.get("QUERY_STRING") or env.get("REQUEST_METHOD") != "POST" or env.get("PATH_INFO") != "/v1/request":
                raise codec.WireError("invalid_schema")
            if env.get("CONTENT_TYPE") != "application/json" or env.get("HTTP_TRANSFER_ENCODING"):
                raise codec.WireError("invalid_schema")
            text = env.get("CONTENT_LENGTH", "")
            if type(text) is not str or re.fullmatch(r"0|[1-9][0-9]{0,5}", text) is None:
                raise codec.WireError("invalid_schema")
            size = int(text)
            if size > codec.MAX_REQUEST:
                raise codec.WireError("bounds")
            data = env["wsgi.input"].read(size)
            if len(data) != size:
                raise codec.WireError("invalid_json")
            request = codec.decode(data)
            version, rid, payload = request["version"], request["request_id"], request["payload"]
            op = payload["operation"]
            if op == "discover":
                features: set[tuple[str, str]] = set()
                admitted = False
                with ledger.lock:
                    n = ledger.now()
                    for discovery_dataset in ledger.datasets.values():
                        if ledger.authorized(credential, discovery_dataset, (), "discover", n):
                            admitted = True
                            features.update(discovery_dataset.features)
                if not admitted:
                    result = _error(403, "authorization", "not_permitted", version, rid)
                else:
                    versions = ["1.0"] if version == "1.0" else ["1.0", "1.1"]
                    result = _Prepared(200, {"schema": "equity.remote", "version": version, "kind": "discovery", "request_id": rid,
                        "payload": {"transport_versions": versions, "registry_snapshot": ledger.registry_snapshot,
                                    "features": [{"feature_id": f, "algorithm_version": a} for f, a in sorted(features)]}}, credential, action="discover")
            elif op == "slice":
                dataset = ledger.datasets.get(payload["dataset"]["dataset_id"])
                columns = tuple(payload["columns"])
                action = "raw_read" if dataset is not None and dataset.kind == "raw" else "derived_read"
                with ledger.lock:
                    n = ledger.now()
                    allowed = (dataset is not None and payload["dataset"] == dataset.identity.wire()
                        and payload["scope"] == dataset.scope.wire() and len(set(columns)) == len(columns)
                        and bool(columns) and set(columns) <= set(dataset.columns)
                        and ledger.authorized(credential, dataset, columns, action, n) is not None)
                if not allowed or dataset is None:
                    result = _error(403, "authorization", "not_permitted", version, rid)
                elif payload["cursor"] is not None:
                    result = _error(400, "transport", "invalid_schema", version, rid)
                elif dataset.kind == "feature" and version != "1.1":
                    result = _error(400, "transport", "incompatible_version", version, rid)
                else:
                    amount = dataset.size_hint(columns, version, rid)
                    with ledger.lock:
                        current = ledger.authorized(credential, dataset, columns, action, ledger.now()) is not None
                        held = current and ledger.reserve(credential.principal, amount)
                    if not current:
                        result = _error(403, "authorization", "not_permitted", version, rid)
                    elif not held:
                        result = _error(429, "quota", "quota_exceeded", version, rid)
                    else:
                        try:
                            kind, body = dataset.produce(columns, version, _CurrentGrant(ledger, credential, dataset, columns, action))
                            result = _Prepared(200, {"schema": "equity.remote", "version": version, "kind": kind,
                                "request_id": rid, "payload": body}, credential, dataset, columns, action)
                        except codec.WireError as e:
                            result = (_error(429, "quota", "quota_exceeded", version, rid) if e.code == "bounds"
                                      else _error(422, "contract", e.code, version, rid))
                        except ContractError as e:
                            result = _error(422, "contract", e.code.value, version, rid)
                        except Exception:
                            result = _error(500, "internal", "internal_error", version, rid)
                        result.dataset, result.columns, result.action = dataset, columns, action
                        result.reserved_bytes = amount
            else:
                result = _error(400, "transport", "invalid_schema", version, rid)
            result.credential = credential
            return result
        except codec.WireError as error:
            result = _error(400, "transport", error.code)
        except Exception:
            result = _error(500, "internal", "internal_error")
        result.credential = credential
        return result

    def __call__(self, env: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        return _Emission(self, self._prepare(env), start_response)


class _Emission(Iterator[bytes]):
    def __init__(self, service: Service, prepared: _Prepared, start_response: StartResponse) -> None:
        self.service, self.prepared, self.start_response = service, prepared, start_response
        self.emitted = False

    def __iter__(self) -> _Emission:
        return self

    def __next__(self) -> bytes:
        if self.emitted:
            raise StopIteration
        self.emitted = True
        ledger, p = self.service.ledger, self.prepared
        with ledger.lock:
            if p.reserved_bytes and p.credential is not None:
                ledger.release(p.credential.principal, p.reserved_bytes)
                p.reserved_bytes = 0
            try:
                n = ledger.now()
                c = p.credential
                if (p.status == 200 or p.dataset is not None) and c is not None:
                    if not c.valid_from_ns <= n < c.expires_at_ns:
                        p = _error(401, "authentication", "unauthenticated", p.envelope["version"], p.envelope["request_id"])
                    elif p.action == "discover":
                        # Rebuild permitted metadata after concurrent revocation; no stale vocabulary escapes.
                        features: set[tuple[str, str]] = set()
                        admitted = False
                        for dataset in ledger.datasets.values():
                            if ledger.authorized(c, dataset, (), "discover", n):
                                admitted = True
                                features.update(dataset.features)
                        if not admitted:
                            p = _error(403, "authorization", "not_permitted", p.envelope["version"], p.envelope["request_id"])
                        else:
                            p.envelope["payload"]["features"] = [{"feature_id": f, "algorithm_version": a} for f, a in sorted(features)]
                    elif p.dataset is None or ledger.authorized(c, p.dataset, p.columns, p.action, n) is None:
                        p = _error(403, "authorization", "not_permitted", p.envelope["version"], p.envelope["request_id"])
                try:
                    data = codec.encode(p.envelope)
                except codec.WireError:
                    p = _error(429, "quota", "quota_exceeded")
                    data = codec.encode(p.envelope)
                principal = self.prepared.credential.principal if self.prepared.credential else None
                if not ledger.transfer(principal, len(data)):
                    p = _error(429, "quota", "quota_exceeded")
                    data = codec.encode(p.envelope)
                    if not ledger.transfer(principal, len(data)):
                        data = b""  # No free error bytes once transfer allowance is exhausted.
            except Exception:
                p, data = _error(500, "internal", "internal_error"), b""
            phrase = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 422: "Unprocessable Entity", 429: "Too Many Requests", 500: "Internal Server Error"}[p.status]
            headers = [("Content-Type", "application/json"), ("Content-Length", str(len(data))),
                       ("Cache-Control", "no-store"), ("X-Content-Type-Options", "nosniff")]
            if p.status == 401:
                headers.append(("WWW-Authenticate", "Bearer"))
            # This single bounded chunk is the authorization/transfer visibility linearization point.
            self.start_response(str(p.status) + " " + phrase, headers)
            return data

    def close(self) -> None:
        if not self.emitted:
            with self.service.ledger.lock:
                p = self.prepared
                if p.reserved_bytes and p.credential is not None:
                    self.service.ledger.release(p.credential.principal, p.reserved_bytes)
                    p.reserved_bytes = 0
            self.emitted = True
