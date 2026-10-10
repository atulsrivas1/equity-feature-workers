"""Finite owned-synthetic diagnostics. Caller text is never an audit field.

The controller holds its shared ledger lock for every store operation.
Reservations count until actual HTTP/native completion, independently of TTL.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import re
import secrets
from typing import Any

_MAX = 2**63 - 1
_TTL = 300_000_000_000
_ACTIONS = frozenset(('discover','slice','calculate','job_status','job_cancel','result_read','artifact_read','unknown'))
_DECISIONS = frozenset(('admitted','emitted','denied_auth','denied_contract','denied_quota','internal_failure',
                       'abandoned','started','succeeded','failed','cancelled','expired','historical_commit'))
_STAGES = frozenset(('attempt','finish','native_start','native_finish'))
_TOTALS = ('request_used','transfer_used','transfer_reserved','retained_used','cache_used')


class AuditDenied(RuntimeError):
    pass


@dataclass(frozen=True, eq=False)
class AuditReadPermit:
    valid_from_ns: int
    expires_at_ns: int
    action: str = 'audit_read'


@dataclass(eq=False)
class AuditReservation:
    remaining: int
    actor: str | None
    command: str | None
    grant: str | None
    policy: str | None
    action: str


class OwnedAudit:
    """Explicit operator admission for owned synthetic diagnostics only."""
    def __init__(self, *, owned_synthetic: bool, valid_from_ns: int, expires_at_ns: int) -> None:
        if (owned_synthetic is not True or type(valid_from_ns) is not int or type(expires_at_ns) is not int
                or not 0 <= valid_from_ns < expires_at_ns <= _MAX or expires_at_ns-valid_from_ns > _TTL):
            raise ValueError('unadmitted_audit')
        self.permit = AuditReadPermit(valid_from_ns,expires_at_ns)
        self._secret = secrets.token_bytes(32)
        self._reservations: dict[int,AuditReservation] = {}
        self._records: list[tuple[int,bytes]] = []
        self._bytes = 0
        self._sequence = 0
        self._last_ns: int | None = None
        self.overflow = 0
        self.failures = 0
        self._revoked = False
        self._owner: object | None = None

    def attach(self, owner: object) -> None:
        if self._owner is not None and self._owner is not owner:
            raise ValueError('shared_audit_ledger_required')
        self._owner = owner

    def _deny(self, *, overflow: bool = False) -> None:
        if overflow:
            self.overflow = min(_MAX,self.overflow+1)
        else:
            self.failures = min(_MAX,self.failures+1)
        raise AuditDenied('audit_unavailable')

    def fault(self) -> None:
        self.failures = min(_MAX,self.failures+1)

    def _now(self, n: int) -> None:
        if type(n) is not int or not 0 <= n <= _MAX or self._last_ns is not None and n < self._last_ns:
            self._deny()
        self._last_ns = n
        kept = [(at,body) for at,body in self._records if n-at < _TTL]
        self._records = kept
        self._bytes = sum(len(body) for _,body in kept)

    def purge(self, n: int) -> None:
        self._now(n)

    def _ref(self, domain: str, value: str | None) -> str | None:
        if value is None:
            return None
        if type(value) is not str or not 0 < len(value) <= 256:
            self._deny()
        # Server record only. A per-epoch secret and domain prevent correlation
        # across stores and prevent a command reference from matching an actor.
        return hmac.new(self._secret,(domain+'\0'+value).encode('utf-8'),hashlib.sha256).hexdigest()[:32]

    @property
    def reserved_count(self) -> int:
        return sum(r.remaining for r in self._reservations.values())

    def reserve(self, n: int, *, actor: str | None = None, command: str | None = None,
                grant: str | None = None, policy: str | None = None, action: str = 'unknown') -> AuditReservation:
        self._now(n)
        if action not in _ACTIONS:
            self._deny()
        if len(self._records)+self.reserved_count+2 > 128 or self._bytes+(self.reserved_count+2)*512 > 65_536:
            self._deny(overflow=True)
        refs = [self._ref(domain,value) for domain,value in zip(('actor','command','grant','policy'),(actor,command,grant,policy))]
        reservation = AuditReservation(2,refs[0],refs[1],refs[2],refs[3],action)
        self._reservations[id(reservation)] = reservation
        return reservation

    def bind(self, reservation: AuditReservation, *, command: str | None = None,
             grant: str | None = None, action: str = 'unknown') -> None:
        self._owned(reservation)
        if action not in _ACTIONS:
            self._deny()
        reservation.command, reservation.grant = self._ref('command',command),self._ref('grant',grant)
        reservation.action = action

    def _owned(self, reservation: AuditReservation) -> None:
        if self._reservations.get(id(reservation)) is not reservation or reservation.remaining <= 0:
            self._deny()

    def settle(self, reservation: AuditReservation, n: int, stage: str, decision: str,
               totals: dict[str,int]) -> None:
        body = self.prepare_settlement(reservation,n,stage,decision,totals)
        self.commit_settlement(reservation,n,body)

    def prepare_settlement(self, reservation: AuditReservation, n: int, stage: str, decision: str,
                           totals: dict[str,int]) -> bytes:
        self._owned(reservation)
        self._now(n)
        if (stage not in _STAGES or decision not in _DECISIONS or set(totals) != set(_TOTALS)
                or reservation.action not in _ACTIONS
                or any(v is not None and (type(v) is not str or re.fullmatch('[0-9a-f]{32}',v) is None)
                       for v in (reservation.actor,reservation.command,reservation.grant,reservation.policy))
                or any(type(v) is not int or not 0 <= v <= _MAX for v in totals.values()) or self._sequence == _MAX):
            self._deny()
        record: dict[str,Any] = dict(sequence=self._sequence,time_ns=n,stage=stage,actor=reservation.actor,
            command=reservation.command,grant=reservation.grant,policy=reservation.policy,
            action=reservation.action,decision=decision,**totals)
        body = json.dumps(record,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')
        if len(body) > 512:
            self._deny()
        return body

    def commit_settlement(self, reservation: AuditReservation, n: int, body: bytes) -> None:
        # Called immediately under the same ledger lock as preparation. No clock,
        # encoding, user callbacks or new allocation-sized inputs are consulted.
        self._owned(reservation)
        self._records.append((n,body))
        self._bytes += len(body)
        self._sequence += 1
        reservation.remaining -= 1
        if reservation.remaining == 0:
            del self._reservations[id(reservation)]

    def abandon_after_exit(self, reservation: AuditReservation) -> None:
        """Clock/encoding failure: caller proves real exit, records failure only."""
        self._owned(reservation)
        del self._reservations[id(reservation)]
        reservation.remaining = 0
        self.fault()

    def revoke_reader(self) -> None:
        self._revoked = True

    def snapshot(self, permit: AuditReadPermit, n: int) -> bytes:
        self._now(n)
        if (permit is not self.permit or self._revoked or permit.action != 'audit_read'
                or not permit.valid_from_ns <= n < permit.expires_at_ns):
            self._deny()
        records = [json.loads(body) for _,body in self._records]
        body = json.dumps(dict(records=records,record_bytes=self._bytes,reserved_records=self.reserved_count,
            reserved_bytes=self.reserved_count*512,overflow=self.overflow,failures=self.failures),
            sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')
        if len(body) > 131_072:
            self._deny()
        return body
