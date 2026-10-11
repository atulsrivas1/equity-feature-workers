"""Finite process-local references; no payloads or remote authority claims."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import re
import secrets
from typing import Callable

from .models import MCPProfile
from ._wire import canonical

MAX_NS = (1 << 63) - 1


class ReferenceError(Exception):
    """Fixed internal denial code, never caller or provider text."""


@dataclass(frozen=True, slots=True)
class Record:
    reference: str
    process_nonce: str
    owner_context: str
    profile_id: str
    profile_sha256: str
    command_digest: str
    job_id: str
    result_id: str | None
    mode: str
    state: str
    stable_key_sha256: str | None
    origin_job_reference: str | None
    created_ns: int
    expires_ns: int


@dataclass(frozen=True, slots=True)
class Reservation:
    token: str
    profile_id: str
    stable_key_sha256: str | None
    created_ns: int
    expires_ns: int


class Ledger:
    def __init__(self, profile: MCPProfile, clock: Callable[[], int]) -> None:
        if type(profile) is not MCPProfile or not callable(clock):
            raise ValueError('profile_invalid')
        self.profile = profile
        self._snapshot = profile.snapshot
        self._clock = clock
        self._last = -1
        self._closed = False
        self._nonce = secrets.token_hex(32)
        self._records: dict[str, Record] = {}
        self._reservations: dict[str, Reservation] = {}

    def close(self) -> None:
        self._closed = True
        self._records.clear()
        self._reservations.clear()

    def sample(self) -> int:
        if self._closed:
            raise ReferenceError('profile_invalid')
        try:
            now = self._clock()
            valid = type(now) is int and self._last <= now <= MAX_NS and now >= 0
            valid = valid and self.profile.snapshot == self._snapshot
        except BaseException:
            valid = False
            now = -1
        if not valid:
            self.close()
            raise ReferenceError('profile_invalid') from None
        self._last = now
        for record in tuple(self._records.values()):
            if record.state == 'live' and now >= record.expires_ns:
                self.invalidate(record)
        return now

    def _expiry(self, now: int) -> int:
        expires = now + self.profile.reference_ttl_seconds * 1000000000
        if expires > MAX_NS:
            self.close()
            raise ReferenceError('profile_invalid')
        return expires

    def _token(self) -> str:
        # A collision is a bounded denial, never an unbounded retry loop.
        token = secrets.token_urlsafe(32)
        if re.fullmatch(r'[A-Za-z0-9_-]{43}', token) is None or token in self._records or token in self._reservations:
            raise ReferenceError('reference_capacity')
        return token

    def reserve(self, profile_id: str, key: str | None = None, *, origin: Record | None = None) -> Reservation:
        now = self.sample()
        expires = self._expiry(now)  # Checked even for a reusable live key.
        if profile_id not in {v.profile_id for v in self.profile.commands}:
            raise ReferenceError('unknown_registration')
        digest = None
        if key is not None:
            if type(key) is not str or re.fullmatch(r'[\x20-\x7e]{1,128}', key) is None:
                raise ReferenceError('invalid_arguments')
            digest = hashlib.sha256(key.encode('ascii')).hexdigest()
            if any(r.profile_id == profile_id and r.stable_key_sha256 == digest and r.state == 'tombstone' for r in self._records.values()):
                raise ReferenceError('reference_expired')
        reusable = next((r for r in self._records.values() if r.profile_id == profile_id and digest is not None and r.stable_key_sha256 == digest and r.mode == 'job' and r.state == 'live'), None)
        if origin is not None:
            if self.lookup(origin.reference) is not origin or origin.mode != 'job' or origin.profile_id != profile_id:
                raise ReferenceError('profile_invalid')
            reusable = next((r for r in self._records.values() if r.origin_job_reference == origin.reference and r.mode == 'artifact' and r.state == 'live'), None)
        if reusable is None and len(self._records.keys() | self._reservations.keys()) >= 16:
            raise ReferenceError('reference_capacity')
        token = self._token() if reusable is None else reusable.reference
        if token in self._reservations:
            raise ReferenceError('reference_capacity')
        reservation = Reservation(token, profile_id, digest, now if reusable is None else reusable.created_ns,
                                  (expires if origin is None else min(expires, origin.expires_ns)) if reusable is None else reusable.expires_ns)
        self._reservations[reservation.token] = reservation
        return reservation

    def release(self, reservation: Reservation) -> None:
        if self._reservations.get(reservation.token) is reservation:
            del self._reservations[reservation.token]

    def lookup(self, reference: str) -> Record:
        self.sample()
        record = self._records.get(reference) if type(reference) is str else None
        if record is None:
            raise ReferenceError('unknown_reference')
        if record.state != 'live':
            raise ReferenceError('reference_expired')
        return record

    def invalidate(self, record: Record) -> None:
        for token, linked in tuple(self._records.items()):
            if linked.job_id == record.job_id:
                self._records[token] = replace(linked, state='tombstone')

    def commit(self, reservation: Reservation, command_digest: str, job_id: str,
               result_id: str | None = None, origin: Record | None = None) -> Record:
        try:
            now = self.sample()
            if self._reservations.get(reservation.token) is not reservation:
                raise ReferenceError('reference_capacity')
            command = next(v for v in self.profile.commands if v.profile_id == reservation.profile_id)
            if command_digest != command.expectation.command_digest:
                raise ReferenceError('profile_invalid')
            if type(job_id) is not str or re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', job_id) is None:
                raise ReferenceError('profile_invalid')
            if result_id is not None and (type(result_id) is not str or re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', result_id) is None):
                raise ReferenceError('profile_invalid')
            linked = [r for r in self._records.values() if r.profile_id == reservation.profile_id and r.job_id == job_id]
            previous = self._records.get(reservation.token)
            if previous is not None and previous.job_id != job_id:
                raise ReferenceError('profile_invalid')
            if any(r.job_id == job_id and r.state == 'tombstone' for r in self._records.values()):
                raise ReferenceError('reference_expired')
            mode = 'job' if origin is None else 'artifact'
            if origin is not None:
                current = self.lookup(origin.reference)
                if current is not origin or origin.mode != 'job' or origin.job_id != job_id or origin.profile_id != reservation.profile_id or result_id is None:
                    raise ReferenceError('profile_invalid')
            for existing in linked:
                if existing.mode == mode:
                    if result_id is not None and existing.result_id not in (None, result_id):
                        raise ReferenceError('profile_invalid')
                    return self.set_result(existing, result_id) if result_id is not None else existing
            record = Record(reservation.token, self._nonce, self.profile.owner_context_id,
                            reservation.profile_id, self.profile.snapshot_sha256, command_digest,
                            job_id, result_id, mode, 'tombstone' if now >= reservation.expires_ns else 'live', reservation.stable_key_sha256,
                            None if origin is None else origin.reference, reservation.created_ns,
                            reservation.expires_ns if origin is None else min(reservation.expires_ns, origin.expires_ns))
            encoded = canonical(asdict(record))
            if len(encoded) > 4096 or sum(len(canonical(asdict(r))) for r in self._records.values()) + len(encoded) + (len(self._reservations) - 1) * 4096 > 65536:
                raise ReferenceError('reference_capacity')
            self._records[record.reference] = record
            if record.state == 'tombstone':
                raise ReferenceError('reference_expired')
            return record
        finally:
            self.release(reservation)

    def set_result(self, record: Record, result_id: str) -> Record:
        current = self.lookup(record.reference)
        if current is not record or type(result_id) is not str or re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', result_id) is None or record.result_id not in (None, result_id):
            raise ReferenceError('profile_invalid')
        if record.result_id == result_id:
            return record
        updated = replace(record, result_id=result_id)
        self._records[record.reference] = updated
        return updated
