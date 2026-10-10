"""Internal finite verified-result representations; no authorization or I/O."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any


def canonical_record(value: Any) -> bytes:
    """Bound the complete record, preserving integer and ordered tuple semantics."""
    def normalize(v: Any) -> Any:
        if isinstance(v, frozenset):
            return sorted(normalize(x) for x in v)
        if isinstance(v, (tuple, list)):
            return [normalize(x) for x in v]
        if type(v) is dict:
            return {k: normalize(x) for k, x in v.items()}
        if v is None or type(v) in (str, int, bool):
            return v
        raise ValueError('invalid_cache_record')
    encoder = json.JSONEncoder(sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)
    chunks: list[str] = []
    used = 0
    for chunk in encoder.iterencode(normalize(value)):
        used += len(chunk)
        if used > 65_536:
            raise ValueError('cache_record_bounds')
        chunks.append(chunk)
    return ''.join(chunks).encode('ascii')


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_record(value)).hexdigest()


_FIELDS = frozenset(('schema', 'principal', 'original_credential_digest', 'grant_sha256',
    'job_grant_sha256', 'rights_sha256', 'dataset_identity', 'acquisition_commitment',
    'context_sha256', 'registration_sha256', 'result_id', 'epoch', 'native_content_sha256',
    'committed_receipt_sha256', 'producer_wire_sha256', 'operation', 'projection', 'transport_version'))
_DIGESTS = frozenset(('original_credential_digest', 'grant_sha256', 'job_grant_sha256',
    'rights_sha256', 'context_sha256', 'registration_sha256', 'native_content_sha256',
    'committed_receipt_sha256', 'producer_wire_sha256'))


def identity_key(record: dict[str, Any]) -> str:
    if (type(record) is not dict or record.keys() != _FIELDS or record['schema'] != 'cache.identity.v1'
            or record['operation'] not in ('result_read', 'artifact_read') or record['transport_version'] != '1.1'
            or any(type(record[k]) is not str or re.fullmatch('[0-9a-f]{64}', record[k]) is None for k in _DIGESTS)
            or any(type(record[k]) is not str or not 0 < len(record[k]) <= 256 for k in ('principal', 'result_id', 'epoch'))
            or type(record['projection']) is not list or not 1 <= len(record['projection']) <= 39
            or any(type(x) is not str or not 0 < len(x) <= 256 for x in record['projection'])
            or type(record['dataset_identity']) is not dict
            or record['dataset_identity'].keys() != {'dataset_id','revision','snapshot_id','mapping_version','source_id','input_id'}
            or any(type(x) is not str or not 0 < len(x) <= 256 for x in record['dataset_identity'].values())
            or type(record['acquisition_commitment']) is not dict
            or record['acquisition_commitment'].keys() != {'content_sha256', 'receipt_sha256'}
            or any(type(x) is not str or re.fullmatch('[0-9a-f]{64}', x) is None
                   for x in record['acquisition_commitment'].values())):
        raise ValueError('invalid_cache_identity')
    return fingerprint(record)


@dataclass(frozen=True)
class _Entry:
    principal: str
    result_id: str
    expires_ns: int
    payload: bytes


class ResultCache:
    """Caller must hold the same ledger lock as native result reservations."""
    def __init__(self) -> None:
        self.entries: dict[str, _Entry] = {}

    @property
    def total(self) -> int:
        return sum(len(e.payload) for e in self.entries.values())

    def principal_bytes(self, principal: str) -> int:
        return sum(len(e.payload) for e in self.entries.values() if e.principal == principal)

    def purge(self, now: int, result_id: str | None = None) -> None:
        for key, entry in tuple(self.entries.items()):
            if now >= entry.expires_ns or result_id is not None and entry.result_id == result_id:
                del self.entries[key]

    def representation(self, key: str, principal: str, result_id: str, payload: bytes,
                       expires_ns: int, now: int, job_principal_bytes: int, job_total_bytes: int) -> bytes:
        self.purge(now)
        entry = self.entries.get(key)
        # A key is not authorization or integrity evidence: caller independently
        # verifies complete native forms and supplies the exact current payload.
        if entry is not None:
            if (entry.principal == principal and entry.result_id == result_id
                    and entry.expires_ns == expires_ns and entry.payload == payload):
                return entry.payload
            del self.entries[key]
        own = [e for e in self.entries.values() if e.principal == principal]
        if (now < expires_ns and len(self.entries) < 8 and len(own) < 4
                and self.principal_bytes(principal) + len(payload) <= 131_072
                and self.total + len(payload) <= 262_144
                and job_principal_bytes + self.principal_bytes(principal) + len(payload) <= 1_048_576
                and job_total_bytes + self.total + len(payload) <= 2_097_152):
            self.entries[key] = _Entry(principal, result_id, expires_ns, payload)
        return payload
