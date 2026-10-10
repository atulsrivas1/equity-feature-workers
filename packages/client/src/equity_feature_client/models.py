"""Owned immutable request and expectation values with closed failures."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Generic, Literal, Mapping, TypeVar, cast

from . import _wire


@dataclass(frozen=True, slots=True)
class DatasetKey:
    dataset_id: str
    revision: str
    snapshot_id: str
    mapping_version: str
    source_id: str
    input_id: str

    def __post_init__(self) -> None:
        if any(type(getattr(self, name)) is not str for name in self.__slots__):
            raise ValueError('invalid_expectation')
        _wire.definition('dataset', self.payload_json())

    def payload_json(self) -> dict[str, str]:
        return {name: getattr(self, name) for name in self.__slots__}


@dataclass(frozen=True, slots=True)
class ScopeKey:
    instrument_id: str
    session_id: str
    start_ns: int
    end_ns: int

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not str or type(self.session_id) is not str or type(self.start_ns) is not int or type(self.end_ns) is not int:
            raise ValueError('invalid_expectation')
        _wire.definition('scope', self.payload_json())

    def payload_json(self) -> dict[str, str]:
        return dict(instrument_id=self.instrument_id, session_id=self.session_id,
                    start_ns=str(self.start_ns), end_ns=str(self.end_ns))


@dataclass(frozen=True, slots=True)
class FeatureRef:
    feature_id: str
    algorithm_version: str

    def __post_init__(self) -> None:
        if type(self.feature_id) is not str or type(self.algorithm_version) is not str:
            raise ValueError('invalid_expectation')
        _wire.definition('feature', self.payload_json())

    def payload_json(self) -> dict[str, str]:
        return dict(feature_id=self.feature_id, algorithm_version=self.algorithm_version)


def _features(values: object) -> tuple[FeatureRef, ...]:
    if type(values) not in (tuple, list):
        raise ValueError('invalid_expectation')
    result = tuple(cast(tuple[FeatureRef, ...] | list[FeatureRef], values))
    if not 1 <= len(result) <= 39 or any(type(v) is not FeatureRef for v in result):
        raise ValueError('invalid_expectation')
    if len({v.feature_id for v in result}) != len(result):
        raise ValueError('invalid_expectation')
    return result


@dataclass(frozen=True, slots=True)
class RawExpectation:
    dataset: DatasetKey
    scope: ScopeKey
    data_kind: Literal['trade', 'bar', 'quote']
    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.dataset) is not DatasetKey or type(self.scope) is not ScopeKey or type(self.data_kind) is not str or self.data_kind not in ('trade', 'bar', 'quote'):
            raise ValueError('invalid_expectation')
        if type(self.columns) not in (tuple, list):
            raise ValueError('invalid_expectation')
        columns = tuple(self.columns)
        if any(type(column) is not str for column in columns):
            raise ValueError('invalid_expectation')
        _wire.definition('request', dict(operation='slice', dataset=self.dataset.payload_json(),
                         scope=self.scope.payload_json(), columns=list(columns), cursor=None))
        object.__setattr__(self, 'columns', columns)


@dataclass(frozen=True, slots=True, init=False)
class ProducerExpectation:
    _context: bytes
    scope: ScopeKey
    executed_features: tuple[FeatureRef, ...]
    backend_id: str
    backend_version: str
    command_digest: str

    def __init__(self, context: Mapping[str, object], scope: ScopeKey,
                 executed_features: tuple[FeatureRef, ...], backend_id: str, backend_version: str) -> None:
        if not isinstance(context, Mapping) or type(scope) is not ScopeKey:
            raise ValueError('invalid_expectation')
        data = _wire.canonical(dict(context))
        copied = json.loads(data)
        _wire.definition('context', copied)
        inventory = _features(executed_features)
        for label in (backend_id, backend_version):
            if type(label) is not str:
                raise ValueError('invalid_expectation')
            _wire.definition('label', label)
        by_id = {v.feature_id: v.algorithm_version for v in inventory}
        if any(by_id.get(v['feature_id']) != v['algorithm_version'] for v in copied['features']):
            raise ValueError('invalid_expectation')
        digest = hashlib.sha256(_wire.canonical(dict(operation='calculate', context=copied, scope=scope.payload_json()))).hexdigest()
        for name, value in (('_context', data), ('scope', scope), ('executed_features', inventory),
                            ('backend_id', backend_id), ('backend_version', backend_version), ('command_digest', digest)):
            object.__setattr__(self, name, value)

    def context_json(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self._context))


@dataclass(frozen=True, slots=True)
class JobExpectation:
    job_id: str
    command_digest: str

    def __post_init__(self) -> None:
        if type(self.job_id) is not str or type(self.command_digest) is not str:
            raise ValueError('invalid_expectation')
        _wire.definition('id', self.job_id)
        _wire.definition('digest', self.command_digest)


@dataclass(frozen=True, slots=True, init=False)
class Request:
    version: str
    request_id: str
    encoded: bytes

    def __init__(self, version: str, request_id: str, payload: Mapping[str, object]) -> None:
        if type(version) is not str or type(request_id) is not str or not isinstance(payload, Mapping):
            raise ValueError('invalid_request')
        value = dict(schema='equity.remote', version=version, request_id=request_id, kind='request', payload=dict(payload))
        data = _wire.canonical(value)
        # Validate exactly the snapshot that will be sent, never caller-owned
        # nested mappings that can mutate between encoding and validation.
        _wire.validate(_wire.parse(data, limit=_wire.REQUEST_LIMIT))
        object.__setattr__(self, 'version', version)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'encoded', data)

    def payload_json(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self.encoded)['payload'])


_CATEGORIES = frozenset(('configuration', 'credentials', 'transport', 'protocol', 'authorization', 'contract', 'quota', 'internal', 'capability', 'client', 'unknown'))
_PHASES = frozenset(('prepare', 'provider', 'connect', 'send', 'read', 'decode', 'convert'))
_CODES = frozenset(('invalid_configuration', 'invalid_origin', 'invalid_request', 'invalid_expectation', 'token_invalid', 'provider_failed', 'client_closed', 'busy', 'timeout', 'disconnected', 'tls_verification_failed', 'invalid_response', 'inconsistent_identity', 'bounds', 'remote_denial', 'capability_unavailable', 'invalid_conversion', 'outcome_unknown', 'cancelled'))


@dataclass(frozen=True, slots=True)
class Failure:
    category: str
    code: str
    phase: str
    status: int | None = None

    def __post_init__(self) -> None:
        if type(self.category) is not str or self.category not in _CATEGORIES or type(self.code) is not str or self.code not in _CODES or type(self.phase) is not str or self.phase not in _PHASES:
            raise ValueError('invalid_configuration')
        if self.status is not None and (type(self.status) is not int or not 100 <= self.status <= 599):
            raise ValueError('invalid_configuration')


T = TypeVar('T')


@dataclass(frozen=True, slots=True)
class Outcome(Generic[T]):
    view: T | None = None
    failure: Failure | None = None

    def __post_init__(self) -> None:
        if (self.view is None) == (self.failure is None) or (self.failure is not None and type(self.failure) is not Failure):
            raise ValueError('invalid_configuration')

    @property
    def ok(self) -> bool:
        return self.failure is None
