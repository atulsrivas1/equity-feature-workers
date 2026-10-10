"""Owner-admitted immutable profiles over the public remote client API."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, cast

from equity_feature_client import DatasetKey, FeatureRef, ProducerExpectation, RawExpectation


def _label(value: object) -> str:
    if type(value) is not str or re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', value) is None:
        raise ValueError('profile_invalid')
    return value


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')


def _producer(value: ProducerExpectation) -> dict[str, Any]:
    if type(value) is not ProducerExpectation or len(value.executed_features) > 16:
        raise ValueError('profile_invalid')
    return dict(context=value.context_json(), scope=value.scope.payload_json(),
                executed_features=[v.payload_json() for v in value.executed_features],
                backend_id=value.backend_id, backend_version=value.backend_version,
                command_digest=value.command_digest)


@dataclass(frozen=True, slots=True)
class RawSliceRegistration:
    profile_id: str
    expectation: RawExpectation

    def __post_init__(self) -> None:
        _label(self.profile_id)
        if type(self.expectation) is not RawExpectation or not 1 <= len(self.expectation.columns) <= 16:
            raise ValueError('profile_invalid')

    def payload_json(self) -> dict[str, Any]:
        v = self.expectation
        return dict(kind='raw', profile_id=self.profile_id, dataset=v.dataset.payload_json(),
                    scope=v.scope.payload_json(), data_kind=v.data_kind, columns=list(v.columns))


@dataclass(frozen=True, slots=True)
class FeatureSliceRegistration:
    profile_id: str
    dataset: DatasetKey
    expectation: ProducerExpectation
    selected: tuple[FeatureRef, ...]

    def __post_init__(self) -> None:
        _label(self.profile_id)
        if type(self.dataset) is not DatasetKey or type(self.selected) is not tuple or not 1 <= len(self.selected) <= 16:
            raise ValueError('profile_invalid')
        _producer(self.expectation)
        if any(type(v) is not FeatureRef for v in self.selected):
            raise ValueError('profile_invalid')
        if len({v.feature_id for v in self.selected}) != len(self.selected):
            raise ValueError('profile_invalid')
        if any(v not in self.expectation.executed_features for v in self.selected):
            raise ValueError('profile_invalid')

    def payload_json(self) -> dict[str, Any]:
        return dict(kind='feature', profile_id=self.profile_id, dataset=self.dataset.payload_json(),
                    expectation=_producer(self.expectation), selected=[v.payload_json() for v in self.selected])


@dataclass(frozen=True, slots=True)
class CommandRegistration:
    profile_id: str
    expectation: ProducerExpectation

    def __post_init__(self) -> None:
        _label(self.profile_id)
        _producer(self.expectation)

    def payload_json(self) -> dict[str, Any]:
        return dict(kind='command', profile_id=self.profile_id, expectation=_producer(self.expectation))


@dataclass(frozen=True, slots=True, init=False)
class MCPProfile:
    owner_context_id: str
    slices: tuple[RawSliceRegistration | FeatureSliceRegistration, ...]
    commands: tuple[CommandRegistration, ...]
    reference_ttl_seconds: int
    snapshot: bytes
    snapshot_sha256: str

    def __init__(self, owner_context_id: str,
                 slices: tuple[RawSliceRegistration | FeatureSliceRegistration, ...],
                 commands: tuple[CommandRegistration, ...], reference_ttl_seconds: int = 60) -> None:
        _label(owner_context_id)
        if type(slices) is not tuple or type(commands) is not tuple:
            raise ValueError('profile_invalid')
        if any(type(v) not in (RawSliceRegistration, FeatureSliceRegistration) for v in slices) or any(type(v) is not CommandRegistration for v in commands):
            raise ValueError('profile_invalid')
        all_values = (*slices, *commands)
        if not 1 <= len(all_values) <= 8 or len({v.profile_id for v in all_values}) != len(all_values):
            raise ValueError('profile_invalid')
        if type(reference_ttl_seconds) is not int or not 1 <= reference_ttl_seconds <= 300:
            raise ValueError('profile_invalid')
        value = dict(owner_context_id=owner_context_id, slices=[v.payload_json() for v in slices],
                     commands=[v.payload_json() for v in commands], reference_ttl_seconds=reference_ttl_seconds)
        snapshot = _canonical(value)
        if len(snapshot) > 65536:
            raise ValueError('profile_invalid')
        for name, val in (('owner_context_id', owner_context_id), ('slices', slices), ('commands', commands),
                          ('reference_ttl_seconds', reference_ttl_seconds), ('snapshot', snapshot),
                          ('snapshot_sha256', hashlib.sha256(snapshot).hexdigest())):
            object.__setattr__(self, name, val)

    def payload_json(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self.snapshot))
