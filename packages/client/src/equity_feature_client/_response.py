"""Closed response views and caller-pinned transport identity checks."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, ClassVar, cast

from . import _wire
from .models import Failure, FeatureRef, JobExpectation, ProducerExpectation, RawExpectation, Request
from ._transport import Frame


@dataclass(frozen=True, slots=True)
class _View:
    encoded: bytes
    _kind: ClassVar[str] = ''

    def __post_init__(self) -> None:
        value = _wire.parse(self.encoded)
        _wire.validate(value)
        if value['kind'] != self._kind:
            raise ValueError('invalid_response')

    def payload_json(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self.encoded)['payload'])


class DiscoveryView(_View):
    __slots__ = ()
    _kind = 'discovery'


class RawSliceView(_View):
    __slots__ = ()
    _kind = 'slice'


class FeatureSliceView(_View):
    __slots__ = ()
    _kind = 'feature_slice'


class JobView(_View):
    __slots__ = ()
    _kind = 'job'


@dataclass(frozen=True, slots=True)
class ResultView(_View):
    filename: str | None = None
    _kind: ClassVar[str] = 'result'

    def __post_init__(self) -> None:
        _View.__post_init__(self)
        if self.filename is not None:
            import re
            if type(self.filename) is not str or re.fullmatch(r'result-[0-9a-f]{64}\.json', self.filename) is None:
                raise ValueError('invalid_response')


View = DiscoveryView | RawSliceView | FeatureSliceView | JobView | ResultView


def _identity(condition: bool) -> None:
    if not condition:
        raise ValueError('inconsistent_identity')


def _cell(record: dict[str, Any], name: str, tag: str) -> Any:
    value = record['fields'][name]
    if type(value) is not dict or value.get('type') != tag:
        raise ValueError('invalid_response')
    return value['value']


def _scope(metadata: dict[str, Any], expected: ProducerExpectation | RawExpectation) -> None:
    _identity(metadata.get('name') == 'BatchMetadata')
    scope = metadata['fields']['scope']
    _identity(scope.get('name') == 'InputScope')
    _identity(_cell(scope, 'start_ns', 'int64') == str(expected.scope.start_ns))
    _identity(_cell(scope, 'end_ns', 'int64') == str(expected.scope.end_ns))


def _source(metadata: dict[str, Any], dataset: dict[str, Any]) -> bool:
    source = metadata['fields']['source']
    return source.get('name') == 'SourceBinding' and all(_cell(source, name, 'string') == dataset[name] for name in ('source_id', 'snapshot_id', 'mapping_version', 'input_id'))


def _columns(columns: list[dict[str, Any]], *, raw: bool, expected: ProducerExpectation | RawExpectation) -> None:
    for column in columns:
        values = column['values']
        dtype = column['dtype']
        if raw:
            if type(expected) is not RawExpectation:
                raise ValueError('invalid_expectation')
            if column['name'] in ('instrument_id', 'session_id'):
                identity = expected.scope.instrument_id if column['name'] == 'instrument_id' else expected.scope.session_id
                _identity(all(v == {'type': 'string', 'value': identity} for v in values))
        else:
            _identity(len(values) == len(column['entities']))
            _identity(all(e == {'instrument_id': expected.scope.instrument_id, 'session_id': expected.scope.session_id} for e in column['entities']))
        for value in values:
            if value is not None:
                structured = {'interval_ohlcv': 'IntervalOHLCV', 'interval_volume_shares': 'IntervalVolumeShares',
                              'top_k_trades': 'TopKTrades', 'quote_state_counts': 'QuoteStateCounts',
                              'sampled_spread': 'SampledSpread', 'time_weighted_spread': 'TimeWeightedSpread',
                              'breadth_counts': 'BreadthCounts', 'breadth_fraction': 'BreadthFraction'}
                _identity(value.get('type') == ('record' if dtype in structured else dtype))
                if dtype in structured:
                    _identity(value.get('name') == structured[dtype])


def check_raw(payload: dict[str, Any], expected: RawExpectation) -> None:
    _identity(payload['dataset'] == expected.dataset.payload_json() and payload['scope'] == expected.scope.payload_json())
    _identity(payload['next_cursor'] is None)
    _identity(tuple(c['name'] for c in payload['columns']) == expected.columns)
    _identity(len({len(c['values']) for c in payload['columns']}) <= 1)
    _scope(payload['metadata'], expected)
    _identity(_source(payload['metadata'], expected.dataset.payload_json()))
    _columns(payload['columns'], raw=True, expected=expected)


def check_producer(payload: dict[str, Any], expected: ProducerExpectation,
                   selected: tuple[FeatureRef, ...] | None = None) -> None:
    _identity(payload['context'] == expected.context_json())
    _identity(payload['command_digest'] == expected.command_digest)
    _identity(payload['backend_id'] == expected.backend_id and payload['backend_version'] == expected.backend_version)
    _identity(payload['executed_features'] == [f.payload_json() for f in expected.executed_features])
    wanted = expected.executed_features if selected is None else selected
    _identity([(c['feature_id'], c['algorithm_version']) for c in payload['columns']] == [(f.feature_id, f.algorithm_version) for f in wanted])
    _identity(payload['next_cursor'] is None)
    metadata = payload['metadata']
    _identity(metadata.get('name') == 'ResultMetadata')
    context = expected.context_json()
    for name, value in (('namespace', context['namespace']), ('math_policy_version', context['math_policy_version']),
                        ('config_digest', context['config']['digest']), ('session_id', expected.scope.session_id),
                        ('backend_id', expected.backend_id), ('backend_version', expected.backend_version)):
        _identity(_cell(metadata, name, 'string') == value)
    availability = metadata['fields']['availability']
    _identity(availability.get('name') == 'AvailabilitySpec')
    for name, value in context['availability'].items():
        if value is None:
            _identity(availability['fields'][name] is None)
        else:
            _identity(_cell(availability, name, 'int64' if name.endswith('_ns') else 'string') == value)
    inputs = metadata['fields']['inputs']
    # Initial admitted service inventory is one SessionCommandSpec source.
    # The frozen expectation pins exactly that source, never extra provenance.
    _identity(inputs.get('type') == 'list' and len(inputs['items']) == 1)
    for binding in inputs['items']:
        _identity(binding.get('name') == 'InputBinding')
        md = binding['fields']['metadata']
        _scope(md, expected)
        _identity(_source(md, context['dataset']))
        _identity(_cell(md, 'namespace', 'string') == context['namespace'])
    _columns(payload['columns'], raw=False, expected=expected)
    feature_ids = {f.feature_id for f in wanted}
    quality_keys = [(q['feature_id'], q['entity']['instrument_id'], q['entity']['session_id']) for q in payload['quality']]
    _identity(len(quality_keys) == len(set(quality_keys)))
    expected_quality = {(c['feature_id'], e['instrument_id'], e['session_id']) for c in payload['columns'] for e in c['entities']}
    _identity(set(quality_keys) == expected_quality)
    _identity(all(f in feature_ids and instrument == expected.scope.instrument_id and session == expected.scope.session_id for f, instrument, session in quality_keys))
    for evidence in payload['evidence']:
        _identity(evidence.get('name') == 'EvidenceRow')
        _identity(_cell(evidence, 'feature_id', 'string') in feature_ids)
        entity = evidence['fields']['entity']
        _identity(entity.get('name') == 'EntityKey')
        _identity(_cell(entity, 'instrument_id', 'string') == expected.scope.instrument_id and _cell(entity, 'session_id', 'string') == expected.scope.session_id)
        _identity(_cell(evidence, 'input_id', 'string') == context['dataset']['input_id'])


_NULL_ERRORS = frozenset(((401, 'authentication', 'unauthenticated'), (403, 'authorization', 'not_permitted'),
                          (429, 'quota', 'quota_exceeded'), (500, 'internal', 'internal_error'),
                          *((400, 'transport', code) for code in ('invalid_schema', 'invalid_json', 'incompatible_version', 'bounds'))))
_ERROR_STATUS = {'authentication': 401, 'authorization': 403, 'quota': 429, 'internal': 500, 'transport': 400, 'contract': 400}


def response(frame: Frame, request: Request, expected: RawExpectation | ProducerExpectation | JobExpectation | None,
             selected: tuple[FeatureRef, ...] | None = None) -> View | Failure:
    value = _wire.parse(frame.body)
    _wire.validate(value)
    payload = value['payload']
    operation = request.payload_json()['operation']
    if value['kind'] == 'error':
        if frame.header('content-disposition') is not None or payload['retryable'] is not False:
            raise ValueError('invalid_response')
        category = payload['category']
        if _ERROR_STATUS[category] != frame.status:
            raise ValueError('invalid_response')
        if value['request_id'] is None:
            _identity(value['version'] == '1.0' and (frame.status, category, payload['code']) in _NULL_ERRORS)
        else:
            _identity(value['version'] == request.version and value['request_id'] == request.request_id)
        return Failure('credentials' if category == 'authentication' else category, 'remote_denial', 'decode', frame.status)
    _identity(frame.status == 200 and value['version'] == request.version and value['request_id'] == request.request_id)
    kind = {'discover': 'discovery', 'slice': 'slice' if type(expected) is RawExpectation else 'feature_slice',
            'calculate': 'job', 'job_status': 'job', 'job_cancel': 'job', 'result_read': 'result', 'artifact_read': 'result'}[operation]
    _identity(value['kind'] == kind)
    if operation != 'artifact_read' and frame.header('content-disposition') is not None:
        raise ValueError('invalid_response')
    if kind == 'discovery':
        return DiscoveryView(frame.body)
    if kind == 'slice':
        if type(expected) is not RawExpectation:
            raise ValueError('invalid_expectation')
        check_raw(payload, expected)
        return RawSliceView(frame.body)
    if kind == 'job':
        if type(expected) is JobExpectation:
            _identity(payload['job_id'] == expected.job_id and payload['command_digest'] == expected.command_digest)
        elif type(expected) is ProducerExpectation:
            _identity(payload['command_digest'] == expected.command_digest)
        else:
            raise ValueError('invalid_expectation')
        return JobView(frame.body)
    if type(expected) is not ProducerExpectation:
        raise ValueError('invalid_expectation')
    if kind == 'feature_slice':
        _identity(request.version == '1.1')
        _identity(payload['dataset'] == request.payload_json()['dataset'] and payload['scope'] == expected.scope.payload_json())
        _identity(selected is not None)
        assert selected is not None
        ordered = tuple(f for f in expected.executed_features if f in selected)
        _identity(payload['selected_features'] == [f.payload_json() for f in ordered])
        check_producer(payload['feature_result'], expected, ordered)
        return FeatureSliceView(frame.body)
    _identity(request.version == '1.1')
    check_producer(payload, expected)
    filename = None
    if operation == 'artifact_read':
        result_id = request.payload_json()['result_id']
        filename = 'result-' + hashlib.sha256(result_id.encode('ascii')).hexdigest() + '.json'
        _identity(frame.header('content-disposition') == 'attachment; filename="' + filename + '"')
    return ResultView(frame.body, filename)
