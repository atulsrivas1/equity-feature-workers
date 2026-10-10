"""Optional pure conversion through fixed public contracts and SDK constructors."""
from __future__ import annotations

from dataclasses import fields
from enum import StrEnum
import math
import struct
from typing import Any

from equity_feature_contracts import CanonicalBatch, Column, ConfigSpec, DataKind, FeatureColumn, FeatureResult
from equity_feature_contracts import inputs as i, results as r, specs as s
from equity_feature_contracts.validation import validate_batch
from equity_feature_io_sdk import decode_result, encode_result

from . import _wire
from .models import Failure, Outcome, ProducerExpectation, RawExpectation
from ._response import RawSliceView, ResultView, check_raw, check_producer

_TYPES = (
    i.SourceBinding, i.Coverage, i.PriceUnit, i.AdjustmentSpec, i.InputScope,
    i.IntervalCoverage, i.BatchMetadata, s.AvailabilitySpec, s.IntervalSpec,
    r.EntityKey, r.InputBinding, r.ResultMetadata, r.EvidenceRow, r.QualityRow,
    r.BreadthCounts, r.BreadthFraction, r.IntervalOHLCVRow,
    r.IntervalVolumeShareRow, r.IntervalOHLCV, r.IntervalVolumeShares,
    r.TopKTradeRow, r.TopKTrades, r.QuoteStateCounts, r.QuoteObservation,
    r.SampledSpread, r.QuoteDurations, r.TimeWeightedSpread,
)
_RECORDS = {cls.__name__: cls for cls in _TYPES}


def _cell(value: Any) -> Any:
    if value is None:
        return None
    tag = value['type']
    if tag == 'int64':
        return _wire.integer(value['value'])
    if tag == 'decimal128':
        if value['scale'] != 0:
            raise ValueError('invalid_conversion')
        coefficient = int(value['coefficient'])
        if not -(10**38) < coefficient < 10**38:
            raise ValueError('invalid_conversion')
        return coefficient
    if tag == 'float64':
        number = struct.unpack('>d', bytes.fromhex(value['bits']))[0]
        if not math.isfinite(number):
            raise ValueError('invalid_conversion')
        return number
    if tag in ('string', 'bool'):
        return value['value']
    if tag == 'list':
        return tuple(_cell(item) for item in value['items'])
    if tag != 'record' or value['name'] not in _RECORDS:
        raise ValueError('invalid_conversion')
    cls = _RECORDS[value['name']]
    if set(value['fields']) != {field.name for field in fields(cls)}:
        raise ValueError('invalid_conversion')
    args = {name: _cell(cell) for name, cell in value['fields'].items()}
    if cls is r.InputBinding:
        args['kind'] = DataKind(args['kind'])
    elif cls is r.QualityRow:
        args['status'] = r.Status(args['status'])
        args['reasons'] = tuple(r.Reason(reason) for reason in args['reasons'])
    elif cls is r.EvidenceRow and args['exclusion_reason'] is not None:
        args['exclusion_reason'] = r.Reason(args['exclusion_reason'])
    return cls(**args)


def _encode_cell(value: Any, *, decimal: bool = False) -> Any:
    if value is None:
        return None
    if isinstance(value, StrEnum):
        return {'type': 'string', 'value': value.value}
    if type(value) is bool:
        return {'type': 'bool', 'value': value}
    if type(value) is str:
        return {'type': 'string', 'value': value}
    if type(value) is int:
        if decimal:
            if not -(10**38) < value < 10**38:
                raise ValueError('invalid_conversion')
            return {'type': 'decimal128', 'coefficient': str(value), 'scale': 0}
        _wire.integer(str(value))
        return {'type': 'int64', 'value': str(value)}
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError('invalid_conversion')
        return {'type': 'float64', 'bits': struct.pack('>d', value).hex()}
    if type(value) is tuple:
        return {'type': 'list', 'items': [_encode_cell(item) for item in value]}
    if type(value) in _TYPES:
        return {'type': 'record', 'name': type(value).__name__, 'fields': {field.name: _encode_cell(getattr(value, field.name)) for field in fields(value)}}
    raise ValueError('invalid_conversion')


def convert_raw(view: RawSliceView, expectation: RawExpectation) -> Outcome[CanonicalBatch]:
    try:
        if type(view) is not RawSliceView or type(expectation) is not RawExpectation:
            raise ValueError('invalid_conversion')
        payload = view.payload_json()
        check_raw(payload, expectation)
        metadata = _cell(payload['metadata'])
        if type(metadata) is not i.BatchMetadata:
            raise ValueError('invalid_conversion')
        columns = tuple(Column(column['name'], tuple(_cell(value) for value in column['values'])) for column in payload['columns'])
        result = CanonicalBatch(DataKind(expectation.data_kind), columns, metadata)
        validate_batch(result)
        schema = i.schema_for(result.kind)
        for supplied, native in zip(payload['columns'], result.columns, strict=True):
            field = schema.field(native.name)
            if supplied['dtype'] != ('int64' if field.dtype == i.DType.UTC_NS else field.dtype.value):
                raise ValueError('invalid_conversion')
            expected_unit = 'UTCns' if field.dtype == i.DType.UTC_NS else 'scaled_price' if native.name in ('price', 'bid', 'ask', 'open', 'high', 'low', 'close') else 'shares' if native.name in ('size', 'volume', 'bid_size', 'ask_size') else 'dimensionless'
            if supplied['unit'] != expected_unit:
                raise ValueError('invalid_conversion')
            if supplied['values'] != [_encode_cell(v, decimal=field.dtype == i.DType.DECIMAL128) for v in native.values]:
                raise ValueError('invalid_conversion')
        if _encode_cell(result.metadata) != payload['metadata']:
            raise ValueError('invalid_conversion')
        return Outcome(view=result)
    except BaseException:
        return Outcome(failure=Failure('contract', 'invalid_conversion', 'convert'))


def _producer(native: FeatureResult, original: dict[str, Any]) -> dict[str, Any]:
    return dict(context=original['context'], command_digest=original['command_digest'],
                backend_id=native.metadata.backend_id, backend_version=native.metadata.backend_version,
                metadata=_encode_cell(native.metadata),
                columns=[dict(feature_id=c.feature_id, algorithm_version=c.algorithm_version,
                              schema_version=c.schema_version, dtype=c.dtype.value, unit=c.unit,
                              entities=[dict(instrument_id=e.instrument_id, session_id=e.session_id) for e in c.entities],
                              values=[_encode_cell(v, decimal=c.dtype == r.ValueType.DECIMAL128) for v in c.values]) for c in native.values],
                quality=[dict(entity=dict(instrument_id=q.entity.instrument_id, session_id=q.entity.session_id),
                              feature_id=q.feature_id, status=q.status.value,
                              expected=None if q.expected is None else str(q.expected), observed=str(q.observed),
                              reasons=[reason.value for reason in q.reasons]) for q in native.quality],
                evidence=[_encode_cell(e) for e in native.evidence], next_cursor=None,
                executed_features=[dict(feature_id=c.feature_id, algorithm_version=c.algorithm_version) for c in native.values])


def convert_result(view: ResultView, expectation: ProducerExpectation, *, config: ConfigSpec | None = None) -> Outcome[FeatureResult]:
    try:
        if type(view) is not ResultView or type(expectation) is not ProducerExpectation:
            raise ValueError('invalid_conversion')
        payload = view.payload_json()
        check_producer(payload, expectation)
        if config is not None and (type(config) is not ConfigSpec or config.digest != expectation.context_json()['config']['digest']):
            raise ValueError('invalid_conversion')
        columns = tuple(FeatureColumn(c['feature_id'], c['algorithm_version'], r.ValueType(c['dtype']), c['unit'],
                                      tuple(r.EntityKey(**e) for e in c['entities']), tuple(_cell(v) for v in c['values']), c['schema_version']) for c in payload['columns'])
        quality = tuple(r.QualityRow(r.EntityKey(**q['entity']), q['feature_id'], r.Status(q['status']),
                                    None if q['expected'] is None else _wire.integer(q['expected']),
                                    _wire.integer(q['observed']), tuple(r.Reason(reason) for reason in q['reasons'])) for q in payload['quality'])
        metadata = _cell(payload['metadata'])
        evidence = tuple(_cell(e) for e in payload['evidence'])
        if type(metadata) is not r.ResultMetadata or any(type(e) is not r.EvidenceRow for e in evidence):
            raise ValueError('invalid_conversion')
        constructed = FeatureResult(columns, quality, metadata, evidence)
        result = decode_result(encode_result(constructed))
        # Pure reconstruction must retain the complete transported producer;
        # no publication receipt/envelope is invented by this check.
        if _wire.canonical(_producer(result, payload), limit=_wire.RESPONSE_LIMIT) != _wire.canonical(payload, limit=_wire.RESPONSE_LIMIT):
            raise ValueError('invalid_conversion')
        return Outcome(view=result)
    except BaseException:
        return Outcome(failure=Failure('contract', 'invalid_conversion', 'convert'))
