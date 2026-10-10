"""Closed data-only ingress and exact outbound native cell conversion."""
from __future__ import annotations

from dataclasses import fields
from enum import StrEnum
from functools import lru_cache
from importlib.resources import files
import json
import math
import re
import struct
from typing import Any

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from equity_feature_contracts import inputs as i, results as r, specs as s

Json = dict[str, Any]
MAX_REQUEST, MAX_RESPONSE = 16_384, 262_144
_TYPES = (
    i.SourceBinding, i.Coverage, i.PriceUnit, i.AdjustmentSpec, i.InputScope,
    i.IntervalCoverage, i.BatchMetadata, s.AvailabilitySpec, s.IntervalSpec,
    r.EntityKey, r.InputBinding, r.ResultMetadata, r.EvidenceRow, r.QualityRow,
    r.BreadthCounts, r.BreadthFraction, r.IntervalOHLCVRow,
    r.IntervalVolumeShareRow, r.IntervalOHLCV, r.IntervalVolumeShares,
    r.TopKTradeRow, r.TopKTrades, r.QuoteStateCounts, r.QuoteObservation,
    r.SampledSpread, r.QuoteDurations, r.TimeWeightedSpread,
)


class WireError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def integer(text: object) -> int:
    if type(text) is not str or re.fullmatch(r"0|-?[1-9][0-9]{0,18}", text) is None:
        raise WireError("invalid_schema")
    value = int(text)
    if not i.I64_MIN <= value <= i.I64_MAX:
        raise WireError("bounds")
    return value


def _pairs(pairs: list[tuple[str, Any]]) -> Json:
    result: Json = {}
    for k, v in pairs:
        if k in result:
            raise WireError("invalid_json")
        result[k] = v
    return result


def _constant(_: str) -> Any:
    raise WireError("invalid_json")


def _walk(value: Any, depth: int = 0, count: list[int] | None = None) -> None:
    if count is None:
        count = [0]
    count[0] += 1
    if depth > 32 or count[0] > 10_000:
        raise WireError("bounds")
    if isinstance(value, str) and any(0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise WireError("invalid_json")
    if type(value) is dict:
        for k, v in value.items():
            _walk(k, depth + 1, count)
            _walk(v, depth + 1, count)
    elif type(value) is list:
        for v in value:
            _walk(v, depth + 1, count)


@lru_cache(maxsize=2)
def _schema(version: str) -> Any:
    if version not in ("1.0", "1.1"):
        raise WireError("incompatible_version")
    name = "remote-v1.schema.json" if version == "1.0" else "remote-v1.1.schema.json"
    schema = json.loads(files(__package__).joinpath("schemas", name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def validate(value: Json) -> Json:
    _walk(value)
    if type(value) is not dict or value.get("schema") != "equity.remote":
        raise WireError("invalid_schema")
    version = value.get("version")
    if type(version) is not str or version not in ("1.0", "1.1"):
        raise WireError("incompatible_version")
    if next(_schema(version).iter_errors(value), None) is not None:
        raise WireError("invalid_schema")
    _semantics(value)
    return value


def _semantics(value: Any) -> None:
    if type(value) is dict:
        tag = value.get("type")
        if tag == "int64":
            integer(value["value"])
        elif tag == "decimal128":
            c = value["coefficient"]
            if re.fullmatch(r"0|-?[1-9][0-9]{0,37}", c) is None or type(value["scale"]) is not int:
                raise WireError("invalid_schema")
            if not -(10**38) < int(c) < 10**38:
                raise WireError("bounds")
        elif tag == "float64":
            if not math.isfinite(struct.unpack(">d", bytes.fromhex(value["bits"]))[0]):
                raise WireError("invalid_schema")
        if {"start_ns", "end_ns"} <= value.keys() and type(value["start_ns"]) is str:
            if integer(value["start_ns"]) >= integer(value["end_ns"]):
                raise WireError("bounds")
        for v in value.values():
            _semantics(v)
    elif type(value) is list:
        for v in value:
            _semantics(v)


def decode(data: bytes) -> Json:
    if type(data) is not bytes or len(data) > MAX_REQUEST:
        raise WireError("bounds")
    try:
        text = data.decode("utf-8")
        depth, quoted, escaped = 0, False, False
        for c in text:
            if quoted:
                if escaped:
                    escaped = False
                elif c == "\\":
                    escaped = True
                elif c == '"':
                    quoted = False
            elif c == '"':
                quoted = True
            elif c in "[{":
                depth += 1
                if depth > 32:
                    raise WireError("bounds")
            elif c in "]}":
                depth -= 1
        value = json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)
    except (UnicodeError, ValueError, RecursionError) as e:
        if isinstance(e, WireError):
            raise
        raise WireError("invalid_json") from None
    if type(value) is not dict or value.get("kind") != "request":
        raise WireError("invalid_schema")
    return validate(value)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def encode(value: Json) -> bytes:
    validate(value)
    data = canonical(value)
    if len(data) > MAX_RESPONSE:
        raise WireError("bounds")
    return data


def cell(value: object, *, decimal: bool = False) -> Any:
    """Outbound trusted native types only; never constructs a class from a label."""
    if value is None:
        return None
    cls = type(value)
    if isinstance(value, StrEnum):
        return {"type": "string", "value": value.value}
    if cls is bool:
        return {"type": "bool", "value": value}
    if cls is str:
        return {"type": "string", "value": value}
    if cls is int:
        if decimal:
            if not -(10**38) < value < 10**38:  # type: ignore[operator]
                raise WireError("bounds")
            return {"type": "decimal128", "coefficient": str(value), "scale": 0}
        integer(str(value))
        return {"type": "int64", "value": str(value)}
    if cls is float:
        if not isinstance(value, float) or not math.isfinite(value):
            raise WireError("invalid_schema")
        return {"type": "float64", "bits": struct.pack(">d", value).hex()}
    if cls in (tuple, list):
        assert isinstance(value, (tuple, list))
        return {"type": "list", "items": [cell(v) for v in value]}
    if cls in _TYPES:
        return {"type": "record", "name": cls.__name__,
                "fields": {f.name: cell(getattr(value, f.name)) for f in fields(value)}}  # type: ignore[arg-type]
    raise WireError("invalid_schema")
