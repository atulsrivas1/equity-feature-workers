"""Independent closed JSON validation; no service or native imports."""
from __future__ import annotations

from functools import lru_cache
from importlib.resources import files
import json
import math
import re
import struct
from typing import Any, NoReturn, cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

REQUEST_LIMIT = 16_384
RESPONSE_LIMIT = 262_144


def reject(code: str = "invalid_request") -> NoReturn:
    raise ValueError(code)


def integer(value: object) -> int:
    if type(value) is not str or re.fullmatch(r"0|-?[1-9][0-9]{0,18}", value) is None:
        raise ValueError("invalid_request")
    number = int(value)
    if not -(2**63) <= number < 2**63:
        raise ValueError("bounds")
    return number


def _walk(value: Any, depth: int = 0, count: list[int] | None = None) -> None:
    if count is None:
        count = [0]
    count[0] += 1
    if depth > 32 or count[0] > 10_000:
        reject("bounds")
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                reject()
            _walk(key, depth + 1, count)
            _walk(item, depth + 1, count)
        if value.get("type") == "int64":
            integer(value.get("value"))
        elif value.get("type") == "decimal128":
            c = value.get("coefficient")
            if type(c) is not str or re.fullmatch(r"0|-?[1-9][0-9]{0,37}", c) is None:
                reject()
            if not -(10**38) < int(c) < 10**38 or type(value.get("scale")) is not int:
                reject("bounds")
        elif value.get("type") == "float64":
            bits = value.get("bits")
            if type(bits) is not str or re.fullmatch(r"[0-9a-f]{16}", bits) is None:
                reject()
            if not math.isfinite(struct.unpack(">d", bytes.fromhex(bits))[0]):
                reject()
        if "start_ns" in value and "end_ns" in value:
            if type(value['start_ns']) is str and integer(value['start_ns']) >= integer(value['end_ns']):
                reject("bounds")
    elif type(value) is list:
        for item in value:
            _walk(item, depth + 1, count)
    elif type(value) is str:
        if any(0xD800 <= ord(c) <= 0xDFFF for c in value):
            reject()
    elif value is None or type(value) in (bool, int):
        pass
    elif type(value) is float:
        if not math.isfinite(value):
            reject()
    else:
        reject()


def canonical(value: Any, *, limit: int = REQUEST_LIMIT) -> bytes:
    _walk(value)
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode('ascii')
    if len(data) > limit:
        reject("bounds")
    return data


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            reject("invalid_response")
        result[key] = value
    return result


def _constant(_: str) -> Any:
    reject("invalid_response")


def parse(data: bytes, *, limit: int = RESPONSE_LIMIT) -> dict[str, Any]:
    if type(data) is not bytes or len(data) > limit:
        reject("bounds")
    # Reject nesting before json.loads can construct a deep object graph.
    depth = 0
    quoted = escaped = False
    for c in data:
        if quoted:
            if escaped:
                escaped = False
            elif c == 92:
                escaped = True
            elif c == 34:
                quoted = False
        elif c == 34:
            quoted = True
        elif c in (91, 123):
            depth += 1
            if depth > 32:
                reject("bounds")
        elif c in (93, 125):
            depth -= 1
    try:
        value = json.loads(data.decode('utf-8'), object_pairs_hook=_pairs, parse_constant=_constant)
    except (UnicodeError, ValueError, RecursionError):
        raise ValueError("invalid_response") from None
    _walk(value)
    if type(value) is not dict:
        reject("invalid_response")
    return cast(dict[str, Any], value)


@lru_cache(maxsize=2)
def schema(version: str) -> Any:
    if version not in ('1.0', '1.1'):
        reject()
    name = 'remote-v1.schema.json' if version == '1.0' else 'remote-v1.1.schema.json'
    value = json.loads(files('equity_feature_client').joinpath('schemas', name).read_text(encoding='utf-8'))
    Draft202012Validator.check_schema(value)
    return Draft202012Validator(value)


def validate(value: dict[str, Any]) -> None:
    _walk(value)
    if next(schema(value.get('version', '')).iter_errors(value), None) is not None:
        reject()


def definition(name: str, value: Any) -> None:
    _walk(value)
    root = schema('1.1').schema
    validator = Draft202012Validator({'$defs': root['$defs'], '$ref': '#/$defs/' + name})
    if next(validator.iter_errors(value), None) is not None:
        reject("invalid_expectation")
