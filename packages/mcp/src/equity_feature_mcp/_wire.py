"""Finite binary LF framing and strict owned JSON; no protocol dispatch."""
from __future__ import annotations

import json
import math
from typing import Any, BinaryIO

INPUT_LIMIT = 16384
OUTPUT_LIMIT = 65536


class FrameError(Exception):
    """Internal fixed-code failure; caller text is never retained as a message."""


def _depth(data: str) -> None:
    depth = 0
    quoted = escaped = False
    for char in data:
        if quoted:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in '[{':
            depth += 1
            if depth > 32:
                raise FrameError('invalid_json')
        elif char in ']}':
            depth -= 1


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            raise FrameError('invalid_json')
        result[key] = value
    return result


def _constant(value: str) -> Any:
    raise FrameError('invalid_json')


def _nodes(value: Any) -> None:
    pending = [value]
    count = 0
    while pending:
        item = pending.pop()
        count += 1
        if count > 10000 or (type(item) is float and not math.isfinite(item)):
            raise FrameError('invalid_json')
        if type(item) is dict:
            pending.extend(item.keys())
            pending.extend(item.values())
        elif type(item) is list:
            pending.extend(item)


def parse(data: bytes) -> Any:
    if type(data) is not bytes or len(data) > INPUT_LIMIT or not data.endswith(b'\n'):
        raise FrameError('invalid_json')
    try:
        text = data.decode('utf-8', errors='strict')
        _depth(text)
        value = json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)
        _nodes(value)
        return value
    except (UnicodeError, ValueError, RecursionError):
        raise FrameError('invalid_json') from None


def read_frame(stream: BinaryIO) -> bytes | None:
    data = bytearray()
    while len(data) <= INPUT_LIMIT:
        try:
            piece = stream.read(1)
        except (OSError, ValueError):
            raise FrameError('input_failed') from None
        if type(piece) is not bytes or len(piece) > 1:
            raise FrameError('input_failed')
        if not piece:
            if data:
                raise FrameError('partial_eof')
            return None
        data.extend(piece)
        if len(data) > INPUT_LIMIT:
            raise FrameError('input_bounds')
        if piece == b'\n':
            return bytes(data)
    raise FrameError('input_bounds')


def canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    except (TypeError, ValueError, RecursionError):
        raise FrameError('output_bounds') from None


def encode_frame(value: object) -> bytes:
    frame = canonical(value) + b'\n'
    if len(frame) > OUTPUT_LIMIT:
        raise FrameError('output_bounds')
    return frame


def write_frame(stream: BinaryIO, frame: bytes) -> None:
    if type(frame) is not bytes or not frame.endswith(b'\n') or len(frame) > OUTPUT_LIMIT:
        raise FrameError('output_bounds')
    try:
        if stream.write(frame) != len(frame):
            raise FrameError('incomplete_output')
        stream.flush()
    except (OSError, ValueError):
        raise FrameError('incomplete_output') from None
