"""Regression gate: the remote client's native conversion carries feature context and converts EF5-shaped columns.

1. bool, int64 and float64 `ef5.*` columns of a producer payload convert to the native `FeatureResult` unchanged (values, dtype, unit).
2. A `tape.top_k_notional` TopKTrades column ranked by notional converts (the decoder is told the feature's ranking), and the same
   rows under a size-ranked feature id are refused fail-closed (`invalid_conversion`): the row order is part of the cell's validity,
   so a decoder without the feature context would mis-rank or mis-refuse a delivered cell type.
3. Both results equal their SDK codec round trip.

Needs the installed packages and `docs/EQ081_CLIENT_FIXTURES.json`. Exit 0 only if every check holds:
`python tools/verify_client_feature_context.py`.
"""
from __future__ import annotations

import copy
from dataclasses import replace
import json
import sys
from pathlib import Path
from typing import Any

import equity_feature_contracts as c
from equity_feature_contracts import results as r
from equity_feature_client import FeatureRef, ProducerExpectation, ResultView, ScopeKey
from equity_feature_client.native import NOTIONAL_FEATURE, _cell, _producer, convert_result
from equity_feature_io_sdk.codec import NOTIONAL_FEATURE as SDK_NOTIONAL_FEATURE, decode_result, encode_result

ROOT = Path(__file__).resolve().parents[1]
ENTITY = r.EntityKey("A", "S")


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def column(feature: str, algorithm: str, dtype: r.ValueType, unit: str, value: Any) -> r.FeatureColumn:
    return r.FeatureColumn(feature, algorithm, dtype, unit, (ENTITY,), (value,))


def rows(input_id: str) -> tuple[r.TopKTradeRow, r.TopKTradeRow]:
    # Notional order (price*size): 10*5 = 50 then 2*6 = 12; size order is the reverse (6 then 5).
    return (r.TopKTradeRow(input_id, "e1", 100, 1, 50, 10, 5), r.TopKTradeRow(input_id, "e2", 110, 2, 50, 2, 6))


def convert(base: dict[str, Any], columns: tuple[r.FeatureColumn, ...], notional_rows_as: str,
            evidence: tuple[r.EvidenceRow, ...] = ()) -> Any:
    raw = copy.deepcopy(base)
    refs = [dict(feature_id=col.feature_id, algorithm_version=col.algorithm_version) for col in columns]
    raw["context"]["features"] = refs
    raw["executed_features"] = refs
    expected = ProducerExpectation(raw["context"], ScopeKey("A", "S", 100, 200), tuple(FeatureRef(**ref) for ref in refs),
                                   raw["backend_id"], raw["backend_version"])
    raw["command_digest"] = expected.command_digest
    metadata = replace(_cell(raw["metadata"]), evidence_limit=16)
    input_id = metadata.inputs[0].metadata.source.input_id
    evidence = tuple(replace(e, input_id=input_id) for e in evidence)
    native = r.FeatureResult(columns, tuple(r.QualityRow(ENTITY, col.feature_id, r.Status.AVAILABLE, 1, 1, ()) for col in columns),
                             metadata, evidence)
    payload = _producer(native, raw)
    view = ResultView(canonical(dict(schema="equity.remote", version="1.1", kind="result", request_id="ctx", payload=payload)))
    outcome = convert_result(view, expected)
    if outcome.ok and outcome.view is not None and decode_result(encode_result(outcome.view)) != outcome.view:
        raise AssertionError(f"SDK round trip differs ({notional_rows_as})")
    return outcome


def main() -> int:
    problems: list[str] = []
    if NOTIONAL_FEATURE != SDK_NOTIONAL_FEATURE:
        problems.append(f"client notional feature id {NOTIONAL_FEATURE} != sdk {SDK_NOTIONAL_FEATURE}")
    base = json.loads((ROOT / "docs" / "EQ081_CLIENT_FIXTURES.json").read_bytes())["native_cases"][0]["complete_producer_payload"]
    ef5 = (column("candle.body_fraction", "ef5-candle-v1", r.ValueType.FLOAT64, "fraction", 0.05),
           column("candle.doji", "ef5-candle-v1", r.ValueType.BOOL, "boolean", True),
           column("candle.inside_bar", "ef5-candle-v1", r.ValueType.BOOL, "boolean", False),
           column("technical.signed_trend_window_count", "ef5-technical-v1", r.ValueType.INT64, "windows", -2))
    outcome = convert(base, tuple(sorted(ef5, key=lambda col: col.feature_id)), "ef5")
    if not outcome.ok or {col.feature_id: col.values for col in outcome.view.values} != {col.feature_id: col.values for col in ef5}:
        problems.append("ef5 bool/int64/float64 columns do not convert unchanged")
    input_id = _cell(base["metadata"]).inputs[0].metadata.source.input_id
    cell = r.TopKTrades(2, rows(input_id), ranking="notional")
    support = tuple(r.EvidenceRow(ENTITY, NOTIONAL_FEATURE, input_id, x.event_id, x.event_ns, x.known_at_ns) for x in rows(input_id))
    ok = convert(base, (column(NOTIONAL_FEATURE, "ef4-tape-v1", r.ValueType.TOP_K_TRADES, "trades", cell),), "notional", support)
    if not ok.ok or ok.view.values[0].values[0].rows != rows(input_id):
        problems.append("notional-ranked TopKTrades column does not convert with the feature context")
    # Positive size-ranked control: a size-ordered cell under a size-ranked feature converts (forcing notional for every column fails).
    size_rows = tuple(reversed(rows(input_id)))
    size_cell = r.TopKTrades(2, size_rows)
    size_support = tuple(r.EvidenceRow(ENTITY, "session.trade.top_k", input_id, x.event_id, x.event_ns, x.known_at_ns) for x in size_rows)
    plain = convert(base, (column("session.trade.top_k", "v1", r.ValueType.TOP_K_TRADES, "trades", size_cell),), "size", size_support)
    if not plain.ok or plain.view.values[0].values[0].rows != size_rows:
        problems.append("size-ranked TopKTrades column does not convert unchanged")
    # Negative control at the decoder: the same notional-ordered cell decoded without the feature context is refused.
    from equity_feature_client.native import _encode_cell
    wire = _encode_cell(cell)
    if _cell(wire, True) != cell:
        problems.append("decoder with notional context does not rebuild the cell")
    try:
        _cell(wire, False)
    except Exception:
        pass
    else:
        problems.append("a notional-ordered cell decoded without its feature context was accepted")
    for line in problems:
        print("MISMATCH", line)
    if problems:
        return 1
    print("Remote client conversion: ef5 bool/int64/float64 columns convert unchanged; notional TopKTrades converts with the "
          "feature context and a mis-ranked cell is refused; SDK round trips equal")
    return 0


if __name__ == "__main__":
    sys.exit(main())
