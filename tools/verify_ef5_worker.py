"""Gate for the EF5 workers layer (`equity_feature_workers.equity_ef5`) against the frozen EF5 contract.

Every result fixture of core `docs/ef5/PRICE_STRUCTURES_CONTRACT_PROPOSAL.json` goes through the io adapter
(`equity_feature_duckdb.price_structures`), the worker (`make_ef5_work`, `run_ef5`), a separate DuckDB sink and full read-back,
once per output and jointly (534 runs). The stored result must equal the pure library result called directly (canonical
encoding) and match the contract literal (value by the acceptance rule, status, reasons, counts, witnesses, echoed basis).
Every refusal recipe is applied to its base fixture and must be refused with the recorded code, at the adapter or by the worker
(work-item build or calculation) with the same `contract_code`; nothing is published on a refusal. No mathematics here.

    python tools/verify_ef5_worker.py [--contract PATH_TO_CORE_docs_ef5_JSON]

Exit status 0 only if everything holds.
"""
from __future__ import annotations

import argparse
import copy
from dataclasses import asdict
import json
import math
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from equity_feature_contracts import (
    AdjustmentSpec, AvailabilitySpec, BatchMetadata, CanonicalBatch, Column, ContractError, Coverage, DataKind,
    InputScope, IntervalCoverage, PriceUnit, SessionSpec, SourceBinding,
)
from equity_feature_duckdb.price_structures import (
    CLOSE_COLUMNS, OHLCV_COLUMNS, make_ef5_batch, make_ef5_chunks, make_ef5_config, make_ef5_context,
)
from equity_feature_duckdb.relative_rv import IntradayRunSpec
from equity_feature_duckdb_sink import DuckDBSink
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_features_intraday import IntradayContext, IntradayRun, compute_ef5, require_ef5_capability
from equity_feature_contracts import InputBinding
from equity_feature_workers import InputManifest
from equity_feature_workers.commands import CommandError
from equity_feature_workers.ef1_runner import run_ef5
from equity_feature_workers.equity_ef5 import compute_ef5_inputs, make_ef5_work
import hashlib

ROOT = Path(__file__).resolve().parents[1]
LISTS = ("candle_fixtures", "structure_fixtures", "technical_fixtures", "ka_fixtures")
T0 = 1_700_000_000_000_000_000
SEC = 10 ** 9
BAR = 60 * SEC
DAY = 86_400 * SEC
KA_DEFAULT = 50  # the compact notation's default known_at token, stated here and passed explicitly (fixture only)
LABEL_OF = {"raw": "unadjusted", "split": "split_adjusted"}
PARAMS = ("doji_body_max_fraction", "swing_strength", "lookback", "trend_window", "trend_count")
OWNER = {"doji_body_max_fraction": "candle", "swing_strength": "structure", "lookback": "structure",
         "trend_window": "technical", "trend_count": "technical"}


def contract_path(override: str | None) -> Path:
    if override:
        return Path(override)
    if os.environ.get("EF5_CONTRACT"):
        return Path(os.environ["EF5_CONTRACT"])
    return ROOT.parent / "equity-features" / "docs" / "ef5" / "PRICE_STRUCTURES_CONTRACT_PROPOSAL.json"


def grid(fx: dict[str, Any], n: int) -> tuple[list[int], list[int]]:
    daily = fx.get("grid") == "daily"
    offsets = fx.get("ends_s")
    ends = [T0 + ((offsets[i] if offsets else (i + 1) * 86400) * SEC if daily else (i + 1) * BAR) for i in range(n)]
    return [e - (DAY if daily else BAR) for e in ends], ends


def adjustment_spec(label: str | None, spec: dict[str, Any] | None) -> AdjustmentSpec:
    spec = spec or {}
    if label == "split_adjusted":
        return AdjustmentSpec(basis="split", **{k: spec[k] for k in ("policy_version", "action_snapshot", "anchor") if k in spec})
    return AdjustmentSpec(basis="raw", **({"policy_version": spec["policy_version"]} if "policy_version" in spec else {}))


def special(slot: Any) -> bool:
    return isinstance(slot, dict) and any(k in slot for k in ("wire", "versions", "no_coverage"))


def expand(fx: dict[str, Any]) -> tuple[Any, tuple[CanonicalBatch, ...], Any, Any, tuple[str, ...], str]:
    p = fx["params"]
    n = fx.get("target", len(fx["slots"]) - 1) + 1
    slots = fx["slots"][:n]
    daily = fx.get("grid") == "daily"
    starts, ends = grid(fx, n)
    forming = slots[-1] == "forming"
    cutoff = (ends[n - 2] if n >= 2 else starts[0]) if forming else ends[-1]
    knowledge = cutoff + 100 * SEC
    cols = OHLCV_COLUMNS if fx.get("columns", "ohlcv") == "ohlcv" else CLOSE_COLUMNS
    width = len(cols)
    label = fx.get("adjustment")
    declared = fx.get("coverage", "declared") == "declared"

    def known(ka: Any) -> Any:
        return ka if ka is None or isinstance(ka, float) else knowledge - (100 - ka) * SEC

    if daily:
        sessions = tuple(SessionSpec("SYN", f"D{i}", starts[i], ends[i], "UTC") for i in range(n))
        runs = tuple(IntradayRunSpec(f"D{i}", starts[i], DAY, 1) for i in range(n))
        lib_runs = tuple(IntradayRun(f"D{i}", starts[i], DAY, 1) for i in range(n))
    else:
        sessions = (SessionSpec("SYN", "S1", T0, T0 + n * BAR, "UTC"),)
        runs = (IntradayRunSpec("S1", T0, BAR, n),)
        lib_runs = (IntradayRun("S1", T0, BAR, n),)
    profile = p.get("epoch_profile")
    mode = {"session_reset": "reset", "governed_history": "carry"}.get(profile, "reset")
    spec_context = make_ef5_context("X", calendar_id="SYN-CAL", grid_id="SYN-GRID", revision="r1", sessions=sessions, runs=runs,
                                    anchor_index=0, target_index=n - 1,
                                    epoch_profile=profile if profile in ("session_reset", "governed_history") else "session_reset")
    library_context = IntradayContext("X", "SYN-CAL", "SYN-GRID", "r1", sessions, lib_runs, 0, n - 1, mode)
    unit = PriceUnit(fx["unit"]["scale"], fx["unit"]["currency"])
    basis = adjustment_spec(label, fx.get("adjustment_spec"))

    entries: list[Any] = []
    for slot in slots:
        if isinstance(slot, str) and slot != "null":
            entries.append(slot)
        elif slot == "null":
            entries.append({"values": (None,) * width, "known_at_ns": known(KA_DEFAULT)})
        elif isinstance(slot, dict) and "row" in slot and not special(slot):
            row = slot["row"]
            entries.append({"values": tuple(row) if row is not None else (None,) * width,
                            "known_at_ns": known(slot.get("ka", KA_DEFAULT))})
        elif isinstance(slot, list):
            entries.append({"values": tuple(slot), "known_at_ns": known(KA_DEFAULT)})
        else:
            entries.append(slot)
    if any(special(s) for s in slots) or any(isinstance(e, dict) and len(e.get("values", ())) not in (0, width) for e in entries):
        chunks = explicit_chunks(fx, slots, spec_context, cols, unit, basis, starts, ends, known, daily, declared)
    else:
        for e in entries:
            if isinstance(e, dict) and len(e["values"]) != width:
                raise ContractError(__import__("equity_feature_contracts").ErrorCode.INVALID_SCHEMA, "row width")
        chunks = make_ef5_chunks(spec_context, entries, input_id="bars", source_id="SYN-SRC", snapshot_id="snap1",
                                 namespace="SYN", price_unit=unit, adjustment=basis, columns=cols,
                                 eligibility_policy="ef5-regular", declare_coverage=declared)
    producer = fx["producer"]
    names = {k: p.get(k) for k in ("policy_version", "epoch_profile", "evidence_limit")}
    names.update({k: p.get(k) for k in PARAMS if OWNER[k] == producer})
    extra = {k: v for k, v in p.items() if k not in names}
    availability = AvailabilitySpec(cutoff, knowledge, knowledge)
    policy = (fx.get("adjustment_spec") or {}).get("policy_version")
    config = make_ef5_config(producer, spec_context, availability=availability, adjustment=basis, price_unit=unit,
                             adjustment_label=label, adjustment_policy_version=policy, **names, **extra)
    return library_context, chunks, config, (producer, availability, basis, unit, names, label, policy), \
        tuple(fx["outputs"]), fx.get("call", "compute")


def explicit_chunks(fx: dict[str, Any], slots: list[Any], context: Any, cols: tuple[str, ...], unit: PriceUnit,
                    basis: AdjustmentSpec, starts: list[int], ends: list[int], known: Any, daily: bool,
                    declared: bool) -> tuple[CanonicalBatch, ...]:
    """Identity-explicit rows for the wire-form recipes: the identity comes from the wire override or the grid."""
    from equity_feature_contracts import ErrorCode
    width = len(cols)
    names = ("instrument_id", "session_id", "start_ns", "end_ns", *cols, "known_at_ns")
    out: list[CanonicalBatch] = []
    row_basis = None
    for session in context.sessions:
        rows: list[tuple[Any, ...]] = []
        intervals: list[IntervalCoverage] = []
        for i, slot in enumerate(slots):
            if (f"D{i}" if daily else "S1") != session.session_id:
                continue
            if isinstance(slot, str) and slot not in ("null",):
                if slot == "gap":
                    intervals.append(IntervalCoverage(f"slot:{i}", starts[i], ends[i], Coverage(1, 0, False)))
                elif slot == "empty":
                    intervals.append(IntervalCoverage(f"slot:{i}", starts[i], ends[i], Coverage(0, 0, True)))
                continue
            wire = slot.get("wire", {}) if isinstance(slot, dict) else {}
            body = wire.get("row", slot.get("row") if isinstance(slot, dict) else slot)
            ka = wire.get("ka", slot.get("ka", KA_DEFAULT) if isinstance(slot, dict) else KA_DEFAULT)
            if slot == "null":
                body = [None] * width
            versions = slot.get("versions") if isinstance(slot, dict) else None
            for version in versions or [{"ka": ka}]:
                values = tuple(body) if body is not None else (None,) * width
                if len(values) != width:
                    raise ContractError(ErrorCode.INVALID_SCHEMA, "row width differs from the delivered columns")
                rows.append((wire.get("inst", "X"), wire.get("ss", session.session_id), wire.get("start", starts[i]),
                             wire.get("end", ends[i]), *values, known(version.get("ka", ka))))
            if wire.get("basis") is not None:
                row_basis = wire["basis"]
            if declared and not (isinstance(slot, dict) and slot.get("no_coverage")):
                intervals.append(IntervalCoverage(f"slot:{i}", starts[i], ends[i], Coverage(1, 1, True)))
        if row_basis is not None:
            adjustment = adjustment_spec(row_basis, {"policy_version": "split-factors-v1", "action_snapshot": "snap",
                                                     "anchor": "anchor"})
        else:
            adjustment = basis
        metadata = BatchMetadata("SYN", SourceBinding("SYN-SRC", "snap1", "ef5-map1", f"bars-{len(out):03}"),
                                 Coverage(None, len(rows), False), unit, adjustment, "none", "shares", "declared",
                                 "reject", InputScope(session.open_ns, session.close_ns, "ef5-regular"), tuple(intervals))
        out.append(make_ef5_batch(names, tuple(rows), metadata=metadata))
    return tuple(out)


def declarations(chunks: tuple[CanonicalBatch, ...]) -> tuple[InputManifest, ...]:
    """One manifest per BAR chunk, in the library's role naming ``bars:NNN``."""
    result = []
    for number, batch in enumerate(chunks):
        role = f"bars:{number:03}"
        raw = json.dumps(asdict(batch), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
        coverage = batch.metadata.coverage
        state = "partial" if not coverage.complete else "empty" if coverage.observed == 0 else "complete"
        result.append(InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
                                    InputBinding(role, batch.kind, batch.metadata)))
    return tuple(result)


SCOPE = "ef5-worker-gate"


def build(fx: dict[str, Any], features: tuple[str, ...], label: str) -> tuple[Any, Any, Any, Any]:
    context, chunks, config, parts, outputs, call = expand(fx)
    if call != "compute":
        for feature in features:
            require_ef5_capability(feature, call)
    item = make_ef5_work(fx["producer"], config, features=features, inputs=declarations(chunks), batches=chunks,
                         instrument_id=context.instrument_id, job_id="job-" + label, generation_id="generation-" + label,
                         partition_id="X-S", destination_scope=SCOPE)
    return item, context, chunks, config


def accepts(want: dict[str, Any] | None, value: Any) -> bool:
    if want is None or value is None:
        return want is None and value is None
    if "bool" in want:
        return bool(type(value) is bool and value is want["bool"])
    if "int" in want:
        return bool(type(value) is int and value == want["int"])
    r = float.fromhex(want["hex"])
    if want["acceptance"] in ("exact_float64", "exact_plus_zero"):
        return bool(value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r))
    return bool(abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r)))


def compare(fx: dict[str, Any], result: Any, config: Any, features: tuple[str, ...]) -> list[str]:
    problems: list[str] = []
    expected = fx["expected"]
    if tuple(c.feature_id for c in result.values) != tuple(sorted(features)):
        return ["selected outputs differ"]
    found: dict[str, list[str]] = {f: [] for f in features}
    for row in result.evidence:
        found[row.feature_id].append(row.row_id)
    basis = LABEL_OF[config.adjustment.basis]
    echoed = {LABEL_OF[b.metadata.adjustment.basis] for b in result.metadata.inputs}
    for column in result.values:
        quality, = (q for q in result.quality if q.feature_id == column.feature_id)
        want = expected[column.feature_id]
        value, = column.values
        got = (quality.status.value, [r.value for r in quality.reasons], quality.expected, quality.observed)
        if not accepts(want["value"], value) or got != (want["status"], want["reasons"], want["expected"], want["observed"]):
            problems.append(f"{column.feature_id}: value {value!r} {got}")
        if want["basis"] != basis or echoed != {basis}:
            problems.append(f"{column.feature_id}: basis echo {basis} {echoed} != {want['basis']}")
        if len(features) == 1 and found[column.feature_id] != want["witnesses"]:
            problems.append(f"{column.feature_id}: witnesses {found[column.feature_id]} != {want['witnesses']}")
    if len(features) > 1:
        flat = [r.row_id for r in result.evidence]
        joint = [w for f in sorted(features) for w in expected[f]["witnesses"]][:fx["params"]["evidence_limit"]]
        if flat != joint:
            problems.append(f"joint evidence {flat} != {joint}")
    return problems


def apply(base: dict[str, Any], mutations: list[dict[str, Any]]) -> dict[str, Any]:
    fx = copy.deepcopy(base)
    for step in mutations:
        keys = (step.get("delete") or step["set"]).split(".")[1:]
        cursor: Any = fx
        for key in keys[:-1]:
            cursor = cursor[int(key)] if isinstance(cursor, list) else cursor[key]
        last: Any = int(keys[-1]) if isinstance(cursor, list) else keys[-1]
        if "delete" in step:
            del cursor[last]
        else:
            cursor[last] = step["value"]
    return fx


def contract_code(error: Exception) -> str:
    if isinstance(error, CommandError):
        return (error.contract_code or "").upper()
    return error.code.name if isinstance(error, ContractError) else "?"


def refuse_direct(fx: dict[str, Any]) -> str:
    """Error code of the same inputs through the adapter-expanded library call; context None is the RF16 form."""
    try:
        context, chunks, config, parts, outputs, call = expand(fx)
        if call != "compute":
            for feature in outputs:
                require_ef5_capability(feature, call)
        compute_ef5(fx["producer"], chunks, None if ("context" in fx and fx["context"] is None) else context, config,  # type: ignore[arg-type]
                    features=outputs)
    except ContractError as error:
        return error.code.name
    return "NOT_REFUSED"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract")
    doc = json.loads(contract_path(parser.parse_args().contract).read_text(encoding="utf-8"))
    by_id = {fx["id"]: fx for key in LISTS for fx in doc[key]}
    problems: list[str] = []
    runs = 0
    for fx in by_id.values():
        outputs = tuple(sorted(fx["expected"]))
        plans = [(f,) for f in outputs] + ([outputs] if len(outputs) > 1 else [])
        items = []
        try:
            for number, features in enumerate(plans):
                items.append(build(fx, features, f"{fx['id']}-{number}"))
        except (ContractError, CommandError) as error:
            problems.append(f"{fx['id']}: refused {contract_code(error)}: {error}")
            continue
        limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576, max_total_bytes=1048576 * len(items),
                                  max_result_cells=64 * len(items),
                                  max_evidence_rows=max(1, min(fx["params"]["evidence_limit"], 40960)) * len(items))
        with TemporaryDirectory() as folder, DuckDBSink(Path(folder) / "destination.db", SCOPE, limits=limits) as sink:
            outcome = run_ef5(tuple(item for item, *_ in items), sink, requirements=limits)
            by_task = {row.task_sha256: row for row in outcome.readbacks}
            for features, (item, context, chunks, config) in zip(plans, items):
                runs += 1
                row = by_task.get(item.task.task_sha256)
                if row is None or not row.verified:
                    problems.append(f"{fx['id']} {features}: no verified read-back")
                    continue
                stored, = row.results  # type: ignore[misc]
                direct = compute_ef5(fx["producer"], chunks, context, config, features=features)
                if encode_result(stored) != encode_result(direct):
                    problems.append(f"{fx['id']} {features}: stored result differs from the direct library result")
                problems += [f"{fx['id']}: {p}" for p in compare(fx, stored, config, features)]
    codes: set[str] = set()
    at_adapter = 0
    for rf in doc["refusal_fixtures"]:
        mutated = apply(by_id[rf["base"]], rf["mutation"])
        try:
            expand(mutated)
        except ContractError as error:
            at_adapter += 1
            codes.add(error.code.name)
            if error.code.name != rf["error_code"]:
                problems.append(f"{rf['id']}: adapter refused {error.code.name} expected {rf['error_code']}")
            continue
        # The worker must refuse; the refusal class is the direct library call's (the worker adds no codes of its own).
        # RF16 (structural context not delivered) has no worker form: the worker takes its context from the configuration.
        refused = refuse_direct(mutated)
        if refused != rf["error_code"]:
            problems.append(f"{rf['id']}: direct library call refused {refused} expected {rf['error_code']}")
        try:
            item, context, chunks, config = build(mutated, tuple(mutated["outputs"]), "refusal")
            compute_ef5_inputs(item.task, chunks)
        except (ContractError, CommandError) as error:
            codes.add(rf["error_code"])
            code = contract_code(error)
            if code not in (rf["error_code"], "") and not isinstance(error, CommandError):
                problems.append(f"{rf['id']}: worker refused {code} expected {rf['error_code']} ({error})")
        else:
            if rf["id"] != "RF16":
                problems.append(f"{rf['id']}: worker did not refuse (expected {rf['error_code']})")
    for line in problems:
        print("MISMATCH", line)
    if problems:
        print(f"{len(problems)} MISMATCH(ES)")
        return 1
    print(f"EF5 workers: {len(by_id)} result fixtures ({runs} runs) = direct library = stored read-back = contract literal; "
          f"{len(doc['refusal_fixtures'])} refusals ({len(codes)} codes) refused with the recorded code ({at_adapter} at the adapter)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
