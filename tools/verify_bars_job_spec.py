"""Refusal recipe for RL-01-14 (workers part): the native ``bars`` job spec lists the closed 13-id set from core.

Internal, synthetic, pre-release. Run with core, io and workers sources on PYTHONPATH plus the io tutorial package
``examples/third_party/src`` (for the synthetic request only):

    python tools/verify_bars_job_spec.py

Checks (exit 1 on any failure, so the old behaviour of retyping the list would fail):
  1. the workers bars header set IS core's ``BAR_IDS`` object (imported, not retyped) and has 13 ids;
  2. the native ``bars`` job spec built from the library descriptor lists exactly ``BAR_IDS`` in order and is admitted;
  3. a 12-id spec (any one id dropped) is refused with ``INVALID_COMMAND``;
  4. a 13-id spec in a different order, and a spec with one id replaced, are refused as well.
The 39 per-job cap is not touched here.
"""
from __future__ import annotations

import sys

from equity_feature_contracts import (
    BAR_IDS, AvailabilitySpec, ConfigSpec, EntityKey, Parameter, PriceUnit, SessionSpec, WindowSpec,
)
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_sdk import descriptor
from equity_features.session import compute_bars
from equity_feature_example_extensions import example_request
from equity_feature_workers import supplied_session
from equity_feature_workers.commands import CommandError, CommandErrorCode, SessionCommandSpec

failures: list[str] = []


def check(name: str, ok: bool) -> None:
    print(("PASS  " if ok else "FAIL  ") + name)
    if not ok:
        failures.append(name)


def spec(features: tuple) -> SessionCommandSpec:
    config = ConfigSpec("synthetic-bars", "v1", (Parameter("eligibility_policy", "example-v1"),),
                        SessionSpec("demo", "S", 100, 200, "caller-supplied"),
                        WindowSpec(1, "S", ("P", "S")), AvailabilitySpec(200, 210, 210), price_unit=PriceUnit(0, "USD"))
    return SessionCommandSpec("example1", "generation1", "A-S", "bars", example_request(), config, features,
                              (IntervalSpec("P", 0, 100), IntervalSpec("S", 100, 200)), "revision1",
                              "synthetic-conformance", 65536)


def refused(features: tuple) -> bool:
    try:
        spec(features)
    except CommandError as error:
        return error.code is CommandErrorCode.CONFIG and error.code.value == "INVALID_COMMAND"
    return False


config = ConfigSpec("synthetic-bars", "v1", (Parameter("eligibility_policy", "example-v1"),),
                    SessionSpec("demo", "S", 100, 200, "caller-supplied"),
                    WindowSpec(1, "S", ("P", "S")), AvailabilitySpec(200, 210, 210), price_unit=PriceUnit(0, "USD"))
headers = descriptor(compute_bars(None, config, entity=EntityKey("A", "S"))).features

check("core BAR_IDS has 13 ids", len(BAR_IDS) == 13)
check("workers bars header set is core's BAR_IDS object (not retyped)", supplied_session._BARS is BAR_IDS)
check("supplied bars_prior_close headers are exactly BAR_IDS in order",
      tuple(h.feature_id for h in supplied_session.supplied_headers("bars_prior_close")) == BAR_IDS)
check("native bars job spec lists exactly BAR_IDS in order", tuple(h.feature_id for h in headers) == BAR_IDS)
try:
    spec(headers)
    check("13-id spec is admitted", True)
except CommandError:
    check("13-id spec is admitted", False)
for i, fid in enumerate(BAR_IDS):
    check(f"12-id spec without {fid} is refused INVALID_COMMAND", refused(headers[:i] + headers[i + 1:]))
check("13-id spec in reversed order is refused INVALID_COMMAND", refused(tuple(reversed(headers))))
check("empty spec is refused INVALID_COMMAND", refused(()))
print("FAILED" if failures else "OK", len(failures), "failure(s)")
sys.exit(1 if failures else 0)
