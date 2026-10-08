"""Explicitly approved owned CSV -> planned factory -> accepted worker readback."""
from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import time

from equity_feature_contracts import (AvailabilitySpec, ConfigSpec, Coverage, DataKind, EntityKey,
    InputScope, Parameter, PriceUnit, SessionSpec, SourceBinding, WindowSpec)
from equity_feature_contracts.adapters import AcquisitionRequest, AdapterCapabilities, HistoricalAdapter
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_contracts import ResultSink
from equity_feature_io_sdk import SinkRegistry, SourceRegistry, descriptor, encode_result
from equity_features.session import compute_trades

from .acquisition import ExecutionApproval, PlanningError, PlanningErrorCode, _call, execute_plan, plan_acquisition, SourceOffer
from .commands import CommandOutcome, SessionCommandSpec

OWNED_CSV = ("instrument_id,session_id,event_ns,order_key,event_id,eligible,price,size,known_at_ns\n"
    "A,S,110,1,owned-t1,1,100,2,110\nA,S,130,2,owned-t2,1,102,3,130\n"
    "A,S,160,3,owned-t3,1,101,5,210\n").encode()


class NoCredentials:
    def get(self, name: str) -> str | None:
        raise AssertionError("Owned local demo must never consult credentials")


def run_owned_acquisition(*, approved: bool = False, reconstruction: bool = True) -> dict[str, object]:
    """Approval applies only to the generated owned synthetic fixture; default denied."""
    if type(approved) is not bool or type(reconstruction) is not bool:
        raise PlanningError(PlanningErrorCode.INVALID)
    if not approved:
        raise PlanningError(PlanningErrorCode.UNAUTHORIZED)
    return _call(lambda: _run(reconstruction), PlanningErrorCode.NO_CAPABILITY)


def _run(reconstruction: bool) -> dict[str, object]:
    # Explicit demo extras only; default worker imports neither concrete files nor codecs.
    from equity_feature_acquisition import AcquisitionLimits, AcquisitionScope, DownloadApproval
    from equity_feature_files import FileProfile, LocalFileFactory
    from equity_feature_example_extensions import ExampleSink
    from equity_feature_example_extensions.factories import SinkFactory
    from equity_feature_example_extensions.sink import LIMITS

    def owned_local_use(scope: AcquisitionScope) -> DownloadApproval:
        return DownloadApproval(scope, "explicit-owned-fixture-use", time.monotonic_ns()+10_000_000_000,
                                AcquisitionLimits(1, 3, 4096, 10_000_000_000))

    unit = PriceUnit(0, "USD")
    availability = AvailabilitySpec(200, 210 if reconstruction else 150, 210)
    if reconstruction:
        availability = replace(availability, mode="reconstruction", reconstruction_reason="owned-replay")
    config = ConfigSpec("owned-trades", "v1", (Parameter("eligibility_policy", "example-v1"),),
        SessionSpec("demo", "S", 100, 200, "supplied"), WindowSpec(1, "S", ("P", "S")), availability, price_unit=unit)
    request = AcquisitionRequest("owned-request", DataKind.TRADE, "demo", ("A",), ("S",), 100, 200,
        "owned-snapshot1", unit, availability, max_batch_rows=3, max_rows=3, max_batches=1)
    spec = SessionCommandSpec("owned-job", "owned-generation", "A-S", "trades", request, config,
        descriptor(compute_trades(None, config, entity=EntityKey("A", "S"))).features,
        (IntervalSpec("P", 0, 100), IntervalSpec("S", 100, 200)), "owned-revision1", "synthetic-conformance", 65536)
    profile = FileProfile("csv", "demo", SourceBinding("owned-local", "owned-snapshot1", "mapping1", "fixture1"),
        unit, Coverage(3, 3, True), InputScope(100, 200, "example-v1"), hashlib.sha256(OWNED_CSV).hexdigest())
    sources: SourceRegistry[HistoricalAdapter] = SourceRegistry()
    sources.register("owned.csv", LocalFileFactory(profile, approve=owned_local_use))
    sinks: SinkRegistry[ResultSink] = SinkRegistry()
    sinks.register("owned.memory", SinkFactory())
    with tempfile.TemporaryDirectory(prefix="owned-acquisition-") as temporary:
        path = Path(temporary)/"owned.csv"
        path.write_bytes(OWNED_CSV)
        offer = SourceOffer("owned.csv", AdapterCapabilities((DataKind.TRADE,), ("demo",), (unit,)), {"path": str(path)})
        plan = plan_acquisition(spec, ("session.trade.volume", "session.trade.vwap"), sources=sources, offers=(offer,),
            sinks=sinks, sink_id="owned.memory", sink_config={"destination_scope": "synthetic-conformance"}, requirements=LIMITS)
        outcome = execute_plan(spec, plan, sources=sources, sinks=sinks, credentials=NoCredentials(),
                               authorize=lambda p: ExecutionApproval(p.plan_sha256))
        assert isinstance(outcome, CommandOutcome)
        # Existing worker verifies source/result/receipt bytes; assert the literal arithmetic independently.
        values = {c.feature_id: c.values[0] for c in outcome.results[0].values}
        if reconstruction:
            assert values["session.trade.count"] == 3 and values["session.trade.volume"] == 10
            assert values["session.trade.notional"] == 1011
            vwap = values["session.trade.vwap"]
            assert type(vwap) is float and abs(vwap-101.1) < 1e-12
            mean = values["session.trade.mean_size"]
            assert type(mean) is float and abs(mean-10/3) < 1e-12
        receipt = outcome.output.receipt
        assert receipt is not None and outcome.output.committed
        return {"owned_synthetic_only": True, "provider_access": False, "in_memory_instance_only": True,
                "verified_readback": True, "requested_features": list(plan.requested_features),
                "execution_features": list(plan.execution_features), "plan_sha256": plan.plan_sha256,
                "task_sha256": outcome.output.task.task_sha256, "content_sha256": receipt.content_sha256,
                "values": values, "result": encode_result(outcome.results[0]).decode("ascii")}
