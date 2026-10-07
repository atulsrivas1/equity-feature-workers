"""Explicit installed synthetic command demonstration; no dynamic backend loading."""
from __future__ import annotations
import argparse
import json
from equity_feature_contracts import AvailabilitySpec, ConfigSpec, EntityKey, Parameter, PriceUnit, SessionSpec, WindowSpec
from equity_feature_contracts.adapters import HistoricalAdapter
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_contracts import ResultSink, SinkRequirements
from equity_feature_io_sdk import SinkRegistry, SourceRegistry, descriptor, encode_result
from equity_features.session import compute_bars
from .commands import CommandError, CommandOutcome, SessionCommandSpec, run_registered, run_session
from .required_inputs import RequiredCommandSpec, run_required


class NoCredentials:
    def get(self, name: str) -> str | None:
        raise ValueError("Synthetic demonstration does not use credentials")


def run_demo(*, factory: bool = False) -> dict[str, object]:
    # Optional, explicitly selected tutorial components; never discovered from configuration.
    from equity_feature_example_extensions import ExampleSink, ExampleSource, SinkFactory, SourceFactory, example_request
    from equity_feature_example_extensions.sink import LIMITS
    config = ConfigSpec("synthetic-bars", "v1", (Parameter("eligibility_policy", "example-v1"),),
                        SessionSpec("demo", "S", 100, 200, "caller-supplied"),
                        WindowSpec(1, "S", ("P", "S")), AvailabilitySpec(200, 210, 210), price_unit=PriceUnit(0, "USD"))
    headers = descriptor(compute_bars(None, config, entity=EntityKey("A", "S"))).features
    spec = SessionCommandSpec("example1", "generation1", "A-S", "bars", example_request(), config, headers,
                              (IntervalSpec("P", 0, 100), IntervalSpec("S", 100, 200)), "revision1",
                              "synthetic-conformance", 65536)
    if factory:
        sources: SourceRegistry[HistoricalAdapter] = SourceRegistry()
        sinks: SinkRegistry[ResultSink] = SinkRegistry()
        sources.register("example.bars", SourceFactory())
        sinks.register("example.memory", SinkFactory())
        outcome = run_registered(spec, sources=sources, sinks=sinks, source_id="example.bars", source_config={"namespace": "demo"},
                                 sink_id="example.memory", sink_config={"destination_scope": "synthetic-conformance"},
                                 credentials=NoCredentials(), requirements=LIMITS)
    else:
        outcome = run_session(spec, ExampleSource(), ExampleSink(), requirements=LIMITS)
    record = _record(outcome, "factory" if factory else "direct", in_memory=True)
    record["values"] = {c.feature_id: c.values[0] for c in outcome.results[0].values}
    return record


def _record(outcome: CommandOutcome, mode: str, *, in_memory: bool = False) -> dict[str, object]:
    receipt = outcome.output.receipt
    assert receipt is not None
    return {"mode": mode, "verified_readback": True, "in_memory_instance_only": in_memory,
            "task_sha256": outcome.output.task.task_sha256, "content_sha256": receipt.content_sha256,
            "result": encode_result(outcome.results[0]).decode("ascii")}


def main(argv: list[str] | None = None, *, spec: SessionCommandSpec | RequiredCommandSpec | None = None,
         source: HistoricalAdapter | None = None, sink: ResultSink | None = None,
         requirements: SinkRequirements | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    choices = parser.add_mutually_exclusive_group(required=True)
    choices.add_argument("--demo", choices=("direct", "factory", "both"),
                        help="requires separately installed equity-feature-example-extensions0.1.0a0")
    choices.add_argument("--session", choices=("bars", "trades", "quotes"),
                         help="requires caller-injected spec, source, sink and requirements")
    choices.add_argument("--required", choices=("history", "sma_reference", "daily_baseline", "interval_baseline",
                        "relative_volume", "interval_relative_volume", "relative_returns"),
                        help="requires caller-injected required-input spec, sink and requirements; explicit source or absence")
    args = parser.parse_args(argv)
    try:
        if args.session is not None:
            if type(spec) is not SessionCommandSpec or source is None or sink is None or requirements is None or spec.family != args.session:
                parser.exit(2, "Session components required: use an explicit caller wrapper\n")
            records = [_record(run_session(spec, source, sink, requirements=requirements), "injected")]
        elif args.required is not None:
            if type(spec) is not RequiredCommandSpec or sink is None or requirements is None or spec.family != args.required:
                parser.exit(2, "Required-input components required: use an explicit caller wrapper\n")
            outcome = run_required(spec, source, sink, requirements=requirements)
            record = _record(outcome.command, "injected")
            witness = outcome.witness
            # Diagnostics only: the existing owned witness is returned by the Python API.
            # This is deliberately not a witness reconstruction or persistence format.
            record["owned_witness_type"] = type(witness).__name__ if witness is not None else None
            record["witness_persistence"] = "caller-owned; replay qualified inputs for reconstruction"
            records = [record]
        else:
            records = [run_demo(factory=f) for f in ((False, True) if args.demo == "both" else (args.demo == "factory",))]
    except ImportError:
        parser.exit(2, "Synthetic components unavailable: install equity-feature-example-extensions==0.1.0a0\n")
    except CommandError as error:
        parser.exit(1, str(error) + "\n")
    print(json.dumps(records, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
