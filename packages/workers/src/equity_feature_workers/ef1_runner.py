"""Thin local supplied EF1 execution/readback; closed producer-specific wrappers."""
from __future__ import annotations

from dataclasses import dataclass, field
from types import ModuleType
from typing import Callable

from equity_feature_contracts import CanonicalBatch, FeatureResult
from equity_feature_io_contracts import ResultSink, SinkRequirements
from equity_feature_io_sdk import Cancellation, encode_result, verify_receipt

from .commands import CommandErrorCode, NeverCancelled
from ._supplied import fail, translated
from .equity_sessions import _admit_anchor, compute_ef1_inputs
from .equity_clock import _admit_clock, compute_clock_inputs
from .equity_status import _admit_status, compute_status_inputs
from .manifests import OutputManifest, TaskManifest
from .publication import PublicationLimits, SerialPublisher
from .supervisor import BoundedSupervisor, Calculation, ResourceBudget, SupervisorOutcome, WorkItem


@dataclass(frozen=True)
class EF1Readback:
    task_sha256: str
    output: OutputManifest | None
    results: tuple[FeatureResult, ...] | None
    reason: str | None

    @property
    def verified(self) -> bool:
        return self.output is not None and self.results is not None and self.reason is None


@dataclass(frozen=True)
class EF1RunOutcome:
    execution: SupervisorOutcome
    readbacks: tuple[EF1Readback, ...]
    # Retain pending publisher state for caller-controlled lookup/drain/recovery.
    # The caller still owns the sink lifetime and must keep it open for recovery.
    publisher: SerialPublisher = field(repr=False, compare=False)


def _run_ef1(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget,
        publication_limits: PublicationLimits, cancellation: Cancellation | None,
        calculate: Calculation,
        admit_inputs: Callable[[TaskManifest, tuple[CanonicalBatch, ...]], ModuleType]) -> EF1RunOutcome:
    """One bounded sequential pass using the existing serialized publication path.

    Source/sink creation, path qualification and lifetime belong to the caller.
    No retry loop, process pool, dynamic import path or private source access.
    An unresolved publication remains pending; an output receipt is never erased
    merely because this additional explicit full-result readback failed.
    """
    if (type(items) is not tuple or not items or type(budget) is not ResourceBudget
            or budget.workers != 1 or budget.max_in_flight != 1
            or type(publication_limits) is not PublicationLimits
            or publication_limits.max_receipt_wire_bytes is None):
        fail()
    if len(items) > budget.max_tasks:
        fail(CommandErrorCode.LIMIT)
    jobs: dict[str, str] = {}
    generations: dict[str, str] = {}
    for item in items:
        if type(item) is not WorkItem or item.calculate is not calculate:
            fail()
        with translated():
            admit_inputs(item.task, item.batches)
        digest = item.task.config.digest
        for identities, key in ((jobs, item.task.job_id), (generations, item.task.generation_id)):
            if key in identities and identities[key] != digest:
                fail(CommandErrorCode.RESULT)
            identities[key] = digest
    scopes = {item.task.destination_scope for item in items}
    if len(scopes) != 1:
        fail()
    token = cancellation if cancellation is not None else NeverCancelled()
    publisher = SerialPublisher(sink, items[0].task.destination_scope,
        limits=publication_limits, requirements=requirements)
    execution = BoundedSupervisor(budget=budget, mode='sequential').run(
        items, cancellation=token, publisher=publisher)
    readbacks: list[EF1Readback] = []
    for task in execution.tasks:
        output = task.output
        if output is None or output.receipt is None:
            readbacks.append(EF1Readback(task.task.task_sha256, output, None,
                task.reason or 'PUBLICATION_WAITING'))
            continue
        if token.is_cancelled():
            readbacks.append(EF1Readback(task.task.task_sha256, output, None, 'CANCELLED'))
            continue
        try:
            actual = sink.read(output.receipt)
            verify_receipt(output.receipt, output.envelope, actual)
            if (task.results is None
                    or tuple(map(encode_result, actual)) != tuple(map(encode_result, task.results))):
                fail(CommandErrorCode.READBACK)
            readbacks.append(EF1Readback(task.task.task_sha256, output, actual, None))
        except Exception:
            # The structurally confirmed receipt remains visible above, including
            # any prior actual commit; this error is not a rollback assertion.
            readbacks.append(EF1Readback(task.task.task_sha256, output, None,
                CommandErrorCode.READBACK.value))
    return EF1RunOutcome(execution, tuple(readbacks), publisher)


def run_ef1_anchors(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Preserve the closed anchor-only API and its existing admission behavior."""
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_ef1_inputs, admit_inputs=_admit_anchor)


def run_ef1_clocks(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Clock-only API; reuse the unchanged bounded publication/readback path."""
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_clock_inputs, admit_inputs=_admit_clock)


def run_ef1_statuses(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Status-only API; unchanged bounded serialized publication and readback."""
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_status_inputs, admit_inputs=_admit_status)


__all__ = ['EF1Readback', 'EF1RunOutcome', 'run_ef1_anchors', 'run_ef1_clocks', 'run_ef1_statuses']


def run_ef1_path_gaps(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Gap-only API; unchanged bounded serialized publication and readback."""
    from .equity_path_gap import _admit_path_gap, compute_path_gap_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_path_gap_inputs, admit_inputs=_admit_path_gap)


def run_ef1_path_drives(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Drive-only API; unchanged bounded serialized publication and readback."""
    from .equity_path_drive import _admit_path_drive, compute_path_drive_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_path_drive_inputs, admit_inputs=_admit_path_drive)


__all__ += ['run_ef1_path_gaps', 'run_ef1_path_drives']


def run_ef1_reference_classifications(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Classification-only opt-in using the existing full readback path."""
    from .equity_reference_classification import _admit_reference_classification, compute_reference_classification_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_reference_classification_inputs, admit_inputs=_admit_reference_classification)


def run_ef1_reference_actions(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Action-admission-only opt-in; no transformation or scheduler change."""
    from .equity_reference_action import _admit_reference_action, compute_reference_action_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_reference_action_inputs, admit_inputs=_admit_reference_action)


__all__ += ['run_ef1_reference_classifications', 'run_ef1_reference_actions']


def run_ef1_periods(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Period-only opt-in using unchanged bounded publication and readback."""
    from .equity_period import _admit_period, compute_period_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_period_inputs, admit_inputs=_admit_period)


def run_ef1_extended(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Extended-phase-only opt-in; no phase or denominator mathematics."""
    from .equity_extended import _admit_extended, compute_extended_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_extended_inputs, admit_inputs=_admit_extended)


__all__ += ['run_ef1_periods', 'run_ef1_extended']


def run_ef1_opening_trade(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Closed opening_trade callback; existing bounded full readback path."""
    from .equity_opening_trade import _admit_opening_trade, compute_opening_trade_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_opening_trade_inputs, admit_inputs=_admit_opening_trade)


__all__ += ['run_ef1_opening_trade']


def run_ef1_opening_trade_seconds(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Closed opening_trade_seconds callback; existing bounded full readback path."""
    from .equity_opening_trade_seconds import _admit_opening_trade_seconds, compute_opening_trade_seconds_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_opening_trade_seconds_inputs, admit_inputs=_admit_opening_trade_seconds)


__all__ += ['run_ef1_opening_trade_seconds']


def run_ef1_status_history(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Closed status_history callback; existing bounded full readback path."""
    from .equity_status_history import _admit_status_history, compute_status_history_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_status_history_inputs, admit_inputs=_admit_status_history)


__all__ += ['run_ef1_status_history']


def run_ef1_daily_observations(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Daily-only opt-in through unchanged serialized publication/readback."""
    from .equity_daily_observations import _admit_daily_observations, compute_daily_observations_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_daily_observations_inputs, admit_inputs=_admit_daily_observations)


__all__ += ['run_ef1_daily_observations']


def run_ef1_gap_endpoints(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Endpoint-only opt-in through unchanged serialized publication/readback."""
    from .equity_gap_endpoint import _admit_gap_endpoint, compute_gap_endpoint_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_gap_endpoint_inputs, admit_inputs=_admit_gap_endpoint)


__all__ += ['run_ef1_gap_endpoints']


def run_ef1_regular_bar_prices(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Regular completed-BAR prices through existing publication/full readback."""
    from .equity_regular_bar_prices import _admit_regular_bar_prices, compute_regular_bar_price_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_regular_bar_price_inputs, admit_inputs=_admit_regular_bar_prices)


__all__ += ['run_ef1_regular_bar_prices']


def run_ef1_regular_first_trade_price(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Regular first-trade price through existing publication/full readback."""
    from .equity_regular_first_trade_price import _admit_regular_first_trade_price, compute_regular_first_trade_price_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_regular_first_trade_price_inputs, admit_inputs=_admit_regular_first_trade_price)


__all__ += ['run_ef1_regular_first_trade_price']


def run_ef1_reference_close(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Reference-close comparison through existing publication/full readback."""
    from .equity_reference_close import _admit_reference_close, compute_reference_close_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_reference_close_inputs, admit_inputs=_admit_reference_close)


__all__ += ['run_ef1_reference_close']
