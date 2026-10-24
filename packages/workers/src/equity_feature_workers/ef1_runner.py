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


def run_ef1_venue_status(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """Multi-venue status through existing publication/full readback."""
    from .equity_venue_status import _admit_venue_status, compute_venue_status_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_venue_status_inputs, admit_inputs=_admit_venue_status)


__all__ += ['run_ef1_venue_status']


def run_ef2_money_flow(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF2 money flow (OBV, A/D, CMF) through existing publication/full readback."""
    from .equity_money_flow import _admit_money_flow, compute_money_flow_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_money_flow_inputs, admit_inputs=_admit_money_flow)


def run_ef2_retracement(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF2 retracement level through existing publication/full readback."""
    from .equity_retracement import _admit_retracement, compute_retracement_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_retracement_inputs, admit_inputs=_admit_retracement)


__all__ += ['run_ef2_money_flow', 'run_ef2_retracement']


def run_ef2_momentum(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF2 momentum and trend (MACD, DMI/ADX, stochastic, Williams %R) through existing publication/full readback."""
    from .equity_momentum import _admit_momentum, compute_momentum_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_momentum_inputs, admit_inputs=_admit_momentum)


__all__ += ['run_ef2_momentum']


def run_ef2_channels(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF2 Ichimoku and price channels (six producers) through existing publication/full readback."""
    from .equity_channels import _admit_channels, compute_channels_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_channels_inputs, admit_inputs=_admit_channels)


__all__ += ['run_ef2_channels']


def run_ef3_relative(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF3 relative statistics (ef3-rel1, daily grid) through existing publication/full readback."""
    from .equity_relative_rv import _admit_relative, compute_relative_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_relative_inputs, admit_inputs=_admit_relative)


def run_ef3_log_volatility(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF3 daily log volatility (ef3-dlv1, daily grid) through existing publication/full readback."""
    from .equity_relative_rv import _admit_log_volatility, compute_log_volatility_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_log_volatility_inputs, admit_inputs=_admit_log_volatility)


def run_ef3_realized_variance(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF3 intraday realized variance (ef3-rv1, R17 context) through existing publication/full readback."""
    from .equity_relative_rv import _admit_realized_variance, compute_realized_variance_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_realized_variance_inputs, admit_inputs=_admit_realized_variance)


__all__ += ['run_ef3_relative', 'run_ef3_log_volatility', 'run_ef3_realized_variance']


def run_ef3_breadth_adaptive(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF3 breadth and adaptive volume (ef3-breadth1, ef3-obsbreadth1, ef3-adaptive1) through existing publication/full readback."""
    from .equity_breadth_adaptive import admit_breadth_adaptive, compute_breadth_adaptive_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_breadth_adaptive_inputs, admit_inputs=admit_breadth_adaptive)


__all__ += ['run_ef3_breadth_adaptive']


def run_ef3_robust_history(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF3 robust history and cohort (ef3-robust1) through existing publication/full readback."""
    from .equity_robust_pace import _admit, compute_robust_history_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_robust_history_inputs, admit_inputs=lambda task, batches: _admit(task, batches, 'robust_history'))


def run_ef3_volume_pace(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF3 robust volume pace (ef3-pace1) through existing publication/full readback."""
    from .equity_robust_pace import _admit, compute_volume_pace_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_volume_pace_inputs, admit_inputs=lambda task, batches: _admit(task, batches, 'volume_pace_robust'))


def run_ef3_volume_ratio_mean(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF3 volume mean-ratio companion (ef3-mean1) through existing publication/full readback."""
    from .equity_robust_pace import _admit, compute_volume_ratio_mean_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_volume_ratio_mean_inputs, admit_inputs=lambda task, batches: _admit(task, batches, 'volume_ratio_mean'))


__all__ += ['run_ef3_robust_history', 'run_ef3_volume_pace', 'run_ef3_volume_ratio_mean']


def run_ef4_path_efficiency(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 path efficiency and choppiness (ef4-path1, R17 context) through existing publication/full readback."""
    from .equity_path_efficiency import _admit_path, compute_path_efficiency_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_path_efficiency_inputs, admit_inputs=_admit_path)


__all__ += ['run_ef4_path_efficiency']


def run_ef4_quote_dynamics(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 quote dynamics (ef4-quote1) through existing publication/full readback."""
    from .equity_quote_vwap import QUOTE_FAMILY, _admit, compute_quote_dynamics_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_quote_dynamics_inputs, admit_inputs=lambda task, batches: _admit(task, batches, QUOTE_FAMILY))


def run_ef4_vwap_path(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 VWAP path (ef4-vwap1) through existing publication/full readback."""
    from .equity_quote_vwap import VWAP_FAMILY, _admit, compute_vwap_path_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_vwap_path_inputs, admit_inputs=lambda task, batches: _admit(task, batches, VWAP_FAMILY))


__all__ += ['run_ef4_quote_dynamics', 'run_ef4_vwap_path']


def run_ef4_session_projection(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 session projection (ef4-proj1, daily grid) through existing publication/full readback."""
    from .equity_projections import SESSION_FAMILY, _admit, compute_session_projection_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_session_projection_inputs, admit_inputs=lambda task, batches: _admit(task, batches, SESSION_FAMILY))


def run_ef4_bar_projection(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 bar projection (ef4-proj1, epoch bar grid) through existing publication/full readback."""
    from .equity_projections import BAR_FAMILY, _admit, compute_bar_projection_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_bar_projection_inputs, admit_inputs=lambda task, batches: _admit(task, batches, BAR_FAMILY))


def run_ef4_relative_excess(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 relative excess (ef4-rel1) through existing publication/full readback."""
    from .equity_projections import RELATIVE_FAMILY, _admit, compute_relative_excess_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_relative_excess_inputs, admit_inputs=lambda task, batches: _admit(task, batches, RELATIVE_FAMILY))


def run_ef4_rate_projection(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 rate projection (ef4-rate1) through existing publication/full readback."""
    from .equity_projections import RATE_FAMILY, _admit, compute_rate_projection_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_rate_projection_inputs, admit_inputs=lambda task, batches: _admit(task, batches, RATE_FAMILY))


__all__ += ['run_ef4_session_projection', 'run_ef4_bar_projection', 'run_ef4_relative_excess', 'run_ef4_rate_projection']


def run_ef4_tape_window(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 tape window (ef4-tape1) through existing publication/full readback."""
    from .equity_tape import WINDOW_FAMILY, _admit, compute_tape_window_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_tape_window_inputs, admit_inputs=lambda task, batches: _admit(task, batches, WINDOW_FAMILY))


def run_ef4_tape_top_k(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 notional top-K (ef4-notional1) through existing publication/full readback."""
    from .equity_tape import TOP_K_FAMILY, _admit, compute_tape_top_k_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_tape_top_k_inputs, admit_inputs=lambda task, batches: _admit(task, batches, TOP_K_FAMILY))


def run_ef4_tape_top_k_merge(items: tuple[WorkItem, ...], sink: ResultSink, *,
        requirements: SinkRequirements, budget: ResourceBudget = ResourceBudget(),
        publication_limits: PublicationLimits = PublicationLimits(max_receipt_wire_bytes=400000),
        cancellation: Cancellation | None = None) -> EF1RunOutcome:
    """EF4 notional top-K partition merge (ef4-notional1) through existing publication/full readback."""
    from .equity_tape import MERGE_FAMILY, _admit, compute_tape_top_k_merge_inputs
    return _run_ef1(items, sink, requirements=requirements, budget=budget,
        publication_limits=publication_limits, cancellation=cancellation,
        calculate=compute_tape_top_k_merge_inputs, admit_inputs=lambda task, batches: _admit(task, batches, MERGE_FAMILY))


__all__ += ['run_ef4_tape_window', 'run_ef4_tape_top_k', 'run_ef4_tape_top_k_merge']
