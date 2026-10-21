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
