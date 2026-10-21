"""Closed supplied EF1 status_history worker; no calendar mathematics."""
from __future__ import annotations
from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader
from . import _observable_routes as route
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem


def status_history_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, 'status_history')


def required_status_history_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, 'status_history')


def _admit_status_history(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    return route.admit_observable(task, batches, 'status_history')


def compute_status_history_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return route.compute(task, batches, 'status_history')


def make_ef1_status_history_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    return route.make(config, producer='status_history', calculate=compute_status_history_inputs,
        inputs=inputs, batches=batches, instrument_id=instrument_id, job_id=job_id,
        generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes)


__all__ = ['status_history_headers', 'required_status_history_inputs',
    'compute_status_history_inputs', 'make_ef1_status_history_work']
