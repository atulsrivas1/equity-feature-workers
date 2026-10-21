"""Closed supplied EF1 period worker; no calendar mathematics."""
from __future__ import annotations
from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader
from . import _period_routes as route
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem


def period_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, 'period')


def required_period_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, 'period')


def _admit_period(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    return route.admit_period(task, batches, 'period')


def compute_period_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return route.compute(task, batches, 'period')


def make_ef1_period_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    return route.make(config, producer='period', calculate=compute_period_inputs,
        inputs=inputs, batches=batches, instrument_id=instrument_id, job_id=job_id,
        generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes)


__all__ = ['period_headers', 'required_period_inputs',
    'compute_period_inputs', 'make_ef1_period_work']
