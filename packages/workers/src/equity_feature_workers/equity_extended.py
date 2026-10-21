"""Closed supplied EF1 extended worker; no calendar mathematics."""
from __future__ import annotations
from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader
from . import _period_routes as route
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem


def extended_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, 'extended')


def required_extended_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, 'extended')


def _admit_extended(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    return route.admit_period(task, batches, 'extended')


def compute_extended_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return route.compute(task, batches, 'extended')


def make_ef1_extended_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    return route.make(config, producer='extended', calculate=compute_extended_inputs,
        inputs=inputs, batches=batches, instrument_id=instrument_id, job_id=job_id,
        generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes)


__all__ = ['extended_headers', 'required_extended_inputs',
    'compute_extended_inputs', 'make_ef1_extended_work']
