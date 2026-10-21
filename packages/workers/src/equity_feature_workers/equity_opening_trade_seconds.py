"""Closed supplied EF1 opening_trade_seconds worker; no calendar mathematics."""
from __future__ import annotations
from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader
from . import _observable_routes as route
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem


def opening_trade_seconds_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, 'opening_trade_seconds')


def required_opening_trade_seconds_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, 'opening_trade_seconds')


def _admit_opening_trade_seconds(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    return route.admit_observable(task, batches, 'opening_trade_seconds')


def compute_opening_trade_seconds_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return route.compute(task, batches, 'opening_trade_seconds')


def make_ef1_opening_trade_seconds_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    return route.make(config, producer='opening_trade_seconds', calculate=compute_opening_trade_seconds_inputs,
        inputs=inputs, batches=batches, instrument_id=instrument_id, job_id=job_id,
        generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes)


__all__ = ['opening_trade_seconds_headers', 'required_opening_trade_seconds_inputs',
    'compute_opening_trade_seconds_inputs', 'make_ef1_opening_trade_seconds_work']
