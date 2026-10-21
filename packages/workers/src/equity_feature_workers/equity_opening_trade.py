"""Closed supplied EF1 opening_trade worker; no calendar mathematics."""
from __future__ import annotations
from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader
from . import _observable_routes as route
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem


def opening_trade_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, 'opening_trade')


def required_opening_trade_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, 'opening_trade')


def _admit_opening_trade(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    return route.admit_observable(task, batches, 'opening_trade')


def compute_opening_trade_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return route.compute(task, batches, 'opening_trade')


def make_ef1_opening_trade_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    return route.make(config, producer='opening_trade', calculate=compute_opening_trade_inputs,
        inputs=inputs, batches=batches, instrument_id=instrument_id, job_id=job_id,
        generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes)


__all__ = ['opening_trade_headers', 'required_opening_trade_inputs',
    'compute_opening_trade_inputs', 'make_ef1_opening_trade_work']
