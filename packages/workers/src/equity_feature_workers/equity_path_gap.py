"""Closed supplied EF1 path_gap orchestration; full unchanged producer result."""
from __future__ import annotations

from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader
from . import _path_routes as route
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem


def path_gap_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, 'path_gap')


def required_path_gap_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, 'path_gap')


def _admit_path_gap(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    return route.admit_path(task, batches, 'path_gap')


def compute_path_gap_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return route.compute(task, batches, 'path_gap')


def make_ef1_path_gap_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    return route.make(config, producer='path_gap', calculate=compute_path_gap_inputs,
        inputs=inputs, batches=batches, instrument_id=instrument_id, job_id=job_id,
        generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes)


__all__ = ['path_gap_headers', 'required_path_gap_inputs',
    'compute_path_gap_inputs', 'make_ef1_path_gap_work']
