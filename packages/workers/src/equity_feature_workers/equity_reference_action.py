"""Closed supplied EF1 reference_action worker; no reference mathematics."""
from __future__ import annotations
from types import ModuleType
from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, InputRequirement
from equity_feature_io_contracts import FeatureHeader
from . import _reference_routes as route
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem


def reference_action_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    return route.headers(config, 'reference_action')


def required_reference_action_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    return route.requirements(config, 'reference_action')


def required_reference_action_alternatives(config: ConfigSpec) -> tuple[tuple[InputRequirement, ...], ...]:
    return route.alternatives(config, 'reference_action')


def _admit_reference_action(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    return route.admit_reference(task, batches, 'reference_action')


def compute_reference_action_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return route.compute(task, batches, 'reference_action')


def make_ef1_reference_action_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    return route.make(config, producer='reference_action', calculate=compute_reference_action_inputs,
        inputs=inputs, batches=batches, instrument_id=instrument_id, job_id=job_id,
        generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes)


__all__ = ['reference_action_headers', 'required_reference_action_inputs',
    'required_reference_action_alternatives', 'compute_reference_action_inputs', 'make_ef1_reference_action_work']
