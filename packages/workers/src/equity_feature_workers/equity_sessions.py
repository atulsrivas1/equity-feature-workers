"""Explicit EF1 anchor routes; canonical inputs and unchanged full producer results.

Import this module explicitly. Default dispatch and other identity checks are preserved.
The EF1 route requires exactly features0.0.4a6 with canonical contracts0.0.4a5.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
import json
from types import ModuleType

from equity_feature_contracts import (
    CanonicalBatch, ConfigSpec, DataKind, EntityKey, FeatureResult,
    InputRequirement, IntervalSpec,
)
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode, _record_hash
from .manifests import InputManifest, MAX_MANIFEST_BYTES, TaskManifest
from .supervisor import WorkItem
from ._versions import CANONICAL_CONTRACTS_VERSION, FEATURES_VERSION


def _library() -> ModuleType:
    import equity_features
    from equity_feature_contracts import __version__ as contracts_version

    # Distribution version and canonical wire version are distinct. Never relax
    # exact callback backend checks to accept arbitrary feature versions.
    if (equity_features.__version__, contracts_version) != (FEATURES_VERSION, CANONICAL_CONTRACTS_VERSION):
        fail()
    from equity_features import ef1

    return ef1


def _context(config: ConfigSpec) -> tuple[ModuleType, Mapping[str, object]]:
    if type(config) is not ConfigSpec:
        fail()
    library = _library()
    context = library.decode_ef1_context(config)
    if not isinstance(context, Mapping) or context.get('producer') != 'anchor':
        fail()
    if len(config.parameters) != 1 or config.parameters[0].name != 'ef1_context_json':
        fail()
    text = config.parameters[0].value
    if type(text) is not str or not text.isascii() or not 1 <= len(text) <= 4096:
        fail()
    # The public decoder above owns the closed grammar, duplicate/unknown-field
    # checks and canonical wire admission. Rebuild the entire public config.
    expected = library.build_ef1_config(json.loads(text), session=config.session,
        window=config.window, availability=config.availability,
        price_unit=config.price_unit, adjustment=config.adjustment)
    if expected != config or expected.digest != config.digest:
        fail(CommandErrorCode.RESULT)
    return library, context


def _mode(context: Mapping[str, object]) -> tuple[str, DataKind]:
    selection = context.get('selection')
    if not isinstance(selection, Mapping):
        fail()
    mode = selection.get('mode')
    if mode == 'trades':
        return 'anchor_trades', DataKind.TRADE
    if mode == 'bars_actual_notional':
        return 'anchor_bars', DataKind.BAR
    fail()


def ef1_headers(config: ConfigSpec) -> tuple[FeatureHeader, ...]:
    """The full fixed anchor producer set; no output-subsetting semantics."""
    with translated():
        library, _ = _context(config)
        return tuple(FeatureHeader(d.feature_id, d.algorithm_version, d.dtype,
            d.unit, d.schema_version) for d in library.ef1_definitions(config))


def required_ef1_inputs(config: ConfigSpec) -> tuple[InputRequirement, ...]:
    """Return the actual public producer requirements without a second schema."""
    with translated():
        library, _ = _context(config)
        result: list[InputRequirement] = []
        for definition in library.ef1_definitions(config):
            for requirement in definition.requirements:
                if requirement not in result:
                    result.append(requirement)
        return tuple(result)


def ef1_history(config: ConfigSpec, inputs: tuple[InputManifest, ...],
                governed_sessions: tuple[IntervalSpec, ...]) -> tuple[bool, tuple[str, ...], str]:
    """Bind exact anchor context and actual declarations; no incremental state."""
    with translated():
        _, context = _context(config)
        family, _ = _mode(context)
        primary = 'trades' if family == 'anchor_trades' else 'bars'
        if (type(inputs) is not tuple or any(type(i) is not InputManifest for i in inputs)
                or tuple(i.role for i in inputs) != (primary, 'anchor_provenance')
                or type(governed_sessions) is not tuple
                or any(type(s) is not IntervalSpec for s in governed_sessions)):
            fail()
        # Frozen anchor context has calendar=null and target-only window.
        expected = (IntervalSpec(config.session.session_id,
            config.session.open_ns, config.session.close_ns),)
        if governed_sessions != expected or config.window.governed_sessions != (config.session.session_id,):
            fail(CommandErrorCode.RESULT)
        initialization, _ = _record_hash({'family': family,
            'config_digest': config.digest,
            'governed_sessions': [asdict(s) for s in governed_sessions],
            'inputs': [asdict(i) for i in inputs]}, b'efworker-ef1-anchor-init1\0', MAX_MANIFEST_BYTES)
        return True, (), initialization


def _admit_anchor(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    admit(task, batches)
    library, context = _context(task.config)
    family, kind = _mode(context)
    if task.family != family:
        fail()
    if task.features != ef1_headers(task.config):
        fail(CommandErrorCode.RESULT)
    if (task.ordered_history, task.warmup_sessions, task.initialization_sha256) != ef1_history(
            task.config, task.inputs, task.governed_sessions):
        fail(CommandErrorCode.RESULT)
    actual = iter(batches)
    for declaration, expected_kind in zip(task.inputs, (kind, DataKind.REFERENCE)):
        if declaration.binding is not None and next(actual).kind is not expected_kind:
            fail()
    return library


def compute_ef1_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    """One homogeneous anchor WorkItem produces exactly one unchanged result."""
    with translated():
        library = _admit_anchor(task, batches)
        result = library.compute_ef1(batches, task.config,
            entity=EntityKey(task.instruments[0], task.config.session.session_id))
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', FEATURES_VERSION)
        entity = EntityKey(task.instruments[0], task.config.session.session_id)
        if any(column.entities != (entity,) for column in result.values):
            fail(CommandErrorCode.RESULT)
        return (result,)


def make_ef1_anchor_work(config: ConfigSpec, *, inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = 1048576) -> WorkItem:
    """Seal caller/adapter-owned acquisition identities; no acquisition or math."""
    with translated():
        _, context = _context(config)
        family, _ = _mode(context)
        governed = (IntervalSpec(config.session.session_id,
            config.session.open_ns, config.session.close_ns),)
        history = ef1_history(config, inputs, governed)
        task = TaskManifest(job_id, generation_id, partition_id, family,
            (instrument_id,), config, ef1_headers(config), inputs, governed,
            destination_scope, 'serialized_destination', 2, max_input_bytes,
            history[0], history[1], history[2])
        _admit_anchor(task, batches)
        return WorkItem(task, batches, compute_ef1_inputs)


__all__ = ['ef1_headers', 'required_ef1_inputs', 'ef1_history',
    'compute_ef1_inputs', 'make_ef1_anchor_work']
