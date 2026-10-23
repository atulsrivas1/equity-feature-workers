"""Closed EF4 path efficiency and choppiness orchestration (``ef4-path1``, config ``ef4.path_efficiency``).

One producer, one closed route, over explicitly supplied inputs only. The worker admits the supplied BAR chunks
against the reviewed configuration and passes them to the pure library (``equity_features_intraday``). It contains no
net displacement, path length, efficiency ratio, logarithm, true range, window range or choppiness mathematics, and
never infers, fills, sorts, rescales or defaults a role, slot, window, epoch profile or grid. The library owns
admission, mathematics, quality, witnesses and every typed refusal; a refusal surfaces as a worker failure and
publishes nothing. There is no process-level registry: the configuration carries the whole governed context.
"""
from __future__ import annotations

from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, FeatureResult, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

PATH_FAMILY = 'path_efficiency'
MAX_INPUT_BYTES = 1048576


def _intraday() -> ModuleType:
    import equity_features_intraday
    return equity_features_intraday


def _param(config: ConfigSpec, name: str):
    return {p.name: p.value for p in config.parameters}.get(name)


def _features() -> tuple[str, ...]:
    return tuple(e.feature_id for e in _intraday().describe_path_efficiency())


def _selection(features: tuple[str, ...]) -> tuple[str, ...]:
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in _features() for f in features)):
        fail()
    return tuple(sorted(features))


def _intervals(sessions) -> tuple[IntervalSpec, ...]:
    return tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in sessions)


def _path_context(config: ConfigSpec):
    """Decode the chunked R17 context and require the exact reviewed envelope (the library decoder owns every rule)."""
    if type(config) is not ConfigSpec:
        fail()
    library = _intraday()
    count = _param(config, 'context_chunks')
    if type(count) is not int or not 1 <= count <= 16:
        fail()
    chunks = tuple(_param(config, f'context_{n:03}') for n in range(count))
    if any(type(chunk) is not str or not 1 <= len(chunk) <= 4096 or (n < count - 1 and len(chunk) != 4096)
            for n, chunk in enumerate(chunks)):
        fail()
    context = library.IntradayContext.from_json(''.join(chunks))
    settings = library.decode_path_efficiency_config(context, config)
    if (config.identity, config.algorithm_version, config.schema_version) != (
            library.PATH_EFFICIENCY_IDENTITY, library.PATH_EFFICIENCY_ALGORITHM, '1'):
        fail(CommandErrorCode.RESULT)
    return context, settings


def _headers(selected: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    catalog = {entry.feature_id: entry for entry in _intraday().describe_path_efficiency()}
    return tuple(FeatureHeader(f, catalog[f].algorithm_version, ValueType.FLOAT64, catalog[f].unit, '1')
                 for f in selected)


def _admit_path(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    with translated():
        library = _intraday()
        context, settings = _path_context(task.config)
        if task.family != PATH_FAMILY or task.instruments != (context.instrument_id,):
            fail(CommandErrorCode.RESULT)
        admit(task, batches)
        if (task.max_input_bytes > MAX_INPUT_BYTES or task.math_policy_version != 'v1'
                or task.ownership != 'serialized_destination'):
            fail(CommandErrorCode.LIMIT if task.max_input_bytes > MAX_INPUT_BYTES else CommandErrorCode.RESULT)
        selected = _selection(tuple(h.feature_id for h in task.features))
        for feature in selected:
            library.require_path_efficiency_capability(feature, 'batch')
        if task.features != _headers(selected) or task.governed_sessions != _intervals(context.sessions):
            fail(CommandErrorCode.RESULT)
        if task.ordered_history is not True or task.warmup_sessions != () or task.initialization_sha256 != context.digest:
            fail(CommandErrorCode.RESULT)
        if not batches and not (len(task.inputs) == 1 and task.inputs[0].state == 'missing'):
            fail()
        return library


def compute_path_efficiency_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_path(task, batches)
        context, settings = _path_context(task.config)
        selected = tuple(h.feature_id for h in task.features)
        # Every value, status, reason, count and witness is the library's; nothing is computed here.
        result = library.compute_path_efficiency(batches or None, context, task.config, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'intraday.python', library.__version__)
        return (result,)


def make_ef4_path_efficiency_work(config: ConfigSpec, *, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    with translated():
        context, settings = _path_context(config)
        if instrument_id != context.instrument_id:
            fail(CommandErrorCode.RESULT)
        selected = _selection(features)
        task = TaskManifest(job_id, generation_id, partition_id, PATH_FAMILY, (instrument_id,), config,
            _headers(selected), inputs, _intervals(context.sessions), destination_scope,
            'serialized_destination', max(1, len(batches)), max_input_bytes, True, (), context.digest)
        _admit_path(task, batches)
        return WorkItem(task, batches, compute_path_efficiency_inputs)


__all__ = ['PATH_FAMILY', 'make_ef4_path_efficiency_work', 'compute_path_efficiency_inputs']
