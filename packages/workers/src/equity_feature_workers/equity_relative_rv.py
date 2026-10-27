"""Closed EF3 relative-statistics, daily log-volatility and realized-variance orchestration (ef3-rel1, ef3-dlv1, ef3-rv1).

Three producers, one closed route each, over explicitly supplied inputs only. The worker admits the supplied
batches against the reviewed configuration and passes them to the pure library (``equity_features.ef3`` for the two
daily-grid producers, ``equity_features_intraday`` for realized variance). It contains no return, beta, correlation,
logarithm, variance, volatility or annualization mathematics and never infers, fills, sorts, rescales or defaults a
role, slot, window, period, factor or grid. The library owns admission, mathematics, quality, witnesses and every
typed refusal; a refusal surfaces as a worker failure and publishes nothing.

The daily producers bind a local ``DailyGrid`` by its SHA-256 (the library carries no grid in the configuration).
The grid is registered under its own digest when the work item is built, and calculation resolves it by the digest
that the configuration binds; the library verifies the digest again. Registration is content-addressed and in-process,
so the two daily routes are for the sequential in-process runner only.
"""
from __future__ import annotations

import pickle
from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, ErrorCode, FeatureResult, IntervalSpec, ValueType
from equity_feature_io_contracts import FeatureHeader

from ._supplied import admit, fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

RELATIVE_FAMILY = 'relative_stats'
LOG_VOL_FAMILY = 'daily_log_volatility'
RV_FAMILY = 'intraday_realized_variance'
LOG_VOL_ROLE = 'daily'
MAX_INPUT_BYTES = 1048576
_GRIDS: dict[str, object] = {}


def _ef3() -> ModuleType:
    from equity_features import ef3
    return ef3


def _package_version() -> str:
    import equity_features
    return equity_features.__version__


def _intraday() -> ModuleType:
    import equity_features_intraday
    return equity_features_intraday


def _param(config: ConfigSpec, name: str):
    return {p.name: p.value for p in config.parameters}.get(name)


def _selection(features: tuple[str, ...], allowed: tuple[str, ...]) -> tuple[str, ...]:
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features)):
        fail(contract_code=ErrorCode.INVALID_SCHEMA.value)
    if any(f not in allowed for f in features):
        fail(contract_code=ErrorCode.UNKNOWN_FEATURE.value)
    return tuple(sorted(features))


def _register(grid: object, config: ConfigSpec) -> None:
    """Keep a daily grid only under its own digest, and only when the configuration binds exactly that digest."""
    if type(config) is not ConfigSpec or type(grid) is not _ef3().DailyGrid or grid.digest != _param(config, 'grid_sha256'):
        fail(CommandErrorCode.CONFIG)
    _GRIDS[grid.digest] = grid


def _registered_grid(config: ConfigSpec):
    digest = _param(config, 'grid_sha256')
    grid = _GRIDS.get(digest) if type(digest) is str else None
    if grid is None or grid.digest != digest:
        fail(CommandErrorCode.CONFIG)
    return grid


def _admit_roles(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str,
        instruments: dict[str, str]) -> None:
    """Role-keyed admission: each bound batch matches its manifest binding, namespace, coverage and role instrument."""
    if (type(task) is not TaskManifest or type(batches) is not tuple or any(type(b) is not CanonicalBatch for b in batches)
            or task.family != family or task.ownership != 'serialized_destination' or task.math_policy_version != 'v1'
            or len(task.instruments) != 1):
        fail()
    if task.max_input_bytes > MAX_INPUT_BYTES:
        fail(CommandErrorCode.LIMIT)
    if len(batches) > task.max_input_batches or len(pickle.dumps(batches, protocol=5)) > task.max_input_bytes:
        fail(CommandErrorCode.LIMIT)
    if {i.role for i in task.inputs} != set(instruments):
        fail(CommandErrorCode.RESULT)
    bound = tuple(i for i in task.inputs if i.binding is not None)
    if len(bound) != len(batches) or len({b.metadata.source.input_id for b in batches}) != len(batches):
        fail(CommandErrorCode.RESULT)
    for manifest, batch in zip(bound, batches):
        binding = manifest.binding
        if ((binding.role, binding.kind, binding.metadata) != (manifest.role, batch.kind, batch.metadata)
                or batch.metadata.coverage.observed != batch.row_count
                or batch.metadata.namespace != task.config.session.namespace):
            # A coverage or namespace contradiction (binding matches the manifest) mirrors the library's INCONSISTENT_IDENTITY
            # and carries that contract code; a manifest/binding mismatch has no library counterpart and carries none.
            fail(CommandErrorCode.RESULT, ErrorCode.INCONSISTENT_IDENTITY.value
                 if (binding.role, binding.kind, binding.metadata) == (manifest.role, batch.kind, batch.metadata) else None)
        column = batch.column('instrument_id')
        if column is None or any(value != instruments[manifest.role] for value in column.values):
            fail(CommandErrorCode.RESULT, ErrorCode.INCONSISTENT_IDENTITY.value)


def _role_batches(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> dict[str, CanonicalBatch]:
    bound = tuple(i for i in task.inputs if i.binding is not None)
    return {manifest.role: batch for manifest, batch in zip(bound, batches)}


def _daily_headers(grid: object, config: ConfigSpec, selected: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    """Headers from the library's own discovery for the bound grid and configuration (the worker owns no units)."""
    definitions = {d.feature_id: d for d in _ef3().ef3_definitions(grid, config)}
    return tuple(FeatureHeader(f, definitions[f].algorithm_version, definitions[f].dtype, definitions[f].unit, '1')
                 for f in selected)


def _intervals(sessions) -> tuple[IntervalSpec, ...]:
    return tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in sessions)


# ---- ef3.relative_stats ---------------------------------------------------------------------------------------------

def _admit_relative(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    with translated():
        library = _ef3()
        grid = _registered_grid(task.config)
        settings = library.decode_relative_stats_config(grid, task.config)
        if (task.config.identity, task.config.algorithm_version, task.config.schema_version) != (
                library.RELATIVE_IDENTITY, library.RELATIVE_ALGORITHM, '1'):
            fail(CommandErrorCode.RESULT)
        instruments = {'instrument': grid.instrument_id, 'benchmark': settings.benchmark_id}
        _admit_roles(task, batches, RELATIVE_FAMILY, instruments)
        selected = _selection(tuple(h.feature_id for h in task.features), library.RELATIVE_FEATURES)
        target, lo = grid.target_index, max(0, grid.target_index - settings.window)
        if (task.features != _daily_headers(grid, task.config, selected) or task.instruments != (grid.instrument_id,)
                or task.governed_sessions != _intervals(grid.sessions) or task.ordered_history is not True
                or task.warmup_sessions != tuple(s.session_id for s in grid.sessions[lo:target])
                or task.initialization_sha256 != grid.digest):
            fail(CommandErrorCode.RESULT)
        return library


def compute_relative_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_relative(task, batches)
        grid = _registered_grid(task.config)
        selected = _selection(tuple(h.feature_id for h in task.features), library.RELATIVE_FEATURES)
        roles = _role_batches(task, batches)
        # Operands, correlation, beta, residual, readiness and witnesses are the library's; nothing is computed here.
        result = library.compute_relative_stats((roles.get('instrument'), roles.get('benchmark')), grid, task.config,
            features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', _package_version())
        return (result,)


def make_ef3_relative_work(config: ConfigSpec, *, grid: object, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    with translated():
        library = _ef3()
        _register(grid, config)
        if instrument_id != grid.instrument_id:
            fail(CommandErrorCode.RESULT)
        selected = _selection(features, library.RELATIVE_FEATURES)
        settings = library.decode_relative_stats_config(grid, config)
        lo = max(0, grid.target_index - settings.window)
        task = TaskManifest(job_id, generation_id, partition_id, RELATIVE_FAMILY, (instrument_id,), config,
            _daily_headers(grid, config, selected), inputs, _intervals(grid.sessions), destination_scope,
            'serialized_destination', max(1, len(batches)), max_input_bytes, True,
            tuple(s.session_id for s in grid.sessions[lo:grid.target_index]), grid.digest)
        _admit_relative(task, batches)
        return WorkItem(task, batches, compute_relative_inputs)


# ---- ef3.daily_log_volatility ---------------------------------------------------------------------------------------

def _admit_log_volatility(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    with translated():
        library = _ef3()
        grid = _registered_grid(task.config)
        settings = library.decode_log_volatility_config(grid, task.config)
        if (task.config.identity, task.config.algorithm_version, task.config.schema_version) != (
                library.LOG_VOL_IDENTITY, library.LOG_VOL_ALGORITHM, '1'):
            fail(CommandErrorCode.RESULT)
        _admit_roles(task, batches, LOG_VOL_FAMILY, {LOG_VOL_ROLE: grid.instrument_id})
        selected = _selection(tuple(h.feature_id for h in task.features), library.LOG_VOL_FEATURES)
        lo = max(0, grid.target_index - settings.period)
        if (task.features != _daily_headers(grid, task.config, selected)
                or task.instruments != (grid.instrument_id,) or task.governed_sessions != _intervals(grid.sessions)
                or task.ordered_history is not True
                or task.warmup_sessions != tuple(s.session_id for s in grid.sessions[lo:grid.target_index])
                or task.initialization_sha256 != grid.digest):
            fail(CommandErrorCode.RESULT)
        return library


def compute_log_volatility_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_log_volatility(task, batches)
        grid = _registered_grid(task.config)
        selected = _selection(tuple(h.feature_id for h in task.features), library.LOG_VOL_FEATURES)
        roles = _role_batches(task, batches)
        # Logarithms, centering, annualization and readiness are the library's; the worker only forwards.
        result = library.compute_log_volatility((roles.get(LOG_VOL_ROLE),), grid, task.config, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', _package_version())
        return (result,)


def make_ef3_log_volatility_work(config: ConfigSpec, *, grid: object, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    with translated():
        library = _ef3()
        _register(grid, config)
        if instrument_id != grid.instrument_id:
            fail(CommandErrorCode.RESULT)
        selected = _selection(features, library.LOG_VOL_FEATURES)
        settings = library.decode_log_volatility_config(grid, config)
        lo = max(0, grid.target_index - settings.period)
        task = TaskManifest(job_id, generation_id, partition_id, LOG_VOL_FAMILY, (instrument_id,), config,
            _daily_headers(grid, config, selected), inputs, _intervals(grid.sessions), destination_scope,
            'serialized_destination', max(1, len(batches)), max_input_bytes, True,
            tuple(s.session_id for s in grid.sessions[lo:grid.target_index]), grid.digest)
        _admit_log_volatility(task, batches)
        return WorkItem(task, batches, compute_log_volatility_inputs)


# ---- ef3.intraday_realized_variance ---------------------------------------------------------------------------------

def _rv_context(config: ConfigSpec):
    """Decode the chunked R17 context and require the exact reviewed configuration (the library decoder)."""
    if type(config) is not ConfigSpec or config.price_unit is None:
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
    settings = library.decode_realized_variance_config(context, config)
    if (config.identity, config.algorithm_version, config.schema_version) != (
            library.REALIZED_VARIANCE_IDENTITY, library.REALIZED_VARIANCE_ALGORITHM, '1'):
        fail(CommandErrorCode.RESULT)
    return context, settings


def _rv_headers(selected: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    catalog = {entry.feature_id: entry for entry in _intraday().describe_realized_variance()}
    return tuple(FeatureHeader(f, catalog[f].algorithm_version, ValueType.FLOAT64, catalog[f].unit, '1')
                 for f in selected)


def _admit_realized_variance(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> ModuleType:
    with translated():
        library = _intraday()
        context, settings = _rv_context(task.config)
        if task.family != RV_FAMILY or task.instruments != (context.instrument_id,):
            fail(CommandErrorCode.RESULT)
        admit(task, batches)
        if task.max_input_bytes > MAX_INPUT_BYTES or task.math_policy_version != 'v1' or task.ownership != 'serialized_destination':
            fail(CommandErrorCode.LIMIT if task.max_input_bytes > MAX_INPUT_BYTES else CommandErrorCode.RESULT)
        selected = _selection(tuple(h.feature_id for h in task.features),
            tuple(e.feature_id for e in library.describe_realized_variance()))
        if task.features != _rv_headers(selected):
            fail(CommandErrorCode.RESULT)
        if task.governed_sessions != _intervals(context.sessions):
            fail(CommandErrorCode.RESULT)
        if task.ordered_history is not True or task.warmup_sessions != () or task.initialization_sha256 != context.digest:
            fail(CommandErrorCode.RESULT)
        if not batches and not (len(task.inputs) == 1 and task.inputs[0].state == 'missing'):
            fail()
        return library


def compute_realized_variance_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit_realized_variance(task, batches)
        context, settings = _rv_context(task.config)
        selected = tuple(h.feature_id for h in task.features)
        result = library.compute_realized_variance(batches or None, context, task.config, features=selected)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'intraday.python', library.__version__)
        return (result,)


def make_ef3_realized_variance_work(config: ConfigSpec, *, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str,
        job_id: str, generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    with translated():
        context, settings = _rv_context(config)
        if instrument_id != context.instrument_id:
            fail(CommandErrorCode.RESULT)
        selected = _selection(features, tuple(e.feature_id for e in _intraday().describe_realized_variance()))
        task = TaskManifest(job_id, generation_id, partition_id, RV_FAMILY, (instrument_id,), config,
            _rv_headers(selected), inputs, _intervals(context.sessions), destination_scope,
            'serialized_destination', max(1, len(batches)), max_input_bytes, True, (), context.digest)
        _admit_realized_variance(task, batches)
        return WorkItem(task, batches, compute_realized_variance_inputs)


__all__ = ['RELATIVE_FAMILY', 'LOG_VOL_FAMILY', 'RV_FAMILY', 'make_ef3_relative_work', 'compute_relative_inputs',
           'make_ef3_log_volatility_work', 'compute_log_volatility_inputs', 'make_ef3_realized_variance_work',
           'compute_realized_variance_inputs']
