"""Closed EF4 statistical projections orchestration (ef4-proj1, ef4-rel1, ef4-rate1; contract I2, EQ-172).

Four producers, one closed route each, over explicitly supplied batches only: session projection (daily close grid),
bar projection (epoch bar grid), relative excess (leg and membership facts) and rate projection (tape-window and leg
facts). The worker admits the supplied batches against the reviewed configuration, binds them to the fixed library role
slots and calls the closed public library dispatch (``equity_features.ef4.compute_ef4_path_projections``). It contains no
return, excess, drawdown, SMA, dollar-volume, rate or comparison mathematics and never infers, fills, sorts, rescales,
imputes or defaults a role, slot, grid, benchmark, window or leg. The library owns admission, mathematics, quality,
witnesses and every typed refusal; a refusal surfaces as a worker failure and publishes nothing.

No entity, instrument, benchmark, session, role, grid or reference identifier is fixed here: the target instrument comes
from the task manifest, the session and the benchmarks from the configuration. The two grid producers (session and bar
projection) bind their grid by its SHA-256 as the EF3 daily routes do (the library carries no grid in the
configuration): the grid is registered under its own digest when the work item is built and calculation resolves it by
the digest that the configuration binds, which the library verifies again. Registration is content-addressed and
in-process, so those two routes are for the sequential in-process runner only. The relative-excess and rate routes need
no registry.
"""
from __future__ import annotations

import pickle
from types import ModuleType

from equity_feature_contracts import CanonicalBatch, ConfigSpec, EntityKey, FeatureResult, IntervalSpec
from equity_feature_io_contracts import FeatureHeader

from ._supplied import fail, result_identity, translated
from .commands import CommandErrorCode
from .manifests import InputManifest, TaskManifest
from .supervisor import WorkItem

SESSION_FAMILY = 'session_projection'
BAR_FAMILY = 'bar_projection'
RELATIVE_FAMILY = 'relative_excess'
RATE_FAMILY = 'rate_projection'
MAX_INPUT_BYTES = 1048576
# family -> (identity name, algorithm name, features name) in the library, and the library's fixed role slot order.
_PRODUCERS = {
    SESSION_FAMILY: ('SESSION_IDENTITY', 'SESSION_ALGORITHM', 'SESSION_FEATURES', ('daily',)),
    BAR_FAMILY: ('BAR_IDENTITY', 'BAR_ALGORITHM', 'BAR_FEATURES', ('bars',)),
    RELATIVE_FAMILY: ('RELATIVE_IDENTITY', 'RELATIVE_ALGORITHM', 'RELATIVE_FEATURES', ('legs', 'membership')),
    RATE_FAMILY: ('RATE_IDENTITY', 'RATE_ALGORITHM', 'RATE_FEATURES', ('tape', 'legs')),
}
# Roles whose rows must all belong to the target instrument. The relative-excess legs batch carries the benchmarks'
# legs as well, so the library (not the worker) decides on its instruments.
_TARGET_ONLY = {SESSION_FAMILY: ('daily',), BAR_FAMILY: ('bars',), RELATIVE_FAMILY: ('membership',),
                RATE_FAMILY: ('tape', 'legs')}
_GRIDS: dict[str, object] = {}


def _ef4() -> ModuleType:
    from equity_features import ef4
    return ef4


def _package_version() -> str:
    import equity_features
    return equity_features.__version__


def _param(config: ConfigSpec, name: str):
    return {p.name: p.value for p in config.parameters}.get(name)


def _selection(features: tuple[str, ...], allowed: tuple[str, ...]) -> tuple[str, ...]:
    if (type(features) is not tuple or not features or any(type(f) is not str for f in features)
            or len(set(features)) != len(features) or any(f not in allowed for f in features)):
        fail()
    return tuple(sorted(features))


def _grid_type(library: ModuleType, family: str):
    if family == SESSION_FAMILY:
        from equity_features import ef3
        return ef3.DailyGrid
    return library.BarGrid


def _register(grid: object, config: ConfigSpec, family: str) -> None:
    """Keep a grid only under its own digest, and only when the configuration binds exactly that digest."""
    if (type(config) is not ConfigSpec or type(grid) is not _grid_type(_ef4(), family)
            or grid.digest != _param(config, 'grid_sha256')):
        fail(CommandErrorCode.CONFIG)
    _GRIDS[grid.digest] = grid


def _registered_grid(config: ConfigSpec, family: str):
    """The grid of a grid producer (None for the fact producers, which carry no grid)."""
    if family not in (SESSION_FAMILY, BAR_FAMILY):
        return None
    digest = _param(config, 'grid_sha256')
    grid = _GRIDS.get(digest) if type(digest) is str else None
    if grid is None or grid.digest != digest:
        fail(CommandErrorCode.CONFIG)
    return grid


def _headers(config: ConfigSpec, grid: object, selected: tuple[str, ...]) -> tuple[FeatureHeader, ...]:
    """Headers from the library's own discovery for the configuration (the worker owns no units or dtypes)."""
    definitions = {d.feature_id: d for d in _ef4().ef4_path_projections_definitions(config, grid=grid)}
    if set(selected) - set(definitions):
        fail()  # an output the configuration cannot execute (for example a primary output without a primary benchmark)
    return tuple(FeatureHeader(f, definitions[f].algorithm_version, definitions[f].dtype, definitions[f].unit, '1')
                 for f in selected)


def _intervals(sessions) -> tuple[IntervalSpec, ...]:
    return tuple(IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in sessions)


def _history(family: str, grid: object, config: ConfigSpec):
    """Governed sessions, ordered-history flag, warmup sessions and initialization digest for the family."""
    if family == SESSION_FAMILY:
        window = _ef4().decode_session_projection_config(grid, config).window
        lo = max(0, grid.target_index - window)
        return (_intervals(grid.sessions), True, tuple(s.session_id for s in grid.sessions[lo:grid.target_index]),
                grid.digest)
    if family == BAR_FAMILY:
        return _intervals((grid.session,)), True, (), grid.digest
    return _intervals((config.session,)), False, (), None


def _admit(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str) -> ModuleType:
    """Shared closed admission: family, identity, grid, headers, role-keyed bindings and the target instrument."""
    with translated():
        identity, algorithm, features_name, roles = _PRODUCERS[family]
        library = _ef4()
        if (type(task) is not TaskManifest or type(batches) is not tuple or any(type(b) is not CanonicalBatch for b in batches)
                or task.family != family or task.ownership != 'serialized_destination' or task.math_policy_version != 'v1'
                or len(task.instruments) != 1):
            fail()
        config = task.config
        if type(config) is not ConfigSpec or (config.identity, config.algorithm_version, config.schema_version) != (
                getattr(library, identity), getattr(library, algorithm), '1'):
            fail(CommandErrorCode.RESULT)
        if task.max_input_bytes > MAX_INPUT_BYTES:
            fail(CommandErrorCode.LIMIT)
        if len(batches) > task.max_input_batches or len(pickle.dumps(batches, protocol=5)) > task.max_input_bytes:
            fail(CommandErrorCode.LIMIT)
        grid = _registered_grid(config, family)
        if grid is not None and task.instruments != (grid.instrument_id,):
            fail(CommandErrorCode.RESULT)
        selected = _selection(tuple(h.feature_id for h in task.features), getattr(library, features_name))
        if task.features != _headers(config, grid, selected):
            fail(CommandErrorCode.RESULT)
        if {i.role for i in task.inputs} - set(roles):
            fail(CommandErrorCode.RESULT)
        bound = tuple(i for i in task.inputs if i.binding is not None)
        if (len(bound) != len(batches) or len({b.metadata.source.input_id for b in batches}) != len(batches)
                or len({i.role for i in bound}) != len(bound)):
            fail(CommandErrorCode.RESULT)
        for manifest, batch in zip(bound, batches):
            binding = manifest.binding
            if ((binding.role, binding.kind, binding.metadata) != (manifest.role, batch.kind, batch.metadata)
                    or batch.metadata.coverage.observed != batch.row_count
                    or batch.metadata.namespace != config.session.namespace or manifest.role not in roles):
                fail(CommandErrorCode.RESULT)
            if manifest.role in _TARGET_ONLY[family]:
                column = batch.column('instrument_id')
                if column is None or any(value != task.instruments[0] for value in column.values):
                    fail(CommandErrorCode.RESULT)
        governed, ordered, warmup, initialization = _history(family, grid, config)
        if (task.governed_sessions != governed or task.ordered_history is not ordered or task.warmup_sessions != warmup
                or task.initialization_sha256 != initialization):
            fail(CommandErrorCode.RESULT)
        return library


def _slots(task: TaskManifest, batches: tuple[CanonicalBatch, ...], roles: tuple[str, ...]) -> tuple:
    """Bind each supplied batch to its fixed library slot by its manifest role; roles not delivered stay None."""
    by_role = {manifest.role: batch for manifest, batch in zip(
        tuple(i for i in task.inputs if i.binding is not None), batches)}
    return tuple(by_role.get(role) for role in roles)


def _compute(task: TaskManifest, batches: tuple[CanonicalBatch, ...], family: str) -> tuple[FeatureResult, ...]:
    with translated():
        library = _admit(task, batches, family)
        roles = _PRODUCERS[family][3]
        selected = tuple(h.feature_id for h in task.features)
        grid = _registered_grid(task.config, family)
        entity = None if grid is not None else EntityKey(task.instruments[0], task.config.session.session_id)
        # Every value, status, reason, count and witness is the library's; nothing is computed here.
        result = library.compute_ef4_path_projections(_slots(task, batches, roles), task.config, features=selected,
            grid=grid, entity=entity)
        if type(result) is not FeatureResult:
            fail(CommandErrorCode.RESULT)
        result_identity(task, result, 'python-exact', _package_version())
        return (result,)


def compute_session_projection_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, SESSION_FAMILY)


def compute_bar_projection_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, BAR_FAMILY)


def compute_relative_excess_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, RELATIVE_FAMILY)


def compute_rate_projection_inputs(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> tuple[FeatureResult, ...]:
    return _compute(task, batches, RATE_FAMILY)


def _make(config: ConfigSpec, family: str, *, grid: object, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str, max_input_bytes: int, calculate) -> WorkItem:
    with translated():
        features_name = _PRODUCERS[family][2]
        if type(config) is not ConfigSpec:
            fail()
        if family in (SESSION_FAMILY, BAR_FAMILY):
            _register(grid, config, family)
            if instrument_id != grid.instrument_id:
                fail(CommandErrorCode.RESULT)
        elif grid is not None:
            fail()  # the fact producers carry no grid
        selected = _selection(features, getattr(_ef4(), features_name))
        registered = _registered_grid(config, family)
        governed, ordered, warmup, initialization = _history(family, registered, config)
        task = TaskManifest(job_id, generation_id, partition_id, family, (instrument_id,), config,
            _headers(config, registered, selected), inputs, governed, destination_scope, 'serialized_destination',
            max(1, len(batches)), max_input_bytes, ordered, warmup, initialization)
        _admit(task, batches, family)
        return WorkItem(task, batches, calculate)


def make_session_projection_work(config: ConfigSpec, *, grid: object, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`grid` is the library daily grid whose digest the configuration binds; `inputs` bind the role `daily`."""
    return _make(config, SESSION_FAMILY, grid=grid, features=features, inputs=inputs, batches=batches,
        instrument_id=instrument_id, job_id=job_id, generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes, calculate=compute_session_projection_inputs)


def make_bar_projection_work(config: ConfigSpec, *, grid: object, features: tuple[str, ...],
        inputs: tuple[InputManifest, ...], batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str,
        generation_id: str, partition_id: str, destination_scope: str,
        max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`grid` is the library bar grid whose digest the configuration binds; `inputs` bind the role `bars`."""
    return _make(config, BAR_FAMILY, grid=grid, features=features, inputs=inputs, batches=batches,
        instrument_id=instrument_id, job_id=job_id, generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes, calculate=compute_bar_projection_inputs)


def make_relative_excess_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`inputs` bind the delivered roles (`legs`, `membership`) to `batches` in the same order."""
    return _make(config, RELATIVE_FAMILY, grid=None, features=features, inputs=inputs, batches=batches,
        instrument_id=instrument_id, job_id=job_id, generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes, calculate=compute_relative_excess_inputs)


def make_rate_projection_work(config: ConfigSpec, *, features: tuple[str, ...], inputs: tuple[InputManifest, ...],
        batches: tuple[CanonicalBatch, ...], instrument_id: str, job_id: str, generation_id: str, partition_id: str,
        destination_scope: str, max_input_bytes: int = MAX_INPUT_BYTES) -> WorkItem:
    """`inputs` bind the delivered roles (`tape`, `legs`) to `batches` in the same order."""
    return _make(config, RATE_FAMILY, grid=None, features=features, inputs=inputs, batches=batches,
        instrument_id=instrument_id, job_id=job_id, generation_id=generation_id, partition_id=partition_id,
        destination_scope=destination_scope, max_input_bytes=max_input_bytes, calculate=compute_rate_projection_inputs)


__all__ = ['SESSION_FAMILY', 'BAR_FAMILY', 'RELATIVE_FAMILY', 'RATE_FAMILY', 'make_session_projection_work',
           'make_bar_projection_work', 'make_relative_excess_work', 'make_rate_projection_work',
           'compute_session_projection_inputs', 'compute_bar_projection_inputs', 'compute_relative_excess_inputs',
           'compute_rate_projection_inputs']
