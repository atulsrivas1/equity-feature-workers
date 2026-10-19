"""Original supplied facts through an optional pure intraday worker; no acquisition."""
from fractions import Fraction
import hashlib
import pickle

import equity_feature_contracts as c
from equity_features_intraday import IntradayContext, IntradayRun, build_intraday_config, describe_intraday
from equity_feature_workers import BoundedSupervisor, InputManifest, ResourceBudget, TaskManifest, WorkItem
from equity_feature_workers.intraday import compute_intraday_inputs, intraday_headers, intraday_history


def calculate(mode: str = 'sequential', workers: int = 1) -> c.FeatureResult:
    context = IntradayContext('SYNTHETIC', 'caller-calendar', '10ns', 'revision1',
        (c.SessionSpec('example', 'S', 0, 60, 'supplied'),), (IntradayRun('S', 0, 10, 6),), 0, 5, 'reset')
    config = build_intraday_config(context, period=3, band_k=Fraction(2),
        availability=c.AvailabilitySpec(60, 60, 60), adjustment=c.AdjustmentSpec(), price_unit=c.PriceUnit(0, 'USD'))
    data: dict[str, tuple[str | int | bool | None, ...]] = dict(instrument_id=('SYNTHETIC',) * 6, session_id=('S',) * 6,
        start_ns=(0, 10, 20, 30, 40, 50), end_ns=(10, 20, 30, 40, 50, 60), known_at_ns=(10, 20, 30, 40, 50, 60),
        close=(10, 12, 11, 15, 14, 16), high=(12, 14, 13, 17, 16, 18), low=(8, 10, 9, 13, 12, 14))
    batch = c.CanonicalBatch(c.DataKind.BAR, tuple(c.Column(name, values) for name, values in data.items()),
        c.BatchMetadata('example', c.SourceBinding('synthetic', 'snapshot1', 'map1', 'bars1'), c.Coverage(6, 6, True), c.PriceUnit(0, 'USD'),
            scope=c.InputScope(0, 60, 'original-v1'), interval_coverage=tuple(
                c.IntervalCoverage(f'slot:{n}', n * 10, (n + 1) * 10, c.Coverage(1, 1, True)) for n in range(6))))
    features = tuple(entry.feature_id for entry in describe_intraday())
    history = intraday_history(config, features)
    identity = hashlib.sha256(pickle.dumps(batch, protocol=5)).hexdigest()
    task = TaskManifest('example-job', 'generation1', 'partition1', 'intraday', ('SYNTHETIC',), config,
        intraday_headers(features), (InputManifest('bars:000', identity, 'revision1', 'complete', c.InputBinding('bars:000', c.DataKind.BAR, batch.metadata)),),
        (c.IntervalSpec('S', 0, 60),), 'caller-destination', 'serialized_destination', 1, 1048576,
        history[0], history[1], history[2])
    outcome = BoundedSupervisor(mode=mode, budget=ResourceBudget(workers=workers, max_cpu_slots=workers + 1,
        max_in_flight=workers)).run((WorkItem(task, (batch,), compute_intraday_inputs),))
    execution = outcome.tasks[0]
    if execution.reason is not None or execution.results is None:
        raise RuntimeError('Supplied example did not calculate: ' + str(execution.reason))
    return execution.results[0]


if __name__ == '__main__':
    result = calculate()
    assert next(col.values[0] for col in result.values if col.feature_id == 'intraday.ema') == 14.75
    print('Original supplied worker example: eight outputs, EMA14.75, source identity preserved.')
