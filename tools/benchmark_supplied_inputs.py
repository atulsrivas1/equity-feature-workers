"""EQ140 frozen supplied-input comparisons; explicit alternatives, no mode change.

Run only after source/tooling review and clean qualification. One Windows Job per
warmup or retained sample. No persistent pools or physical memory enforcement.
"""
from __future__ import annotations
import argparse
import ctypes as ct
from dataclasses import asdict
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import importlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import pickle
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
PROFILES = {'small': (256,), 'balanced': (1024,)*16, 'skewed': (256,)*15+(1024,)}
SERIES = ('compute', 'e2e')
WARMUPS = 2
REPEATS = 7
FEATURES = ('intraday.atr', 'intraday.band_lower', 'intraday.band_middle', 'intraday.band_position',
    'intraday.band_upper', 'intraday.band_width_ratio', 'intraday.ema', 'intraday.rsi')


def wire(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def save(path, value):
    temporary = path.with_suffix(path.suffix+'.partial')
    temporary.write_bytes(wire(value)); temporary.replace(path)


def original_oracle(prices, period=20):
    """Expanded geometric weights and centered moments, outside timing.

    No production indicator/preparation helper participates in this oracle.
    """
    assert len(prices) > period >= 2
    with localcontext() as context:
        context.prec = 350
        x = [Decimal(n) for n in prices]
        def weighted(values, ratio):
            seed = sum(values[:period])/period
            tail = values[period:]
            return seed*ratio**len(tail)+(1-ratio)*sum(
                value*ratio**(len(tail)-i-1) for i, value in enumerate(tail))
        ema = weighted(x, Decimal(period-1)/(period+1))
        changes = [b-a for a, b in zip(x, x[1:])]
        gain = weighted([max(d, Decimal(0)) for d in changes], Decimal(period-1)/period)
        loss = weighted([max(-d, Decimal(0)) for d in changes], Decimal(period-1)/period)
        rsi = Decimal(50) if gain == loss == 0 else 100*gain/(gain+loss)
        tr = [max(Decimal(4), abs(now+2-prior), abs(now-2-prior)) for prior, now in zip(x, x[1:])]
        atr = weighted(tr, Decimal(period-1)/period)
        window = x[-period:]; mean = sum(window)/period
        variance = sum((value-mean)**2 for value in window)/period
        radius = 2*variance.sqrt()
        assert radius > 0
        return {key: float(value) for key, value in {
            'intraday.ema': ema, 'intraday.rsi': rsi, 'intraday.atr': atr,
            'intraday.band_middle': mean, 'intraday.band_upper': mean+radius,
            'intraday.band_lower': mean-radius, 'intraday.band_width_ratio': 2*radius/mean,
            'intraday.band_position': Decimal('0.5')+(x[-1]-mean)/(2*radius)}.items()}


def prices_for(count):
    return tuple(100+(i % 11)*3+(i // 11) % 7 for i in range(count))


def installed_runtime():
    """Measurements require actual fresh installed distributions, not editable imports."""
    result = {}
    from build_supplied_inputs import PACKAGES
    root = Path(sys.prefix).resolve()
    for _, (_, _, module, version) in PACKAGES.items():
        dist = importlib.metadata.distribution(module.replace('_', '-'))
        assert dist.version == version
        location = Path(importlib.import_module(module).__file__).resolve()
        assert location.is_relative_to(root) and 'site-packages' in location.parts
        assert dist.files is not None
        files = {}
        for name in dist.files:
            relative = str(name).replace('\\', '/')
            if '__pycache__' in relative or relative.endswith(('.pyc', '.pyo')):
                continue
            declared_path = Path(dist.locate_file(name))
            assert not declared_path.is_symlink()
            path = declared_path.resolve()
            assert path.is_relative_to(root) and path.is_file()
            files[relative] = {'sha256': digest(path.read_bytes()), 'bytes': path.stat().st_size}
        site = Path(dist.locate_file('')).resolve()
        info = module+'-'+version+'.dist-info'
        for namespace in (module, info):
            actual = set()
            for entry in (site/namespace).rglob('*'):
                assert not entry.is_symlink()
                if entry.is_file() and '__pycache__' not in entry.parts:
                    actual.add(entry.relative_to(site).as_posix())
            assert actual == {name for name in files if name.startswith(namespace+'/')}, 'unexpected installed namespace member'
        direct = json.loads(dist.read_text('direct_url.json'))
        assert direct.get('dir_info', {}).get('editable') is not True
        result[module] = {'version': version, 'origin': location.relative_to(root).as_posix(), 'files': files}
    return result


def bound_expected(path, expected_sha256):
    body = path.read_bytes()
    assert digest(body) == expected_sha256, 'changed expected-file bytes'
    return json.loads(body)


def bound_runtime(expected):
    actual = installed_runtime()
    assert actual == expected, 'runner installed runtime differs from frozen admission'
    return digest(wire(actual))


def complete_sample(path, result, reference, expected_path, expected_sha256, runtime_before):
    """Persist phase evidence even when the after-phase binding rejects it."""
    save(path, result)
    checked = time.perf_counter_ns()
    try:
        assert bound_expected(expected_path, expected_sha256) == reference
        result['runtime_after_sha256'] = bound_runtime(reference['runtime'])
        assert result['runtime_after_sha256'] == runtime_before
        result['status'] = 'completed'
        result['post_phase_runtime_validation_ns'] = time.perf_counter_ns()-checked
        save(path, result)
    except BaseException as error:
        result.update(status='failed_after_phase', failure_type=type(error).__name__)
        save(path, result); raise


def items_for(profile):
    import equity_feature_contracts as c
    from equity_features_intraday import IntradayContext, IntradayRun, build_intraday_config
    from equity_feature_workers import InputManifest, TaskManifest, WorkItem
    from equity_feature_workers.intraday import compute_intraday_inputs, intraday_headers, intraday_history
    items = []; contexts = []
    for ordinal, count in enumerate(PROFILES[profile]):
        instrument = 'SYNTHETIC'+str(ordinal); close = prices_for(count)
        context = IntradayContext(instrument, 'original-calendar', '10ns-grid', 'revision1',
            (c.SessionSpec('supplied-benchmark', 'S', 0, count*10, 'supplied'),),
            (IntradayRun('S', 0, 10, count),), 0, count-1, 'reset')
        config = build_intraday_config(context, period=20, band_k=Fraction(2), evidence_limit=0,
            availability=c.AvailabilitySpec(count*10, count*10, count*10),
            adjustment=c.AdjustmentSpec(), price_unit=c.PriceUnit(0, 'USD'))
        batch = c.CanonicalBatch(c.DataKind.BAR, tuple(c.Column(name, values) for name, values in dict(
            instrument_id=(instrument,)*count, session_id=('S',)*count,
            start_ns=tuple(i*10 for i in range(count)), end_ns=tuple((i+1)*10 for i in range(count)),
            known_at_ns=tuple((i+1)*10 for i in range(count)), close=close,
            high=tuple(x+2 for x in close), low=tuple(x-2 for x in close)).items()),
            c.BatchMetadata('supplied-benchmark', c.SourceBinding('synthetic', 'fixture1', 'map1', 'bars-'+instrument),
                c.Coverage(count, count, True), c.PriceUnit(0, 'USD'), scope=c.InputScope(0, count*10, 'original-v1'),
                interval_coverage=tuple(c.IntervalCoverage('slot:'+str(i), i*10, (i+1)*10, c.Coverage(1, 1, True)) for i in range(count))))
        headers = intraday_headers(FEATURES)
        history = intraday_history(config, tuple(h.feature_id for h in headers))
        task = TaskManifest('supplied-benchmark', 'generation1', 'symbol'+str(ordinal), 'intraday', (instrument,), config,
            headers, (InputManifest('bars:000', digest(pickle.dumps(batch, protocol=5)), 'revision1', 'complete',
                c.InputBinding('bars:000', c.DataKind.BAR, batch.metadata)),),
            (c.IntervalSpec('S', 0, count*10),), 'original-destination', 'serialized_destination', 1, 1048576,
            history[0], history[1], history[2])
        items.append(WorkItem(task, (batch,), compute_intraday_inputs)); contexts.append(context)
    return tuple(items), tuple(contexts)


def publisher():
    from equity_feature_io_contracts import SinkRequirements
    from equity_feature_workers import PublicationLimits, SerialPublisher
    sys.path.insert(0, str(ROOT/'tests'))
    from supplied_memory import SerializedMemorySink
    sink = SerializedMemorySink()
    pub = SerialPublisher(sink, 'original-destination', limits=PublicationLimits(max_pending_tasks=16,
        max_pending_bytes=8388608, max_receipt_wire_bytes=400000), requirements=SinkRequirements(
        max_results=1, max_chunk_bytes=1048576, max_total_bytes=1048576, max_result_cells=100, max_evidence_rows=1))
    return sink, pub


def preflight(profile):
    import equity_feature_contracts as c
    from equity_features_intraday import compute_intraday
    from equity_feature_io_sdk import encode_result, encode_receipt
    from equity_feature_workers import encode_output
    items, contexts = items_for(profile); expected = {}; source_bytes = 0
    sink, pub = publisher()
    for item, context in zip(items, contexts):
        result = compute_intraday(item.batches, context, item.task.config,
            features=tuple(h.feature_id for h in item.task.features))
        oracle = original_oracle(prices_for(item.rows))
        assert len(result.values) == 8 and all(q.status == c.Status.AVAILABLE for q in result.quality)
        for column in result.values:
            assert math.isclose(column.values[0], oracle[column.feature_id], rel_tol=2e-15, abs_tol=1e-12)
        assert not result.evidence
        pub.submit(item.task, (result,)); progress = pub.drain()
        assert len(progress) == 1 and progress[0].committed and not pub.pending
        out = progress[0].output
        assert sink.read(out.receipt) == (result,)
        receipt_contribution = len(json.dumps(encode_receipt(out.receipt).decode(), ensure_ascii=True).encode())
        assert receipt_contribution <= 400000 and len(encode_output(out)) <= 1048576
        encoded = encode_result(result); source_bytes += len(pickle.dumps(item.batches, protocol=5))
        expected[item.task.task_sha256] = {'result_sha256': digest(encoded), 'result_bytes': len(encoded),
            'output_sha256': digest(encode_output(out)), 'output_bytes': len(encode_output(out)),
            'receipt_wire_bytes': receipt_contribution, 'scalar_oracle': oracle}
    assert source_bytes <= 16777216
    return {'tasks': expected, 'source_pickle_bytes': source_bytes, 'rows': sum(i.rows for i in items)}


def runner(args):
    started = time.perf_counter_ns()
    sys.path.insert(0, str(ROOT/'tools'))
    from supplied_native import counters
    from equity_feature_io_contracts import PublicationState
    from equity_feature_io_sdk import encode_result
    from equity_feature_workers import BoundedSupervisor, ResourceBudget, ProgressRecorder, encode_output
    imports_done = time.perf_counter_ns()
    try:
        reference = bound_expected(args.expected, args.expected_sha256)
        runtime_before = bound_runtime(reference['runtime'])
        expected = reference['profiles'][args.profile]
    except BaseException as error:
        save(args.output, {'status': 'failed_before_phase', 'failure_type': type(error).__name__})
        raise
    runtime_admitted = time.perf_counter_ns()
    items, _ = items_for(args.profile)
    admitted = time.perf_counter_ns()
    budget = ResourceBudget(max_tasks=16, workers=args.degree, max_cpu_slots=args.cpu_slots,
        max_in_flight=args.degree, max_input_bytes=16777216, max_resident_bytes=args.memory_cap,
        max_task_result_bytes=262144, max_task_transport_bytes=1048576, max_total_result_bytes=16777216)
    sink, pub = publisher() if args.series == 'e2e' else (None, None)
    progress = ProgressRecorder(max_tasks=16)
    before = counters(args.job); begin = time.perf_counter_ns()
    outcome = BoundedSupervisor(mode=args.mode, budget=budget).run(items, publisher=pub, progress=progress)
    stable = []; stage_observations = []
    assert len(outcome.tasks) == len(expected['tasks']) and not outcome.cancelled
    for task in outcome.tasks:
        want = expected['tasks'][task.task.task_sha256]
        assert task.reason is None and task.results is not None and len(task.results) == 1 and task.spill is None
        encoded = encode_result(task.results[0]); assert digest(encoded) == want['result_sha256']
        if pub is not None:
            assert task.output is not None and not pub.pending
            assert digest(encode_output(task.output)) == want['output_sha256']
            receipt = task.output.receipt
            assert sink.lookup(receipt.idempotency_key).state == PublicationState.COMMITTED
            assert tuple(encode_result(r) for r in sink.read(receipt)) == (encoded,)
        else: assert task.output is None
        stable.append({'task_sha256': task.task.task_sha256, 'result_sha256': digest(encoded),
            'result_bytes': len(encoded), 'reason': task.reason, 'output_sha256':
            digest(encode_output(task.output)) if task.output is not None else None})
        stage_observations.append({'task_sha256': task.task.task_sha256, 'timings': [asdict(t) for t in task.timings]})
    diagnostics = progress.snapshot()
    projection = [{'task_sha256': d.task_sha256, 'intent_sha256': d.intent_sha256, 'family': d.family,
        'family_sha256': d.family_sha256, 'partition_sha256': d.partition_sha256,
        'status': d.status, 'reason': d.reason} for d in diagnostics.tasks]
    finish = time.perf_counter_ns(); after = counters(args.job)
    delta = {key: after[key]-before[key] for key in ('user', 'kernel')}
    assert all(value >= 0 for value in delta.values())
    result = {'schema': 'supplied-sample:1', 'status': 'phase_complete_pending_runtime',
        'runtime_before_sha256': runtime_before, 'expected_sha256': args.expected_sha256,
        'mode': args.mode, 'degree': args.degree,
        'profile': args.profile, 'series': args.series, 'phase_wall_ns': finish-begin,
        'phase_before': before, 'phase_after': after, 'phase_cpu_seconds': sum(delta.values())/10000000,
        'runner_import_ns': imports_done-started, 'reference_runtime_admission_ns': runtime_admitted-imports_done,
        'source_admission_ns': admitted-runtime_admitted,
        'stable': stable, 'diagnostic_projection': projection, 'diagnostics': json.loads(diagnostics.encode()),
        'stage_observations': stage_observations, 'row_ledger': outcome.row_ledger,
        'distinct_reuse_rows': outcome.distinct_reuse_rows, 'admitted_input_bytes': outcome.admitted_input_bytes,
        'reserved_resident_bytes': outcome.reserved_resident_bytes, 'result_bytes': outcome.result_bytes,
        'copy_limits': 'Pickled input/result sizes and stage observations; no claim to count every physical copy or exclusive stage.'}
    # Persist the completed phase before checking the post-phase environment:
    # rejection must retain actual phase results rather than erase them.
    complete_sample(args.output, result, reference, args.expected, args.expected_sha256, runtime_before)


def distribution(samples):
    assert len(samples) == REPEATS
    walls = [s['wall_seconds'] for s in samples]; cpu = [s['cpu_seconds'] for s in samples]
    rss = [s['native']['aggregate_sampled_peak_rss_bytes'] for s in samples]
    q = statistics.quantiles(walls, n=4, method='inclusive')
    median = statistics.median(walls)
    return {'median_wall_seconds': median, 'iqr_seconds': q[2]-q[0],
        'p95_wall_seconds': sorted(walls)[math.ceil(.95*len(walls))-1],
        'median_cpu_seconds': statistics.median(cpu), 'max_sampled_rss_bytes': max(rss),
        'noise_improvement_floor': max(.05, 3*(q[2]-q[0])/median)}


def decision(a, b, cap, small_a, small_b, resource_valid):
    improvement = 1-b['median_wall_seconds']/a['median_wall_seconds']
    checks = {'wall': b['median_wall_seconds'] < a['median_wall_seconds']*(1-a['noise_improvement_floor']),
        'cpu': a['median_cpu_seconds'] > 0 and b['median_cpu_seconds'] <= 1.25*a['median_cpu_seconds'],
        'memory': b['max_sampled_rss_bytes'] <= min(cap, 1.25*a['max_sampled_rss_bytes']),
        'small_p95': small_b['p95_wall_seconds'] <= 1.10*small_a['p95_wall_seconds'],
        'complete_resource_evidence': resource_valid}
    return {'adopted': all(checks.values()), 'checks': checks, 'median_wall_improvement': improvement,
        'fallback': 'sequential', 'default_mode_changed': False}


def freeze_baseline(record, directory, head):
    assert not record['candidates'], 'candidate work already present'
    assert set(record['baseline']) == {series+'-'+profile for series in SERIES for profile in PROFILES}
    for cohort in record['baseline'].values():
        assert len(cohort['warmups']) == WARMUPS and len(cohort['samples']) == REPEATS
        assert cohort['distribution'] == distribution(cohort['samples'])
    path = directory/'baseline.json'
    assert not path.exists() and not (directory/'baseline-sha256.json').exists()
    save(path, {'schema': 'supplied-baseline:1', 'source_commit': head, 'baseline': record['baseline'],
        'expected_sha256': record['expected_sha256'], 'machine': record['machine']})
    body = path.read_bytes()
    save(directory/'baseline-sha256.json', {'sha256': digest(body), 'frozen_before_any_candidate': True})
    return body


def machine():
    assert os.name == 'nt', 'Windows accounting only; Linux correctness is a separate gate'
    from ctypes import wintypes as w
    kernel = ct.WinDLL('kernel32', use_last_error=True)
    kernel.GetCurrentProcess.restype = w.HANDLE
    kernel.GetProcessAffinityMask.argtypes = [w.HANDLE, ct.POINTER(ct.c_size_t), ct.POINTER(ct.c_size_t)]
    kernel.GetProcessAffinityMask.restype = w.BOOL
    process = ct.c_size_t(); system = ct.c_size_t()
    assert kernel.GetProcessAffinityMask(kernel.GetCurrentProcess(), ct.byref(process), ct.byref(system))
    class Memory(ct.Structure):
        _fields_ = [('length', w.DWORD), ('load', w.DWORD)]+[(n, ct.c_ulonglong) for n in
            ('total_physical', 'available_physical', 'total_pagefile', 'available_pagefile', 'total_virtual', 'available_virtual', 'available_extended')]
    kernel.GlobalMemoryStatusEx.argtypes = [ct.POINTER(Memory)]; kernel.GlobalMemoryStatusEx.restype = w.BOOL
    memory = Memory(); memory.length = ct.sizeof(memory)
    assert kernel.GlobalMemoryStatusEx(ct.byref(memory))
    slots = process.value.bit_count(); assert slots >= 2
    return {'allowed_affinity_mask': hex(process.value), 'allowed_cpu_slots': slots,
        'initial_free_physical_bytes': int(memory.available_physical),
        'memory_cap_bytes': min(1073741824, int(memory.available_physical)//4),
        'degrees': [n for n in (1, 2, 4, 8) if n <= slots-1],
        'omitted_degrees': [n for n in (1, 2, 4, 8) if n > slots-1], 'backend_slots': 1}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--runner', action='store_true')
    parser.add_argument('--expected', type=Path)
    parser.add_argument('--expected-sha256')
    parser.add_argument('--profile', choices=tuple(PROFILES))
    parser.add_argument('--series', choices=SERIES)
    parser.add_argument('--mode', choices=('sequential', 'thread', 'process'))
    parser.add_argument('--degree', type=int)
    parser.add_argument('--cpu-slots', type=int)
    parser.add_argument('--memory-cap', type=int)
    parser.add_argument('--job')
    args = parser.parse_args()
    if args.runner: runner(args); return
    sys.path.insert(0, str(ROOT/'tools'))
    from supplied_native import NativeFailure, run_owned
    from build_supplied_inputs import freeze
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    source_bytes, source, closure = freeze(ROOT, head, ['packages/workers', 'tests', 'tools', 'examples', 'docs', '.github/workflows/supplied-inputs.yml'])
    args.output.mkdir(parents=True, exist_ok=False)
    qualification = {'schema': 'supplied-benchmark-admission:1', 'status': 'running', 'source_commit': head}
    save(args.output/'admission.json', qualification)
    try:
        machine_info = machine(); runtime = installed_runtime()
        worker = importlib.import_module('equity_feature_workers')
        installed = Path(worker.__file__).parent
        prefix = 'packages/workers/src/equity_feature_workers/'
        assert {n.removeprefix(prefix): b for n, b in source_bytes.items() if n.startswith(prefix)} == {
            p.relative_to(installed).as_posix(): p.read_bytes() for p in installed.rglob('*')
            if p.is_file() and '__pycache__' not in p.parts}, 'installed worker differs from measured source'
        expected_path = args.output/'expected.json'
        save(expected_path, {'runtime': runtime, 'profiles': {profile: preflight(profile) for profile in PROFILES}})
        controls = subprocess.run([sys.executable, '-I', '-B', '-m', 'unittest', 'discover', '-s', str(ROOT/'tests'), '-p', 'test_supplied_native.py'], cwd=ROOT)
        assert controls.returncode == 0
        closure(); qualification.update(status='completed', runtime=runtime, machine=machine_info,
            expected_sha256=digest(expected_path.read_bytes()), native_controls_passed=True)
        save(args.output/'admission.json', qualification)
    except BaseException as error:
        qualification.update(status='failed', failure_type=type(error).__name__)
        save(args.output/'admission.json', qualification); raise
    record = {'schema': 'supplied-benchmark:1', 'status': 'running', 'source': source, 'machine': machine_info,
        'runtime': runtime, 'expected_sha256': digest(expected_path.read_bytes()), 'warmups_per_profile': WARMUPS,
        'retained_per_profile': REPEATS, 'controls_passed': True, 'baseline': {}, 'candidates': {},
        'limits': 'Synthetic cold fresh-runner/pool snapshots, in-memory owner-guarded publication/readback. CPU includes Job descendants; sampled RSS includes whole Job conservatively in both series. No live/fastest/persistent/physical-memory claim.'}
    raw = args.output/'samples.json'; save(raw, record)
    def sample(series, profile, mode, degree, index, warmup):
        closure()
        assert digest(expected_path.read_bytes()) == record['expected_sha256']
        assert installed_runtime() == runtime
        label = f'{series}-{profile}-{mode}-{degree}-'+('warmup' if warmup else 'retained')+str(index)
        destination = args.output/(label+'.json')
        command = [sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--runner', '--output', str(destination),
            '--expected', str(expected_path), '--expected-sha256', record['expected_sha256'],
            '--profile', profile, '--series', series, '--mode', mode, '--degree', str(degree),
            '--cpu-slots', str(machine_info['allowed_cpu_slots']), '--memory-cap', str(machine_info['memory_cap_bytes']), '--job', '{job_name}']
        begin = time.perf_counter_ns()
        try:
            native = run_owned(command, cwd=ROOT, timeout_s=300)
            assert native['exit_code'] == 0
            result = json.loads(destination.read_bytes())
            wall = time.perf_counter_ns()-begin
            assert native['final_accounting']['active_processes'] == 0
            assert result['status'] == 'completed' and result['expected_sha256'] == record['expected_sha256']
            assert result['runtime_before_sha256'] == result['runtime_after_sha256'] == digest(wire(runtime))
            assert digest(expected_path.read_bytes()) == record['expected_sha256']
            result.update(native=native, wall_seconds=(wall if series == 'e2e' else result['phase_wall_ns'])/1000000000,
                cpu_seconds=native['cpu_seconds'] if series == 'e2e' else result['phase_cpu_seconds'],
                warmup=warmup, sample=index)
            save(destination, result)
            return result
        except BaseException as error:
            save(args.output/(label+'-FAILED.json'), {'status': 'failed', 'type': type(error).__name__,
                'native': error.evidence if type(error) is NativeFailure else locals().get('native'), 'sample_output_present': destination.exists()})
            raise
    def cohort(series, profile, mode, degree):
        warmups = [sample(series, profile, mode, degree, i, True) for i in range(WARMUPS)]
        samples = [sample(series, profile, mode, degree, i, False) for i in range(REPEATS)]
        projection = lambda s: [s['stable'], s['diagnostic_projection'], s['row_ledger'], s['distinct_reuse_rows'], s['admitted_input_bytes'], s['result_bytes']]
        target = projection(samples[0])
        assert all(projection(s) == target for s in warmups+samples)
        valid = all(not s['native']['probe_errors'] and
            0 < s['native']['aggregate_sampled_peak_rss_bytes'] <= machine_info['memory_cap_bytes'] for s in samples)
        return {'warmups': warmups, 'samples': samples, 'projection': target, 'distribution': distribution(samples),
            'resource_valid': valid, 'resource_status': 'qualified_sampled_evidence' if valid else 'invalid_for_adoption'}
    try:
        for series in SERIES:
            for profile in PROFILES:
                key = series+'-'+profile
                record['baseline'][key] = cohort(series, profile, 'sequential', 1); save(raw, record)
        baseline = args.output/'baseline.json'
        frozen = freeze_baseline(record, args.output, head); frozen_sha = digest(frozen)
        record['baseline_sha256'] = frozen_sha; save(raw, record)
        for series in SERIES:
            for mode in ('thread', 'process'):
                for degree in machine_info['degrees']:
                    alternatives = {p: cohort(series, p, mode, degree) for p in PROFILES}
                    for profile, b in alternatives.items():
                        a = record['baseline'][series+'-'+profile]
                        assert b['projection'] == a['projection']
                        b['decision'] = decision(a['distribution'], b['distribution'], machine_info['memory_cap_bytes'],
                            record['baseline'][series+'-small']['distribution'], alternatives['small']['distribution'],
                            a['resource_valid'] and b['resource_valid'])
                        record['candidates'][f'{series}-{profile}-{mode}-{degree}'] = b
                    assert baseline.read_bytes() == frozen
                    closure(); save(raw, record)
        closure(); assert installed_runtime() == runtime
        record['status'] = 'completed'; save(raw, record)
    except BaseException as error:
        record.update(status='failed', failure_type=type(error).__name__); save(raw, record); raise


if __name__ == '__main__':
    main()
