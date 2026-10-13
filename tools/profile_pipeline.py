"""EQ085 trusted synthetic attribution; no calculation/public runtime changes."""
from __future__ import annotations

import argparse
import cProfile
import hashlib
import importlib.metadata as metadata
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import pstats
import re
import statistics
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
MAX_FUNCTIONS = 20000
MAX_REPORT_BYTES = 8388608
sys.path.insert(0, str(ROOT / 'tests'))  # fixture helpers only, never package source


def source_name(filename: str) -> str:
    """Keep namespace identity without publishing private absolute origins."""
    value = filename.replace('\\', '/')
    if filename == '~':
        return 'native'
    for marker, prefix in (('/site-packages/', 'installed/'), ('/Lib/', 'stdlib/'),
                           ('/lib/python3.12/', 'stdlib/')):
        if marker in value:
            return prefix + value.split(marker, 1)[1]
    for directory in ('tests', 'tools', 'benchmarks'):
        if '/'+directory+'/' in value:
            return 'harness/'+directory+'/'+value.rsplit('/'+directory+'/', 1)[1]
    if value.startswith('<') and value.endswith('>'):
        return 'generated/'+value[1:-1]
    name = value.rsplit('/', 1)[-1]
    return 'external/'+name+'#'+hashlib.sha256(value.encode()).hexdigest()[:16]


def category(source: str, function: str) -> str:
    if source == 'native':
        return 'opaque_native'
    if 'equity_features/' in source:
        return 'calculation'
    if 'equity_feature_contracts/columnar' in source:
        return 'conversion'
    if 'equity_feature_contracts/' in source:
        return 'contract_validation'
    if 'equity_feature_io_sdk/' in source:
        return 'codec_and_io_validation'
    if 'equity_feature_duckdb/' in source:
        return 'adapter'
    if any(n in source for n in ('equity_feature_parquet/', 'equity_feature_duckdb_sink/')):
        return 'sink'
    if 'equity_feature_workers/' in source:
        return 'worker_orchestration'
    if source.startswith('harness/'):
        return 'qualification_harness'
    return 'unclassified'


def export_profile(stats: dict) -> dict:
    if len(stats) > MAX_FUNCTIONS:
        raise ValueError('profile function limit exceeded')
    rows = []
    for (filename, line, function), (primitive, total, own, cumulative, _callers) in stats.items():
        if type(primitive) is not int or type(total) is not int or not 0 <= primitive <= total:
            raise ValueError('invalid profile call count')
        if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in (own, cumulative)):
            raise ValueError('invalid profile duration')
        source = source_name(filename)
        rows.append(dict(source=source, line=line, function=function, primitive_calls=primitive,
                         total_calls=total, self_seconds=own, cumulative_seconds=cumulative,
                         category=category(source, function)))
    rows.sort(key=lambda r: (-r['self_seconds'], r['source'], r['line'], r['function']))
    groups = {}
    for row in rows:
        groups[row['category']] = groups.get(row['category'], 0.0) + row['self_seconds']
    return dict(functions=rows, category_self_seconds=groups, cumulative_is_nonadditive=True,
                native_attribution='opaque calls; no native stack/CPU/allocation decomposition',
                scope='sequential coordinator; profiled timing is diagnostic, not baseline')


def profile_call(call):
    profiler = cProfile.Profile(timer=time.perf_counter_ns, timeunit=1e-9)
    start = time.perf_counter_ns()
    result = profiler.runcall(call)
    elapsed = time.perf_counter_ns()-start
    if elapsed <= 0:
        raise ValueError('invalid diagnostic elapsed')
    return result, dict(elapsed_ns=elapsed, **export_profile(pstats.Stats(profiler).stats))


def installed_identity() -> dict:
    names = ('equity-feature-contracts', 'equity-features', 'equity-feature-workers',
             'equity-feature-io-contracts', 'equity-feature-io-sdk', 'equity-feature-duckdb',
             'equity-feature-parquet', 'equity-feature-duckdb-sink')
    result = {}
    for name in names:
        dist = metadata.distribution(name)
        files = {}
        for entry in dist.files or ():
            if str(entry).endswith('.py'):
                origin = Path(dist.locate_file(entry)).resolve()
                if 'site-packages' not in origin.parts:
                    raise ValueError('installed package origin required')
                files[str(entry).replace('\\', '/')] = hashlib.sha256(origin.read_bytes()).hexdigest()
        if not files:
            raise ValueError('installed Python source identity unavailable')
        result[name] = dict(version=dist.version, python_files_sha256=files)
    return result


def validate_sample(sample, reference=None):
    if ('skipped' in sample or sample.get('full_result_quality_evidence_input_parity') is not True
            or sample.get('independent_five_trade_goldens') is not True):
        raise ValueError('pipeline parity/golden unavailable')
    if type(sample.get('pipeline_elapsed_ns')) is not int or sample['pipeline_elapsed_ns'] <= 0:
        raise ValueError('invalid baseline elapsed')
    if not isinstance(sample.get('content_sha256'), str) or not re.fullmatch('[0-9a-f]{64}', sample['content_sha256']):
        raise ValueError('invalid content identity')
    if reference is not None:
        for field in ('content_sha256', 'input_population', 'rows', 'tasks', 'versions'):
            if sample.get(field) != reference.get(field):
                raise ValueError('pipeline identity/parity drift: '+field)
    return sample


def child(settings):
    if sys.flags.optimize:
        raise ValueError('golden assertions require unoptimized Python')
    identity = installed_identity()
    import pyarrow as pa
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    if settings['kind'] == 'pipeline':
        from pilot_fixture import run_pipeline
        call = lambda: run_pipeline(Path(settings['fixture']), Path(settings['output']),
                                    settings['workload'], settings['sink'], 'sequential', 1, True)
        if settings['profile']:
            sample, diagnostic = profile_call(call)
        else:
            sample, diagnostic = call(), None
        validate_sample(sample)
        return dict(sample=sample, profile=diagnostic, installed=identity)
    core = Path(settings['core_benchmark'])
    spec = importlib.util.spec_from_file_location('eq085_core_fixture', core)
    if spec is None or spec.loader is None:
        raise ValueError('core fixture unavailable')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # fixed trusted committed qualification helper
    args = SimpleNamespace(rows=settings['rows'], seed=41001, skew=settings['skew'], k=8,
                           chunks=[128, 1024], repetitions=3)
    baseline = module.child(args)
    batch, config, entity, prices, sizes, _ = module.trade_fixture(args.rows, args.seed, args.skew)
    from equity_features.session import compute_trades
    from equity_feature_contracts.columnar import to_arrow, from_arrow, to_numpy, from_numpy
    from equity_feature_io_sdk import encode_result, decode_result
    pure = lambda: compute_trades(batch, config, entity=entity)
    expected = pure()
    module.verify_trades(expected, prices, sizes)
    arrow, numpy = to_arrow(batch), to_numpy(batch)
    wire = encode_result(expected)
    calls = {'calculation': pure, 'to_arrow': lambda: to_arrow(batch),
             'from_arrow': lambda: from_arrow(arrow), 'to_numpy': lambda: to_numpy(batch),
             'from_numpy': lambda: from_numpy(batch.kind, numpy, batch.metadata),
             'encode_result': lambda: encode_result(expected), 'decode_result': lambda: decode_result(wire),
             'history_checked_harness': lambda: module.history_case(512, args.seed, 3)}
    profiles = {}
    codec_timings = {}
    for name, call in calls.items():
        if name in ('encode_result', 'decode_result'):
            codec_timings[name] = module.measured(call, args.rows, 3)[0]
        result, profiles[name] = profile_call(call)
        if name == 'calculation' and result != expected:
            raise ValueError('profile changed complete result')
        if name == 'encode_result' and result != wire:
            raise ValueError('profile changed result bytes')
        if name == 'decode_result' and result != expected:
            raise ValueError('profile changed decoded result')
        if name in ('from_arrow', 'from_numpy') and result != batch:
            raise ValueError('profile changed conversion')
        if name == 'to_arrow' and from_arrow(result) != batch:
            raise ValueError('profile changed Arrow conversion')
        if name == 'to_numpy' and from_numpy(batch.kind, result, batch.metadata) != batch:
            raise ValueError('profile changed NumPy conversion')
    return dict(baseline=baseline, profiles=profiles, codec_baseline=codec_timings,
                codec_bytes=len(wire), full_result_sha256=hashlib.sha256(wire).hexdigest(), installed=identity)


def run_child(settings):
    from pilot_process import communicate_owned
    env = dict(os.environ)
    for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        env[name] = '1'
    stdout, stderr, _memory, code = communicate_owned(
        [sys.executable, '-I', str(Path(__file__).resolve()), '--child'],
        json.dumps(settings), timeout=300, env=env,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    if code:
        raise RuntimeError('owned synthetic profile failed; diagnostic stays in local stderr: '+stderr)
    if len(stdout.encode()) > MAX_REPORT_BYTES:
        raise ValueError('child report limit exceeded')
    return json.loads(stdout)


def check_locations(work: Path, out: Path):
    work, out = work.resolve(), out.resolve()
    if work.exists() or out.exists() or work == out or work in out.parents or out in work.parents:
        raise ValueError('distinct new work/report locations required')
    for protected in (ROOT,):
        if any(p == protected or protected in p.parents or p in protected.parents for p in (work, out)):
            raise ValueError('evidence overlaps source repository')
    for path in (work, out):
        if any((parent / '.git').exists() for parent in path.parents):
            raise ValueError('evidence lies in a Git checkout')
    return work, out


def write_report(out: Path, report):
    raw = (json.dumps(report, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()
    if len(raw) > MAX_REPORT_BYTES:
        raise ValueError('complete report limit exceeded')
    out.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(prefix='.eq085-', dir=out.parent)
    temporary = Path(name)
    try:
        with os.fdopen(handle, 'wb') as stream:
            stream.write(raw)
        os.link(temporary, out)  # exclusive publication; never overwrite a prior report
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--child', action='store_true')
    parser.add_argument('--work-root', type=Path)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--core-benchmark', type=Path)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(sys.stdin.read())), allow_nan=False))
        return
    if args.work_root is None or args.out is None or args.core_benchmark is None:
        parser.error('explicit work root, report and committed core benchmark required')
    work, out = check_locations(args.work_root, args.out)
    core = args.core_benchmark.resolve()
    core_root = core.parent.parent
    core_head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=core_root, text=True).strip()
    core_git = subprocess.check_output(['git', 'show', core_head+':benchmarks/run_baseline.py'], cwd=core_root)
    if core.read_bytes().replace(b'\r\n', b'\n') != core_git:
        raise ValueError('core benchmark differs from committed source')
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    scoped = ('tools/profile_pipeline.py', 'tests/pilot_fixture.py', 'tests/pilot_oracle.json',
              'tests/pilot_process.py', 'tests/pilot_memory.py')
    hashes = {}
    for path in scoped:
        raw = subprocess.check_output(['git', 'show', head+':'+path], cwd=ROOT)
        if (ROOT/path).read_bytes().replace(b'\r\n', b'\n') != raw:
            raise ValueError('profile inputs differ from committed source')
        hashes[path] = hashlib.sha256(raw).hexdigest()
    work.mkdir(parents=True)
    # Fixture generation is outside all profiled/baseline calls.
    from pilot_fixture import make_fixture
    records, references = [], {}
    workloads = ('small',) if args.smoke else ('small', 'large', 'skew')
    for workload in workloads:
        fixture = work / ('fixture-'+workload)
        make_fixture(fixture, workload)
        for sink in ('parquet', 'duckdb'):
            settings = dict(kind='pipeline', workload=workload, sink=sink, fixture=str(fixture), profile=False)
            settings['output'] = str(work/(workload+'-'+sink+'-reference'))
            reference = run_child(settings)
            validate_sample(reference['sample'])
            if workload in references and reference['sample']['content_sha256'] != references[workload]:
                raise ValueError('sink full result parity mismatch')
            references[workload] = reference['sample']['content_sha256']
            samples = []
            for repeat in range(3):
                settings['output'] = str(work/(workload+'-'+sink+'-'+str(repeat)))
                value = run_child(settings)
                validate_sample(value['sample'], reference['sample'])
                if value['installed'] != reference['installed']:
                    raise ValueError('installed source identity drift')
                samples.append(value['sample'])
            settings.update(profile=True, output=str(work/(workload+'-'+sink+'-profile')))
            diagnostic = run_child(settings)
            validate_sample(diagnostic['sample'], reference['sample'])
            if diagnostic['installed'] != reference['installed']:
                raise ValueError('profile installed source identity drift')
            record = dict(workload=workload, sink=sink, reference=reference, baseline_samples=samples,
                          baseline_median_ns=statistics.median(s['pipeline_elapsed_ns'] for s in samples),
                          diagnostic=diagnostic)
            records.append(record)
            print(f'{workload} {sink}: three baselines and separate profile verified', flush=True)
    core_records = []
    cases = [(128, False)] if args.smoke else [(128, False), (16384, False), (16384, True)]
    for rows, skew in cases:
        core_records.append(run_child(dict(kind='core', core_benchmark=str(core), rows=rows, skew=skew)))
    report = dict(schema='eq085-profile1', source_head=head, source_sha256=hashes,
                  core_benchmark_head=core_head, core_benchmark_sha256=hashlib.sha256(core_git).hexdigest(),
                  system=platform.system(), machine=platform.machine(), python=platform.python_version(),
                  smoke=args.smoke, pipeline=records, core=core_records,
                  protocol='3 unprofiled calls after reference, separate cProfile diagnostic; sequential1 only',
                  limits='synthetic warmed source, uncontrolled cache/background; native opaque; diagnostic cumulative nonadditive; no optimization/capacity/physical cleanup claim')
    write_report(out, report)
    print('Complete parity-checked profile report published')


if __name__ == '__main__':
    main()
