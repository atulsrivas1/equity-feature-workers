"""EQ088 frozen synthetic pipeline and physical layout qualification, no production edits."""
from __future__ import annotations

import argparse
import hashlib
from importlib import metadata, import_module
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from pilot_fixture import ORACLE, make_fixture, isolated_sample, population
from pilot_memory import hardware, admission
from pilot_process import communicate_owned

MODES = [('sequential', 1), ('thread', 2), ('thread', 4)]


def groups(records):
    result = []
    for workload in sorted({r['workload'] for r in records}):
        for backend in ('parquet', 'duckdb'):
            for mode, workers in MODES:
                sample = [r for r in records if (r['workload'], r['backend'], r['mode'], r['workers']) == (workload, backend, mode, workers) and 'skipped' not in r]
                if sample:
                    times = [r['pipeline_elapsed_ns'] for r in sample]
                    result.append(dict(workload=workload, backend=backend, mode=mode, workers=workers,
                        samples=len(times), median_ns=statistics.median(times), min_ns=min(times), max_ns=max(times)))
    return result


def scheduling_decision(values):
    lookup = {(r['workload'], r['backend'], r['mode'], r['workers']): r for r in values}
    if len(lookup) != 24 or any(r['samples'] != 3 for r in values):
        return dict(adopt_workers=None, reason='incomplete supported comparison; retain sequential1')
    qualified = []
    for workers in (2, 4):
        good = True
        for workload in ('small', 'large', 'skew', 'capacity'):
            for backend in ('parquet', 'duckdb'):
                baseline = lookup[workload, backend, 'sequential', 1]
                candidate = lookup[workload, backend, 'thread', workers]
                if workload == 'small':
                    good &= candidate['median_ns'] <= baseline['median_ns'] * 1.1
                else:
                    good &= candidate['median_ns'] <= baseline['median_ns'] * .9
                    good &= candidate['max_ns'] < baseline['min_ns']
        if good:
            qualified.append(workers)
    return dict(adopt_workers=min(qualified) if qualified else None,
        reason='qualified proposal requires explicit reviewed adoption' if qualified else 'negative or overlapping observations; retain sequential1',
        defaults_changed=False)


def native_probe(settings):
    environment = dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1')
    out, err, memory, code = communicate_owned([sys.executable, '-I', str(ROOT / 'tools/scaling_probe.py')],
        json.dumps(settings), timeout=120, env=environment,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    if code:
        private = Path(settings['root']).parent / ('failed-' + Path(settings['root']).name + '.log')
        private.write_text(err, encoding='utf-8')
        raise RuntimeError('scaling diagnostic failed; private stderr retained outside public report')
    result = json.loads(out)
    result['memory_observation'] = memory
    return result


def installed_record(dist):
    record = dist.read_text('RECORD')
    if not record or not record.strip():
        raise ValueError('installed distribution RECORD evidence unavailable')
    return hashlib.sha256(record.encode()).hexdigest()


def frozen_bytes(head, names):
    hashes = {}
    for name in names:
        expected = subprocess.check_output(['git', 'show', head + ':' + name], cwd=ROOT)
        actual = (ROOT / name).read_bytes()
        assert actual == expected, 'executed worktree bytes differ from committed source'
        hashes[name] = hashlib.sha256(actual).hexdigest()
    return hashes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work-root', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    work, out = args.work_root.resolve(), args.out.resolve()
    assert not work.exists() and not out.exists()
    assert not any(p == ROOT or ROOT in p.parents or p in ROOT.parents for p in (work, out))
    assert work != out and work not in out.parents and out not in work.parents
    scoped = ('packages', 'tools', 'tests', 'requirements-dev.txt', '.github/workflows/foundation.yml')
    def git(*a):
        return subprocess.check_output(['git', *a], cwd=ROOT)
    assert not git('status', '--porcelain', '--', *scoped), 'commit qualification inputs first'
    head = git('rev-parse', 'HEAD').decode().strip()
    names = git('ls-files', '--', *scoped).decode().splitlines()
    source = frozen_bytes(head, names)
    # Runtime import origins, not just sys.path claims. All product modules must be installed.
    installed = {}
    for name in ('equity-feature-contracts', 'equity-features', 'equity-feature-workers', 'equity-feature-io-contracts',
                 'equity-feature-io-sdk', 'equity-feature-duckdb', 'equity-feature-parquet', 'equity-feature-duckdb-sink'):
        dist = metadata.distribution(name)
        site = Path(dist.locate_file('')).resolve()
        assert 'site-packages' in site.parts
        module = import_module(name.replace('-', '_'))
        assert Path(module.__file__).resolve().is_relative_to(site), 'source-tree product fallback'
        installed[name] = dict(version=dist.version, record_sha256=installed_record(dist))
    work.mkdir(parents=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    report = dict(schema='eq088-scaling1', smoke=args.smoke, head=head, source_sha256=source, installed=installed,
        core_commit='20c08c7370581d03c8a0404579667f68d67ac88b', io_commit='19aa266b9debb92a6c5eb707baafeb9ae7a82942',
        hardware=hardware(), records=[], references=[], diagnostics=[], attempts=[], failures=[], status='running',
        defaults_changed=False, cache='same warmed inputs; uncontrolled OS page cache/background/thermal state')
    def save():
        out.write_text(json.dumps(report, sort_keys=True, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    def disk():
        observed = sum(p.stat().st_size for p in work.rglob('*') if p.is_file())
        report['observed_owned_disk_bytes'] = observed
        assert observed <= 1073741824, 'observed owned disk admission ceiling'
    save()
    def attempt(identity, callback):
        # Commit the attempt identity BEFORE launch; exceptions keep the same entry.
        entry = dict(identity=identity, status='started', started_monotonic_ns=time.monotonic_ns())
        report['attempts'].append(entry)
        save()
        try:
            result = callback()
        except BaseException as error:
            private = work / ('failed-attempt-' + str(len(report['attempts'])) + '.log')
            try:
                private.write_text(traceback.format_exc(), encoding='utf-8')
                entry['private_traceback_retained'] = True
                entry['private_traceback_sha256'] = hashlib.sha256(private.read_bytes()).hexdigest()
            except Exception:
                entry['private_traceback_retained'] = False
            entry.update(status='failed', error_type=type(error).__name__,
                timeout=isinstance(error, subprocess.TimeoutExpired),
                cleanup_outcome='error-notes-present' if getattr(error, '__notes__', ()) else 'not-independently-established')
            entry['ended_monotonic_ns'] = time.monotonic_ns()
            save()
            raise
        raw_result = json.dumps(result, sort_keys=True, indent=2, allow_nan=False).encode() + b'\n'
        (work / ('attempt-result-' + str(len(report['attempts'])) + '.json')).write_bytes(raw_result)
        entry['returned_result_sha256'] = hashlib.sha256(raw_result).hexdigest()
        entry['returned_result_bytes'] = len(raw_result)
        entry.update(status='returned', ended_monotonic_ns=time.monotonic_ns(),
                     returned_status=result.get('status', 'skipped' if 'skipped' in result else 'completed'))
        save()
        return result
    try:
        workloads = ('small',) if args.smoke else ('small', 'large', 'skew', 'capacity')
        modes = MODES[:1] if args.smoke else MODES
        repeats = 1 if args.smoke else 3
        for workload in workloads:
            fixture = work / ('input-' + workload)
            inputs = make_fixture(fixture, workload)
            references = {}
            for backend in ('parquet', 'duckdb'):
                assert admission(report['hardware'], 1) is None, 'reference host admission unavailable'
                sample = attempt(dict(kind='reference', workload=workload, backend=backend, mode='sequential', workers=1),
                    lambda: isolated_sample(fixture, work / f'{workload}-{backend}-reference', workload, backend, 'sequential', 1, True))
                assert 'skipped' not in sample
                references[backend] = sample['content_sha256']
                report['references'].append(sample)
                save(); disk()
            assert len(set(references.values())) == 1
            for repeat in range(repeats):
                order = modes[repeat % len(modes):] + modes[:repeat % len(modes)]
                for mode, workers in order:
                    for backend in ('parquet', 'duckdb'):
                        identity = dict(workload=workload, backend=backend, mode=mode, workers=workers, repeat=repeat)
                        reason = admission(hardware(), workers)
                        if reason:
                            report['records'].append(identity | dict(skipped=reason))
                        else:
                            sample = attempt(dict(kind='pipeline', **identity),
                                lambda: isolated_sample(fixture, work / f'{workload}-{backend}-{mode}{workers}-{repeat}', workload, backend, mode, workers, True))
                            if 'skipped' not in sample:
                                assert sample['content_sha256'] == references[backend]
                                assert sample['input_population'] == inputs
                            report['records'].append(identity | sample)
                        save(); disk()
                        print(f'{workload} {backend} {mode}{workers} repeat{repeat} recorded', flush=True)
            assert population(fixture) == inputs
        counts = (512,) if args.smoke else (500000, 2000000)
        for count in counts:
            for policy, memory in [('sink', '128MiB'), ('source', '32MB'), ('source', '256MB')]:
                settings = dict(kind='sort', root=str(work / f'sort-{policy}-{memory}-{count}'), count=count,
                                sink=policy == 'sink', memory=memory)
                identity = {k: v for k, v in settings.items() if k != 'root'}
                report['diagnostics'].append(attempt(identity, lambda: native_probe(settings))); save(); disk()
        layout_settings = dict(kind='layout', root=str(work / 'layout'), count=256 if args.smoke else 65536)
        report['diagnostics'].append(attempt(dict(kind='layout', count=layout_settings['count']), lambda: native_probe(layout_settings)))
        report['groups'] = groups(report['records'])
        report['scheduling_decision'] = scheduling_decision(report['groups']) if not args.smoke else 'smoke-not-decision'
        report['status'] = 'completed-protocol-not-automatic-acceptance'
        assert not git('status', '--porcelain', '--', *scoped)
        assert git('rev-parse', 'HEAD').decode().strip() == head
        assert frozen_bytes(head, names) == source
        disk(); save()
    except BaseException as error:
        report['status'] = 'failed'
        report['failures'].append(dict(type=type(error).__name__, notes='private traceback retained outside public report'))
        save()
        raise


if __name__ == '__main__':
    main()
