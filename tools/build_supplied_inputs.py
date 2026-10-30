"""Frozen Git inputs, closed archives and four independent installed forms."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

from supplied_archives import inspect, safe_name
sys.path.insert(0, str(Path(__file__).resolve().parent))  # tools/_versions.py, also when loaded by path from tests
from _versions import VERSIONS

ROOT = Path(__file__).resolve().parents[1]
# Current candidate pins; coordinator substitutes exact reviewed SHAs before push.
CORE = 'f43434b094b15abbd100dc080a44689871ca94e7'
IO = 'e47b0c5759ba58eed6afd0ec889b4f8d9cb813a0'
EPOCH = '1700000000'
PACKAGES = {
    'contracts': ('core', 'packages/contracts', 'equity_feature_contracts', VERSIONS['equity-feature-contracts']),
    'features': ('core', 'packages/features', 'equity_features', VERSIONS['equity-features']),
    'intraday': ('core', 'packages/intraday', 'equity_features_intraday', VERSIONS['equity-features-intraday']),
    'io-contracts': ('io', 'packages/io-contracts', 'equity_feature_io_contracts', VERSIONS['equity-feature-io-contracts']),
    'io-sdk': ('io', 'packages/io-sdk', 'equity_feature_io_sdk', VERSIONS['equity-feature-io-sdk']),
    'workers': ('worker', 'packages/workers', 'equity_feature_workers', VERSIONS['equity-feature-workers']),
}


def sha(body):
    return hashlib.sha256(body).hexdigest()


def environment():
    result = dict(os.environ, SOURCE_DATE_EPOCH=EPOCH, PYTHONHASHSEED='0', PYTHONDONTWRITEBYTECODE='1')
    for key in ('PYTHONPATH', 'PYTHONHOME', 'PYTHONUSERBASE', 'MYPYPATH'):
        result.pop(key, None)
    return result


def output(*args, cwd=ROOT):
    return subprocess.check_output([str(x) for x in args], cwd=cwd, env=environment())


def run(*args, cwd=ROOT):
    subprocess.run([str(x) for x in args], cwd=cwd, env=environment(), check=True)


def freeze(root, head, paths):
    """Read verified blob bytes, never archive a mutable working directory."""
    assert output('git', 'rev-parse', head, cwd=root).decode().strip() == head
    working_head = output('git', 'rev-parse', 'HEAD', cwd=root)
    assert not output('git', 'status', '--porcelain', '--untracked-files=all', '--', *paths, cwd=root)
    tree = output('git', 'ls-tree', '-r', '--full-tree', head, cwd=root)
    entries = []
    for line in tree.decode().splitlines():
        prefix, name = line.split('\t'); mode, kind, blob = prefix.split()
        assert kind == 'blob' and mode in ('100644', '100755')
        safe_name(name)
        entries.append((name, mode, blob))
    assert len(entries) == len({x[0] for x in entries})
    process = subprocess.run(['git', 'cat-file', '--batch'], cwd=root, check=True,
        input=b''.join(blob.encode()+b'\n' for _, _, blob in entries), stdout=subprocess.PIPE)
    offset = 0; whole = {}; selected = {}; live = {}
    for name, mode, blob in entries:
        end = process.stdout.index(b'\n', offset)
        header = process.stdout[offset:end].split()
        assert len(header) == 3 and header[:2] == [blob.encode(), b'blob']
        size = int(header[2]); body = process.stdout[end+1:end+1+size]
        assert len(body) == size and process.stdout[end+1+size:end+2+size] == b'\n'
        assert hashlib.sha1(b'blob '+str(size).encode()+b'\0'+body).hexdigest() == blob
        whole[name] = {'blob': blob, 'mode': mode, 'bytes': size, 'sha256': sha(body)}
        offset = end+2+size
        if any(name == path or name.startswith(path+'/') for path in paths):
            selected[name] = body; live[name] = (root/name).read_bytes()
            assert live[name] == body or live[name].replace(b'\r\n', b'\n') == body
    assert offset == len(process.stdout) and selected
    index = output('git', 'ls-files', '--', *paths, cwd=root).decode().splitlines()
    assert set(index) == set(selected)
    def closure():
        assert output('git', 'rev-parse', 'HEAD', cwd=root) == working_head
        assert output('git', 'rev-parse', head, cwd=root).decode().strip() == head
        assert output('git', 'ls-tree', '-r', '--full-tree', head, cwd=root) == tree
        assert output('git', 'ls-files', '--', *paths, cwd=root).decode().splitlines() == index
        assert not output('git', 'status', '--porcelain', '--untracked-files=all', '--', *paths, cwd=root)
        assert all((root/name).read_bytes() == body for name, body in live.items())
    return selected, {'commit': head, 'whole_tree_sha256': sha(tree), 'whole_tree': whole,
        'selected': {n: {'git_sha256': sha(b), 'working_sha256': sha(live[n])} for n, b in selected.items()}}, closure


def materialize(directory, files):
    directory.mkdir(parents=True, exist_ok=False)
    for name, body in files.items():
        safe_name(name); target = directory/name
        target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(body)


def normalize(path):
    data = io.BytesIO()
    with tarfile.open(path, 'r:gz') as source, tarfile.open(fileobj=data, mode='w', format=tarfile.PAX_FORMAT) as target:
        for member in sorted(source.getmembers(), key=lambda x: x.name):
            member.mtime = int(EPOCH); member.uid = member.gid = 0
            member.uname = member.gname = ''; member.pax_headers = {}
            member.mode = 0o755 if member.isdir() else 0o644
            target.addfile(member, source.extractfile(member) if member.isfile() else None)
    with path.open('wb') as stream, gzip.GzipFile(fileobj=stream, mode='wb', filename='', mtime=int(EPOCH)) as zipped:
        zipped.write(data.getvalue())


def rebuild_inputs(directory, files, prefix, module):
    expected = dict(files); manifest = prefix+'src/'+module+'.egg-info/SOURCES.txt'
    names = set(files[manifest].decode().splitlines()) | {'setup.cfg'}
    expected[manifest] = '\n'.join(sorted(names, key=lambda n: (str(PurePosixPath(n).parent), PurePosixPath(n).name))).encode()
    # Wheel construction copies the selected package into build/lib. Close that
    # generated namespace against exact selected bytes instead of treating the
    # legitimate copies as changed source or ignoring arbitrary generated files.
    expected.update({prefix+'build/lib/'+n.removeprefix(prefix+'src/'): body
        for n, body in files.items() if n.startswith(prefix+'src/'+module+'/')})
    assert not any(p.is_symlink() for p in directory.rglob('*'))
    actual = {prefix+p.relative_to(directory).as_posix(): p.read_bytes() for p in directory.rglob('*') if p.is_file()}
    assert actual == expected, 'selected SDist input drift'
    return {n: {'sha256': sha(b), 'bytes': len(b)} for n, b in expected.items()}


def typing(py, verification, directory, combined):
    examples = verification/'examples/typing'
    expected = json.loads((examples/'supplied_negative_expected.json').read_bytes())
    assert output(py, '-I', '-B', '-m', 'mypy', '--version', cwd=directory).decode().startswith('mypy '+expected['mypy_version'])
    report = {}
    for key, stem in [('base', 'supplied')]+([('intraday', 'supplied_intraday')] if combined else []):
        command = [py, '-I', '-B', '-m', 'mypy', '--strict', '--no-incremental', '--show-error-codes',
            '--no-error-summary', '--config-file', examples/'mypy.ini']
        positive = output(*command, examples/(stem+'_positive.py'), cwd=directory)
        run(py, '-I', '-B', examples/(stem+'_positive.py'), cwd=directory)
        result = subprocess.run([str(x) for x in [*command, examples/(stem+'_negative.py')]],
            cwd=directory, env=environment(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert result.returncode == 1 and not result.stderr, result.stdout+result.stderr
        rows = []
        for line in result.stdout.decode().splitlines():
            match = re.fullmatch(r'.*'+stem+r'_negative\.py:(\d+): error: (.*)  \[([a-z-]+)\]', line)
            assert match, line
            rows.append((int(match[1]), match[3], match[2]))
        assert len(rows) == len(expected[key])
        for row, want in zip(rows, expected[key]):
            assert row[0] == want['line'] and row[1] == want['code'] and want['contains'] in row[2]
        report[key] = {'positive_sha256': sha(positive), 'negative_sha256': sha(result.stdout), 'diagnostics': expected[key]}
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--core-root', required=True, type=Path)
    parser.add_argument('--io-root', required=True, type=Path)
    args = parser.parse_args()
    assert sys.version_info[:2] == (3, 12)
    assert all(re.fullmatch(r'[0-9a-f]{40}', pin) for pin in (CORE, IO)), \
        'Replace current EF1 source pins with reviewed exact commits'
    out_root = ROOT/'dist/supplied-inputs'; out_root.mkdir(parents=True, exist_ok=True)
    out = Path(tempfile.mkdtemp(prefix='qualification-', dir=out_root))
    head = output('git', 'rev-parse', 'HEAD').decode().strip()
    inputs = {}; provenance = {}; closures = []
    for key, root, commit, paths in [
        ('worker', ROOT, head, ['packages/workers', 'tools', 'tests', 'examples', 'docs', 'AGENTS.md',
            'requirements-dev.txt']),
        ('core', args.core_root.resolve(), CORE, ['packages/contracts', 'packages/features', 'packages/intraday', 'tools/continuous_legacy.py']),
        ('io', args.io_root.resolve(), IO, ['packages/io-contracts', 'packages/io-sdk', 'examples/factory_consumer/src/equity_feature_factory_fixture/publication.py'])]:
        inputs[key], provenance[key], check = freeze(root, commit, paths); closures.append(check)
    report = {'schema': 'supplied-inputs-qualification:1', 'status': 'running', 'source': provenance,
        'system': platform.system(), 'python': platform.python_version(), 'artifacts': {}, 'forms': [],
        'limits': 'Synthetic supplied inputs and memory publication. No speed, physical durability, production scheduling or live qualification.'}
    def save():
        (out/'qualification.json').write_bytes((json.dumps(report, sort_keys=True, indent=2)+'\n').encode())
    save()
    try:
        work = ROOT/'work'; work.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='supplied-', dir=work) as temporary:
            owned = Path(temporary); verification = owned/'verification'
            materialize(verification, inputs['worker'])
            def verify():
                assert {p.relative_to(verification).as_posix(): p.read_bytes() for p in verification.rglob('*') if p.is_file()} == inputs['worker']
                for check in closures: check()
            verify()
            sources = {}; wheels = {}; archives = {}; rebuilt = {}
            delegate = inputs['io']['examples/factory_consumer/src/equity_feature_factory_fixture/publication.py']
            assert inputs['worker']['tests/supplied_memory_delegate.py'] == delegate, 'original SDK fixture drift'
            for name, (repo, prefix, module, version) in PACKAGES.items():
                sources[name] = {n.removeprefix(prefix+'/'): b for n, b in inputs[repo].items() if n.startswith(prefix+'/')}
                for repeat in (0, 1):
                    directory = owned/f'source-{name}-{repeat}'; materialize(directory, sources[name])
                    dest = owned/f'build-{name}-{repeat}'; dest.mkdir()
                    run(sys.executable, '-I', '-B', '-m', 'build', '--no-isolation', '--outdir', dest, directory)
                    assert all((directory/n).read_bytes() == b for n, b in sources[name].items())
                    for suffix in ('.whl', '.tar.gz'):
                        artifact = dest/(module+'-'+version+('-py3-none-any.whl' if suffix == '.whl' else suffix))
                        try:
                            files, receipt = inspect(artifact, sources[name], module)
                        except BaseException:
                            shutil.copyfile(artifact, out/('FAILED_UNVERIFIED-'+artifact.name)); raise
                        if suffix == '.tar.gz':
                            normalize(artifact); files, receipt = inspect(artifact, sources[name], module)
                        if repeat == 0:
                            report['artifacts'][artifact.name] = receipt
                            target = out/artifact.name; shutil.copyfile(artifact, target); archives[name, suffix] = target
                            if suffix == '.whl': wheels[name] = files
                        else: assert receipt == report['artifacts'][artifact.name], 'repeat archive drift'
                save()
            assert len(report['artifacts']) == 12
            report['repeat_archive_parity'] = True; save()
            for name, (_, _, module, version) in PACKAGES.items():
                files, receipt = inspect(archives[name, '.tar.gz'], sources[name], module)
                prefix = module+'-'+version+'/'
                selected = owned/('sdist-'+name)
                materialize(selected, {n.removeprefix(prefix): b for n, b in files.items()})
                dest = owned/('rebuilt-'+name); dest.mkdir()
                run(sys.executable, '-I', '-B', '-m', 'build', '--wheel', '--no-isolation', '--outdir', dest, selected)
                transition = rebuild_inputs(selected, files, prefix, module)
                wheel = dest/(module+'-'+version+'-py3-none-any.whl')
                actual, rebuilt_receipt = inspect(wheel, sources[name], module)
                assert actual == wheels[name], 'selected SDist rebuilt payload drift'
                rebuilt[name] = wheel
                report.setdefault('sdist_rebuilds', {})[name] = {'selected': receipt, 'after_build_inputs': transition, 'rebuilt': rebuilt_receipt}
                save()
            for form in ('light-wheel', 'light-sdist', 'combined-wheel', 'combined-sdist'):
                combined = form.startswith('combined'); directory = owned/form
                run(sys.executable, '-I', '-B', '-m', 'venv', directory)
                py = directory/('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
                run(py, '-I', '-B', '-m', 'pip', 'install', '--no-compile', '--no-deps', 'pip==25.1.1',
                    'mypy==1.15.0', 'mypy_extensions==1.1.0', 'typing_extensions==4.16.0')
                selected = rebuilt if form.endswith('sdist') else {n: archives[n, '.whl'] for n in PACKAGES}
                def install(names):
                    run(py, '-I', '-B', '-m', 'pip', 'install', '--no-compile', '--no-index', '--no-deps', *[selected[n] for n in names])
                def installed(names):
                    payload = owned/(form+'-selected.json')
                    payload.write_bytes(json.dumps([{'module': PACKAGES[n][2], 'archive': str(selected[n])} for n in names]).encode())
                    return json.loads(output(py, '-I', '-B', verification/'tools/supplied_installed.py', payload, cwd=directory))
                install(['contracts', 'features']); before = installed(['contracts', 'features'])
                legacy = owned/'continuous_legacy.py'
                legacy.write_bytes(inputs['core']['tools/continuous_legacy.py'])
                run(py, '-I', '-B', legacy, cwd=directory)
                names = ['contracts', 'features', 'io-contracts', 'io-sdk', 'workers']+(['intraday'] if combined else [])
                install(names[2:]); after = installed(names)
                assert all(after[m] == receipt for m, receipt in before.items())
                run(py, '-I', '-B', legacy, cwd=directory)
                run(py, '-I', '-B', '-m', 'pip', 'check', cwd=directory)
                absent = ['numpy', 'pyarrow', 'duckdb', 'equity_feature_acquisition', 'equity_feature_files', 'equity_feature_service']
                if not combined: absent.append('equity_features_intraday')
                run(py, '-I', '-B', '-c', 'import importlib.util,sys;assert all(importlib.util.find_spec(n) is None for n in sys.argv[1:]);import equity_feature_workers;assert "equity_feature_workers.intraday" not in sys.modules', *absent, cwd=directory)
                test_report = owned/(form+'-tests.json')
                run(py, '-I', '-B', verification/'tools/supplied_test_runner.py', verification/'tests', test_report, str(int(combined)), cwd=directory)
                tests = json.loads(test_report.read_bytes())
                typed = typing(py, verification, directory, combined)
                if combined:
                    run(py, '-I', '-B', '-m', 'mypy', '--strict', '--no-incremental',
                        '-m', 'equity_feature_workers._supplied', '-m', 'equity_feature_workers.intraday',
                        '-m', 'equity_feature_workers.supplied_session', '-m', 'equity_feature_workers.publication', cwd=directory)
                if combined: run(py, '-I', '-B', verification/'examples/supplied_intraday.py', cwd=directory)
                assert installed(names) == after
                verify()
                report['forms'].append({'form': form, 'installed': after, 'core_unchanged': True, 'tests': tests,
                    'typing': typed, 'optional_absence': absent, 'synthetic_example_passed': combined}); save()
            verify()
    except BaseException as error:
        report.update(status='failed', failure_type=type(error).__name__, failure_scope='Partial forms are not complete acceptance.')
        save(); raise
    report.update(status='completed', source_head_and_materialization_closure=True); save()
    print('Supplied inputs: repeat archives, selected SDist rebuilds and four fresh forms PASS')


if __name__ == '__main__':
    main()
