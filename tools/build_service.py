"""Committed repeat archives and fresh optional slice/job qualification."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import zipfile
import tarfile

from build_foundation import build, fingerprint, git, run, sha, snapshot, CORE_COMMIT, IO_COMMIT

ROOT = Path(__file__).resolve().parents[1]
DEPENDENCIES = ('packages/contracts', 'packages/features', 'packages/io-contracts', 'packages/io-sdk', 'packages/duckdb')
WORKER_COMMIT = '7d86deb94b6c338be9cc893c729352c59517e28f'


def audit(path, source):
    if path.suffix == '.whl':
        with zipfile.ZipFile(path) as archive:
            files = {p: archive.read(p) for p in archive.namelist() if not p.endswith('/')}
        assert all(p.startswith(('equity_feature_service/', 'equity_feature_service-')) for p in files)
        assert 'equity_feature_service/py.typed' in files
        assert any(p.endswith('/licenses/LICENSE') for p in files)
        metadata = next(v for p, v in files.items() if p.endswith('/METADATA'))
        for p in source.rglob('*'):
            if p.is_file(): assert files[p.relative_to(source.parent).as_posix()] == p.read_bytes()
    else:
        with tarfile.open(path) as archive:
            assert not any(m.issym() or m.islnk() for m in archive.getmembers())
            files = {m.name: archive.extractfile(m).read() for m in archive.getmembers() if m.isfile()}
        prefix = path.name.removesuffix('.tar.gz') + '/'
        metadata = files[prefix + 'PKG-INFO']
        allowed = {prefix + p for p in ('LICENSE', 'README.md', 'PKG-INFO', 'pyproject.toml', 'setup.cfg')}
        assert all(p.startswith((prefix + 'src/equity_feature_service/', prefix + 'src/equity_feature_service.egg-info/')) or p in allowed for p in files)
        for p in source.rglob('*'):
            if p.is_file(): assert files[prefix + 'src/' + p.relative_to(source.parent).as_posix()] == p.read_bytes()
    assert b'License-Expression: Apache-2.0' in metadata
    assert b'Version: 0.1.0a1' in metadata
    for name, data in files.items():
        assert '..' not in Path(name).parts and not name.startswith(('/', '\\'))
        assert '__pycache__' not in name and not name.endswith('.pyc')
        assert b'C:\\Users\\' not in data and b'/home/runner/' not in data
        assert not name.endswith('entry_points.txt')


def qualify(form, artifact, deps, committed):
    with tempfile.TemporaryDirectory(prefix='service-install-', dir=ROOT / 'work') as tmp:
        location = Path(tmp)
        run(sys.executable, '-m', 'venv', location)
        py = location / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
        run(py, '-m', 'pip', 'install', '--no-deps', 'setuptools==80.9.0', 'mypy==1.15.0', 'mypy_extensions==1.1.0', '-r', committed / 'tools/requirements-service.txt')
        core = [p for p in deps if p.name.startswith(('equity_feature_contracts-', 'equity_features-'))]
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', *core)
        before = fingerprint(py, location)
        light = [p for p in deps if p.name.startswith(('equity_feature_io_contracts-', 'equity_feature_io_sdk-'))]
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', '--no-build-isolation', *light, artifact)
        run(py, '-I', '-c', "import importlib.util;import equity_feature_service as s;from pathlib import Path;assert 'site-packages' in Path(s.__file__).resolve().parts;assert s.__version__=='0.1.0a1';assert importlib.util.find_spec('duckdb') is None;assert importlib.util.find_spec('equity_feature_duckdb') is None;assert importlib.util.find_spec('equity_feature_workers') is None", cwd=location)
        assert fingerprint(py, location) == before
        run(py, '-m', 'pip', 'check')
        run(py, '-I', committed / 'examples/service_owned.py', cwd=location)
        run(py, '-m', 'pip', 'install', '--no-deps', 'duckdb==1.5.6', 'numpy==2.2.6')
        native = [p for p in deps if p.name.startswith('equity_feature_duckdb-')]
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', *native)
        jobs = [p for p in deps if p.name.startswith(('equity_feature_workers-', 'equity_feature_example_extensions-'))]
        assert len(jobs) == 2
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', *jobs)
        run(py, '-I', '-m', 'unittest', 'discover', '-s', committed / 'tests/service', '-v', cwd=location)
        run(py, '-I', '-m', 'mypy', '--strict', '-p', 'equity_feature_service', cwd=location)
        positive = location / 'consumer.py'
        positive.write_text('from equity_feature_service import Scope, Credential, Limits\ns: Scope = Scope("owned:ONE", "session-1", 1, 2)\nc: Credential = Credential.provision("owned", "a" * 43, 0, 100)\nl: Limits = Limits(1, 1024, 2048, 60000000000)\n', encoding='utf-8')
        run(py, '-I', '-m', 'mypy', '--strict', positive, cwd=location)
        positive.write_text('from equity_feature_service import Scope\ns = Scope("owned:ONE", "session-1", "bad", 2)\n', encoding='utf-8')
        invalid = subprocess.run([str(py), '-I', '-m', 'mypy', '--strict', str(positive)], cwd=location, capture_output=True, text=True)
        assert invalid.returncode == 1 and '[arg-type]' in invalid.stdout
        run(py, '-m', 'pip', 'check')
        assert fingerprint(py, location) == before
        return {'form':form, 'light_optional_absence':True, 'installed_site_packages':True, 'native_http_tests':True, 'native_job_tests':True, 'strict_package_typing':True, 'consumer_positive_negative':True, 'core_before_after_sha256':hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest()}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--core-root', type=Path, required=True)
    p.add_argument('--io-root', type=Path, required=True)
    p.add_argument('--outdir', type=Path, default=ROOT / 'dist/service')
    args = p.parse_args()
    commit = git(ROOT, 'rev-parse', 'HEAD')
    (ROOT / 'work').mkdir(exist_ok=True)
    output = args.outdir.resolve()
    assert not output.exists(), 'Use a new qualification checkout/output; preserve prior evidence.'
    output.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix='service-build-', dir=ROOT / 'work') as tmp:
        temp = Path(tmp)
        committed = snapshot(ROOT, commit, temp / 'component', ('packages/service','tests/service','tools','examples/service_owned.py'))
        core = snapshot(args.core_root.resolve(), CORE_COMMIT, temp / 'core')
        io = snapshot(args.io_root.resolve(), IO_COMMIT, temp / 'io')
        workers = snapshot(ROOT, WORKER_COMMIT, temp / 'workers', ('packages/workers',))
        for relative in DEPENDENCIES:
            repo = core if relative in DEPENDENCIES[:2] else io
            build(repo / relative, temp / 'dependencies')
        build(workers / 'packages/workers', temp / 'dependencies')
        build(io / 'examples/third_party', temp / 'dependencies')
        deps = sorted((temp / 'dependencies').glob('*.whl'))
        assert len(deps) == 7
        # Fresh source snapshots ensure build debris cannot enter either repeat.
        other = snapshot(ROOT, commit, temp / 'repeat', ('packages/service',))
        build(committed / 'packages/service', temp / 'first')
        build(other / 'packages/service', temp / 'second')
        first = {p.name:sha(p) for p in (temp / 'first').iterdir()}
        second = {p.name:sha(p) for p in (temp / 'second').iterdir()}
        assert first == second and len(first) == 2
        forms = []
        for artifact in sorted((temp / 'first').iterdir()):
            audit(artifact, other / 'packages/service/src/equity_feature_service')
            forms.append(qualify('wheel' if artifact.suffix == '.whl' else 'sdist', artifact, deps, committed))
            (output / artifact.name).write_bytes(artifact.read_bytes())
        for artifact in deps: (output / artifact.name).write_bytes(artifact.read_bytes())
        inputs = {p.relative_to(committed).as_posix():sha(p) for p in committed.rglob('*') if p.is_file() and '__pycache__' not in p.parts and 'build' not in p.parts and not any(part.endswith('.egg-info') for part in p.parts)}
        worker_inputs = {p.relative_to(workers).as_posix():sha(p) for p in workers.rglob('*') if p.is_file() and '__pycache__' not in p.parts and 'build' not in p.parts and not any(part.endswith('.egg-info') for part in p.parts)}
        receipt = {'schema':'service1','commit':commit,'core_commit':CORE_COMMIT,'io_commit':IO_COMMIT,'worker_commit':WORKER_COMMIT,'worker_source_inputs':worker_inputs,'python':platform.python_version(),'system':platform.system(),'machine':platform.machine(),'epoch':1700000000,'repeat_artifacts':first,'dependencies':{p.name:sha(p) for p in deps},'source_inputs':inputs,'forms':forms}
        (output / 'service-receipt.json').write_text(json.dumps(receipt, sort_keys=True, indent=2) + '\n', encoding='utf-8', newline='\n')
    print('Committed repeat archives, fresh wheel/sdist, native HTTP/source fixtures and installed boundaries PASS')


if __name__ == '__main__': main()
