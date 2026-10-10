"""Committed client archives, optional absence, fresh native forms and receipts."""
import argparse
import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import tarfile
import tomllib
import zipfile

from build_foundation import build, fingerprint, git, run, sha, snapshot, CORE_COMMIT, IO_COMMIT

ROOT = Path(__file__).resolve().parents[1]
WORKER_COMMIT = '7d86deb94b6c338be9cc893c729352c59517e28f'
SERVICE_COMMIT = 'bafc86a0305621fdb0206fb89753475186954ac3'
INPUTS = ('packages/client', 'tests/client', 'tools', 'docs/EQ081_CLIENT_FIXTURES.json',
          'docs/EQ081_OWNED_HTTP_ENTRY.txt', 'docs/EQ081_AUTHORITY_HTTP_ENTRY.txt', '.github/workflows/client.yml')


def audit_client(artifact, source):
    if artifact.suffix == '.whl':
        with zipfile.ZipFile(artifact) as archive:
            files = {n: archive.read(n) for n in archive.namelist() if not n.endswith('/')}
        prefix = ''
        allowed = ('equity_feature_client/', 'equity_feature_client-')
        metadata = next(v for n, v in files.items() if n.endswith('/METADATA'))
    else:
        with tarfile.open(artifact) as archive:
            assert not any(m.issym() or m.islnk() for m in archive.getmembers())
            files = {m.name: archive.extractfile(m).read() for m in archive.getmembers() if m.isfile()}
        prefix = artifact.name.removesuffix('.tar.gz') + '/src/'
        root = artifact.name.removesuffix('.tar.gz') + '/'
        allowed = (prefix + 'equity_feature_client/', prefix + 'equity_feature_client.egg-info/')
        extras = {root + p for p in ('LICENSE', 'README.md', 'PKG-INFO', 'pyproject.toml', 'setup.cfg')}
        assert all(n.startswith(allowed) or n in extras for n in files)
        metadata = files[root + 'PKG-INFO']
    if artifact.suffix == '.whl':
        assert all(n.startswith(allowed) for n in files)
    for path in (source / 'src/equity_feature_client').rglob('*'):
        if path.is_file():
            assert files[prefix + path.relative_to(source / 'src').as_posix()] == path.read_bytes()
    assert prefix + 'equity_feature_client/py.typed' in files
    assert b'License-Expression: Apache-2.0' in metadata and b'Version: 0.1.0a0' in metadata
    project = tomllib.loads((source / 'pyproject.toml').read_text(encoding='utf-8'))['project']
    requires = sorted(line[len('Requires-Dist: '):].strip() for line in metadata.decode().splitlines() if line.startswith('Requires-Dist: '))
    expected = project['dependencies'] + [dep + '; extra == "native"' for dep in project['optional-dependencies']['native']]
    assert requires == sorted(expected), requires
    assert any(n.endswith('/licenses/LICENSE') if artifact.suffix == '.whl' else n.endswith('/LICENSE') for n in files)
    for name, data in files.items():
        assert '..' not in Path(name).parts and not name.startswith(('/', '\\'))
        assert '__pycache__' not in name and not name.endswith(('.pyc', 'entry_points.txt'))
        assert b'C:\\Users\\' not in data and b'/home/runner/' not in data


def wheel_record(path):
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        record = next(n for n in names if n.endswith('.dist-info/RECORD'))
        rows = list(csv.reader(io.StringIO(archive.read(record).decode('utf-8'))))
        assert {row[0] for row in rows} == names
        for name, digest, size in rows:
            if name == record:
                assert digest == size == ''
            else:
                data = archive.read(name)
                assert digest == 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode('ascii')
                assert size == str(len(data))


def provenance(root):
    return {p.relative_to(root).as_posix(): sha(p) for p in root.rglob('*') if p.is_file()
            and not any(part in ('build', '__pycache__') or part.endswith('.egg-info') for part in p.parts)}


def qualify(artifact, deps, committed, service):
    with tempfile.TemporaryDirectory(prefix='client-install-', dir=ROOT / 'work') as tmp:
        location = Path(tmp)
        run(sys.executable, '-m', 'venv', location / 'venv')
        py = location / 'venv' / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
        run(py, '-m', 'pip', 'install', '--no-deps', 'setuptools==80.9.0', 'mypy==1.15.0', 'mypy_extensions==1.1.0', '-r', committed / 'tools/requirements-service.txt')
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', '--no-build-isolation', artifact)
        run(py, '-I', '-c', "import sys,importlib.util;from pathlib import Path;import equity_feature_client as c;assert 'site-packages' in Path(c.__file__).resolve().parts;assert not c.native_available();assert all(importlib.util.find_spec(n) is None for n in ('equity_feature_contracts','equity_feature_io_sdk','equity_features','equity_feature_service','equity_feature_workers','duckdb'));assert c.RemoteClient('http://127.0.0.1:9',lambda:'A'*43).discover(request_id='owned').failure.code=='disconnected';assert not any(n.startswith(('equity_feature_contracts','equity_feature_io','equity_features','equity_feature_service','equity_feature_workers')) for n in sys.modules)", cwd=location)
        run(py, '-m', 'pip', 'check')
        core = [p for p in deps if p.name.startswith(('equity_feature_contracts-', 'equity_features-'))]
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', *core)
        before = fingerprint(py, location)
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', '--no-build-isolation', '--force-reinstall', artifact)
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', *[p for p in deps if p not in core])
        run(py, '-m', 'pip', 'install', '--no-deps', 'duckdb==1.5.6', 'numpy==2.2.6')
        # The qualification harness has no source package directories. Tests'
        # development path insertions therefore cannot shadow installed code.
        harness = location / 'harness'
        shutil.copytree(committed / 'tests/client', harness / 'tests/client')
        shutil.copytree(service / 'tests/service', harness / 'tests/service')
        (harness / 'docs').mkdir()
        for name in ('EQ081_CLIENT_FIXTURES.json', 'EQ081_OWNED_HTTP_ENTRY.txt', 'EQ081_AUTHORITY_HTTP_ENTRY.txt'):
            shutil.copy2(committed / 'docs' / name, harness / 'docs' / name)
        assert not (harness / 'packages').exists()
        run(py, '-I', '-c', "from pathlib import Path;import equity_feature_client as c;assert 'site-packages' in Path(c.__file__).resolve().parts;assert c.native_available()", cwd=location)
        run(py, '-I', '-m', 'unittest', 'discover', '-s', harness / 'tests/client', '-v', cwd=location)
        run(py, '-I', '-m', 'mypy', '--strict', '-p', 'equity_feature_client', cwd=location)
        consumer = location / 'consumer.py'
        consumer.write_text('from equity_feature_client import RemoteClient, Outcome, DiscoveryView\nfrom equity_feature_client.native import convert_raw\nc: RemoteClient = RemoteClient("http://127.0.0.1:9", lambda: "A"*43)\no: Outcome[DiscoveryView] = c.discover(request_id="owned")\n', encoding='utf-8')
        run(py, '-I', '-m', 'mypy', '--strict', consumer, cwd=location)
        consumer.write_text('from equity_feature_client import RemoteClient\nc = RemoteClient("http://127.0.0.1:9", lambda: 123)\nc.discover(request_id=123)\n', encoding='utf-8')
        rejected = subprocess.run([str(py), '-I', '-m', 'mypy', '--strict', str(consumer)], cwd=location, capture_output=True, text=True)
        assert rejected.returncode == 1 and '[arg-type]' in rejected.stdout
        run(py, '-m', 'pip', 'check')
        assert fingerprint(py, location) == before
        return {'form': 'wheel' if artifact.suffix == '.whl' else 'sdist', 'light_optional_absence': True,
                'installed_without_source_shadow': True, 'native_tests': True, 'strict_typing': True,
                'consumer_positive_negative': True, 'core_before_after_sha256': hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--core-root', type=Path, required=True)
    parser.add_argument('--io-root', type=Path, required=True)
    parser.add_argument('--outdir', type=Path, default=ROOT / 'dist/client')
    args = parser.parse_args()
    commit = git(ROOT, 'rev-parse', 'HEAD')
    assert not git(ROOT, 'status', '--porcelain', '--', *INPUTS), 'Qualification inputs must be committed and clean.'
    (ROOT / 'work').mkdir(exist_ok=True)
    output = args.outdir.resolve()
    assert not output.exists(), 'Use a fresh output; preserve previous evidence.'
    output.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix='client-build-', dir=ROOT / 'work') as tmp:
        temp = Path(tmp)
        committed = snapshot(ROOT, commit, temp / 'component', INPUTS)
        for name in ('build_client.py', 'build_foundation.py'):
            assert (ROOT / 'tools' / name).read_bytes() == (committed / 'tools' / name).read_bytes()
        service = snapshot(ROOT, SERVICE_COMMIT, temp / 'service', ('packages/service', 'tests/service'))
        workers = snapshot(ROOT, WORKER_COMMIT, temp / 'workers', ('packages/workers',))
        core = snapshot(args.core_root.resolve(), CORE_COMMIT, temp / 'core')
        io_source = snapshot(args.io_root.resolve(), IO_COMMIT, temp / 'io', ('packages', 'examples/third_party'))
        for repository, paths in ((core, ('packages/contracts', 'packages/features')),
                                   (io_source, ('packages/io-contracts', 'packages/io-sdk', 'packages/duckdb', 'examples/third_party')),
                                   (workers, ('packages/workers',)), (service, ('packages/service',))):
            for relative in paths:
                build(repository / relative, temp / 'dependencies')
        deps = sorted((temp / 'dependencies').glob('*.whl'))
        assert len(deps) == 8
        repeat = snapshot(ROOT, commit, temp / 'repeat', ('packages/client',))
        build(committed / 'packages/client', temp / 'first')
        build(repeat / 'packages/client', temp / 'second')
        first = {p.name: sha(p) for p in (temp / 'first').iterdir()}
        assert len(first) == 2 and first == {p.name: sha(p) for p in (temp / 'second').iterdir()}
        forms = []
        for artifact in sorted((temp / 'first').iterdir()):
            audit_client(artifact, repeat / 'packages/client')
            if artifact.suffix == '.whl':
                wheel_record(artifact)
            forms.append(qualify(artifact, deps, committed, service))
            shutil.copy2(artifact, output / artifact.name)
        for artifact in deps:
            wheel_record(artifact)
            shutil.copy2(artifact, output / artifact.name)
        receipt = dict(schema='client1', commit=commit, core_commit=CORE_COMMIT, io_commit=IO_COMMIT,
                       worker_commit=WORKER_COMMIT, service_commit=SERVICE_COMMIT,
                       source_inputs=provenance(committed), service_source_inputs=provenance(service),
                       worker_source_inputs=provenance(workers), example_source_inputs=provenance(io_source / 'examples/third_party'),
                       repeat_artifacts=first, dependencies={p.name: sha(p) for p in deps}, forms=forms,
                       python=platform.python_version(), system=platform.system(), machine=platform.machine(), epoch=1700000000)
        (output / 'client-receipt.json').write_text(json.dumps(receipt, sort_keys=True, indent=2)+'\n', encoding='utf-8', newline='\n')
    print('Client repeat archives, fresh light/native forms, installed boundaries and core invariance PASS')


if __name__ == '__main__':
    main()
