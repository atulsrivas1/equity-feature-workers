"""Committed EQ083 repeat archives and fresh four-form installed qualification."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import shutil
import subprocess
import sys
import tempfile
import tarfile
import zipfile
import base64
import csv
import io
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tests/resilience'))
from private_directory import private_directory

from build_foundation import build, fingerprint, git, run, sha, snapshot, CORE_COMMIT, IO_COMMIT
from build_client import audit_client, wheel_record, provenance
from build_service import audit as audit_service
from build_mcp import audit_mcp

ROOT = Path(__file__).resolve().parents[1]
INPUTS = ('packages', 'tools', 'tests', 'docs', '.github/workflows/resilience.yml')
GROUPS = ('R07', 'AUTHORITY', 'STATE', 'EPOCH', 'UNKNOWN', 'VERSION', 'REFERENCE_FAULT', 'TLS', 'REMAINING')
PROJECTS = ('service', 'client', 'mcp')
BOOTSTRAP = ('pip==25.1.1', 'setuptools==80.9.0', 'mypy==1.15.0', 'mypy_extensions==1.1.0')
ACCEPTED_WORKER_COMMIT = '84b15c01fcb9526e4557df83d055daa5303db639'


def wheel_archive_record(path):
    """Root wheel RECORD owns vendored metadata files too."""
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries) != len({item.filename for item in entries}):raise ValueError('duplicate wheel entry')
        for item in entries:
            if type(item.orig_filename) is not str or item.orig_filename != item.filename:
                raise ValueError('owned original ZIP spelling')
            safe_relative(item.orig_filename[:-1] if item.is_dir() else item.orig_filename)
        names = {item.filename for item in entries if not item.is_dir()}
        records = [name for name in names if len(PurePosixPath(name).parts) == 2
                   and PurePosixPath(name).parts[0].endswith('.dist-info') and name.endswith('/RECORD')]
        if len(records) != 1:raise ValueError('owned root wheel RECORD')
        record = records[0]
        rows = list(csv.reader(io.StringIO(archive.read(record).decode('utf-8'))))
        if any(len(row) != 3 for row in rows) or len(rows) != len({row[0] for row in rows}) or {row[0] for row in rows} != names:
            raise ValueError('owned wheel RECORD closure')
        for name, digest, size in rows:
            if name == record:
                if digest or size:raise ValueError('owned wheel RECORD self entry')
            else:
                raw = archive.read(name)
                expected = 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode('ascii')
                if digest != expected or size != str(len(raw)):raise ValueError('owned wheel RECORD file drift')


def archive_entries(path):
    """Every actual archive file, independent of the archive-level digest."""
    if path.suffix == '.whl':
        with zipfile.ZipFile(path) as archive:
            names = [item.filename for item in archive.infolist() if not item.is_dir()]
            if len(names) != len(set(names)):raise ValueError('duplicate archive file')
            files = {name: archive.read(name) for name in names}
    else:
        with tarfile.open(path) as archive:
            members = archive.getmembers()
            if any(item.issym() or item.islnk() for item in members):raise ValueError('archive link')
            names = [item.name for item in members if item.isfile()]
            if len(names) != len(set(names)):raise ValueError('duplicate archive file')
            files = {item.name: archive.extractfile(item).read() for item in members if item.isfile()}
    for name in files:safe_relative(name)
    return {name: dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()) for name, raw in files.items()}


def safe_relative(value):
    if (type(value) is not str or not value or '\\' in value or ':' in value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError('owned relative path')
    path = PurePosixPath(value)
    if path.is_absolute() or '..' in path.parts or '/'.join(path.parts) != value:
        raise ValueError('owned relative path')
    return path


def prepare_harness(committed, location):
    """Only committed helpers/fixtures; product packages remain installed exclusively."""
    harness = location / 'harness'
    harness.mkdir()
    for name in ('service', 'client', 'mcp', 'resilience'):
        shutil.copytree(committed / 'tests' / name, harness / 'tests' / name)
    shutil.copytree(committed / 'tests/resilience', harness / 'resilience')
    (harness / 'docs').mkdir()
    (harness / 'schemas').mkdir()
    for name in ('EQ081_CLIENT_FIXTURES.json', 'EQ081_OWNED_HTTP_ENTRY.txt', 'EQ081_AUTHORITY_HTTP_ENTRY.txt',
                 'EQ082_PRODUCT_REFERENCE_ENTRY.txt', 'EQ082_PRODUCT_REFERENCE_PROBE.txt',
                 'EQ082_NATIVE_FAMILY_ENTRY.txt', 'EQ082_NATIVE_STATE_ENTRY.txt'):
        shutil.copy2(committed / 'docs' / name, harness / 'docs' / name)
    schemas = {'remote-v1.schema.json': 'packages/client/src/equity_feature_client/schemas/remote-v1.schema.json',
               'remote-v1.1.schema.json': 'packages/client/src/equity_feature_client/schemas/remote-v1.1.schema.json',
               'mcp-tools.json': 'packages/mcp/src/equity_feature_mcp/schemas/tools.json'}
    for name, source in schemas.items():
        shutil.copy2(committed / source, harness / 'schemas' / name)
    groups = []
    for name in GROUPS:
        suffix = 'PARENT_CORRECTIVE_INPUTS' if name == 'REMAINING' else 'PARENT_INPUTS'
        manifest_name = 'EQ083_' + name + '_' + suffix + '.json'
        manifest = json.loads((committed / 'docs' / manifest_name).read_bytes())
        for item in manifest['inputs']:
            safe_relative(item['path']);safe_relative(item['harness_path'])
            source, target = committed / item['path'], harness / item['harness_path']
            if source.is_symlink() or not source.is_file():
                raise ValueError('owned committed input missing/link')
            raw = source.read_bytes()
            if len(raw) != item['bytes'] or hashlib.sha256(raw).hexdigest() != item['sha256']:
                raise ValueError('owned manifest source mismatch')
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and target.read_bytes() != raw:
                raise ValueError('owned relocation collision')
            if not target.exists():target.write_bytes(raw)
        shutil.copy2(committed / 'docs' / manifest_name, harness / 'docs' / manifest_name)
        parent_name = 'EQ083_' + name + ('_PROBE_CORRECTIVE.txt' if name == 'REMAINING' else '_PROBE.txt')
        shutil.copy2(committed / 'docs' / parent_name, harness / 'docs' / parent_name)
        groups.append(dict(name=name, parent='docs/' + parent_name, timeout_seconds=180))
    helper = 'docs/EQ083_CERTIFICATE_HELPER.txt'
    shutil.copy2(committed / helper, harness / helper)
    assert not (harness / 'packages').exists()
    inputs = {file.relative_to(harness).as_posix(): dict(bytes=file.stat().st_size, sha256=sha(file))
              for file in harness.rglob('*') if file.is_file()}
    inventory = dict(source_inputs=inputs, case_groups=groups, certificate_helper=helper,
                     certificate_loader='resilience/certificate_pins.py', package_source_inputs={})
    for project in ('client', 'mcp', 'service', 'workers'):
        namespace = 'equity_feature_' + project
        source = committed / 'packages' / project / 'src'
        inventory['package_source_inputs'][namespace] = {
            path.relative_to(source).as_posix(): dict(bytes=path.stat().st_size, sha256=sha(path))
            for path in (source / namespace).rglob('*') if path.is_file()}
    (harness / 'resilience/source-inventory.json').write_text(json.dumps(inventory, sort_keys=True, indent=2) + '\n',
                                                           encoding='utf-8', newline='\n')
    return harness


def qualify(form, archives, dependencies, reference, bootstrap, committed, output):
    location = Path(tempfile.mkdtemp(prefix='resilience-owned-' + form + '-', dir=ROOT / 'work'))
    private_directory(location, secure=True)
    run(sys.executable, '-m', 'venv', location / 'venv')
    py = location / 'venv' / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
    def install(values):
        run(py, '-m', 'pip', 'install', '--no-index', '--no-deps', '--no-build-isolation', *values, cwd=location)
    install(bootstrap)
    light_names = ('jsonschema-', 'attrs-', 'jsonschema_specifications-', 'referencing-', 'rpds_py-', 'typing_extensions-')
    install([path for path in reference if path.name.startswith(light_names)])
    install([path for name, path in archives.items() if name in ('client', 'mcp')])
    harness = prepare_harness(committed, location)
    run(py, '-I', '-c', "import sys,importlib.util;from pathlib import Path;import equity_feature_client as c;import equity_feature_mcp as m;assert all('site-packages' in Path(v.__file__).resolve().parts for v in (c,m));assert not c.native_available();assert all(importlib.util.find_spec(n) is None for n in ('equity_feature_contracts','equity_feature_io_sdk','equity_features','equity_feature_service','equity_feature_workers','duckdb','mcp'));assert c.RemoteClient('http://127.0.0.1:9',lambda:'A'*43).discover(request_id='owned').failure.code=='disconnected';assert not any(n.startswith(('equity_feature_contracts','equity_feature_io','equity_features','equity_feature_service','equity_feature_workers','mcp.')) for n in sys.modules)", cwd=location)
    run(py, '-m', 'pip', 'check', cwd=location)
    # Real bounded synthetic peer and product stdio dialogue before any native/reference installation.
    run(py, '-I', '-m', 'unittest', 'discover', '-s', harness / 'tests/client', '-p', 'test_client.py', '-v', cwd=location)
    run(py, '-I', '-m', 'unittest', 'discover', '-s', harness / 'tests/mcp', '-p', 'test_transport_fixtures.py', '-v', cwd=location)
    core = [path for path in dependencies if path.name.startswith(('equity_feature_contracts-', 'equity_features-'))]
    install(core);before = fingerprint(py, location)
    install([path for path in dependencies if path not in core]);install(reference)
    install([archives['service']])
    run(py, '-m', 'pip', 'check', cwd=location)
    for name in ('service', 'client', 'mcp'):
        run(py, '-I', '-m', 'unittest', 'discover', '-s', harness / 'tests' / name, '-v', cwd=location)
        run(py, '-I', '-m', 'mypy', '--strict', '-p', 'equity_feature_' + name, cwd=location)
    consumer = location / 'consumer.py'
    consumer.write_text('from equity_feature_client import RemoteClient\nfrom equity_feature_mcp import MCPProfile, StdioServer\nfrom equity_feature_service import Scope, Limits\nclient = RemoteClient("http://127.0.0.1:9", lambda: "A" * 43)\nprofile = MCPProfile("owned", (), (), 60)\nserver = StdioServer(client, profile)\nscope = Scope("A", "S", 1, 2)\nlimits = Limits(8, 1024, 2048, 60000000000)\n', encoding='utf-8')
    run(py, '-I', '-m', 'mypy', '--strict', consumer, cwd=location)
    consumer.write_text('from equity_feature_mcp import MCPProfile\nfrom equity_feature_service import Scope\nprofile = MCPProfile("owned", [], (), "60")\nscope = Scope("A", "S", "bad", 2)\n', encoding='utf-8')
    negative = subprocess.run([str(py), '-I', '-m', 'mypy', '--strict', str(consumer)], cwd=location,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if negative.returncode != 1 or b'[arg-type]' not in negative.stdout:
        raise ValueError('owned negative typing consumer not rejected')
    runtime = harness / 'runtime-report.json'
    run(py, '-I', harness / 'resilience/run_qualification.py', harness, runtime, cwd=location)
    assert fingerprint(py, location) == before
    runtime_sha = sha(runtime)
    report_bytes = runtime.read_bytes()
    # Private keys/logs/venvs never enter the uploaded artifact tree. Only the newly owned root is removed.
    work_root = (ROOT / 'work').resolve()
    if (location.is_symlink() or not location.resolve().is_relative_to(work_root)
            or location.parent.resolve() != work_root or not location.name.startswith('resilience-owned-')):
        raise ValueError('owned private cleanup root mismatch')
    shutil.rmtree(location)
    if location.exists():raise ValueError('owned private cleanup incomplete')
    (output / (form + '-runtime-report.json')).write_bytes(report_bytes)
    return dict(form=form, light_optional_absence=True, installed_parent_child=True,
                pipcheck=True, strict_package_typing=True, consumer_positive_negative=True,
                core_fingerprint_sha256=hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest(),
                runtime_report_sha256=runtime_sha, owned_private_cleanup=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--core-root', type=Path, required=True)
    parser.add_argument('--io-root', type=Path, required=True)
    parser.add_argument('--outdir', type=Path, default=ROOT / 'dist/resilience')
    args = parser.parse_args()
    commit = git(ROOT, 'rev-parse', 'HEAD')
    assert not git(ROOT, 'status', '--porcelain', '--', *INPUTS), 'Committed source closure required.'
    output = args.outdir.resolve();assert not output.exists(), 'Fresh output; preserve prior failures.'
    output.mkdir(parents=True);(ROOT / 'work').mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='resilience-build-', dir=ROOT / 'work') as directory:
        temp = Path(directory);committed = snapshot(ROOT, commit, temp / 'component', INPUTS)
        core = snapshot(args.core_root.resolve(), CORE_COMMIT, temp / 'core')
        io = snapshot(args.io_root.resolve(), IO_COMMIT, temp / 'io', ('packages', 'examples/third_party'))
        accepted = snapshot(ROOT, ACCEPTED_WORKER_COMMIT, temp / 'accepted', ('packages',))
        assert provenance(committed / 'packages') == provenance(accepted / 'packages'), 'Accepted package source drift'
        assert (ROOT / 'tools/build_resilience.py').read_bytes() == (committed / 'tools/build_resilience.py').read_bytes()
        for repository, paths in ((core, ('packages/contracts', 'packages/features')),
                (io, ('packages/io-contracts', 'packages/io-sdk', 'packages/duckdb', 'examples/third_party')),
                (committed, ('packages/workers',))):
            for relative in paths:build(repository / relative, temp / 'dependencies')
        dependencies = sorted((temp / 'dependencies').glob('*.whl'));assert len(dependencies) == 7
        reference_dir = temp / 'reference';reference_dir.mkdir()
        run(sys.executable, '-m', 'pip', 'download', '--only-binary=:all:', '--no-deps',
            '-r', committed / 'tools/requirements-mcp-reference.txt', '-d', reference_dir)
        reference = sorted(reference_dir.glob('*.whl'));assert len(reference) == (33 if os.name == 'nt' else 31)
        bootstrap_dir = temp / 'bootstrap';bootstrap_dir.mkdir()
        run(sys.executable, '-m', 'pip', 'download', '--only-binary=:all:', '--no-deps', *BOOTSTRAP, '-d', bootstrap_dir)
        bootstrap = sorted(bootstrap_dir.glob('*.whl'));assert len(bootstrap) == 4
        products = {}
        for name in PROJECTS:
            repeat = snapshot(ROOT, commit, temp / ('repeat-' + name), ('packages/' + name,))
            first, second = temp / ('first-' + name), temp / ('second-' + name)
            build(committed / 'packages' / name, first);build(repeat / 'packages' / name, second)
            artifacts = sorted(first.iterdir());assert len(artifacts) == 2
            assert {path.name: sha(path) for path in artifacts} == {path.name: sha(path) for path in second.iterdir()}
            for path in artifacts:
                if name == 'service':audit_service(path, repeat / 'packages/service/src/equity_feature_service')
                elif name == 'client':audit_client(path, repeat / 'packages/client')
                else:audit_mcp(path, repeat / 'packages/mcp')
                if path.suffix == '.whl':wheel_archive_record(path)
                shutil.copy2(path, output / path.name)
            products[name] = {('wheel' if path.suffix == '.whl' else 'sdist'): path for path in artifacts}
        for path in dependencies + reference + bootstrap:
            wheel_archive_record(path);shutil.copy2(path, output / path.name)
        forms = [qualify(form, {name: paths[form] for name, paths in products.items()}, dependencies,
                         reference, bootstrap, committed, output) for form in ('wheel', 'sdist')]
        receipt = dict(schema='eq083.resilience.v1', commit=commit, core_commit=CORE_COMMIT, io_commit=IO_COMMIT,
                       system=platform.system(), machine=platform.machine(), python=platform.python_version(),
                       source_inputs=provenance(committed), forms=forms,
                       core_source_inputs=provenance(core), io_source_inputs=provenance(io),
                       accepted_worker_commit=ACCEPTED_WORKER_COMMIT,
                       archive_entries={path.name: archive_entries(path) for path in output.iterdir()
                                        if path.name.endswith(('.whl', '.tar.gz'))},
                       published_files={path.name: sha(path) for path in output.iterdir() if path.is_file()},
                       package_source_unchanged=True, guarantees='operator-controlled cooperative service qualification')
        (output / 'resilience-receipt.json').write_text(json.dumps(receipt, sort_keys=True, indent=2) + '\n', encoding='utf-8', newline='\n')
    print('EQ083 repeat packages/fresh wheel-sdist light-native forms qualified')


if __name__ == '__main__':
    main()
