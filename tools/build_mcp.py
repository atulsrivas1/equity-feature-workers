"""Committed bounded MCP archives and fresh isolated reference/native qualification."""
import argparse,base64,csv,hashlib,io,json,os,platform,shutil,subprocess,sys,tempfile,tarfile,tomllib,zipfile
from pathlib import Path,PurePosixPath,PureWindowsPath
from build_foundation import build,fingerprint,git,run,sha,snapshot,CORE_COMMIT,IO_COMMIT
from build_client import wheel_record,provenance,WORKER_COMMIT,SERVICE_COMMIT
ROOT=Path(__file__).resolve().parents[1]
CLIENT_COMMIT='4365fe90aa5125662b222782873bf106bd49b778'
INPUTS=('packages/mcp','tests/mcp','tests/qualification/test_mcp_records.py','tools','docs/EQ082_PRODUCT_REFERENCE_ENTRY.txt','docs/EQ082_PRODUCT_REFERENCE_PROBE.txt','docs/EQ082_NATIVE_FAMILY_ENTRY.txt','.github/workflows/mcp.yml')
CLIENT_INPUTS=('packages/client','tests/client','docs/EQ081_CLIENT_FIXTURES.json','docs/EQ081_OWNED_HTTP_ENTRY.txt','docs/EQ081_AUTHORITY_HTTP_ENTRY.txt')

def reference_record(path):
    """Official wheel archives may include directory entries, which are not files."""
    with zipfile.ZipFile(path) as archive:
        entries=archive.infolist()
        assert len({item.filename for item in entries})==len(entries)
        for item in entries:
            name=item.filename[:-1] if item.is_dir() else item.filename
            posix=PurePosixPath(name);windows=PureWindowsPath(name)
            assert name and '\\' not in name and ':' not in name
            assert not posix.is_absolute() and not windows.drive and not windows.root
            assert '..' not in posix.parts and '/'.join(posix.parts)==name
        names={item.filename for item in entries if not item.is_dir()}
        records=[n for n in names if n.endswith('.dist-info/RECORD')];assert len(records)==1
        record=records[0]
        rows=list(csv.reader(io.StringIO(archive.read(record).decode('utf-8'))))
        assert len({r[0] for r in rows})==len(rows) and {r[0] for r in rows}==names
        for name,digest,size in rows:
            if name==record:assert digest==size==''
            else:
                data=archive.read(name)
                assert digest=='sha256='+base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode('ascii')
                assert size==str(len(data))
def audit_mcp(artifact, source):
    if artifact.suffix == '.whl':
        with zipfile.ZipFile(artifact) as archive:
            files = {n: archive.read(n) for n in archive.namelist() if not n.endswith('/')}
        prefix = ''
        allowed = ('equity_feature_mcp/', 'equity_feature_mcp-')
        metadata = next(v for n, v in files.items() if n.endswith('/METADATA'))
    else:
        with tarfile.open(artifact) as archive:
            assert not any(m.issym() or m.islnk() for m in archive.getmembers())
            files = {m.name: archive.extractfile(m).read() for m in archive.getmembers() if m.isfile()}
        prefix = artifact.name.removesuffix('.tar.gz') + '/src/'
        root = artifact.name.removesuffix('.tar.gz') + '/'
        allowed = (prefix + 'equity_feature_mcp/', prefix + 'equity_feature_mcp.egg-info/')
        extras = {root + p for p in ('LICENSE', 'README.md', 'PKG-INFO', 'pyproject.toml', 'setup.cfg')}
        assert all(n.startswith(allowed) or n in extras for n in files)
        metadata = files[root + 'PKG-INFO']
    if artifact.suffix == '.whl':
        assert all(n.startswith(allowed) for n in files)
    for path in (source / 'src/equity_feature_mcp').rglob('*'):
        if path.is_file():
            assert files[prefix + path.relative_to(source / 'src').as_posix()] == path.read_bytes()
    assert prefix + 'equity_feature_mcp/py.typed' in files
    assert b'License-Expression: Apache-2.0' in metadata and b'Version: 0.1.0a0' in metadata
    project = tomllib.loads((source / 'pyproject.toml').read_text(encoding='utf-8'))['project']
    requires = sorted(line[len('Requires-Dist: '):].strip() for line in metadata.decode().splitlines() if line.startswith('Requires-Dist: '))
    expected = project['dependencies']
    assert requires == sorted(expected), requires
    assert any(n.endswith('/licenses/LICENSE') if artifact.suffix == '.whl' else n.endswith('/LICENSE') for n in files)
    for name, data in files.items():
        assert '..' not in Path(name).parts and not name.startswith(('/', '\\'))
        assert '__pycache__' not in name and not name.endswith(('.pyc', 'entry_points.txt'))
        assert b'C:\\Users\\' not in data and b'/home/runner/' not in data


def qualify(artifact,deps,reference,committed,client,service):
    with tempfile.TemporaryDirectory(prefix='mcp-install-',dir=ROOT/'work') as tmp:
        location=Path(tmp)
        run(sys.executable,'-m','venv',location/'venv')
        py=location/'venv'/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
        run(py,'-m','pip','install','--no-deps','setuptools==80.9.0','mypy==1.15.0','mypy_extensions==1.1.0','-r',committed/'tools/requirements-service.txt')
        client_wheel=next(p for p in deps if p.name.startswith('equity_feature_client-'))
        run(py,'-m','pip','install','--no-index','--no-deps','--no-build-isolation',client_wheel,artifact)
        run(py,'-I','-c',"import sys,importlib.util;from pathlib import Path;import equity_feature_client as c;import equity_feature_mcp as m;assert all('site-packages' in Path(v.__file__).resolve().parts for v in (c,m));assert not c.native_available();assert all(importlib.util.find_spec(n) is None for n in ('equity_feature_contracts','equity_feature_io_sdk','equity_features','equity_feature_service','equity_feature_workers','duckdb','mcp'));assert c.RemoteClient('http://127.0.0.1:9',lambda:'A'*43).discover(request_id='owned').failure.code=='disconnected';assert not any(n.startswith(('equity_feature_contracts','equity_feature_io','equity_features','equity_feature_service','equity_feature_workers','mcp.')) for n in sys.modules)",cwd=location)
        run(py,'-m','pip','check')
        core=[p for p in deps if p.name.startswith(('equity_feature_contracts-','equity_features-'))]
        run(py,'-m','pip','install','--no-index','--no-deps',*core)
        before=fingerprint(py,location)
        run(py,'-m','pip','install','--no-index','--no-deps',*[p for p in deps if p not in core])
        run(py,'-m','pip','install','--no-index','--no-deps','--force-reinstall',*reference)
        harness=location/'harness'
        for source,target in ((committed/'tests/mcp','tests/mcp'),(client/'tests/client','tests/client'),(service/'tests/service','tests/service')):
            shutil.copytree(source,harness/target)
        (harness/'docs').mkdir()
        for name in ('EQ081_CLIENT_FIXTURES.json','EQ081_OWNED_HTTP_ENTRY.txt','EQ081_AUTHORITY_HTTP_ENTRY.txt'):
            shutil.copy2(client/'docs'/name,harness/'docs'/name)
        for name in ('EQ082_PRODUCT_REFERENCE_ENTRY.txt','EQ082_PRODUCT_REFERENCE_PROBE.txt','EQ082_NATIVE_FAMILY_ENTRY.txt'):
            shutil.copy2(committed/'docs'/name,harness/'docs'/name)
        assert not (harness/'packages').exists()
        run(py,'-I','-c',"from pathlib import Path;import equity_feature_client as c;import equity_feature_mcp as m;assert all('site-packages' in Path(v.__file__).resolve().parts for v in (c,m));assert c.native_available()",cwd=location)
        run(py,'-I','-m','unittest','discover','-s',harness/'tests/mcp','-v',cwd=location)
        entry_hash=location/'entry-sha.txt';entry_hash.write_text(sha(harness/'docs/EQ082_PRODUCT_REFERENCE_ENTRY.txt'),encoding='ascii')
        run(py,'-I',harness/'docs/EQ082_PRODUCT_REFERENCE_PROBE.txt',harness,entry_hash,cwd=location)
        run(py,'-I','-m','mypy','--strict','-p','equity_feature_mcp',cwd=location)
        consumer=location/'consumer.py'
        consumer.write_text('from equity_feature_client import RemoteClient, ProducerExpectation\nfrom equity_feature_mcp import StdioServer, MCPProfile, CommandRegistration\ndef create(c: RemoteClient, e: ProducerExpectation) -> StdioServer:\n    return StdioServer(c, MCPProfile("owned", (), (CommandRegistration("trade", e),), 60))\n',encoding='utf-8')
        run(py,'-I','-m','mypy','--strict',consumer,cwd=location)
        consumer.write_text('from equity_feature_mcp import MCPProfile, StdioServer\np = MCPProfile("owned", [], (), "60")\ns = StdioServer(123, p)\n',encoding='utf-8')
        rejected=subprocess.run([str(py),'-I','-m','mypy','--strict',str(consumer)],cwd=location,capture_output=True,text=True)
        assert rejected.returncode==1 and '[arg-type]' in rejected.stdout
        run(py,'-m','pip','check')
        assert fingerprint(py,location)==before
        installed=json.loads(subprocess.check_output([str(py),'-m','pip','list','--format=json'],encoding='utf-8'))
        return dict(form='wheel' if artifact.suffix=='.whl' else 'sdist',light_optional_absence=True,
                    parent_child_installed_without_source_shadow=True,native_tests=True,reference_protocol='2025-11-25',
                    strict_typing=True,consumer_positive_negative=True,installed_distributions=installed,
                    core_before_after_sha256=hashlib.sha256(json.dumps(before,sort_keys=True).encode()).hexdigest())

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--core-root',type=Path,required=True);parser.add_argument('--io-root',type=Path,required=True)
    parser.add_argument('--outdir',type=Path,default=ROOT/'dist/mcp');args=parser.parse_args()
    commit=git(ROOT,'rev-parse','HEAD')
    assert not git(ROOT,'status','--porcelain','--',*INPUTS),'Qualification inputs must be committed and clean.'
    (ROOT/'work').mkdir(exist_ok=True)
    output=args.outdir.resolve();assert not output.exists(),'Use a fresh output; preserve evidence.'
    output.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix='mcp-build-',dir=ROOT/'work') as tmp:
        temp=Path(tmp);committed=snapshot(ROOT,commit,temp/'component',INPUTS)
        for name in ('build_mcp.py','build_client.py','build_foundation.py'):
            assert (ROOT/'tools'/name).read_bytes()==(committed/'tools'/name).read_bytes()
        run(sys.executable,'-m','unittest','discover','-s',committed/'tests/qualification','-p','test_mcp_records.py','-v')
        client=snapshot(ROOT,CLIENT_COMMIT,temp/'client',CLIENT_INPUTS)
        service=snapshot(ROOT,SERVICE_COMMIT,temp/'service',('packages/service','tests/service'))
        workers=snapshot(ROOT,WORKER_COMMIT,temp/'workers',('packages/workers',))
        core=snapshot(args.core_root.resolve(),CORE_COMMIT,temp/'core')
        io_source=snapshot(args.io_root.resolve(),IO_COMMIT,temp/'io',('packages','examples/third_party'))
        for repository,paths in ((core,('packages/contracts','packages/features')),
                (io_source,('packages/io-contracts','packages/io-sdk','packages/duckdb','examples/third_party')),
                (workers,('packages/workers',)),(service,('packages/service',)),(client,('packages/client',))):
            for relative in paths:build(repository/relative,temp/'dependencies')
        deps=sorted((temp/'dependencies').glob('*.whl'));assert len(deps)==9
        reference_dir=temp/'reference';reference_dir.mkdir()
        run(sys.executable,'-m','pip','download','--only-binary=:all:','--no-deps','-r',committed/'tools/requirements-mcp-reference.txt','-d',reference_dir)
        reference=sorted(reference_dir.glob('*.whl'))
        # All resolver inputs are pinned; no online installation or dependency fallback in fresh full forms.
        assert len(reference)==(33 if os.name=='nt' else 31),len(reference)
        repeat=snapshot(ROOT,commit,temp/'repeat',('packages/mcp',))
        build(committed/'packages/mcp',temp/'first');build(repeat/'packages/mcp',temp/'second')
        first={p.name:sha(p) for p in (temp/'first').iterdir()}
        assert len(first)==2 and first=={p.name:sha(p) for p in (temp/'second').iterdir()}
        forms=[]
        for artifact in sorted((temp/'first').iterdir()):
            audit_mcp(artifact,repeat/'packages/mcp')
            if artifact.suffix=='.whl':wheel_record(artifact)
            forms.append(qualify(artifact,deps,reference,committed,client,service));shutil.copy2(artifact,output/artifact.name)
        for artifact in deps:
            wheel_record(artifact);shutil.copy2(artifact,output/artifact.name)
        for artifact in reference:
            reference_record(artifact);shutil.copy2(artifact,output/artifact.name)
        receipt=dict(schema='mcp1',commit=commit,core_commit=CORE_COMMIT,io_commit=IO_COMMIT,worker_commit=WORKER_COMMIT,
            service_commit=SERVICE_COMMIT,client_commit=CLIENT_COMMIT,source_inputs=provenance(committed),
            client_source_inputs=provenance(client),service_source_inputs=provenance(service),worker_source_inputs=provenance(workers),
            example_source_inputs=provenance(io_source/'examples/third_party'),repeat_artifacts=first,
            dependencies={p.name:sha(p) for p in deps},reference_dependencies={p.name:sha(p) for p in reference},forms=forms,
            python=platform.python_version(),system=platform.system(),machine=platform.machine(),epoch=1700000000)
        (output/'mcp-receipt.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n',encoding='utf-8',newline='\n')
    print('MCP repeat archives, fresh light/reference-native forms, installed parent/child and core invariance PASS')

if __name__=='__main__':main()
