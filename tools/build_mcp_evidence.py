"""UNEXECUTED EQ108 committed-source four-form builder; exact review before run."""
import argparse,configparser,hashlib,importlib.util,io,json,os,platform,shutil,subprocess,sys,tempfile
from pathlib import Path
from build_foundation import build,fingerprint,git,run,snapshot,PINS_CORE as CORE_COMMIT, PINS_IO as IO_COMMIT
from build_client import wheel_record,provenance,WORKER_COMMIT,SERVICE_COMMIT
from build_mcp import reference_record,CLIENT_COMMIT,CLIENT_INPUTS
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"tools"))
from _versions import VERSIONS
OPTIONAL_CORE='98778e79f9422904bbb043d067a7ce9e77675311'
INPUTS=('packages/mcp','tests/mcp','tests/mcp_evidence','tests/qualification/test_mcp_records.py','tools','examples/mcp_evidence.py',
    'docs/EQ082_PRODUCT_REFERENCE_ENTRY.txt','docs/EQ108_LEGACY_REFERENCE_PROBE.txt','docs/EQ082_NATIVE_FAMILY_ENTRY.txt','docs/EQ082_NATIVE_STATE_ENTRY.txt',
    'docs/EQ108_EVIDENCE_REFERENCE_ENTRY.txt','docs/EQ108_EVIDENCE_REFERENCE_PROBE.txt','docs/EQ083_REFERENCE_PROCESS.txt','docs/EQ083_LINUX_REFERENCE_CHILD.txt',
    'docs/stories/EQ-108_ORACLES.json','docs/stories/EQ-108_ADMISSION.json','docs/stories/EQ-108_TOOL_SCHEMAS.json','docs/stories/EQ-108_WIRE_BOUNDS.json')
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def load_core_module(name,path):
    """Load a core tool by path with core's own `_versions` (this repository's `_versions` has other keys and would shadow it)."""
    spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec)
    saved=sys.modules.pop('_versions',None);sys.path.insert(0,str(Path(path).resolve().parent))
    previous=sys.dont_write_bytecode;sys.dont_write_bytecode=True
    try:spec.loader.exec_module(value)
    finally:
        sys.dont_write_bytecode=previous;sys.path.remove(str(Path(path).resolve().parent));sys.modules.pop('_versions',None)
        if saved is not None:sys.modules['_versions']=saved
    return value

def generated_config(source,sdist,auditor):
    """Bind the tracked input and complete platform-rendered generated metadata."""
    raw=(source/'setup.cfg').read_bytes()
    assert raw==b'[egg_info]\ntag_build =\ntag_date = 0\n'
    parser=configparser.ConfigParser(interpolation=None)
    parser.read_string(raw.decode('ascii'))
    assert parser.sections()==['egg_info'] and dict(parser['egg_info'])==dict(tag_build='',tag_date='0')
    rendered=io.StringIO();parser.write(rendered)
    expected=rendered.getvalue().replace('\n',os.linesep).encode('ascii')
    files=auditor.contents(sdist);prefix=sdist.name.removesuffix('.tar.gz')+'/'
    actual=files[prefix+'setup.cfg'];sources=files[prefix+'src/equity_feature_mcp.egg-info/SOURCES.txt']
    assert actual==expected and sources.decode('utf-8').splitlines().count('setup.cfg')==1
    return dict(tracked_setup_sha256=hashlib.sha256(raw).hexdigest(),generated_setup_sha256=hashlib.sha256(actual).hexdigest(),
        generated_sources_sha256=hashlib.sha256(sources).hexdigest(),generated_setup_bytes=len(actual),setup_listed_once=True)

def logged(py,args,location,output,label):
    path=output/(label+'.log');assert not path.exists()
    env=dict(os.environ)
    for key in ('PYTHONPATH','PYTHONHOME','MYPYPATH'):env.pop(key,None)
    with path.open('wb') as stream:
        result=subprocess.run([str(py),'-I',*[str(v) for v in args]],cwd=location,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=240)
    if result.returncode:
        print(path.read_text(encoding='utf-8')[-32768:],flush=True)
    assert result.returncode==0,'retained failed log '+path.name
    return dict(log=path.name,sha256=sha(path))

def closure(py,location):
    source='''import base64,csv,hashlib,io,json,sys
from pathlib import Path
from importlib.metadata import distributions
base=Path(sys.prefix).resolve();out={}
for dist in distributions():
 names=tuple(dist.files or ())
 record=tuple(n for n in names if len(n.parts)==2 and n.parts[0].endswith('.dist-info') and n.parts[1]=='RECORD')
 assert len(record)==1
 record_name=record[0].as_posix()
 record_path=Path(dist.locate_file(record[0])).resolve()
 assert record_path.is_relative_to(base) and record_path.is_file()
 rows=list(csv.reader(io.StringIO(record_path.read_text(encoding='utf-8'))))
 assert all(len(row)==3 for row in rows)
 assert len(rows)==len({row[0] for row in rows}) and {row[0] for row in rows}=={n.as_posix() for n in names}
 assert record_name in {row[0] for row in rows}
 files={}
 for name,digest,size in rows:
  path=Path(dist.locate_file(name)).resolve()
  assert path.is_relative_to(base) and path.is_file()
  if name==record_name:assert digest==size==''
  elif '__pycache__' in path.parts and path.suffix=='.pyc' and digest==size=='':continue
  else:
   data=path.read_bytes()
   assert digest=='sha256='+base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode('ascii')
   assert size==str(len(data))
  if '__pycache__' in path.parts and path.suffix=='.pyc':continue
  files[path.relative_to(base).as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
 assert dist.metadata['Name'] not in out
 out[dist.metadata['Name']]={'version':dist.version,'files':files}
print(json.dumps(out,sort_keys=True))
'''
    return json.loads(subprocess.check_output([str(py),'-I','-c',source],cwd=location,encoding='utf-8'))

def installed_matches_archives(mapping,archives,auditor):
    """Compare every original selected wheel file except pip-rewritten RECORD."""
    actual={}
    for distribution in mapping.values():
        for name,digest in distribution['files'].items():
            if '/site-packages/' in name:
                relative=name.split('/site-packages/',1)[1]
                assert relative not in actual or actual[relative]==digest
                actual[relative]=digest
    for archive in archives:
        assert archive.suffix=='.whl'
        files=auditor.contents(archive)
        records=tuple(name for name in files if name.count('/')==1 and name.split('/')[0].endswith('.dist-info') and name.endswith('/RECORD'))
        assert len(records)==1
        for name,data in files.items():
            if name==records[0]:continue
            assert '.data/' not in name,'unselected relocated wheel payload'
            assert actual.get(name)==hashlib.sha256(data).hexdigest(),name

def qualify(artifact,deps,extras,reference,committed,client,service,auditor,output,enabled):
    form=('wheel' if artifact.suffix=='.whl' else 'sdist')+('-evidence' if enabled else '-default')
    # Retain failed work roots; remove only after a complete successful form.
    location=Path(tempfile.mkdtemp(prefix='eq108-'+form+'-',dir=ROOT/'work'))
    run(sys.executable,'-m','venv',location/'venv')
    py=location/'venv'/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
    run(py,'-m','pip','install','--no-deps','setuptools==80.9.0','mypy==1.15.0','mypy_extensions==1.1.0','-r',committed/'tools/requirements-service.txt')
    client_wheel=next(p for p in deps if p.name.startswith('equity_feature_client-'))
    run(py,'-m','pip','install','--no-index','--no-deps','--no-build-isolation',client_wheel,artifact)
    logs=[]
    base="import sys,importlib.util;from pathlib import Path;import equity_feature_client as c;import equity_feature_mcp as m;import equity_feature_mcp.evidence;assert all('site-packages' in Path(v.__file__).resolve().parts for v in (c,m));assert not c.native_available();assert all(importlib.util.find_spec(n) is None for n in ('equity_feature_contracts','equity_feature_io_sdk','equity_features','equity_feature_evidence','equity_feature_explanations','equity_feature_diagnostics','equity_feature_service','equity_feature_workers','mcp'));assert not any(n.startswith(('equity_feature_contracts','equity_feature_evidence','equity_feature_explanations','equity_feature_diagnostics')) for n in sys.modules)"
    logs.append(logged(py,['-c',base],location,output,form+'-base-absence'))
    run(py,'-m','pip','check')
    core=[p for p in deps if p.name.startswith(('equity_feature_contracts-','equity_features-'))]
    run(py,'-m','pip','install','--no-index','--no-deps',*core)
    before=fingerprint(py,location)
    run(py,'-m','pip','install','--no-index','--no-deps',*[p for p in deps if p not in core],*(extras if enabled else []),*reference)
    harness=location/'harness'
    for source,target in ((committed/'tests/mcp','tests/mcp'),(client/'tests/client','tests/client'),(service/'tests/service','tests/service')):
        shutil.copytree(source,harness/target)
    (harness/'docs').mkdir()
    for name in ('EQ081_CLIENT_FIXTURES.json','EQ081_OWNED_HTTP_ENTRY.txt','EQ081_AUTHORITY_HTTP_ENTRY.txt'):
        shutil.copy2(client/'docs'/name,harness/'docs'/name)
    for name in ('EQ082_PRODUCT_REFERENCE_ENTRY.txt','EQ108_LEGACY_REFERENCE_PROBE.txt','EQ082_NATIVE_FAMILY_ENTRY.txt','EQ082_NATIVE_STATE_ENTRY.txt'):
        shutil.copy2(committed/'docs'/name,harness/'docs'/name)
    assert not (harness/'packages').exists()
    logs.append(logged(py,['-m','unittest','discover','-s',harness/'tests/mcp','-v'],location,output,form+'-legacy-methods'))
    entry_hash=location/'entry-sha.txt';entry_hash.write_text(sha(harness/'docs/EQ082_PRODUCT_REFERENCE_ENTRY.txt'),encoding='ascii')
    logs.append(logged(py,[harness/'docs/EQ108_LEGACY_REFERENCE_PROBE.txt',harness,entry_hash],location,output,form+'-legacy-SDK-native'))
    if enabled:
        shutil.copytree(committed/'tests/mcp_evidence',harness/'tests/mcp_evidence')
        shutil.copytree(committed/'docs/stories',harness/'docs/stories')
        for name in ('EQ108_EVIDENCE_REFERENCE_ENTRY.txt','EQ108_EVIDENCE_REFERENCE_PROBE.txt','EQ083_REFERENCE_PROCESS.txt','EQ083_LINUX_REFERENCE_CHILD.txt'):
            shutil.copy2(committed/'docs'/name,harness/'docs'/name)
        pin_names=['docs/'+n for n in ('EQ108_EVIDENCE_REFERENCE_ENTRY.txt','EQ108_EVIDENCE_REFERENCE_PROBE.txt','EQ083_REFERENCE_PROCESS.txt','EQ083_LINUX_REFERENCE_CHILD.txt','EQ081_CLIENT_FIXTURES.json','EQ081_AUTHORITY_HTTP_ENTRY.txt')]
        pin_names+=['tests/client/test_authority_http.py','tests/client/test_client.py','tests/service/test_jobs.py','tests/service/fixtures/jobs_native.json','tests/mcp_evidence/expected_reports.py']
        pins=dict(source_map={n:sha(harness/n) for n in pin_names},config_sha256=sha(harness/'tests/service/fixtures/jobs_native.json'))
        pin_file=location/'eq108-pins.json';pin_file.write_text(json.dumps(pins,sort_keys=True)+'\n',encoding='utf-8')
        logs.append(logged(py,['-m','unittest','discover','-s',harness/'tests/mcp_evidence','-v'],location,output,form+'-evidence-methods'))
        logs.append(logged(py,[harness/'docs/EQ108_EVIDENCE_REFERENCE_PROBE.txt',harness,pin_file],location,output,form+'-evidence-SDK-native'))
        example=location/'examples/mcp_evidence.py';example.parent.mkdir();shutil.copy2(committed/'examples/mcp_evidence.py',example)
        # The example reads only these two fixed synthetic assets under location.
        (location/'docs').mkdir();shutil.copy2(harness/'docs/EQ081_CLIENT_FIXTURES.json',location/'docs/EQ081_CLIENT_FIXTURES.json')
        (location/'tests/service/fixtures').mkdir(parents=True);shutil.copy2(harness/'tests/service/fixtures/jobs_native.json',location/'tests/service/fixtures/jobs_native.json')
        logs.append(logged(py,[example],location,output,form+'-example'))
    else:
        logs.append(logged(py,['-c',"import importlib.util;assert all(importlib.util.find_spec(n) is None for n in ('equity_feature_evidence','equity_feature_explanations','equity_feature_diagnostics'))"],location,output,form+'-optional-remains-absent'))
    logs.append(logged(py,['-m','mypy','--strict','-p','equity_feature_mcp'],location,output,form+'-typing'))
    consumer=location/'consumer.py'
    consumer.write_text('from equity_feature_client import RemoteClient, ProducerExpectation\nfrom equity_feature_mcp import StdioServer, MCPProfile, CommandRegistration\nfrom equity_feature_mcp.evidence import EvidenceCatalog\ndef create(c: RemoteClient, e: ProducerExpectation, evidence: EvidenceCatalog | None = None) -> StdioServer:\n    return StdioServer(c, MCPProfile("owned", (), (CommandRegistration("trade", e),), 60), evidence=evidence)\n',encoding='utf-8')
    logs.append(logged(py,['-m','mypy','--strict',consumer],location,output,form+'-consumer-positive'))
    consumer.write_text('from equity_feature_mcp import MCPProfile, StdioServer\np=MCPProfile("owned", [], (), "60")\ns=StdioServer(123,p,evidence=123)\n',encoding='utf-8')
    rejected=subprocess.run([str(py),'-I','-m','mypy','--strict',str(consumer)],cwd=location,capture_output=True,text=True)
    assert rejected.returncode==1 and '[arg-type]' in rejected.stdout
    (output/(form+'-consumer-negative.log')).write_text(rejected.stdout+rejected.stderr,encoding='utf-8')
    run(py,'-m','pip','check');assert fingerprint(py,location)==before
    mapping=closure(py,location)
    mcp_wheel=next(p for p in artifact.parent.glob('*.whl') if p.name.startswith('equity_feature_mcp-'))
    installed_matches_archives(mapping,[mcp_wheel,*deps,*(extras if enabled else [])],auditor)
    map_file=output/(form+'-installed-map.json');map_file.write_text(json.dumps(mapping,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    record=dict(form=form,logs=logs,installed_map=map_file.name,installed_map_sha256=sha(map_file),base_optional_absence=True,
        enabled=enabled,parent_child_installed=True,installed_RECORD_validated=True,installed_original_wheel_files_equal=True,core_before_after_sha256=hashlib.sha256(json.dumps(before,sort_keys=True).encode()).hexdigest(),
        closure_scope='Registered installed distribution files incl native extensions; observed CPython/platform, not complete host/kernel/stdlib provenance.')
    shutil.rmtree(location)
    return record

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--core-root',type=Path,required=True);parser.add_argument('--io-root',type=Path,required=True)
    parser.add_argument('--outdir',type=Path,default=ROOT/'dist/mcp-evidence');args=parser.parse_args()
    head=git(ROOT,'rev-parse','HEAD');assert not git(ROOT,'status','--porcelain','--',*INPUTS)
    output=args.outdir.resolve();assert not output.exists();output.mkdir(parents=True);(ROOT/'work').mkdir(exist_ok=True)
    temp=Path(tempfile.mkdtemp(prefix='eq108-build-',dir=ROOT/'work'))
    committed=snapshot(ROOT,head,temp/'component',INPUTS)
    for name in ('build_mcp_evidence.py','build_mcp.py','build_client.py','build_foundation.py'):
        assert (ROOT/'tools'/name).read_bytes()==(committed/'tools'/name).read_bytes()
    client=snapshot(ROOT,CLIENT_COMMIT,temp/'client',CLIENT_INPUTS)
    service=snapshot(ROOT,SERVICE_COMMIT,temp/'service',('packages/service','tests/service'))
    workers=snapshot(ROOT,WORKER_COMMIT,temp/'workers',('packages/workers',))
    core=snapshot(args.core_root.resolve(),CORE_COMMIT,temp/'core')
    optional=snapshot(args.core_root.resolve(),OPTIONAL_CORE,temp/'optional',('packages/evidence','packages/explanations','packages/diagnostics','tools/build_agent_ledger.py','tools/_versions.py'))
    io_source=snapshot(args.io_root.resolve(),IO_COMMIT,temp/'io',('packages','examples/third_party'))
    auditor=load_core_module('accepted_archive_auditor',optional/'tools/build_agent_ledger.py')
    auditor.PACKAGES.update({'mcp':('equity_feature_mcp',VERSIONS['equity-feature-mcp']),'explanations':('equity_feature_explanations',VERSIONS['equity-feature-explanations']),'diagnostics':('equity_feature_diagnostics',VERSIONS['equity-feature-diagnostics'])})
    dependencies=temp/'dependencies';extra_dir=temp/'extra'
    for repository,paths in ((core,('packages/contracts','packages/features')),(io_source,('packages/io-contracts','packages/io-sdk','packages/duckdb','examples/third_party')),(workers,('packages/workers',)),(service,('packages/service',)),(client,('packages/client',))):
        for relative in paths:build(repository/relative,dependencies)
    for name in ('evidence','explanations','diagnostics'):build(optional/'packages'/name,extra_dir)
    deps=sorted(dependencies.glob('*.whl'));extras=sorted(extra_dir.glob('*.whl'));assert len(deps)==9 and len(extras)==3
    reference_dir=temp/'reference';reference_dir.mkdir()
    run(sys.executable,'-m','pip','download','--only-binary=:all:','--no-deps','-r',committed/'tools/requirements-mcp-reference.txt','-d',reference_dir)
    reference=sorted(reference_dir.glob('*.whl'));assert len(reference)==(33 if os.name=='nt' else 31)
    build(committed/'packages/mcp',temp/'first')
    first={p.name:sha(p) for p in (temp/'first').iterdir()};assert len(first)==2
    sdist=next((temp/'first').glob('*.tar.gz'));files=auditor.contents(sdist);prefix=sdist.name.removesuffix('.tar.gz')+'/'
    configuration=generated_config(committed/'packages/mcp',sdist,auditor)
    selected=temp/'selected-sdist';selected.mkdir()
    for name,data in files.items():
        assert name.startswith(prefix);target=selected/name.removeprefix(prefix);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
    # Full selected sdist (including generated metadata) is the repeat input.
    build(selected,temp/'repeat');assert first=={p.name:sha(p) for p in (temp/'repeat').iterdir()}
    artifacts={};forms=[]
    for artifact in sorted((temp/'first').iterdir()):
        artifacts[artifact.name]=auditor.inspect(artifact,'mcp',committed/'packages/mcp')
        for enabled in (False,True):forms.append(qualify(artifact,deps,extras,reference,committed,client,service,auditor,output,enabled))
        shutil.copy2(artifact,output/artifact.name)
    for artifact in deps+extras:wheel_record(artifact);shutil.copy2(artifact,output/artifact.name)
    for artifact in reference:reference_record(artifact);shutil.copy2(artifact,output/artifact.name)
    receipt=dict(schema='eq108.mcp.evidence.qualification.v1',commit=head,core_commit=CORE_COMMIT,optional_core_commit=OPTIONAL_CORE,io_commit=IO_COMMIT,
        client_commit=CLIENT_COMMIT,service_commit=SERVICE_COMMIT,worker_commit=WORKER_COMMIT,source_inputs=provenance(committed),client_source_inputs=provenance(client),
        service_source_inputs=provenance(service),worker_source_inputs=provenance(workers),optional_source_inputs=provenance(optional),io_source_inputs=provenance(io_source),
        repeat_artifacts=first,artifacts=artifacts,forms=forms,dependencies={p.name:sha(p) for p in deps+extras},reference_dependencies={p.name:sha(p) for p in reference},
        selected_sdist_source={n:hashlib.sha256(v).hexdigest() for n,v in files.items()},generated_configuration=configuration,python=platform.python_version(),system=platform.system(),machine=platform.machine())
    (output/'mcp-evidence-receipt.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n',encoding='utf-8',newline='\n')
    shutil.rmtree(temp)
    print('EQ108 actual four fresh default/evidence forms complete; see raw logs and scoped receipt.')

if __name__=='__main__':main()
