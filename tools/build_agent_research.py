"""UNEXECUTED EQ109 fresh installed research composition; review before running.

Build inputs are committed public sources. Every original selected SDist member is
materialized for a repeated wheel build; generated metadata is recorded separately.
This is application qualification, not complete host/stdlib/ABI provenance.
"""
from pathlib import Path
import argparse,base64,configparser,csv,hashlib,importlib.util,io,json,os,platform,re,shutil,subprocess,sys,tempfile,tomllib
from email.parser import BytesParser
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from build_foundation import git,snapshot,normalize,CORE_COMMIT
from build_client import WORKER_COMMIT,SERVICE_COMMIT
from build_mcp import CLIENT_COMMIT,CLIENT_INPUTS
from build_mcp_evidence import closure,load_core_module

ROOT=Path(__file__).resolve().parents[1]
CORE_RESEARCH='98778e79f9422904bbb043d067a7ce9e77675311'
IO_RESEARCH='94217cfafe5e54461c8dc9efdf4848809e04082a'
IO_BASE='19aa266b9debb92a6c5eb707baafeb9ae7a82942'
EPOCH='1700000000'
CORE_APPS=('examples/agent_research.py','examples/agent_research_typed.py','tests/agent_research',
           'docs/stories/EQ-109_ORACLES.json','docs/stories/EQ-109_PROFILE.json','docs/stories/EQ-109_CASES.json')
INPUTS=('packages/mcp','examples/agent_research_mcp.py','tests/agent_research','tools',
        'docs/EQ109_ACCEPTED_ARCHIVES.json',
        'docs/EQ109_RESEARCH_REFERENCE_ENTRY.txt','docs/EQ109_RESEARCH_REFERENCE_PROBE.txt',
        'docs/EQ083_REFERENCE_PROCESS.txt','docs/EQ083_LINUX_REFERENCE_CHILD.txt')
LIGHT=('contracts','evidence','explanations','diagnostics','agent-evidence','agent-ledger','bundles')
SUPPORT=('pip==25.1.1','setuptools==80.9.0','mypy==1.15.0','mypy_extensions==1.1.0','typing_extensions==4.16.0','packaging==26.3')


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def digest(raw):return hashlib.sha256(raw).hexdigest()
def files(root):return {p.relative_to(root).as_posix():sha(p) for p in sorted(root.rglob('*')) if p.is_file()}


def run(*args,cwd=ROOT,env=None):
    effective=dict(os.environ if env is None else env)
    for name in ('PYTHONPATH','PYTHONHOME','MYPYPATH'):effective.pop(name,None)
    command=[str(a) for a in args]
    assert len(command)>1
    if command[1]!='-I':command.insert(1,'-I')
    subprocess.run(command,cwd=cwd,env=effective,check=True)


def build(source,output):
    output.mkdir(parents=True,exist_ok=False)
    run(sys.executable,'-m','build','--no-isolation','--outdir',output,source,
        env=dict(os.environ,SOURCE_DATE_EPOCH=EPOCH,PYTHONHASHSEED='0'))
    for path in output.glob('*.tar.gz'):normalize(path)


def entrypoint_text(project):
    # Match the pinned setuptools80.9 renderer; received archive bytes stay exact.
    groups=dict(project.get('entry-points',{}))
    for field,group in (('scripts','console_scripts'),('gui-scripts','gui_scripts')):
        if project.get(field):groups[group]=project[field]
    groups={group:values for group,values in groups.items() if values}
    if not groups:return None
    return '\n'.join(
        '['+group+']\n'+'\n'.join(name+' = '+values[name] for name in sorted(values))+'\n'
        for group,values in sorted(groups.items())
    ).encode('utf-8')


def source_record(repository,commit,materialized,scopes):
    names=git(repository,'ls-tree','-r','--name-only',commit,'--',*scopes).splitlines()
    actual=files(materialized)
    assert set(names)==set(actual)
    # Git raw object bytes and every materialized input are both captured.
    raw={name:digest(subprocess.check_output(['git','show',commit+':'+name],cwd=repository)) for name in names}
    return dict(commit=commit,git_blobs=raw,materialized=actual,scope=list(scopes))


def raw_application(repository,commit,dest,scopes):
    names=git(repository,'ls-tree','-r','--name-only',commit,'--',*scopes).splitlines();assert names
    assert not dest.exists();dest.mkdir(parents=True)
    for name in names:
        data=subprocess.check_output(['git','show',commit+':'+name],cwd=repository)
        target=dest/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
    return source_record(repository,commit,dest,scopes)


def load_auditor(path):
    return load_core_module('eq109_archive_auditor',path)


def reference_archive(path,auditor):
    entries,members=auditor.contents(path,with_members=True)
    records=[n for n in entries if n.count('/')==1 and n.endswith('.dist-info/RECORD')]
    assert len(records)==1;record=records[0]
    rows=list(csv.reader(io.StringIO(entries[record].decode('utf-8'))))
    assert all(len(row)==3 for row in rows)
    assert len(rows)==len({r[0] for r in rows}) and {r[0] for r in rows}==set(entries)
    for name,hashed,size in rows:
        if name==record:assert hashed==size==''
        else:assert hashed=='sha256='+base64.urlsafe_b64encode(hashlib.sha256(entries[name]).digest()).rstrip(b'=').decode('ascii') and size==str(len(entries[name]))
    metadata=BytesParser().parsebytes(entries[record.removesuffix('RECORD')+'METADATA'])
    return dict(sha256=sha(path),bytes=path.stat().st_size,name=metadata['Name'],version=metadata['Version'],
        declared_requirements=metadata.get_all('Requires-Dist',[]),
        entries={n:dict(sha256=digest(data),bytes=len(data)) for n,data in entries.items()},members=members,
        scope='Downloaded exact version wheel/complete physical members/owning RECORD; upstream source-build provenance not established.')


def inspect_archive(path,source,auditor):
    """Full source payload, metadata/readme/license/dependencies and wheel RECORD."""
    project=tomllib.loads((source/'pyproject.toml').read_text(encoding='utf-8'))['project']
    module=project['name'].replace('-','_');version=project['version'];wheel=path.suffix=='.whl'
    entries,members=auditor.contents(path,with_members=True)
    prefix='' if wheel else path.name.removesuffix('.tar.gz')+'/'
    metadata_path=module+'-'+version+'.dist-info/METADATA' if wheel else prefix+'PKG-INFO'
    metadata=BytesParser().parsebytes(entries[metadata_path])
    assert metadata['Name']==project['name'] and metadata['Version']==version
    assert metadata['Summary']==project['description'] and metadata['License-Expression']==project['license']=='Apache-2.0'
    assert str(SpecifierSet(metadata['Requires-Python']))==str(SpecifierSet(project['requires-python']))
    assert metadata.get_all('License-File',[])==project['license-files']==['LICENSE']
    authors=project.get('authors',[])
    assert all(set(item)=={'name'} for item in authors)
    assert metadata['Author']==(', '.join(item['name'] for item in authors) if authors else None)
    assert sorted(metadata.get_all('Project-URL',[]))==sorted(k+', '+v for k,v in project.get('urls',{}).items())
    assert metadata.get_all('Classifier',[])==project.get('classifiers',[])
    extras=project.get('optional-dependencies',{});requires=list(project.get('dependencies',[]))
    for name,items in extras.items():
        for item in items:
            assert ';' not in item;requires.append(item+'; extra == "'+name+'"')
    assert sorted(metadata.get_all('Provides-Extra',[]))==sorted(extras)
    assert sorted(str(Requirement(x)) for x in metadata.get_all('Requires-Dist',[]))==sorted(str(Requirement(x)) for x in requires)
    assert project['readme']=='README.md' and metadata['Description-Content-Type']=='text/markdown'
    assert metadata.get_payload(decode=True).replace(b'\r\n',b'\n')==(source/'README.md').read_bytes().replace(b'\r\n',b'\n')
    payload_prefix=module+'/' if wheel else prefix+'src/'+module+'/'
    expected={p.relative_to(source/'src').as_posix():p.read_bytes() for p in (source/'src'/module).rglob('*') if p.is_file()}
    actual={module+'/'+name.removeprefix(payload_prefix):data for name,data in entries.items() if name.startswith(payload_prefix)}
    assert actual==expected and module+'/py.typed' in actual
    if wheel:
        info=module+'-'+version+'.dist-info/';allowed=set(expected)|{info+n for n in ('METADATA','WHEEL','top_level.txt','licenses/LICENSE','RECORD')}
        entrypoints=entrypoint_text(project)
        if entrypoints is not None:
            allowed.add(info+'entry_points.txt')
            assert entries[info+'entry_points.txt']==entrypoints
        assert set(entries)==allowed and entries[info+'licenses/LICENSE']==(source/'LICENSE').read_bytes()
        assert entries[info+'top_level.txt']==(module+'\n').encode('ascii')
        assert entries[info+'WHEEL']==b'Wheel-Version: 1.0\nGenerator: setuptools (80.9.0)\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n'
        rows=list(csv.reader(io.StringIO(entries[info+'RECORD'].decode('utf-8'))))
        assert len(rows)==len(entries) and {row[0] for row in rows}==set(entries)
        for name,hashed,size in rows:
            if name==info+'RECORD':assert hashed==size==''
            else:assert hashed=='sha256='+base64.urlsafe_b64encode(hashlib.sha256(entries[name]).digest()).rstrip(b'=').decode('ascii') and size==str(len(entries[name]))
    else:
        assert entries[prefix+'LICENSE']==(source/'LICENSE').read_bytes()
        assert entries[prefix+'README.md']==(source/'README.md').read_bytes() and entries[prefix+'pyproject.toml']==(source/'pyproject.toml').read_bytes()
        allowed={prefix+n for n in ('LICENSE','README.md','pyproject.toml','PKG-INFO','setup.cfg')}
        info=prefix+'src/'+module+'.egg-info/'
        generated={'PKG-INFO':entries[prefix+'PKG-INFO'],'dependency_links.txt':b'\n','top_level.txt':(module+'\n').encode('ascii')}
        requires_text=''.join(item+'\n' for item in project.get('dependencies',[]))
        for name,items in extras.items():requires_text+='\n['+name+']\n'+''.join(item+'\n' for item in items)
        if requires_text:generated['requires.txt']=requires_text.encode('utf-8')
        entrypoints=entrypoint_text(project)
        if entrypoints is not None:generated['entry_points.txt']=entrypoints
        assert {n for n in entries if n.startswith(info)}=={info+n for n in (*generated,'SOURCES.txt')}
        for name,raw in generated.items():assert entries[info+name]==raw,'generated egg-info mismatch: '+name
        assert entries[prefix+'setup.cfg']=='[egg_info]\ntag_build = \ntag_date = 0\n\n'.replace('\n',os.linesep).encode('ascii')
        assert all(name in allowed or name.startswith((prefix+'src/'+module+'/',info)) for name in entries)
        sources=entries[info+'SOURCES.txt'].decode('utf-8').splitlines()
        physical={name.removeprefix(prefix) for name in entries}-{'PKG-INFO'}
        if not (source/'setup.cfg').exists():physical.remove('setup.cfg')
        assert len(sources)==len(set(sources)) and set(sources)==physical
    return dict(sha256=sha(path),bytes=path.stat().st_size,project=project['name'],version=version,
        source_modules={n:digest(data) for n,data in expected.items()},metadata_readme_license_dependencies_verified=True,
        entries={n:dict(sha256=digest(data),bytes=len(data)) for n,data in entries.items()},members=members)


def logged(py,args,location,output,label,*,mypy_path=None):
    log=output/(label+'.log');assert not log.exists()
    env=dict(os.environ)
    for name in ('PYTHONPATH','PYTHONHOME','MYPYPATH'):env.pop(name,None)
    if mypy_path is not None:env['MYPYPATH']=str(mypy_path)
    with log.open('wb') as stream:
        result=subprocess.run([str(py),'-I',*[str(x) for x in args]],cwd=location,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=240)
    if result.returncode:print(log.read_text(encoding='utf-8')[-32768:],flush=True)
    assert result.returncode==0,'retained failed log '+log.name
    return dict(log=log.name,sha256=sha(log))


def project_pair(name,source,temp,output,auditor):
    """Repeat clean-source archives; then wheel from EVERY full selected SDist."""
    roots=[];records=[]
    for repeat in (0,1):
        copied=temp/(name+'-source-'+str(repeat));shutil.copytree(source,copied)
        dest=temp/(name+'-build-'+str(repeat));build(copied,dest)
        artifacts=sorted(dest.iterdir());assert len(artifacts)==2
        records.append({p.name:inspect_archive(p,source,auditor) for p in artifacts});roots.append(dest)
    assert records[0]==records[1],'repeat clean source archive mismatch'
    sdist=next(roots[0].glob('*.tar.gz'));wheel=next(roots[0].glob('*.whl'))
    entries=auditor.contents(sdist);prefix=sdist.name.removesuffix('.tar.gz')+'/'
    selected=temp/(name+'-selected-sdist');selected.mkdir()
    for relative,data in entries.items():
        assert relative.startswith(prefix);target=selected/relative.removeprefix(prefix)
        assert target.resolve().is_relative_to(selected.resolve());target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
    before=files(selected);repeat_dir=temp/(name+'-selected-wheel')
    env=dict(os.environ,SOURCE_DATE_EPOCH=EPOCH,PYTHONHASHSEED='0')
    run(sys.executable,'-m','build','--wheel','--no-isolation','--outdir',repeat_dir,selected,env=env)
    repeated=next(repeat_dir.glob('*.whl'));assert sha(repeated)==sha(wheel)
    after=files(selected)
    module=tomllib.loads((source/'pyproject.toml').read_text(encoding='utf-8'))['project']['name'].replace('-','_')
    sources='src/'+module+'.egg-info/SOURCES.txt'
    assert all(after[n]==h for n,h in before.items() if n!=sources),'selected input or generated metadata mutated'
    copied={'build/lib/'+p.relative_to(source/'src').as_posix():sha(p) for p in (source/'src'/module).rglob('*') if p.is_file()}
    assert set(after)==set(before)|set(copied),'selected sdist build changed undeclared physical membership'
    assert all(after[n]==h for n,h in copied.items()),'selected wheel build copies changed source bytes'
    original=entries[prefix+sources].decode('utf-8').splitlines()
    actual=(selected/sources).read_text(encoding='utf-8').splitlines()
    expected=set(original)|{'setup.cfg'}
    assert len(actual)==len(set(actual)) and set(actual)==expected,'unselected generated SOURCES change'
    for path in roots[0].iterdir():shutil.copy2(path,output/path.name)
    return dict(wheel=wheel,sdist=sdist,archives=records[0],selected_sdist_before=before,
        selected_sdist_after=after,selected_generated_sources_sha256=after[sources],
        selected_build_copies=copied,
        selected_sdist_repeat_wheel_sha256=sha(repeated),selected_sdist_full_materialization=True)


def installed_matches_archives(mapping,archives,auditor):
    """Exact owning-wheel payload parity, including two fixed pywin32 relocations."""
    normalize_name=lambda name:re.sub(r'[-_.]+','-',name).lower()
    installed={normalize_name(name):record for name,record in mapping.items()}
    assert len(installed)==len(mapping)
    site='Lib/site-packages/' if os.name=='nt' else 'lib/python3.12/site-packages/'
    data_members={'pywin32-312.data/scripts/pywin32_postinstall.py','pywin32-312.data/scripts/pywin32_testall.py'}
    relocated=[]
    for archive in archives:
        assert archive.suffix=='.whl'
        entries=auditor.contents(archive)
        records=[name for name in entries if name.count('/')==1 and name.endswith('.dist-info/RECORD')]
        assert len(records)==1;owning=records[0];info=owning.rsplit('/',1)[0]+'/'
        metadata=BytesParser().parsebytes(entries[info+'METADATA'])
        identity=normalize_name(metadata['Name']);record=installed[identity]
        if identity=='pywin32':assert archive.name=='pywin32-312-cp312-cp312-win_amd64.whl' and sha(archive)=='b457f6d628a47e8a7346ce22acb7e1a46a4a78b52e1d17e1af56871bd19a93bc'
        assert record['version']==metadata['Version']
        assert site+owning in record['files']
        observed=set()
        for name,data in entries.items():
            if name==owning:continue
            target=site+name
            if name.split('/',1)[0].endswith('.data'):
                assert os.name=='nt' and identity=='pywin32' and metadata['Version']=='312'
                assert archive.name=='pywin32-312-cp312-cp312-win_amd64.whl'
                assert name in data_members,'unsupported relocated wheel member'
                assert not data.splitlines(keepends=True)[0].startswith(b'#!python'),'unselected pip shebang rewrite'
                target='Scripts/'+name.rsplit('/',1)[1];observed.add(name)
                relocated.append(dict(archive=archive.name,distribution=metadata['Name'],version=metadata['Version'],member=name,installed_path=target,sha256=digest(data),bytes=len(data)))
            assert record['files'].get(target)==digest(data),(metadata['Name'],name,target)
        if identity=='pywin32':assert metadata['Version']=='312' and observed==data_members
    assert len(relocated) in (0,2)
    return sorted(relocated,key=lambda item:item['member'])


def qualify(form,projects,support,reference,component,apps,client,service,output,temp,auditor):
    integration=form.endswith('integration');sdist=form.startswith('sdist')
    location=Path(tempfile.mkdtemp(prefix='eq109-'+form+'-',dir=ROOT/'work')).resolve()
    run(sys.executable,'-m','venv',location/'venv');py=location/'venv'/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
    run(py,'-m','pip','install','--no-index','--no-deps',*support)
    selected=list(projects) if integration else list(LIGHT)
    archives=[projects[name]['sdist' if sdist else 'wheel'] for name in selected]
    if integration:run(py,'-m','pip','install','--no-index','--no-deps',*reference)
    run(py,'-m','pip','install','--no-index','--no-deps','--no-build-isolation',*archives);run(py,'-m','pip','check')
    before=closure(py,location)
    modules=[next(iter(projects[name]['archives'].values()))['project'].replace('-','_') for name in selected]
    origin_code='''import importlib,json,sys,hashlib
from pathlib import Path
base=Path(sys.prefix).resolve();out={}
for name in sys.argv[1:]:
 module=importlib.import_module(name);path=Path(module.__file__).resolve()
 assert path.is_relative_to(base) and 'site-packages' in path.parts
 out[name]={'origin':path.relative_to(base).as_posix(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
print(json.dumps(out,sort_keys=True))
'''
    origins=json.loads(subprocess.check_output([str(py),'-I','-c',origin_code,*modules],cwd=location,encoding='utf-8'))
    harness=location/'harness';harness.mkdir()
    shutil.copytree(apps/'examples',harness/'examples');shutil.copytree(apps/'tests',harness/'tests');shutil.copytree(apps/'docs',harness/'docs')
    logs=[]
    if integration:
        shutil.copy2(component/'examples/agent_research_mcp.py',harness/'examples/agent_research_mcp.py')
        shutil.copy2(component/'tests/agent_research/test_mcp_consumer.py',harness/'tests/agent_research/test_mcp_consumer.py')
        for path,target in ((client/'tests/client','tests/client'),(service/'tests/service','tests/service')):shutil.copytree(path,harness/target)
        for name in ('EQ081_CLIENT_FIXTURES.json','EQ081_OWNED_HTTP_ENTRY.txt','EQ081_AUTHORITY_HTTP_ENTRY.txt'):shutil.copy2(client/'docs'/name,harness/'docs'/name)
        for name in ('EQ109_RESEARCH_REFERENCE_ENTRY.txt','EQ109_RESEARCH_REFERENCE_PROBE.txt','EQ083_REFERENCE_PROCESS.txt','EQ083_LINUX_REFERENCE_CHILD.txt'):shutil.copy2(component/'docs'/name,harness/'docs'/name)
    assert not (harness/'packages').exists()
    # Fixed launcher adds only owned example/test source. All packages must resolve
    # to the fresh installed environment; no repository package root is copied.
    launch=harness/'source-tests.py';launch.write_text("import sys,unittest\nfrom pathlib import Path\nr=Path(__file__).parent\nsys.path.insert(0,str(r/'examples'));sys.path.insert(0,str(r/'tests/agent_research'))\nif sys.argv[1:]==['--typed-example']:\n import runpy\n runpy.run_path(str(r/'examples/agent_research_typed.py'),run_name='__main__')\n raise SystemExit(0)\nsuite=unittest.TestSuite()\nfor name in sys.argv[1:]:suite.addTests(unittest.defaultTestLoader.loadTestsFromName(name))\nresult=unittest.TextTestRunner(verbosity=2).run(suite)\nraise SystemExit(0 if result.wasSuccessful() else 1)\n",encoding='utf-8')
    names=files(harness)
    pins=dict(source_map=names,config_sha256=names.get('tests/service/fixtures/jobs_native.json'))
    pinfile=location/'research-pins.json';pinfile.write_text(json.dumps(pins,sort_keys=True)+'\n',encoding='utf-8')
    absence="import importlib.util;assert all(importlib.util.find_spec(n) is None for n in ('equity_features','equity_feature_client','equity_feature_mcp','equity_feature_service','mcp'))"
    if not integration:logs.append(logged(py,['-c',absence],location,output,form+'-absence'))
    modules=['test_consumer','test_replay','test_mcp_consumer'] if integration else ['test_consumer','test_light']
    logs.append(logged(py,[launch,*modules],location,output,form+'-methods'))
    if integration:
        logs.append(logged(py,[harness/'docs/EQ109_RESEARCH_REFERENCE_PROBE.txt',harness,pinfile],location,output,form+'-SDK-native-ledger-replay'))
        logs.append(logged(py,[launch,'--typed-example'],location,output,form+'-typed-example'))
    typed=[harness/'examples/agent_research.py']+([harness/'examples/agent_research_typed.py',harness/'examples/agent_research_mcp.py'] if integration else [])
    logs.append(logged(py,['-m','mypy','--strict','--follow-imports=silent',*typed],location,output,form+'-source-typing',mypy_path=harness/'examples'))
    consumer=location/'consumer.py';consumer.write_text("from agent_research import Assessment,assess\nfrom equity_feature_contracts import ConfigSpec,CanonicalBatch,FeatureResult\ndef run(c:ConfigSpec,b:CanonicalBatch,p:FeatureResult)->Assessment:\n    return assess(c,((\"trades\",b),),p,None,None,None,\"session.trade.count=3\")\n",encoding='utf-8')
    positive_consumer_sha256=sha(consumer)
    logs.append(logged(py,['-m','mypy','--strict',consumer],location,output,form+'-consumer-positive',mypy_path=harness/'examples'))
    consumer.write_text('from agent_research import assess\nassess(1,(),2,None,None,None,False)\n',encoding='utf-8')
    negative_consumer_sha256=sha(consumer)
    env=dict(os.environ,MYPYPATH=str(harness/'examples'))
    for name in ('PYTHONPATH','PYTHONHOME'):env.pop(name,None)
    negative=subprocess.run([str(py),'-I','-m','mypy','--strict',str(consumer)],cwd=location,env=env,capture_output=True,encoding='utf-8',timeout=120)
    assert negative.returncode==1 and '[arg-type]' in negative.stdout
    negfile=output/(form+'-consumer-negative.log');negfile.write_text(negative.stdout+negative.stderr,encoding='utf-8');logs.append(dict(log=negfile.name,sha256=sha(negfile)))
    after=closure(py,location);assert after==before
    wheel_archives=[projects[name]['wheel'] for name in selected]+support+(reference if integration else [])
    relocated=installed_matches_archives(after,wheel_archives,auditor)
    mapfile=output/(form+'-installed-map.json');mapfile.write_text(json.dumps(after,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    record=dict(form=form,logs=logs,installed_map=mapfile.name,installed_map_sha256=sha(mapfile),
        packages=selected,harness_source=names,independent_light_absence=not integration,
        installed_module_origins=origins,pins_sha256=sha(pinfile),relocated_wheel_payloads=relocated,
        positive_consumer_sha256=positive_consumer_sha256,negative_consumer_sha256=negative_consumer_sha256,
        unchanged_registered_files=True,registered_RECORD_validated=True,original_wheel_payload_equal=True,
        application_scope='Source example/tests only in owned harness; every package imported from fresh site-packages. Not full host/stdlib/ABI closure.')
    assert location.parent==ROOT/'work' and location.name.startswith('eq109-') and not location.is_symlink()
    shutil.rmtree(location);record['owned_form_cleanup_completed']=True;return record


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--core-root',type=Path,required=True);parser.add_argument('--io-root',type=Path,required=True)
    parser.add_argument('--outdir',type=Path,default=ROOT/'dist/agent-research');args=parser.parse_args()
    head=git(ROOT,'rev-parse','HEAD');assert not git(ROOT,'status','--porcelain','--',*INPUTS)
    core_repo=args.core_root.resolve();io_repo=args.io_root.resolve()
    assert git(core_repo,'rev-parse',CORE_RESEARCH)==CORE_RESEARCH and git(io_repo,'rev-parse',IO_RESEARCH)==IO_RESEARCH
    output=args.outdir.resolve();assert not output.exists();output.mkdir(parents=True);(ROOT/'work').mkdir(exist_ok=True)
    temp=Path(tempfile.mkdtemp(prefix='eq109-build-',dir=ROOT/'work')).resolve()
    component=snapshot(ROOT,head,temp/'component',INPUTS)
    for path in (component/'tools').glob('*.py'):
        assert (ROOT/'tools'/path.name).read_bytes().replace(b'\r\n',b'\n')==path.read_bytes().replace(b'\r\n',b'\n')
    core=snapshot(core_repo,CORE_COMMIT,temp/'core-base',('packages/contracts','packages/features'))
    optional=snapshot(core_repo,CORE_RESEARCH,temp/'core-optional',tuple('packages/'+n for n in LIGHT if n not in ('contracts','bundles'))+('tools/build_agent_ledger.py','tools/_versions.py'))
    # Agent declaration/ledger accepted builds use raw Git source, unlike older
    # foundation archive materialization. Preserve their declared source profiles.
    for name in ('agent-evidence','agent-ledger'):
        for relative in git(core_repo,'ls-tree','-r','--name-only',CORE_RESEARCH,'--','packages/'+name).splitlines():
            (optional/relative).write_bytes(subprocess.check_output(['git','show',CORE_RESEARCH+':'+relative],cwd=core_repo))
    app_dir=temp/'core-apps';app_record=raw_application(core_repo,CORE_RESEARCH,app_dir,CORE_APPS)
    io_base=snapshot(io_repo,IO_BASE,temp/'io-base',('packages/io-contracts','packages/io-sdk','packages/duckdb','examples/third_party'))
    bundles=snapshot(io_repo,IO_RESEARCH,temp/'io-bundles',('packages/bundles',))
    # Accepted bundle source qualification also writes raw Git bytes.
    for relative in git(io_repo,'ls-tree','-r','--name-only',IO_RESEARCH,'--','packages/bundles').splitlines():
        (bundles/relative).write_bytes(subprocess.check_output(['git','show',IO_RESEARCH+':'+relative],cwd=io_repo))
    workers=snapshot(ROOT,WORKER_COMMIT,temp/'workers',('packages/workers',))
    service=snapshot(ROOT,SERVICE_COMMIT,temp/'service',('packages/service','tests/service'))
    client=snapshot(ROOT,CLIENT_COMMIT,temp/'client',CLIENT_INPUTS)
    auditor=load_auditor(optional/'tools/build_agent_ledger.py')
    assert (component/'tests/agent_research/independent_oracle.py').read_bytes().replace(b'\r\n',b'\n')==(app_dir/'tests/agent_research/independent_oracle.py').read_bytes()
    source_closure={
        'component':source_record(ROOT,head,component,INPUTS),
        'core_base':source_record(core_repo,CORE_COMMIT,core,('packages/contracts','packages/features')),
        'optional':source_record(core_repo,CORE_RESEARCH,optional,tuple('packages/'+n for n in LIGHT if n not in ('contracts','bundles'))+('tools/build_agent_ledger.py',)),
        'io_base':source_record(io_repo,IO_BASE,io_base,('packages/io-contracts','packages/io-sdk','packages/duckdb','examples/third_party')),
        'bundles':source_record(io_repo,IO_RESEARCH,bundles,('packages/bundles',)),
        'workers':source_record(ROOT,WORKER_COMMIT,workers,('packages/workers',)),
        'service':source_record(ROOT,SERVICE_COMMIT,service,('packages/service','tests/service')),
        'client':source_record(ROOT,CLIENT_COMMIT,client,CLIENT_INPUTS),
        'applications':app_record,
    }
    definitions=[('contracts',core,'packages/contracts'),('features',core,'packages/features')]
    definitions += [(name,optional,'packages/'+name) for name in LIGHT if name not in ('contracts','bundles')]
    definitions += [('bundles',bundles,'packages/bundles'),('io-contracts',io_base,'packages/io-contracts'),('io-sdk',io_base,'packages/io-sdk'),('duckdb',io_base,'packages/duckdb'),('demo',io_base,'examples/third_party'),('workers',workers,'packages/workers'),('service',service,'packages/service'),('client',client,'packages/client'),('mcp',component,'packages/mcp')]
    assert len(definitions)==16
    report=dict(schema='eq109.agent.research.qualification.v1',commit=head,core_research=CORE_RESEARCH,io_research=IO_RESEARCH,
        core_base=CORE_COMMIT,io_base=IO_BASE,client=CLIENT_COMMIT,service=SERVICE_COMMIT,workers=WORKER_COMMIT,
        application_source=app_record,materialized_sources={name:files(root/relative) for name,root,relative in definitions},
        complete_selected_source_closure=source_closure,
        status='running',forms=[],projects={},python=platform.python_version(),system=platform.system(),machine=platform.machine())
    receipt=output/'agent-research-receipt.json'
    def save():receipt.write_text(json.dumps(report,sort_keys=True,indent=2)+'\n',encoding='utf-8',newline='\n')
    save();projects={}
    for name,root,relative in definitions:
        result=project_pair(name,root/relative,temp,output,auditor);projects[name]=result
        report['projects'][name]={k:v for k,v in result.items() if k not in ('wheel','sdist')};save()
    baseline=json.loads((component/'docs/EQ109_ACCEPTED_ARCHIVES.json').read_bytes())['systems'][platform.system()]
    produced={n:record for project in projects.values() for n,record in project['archives'].items()}
    assert set(baseline).issubset(produced)
    for name,expected in baseline.items():
        assert all(produced[name][k]==expected[k] for k in ('sha256','bytes')),'accepted project archive identity changed: '+name
    report['accepted_project_archive_identity']={n:produced[n]['sha256'] for n in baseline};save()
    refdir=temp/'reference';refdir.mkdir();supportdir=temp/'support';supportdir.mkdir()
    run(sys.executable,'-m','pip','download','--only-binary=:all:','--no-deps','-r',component/'tools/requirements-mcp-reference.txt','-d',refdir)
    run(sys.executable,'-m','pip','download','--only-binary=:all:','--no-deps',*SUPPORT,'-d',supportdir)
    reference=sorted(refdir.glob('*.whl'));support=sorted(supportdir.glob('*.whl'));assert len(reference)==(33 if os.name=='nt' else 31) and len(support)==6
    for artifact in reference+support:
        reference_archive(artifact,auditor);dest=output/artifact.name
        if dest.exists():assert sha(dest)==sha(artifact)
        else:shutil.copy2(artifact,dest)
    report['reference_archives']={p.name:reference_archive(p,auditor) for p in reference+support};save()
    for form in ('wheel-light','sdist-light','wheel-integration','sdist-integration'):
        report['forms'].append(qualify(form,projects,support,reference,component,app_dir,client,service,output,temp,auditor));save()
    assert git(ROOT,'rev-parse','HEAD')==head and not git(ROOT,'status','--porcelain','--',*INPUTS)
    assert temp.parent==ROOT/'work' and temp.name.startswith('eq109-build-') and not temp.is_symlink()
    shutil.rmtree(temp);report['owned_build_cleanup_completed']=True;report['status']='completed';save()
    print('EQ109 actual fresh4forms complete; scope in receipt/rawlogs, not release acceptance.')


if __name__=='__main__':main()
