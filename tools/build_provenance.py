# RELOCATED from the core repo by ADR-1 (core commit 0fc675d), byte-for-byte except this note.
# Ported to the workers-rooted layout: the canonical checkout is this repository (packages/provenance, packages/workers,
# tests/provenance); core (contracts, features, evidence, diagnostics and their tests/oracles) and io are read at exact
# pinned commits from --core and --io checkouts. Core build_explanations is loaded in isolation for archive inspection.
"""Pinned three-repository archives and fresh installed outer/native consumers."""
from __future__ import annotations
import argparse
import fnmatch
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import tomllib
from email.parser import BytesParser
from build_foundation import PINS_CORE, PINS_IO
from build_mcp_evidence import load_core_module
from _versions import VERSIONS

ROOT=Path(__file__).resolve().parents[1]
PINS={'core':PINS_CORE,'io':PINS_IO}
PACKAGES={'contracts':('equity_feature_contracts',VERSIONS['equity-feature-contracts']), 'features':('equity_features',VERSIONS['equity-features']),
    'evidence':('equity_feature_evidence',VERSIONS['equity-feature-evidence']),'diagnostics':('equity_feature_diagnostics',VERSIONS['equity-feature-diagnostics']),
    'provenance':('equity_feature_provenance',VERSIONS['equity-feature-provenance']),'io-contracts':('equity_feature_io_contracts',VERSIONS['equity-feature-io-contracts']),
    'io-sdk':('equity_feature_io_sdk',VERSIONS['equity-feature-io-sdk']),'duckdb':('equity_feature_duckdb',VERSIONS['equity-feature-duckdb']),
    'duckdb-sink':('equity_feature_duckdb_sink',VERSIONS['equity-feature-duckdb-sink']),'workers':('equity_feature_workers',VERSIONS['equity-feature-workers'])}
common=None  # core's build_explanations, loaded in main() from the pinned --core checkout
def run(*args,cwd=ROOT,env=None):
    return common.run(*args,cwd=cwd,env=env)
def sha(data):
    return common.sha(data)
NATIVE=('duckdb==1.5.6','numpy==2.2.6')
CORE_PACKAGES=('contracts','features','evidence','diagnostics')
IO_PACKAGES=('io-contracts','io-sdk','duckdb','duckdb-sink')


def git(root,*args):return subprocess.check_output(['git',*args],cwd=root)


def installed(py,names):
    """All distribution files, including native payloads and generated entry scripts."""
    script='''import base64,hashlib,json,sys
from pathlib import Path
from importlib.metadata import distribution
root=Path(sys.prefix).resolve();result={}
for name in sys.argv[1:]:
 d=distribution(name);files={}
 assert d.files is not None
 for entry in d.files:
  p=Path(d.locate_file(entry)).resolve()
  assert p.is_relative_to(root)
  if not p.is_file() or p.suffix=='.pyc':continue
  raw=p.read_bytes();digest=hashlib.sha256(raw).digest()
  if entry.hash is not None:
   assert entry.hash.mode=='sha256'
   assert entry.hash.value==base64.urlsafe_b64encode(digest).rstrip(b'=').decode()
   assert entry.size==len(raw)
  files[p.relative_to(root).as_posix()]={'sha256':digest.hex(),'bytes':len(raw)}
 result[name]={'version':d.version,'files':files}
print(json.dumps(result,sort_keys=True))
'''
    env={k:v for k,v in os.environ.items() if k not in ('PYTHONPATH','MYPYPATH','PYTHONHOME','PYTHONUSERBASE')}
    return json.loads(subprocess.check_output([str(py),'-I','-c',script,*names],cwd=py.parent,env=env,encoding='utf-8'))


def verify_payload(mapping,archives):
    for name,path in archives.items():
        module=PACKAGES[name][0];files=common.contents(path)
        prefix=module+'/' if path.suffix=='.whl' else path.name.removesuffix('.tar.gz')+'/src/'+module+'/'
        expected={module+'/'+n.removeprefix(prefix):sha(raw) for n,raw in files.items() if n.startswith(prefix)}
        actual={n.split('site-packages/',1)[1]:v['sha256'] for n,v in mapping[module.replace('_','-')]['files'].items()
            if 'site-packages/'+module+'/' in n}
        assert actual==expected


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--core',type=Path,required=True);parser.add_argument('--io',type=Path,required=True)
    parser.add_argument('--out',type=Path,default=ROOT/'dist/provenance');args=parser.parse_args()
    global common
    assert git(args.core,'rev-parse','HEAD').decode().strip()==PINS['core'],'core checkout must be at the pinned commit'
    common=load_core_module('build_explanations',args.core.resolve()/'tools/build_explanations.py');common.PACKAGES=PACKAGES
    out=args.out.resolve();out.mkdir(parents=True,exist_ok=True);assert not list(out.iterdir()),'fresh output required'
    roots={'core':args.core.resolve(),'io':args.io.resolve(),'canonical':ROOT}
    heads={'canonical':git(ROOT,'rev-parse','HEAD').decode().strip(),**PINS}
    scopes={'core':['packages/'+p for p in CORE_PACKAGES]+['tests/diagnostics','tests/evidence','docs/stories/EQ-096_RECEIPT_ORACLE.json',
        'docs/stories/EQ-097_*','docs/stories/EQ-101_TRUTH_TABLE.json','docs/stories/EQ-103_*','tools/build_explanations.py'],
        'io':['packages/'+p for p in IO_PACKAGES],
        'canonical':['packages/provenance','packages/workers','tests/provenance','docs/api/PROVENANCE.md','examples/provenance_readback.py',
        'tools/build_provenance.py','tools/build_foundation.py','tools/build_mcp_evidence.py','tools/_versions.py','requirements-dev.txt']}
    snapshot={};materialized={};source_map={}
    for repo,root in roots.items():
        # Companions are read at exact immutable refs, never reset or modified.
        git(root,'cat-file','-e',heads[repo]+'^{commit}')
        all_paths=git(root,'ls-tree','-r','--name-only',heads[repo]).decode().splitlines()
        paths=[n for n in all_paths if any(n==p or n.startswith(p+'/') or fnmatch.fnmatchcase(n,p) for p in scopes[repo])]
        assert paths
        for relative in paths:
            data=git(root,'show',heads[repo]+':'+relative);key=repo+'/'+relative
            snapshot[key]=data
            source_map[key]={'sha256':sha(data),'bytes':len(data),'commit':heads[repo]}
            if repo=='canonical':
                live=(root/relative).read_bytes();assert live.replace(b'\r\n',b'\n')==data
                materialized[relative]=live
    def closure():
        assert git(ROOT,'rev-parse','HEAD').decode().strip()==heads['canonical']
        assert not git(ROOT,'status','--porcelain','--untracked-files=all','--',*scopes['canonical'])
        assert all((ROOT/n).read_bytes()==b for n,b in materialized.items())
        for key,data in snapshot.items():
            repo,relative=key.split('/',1);assert git(roots[repo],'show',heads[repo]+':'+relative)==data
    report={'schema':'provenance-qualification:1','source_commit':heads['canonical'],'pins':heads,
        'system':platform.system(),'python':platform.python_version(),'source':source_map,'artifacts':{},'installed':[],'status':'running'}
    def save():(out/'qualification.json').write_text(json.dumps(report,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    closure();save();work=ROOT/'work';work.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='provenance-',dir=work) as temporary:
        owned=Path(temporary).resolve();assert owned.is_relative_to(work.resolve())
        verification=owned/'verification'
        # Core and this repository overlay one verification tree: no path may be supplied by two repositories.
        placed={}
        for key in snapshot:
            repo,relative=key.split('/',1);target=('io' if repo=='io' else 'canonical')+'/'+relative
            assert target not in placed,'path collision across repositories: '+target+' from '+placed.get(target,'')+' and '+key
            placed[target]=key
        for key,data in snapshot.items():
            repo,relative=key.split('/',1);target=verification/('io' if repo=='io' else 'canonical')/relative
            target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
        env=dict(os.environ,SOURCE_DATE_EPOCH='1700000000',PYTHONHASHSEED='0')
        for repeat in (0,1):
            dest=owned/f'build-{repeat}';dest.mkdir()
            for name,(module,version) in PACKAGES.items():
                repo='core' if name in CORE_PACKAGES else 'io' if name in IO_PACKAGES else 'canonical'
                source=owned/f'source-{repeat}-{name}';prefix=repo+'/packages/'+name+'/'
                for key,data in snapshot.items():
                    if key.startswith(prefix):
                        target=source/key.removeprefix(prefix);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
                project=tomllib.loads((source/'pyproject.toml').read_text())['project']
                run(sys.executable,'-m','build','--no-isolation','--outdir',dest,source,env=env)
                archives=list(dest.glob(module+'-*'));assert len(archives)==2
                for path in archives:
                    if path.suffix!='.whl':common.normalize(path)
                    inspection=common.inspect(path,name,source)
                    files=common.contents(path)
                    metadata=next(raw for n,raw in files.items() if n.endswith('/METADATA') or n==path.name.removesuffix('.tar.gz')+'/PKG-INFO')
                    parsed=BytesParser().parsebytes(metadata)
                    assert [d for d in parsed.get_all('Requires-Dist',[]) if 'extra ==' not in d]==project.get('dependencies',[])
                    assert parsed.get_all('Provides-Extra',[])==list(project.get('optional-dependencies',{}))
                    if repeat==0:report['artifacts'][path.name]=inspection;shutil.copyfile(path,out/path.name)
                    else:assert inspection==report['artifacts'][path.name],'repeat archive differs'
        assert len(report['artifacts'])==20;report['repeat_archive_parity']=True;save()
        wheels=owned/'dependencies';wheels.mkdir()
        run(sys.executable,'-m','pip','download','--only-binary=:all:','--no-deps','--index-url','https://pypi.org/simple','--dest',wheels,*NATIVE,'setuptools==80.9.0')
        report['dependency_wheels']={p.name:{'sha256':sha(p.read_bytes()),'bytes':p.stat().st_size} for p in wheels.iterdir()};save()
        for form,suffix in (('wheel','.whl'),('sdist','.tar.gz')):
            archives={n:next(out.glob(module+'-*'+suffix)) for n,(module,_) in PACKAGES.items()}
            for live in (False,True):
                directory=owned/(form+('-live' if live else '-supplied'));run(sys.executable,'-m','venv',directory)
                py=directory/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
                run(py,'-m','pip','install','--no-index','--no-deps','--find-links',wheels,'setuptools==80.9.0')
                pure={n:archives[n] for n in ('contracts','evidence','diagnostics')}
                run(py,'-m','pip','install','--no-index','--no-deps','--no-build-isolation',*pure.values());run(py,'-m','pip','check')
                run(py,'-I','-c',"import equity_feature_contracts,equity_feature_evidence,equity_feature_diagnostics,sys; assert all(n not in sys.modules for n in ('duckdb','numpy','equity_feature_workers','equity_feature_provenance'))",cwd=py.parent)
                baseline=installed(py,[PACKAGES[n][0].replace('_','-') for n in pure]);verify_payload(baseline,pure)
                run(py,'-m','pip','install','--no-index','--no-deps','--find-links',wheels,*NATIVE)
                run(py,'-m','pip','install','--no-index','--no-deps','--no-build-isolation',*(archives[n] for n in PACKAGES if n not in pure));run(py,'-m','pip','check')
                names=[module.replace('_','-') for module,_ in PACKAGES.values()]+['duckdb','numpy']
                before=installed(py,names);verify_payload(before,archives)
                assert all(before[n]==v for n,v in baseline.items())
                tests=verification/'canonical/tests'
                if live:run(py,'-I','-m','unittest','discover','-s',tests/'provenance',cwd=py.parent)
                else:run(py,'-I','-m','unittest','discover','-s',tests/'provenance','-p','test_supplied.py',cwd=py.parent)
                run(py,'-I','-m','unittest','discover','-s',tests/'diagnostics',cwd=py.parent)
                run(py,'-I','-m','unittest','discover','-s',tests/'evidence',cwd=py.parent)
                if live:run(py,'-I',verification/'canonical/examples/provenance_readback.py',cwd=py.parent)
                consumer=py.parent/'provenance_consumer.py'
                consumer.write_text('from equity_feature_provenance import TaskEvidence,ProvenanceReport,bind_supplied_provenance\ndef checked(value: TaskEvidence) -> ProvenanceReport:\n    return bind_supplied_provenance(value)\n',encoding='utf-8')
                run(sys.executable,'-m','mypy','--strict','--python-executable',py,consumer,cwd=py.parent)
                after=installed(py,names);assert after==before
                report['installed'].append({'form':form,'live_native':live,'installed_map':after,'pure_before_outer_unchanged':True,
                    'project_payload_archive_parity':True,'all_distribution_records_verified':True,'tests_passed':True,'typing_passed':True});save()
    closure();report['source_closure']=True;report['status']='completed';save()


if __name__=='__main__':main()
