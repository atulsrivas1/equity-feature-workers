"""EF1 candidate installed-artifact checks; original historical runners are unchanged.

Runs only when explicitly invoked in an admitted execution environment. Tests are
mandatory, with no skipped EF1 tests accepted as qualification. This checks the
anchor, supplied-clock, current-state status, sampled path and supplied reference slices, not the full EF1 release or its inventory/performance gates.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import tomllib
import venv


def run(args: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    subprocess.run(args, cwd=cwd, env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--core-root', type=Path, required=True)
    parser.add_argument('--io-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    worker = Path(__file__).resolve().parents[1]
    core, io = args.core_root.resolve(), args.io_root.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
                       PYTHONHASHSEED='0', SOURCE_DATE_EPOCH='1700000000')
    for key in ('PYTHONPATH', 'PYTHONHOME', 'PYTHONUSERBASE', 'MYPYPATH'):
        environment.pop(key, None)
    projects = [
        (core/'packages/contracts', 'equity-feature-contracts', '0.0.4a4'),
        (core/'packages/features', 'equity-features', '0.0.4a5'),
        (core/'packages/intraday', 'equity-features-intraday', '0.1.0a3'),
        (io/'packages/io-contracts', 'equity-feature-io-contracts', '0.1.0a2'),
        (io/'packages/io-sdk', 'equity-feature-io-sdk', '0.1.0a2'),
        (io/'packages/duckdb', 'equity-feature-duckdb', '0.1.0a9'),
        (io/'packages/duckdb-sink', 'equity-feature-duckdb-sink', '0.1.0a0'),
        (worker/'packages/workers', 'equity-feature-workers', '0.1.0a15'),
    ]
    for path, name, version in projects:
        project = tomllib.loads((path/'pyproject.toml').read_text(encoding='utf-8'))['project']
        if (project['name'], project['version']) != (name, version):
            raise RuntimeError('Candidate package identity mismatch: '+name)
    sources = {}
    for name, root in [('core',core), ('io',io), ('workers',worker)]:
        head = subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
        dirty = subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],cwd=root,text=True)
        if dirty.strip():
            raise RuntimeError('Qualification requires a clean committed checkout: '+name)
        sources[name] = head
    artifacts = output/'archives'
    artifacts.mkdir()
    for path, _, _ in projects:
        run([sys.executable,'-m','build','--no-isolation','--outdir',str(artifacts),str(path)],cwd=worker,env=environment)
    archives = sorted(artifacts.iterdir())
    if len(archives) != 2*len(projects):
        raise RuntimeError('Incomplete wheel/sdist inventory')
    receipt = {'scope':'EF1 anchor/clock/current-state status/sampled path/reference installed artifacts and legacy numerical regression only; not full EF1 acceptance',
        'sources':sources,'python':platform.python_version(),'system':platform.system(),
        'archives':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in archives},'forms':[]}
    # Both physical forms of every package, plus two mixed boundaries. Each form
    # gets a new venv and normal dependency resolution (never --no-deps).
    for form in ('wheel','sdist','core-wheel-components-sdist','core-sdist-components-wheel'):
        with tempfile.TemporaryDirectory(prefix='ef1-installed-') as temp:
            root = Path(temp)
            venv.EnvBuilder(with_pip=True).create(root/'venv')
            python = root/'venv'/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
            run([str(python),'-m','pip','install','pip==25.1.1','setuptools==80.9.0',
                'build==1.2.2.post1','pyproject_hooks==1.3.3','packaging==26.3'],cwd=root,env=environment)
            selected=[]
            for index, (_,name,version) in enumerate(projects):
                wheel = form=='wheel' or form=='core-wheel-components-sdist' and index<3 or form=='core-sdist-components-wheel' and index>=3
                filename=name.replace('-','_')+'-'+version+('-py3-none-any.whl' if wheel else '.tar.gz')
                selected.append(str(artifacts/filename))
            run([str(python),'-m','pip','install','--no-build-isolation',*selected],cwd=root,env=environment)
            run([str(python),'-m','pip','check'],cwd=root,env=environment)
            # The probe runs outside the source trees; imports must resolve to
            # installed candidate artifacts, never source injection.
            probe = root/'probe.py'
            probe.write_text('''import importlib.metadata as md
import pathlib, sys
expected = '''+repr({name:version for _,name,version in projects})+'''
for name, version in expected.items():
    assert md.version(name)==version, (name,md.version(name),version)
import equity_features, equity_feature_workers, equity_feature_duckdb
from equity_features import ef1
from equity_feature_workers import equity_sessions, equity_clock, equity_status, equity_path_gap, equity_path_drive, equity_reference_classification, equity_reference_action
from equity_feature_duckdb import ef1 as ef1_inputs
for module in (equity_features,equity_feature_workers,equity_feature_duckdb,ef1,equity_sessions,equity_clock,equity_status,equity_path_gap,equity_path_drive,equity_reference_classification,equity_reference_action,ef1_inputs):
    assert pathlib.Path(module.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve())
''',encoding='utf-8')
            run([str(python),'-I','-B',str(probe)],cwd=root,env=environment)
            # A custom runner refuses all skipped/empty suites. Test source is
            # explicitly selected, but candidate package imports remain installed.
            runner=root/'run_tests.py'
            runner.write_text('''import multiprocessing, sys, unittest
def main():
    start, pattern, expected_text = sys.argv[1:]
    expected = int(expected_text)
    sys.path.insert(0,start)
    suite=unittest.defaultTestLoader.discover(start,pattern=pattern)
    assert expected>0 and suite.countTestCases()==expected, "Required suite census changed"
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    assert result.testsRun==expected and not result.skipped and result.wasSuccessful(), "Required suite failed/skipped"
if __name__=="__main__":
    multiprocessing.freeze_support()
    main()
''',encoding='utf-8')
            suites = [(core/'tests/unit','test_ef1_anchor.py',21),
                (core/'tests/unit','test_ef1_clock.py',13),
                (core/'tests/unit','test_ef1_status.py',26),
                (core/'tests/unit','test_ef1_path.py',50),
                (io/'tests/ef1','test_anchor*.py',16), (worker/'tests','test_ef1_anchors.py',10),
                (io/'tests/ef1','test_clock*.py',12), (worker/'tests','test_ef1_clock.py',8),
                (io/'tests/ef1','test_status*.py',18), (worker/'tests','test_ef1_status.py',19),
                (io/'tests/ef1','test_path*.py',26), (worker/'tests','test_ef1_paths.py',23),
                (worker/'tests','test_supplied_session.py',13),
                (worker/'tests','test_ef1_reference.py',22),
                (core/'tests/unit','test_ef1_reference.py',99),
                (io/'tests/ef1','test_reference*.py',27)]
            for directory,pattern,expected in suites:
                run([str(python),'-I','-B',str(runner),str(directory),pattern,str(expected)],cwd=root,env=environment)
            demo=output/('demo-'+form)
            demo.mkdir()
            for mode in ('trades','bars_actual_notional'):
                run([str(python),'-I','-B',str(worker/'examples/ef1_anchors.py'),
                    '--output-db',str(demo/(mode+'.duckdb')),'--html',str(demo/(mode+'.html')),
                    '--run-id','synthetic-'+mode,'--mode',mode],cwd=root,env=environment)
            for cutoff,proof in ((10,'present'),(20,'present'),(30,'present'),(40,'present'),
                                 (20,'missing'),(20,'unknown'),(20,'future')):
                name = 'clock-C'+str(cutoff)+'-'+proof
                run([str(python),'-I','-B',str(worker/'examples/ef1_clock.py'),
                    '--output-db',str(demo/(name+'.duckdb')),'--html',str(demo/(name+'.html')),
                    '--run-id','synthetic-'+name,'--cutoff',str(cutoff),'--proof',proof],
                    cwd=root,env=environment)
            for case in ('S01','S02','S03'):
                name = 'status-'+case
                run([str(python),'-I','-B',str(worker/'examples/ef1_status.py'),
                    '--output-db',str(demo/(name+'.duckdb')),'--html',str(demo/(name+'.html')),
                    '--run-id','synthetic-'+name,'--case',case],cwd=root,env=environment)
            for script,case in (('ef1_path_gap.py','G01'),('ef1_path_gap.py','G02'),
                                ('ef1_path_gap.py','G03'),('ef1_path_drive.py','D01')):
                name = 'path-'+case
                run([str(python),'-I','-B',str(worker/'examples'/script),
                    '--output-db',str(demo/(name+'.duckdb')),'--html',str(demo/(name+'.html')),
                    '--run-id','synthetic-'+name,'--case',case],cwd=root,env=environment)
            for script,case in (('ef1_reference_classification.py','R01'),
                                ('ef1_reference_classification.py','R02'),('ef1_reference_action.py','R03')):
                name = 'reference-'+case
                run([str(python),'-I','-B',str(worker/'examples'/script),
                    '--output-db',str(demo/(name+'.duckdb')),'--html',str(demo/(name+'.html')),
                    '--run-id','synthetic-'+name,'--case',case],cwd=root,env=environment)
            receipt['forms'].append({'form':form,'status':'passed','skipped':0,
                'suites':[
                    {'layer':layer, 'directory':directory.relative_to(source).as_posix(),
                     'pattern':pattern, 'expected':expected}
                    for directory,pattern,expected in suites
                    for layer,source in (('core',core),('io',io),('workers',worker))
                    if directory.is_relative_to(source)],
                'packages':{name:version for _,name,version in projects}})
            (output/'qualification.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    receipt['status']='passed'
    (output/'qualification.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':
    main()
