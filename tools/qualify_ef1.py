"""EF1 candidate installed-artifact checks; original historical runners are unchanged.

Runs only when explicitly invoked in an admitted execution environment. Tests are
mandatory, with no skipped EF1 tests accepted as qualification. This checks the
anchor, supplied-clock, current-state status, sampled path supplied reference, period, extended, observable, daily-observation, gap-endpoint and regular completed-BAR price slices, not the full EF1 release or its inventory/performance gates.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
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


_LOG_ROOT: Path | None = None
_COMMAND_INDEX = 0


def run(args: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    """Persist native child output and actual exit before any later hash audit."""
    global _COMMAND_INDEX
    assert _LOG_ROOT is not None
    _COMMAND_INDEX += 1
    command_dir = _LOG_ROOT / f"{_COMMAND_INDEX:04d}"
    command_dir.mkdir(exist_ok=False)
    child_env = dict(env, EF1_SUITE_RECEIPT=str(command_dir / "suite-result.json"))
    retained_roots = {}
    # Preserve new composition outputs beside their native command receipts.
    # The I/O suite has an explicit hook; the worker uses stdlib mkdtemp.
    if 'test_regular_bar_prices_composition.py' in args:
        retained = command_dir / "regular-bar-io-output"
        retained.mkdir(exist_ok=False)
        child_env['EF1_REGULAR_BAR_IO_OUTPUT_ROOT'] = str(retained)
        retained_roots['io'] = str(retained)
    if 'test_ef1_regular_bar_prices.py' in args:
        # Only the worker suite has this pattern with a leaf tests directory;
        # identify its tests directory explicitly, without changing package paths.
        if any(Path(value).name == 'tests' for value in args):
            retained = command_dir / "regular-bar-worker-output"
            retained.mkdir(exist_ok=False)
            child_env.update(TMPDIR=str(retained), TMP=str(retained), TEMP=str(retained))
            retained_roots['workers'] = str(retained)
    started = datetime.now(timezone.utc).isoformat()
    running = {"argv": args, "cwd": str(cwd), "started_utc": started, "retained_output_roots": retained_roots}
    (command_dir / "RUNNING.json").write_text(json.dumps(running, indent=2)+"\n", encoding="utf-8")
    with (command_dir / "native-output.log").open("xb") as log:
        result = subprocess.run(args, cwd=cwd, env=child_env, stdout=log, stderr=subprocess.STDOUT)
    actual = dict(running, actual_exit_code=result.returncode,
                  ended_utc=datetime.now(timezone.utc).isoformat())
    (command_dir / "ACTUAL_EXIT.json").write_text(json.dumps(actual, indent=2)+"\n", encoding="utf-8")
    actual["native_output_sha256"] = hashlib.sha256((command_dir / "native-output.log").read_bytes()).hexdigest()
    (command_dir / "FINAL_COMMAND_RECEIPT.json").write_text(json.dumps(actual, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({"command": _COMMAND_INDEX, "actual_exit": result.returncode,
                      "native_log": str(command_dir / "native-output.log")}), flush=True)
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, args)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker-root', type=Path)
    parser.add_argument('--core-root', type=Path, required=True)
    parser.add_argument('--io-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    worker = args.worker_root.resolve() if args.worker_root else Path(__file__).resolve().parents[1]
    core, io = args.core_root.resolve(), args.io_root.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    global _LOG_ROOT
    _LOG_ROOT = output / 'commands'
    _LOG_ROOT.mkdir(exist_ok=False)
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
                       PYTHONHASHSEED='0', SOURCE_DATE_EPOCH='1700000000',
                       OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1')
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
    receipt = {'scope':'EF1 anchor/clock/current-state status/sampled path/reference/period/extended/observable/daily-observations/gap-endpoint/regular-bar-prices installed artifacts and legacy numerical regression only; not full EF1 acceptance',
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
from equity_feature_workers import equity_sessions, equity_clock, equity_status, equity_path_gap, equity_path_drive, equity_reference_classification, equity_reference_action, equity_period, equity_extended, equity_opening_trade, equity_opening_trade_seconds, equity_status_history, equity_daily_observations, equity_gap_endpoint
from equity_feature_duckdb import ef1 as ef1_inputs
from equity_features.ef1 import _regular_bar_prices
from equity_feature_duckdb import regular_bar_prices as regular_bar_price_inputs
from equity_feature_workers import equity_regular_bar_prices
for module in (equity_features,equity_feature_workers,equity_feature_duckdb,ef1,equity_sessions,equity_clock,equity_status,equity_path_gap,equity_path_drive,equity_reference_classification,equity_reference_action,equity_period,equity_extended,equity_opening_trade,equity_opening_trade_seconds,equity_status_history,equity_daily_observations,equity_gap_endpoint,ef1_inputs,_regular_bar_prices,regular_bar_price_inputs,equity_regular_bar_prices):
    assert pathlib.Path(module.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve())
''',encoding='utf-8')
            run([str(python),'-I','-B',str(probe)],cwd=root,env=environment)
            # A custom runner refuses all skipped/empty suites. Test source is
            # explicitly selected, but candidate package imports remain installed.
            runner=root/'run_tests.py'
            runner.write_text('''import json, multiprocessing, os, pathlib, sys, unittest
def main():
    start, pattern, expected_text = sys.argv[1:]
    expected = int(expected_text)
    sys.path.insert(0,start)
    suite=unittest.defaultTestLoader.discover(start,pattern=pattern)
    assert expected>0 and suite.countTestCases()==expected, "Required suite census changed"
    def ids(value):
        if isinstance(value, unittest.TestSuite):
            return [item for child in value for item in ids(child)]
        return [value.id()]
    case_ids = ids(suite)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    report = {"pattern": pattern, "expected": expected, "case_ids": case_ids,
        "tests_run": result.testsRun, "skipped": len(result.skipped),
        "expected_failures": len(result.expectedFailures), "unexpected_successes": len(result.unexpectedSuccesses),
        "errors": len(result.errors), "failures": len(result.failures), "successful": result.wasSuccessful()}
    pathlib.Path(os.environ["EF1_SUITE_RECEIPT"]).write_text(json.dumps(report, indent=2)+"\\n", encoding="utf-8")
    assert result.testsRun==expected and not result.skipped and not result.expectedFailures and not result.unexpectedSuccesses and result.wasSuccessful(), "Required suite failed/skipped/quarantined"
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
                (io/'tests/ef1','test_reference*.py',27),
                (worker/'tests','test_ef1_period.py',17),
                (io/'tests/ef1','test_period_phase*.py',17),
                (core/'tests/unit','test_ef1_period_phase.py',75),
                (core/'tests/unit','test_ef1_observable.py',111),
                (worker/'tests','test_ef1_observables.py',24),
                (io/'tests/ef1','test_observable*.py',18),
                (core/'tests/unit','test_ef1_daily_observations.py',10),
                (io/'tests/ef1','test_daily_observations_inputs.py',9),
                (io/'tests/ef1','test_daily_observations_composition.py',2),
                (worker/'tests','test_ef1_daily_observations.py',14),
                (core/'tests/unit','test_ef1_gap_endpoint.py',6),
                (io/'tests/ef1','test_gap_endpoint_inputs.py',7),
                (io/'tests/ef1','test_gap_endpoint_composition.py',3),
                (worker/'tests','test_ef1_gap_endpoint.py',8),
                (core/'tests/unit','test_ef1_regular_bar_prices.py',9),
                (io/'tests/ef1','test_regular_bar_prices_inputs.py',8),
                (io/'tests/ef1','test_regular_bar_prices_composition.py',2),
                (worker/'tests','test_ef1_regular_bar_prices.py',9)]
            assert len(suites) == 34 and sum(row[2] for row in suites) == 752
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
            for script in ('ef1_period.py', 'ef1_extended.py'):
                name = script.removesuffix('.py')
                run([str(python),'-I','-B',str(worker/'examples'/script),
                    '--output-db',str(demo/(name+'.duckdb')),'--html',str(demo/(name+'.html')),
                    '--run-id','synthetic-'+name],cwd=root,env=environment)
            for script in ('ef1_opening_trade.py','ef1_opening_trade_seconds.py','ef1_status_history.py','ef1_status_history_missing_capability.py'):
                name = script.removesuffix('.py')
                run([str(python),'-I','-B',str(worker/'examples'/script),
                    '--output-db',str(demo/(name+'.duckdb')),'--html',str(demo/(name+'.html')),
                    '--run-id','synthetic-'+name],cwd=root,env=environment)
            for case in ('V235-literal-last252', 'V201-observed-not-scheduled',
                         'V223-prior-skips-invalid-populated-period', 'V240-full-leap-year-literal-history'):
                name = 'daily-'+case
                run([str(python),'-I','-B',str(worker/'examples/ef1_daily_observations.py'),
                    '--corpus',str((worker/'tests/ef1_daily_observations_oracle.json').resolve()),
                    '--case',case,'--output-db',str(demo/(name+'.duckdb')),
                    '--html',str(demo/(name+'.html')),'--run-id','synthetic-'+name],
                    cwd=root,env=environment)
            for case in ('E01_up_reversal', 'E26_bar_close_at_cutoff'):
                name = 'endpoint-' + case
                run([str(python),'-I','-B',str(worker/'examples/ef1_gap_endpoint.py'),
                    '--case',case,'--output-db',str(demo/(name+'.duckdb')),
                    '--html',str(demo/(name+'.html')),'--run-id','synthetic-'+name],
                    cwd=root,env=environment)
            for case in ('R01_two_independent_endpoints', 'R08_unknown_before_first'):
                name = 'regular-bar-' + case
                run([str(python),'-I','-B',str(worker/'examples/ef1_regular_bar_prices.py'),
                    '--case',case,'--output-db',str(demo/(name+'.duckdb')),
                    '--html',str(demo/(name+'.html')),'--run-id','synthetic-'+name],
                    cwd=root,env=environment)
            receipt['forms'].append({'form':form,'status':'passed','skipped':0,
                'suites':[
                    {'layer':layer, 'directory':directory.relative_to(source).as_posix(),
                     'pattern':pattern, 'expected':expected}
                    for directory,pattern,expected in suites
                    for layer,source in (('core',core),('io',io),('workers',worker))
                    if directory.is_relative_to(source)],
                'packages':{name:version for _,name,version in projects}})
            (output/'qualification.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    for name, source in (('core', core), ('io', io), ('workers', worker)):
        assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip() == sources[name]
        assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],cwd=source,text=True).strip()
    assert _COMMAND_INDEX == 292
    receipt['native_commands'] = _COMMAND_INDEX
    receipt['required_suites_per_form'] = 34
    receipt['required_methods_per_form'] = 752
    receipt['required_demos_per_form'] = 33
    receipt['status']='passed'
    (output/'qualification.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':
    main()
