"""Frozen installed qualification runner; never a public entry/code selector."""
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys


_CASE_LABELS = {
    'EQ083_CERTIFICATE_HELPER.txt': 'CERTIFICATE',
    'EQ083_R07_PROBE.txt': 'R07',
    'EQ083_AUTHORITY_PROBE.txt': 'AUTHORITY',
    'EQ083_STATE_PROBE.txt': 'STATE',
    'EQ083_EPOCH_PROBE.txt': 'EPOCH',
    'EQ083_UNKNOWN_PROBE.txt': 'UNKNOWN',
    'EQ083_VERSION_PROBE.txt': 'VERSION',
    'EQ083_REFERENCE_FAULT_PROBE.txt': 'REFERENCE_FAULT',
    'EQ083_TLS_PROBE.txt': 'TLS',
    'EQ083_REMAINING_PROBE_CORRECTIVE.txt': 'REMAINING',
}


def case_failure_summary(case, returncode, stdout, stderr):
    """Closed synthetic-case metadata only; never echo private output or paths."""
    if (type(case) is not str or case not in _CASE_LABELS
            or type(returncode) is not int or not -(2 ** 31) <= returncode < 2 ** 32
            or type(stdout) is not bytes or type(stderr) is not bytes):
        raise ValueError('owned failure summary arguments')
    return dict(case_group=_CASE_LABELS[case], exit_code=returncode,
                nonzero_exit=returncode != 0, stderr_present=bool(stderr),
                stdout_over_limit=len(stdout) > 262144)


def main():
    if len(sys.argv) != 3 or not sys.flags.isolated or sys.platform not in ('win32', 'linux'):
        raise ValueError('owned qualification runner arguments')
    root, output = (Path(value).resolve() for value in sys.argv[1:])
    if (root / 'packages').exists() or output.exists() or output.parent != root:
        raise ValueError('owned fresh harness/output required')
    with (root / 'resilience/source-inventory.json').open('rb') as stream:
        inventory_raw = stream.read(262145)
    if len(inventory_raw) > 262144:
        raise ValueError('owned source inventory bound')
    inventory = json.loads(inventory_raw)
    if set(inventory) != {'source_inputs', 'case_groups', 'certificate_helper', 'certificate_loader', 'package_source_inputs'}:
        raise ValueError('owned source inventory schema')
    for relative, pin in inventory['source_inputs'].items():
        path = root / relative
        if not path.resolve().is_relative_to(root) or path.is_symlink() or not path.is_file():
            raise ValueError('owned source inventory path')
        with path.open('rb') as stream:
            raw = stream.read(pin['bytes'] + 1)
        if len(raw) != pin['bytes'] or hashlib.sha256(raw).hexdigest() != pin['sha256']:
            raise ValueError('owned source inventory drift')
    import equity_feature_client, equity_feature_mcp, equity_feature_service, equity_feature_workers
    import equity_feature_io_sdk, equity_features, mcp
    modules = (equity_feature_client, equity_feature_mcp, equity_feature_service,
               equity_feature_workers, equity_feature_io_sdk, equity_features, mcp)
    if not all('site-packages' in Path(module.__file__).resolve().parts for module in modules):
        raise ValueError('owned installed product origins')
    if importlib.metadata.version('mcp') != '1.30.0':
        raise ValueError('owned reference version')
    sys.path.insert(0, str(root / 'resilience'))
    from private_directory import private_directory
    from bounded_capture import capture
    from installed_records import installed_records
    private_directory(root.parent)
    installation = installed_records(inventory['package_source_inputs'], root.parent / 'venv')
    environment = dict(os.environ, PYTHONNOUSERSITE='1')
    def invoke(path, args, seconds):
        try:
            returncode, stdout, stderr = capture([sys.executable, '-I', str(path), *map(str, args)],
                                                cwd=root, env=environment, seconds=seconds)
        except BaseException as error:
            # A timeout is failed/unknown cleanup, never permission to continue or replace.
            for suffix in ('stdout', 'stderr'):
                try:
                    raw = getattr(error, suffix, b'')
                    if type(raw) is bytes:
                        with (root / (path.name + '.' + suffix + '.log')).open('xb') as stream:
                            stream.write(raw[:262144])
                except BaseException:pass
            try:
                failure = dict(stage='capture', case=path.name, cleanup='UNCONFIRMED_DENY_REUSE')
                (root / 'FAILED.json').write_text(json.dumps(failure), encoding='utf-8')
            except BaseException:pass
            # Diagnostics are best effort during a failure; they cannot replace its cause.
            raise ValueError('owned qualification capture failed; cleanup unconfirmed') from error
        for suffix, raw in (('stdout', stdout), ('stderr', stderr)):
            # Preserve bounded raw diagnostics privately; successful public artifacts exclude these logs.
            with (root / (path.name + '.' + suffix + '.log')).open('xb') as stream:
                stream.write(raw[:262144])
        if returncode != 0 or stderr or len(stdout) > 262144:
            (root / 'FAILED.json').write_text(json.dumps(dict(stage='case', case=path.name,
                exit_code=returncode, cleanup='NOT_INFERRED_FROM_FAILED_RUN')), encoding='utf-8')
            summary = case_failure_summary(path.name, returncode, stdout, stderr)
            raise ValueError('owned qualification case failed; diagnostic=' +
                             json.dumps(summary, sort_keys=True, separators=(',', ':')))
        lines = stdout.splitlines()
        if not lines or any(not line for line in lines):
            raise ValueError('owned qualification report framing')
        return [json.loads(line) for line in lines]
    # All helper/entry bytes are frozen above before importing products or generating material.
    certificates = root / 'certificates'
    certificate_report = invoke(root / inventory['certificate_helper'], [certificates], 15)
    if len(certificate_report) != 1 or set(certificate_report[0]) != {'valid', 'wrong'}:
        raise ValueError('owned certificate helper report')
    pins = {}
    for role in ('valid', 'wrong'):
        pins[role] = {}
        for kind in ('cert', 'key'):
            path = certificates / (role + '-' + kind + '.pem')
            if path.is_symlink():
                raise ValueError('owned private certificate link')
            with path.open('rb') as stream:
                raw = stream.read(8193)
            if not 0 < len(raw) <= 8192:
                raise ValueError('owned certificate file bound')
            pin = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
            if kind == 'cert' and pin['sha256'] != certificate_report[0][role]['sha256']:
                raise ValueError('owned certificate report mismatch')
            pins[role][kind] = pin
    pin_bytes = json.dumps(pins, sort_keys=True, separators=(',', ':')).encode('ascii')
    with (certificates / 'pins.json').open('xb') as stream:
        if stream.write(pin_bytes) != len(pin_bytes):
            raise ValueError('owned certificate pin short write')
    pin_sha = hashlib.sha256(pin_bytes).hexdigest()
    groups = []
    for group in inventory['case_groups']:
        args = [root, pin_sha] if group['name'] == 'TLS' else [root]
        reports = invoke(root / group['parent'], args, group['timeout_seconds'])
        groups.append(dict(name=group['name'], reports=reports))
    result = dict(schema='eq083.runtime.v1', platform=sys.platform,
                  python=sys.version.split()[0], groups=groups,
                  public_certificates=certificate_report[0],
                  installed_records=installation,
                  installed_versions={name: importlib.metadata.version(name) for name in
                      ('equity-feature-client', 'equity-feature-mcp', 'equity-feature-service',
                       'equity-feature-workers', 'duckdb', 'numpy', 'mcp', 'cryptography')},
                  source_inventory_sha256=hashlib.sha256(inventory_raw).hexdigest(),
                  limits_scope='trusted cooperative tested cases; no whole-platform physical settlement')
    raw = (json.dumps(result, sort_keys=True, indent=2) + '\n').encode('utf-8')
    # Deliberately export only reports/public certificate pins, never the private material directory.
    with output.open('xb') as stream:
        if stream.write(raw) != len(raw):
            raise ValueError('owned report short write')
    print('EQ083 real installed HTTP/client/official-reference qualification completed')


if __name__ == '__main__':
    main()
