"""Six predeclared tiny owned diagnostic children; no benchmark or production edit."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import traceback

from qualify_scaling import ROOT, frozen_bytes, native_probe


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work-root', type=Path, required=True)
    args = parser.parse_args()
    root = args.work_root.resolve()
    assert not root.exists() and root != ROOT and ROOT not in root.parents and root not in ROOT.parents
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip()
    names = subprocess.check_output(['git', 'ls-files', '--', 'tools', 'tests', 'packages'], cwd=ROOT).decode().splitlines()
    source = frozen_bytes(head, names)
    root.mkdir(parents=True)
    report = dict(schema='eq088-capability1', head=head, source_sha256=source, records=[], status='running',
                  child_deadline_seconds=15, extra_cases_allowed=False)
    def save():
        (root / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    cases = [(sink, profiled, True) for sink in (True, False) for profiled in (False, True)]
    cases += [(True, False, False), (True, True, False)]
    for index, (sink, profiled, hashes) in enumerate(cases):
        entry = dict(index=index, sink=sink, profiled=profiled, hashes=hashes, status='started')
        report['records'].append(entry)
        save()
        try:
            entry['result'] = native_probe(dict(kind='capability', root=str(root / str(index)),
                sink=sink, profiled=profiled, hashes=hashes), timeout=15)
            entry['status'] = 'returned'
        except Exception as error:
            raw = traceback.format_exc().encode()
            (root / f'private-error-{index}.log').write_bytes(raw)
            entry.update(status='failed', error_type=type(error).__name__,
                         private_error_sha256=hashlib.sha256(raw).hexdigest())
            if getattr(error, '__notes__', ()):
                report['status'] = 'stopped-with-cleanup-notes'
                save()
                raise
        save()
    assert frozen_bytes(head, names) == source
    report['status'] = 'completed-controls-not-automatic-diagnosis'
    save()


if __name__ == '__main__':
    main()
