"""Machine-readable actual test/skip ledger for fresh supplied-input installs."""
import json
import os
from pathlib import Path
import sys
import unittest


def main():
    tests = Path(sys.argv[1]); combined = sys.argv[3] == '1'
    sys.path.insert(0, str(tests))
    suite = unittest.defaultTestLoader.discover(str(tests), pattern='test_supplied_*.py')
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    skipped = [{'test': case.id(), 'reason': reason} for case, reason in result.skipped]
    allowed = []
    for item in skipped:
        identity = item['test']
        assert (not combined and identity.startswith('test_supplied_intraday.')) or (
            os.name != 'nt' and identity.startswith('test_supplied_native.')), ('unexpected skip', item)
        allowed.append(identity)
    assert any(name.startswith('test_supplied_intraday.') for name in allowed) == (not combined)
    assert any(name.startswith('test_supplied_native.') for name in allowed) == (os.name != 'nt')
    report = {'methods': result.testsRun, 'skipped': skipped, 'failures': len(result.failures),
        'errors': len(result.errors), 'successful': result.wasSuccessful()}
    Path(sys.argv[2]).write_bytes((json.dumps(report, sort_keys=True, indent=2)+'\n').encode())
    assert result.wasSuccessful()


if __name__ == '__main__':
    main()
