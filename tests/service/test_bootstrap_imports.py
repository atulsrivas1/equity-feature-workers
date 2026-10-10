"""No native initialization before the future process boundary; public API parity."""
from pathlib import Path
import subprocess
import sys
import sysconfig
import unittest

import equity_feature_service


class BootstrapImports(unittest.TestCase):
    def test_isolated_bootstrap_without_native_dependencies_then_public_exports(self):
        root = str(Path(equity_feature_service.__file__).resolve().parent.parent)
        site = sysconfig.get_path('purelib')
        code = '''import sys
sys.path.insert(0, sys.argv[1])
import equity_feature_service as service
assert service.__version__ == "0.1.0a3"
assert not any(name.startswith(("equity_feature_contracts", "equity_feature_io", "equity_feature_workers", "equity_features", "duckdb", "jsonschema")) for name in sys.modules)
import site
site.addsitedir(sys.argv[2])
from equity_feature_service import Service, Scope, Credential, Limits
from equity_feature_service.service import Service as direct
assert Service is direct and service.Service is direct
assert Scope("owned", "session", 1, 2).start_ns == 1
assert Credential.provision("owned", "a" * 43, 0, 100).principal == "owned"
assert Limits(1, 1024, 2048, 60000000000).requests == 1
try:
    service.nonexistent_native_export
except AttributeError:
    pass
else:
    raise AssertionError("unknown export")
'''
        result = subprocess.run([sys.executable,'-I','-S','-c',code,root,site],capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)


if __name__ == '__main__':
    unittest.main()
