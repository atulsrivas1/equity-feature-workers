"""Independent invalid inventory fixtures; no crypto/product/process invocation."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests/resilience'))
sys.path.insert(0, str(ROOT / 'tools'))
from certificate_pins import read_pins
from build_resilience import safe_relative


class CertificateInventory(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory).resolve()
        value = {}
        for role in ('valid', 'wrong'):
            value[role] = {}
            for kind in ('cert', 'key'):
                data = (role + '-' + kind + '-synthetic-fixture').encode('ascii')
                (root / (role + '-' + kind + '.pem')).write_bytes(data)
                value[role][kind] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        raw = json.dumps(value, separators=(',', ':')).encode('ascii')
        (root / 'pins.json').write_bytes(raw)
        return root, value, raw

    def test_complete_inventory_and_private_key_substitution(self):
        with tempfile.TemporaryDirectory() as directory:
            root, value, raw = self.fixture(directory)
            self.assertEqual(read_pins(root, hashlib.sha256(raw).hexdigest()), value)
            (root / 'valid-key.pem').write_bytes(b'other-owned-key')
            with self.assertRaisesRegex(ValueError, 'material identity'):
                read_pins(root, hashlib.sha256(raw).hexdigest())

    def test_inventory_drift_duplicate_keys_and_boolean_size(self):
        with tempfile.TemporaryDirectory() as directory:
            root, value, raw = self.fixture(directory)
            digest = hashlib.sha256(raw).hexdigest()
            (root / 'pins.json').write_bytes(raw + b' ')
            with self.assertRaisesRegex(ValueError, 'inventory identity'):
                read_pins(root, digest)
            duplicate = b'{"valid":{},"valid":{},"wrong":{}}'
            (root / 'pins.json').write_bytes(duplicate)
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                read_pins(root, hashlib.sha256(duplicate).hexdigest())
            value['valid']['cert']['bytes'] = True
            raw = json.dumps(value).encode('ascii');(root / 'pins.json').write_bytes(raw)
            with self.assertRaisesRegex(ValueError, 'certificate pin'):
                read_pins(root, hashlib.sha256(raw).hexdigest())

    def test_missing_key_and_wrong_root_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, raw = self.fixture(directory)
            with self.assertRaises(ValueError):read_pins(root, True)
            with self.assertRaises(ValueError):read_pins(root, 'A' * 64)
            (root / 'wrong-key.pem').unlink()
            with self.assertRaises(FileNotFoundError):
                read_pins(root, hashlib.sha256(raw).hexdigest())


class RelocationPaths(unittest.TestCase):
    def test_closed_relative_paths(self):
        self.assertEqual(str(safe_relative('docs/owned.json')), 'docs/owned.json')
        for value in ('', '/owned', '../owned', 'docs/../owned', 'docs//owned', 'C:owned', 'docs\\owned', True):
            with self.subTest(value=value), self.assertRaises(ValueError):safe_relative(value)


if __name__ == '__main__':
    unittest.main()
