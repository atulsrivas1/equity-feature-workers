"""Independent invalid inventory fixtures; no crypto/product/process invocation."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
import base64
import csv
import io
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests/resilience'))
sys.path.insert(0, str(ROOT / 'tools'))
from certificate_pins import read_pins
from build_resilience import safe_relative, wheel_archive_record
from run_qualification import case_failure_summary


class FailureDiagnostics(unittest.TestCase):
    def test_stderr_without_nonzero_exit_and_private_payload_are_distinct(self):
        private = b'PRIVATE_SYNTHETIC_TOKEN=C:\\private\\key.pem\nsecret fixture value'
        actual = case_failure_summary('EQ083_STATE_PROBE.txt', 0, private, private)
        self.assertEqual(actual, dict(case_group='STATE', exit_code=0,
            nonzero_exit=False, stderr_present=True, stdout_over_limit=False))
        self.assertNotIn('PRIVATE_SYNTHETIC', json.dumps(actual))
        self.assertNotIn('key.pem', json.dumps(actual))

    def test_windows_unsigned_exit_and_output_bound_have_independent_flags(self):
        self.assertEqual(case_failure_summary('EQ083_REFERENCE_FAULT_PROBE.txt',
            3221225477, b'', b''), dict(case_group='REFERENCE_FAULT',
            exit_code=3221225477, nonzero_exit=True, stderr_present=False,
            stdout_over_limit=False))
        at_limit = case_failure_summary('EQ083_REMAINING_PROBE_CORRECTIVE.txt',
                                       -1, b'x' * 262144, b'')
        self.assertFalse(at_limit['stdout_over_limit'])
        over_limit = case_failure_summary('EQ083_REMAINING_PROBE_CORRECTIVE.txt',
                                         -1, b'x' * 262145, b'')
        self.assertEqual(over_limit, dict(case_group='REMAINING', exit_code=-1,
            nonzero_exit=True, stderr_present=False, stdout_over_limit=True))

    def test_unknown_case_paths_and_wrong_scalar_types_never_echo_arguments(self):
        values = [('C:\\private\\EQ083_STATE_PROBE.txt', 1, b'', b''),
                  ('EQ083_STATE_PROBE.txt\nPRIVATE', 1, b'', b''),
                  ('EQ083_STATE_PROBE.txt', True, b'', b''),
                  ('EQ083_STATE_PROBE.txt', 2 ** 32, b'', b''),
                  ('EQ083_STATE_PROBE.txt', -(2 ** 31) - 1, b'', b''),
                  ('EQ083_STATE_PROBE.txt', 1, 'PRIVATE', b''),
                  ('EQ083_STATE_PROBE.txt', 1, b'', bytearray(b'PRIVATE'))]
        for args in values:
            with self.subTest(types=tuple(type(v).__name__ for v in args)):
                with self.assertRaisesRegex(ValueError, '^owned failure summary arguments$'):
                    case_failure_summary(*args)


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
        for value in ('', '/owned', '../owned', 'docs/../owned', 'docs//owned', 'C:owned', 'docs\\owned', 'docs/a\x00', 'docs/a\n', True):
            with self.subTest(value=value), self.assertRaises(ValueError):safe_relative(value)


class VendoredWheelRecord(unittest.TestCase):
    def wheel(self, path, *, drift=False, second_root=False):
        files = {'tool/__init__.py': b'# synthetic tool\n',
                 'tool/_vendor/library-1.dist-info/RECORD': b'vendored synthetic metadata\n'}
        if second_root:files['other-1.dist-info/RECORD'] = b''
        rows = [[name, 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode(), str(len(raw))]
                for name, raw in files.items()]
        rows.append(['tool-1.dist-info/RECORD', '', ''])
        stream = io.StringIO();csv.writer(stream).writerows(rows)
        files['tool-1.dist-info/RECORD'] = stream.getvalue().encode()
        if drift:files['tool/_vendor/library-1.dist-info/RECORD'] = b'substituted vendor metadata'
        with zipfile.ZipFile(path, 'w') as archive:
            for name, raw in files.items():archive.writestr(name, raw)

    def test_vendored_record_is_verified_and_second_root_denied(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'synthetic.whl'
            self.wheel(path);wheel_archive_record(path)
            self.wheel(path, drift=True)
            with self.assertRaisesRegex(ValueError, 'file drift'):wheel_archive_record(path)
            self.wheel(path, second_root=True)
            with self.assertRaisesRegex(ValueError, 'root wheel RECORD'):wheel_archive_record(path)

    def test_original_zip_nul_spelling_cannot_alias_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'synthetic.whl'
            digest = 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(b'x').digest()).rstrip(b'=').decode()
            with zipfile.ZipFile(path, 'w') as archive:
                archive.writestr('tool/ax', b'x')
                archive.writestr('tool-1.dist-info/RECORD', 'tool/a,' + digest + ',1\ntool-1.dist-info/RECORD,,\n')
            raw = path.read_bytes();self.assertEqual(raw.count(b'tool/ax'), 2)
            path.write_bytes(raw.replace(b'tool/ax', b'tool/a\x00'))
            with zipfile.ZipFile(path) as archive:
                item = archive.infolist()[0]
                self.assertEqual(item.filename, 'tool/a')
                self.assertEqual(item.orig_filename, 'tool/a\x00')
            with self.assertRaisesRegex(ValueError, 'original ZIP spelling'):wheel_archive_record(path)


if __name__ == '__main__':
    unittest.main()
