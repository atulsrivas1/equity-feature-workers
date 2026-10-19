"""Raw adversarial archive members must fail before extraction/materialization."""
import base64
import io
import os
from pathlib import Path
import stat
import sys
import tarfile
import tempfile
import unittest
import warnings
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from supplied_archives import raw_files, validate_record, entry_points


class SuppliedArchives(unittest.TestCase):
    def wheel(self, folder, entries):
        path = Path(folder) / 'fixture.whl'
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            with zipfile.ZipFile(path, 'w') as archive:
                for name, mode, content in entries:
                    info = zipfile.ZipInfo(name); info.external_attr = mode << 16
                    archive.writestr(info, content)
        return path

    def test_closed_positive_raw_payload(self):
        with tempfile.TemporaryDirectory() as folder:
            path = self.wheel(folder, [('pkg/a.py', stat.S_IFREG | 0o644, b'original')])
            self.assertEqual(raw_files(path, {'pkg/a.py'}), {'pkg/a.py': b'original'})

    def test_duplicate_case_collision_unknown_and_missing_members(self):
        for entries, expected in [
            ([('pkg/a.py', 0o644, b'a')] * 2, {'pkg/a.py'}),
            ([('pkg/a.py', 0o644, b'a'), ('PKG/a.py', 0o644, b'b')], {'pkg/a.py', 'PKG/a.py'}),
            ([('pkg/a.py', 0o644, b'a'), ('extra.py', 0o644, b'b')], {'pkg/a.py'}),
            ([('pkg/a.py', 0o644, b'a')], {'pkg/a.py', 'pkg/b.py'}),
        ]:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(AssertionError):
                    raw_files(self.wheel(folder, entries), expected)

    def test_traversal_absolute_backslash_and_environment_names(self):
        for name in ('../a.py', '/a.py', 'pkg/../a.py', 'pkg\\a.py', 'pkg/.env', 'C:/a.py', 'pkg/__pycache__/a.py'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(AssertionError):
                    raw_files(self.wheel(folder, [(name, 0o644, b'a')]), {name})

    def test_executable_symlink_and_special_members(self):
        for mode in (stat.S_IFREG | 0o755, stat.S_IFLNK | 0o777, stat.S_IFCHR | 0o644):
            with tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(AssertionError):
                    raw_files(self.wheel(folder, [('pkg/a.py', mode, b'a')]), {'pkg/a.py'})

    def test_raw_nul_name_truncation_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = self.wheel(folder, [('pkg/a.pyZbad', 0o644, b'a')])
            path.write_bytes(path.read_bytes().replace(b'pkg/a.pyZbad', b'pkg/a.py\0bad'))
            with self.assertRaises(AssertionError):
                raw_files(path, {'pkg/a.py'})

    def test_tar_links_rejected_without_extracting(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'fixture.tar.gz'
            with tarfile.open(path, 'w:gz') as archive:
                member = tarfile.TarInfo('pkg/a.py'); member.type = tarfile.SYMTYPE; member.linkname = '../../outside'
                archive.addfile(member)
            with self.assertRaises(AssertionError):
                raw_files(path, {'pkg/a.py'})
            self.assertFalse((Path(folder) / 'pkg').exists())

    def test_record_payload_and_owning_row_tampering(self):
        import hashlib
        content = b'original'
        encoded = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b'=').decode()
        files = {'pkg/a.py': content, 'pkg.dist-info/RECORD': f'pkg/a.py,sha256={encoded},8\npkg.dist-info/RECORD,,\n'.encode()}
        validate_record(files, 'pkg.dist-info/RECORD')
        for altered in ({**files, 'pkg/a.py': b'changed!'}, {**files, 'extra': b'a'},
                        {**files, 'pkg.dist-info/RECORD': files['pkg.dist-info/RECORD'] + b'pkg/a.py,,\n'}):
            with self.assertRaises(AssertionError):
                validate_record(altered, 'pkg.dist-info/RECORD')

    def test_only_exact_existing_console_entrypoint(self):
        expected = b'[console_scripts]\nequity-feature-worker = equity_feature_workers.cli:main\n'
        self.assertEqual(entry_points({'scripts': {'equity-feature-worker': 'equity_feature_workers.cli:main'}}), expected)
        with self.assertRaises(AssertionError):
            entry_points({'scripts': {'equity-feature-worker': 'unrelated:execute'}})


if __name__ == '__main__':
    unittest.main()
