"""Owned ZIP adversaries for official dependency file-RECORD qualification."""
import base64,csv,hashlib,io,sys,tempfile,unittest,warnings,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tools'))
from build_mcp import reference_record


class Records(unittest.TestCase):
    def archive(self,name='pkg.py',*,duplicate_file=False,duplicate_record=False,unrecorded=False,wrong_digest=False):
        output=io.BytesIO();record='owned-1.dist-info/RECORD';data=b'owned correct'
        digest='sha256='+base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode('ascii')
        rows=io.StringIO();writer=csv.writer(rows);writer.writerow([name,digest if not wrong_digest else 'sha256=wrong',len(data)]);writer.writerow([record,'',''])
        with warnings.catch_warnings():
            warnings.simplefilter('ignore',UserWarning)
            with zipfile.ZipFile(output,'w') as archive:
                archive.writestr('owned/',b'')
                if duplicate_file:archive.writestr(name,b'unchecked prior bytes')
                archive.writestr(name,data)
                archive.writestr(record,rows.getvalue())
                if duplicate_record:archive.writestr(record,rows.getvalue())
                if unrecorded:archive.writestr('unrecorded.py',b'not hashed')
        return output.getvalue()

    def check(self,raw):
        with tempfile.TemporaryDirectory(prefix='owned-record-') as directory:
            path=Path(directory)/'owned.whl';path.write_bytes(raw);reference_record(path)

    def test_valid_file_record_and_explicit_directory(self):self.check(self.archive())

    def test_duplicate_physical_files_and_records_denied(self):
        for options in ({'duplicate_file':True},{'duplicate_record':True}):
            with self.subTest(options=options),self.assertRaises(AssertionError):self.check(self.archive(**options))

    def test_portable_drive_unc_absolute_and_aliases_denied(self):
        for name in ('C:/escape.py','C:escape.py','/escape.py','//host/share.py','\\\\host\\share.py','../escape.py','pkg/../escape.py','./pkg.py','pkg//entry.py','pkg/./entry.py','pkg:stream.py'):
            with self.subTest(name=name),self.assertRaises(AssertionError):self.check(self.archive(name))

    def test_unrecorded_content_and_bad_hash_denied(self):
        for options in ({'unrecorded':True},{'wrong_digest':True}):
            with self.subTest(options=options),self.assertRaises(AssertionError):self.check(self.archive(**options))


if __name__=='__main__':unittest.main()
