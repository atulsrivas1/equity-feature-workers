"""Independent regression: dirty/untracked source cannot enter committed builds."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

path=Path(__file__).resolve().parents[1]/"tools/build_foundation.py"
spec=importlib.util.spec_from_file_location("foundation_builder",path)
builder=importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)

class Provenance(unittest.TestCase):
    def test_only_declared_committed_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/"repo";root.mkdir()
            def git(*args):return subprocess.check_output(["git",*args],cwd=root,text=True).strip()
            git("init","-b","main");git("config","user.name","Synthetic fixture");git("config","user.email","fixture@example.invalid")
            source=root/"packages/example/src/example";source.mkdir(parents=True)
            (source/"__init__.py").write_text("accepted = 1\n",encoding="utf-8")
            git("add",".");git("commit","-m","synthetic accepted source");commit=git("rev-parse","HEAD")
            (source/"untracked.py").write_text("unexpected = 2\n",encoding="utf-8")
            self.assertEqual(git("diff",commit),"")
            (source/"__init__.py").write_text("modified = 3\n",encoding="utf-8")
            output=builder.snapshot(root,commit,Path(temporary)/"snapshot")
            self.assertEqual((output/"packages/example/src/example/__init__.py").read_text(encoding="utf-8"),"accepted = 1\n")
            self.assertFalse((output/"packages/example/src/example/untracked.py").exists())

if __name__=="__main__":unittest.main()
