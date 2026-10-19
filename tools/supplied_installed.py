"""Inspect each selected installed distribution, including generated RECORD rows."""
from __future__ import annotations
import base64
import csv
import hashlib
import importlib
from importlib.metadata import distribution
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import urlsplit, unquote
import zipfile


def main():
    selected=json.loads(Path(sys.argv[1]).read_bytes())
    root=Path(sys.prefix).resolve();report={}
    for item in selected:
        module=item['module'];archive=Path(item['archive']).resolve()
        with zipfile.ZipFile(archive) as wheel:
            expected={name:wheel.read(name) for name in wheel.namelist()}
        dist=distribution(module.replace('_','-'));info=module+'-'+dist.version+'.dist-info'
        origin=Path(importlib.import_module(module).__file__).resolve()
        site=Path(dist.locate_file('')).resolve()
        assert site.is_relative_to(root) and site.name=='site-packages'
        assert origin==site/module/'__init__.py'
        generated={info+'/'+name for name in ('INSTALLER','REQUESTED','direct_url.json')}
        wrappers={}
        if module=='equity_feature_workers':
            assert distribution('pip').version=='25.1.1' and os.environ.get('SOURCE_DATE_EPOCH')=='1700000000'
            from pip._vendor.distlib.scripts import ScriptMaker
            # Rebuild the installer-owned launcher with its exact pinned
            # generator, executable and reproducible embedded ZIP timestamp.
            with tempfile.TemporaryDirectory(prefix='launcher-',dir=root) as temporary:
                maker=ScriptMaker(None,temporary)
                maker.executable=sys.executable;maker.variants={''};maker.clobber=True
                created=maker.make('equity-feature-worker = equity_feature_workers.cli:main')
                assert len(created)==1
                candidate=Path(created[0]);name='equity-feature-worker.exe' if os.name=='nt' else 'equity-feature-worker'
                assert candidate.name==name
                actual_path=root/('Scripts' if os.name=='nt' else 'bin')/name
                relative=os.path.relpath(actual_path,site).replace('\\','/')
                wrappers[relative]=candidate.read_bytes()
            generated.update(wrappers)
        names=set(expected)|generated
        declared=[str(name).replace('\\','/') for name in dist.files]
        assert len(declared)==len(set(declared)) and set(declared)==names
        actual={}
        for name in names:
            path=site/name
            assert not path.is_symlink() and path.resolve().is_relative_to(root)
            if name not in wrappers:assert path.resolve().is_relative_to(site)
            actual[name]=path.read_bytes()
            if name in wrappers:assert actual[name]==wrappers[name], 'installer launcher drift'
        for directory in (site/module,site/info):
            found=set()
            for path in directory.rglob('*'):
                assert not path.is_symlink()
                if path.is_file():found.add(path.relative_to(site).as_posix())
            assert found=={name for name in names if name.startswith(directory.name+'/')}, 'unrecorded installed file'
        for name,body in expected.items():
            if name!=info+'/RECORD':assert actual[name]==body
        assert actual[info+'/INSTALLER']==b'pip\n' and actual[info+'/REQUESTED']==b''
        direct=json.loads(actual[info+'/direct_url.json'])
        sha=hashlib.sha256(archive.read_bytes()).hexdigest()
        assert set(direct)=={'archive_info','url'}
        assert direct['archive_info']=={'hash':'sha256='+sha,'hashes':{'sha256':sha}}
        url=urlsplit(direct['url']);assert url.scheme=='file' and not url.query and not url.fragment
        # Compare against the actual selected wheel, including Windows drive URI.
        assert unquote(url.path)==unquote(urlsplit(archive.as_uri()).path) and url.netloc==urlsplit(archive.as_uri()).netloc
        rows=list(csv.reader(io.StringIO(actual[info+'/RECORD'].decode())))
        assert len(rows)==len(actual) and all(len(row)==3 for row in rows)
        assert {row[0] for row in rows}==set(actual)
        for name,value,size in rows:
            if name==info+'/RECORD':assert value==size==''
            else:
                encoded=base64.urlsafe_b64encode(hashlib.sha256(actual[name]).digest()).rstrip(b'=').decode()
                assert value=='sha256='+encoded and size==str(len(actual[name]))
        # Private installation directory appears only in the raw local bytes,
        # never in the public report. The file digest still binds those bytes.
        report[module]={'version':dist.version,'origin':origin.relative_to(root).as_posix(),
            'selected_wheel_sha256':sha,'files':{name:{'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)}
            for name,body in sorted(actual.items())}}
    print(json.dumps(report,sort_keys=True))


if __name__=='__main__':main()
