"""Closed archive policy for selected supplied-input dependencies and worker.

Inspect all raw members before exposing any bytes for materialization. This is
package integrity validation, not a whole-host or interpreter attestation.
"""
from __future__ import annotations
import base64
import csv
from email.parser import BytesParser
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import stat
import tarfile
import tomllib
import zipfile


def digest(body):
    return hashlib.sha256(body).hexdigest()


def safe_name(name):
    assert type(name) is str and name and '\x00' not in name and '\\' not in name and ':' not in name
    assert not name.startswith('/') and not name.endswith('/')
    assert all(p not in ('', '.', '..') for p in name.split('/'))
    assert not any(p in ('.git', '__pycache__', '.env') for p in name.split('/'))
    assert not name.endswith(('.pyc', '.pyo'))


def raw_files(path, expected_files):
    """Validate namespace, types and modes before reading/extracting members."""
    seen=set(); folded=set(); files={}; directories=set()
    ancestors={str(parent) for name in expected_files for parent in PurePosixPath(name).parents
               if str(parent)!='.'}
    def entry(name, directory, mode):
        safe_name(name)
        assert name not in seen and name.casefold() not in folded, 'duplicate/case collision'
        seen.add(name);folded.add(name.casefold())
        if directory:
            directory_modes=(0o755,0o775,0o777) if os.name=='nt' else (0o755,0o775)
            assert path.suffix!='.whl' and name in ancestors and mode in directory_modes, ('unexpected directory',name,oct(mode))
            directories.add(name)
        else:
            assert name in expected_files and not mode & 0o7111, 'unexpected/executable file'
    if path.suffix=='.whl':
        with zipfile.ZipFile(path) as archive:
            for item in archive.infolist():
                assert item.orig_filename==item.filename, 'raw ZIP name was truncated'
                assert not item.flag_bits & 1, 'encrypted member'
                mode=item.external_attr>>16
                assert stat.S_IFMT(mode) in (0,stat.S_IFREG), 'nonregular wheel member'
                entry(item.filename,False,stat.S_IMODE(mode))
            files={item.filename:archive.read(item) for item in archive.infolist()}
    else:
        with tarfile.open(path,'r:gz') as archive:
            members=archive.getmembers()
            for item in members:
                assert item.isfile() or item.isdir(), 'link/device/special member'
                entry(item.name.rstrip('/') if item.isdir() else item.name,item.isdir(),item.mode)
            files={item.name:archive.extractfile(item).read() for item in members if item.isfile()}
    assert set(files)==set(expected_files), 'missing archive member'
    assert not set(files)&directories, 'file/directory collision'
    return files


def metadata(body, project, readme):
    # RFC metadata uses LF on the selected Git-source generator. Parse headers
    # independently, refusing duplicate/unknown fields and binding README bytes.
    normalized=body.replace(b'\r\n',b'\n')
    assert body==normalized.replace(b'\n',os.linesep.encode()), 'unexpected generator newlines'
    headers, separator, description=normalized.partition(b'\n\n')
    assert separator and description==readme
    msg=BytesParser().parsebytes(headers+b'\n\n')
    assert not msg.defects
    expected={'Metadata-Version':['2.4'],'Name':[project['name']],
        'Version':[project['version']], 'Summary':[project['description']],
        'Author':[project['authors'][0]['name']], 'License-Expression':['Apache-2.0'],
        'Project-URL':['Repository, '+project['urls']['Repository']],
        'Requires-Python':['<3.13,>=3.12'],'Description-Content-Type':['text/markdown'],
        'License-File':['LICENSE'],'Dynamic':['license-file']}
    deps=list(project['dependencies']);extras=project.get('optional-dependencies',{})
    if extras:expected['Provides-Extra']=list(extras)
    deps += [requirement+'; extra == "'+extra+'"' for extra,values in extras.items() for requirement in values]
    if deps:expected['Requires-Dist']=deps
    assert set(msg.keys())==set(expected)
    assert {key:msg.get_all(key) for key in expected}==expected, 'metadata drift'


def expected_layout(source, module, wheel):
    project=tomllib.loads(source['pyproject.toml'].decode())['project']
    version=project['version'];dist=module+'-'+version+'.dist-info/'
    egg='src/'+module+'.egg-info/'
    package={name:body for name,body in source.items() if name.startswith('src/'+module+'/')}
    assert package and 'src/'+module+'/py.typed' in package
    generated={'PKG-INFO','SOURCES.txt','dependency_links.txt','top_level.txt'}
    if project.get('scripts'):generated.add('entry_points.txt')
    if project['dependencies'] or project.get('optional-dependencies'):generated.add('requires.txt')
    if wheel:
        names={name.removeprefix('src/') for name in package}
        names|={dist+name for name in ('METADATA','WHEEL','top_level.txt','RECORD','licenses/LICENSE')}
        if project.get('scripts'):names.add(dist+'entry_points.txt')
    else:
        names=set(source)|{'PKG-INFO','setup.cfg'}|{egg+name for name in generated}
        names={module+'-'+version+'/'+name for name in names}
    return project,package,dist,egg,names


def inspect(path, source, module):
    wheel=path.suffix=='.whl'
    project,package,dist,egg,names=expected_layout(source,module,wheel)
    expected_name=module+'-'+project['version']+('-py3-none-any.whl' if wheel else '.tar.gz')
    assert path.name==expected_name
    files=raw_files(path,names)
    if wheel:
        for name,body in package.items():assert files[name.removeprefix('src/')]==body, 'source drift'
        assert files[dist+'licenses/LICENSE']==source['LICENSE']
        metadata(files[dist+'METADATA'],project,source['README.md'])
        assert files[dist+'WHEEL']==b'Wheel-Version: 1.0\nGenerator: setuptools (80.9.0)\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n'
        assert files[dist+'top_level.txt']==(module+'\n').encode()
        validate_record(files,dist+'RECORD')
        if project.get('scripts'):
            assert files[dist+'entry_points.txt']==entry_points(project)
    else:
        prefix=module+'-'+project['version']+'/'
        local={name.removeprefix(prefix):body for name,body in files.items()}
        for name,body in source.items():assert local[name]==body, 'source drift'
        metadata(local['PKG-INFO'],project,source['README.md'])
        assert local[egg+'PKG-INFO']==local['PKG-INFO']
        assert local['setup.cfg']==b'[egg_info]\ntag_build = \ntag_date = 0\n\n'.replace(b'\n',os.linesep.encode())
        assert local[egg+'top_level.txt']==(module+'\n').encode()
        assert local[egg+'dependency_links.txt']==b'\n'
        if project.get('scripts'):
            assert local[egg+'entry_points.txt']==entry_points(project)
        listed=local[egg+'SOURCES.txt'].decode().splitlines()
        expected=set(source)|{name for name in local if name.startswith(egg)}
        assert len(listed)==len(set(listed)) and set(listed)==expected, 'SOURCES drift'
        extras=project.get('optional-dependencies',{})
        requirements=''.join(x+'\n' for x in project['dependencies'])
        for extra,values in extras.items():
            requirements+='\n['+extra+']\n'+''.join(x+'\n' for x in values)
        if requirements:assert local[egg+'requires.txt']==requirements.encode()
    return files,{'sha256':digest(path.read_bytes()),'bytes':path.stat().st_size,
        'entries':{name:{'sha256':digest(body),'bytes':len(body)} for name,body in sorted(files.items())}}


def entry_points(project):
    scripts=project['scripts']
    assert scripts=={'equity-feature-worker':'equity_feature_workers.cli:main'}
    return ('[console_scripts]\n'+''.join(name+' = '+value+'\n' for name,value in sorted(scripts.items()))).encode()


def validate_record(files,record):
    rows=list(csv.reader(io.StringIO(files[record].decode('utf-8'))))
    assert all(len(row)==3 for row in rows)
    assert len(rows)==len(files) and {row[0] for row in rows}==set(files)
    for name,value,size in rows:
        if name==record:assert value==size==''
        else:
            encoded=base64.urlsafe_b64encode(hashlib.sha256(files[name]).digest()).rstrip(b'=').decode()
            assert value=='sha256='+encoded and size==str(len(files[name])), 'RECORD drift'
