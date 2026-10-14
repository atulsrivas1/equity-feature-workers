"""Fixed trusted certificate inventory shared by qualification owner/reference only."""
import hashlib
import json
from pathlib import Path
import re


def read_pins(root, expected_sha256):
    root = Path(root)
    if (not root.is_absolute() or root.is_symlink() or root.resolve() != root
            or type(expected_sha256) is not str or re.fullmatch('[0-9a-f]{64}', expected_sha256) is None):
        raise ValueError('owned certificate inventory root')
    path = root / 'pins.json'
    if path.is_symlink():
        raise ValueError('owned certificate inventory file')
    with path.open('rb') as stream:
        raw = stream.read(4097)
    if not raw or len(raw) > 4096 or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('owned certificate inventory identity')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('owned duplicate certificate inventory key')
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs)
    if type(value) is not dict or set(value) != {'valid', 'wrong'}:
        raise ValueError('owned certificate roles')
    for role, files in value.items():
        if type(files) is not dict or set(files) != {'cert', 'key'}:
            raise ValueError('owned certificate forms')
        for kind, pin in files.items():
            if (type(pin) is not dict or set(pin) != {'bytes', 'sha256'}
                    or type(pin['bytes']) is not int or not 0 < pin['bytes'] <= 8192
                    or type(pin['sha256']) is not str or re.fullmatch('[0-9a-f]{64}', pin['sha256']) is None):
                raise ValueError('owned certificate pin')
            file = root / (role + '-' + kind + '.pem')
            if file.is_symlink():
                raise ValueError('owned certificate material link')
            with file.open('rb') as stream:
                data = stream.read(8193)
            if len(data) != pin['bytes'] or hashlib.sha256(data).hexdigest() != pin['sha256']:
                raise ValueError('owned certificate material identity')
    return value
