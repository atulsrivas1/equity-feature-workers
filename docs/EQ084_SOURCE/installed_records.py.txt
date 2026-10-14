"""Installed RECORD verification and committed runtime source equality."""
import base64
import hashlib
import importlib.metadata
from pathlib import Path


def installed_records(source_inputs, venv_root):
    venv_root = Path(venv_root).resolve()
    records = {}
    for name, namespace in (('equity-feature-client', 'equity_feature_client'),
                            ('equity-feature-mcp', 'equity_feature_mcp'),
                            ('equity-feature-service', 'equity_feature_service'),
                            ('equity-feature-workers', 'equity_feature_workers')):
        distribution = importlib.metadata.distribution(name)
        result = {}
        for item in distribution.files or ():
            path = Path(distribution.locate_file(item)).resolve()
            if not path.is_relative_to(venv_root) or not path.is_file():
                raise ValueError('owned installed distribution path')
            data = path.read_bytes()
            if item.hash is None:
                if not (str(item).endswith('/RECORD') or str(item).endswith('.pyc')):
                    raise ValueError('owned installed RECORD missing digest')
            elif (item.hash.mode != 'sha256' or item.size != len(data)
                  or item.hash.value != base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode('ascii')):
                raise ValueError('owned installed RECORD drift')
            if '__pycache__' not in item.parts:
                result[str(item).replace('\\', '/')] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        expected = source_inputs[namespace]
        actual = {path: pin for path, pin in result.items() if path.startswith(namespace + '/')}
        if actual != expected:raise ValueError('owned installed source mismatch')
        records[name] = dict(version=distribution.version, files=result)
    return records
