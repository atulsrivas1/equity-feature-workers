"""The exact core and intraday versions this workers distribution is built against.

One place for the producer backend version the routes assert and for the canonical contracts version the worker
manifest protocol admits. The matching pins live in ``pyproject.toml``; ``tools/verify_versions.py`` checks all of them.
"""
FEATURES_VERSION = '0.0.4a6'
CANONICAL_CONTRACTS_VERSION = '0.0.4a5'
INTRADAY_VERSION = '0.1.0a5'
