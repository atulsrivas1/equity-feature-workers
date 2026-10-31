"""Single source of the experimental package versions for this repository's tools and tests.

A pre-release bump edits this table, the matching ``pyproject.toml`` files and ``__version__`` constants, and
``packages/workers/src/equity_feature_workers/_versions.py`` / the provenance preflight constants (installed packages
cannot import this file). ``tools/verify_versions.py`` fails if any of them disagree. Historical receipts, release
records and frozen archive baselines keep the versions they were produced with.
"""
from __future__ import annotations

# Sibling repositories' distributions this repository is built and tested against.
SIBLINGS = {
    "equity-feature-contracts": "0.0.4a6",
    "equity-features": "0.0.4a7",
    "equity-features-intraday": "0.1.0a5",
    "equity-feature-evidence": "0.1.0a4",
    "equity-feature-explanations": "0.1.0a4",
    "equity-feature-diagnostics": "0.1.0a3",
    "equity-feature-io-contracts": "0.1.0a7",
    "equity-feature-io-sdk": "0.1.0a3",
    "equity-feature-duckdb": "0.1.0a12",
    "equity-feature-duckdb-sink": "0.1.0a1",
    "equity-feature-parquet": "0.1.0a2",
}

# Distributions owned by this repository.
OWN = {
    "equity-feature-workers": "0.1.0a18",
    "equity-feature-provenance": "0.1.0a5",
    "equity-feature-client": "0.1.0a1",
    "equity-feature-mcp": "0.1.0a2",
    "equity-feature-service": "0.1.0a3",
}
VERSIONS = {**SIBLINGS, **OWN}
CONTRACTS = SIBLINGS["equity-feature-contracts"]
FEATURES = SIBLINGS["equity-features"]
