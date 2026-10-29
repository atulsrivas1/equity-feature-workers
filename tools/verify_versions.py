"""Check that every live place that carries a package version agrees with tools/_versions.py (static, source based).

Covers: each package's pyproject version and exact pins, package ``__version__`` constants, the workers ``_versions.py``
constants and the provenance preflight constants. Historical receipts are deliberately not checked.
"""
from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _versions import CONTRACTS, FEATURES, OWN, VERSIONS  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
errors: list[str] = []


def expect(label: str, actual: object, wanted: object) -> None:
    if actual != wanted:
        errors.append(f"{label}: {actual!r} != {wanted!r}")


def check_requirements(label: str, requirements: list[str]) -> None:
    for requirement in requirements:
        m = re.fullmatch(r"([A-Za-z0-9_.-]+)(?:\[[a-z,]+\])?==([0-9][^;\s]*)(?:\s*;.*)?", requirement)
        if m and m.group(1) in VERSIONS:
            expect(f"{label} pin {m.group(1)}", m.group(2), VERSIONS[m.group(1)])


def constant(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)  # type: ignore[no-any-return]
    errors.append(f"{path.relative_to(ROOT)}: {name} not found")
    return ""


_README = re.compile(r"(?<![\w-])(equity-features?(?:-[a-z0-9]+)*)`?(?:==)?\s?(0\.\d+\.\d+a\d+)")
_SHORT = re.compile(r"(?<![\w-])(contracts|features|evidence|diagnostics|conformance|continuous|intraday|explanations|labels|strategies)\s?(0\.\d+\.\d+a\d+)")


def check_readmes(directory: Path, known: dict[str, str], short: dict[str, str]) -> None:
    """Every version a package README states for a distribution must equal the table (gate against stale or altered pins)."""
    for readme in sorted(directory.glob("*/README.md")):
        label = str(readme.relative_to(ROOT))
        historical = False
        for line in readme.read_text(encoding="utf-8").split(chr(10)):
            found = [(n, v) for n, v in _README.findall(line) if n in known]
            if line.startswith("#"):
                # A heading that names an older version of a distribution opens a historical section: its text is a record, not a pin.
                historical = any(v != known[n] for n, v in found)
                continue
            if historical:
                continue
            for name, version in found:
                expect(f"{label} states {name}", version, known[name])
            for name, version in _SHORT.findall(line):
                if name in short:
                    expect(f"{label} states {name}", version, short[name])


for pyproject in sorted((ROOT / "packages").glob("*/pyproject.toml")):
    project = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]
    label = str(pyproject.parent.relative_to(ROOT))
    expect(f"{label} version", project["version"], OWN[project["name"]])
    check_requirements(label, project.get("dependencies", []))
    for group in project.get("optional-dependencies", {}).values():
        check_requirements(label, group)

pkg = ROOT / "packages"
for init, name in (("workers/src/equity_feature_workers/__init__.py", "equity-feature-workers"),
                   ("provenance/src/equity_feature_provenance/__init__.py", "equity-feature-provenance"),
                   ("service/src/equity_feature_service/__init__.py", "equity-feature-service")):
    expect(f"{init} __version__", constant(pkg / init, "__version__"), OWN[name])
server = (pkg / "mcp/src/equity_feature_mcp/server.py").read_text(encoding="utf-8")
expect("mcp serverInfo version", re.search(r"serverInfo=dict\(name='equity-feature-mcp', version='([^']+)'", server).group(1), OWN["equity-feature-mcp"])  # type: ignore[union-attr]
versions_module = pkg / "workers/src/equity_feature_workers/_versions.py"
expect("workers FEATURES_VERSION", constant(versions_module, "FEATURES_VERSION"), FEATURES)
expect("workers CANONICAL_CONTRACTS_VERSION", constant(versions_module, "CANONICAL_CONTRACTS_VERSION"), CONTRACTS)
expect("workers INTRADAY_VERSION", constant(versions_module, "INTRADAY_VERSION"), VERSIONS["equity-features-intraday"])
preflight = pkg / "provenance/src/equity_feature_provenance/_preflight.py"
expect("provenance _DUCKDB_ADAPTER_VERSION", constant(preflight, "_DUCKDB_ADAPTER_VERSION"), VERSIONS["equity-feature-duckdb"])
expect("provenance _CANONICAL_CONTRACTS_VERSION", constant(preflight, "_CANONICAL_CONTRACTS_VERSION"), CONTRACTS)

# tests/supplied_memory_delegate.py is a byte copy of the io factory fixture (build_supplied_inputs asserts that), so it keeps the literal.
expect("supplied_memory_delegate canonical literals", set(re.findall(r"0\.0\.4a\d+", (ROOT / "tests/supplied_memory_delegate.py").read_text(encoding="utf-8"))), {CONTRACTS})

check_readmes(ROOT / "packages", dict(VERSIONS), {n.removeprefix("equity-feature-").removeprefix("equity-"): v for n, v in VERSIONS.items()} | {"intraday": VERSIONS.get("equity-features-intraday", "")})

if errors:
    raise SystemExit("version drift:\n" + "\n".join(errors))
print("Versions consistent across packages, in-source constants and pins.")
