"""EQ121 repeat-build, independent installed boundaries and artifact receipts."""
import argparse
import gzip
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[1]
EPOCH = 1700000000
CORE_COMMIT = "7a6db8c2317de1ee9dd9116e0897cbf6445e359e"
EXPECTED = {
    "equity-feature-io-contracts": ["equity-feature-contracts==0.0.4a4"],
    "equity-feature-io-sdk": ["equity-feature-io-contracts==0.1.0a0"],
    "equity-feature-workers": ["equity-feature-io-sdk==0.1.0a0"],
}


def run(*args, cwd=ROOT, env=None):
    subprocess.run([str(a) for a in args], check=True, cwd=cwd, env=env)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def snapshot(repository, commit, destination, paths=("packages",)):
    """Materialize only committed paths; caller working trees cannot enter builds."""
    destination.mkdir(parents=True, exist_ok=False)
    data = subprocess.check_output(["git", "archive", "--format=tar", commit, *paths], cwd=repository)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        archive.extractall(destination, filter="data")
    return destination


def normalize(path):
    data = io.BytesIO()
    with tarfile.open(path, "r:gz") as source, tarfile.open(fileobj=data, mode="w", format=tarfile.PAX_FORMAT) as target:
        for member in sorted(source.getmembers(), key=lambda item: item.name):
            member.mtime = EPOCH
            member.uid = member.gid = 0
            member.uname = member.gname = ""
            member.pax_headers = {}
            member.mode = 0o755 if member.isdir() else 0o644
            target.addfile(member, source.extractfile(member) if member.isfile() else None)
    with path.open("wb") as stream, gzip.GzipFile(fileobj=stream, mode="wb", filename="", mtime=EPOCH) as output:
        output.write(data.getvalue())


def inspect(path, name):
    namespace = name.replace("-", "_")
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            files = {p: archive.read(p) for p in archive.namelist() if not p.endswith("/")}
        assert all(p.startswith((namespace + "/", namespace + "-")) for p in files)
        assert namespace + "/py.typed" in files
        assert any(p.endswith("/licenses/LICENSE") for p in files)
        metadata = next(v for p, v in files.items() if p.endswith("/METADATA"))
    else:
        with tarfile.open(path, "r:gz") as archive:
            assert not any(p.issym() or p.islnk() for p in archive.getmembers())
            files = {p.name: archive.extractfile(p).read() for p in archive.getmembers() if p.isfile()}
        prefix = path.name.removesuffix(".tar.gz") + "/"
        allowed = {prefix + p for p in ("LICENSE", "README.md", "PKG-INFO", "pyproject.toml", "setup.cfg")}
        assert all(p.startswith((prefix + "src/" + namespace + "/", prefix + "src/" + namespace + ".egg-info/")) or p in allowed for p in files)
        assert any(p.endswith("/src/" + namespace + "/py.typed") for p in files)
        metadata = files[prefix + "PKG-INFO"]
    for p, content in files.items():
        assert not p.startswith(("/", "\\")) and ".." not in Path(p).parts
        assert "__pycache__" not in p and not p.endswith(".pyc")
        assert b"C:\\Users\\" not in content and b"/home/runner/" not in content
    assert b"License-Expression: Apache-2.0" in metadata
    requirements = sorted(line.removeprefix("Requires-Dist: ").strip() for line in metadata.decode().splitlines() if line.startswith("Requires-Dist: "))
    assert requirements == sorted(EXPECTED[name]), (name, requirements)
    assert not any(p.endswith("/entry_points.txt") for p in files)


def build(source, output):
    output.mkdir(parents=True, exist_ok=True)
    run(sys.executable, "-m", "build", "--no-isolation", "--outdir", output, source, env=dict(os.environ, SOURCE_DATE_EPOCH=str(EPOCH)))
    for path in output.glob("*.tar.gz"):
        normalize(path)


def install(py, archives):
    run(py, "-m", "pip", "install", "--no-index", "--no-deps", "--no-build-isolation", *archives)


def fingerprint(py, cwd):
    code = """import hashlib,json
from importlib.metadata import distribution
from pathlib import Path
result={}
for name in ('equity-feature-contracts','equity-features'):
    dist=distribution(name)
    for relative in dist.files:
        path=Path(dist.locate_file(relative)).resolve()
        assert 'site-packages' in path.parts
        if path.is_file() and '__pycache__' not in path.parts:
            result[str(relative)]=hashlib.sha256(path.read_bytes()).hexdigest()
print(json.dumps(result,sort_keys=True))
"""
    return json.loads(subprocess.check_output([str(py), "-I", "-c", code], cwd=cwd, text=True))


def qualify(dependencies, artifacts, form, packages, probe):
    with tempfile.TemporaryDirectory(prefix="install-", dir=ROOT / "work") as temporary:
        location = Path(temporary)
        run(sys.executable, "-m", "venv", location)
        py = location / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        run(py, "-m", "pip", "install", "--no-deps", "setuptools==80.9.0", "mypy==1.15.0", "mypy_extensions==1.1.0", "typing_extensions==4.16.0")
        core = [p for p in dependencies if p.name.startswith(("equity_feature_contracts-", "equity_features-"))]
        install(py, core)
        before = fingerprint(py, location)
        run(py, "-I", probe, "core", cwd=location)
        other = [p for p in dependencies if p not in core]
        install(py, other + artifacts)
        run(py, "-m", "pip", "check")
        run(py, "-I", probe, *packages, cwd=location)
        after = fingerprint(py, location)
        assert before == after, "Companion install/import changed canonical core"
        for name in packages:
            run(py, "-I", "-m", "mypy", "--strict", "-p", name.replace("-", "_"), cwd=location)
        fp = hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest()
        return {"form": form, "packages": packages, "core_before_sha256": fp, "core_after_sha256": fp, "installed_typing": True, "inward_dependencies": True, "source_imports": False, "independent_bar_goldens": True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--core-root", required=True, type=Path)
    parser.add_argument("--io-root", type=Path)
    args = parser.parse_args()
    core = args.core_root.resolve()
    assert git(core, "rev-parse", CORE_COMMIT) == CORE_COMMIT
    assert not git(ROOT, "status", "--porcelain", "--", "packages", "tools", "tests", "requirements-dev.txt"), "Freeze source before qualification"
    output = ROOT / "dist"
    output.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="build-", dir=ROOT / "work") as temp:
        stage = Path(temp)
        dependencies = stage / "dependencies"
        core_source = snapshot(core, CORE_COMMIT, stage / "core-source", ("packages/contracts", "packages/features"))
        component_commit = git(ROOT, "rev-parse", "HEAD")
        component_source = snapshot(ROOT, component_commit, stage / "component-source")
        for folder in ("contracts", "features"):
            build(core_source / "packages" / folder, dependencies)
        dependency_commit = {"equity-features": CORE_COMMIT}
        if args.io_root:
            io_root = args.io_root.resolve()
            assert not git(io_root, "status", "--porcelain", "--", "packages", "tools", "tests", "requirements-dev.txt")
            dependency_commit["equity-feature-io"] = git(io_root, "rev-parse", "HEAD")
            io_source = snapshot(io_root, dependency_commit["equity-feature-io"], stage / "io-source")
            for folder in ("io-contracts", "io-sdk"):
                build(io_source / "packages" / folder, dependencies)
        packages = [tomllib.loads((p / "pyproject.toml").read_text(encoding="utf-8"))["project"]["name"] for p in sorted((component_source / "packages").iterdir())]
        first, repeat = stage / "first", stage / "repeat"
        for target in (first, repeat):
            for source in sorted((component_source / "packages").iterdir()):
                build(source, target)
        artifacts = sorted(first.iterdir())
        assert len(artifacts) == 2 * len(packages)
        for path in artifacts:
            assert sha(path) == sha(repeat / path.name), "Non-repeatable archive: " + path.name
            name = next(n for n in packages if path.name.startswith(n.replace("-", "_") + "-"))
            inspect(path, name)
        deps = sorted(dependencies.glob("*.whl"))
        reports = []
        for form in ("wheel", "sdist"):
            selected = [p for p in artifacts if (p.suffix == ".whl") == (form == "wheel")]
            reports.append(qualify(deps, selected, form, packages, ROOT / "tests/probe_foundation.py"))
        import shutil
        assert not list(output.glob("*.whl")) and not list(output.glob("*.tar.gz")), "Use a fresh dist directory"
        for path in artifacts + deps:
            shutil.copy2(path, output / path.name)
        record = {"schema": "foundation1", "commit": git(ROOT, "rev-parse", "HEAD"), "epoch": EPOCH, "python": platform.python_version(), "system": platform.system(), "machine": platform.machine(), "dependency_commits": dependency_commit, "artifacts": {p.name: sha(p) for p in artifacts}, "dependency_artifacts": {p.name: sha(p) for p in deps}, "probe_sha256": sha(ROOT / "tests/probe_foundation.py"), "builder_sha256": sha(Path(__file__)), "forms": reports}
        (output / "manifest.json").write_text(json.dumps(record, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print("Actual repeat artifacts, both fresh forms, inward graph and independent goldens PASS")


if __name__ == "__main__":
    (ROOT / "work").mkdir(exist_ok=True)
    main()
