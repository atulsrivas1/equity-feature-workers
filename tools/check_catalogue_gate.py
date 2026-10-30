"""Workers gate entry: run the io contract-catalogue drift gate and fail when it fails.

Internal, synthetic, pre-release. This file only invokes ``tools/check_contract_catalogue.py`` of the equity-feature-io
checkout (merged in io #66); it does not reimplement any catalogue comparison, and the header cross-check across
declarations belongs to RL-01-11. Writes nothing.

    python tools/check_catalogue_gate.py [--io-root PATH_TO_IO] [--core-root PATH_TO_CORE] [--core-ref origin/main]

Defaults: sibling checkouts ``../equity-feature-io`` and ``../equity-features`` (env EF_IO_ROOT / EF_CORE_ROOT override).
Exit code is the io gate's own exit code. A missing io checkout, missing gate script or missing core checkout is a
failure (exit 2), never a silent pass: the workers gate must not be skippable.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GATE = Path("tools") / "check_contract_catalogue.py"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--io-root", type=Path, default=Path(os.environ.get("EF_IO_ROOT", ROOT.parent / "equity-feature-io")))
    parser.add_argument("--core-root", type=Path, default=Path(os.environ.get("EF_CORE_ROOT", ROOT.parent / "equity-features")))
    parser.add_argument("--core-ref", default="origin/main")
    args = parser.parse_args()
    # The io gate runs with cwd=io root, so both roots must be absolute before any path is built.
    args.io_root, args.core_root = args.io_root.resolve(), args.core_root.resolve()
    script = args.io_root / GATE
    if not script.is_file():
        print(f"FAIL: io gate script not found: {script} (pass --io-root or set EF_IO_ROOT)")
        return 2
    if not args.core_root.is_dir():
        print(f"FAIL: core checkout not found: {args.core_root} (pass --core-root or set EF_CORE_ROOT)")
        return 2
    command = [sys.executable, str(script), "--core-root", str(args.core_root), "--core-ref", args.core_ref]
    print("running io gate:", " ".join(command))
    # The io gate resolves its docs and package paths from its own checkout, so it runs from the io root.
    return subprocess.run(command, cwd=args.io_root).returncode


if __name__ == "__main__":
    sys.exit(main())
