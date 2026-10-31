"""Link check for the workers catalogue link layer (RL-01-10 workers lane). Internal, synthetic, pre-release.

Reads only. Every workers doc that has a "## Contract catalogue" section is scanned; each link in the section must
resolve: links to the io repository (``https://github.com/atulsrivas1/equity-feature-io/blob/main/<path>``) must name a
file that exists in the io checkout, and relative links must name a file in this repository. The check also requires
that every producer in the io catalogue index is linked, with both its input and output page, from at least one workers
doc. It does not compare catalogue content (that is the io gate, ``tools/check_catalogue_gate.py``).

    python tools/check_catalogue_links.py [--io-root PATH_TO_IO]

Default io root: sibling ``../equity-feature-io`` (env EF_IO_ROOT overrides). A missing io checkout is exit 2, never a
silent pass. Exit 0 when every link resolves and every producer is covered, 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IO_PREFIX = "https://github.com/atulsrivas1/equity-feature-io/blob/main/"
SECTION = "## Contract catalogue"
LINK = re.compile(r"\]\(([^)\s]+)\)")


def sections() -> list[tuple[Path, list[str]]]:
    found = []
    for doc in sorted((ROOT / "docs").glob("*.md")):
        text = doc.read_text(encoding="utf-8")
        if SECTION in text:
            found.append((doc, text.split(SECTION, 1)[1].splitlines()))
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--io-root", type=Path, default=Path(os.environ.get("EF_IO_ROOT", ROOT.parent / "equity-feature-io")))
    io = parser.parse_args().io_root.resolve()
    index = io / "docs" / "contracts" / "catalogue" / "index.json"
    if not index.is_file():
        print(f"FAIL: io catalogue index not found: {index} (pass --io-root or set EF_IO_ROOT)")
        return 2
    producers = sorted(p["producer_id"] if isinstance(p, dict) else p for p in json.loads(index.read_text(encoding="utf-8")).get("producers", []))
    if not producers:
        print("FAIL: no producers found in the io catalogue index")
        return 2
    errors: list[str] = []
    linked: set[tuple[str, str]] = set()
    total = 0
    for doc, lines in sections():
        for line in lines:
            for target in LINK.findall(line):
                total += 1
                if target.startswith(IO_PREFIX):
                    rel = target[len(IO_PREFIX):]
                    ok = (io / rel).is_file()
                    m = re.fullmatch(r"docs/contracts/catalogue/([^/]+)/(input|output)\.md", rel)
                    if m:
                        linked.add((m.group(1), m.group(2)))
                elif target.startswith(("http://", "https://")):
                    ok = False
                    errors.append(f"{doc.name}: link outside the io catalogue is not allowed here: {target}")
                    continue
                else:
                    ok = (doc.parent / target.split("#")[0]).resolve().is_file()
                if not ok:
                    errors.append(f"{doc.name}: unresolved link {target}")
    for pid in producers:
        for view in ("input", "output"):
            if (pid, view) not in linked:
                errors.append(f"producer {pid}: no workers doc links its {view} page")
    for e in errors:
        print("FAIL:", e)
    print(f"{len(sections())} docs with a catalogue section, {total} links, {len(producers)} producers: "
          + ("FAILED, %d problem(s)" % len(errors) if errors else "OK"))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
