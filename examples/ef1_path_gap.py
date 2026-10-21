"""Explicit synthetic G01/G02/G03 -> gap worker -> separate output DB -> HTML."""
from pathlib import Path
import runpy

if __name__ == '__main__':
    namespace = runpy.run_path(str(Path(__file__).with_name('_ef1_path_demo.py')))
    raise SystemExit(namespace['main']('path_gap'))
