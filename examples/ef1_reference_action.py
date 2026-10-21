"""Explicit synthetic reference admission -> worker -> separate output DB -> HTML."""
from pathlib import Path
import runpy

if __name__ == '__main__':
    namespace = runpy.run_path(str(Path(__file__).with_name('_ef1_reference_demo.py')))
    raise SystemExit(namespace['main']('reference_action'))
