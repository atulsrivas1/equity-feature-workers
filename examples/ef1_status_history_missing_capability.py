"""Synthetic missing original identity capability -> truthful unavailable result."""
from pathlib import Path
import runpy

if __name__ == '__main__':
    namespace = runpy.run_path(str(Path(__file__).with_name('_ef1_observable_demo.py')))
    raise SystemExit(namespace['main']('status_history',missing_capability=True))
