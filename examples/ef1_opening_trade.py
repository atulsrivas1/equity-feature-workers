"""Synthetic canonical inputs -> worker -> new DuckDB -> complete readback."""
from pathlib import Path
import runpy

if __name__ == '__main__':
    namespace = runpy.run_path(str(Path(__file__).with_name('_ef1_observable_demo.py')))
    raise SystemExit(namespace['main']('opening_trade'))
