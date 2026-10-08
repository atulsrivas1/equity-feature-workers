"""Installed synthetic selection, supersession and original old-reader verification."""
from pathlib import Path
import sys
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parent))
from test_catalog import stores,publish,dependencies
from test_publication import sink_for,Proxy
with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
    catalog,generations=stores(root)
    a,ad,_=publish(root,sink,'A');old=catalog.select(a,generations,ad,expected_sequence=0).snapshot
    b,bd,_=publish(root,sink,'B');new=catalog.select(b,generations,bd,expected_sequence=1).snapshot
    assert [e.spec.generation_id for e in new.history]==['A','B']
    proxy=Proxy(sink);read=catalog.verify(old,generations,dependencies(old,proxy))
    assert read.accepted and read.snapshot==old and proxy.begins==0
    assert [v.command.results[0].values[0].values[0] for v in read.generation.barrier.verified]==[.2,-.2,0.0]
print('Synthetic A/B selection / original old snapshot / full .2,-.2,0 results / zero sink begin PASS')
