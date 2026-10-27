"""Owned synthetic example; fixture imports contain no project package sources."""
from dataclasses import replace
from pathlib import Path
import sys
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tests/provenance'))
from native_fixture import create,acquire,population
from test_native import LIMITS,publish,deps,store
from equity_feature_workers import GenerationSpec
from equity_feature_duckdb_sink import DuckDBSink
from equity_feature_provenance import bind_supplied_provenance,verify_live_generation

with tempfile.TemporaryDirectory() as folder:
    root=Path(folder);before=create(root/'inputs')
    values=tuple(acquire(root/'inputs',instrument=i,K=200) for i in ('A','B'))
    spec=GenerationSpec('demo','job','G',tuple(v.expected_task for v in values))
    assert not bind_supplied_provenance(values[0]).operational_readback
    with DuckDBSink(root/'output.duckdb','synthetic-output',limits=LIMITS) as sink:
        values=tuple(publish(sink,v) for v in values);dependencies=deps(values,sink)
        generation=store(root/'generations');assert generation.publish(spec,dependencies).complete
        report=verify_live_generation(generation,spec,dependencies,values)
        assert report.operational_readback and report.generation_status=='complete'
        assert all(q.status.value=='missing_input' for v in values for q in v.result.quality)
    assert population(root/'inputs')==before
print('Actual stored generation complete; late-knowledge quality stays unavailable. No source authentication claim.')
