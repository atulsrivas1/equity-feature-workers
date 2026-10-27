"""Fixed copied synthetic helpers; all project packages are installed imports."""
from fractions import Fraction
from pathlib import Path
import sys
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tests/conformance'))
from history_fixture import history_fixture,compute
from native_composition import create,acquire,population,diagnose,LIMITS,store,publish,deps
from equity_feature_evidence import fingerprint_result,canonical_content,encode_receipt
from equity_feature_conformance import compare_content,summarize
from equity_feature_workers import GenerationSpec
from equity_feature_duckdb_sink import DuckDBSink
from equity_feature_provenance import verify_live_generation

batch,context,config=history_fixture();result=compute(batch,context,config)
assert result.values[0].values==(float(Fraction(5,21)),)
changed,ctx,cfg=history_fixture(future_close=12345,future_known=999)
invariant=compare_content('unused-future-slot',fingerprint_result(result),
    fingerprint_result(compute(changed,ctx,cfg)),'equal')
assert invariant.matched
with tempfile.TemporaryDirectory() as folder:
    root=Path(folder);original=create(root/'inputs')
    values=tuple(acquire(root/'inputs',instrument=i,K=200) for i in ('A','B'))
    anchors=tuple((canonical_content(v.result),encode_receipt(v.receipt)) for v in values)
    spec=GenerationSpec('demo','job','G',tuple(v.expected_task for v in values))
    with DuckDBSink(root/'output.duckdb','synthetic-output',limits=LIMITS) as sink:
        values=tuple(publish(sink,v) for v in values);dependencies=deps(values,sink)
        generation=store(root/'generations');assert generation.publish(spec,dependencies).complete
        assert verify_live_generation(generation,spec,dependencies,values).operational_readback
        outcomes=tuple(diagnose(v,200) for v in values)
        assert summarize((invariant,*outcomes)).all_matched
        assert all(o.observed.admission=='denied' for o in outcomes)
        assert anchors==tuple((canonical_content(v.result),encode_receipt(v.receipt)) for v in values)
    assert population(root/'inputs')==original
print('Actual future-slot invariance matched; stored generation complete while late-knowledge admission remains denied.')
