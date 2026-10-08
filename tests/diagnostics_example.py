"""Installed synthetic source -> calculation -> physical sink -> generation -> catalog."""
from pathlib import Path
import sys
import tempfile
sys.path.insert(0, str(Path(__file__).resolve().parent))
from equity_feature_workers import ProgressRecorder, run_session, Dependency, GenerationSpec
from equity_feature_example_extensions.sink import LIMITS
from test_commands import inputs
from test_catalog import stores
from test_publication import sink_for

for backend in ('parquet', 'duckdb'):
    with tempfile.TemporaryDirectory() as root, sink_for(backend, Path(root)) as sink:
        recorder = ProgressRecorder()
        spec, _, source = inputs()
        command = run_session(spec, source, sink, requirements=LIMITS, progress=recorder)
        task = command.output.task
        generation = GenerationSpec(task.config.session.namespace, task.job_id, task.generation_id, (task,))
        dependencies = (Dependency(task.task_sha256, task, command.output, sink),)
        catalog, generations = stores(root)
        assert generations.publish(generation, dependencies, progress=recorder).complete
        assert catalog.select(generation, generations, dependencies, expected_sequence=0, progress=recorder).accepted
        assert [t.status.value for t in recorder.snapshot().tasks] == ['verified', 'generation_complete', 'catalog_accepted']
        values = {c.feature_id: c.values[0] for c in command.results[0].values}
        assert [values[k] for k in ('session.bar.volume', 'session.bar.notional', 'session.bar.close_weighted_price')] == [500, 51200, 102.6]
        assert b'secret' not in recorder.snapshot().encode()
print('Synthetic both-sink source/calculation/generation/catalog; bounded observations; 500/51200/102.6 PASS')
