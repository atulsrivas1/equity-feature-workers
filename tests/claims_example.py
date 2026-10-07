"""Installed synthetic claim, compute and verified receipt replay example."""
from pathlib import Path
import tempfile
import sys

# Only synthetic fixture helpers are loaded here; worker/core packages stay installed.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from equity_feature_workers import ClaimLimits, ClaimStore
from equity_feature_example_extensions.sink import LIMITS
from test_publication import sink_for, work

command=work()[0]
with tempfile.TemporaryDirectory() as temporary,sink_for('parquet',Path(temporary)) as sink:
    claims=ClaimStore(Path(temporary)/'claims',limits=ClaimLimits(),requirements=LIMITS)
    def acquire(owner):
        return claims.acquire(command.output.task,owner_id=owner,attempt_id='example-'+owner,
            issued_at_ns=100,expires_at_ns=200,now_ns=100)
    with acquire('first') as claim:
        first=claim.run(lambda:command.results,sink,now_ns=101)
        assert first.committed and first.results[0].values[0].values[0]==.2
    with acquire('resume') as claim:
        def must_not_recalculate():raise AssertionError('receipt replay called calculation')
        second=claim.run(must_not_recalculate,sink,now_ns=102)
        assert second.committed and second.output==first.output and second.results==first.results
print('Synthetic .2 history result / original receipt / zero-callback restart PASS')
