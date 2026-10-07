"""Frozen synthetic source facts and independent generation coverage oracle."""
from pathlib import Path
import json
from required_fixture import daily
ORACLE=json.loads(Path(__file__).with_name('publication_oracle.json').read_text(encoding='utf-8'))
def source(member):return daily(member,tuple(ORACLE['closes'][member]))
