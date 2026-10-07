"""One-time hand-specified worker wire oracle; never imports worker codec."""
import hashlib
import json
from pathlib import Path

config = {"identity":"oracle", "algorithm_version":"v1", "parameters":[],
    "session":{"namespace":"demo","session_id":"S","open_ns":100,"close_ns":200,
    "timezone_label":"supplied","intervals":[],"scheduled_close_ns":None,"early_close":False,
    "include_opening_auction":False,"include_closing_auction":False},
    "window":{"count":1,"target_session_id":"S","governed_sessions":["S"],"anchor":"completed_eod"},
    "availability":{"market_cutoff_ns":200,"knowledge_cutoff_ns":210,"evaluation_ns":210,
    "mode":"known_at","reconstruction_reason":None},
    "adjustment":{"basis":"raw","policy_version":"raw-v1","action_snapshot":"none","anchor":"none"},
    "price_unit":None,"schema_version":"1"}

def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)

wire = {"record":"TaskManifest","fields":{
    "job_id":"j","generation_id":"g","partition_id":"p","family":"bars","instruments":["A"],
    "config":{"config":canonical(config)},"features":[{"record":"FeatureHeader","fields":{
    "feature_id":"session.bar.volume","algorithm_version":"v1","dtype":{"enum":"ValueType","value":"int64"},"unit":"shares","schema_version":"1"}}],
    "inputs":[],"governed_sessions":[{"record":"IntervalSpec","fields":{"name":"S","start_ns":{"int":"100"},"end_ns":{"int":"200"}}}],
    "destination_scope":"d","ownership":"serialized_destination","max_input_batches":{"int":"1"},"max_input_bytes":{"int":"1024"},
    "ordered_history":False,"warmup_sessions":[],"initialization_sha256":None,"merge_policy":"none",
    "protocol_version":"efworker-task1","canonical_package_version":"0.0.4a4","canonical_schema_version":"1","math_policy_version":"v1"}}
data=canonical(wire)
record={"wire":data,"domain_sha256":hashlib.sha256(b'efworker-task1\0'+data.encode('ascii')).hexdigest()}
Path(__file__).with_name('manifest_golden.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8',newline='\n')
