"""Development-only, source-bound descriptions. Never schedules or calculates.

Records explain dependencies of accepted batch intraday outputs. They grant no
execution, cache, input sealing, incremental-state or publication authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import importlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import re
import sys


class GraphSpecificationError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__('Graph specification failed: ' + code)


def fail(code='invalid_schema'):
    raise GraphSpecificationError(code)


def wire(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'),
                       ensure_ascii=True, allow_nan=False) + '\n').encode('ascii')


def digest(value):
    return hashlib.sha256(value).hexdigest()


def resolve(name):
    """Resolve only named inspected modules and static attributes, no eval."""
    parts = name.split('.')
    for end in range(len(parts), 0, -1):
        module = sys.modules.get('.'.join(parts[:end]))
        if module is not None:
            owner = module
            for part in parts[end:-1]:
                owner = inspect.getattr_static(owner, part)
            return owner, parts[-1]
    fail('source_mismatch')


def function_of(value):
    return value.fget if type(value) is property else value


@dataclass(frozen=True)
class BoundSource:
    """Process-local trusted-source snapshot, deliberately not serializable authority."""
    source_wire: bytes
    sites: tuple
    files: tuple
    callback: object
    callback_code: object
    anchors: tuple
    hook_inventory_sha256: str

    def check(self):
        try:
            for owner, name, original, code in self.sites:
                current = inspect.getattr_static(owner, name)
                if current is not original or getattr(function_of(current), '__code__', None) is not code:
                    fail('source_mismatch')
            for path, expected in self.files:
                if digest(path.read_bytes()) != expected:
                    fail('source_mismatch')
            module = sys.modules['equity_feature_workers.intraday']
            if module.compute_intraday_inputs is not self.callback or self.callback.__code__ is not self.callback_code:
                fail('source_mismatch')
        except GraphSpecificationError:
            raise
        except Exception:
            fail('source_mismatch')

    @property
    def source(self):
        return json.loads(self.source_wire)

    def anchor(self, operation):
        return json.loads(dict(self.anchors)[operation])


def bind_source(expected_inventory):
    """Bind explicit hooks/source records against actual imported files/objects.

    Caller supplies {'hooks': accepted hook inventory, 'source': source_binding}.
    This is cooperative inspected-source qualification, not hostile-code attestation.
    Import paths are the caller's responsibility; file/version checks reject drift.
    """
    if type(expected_inventory) is not dict or set(expected_inventory) != {'hooks', 'source'}:
        fail()
    hooks, source = expected_inventory['hooks'], expected_inventory['source']
    validate_source(source)
    if hooks.get('schema') != 'eq142-closed-hook-inventory:1' or len(hooks.get('records', [])) != 39:
        fail()
    for name, version in source['distributions'].items():
        if importlib.metadata.version(name) != version:
            fail('source_mismatch')
    for name in hooks['inspected_modules']:
        importlib.import_module(name)
    files, sites, anchors = {}, {}, {}
    for record in hooks['records']:
        module = sys.modules[record['source_module']]
        path = Path(module.__file__).resolve()
        raw_hash = digest(path.read_bytes())
        if raw_hash != record['source_sha256']:
            fail('source_mismatch')
        files[path] = raw_hash
        if record['source_category'] == 'installed_package' and source['installed_module_sha256'].get(record['source_module']) != raw_hash:
            fail('source_mismatch')
        owner, name = resolve(record['definition'])
        original = inspect.getattr_static(owner, name)
        function = function_of(original)
        if not inspect.isfunction(function):
            fail('source_mismatch')
        for site in record['patch_sites']:
            site_owner, site_name = resolve(site)
            if inspect.getattr_static(site_owner, site_name) is not original:
                fail('source_mismatch')
            sites[(id(site_owner), site_name)] = (site_owner, site_name, original, function.__code__)
        for alias in record.get('class_aliases', []):
            alias_owner, alias_name = resolve(alias)
            if inspect.getattr_static(alias_owner, alias_name) is not owner:
                fail('source_mismatch')
            sites[(id(alias_owner),alias_name)]=(alias_owner,alias_name,owner,None)
        category = record['source_category']
        repo = 'python_runtime' if category == 'python_runtime' else 'io' if record['source_module'].startswith('equity_feature_io') else 'worker' if record['source_module'].startswith('equity_feature_workers') or category != 'installed_package' else 'core'
        commit = sys.version.split()[0] if repo == 'python_runtime' else source['package_build_sources'][repo]
        module_name = record['source_module']
        if repo == 'python_runtime':
            source_path = module_name
        elif category == 'accepted_worker_fixture':
            source_path = 'tests/' + module_name + '.py'
        else:
            package = 'workers' if repo == 'worker' else 'contracts' if module_name.startswith(('equity_feature_contracts','equity_feature_io_contracts')) else 'sdk' if repo == 'io' else 'intraday' if module_name.startswith('equity_features_intraday') else 'features'
            source_path = 'packages/' + package + '/src/' + module_name.replace('.', '/') + (('/__init__.py') if path.name == '__init__.py' else '.py')
        anchors[record['operation']] = wire({'repository': repo, 'commit_or_runtime': commit,
            'file': source_path, 'symbol': record['definition'], 'file_sha256': raw_hash})
    from equity_feature_workers.intraday import compute_intraday_inputs
    for name, expected in source['tool_sha256'].items():
        tool_path=Path(__file__).resolve().parent / name
        if digest(tool_path.read_bytes()) != expected:
            fail('source_mismatch')
        files[tool_path]=expected
    for name,expected in source['installed_module_sha256'].items():
        module=importlib.import_module(name);path=Path(module.__file__).resolve()
        if not path.is_relative_to(Path(sys.prefix).resolve()) or digest(path.read_bytes())!=expected:
            fail('source_mismatch')
        files[path]=expected
    owner = sys.modules['equity_feature_workers.intraday']
    sites[(id(owner), 'compute_intraday_inputs')] = (owner, 'compute_intraday_inputs', compute_intraday_inputs, compute_intraday_inputs.__code__)
    bound = BoundSource(wire(source), tuple(sites.values()), tuple(files.items()),
                        compute_intraday_inputs, compute_intraday_inputs.__code__, tuple(anchors.items()),digest(wire(hooks)))
    bound.check()
    return bound


CONTRACTS = dict(structural_admission='input_binding_admission', configuration='intraday_configuration',
    operand_readiness='intraday_operand_readiness', ordered_recurrence='intraday_indicator_order',
    compatible_witness='intraday_unrounded_bands', result_join='full_result_identity_and_evidence',
    resource_admission='worker_resource_admission', publication_owner='serialized_publication_ownership')
FEATURES = frozenset(('intraday.ema', 'intraday.rsi', 'intraday.atr', 'intraday.band_lower',
    'intraday.band_middle', 'intraday.band_upper', 'intraday.band_width_ratio', 'intraday.band_position'))
LIMITATIONS = ['batch_full_history_only', 'caller_tuple_ownership_not_deep_sealing',
    'declared_acquisition_not_fresh_body_attestation', 'descriptive_only', 'no_update_restore_merge',
    'task_graph_not_job_scheduler', 'whole_call_errors_preserved']
NODE_FIELDS = {'id','kind','scope_id','outputs','predicate','owner','capabilities','source_anchor','ordering'}
ROOT_FIELDS = {'schema','source','task_sha256','reuse_sha256','selected_headers',
    'input_identity_disposition','scope','disposition','disposition_reason','nodes','edges','limitations'}
KINDS = {'supplied_input','configuration_gate','structural_admission','operand','ordered_state',
    'output','result_join','resource_admission','publication_owner'}
PREDICATES = {'supplied_bar_descriptor_only','valid_intraday_configuration','common_valid_structure',
    'anchored_closes_from_anchor_through_target','predecessor_closes_successor_high_low_anchor_exception',
    'last_n_scheduled_closes_from_epoch','ema_seed_then_ordered_full_epoch',
    'rsi_n_plus_one_seed_then_ordered_full_epoch','atr_anchor_close_n_plus_one_seed_then_ordered_full_epoch',
    'selected_band_from_unrounded_moments','position_requires_nonzero_variance',
    'complete_selected_result_including_unavailable_cells_global_evidence_and_original_whole_call_error_policy',
    'original_logical_resource_admission','original_serialized_publication_owner','unqualified_callback_boundary_only'}


def closed(value, names):
    if type(value) is not dict or set(value) != set(names.split()):fail()


def hexhash(value, length=64):
    if type(value) is not str or re.fullmatch('[0-9a-f]{'+str(length)+'}',value) is None:fail()


def validate_source(source):
    closed(source,'canonical_docs_head package_build_sources distributions installed_module_sha256 tool_sha256 protocol_sha256 expected_fixture_sha256')
    hexhash(source['canonical_docs_head'],40)
    closed(source['package_build_sources'],'core worker io')
    for value in source['package_build_sources'].values():hexhash(value,40)
    closed(source['distributions'],'equity-feature-contracts equity-features equity-features-intraday equity-feature-io-contracts equity-feature-io-sdk equity-feature-workers')
    if any(type(v) is not str or len(v)>64 for v in source['distributions'].values()):fail()
    for field in ['installed_module_sha256','tool_sha256']:
        values=source[field]
        if type(values) is not dict or not 1<=len(values)<=128:fail()
        for name,value in values.items():
            if type(name) is not str or not re.fullmatch('[a-zA-Z0-9_.-]{1,128}',name) or '..' in name:fail()
            hexhash(value)
    hexhash(source['protocol_sha256']);hexhash(source['expected_fixture_sha256'])


def validate_anchor(anchor):
    closed(anchor,'repository commit_or_runtime file symbol file_sha256')
    if anchor['repository'] not in ('core','worker','io','python_runtime'):fail()
    if anchor['repository']=='python_runtime':
        if type(anchor['commit_or_runtime']) is not str or not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+',anchor['commit_or_runtime']):fail()
    else:hexhash(anchor['commit_or_runtime'],40)
    for field in ['file','symbol']:
        if type(anchor[field]) is not str or not re.fullmatch('[a-zA-Z0-9_./]{1,512}',anchor[field]) or '..' in anchor[field]:fail()
    hexhash(anchor['file_sha256'])


def validate_scope(scope):
    closed(scope,'population context_json context_sha256 config_sha256 full_task_wire_sha256 scope_id')
    pop=scope['population']
    if type(pop) is not list or not pop or len(pop)>4096 or any(type(v) is not str or not v or len(v)>4096 for v in pop) or len(set(pop))!=len(pop):fail()
    for field in ['config_sha256','full_task_wire_sha256','scope_id']:hexhash(scope[field])
    if (scope['context_json'] is None)!=(scope['context_sha256'] is None):fail()
    if scope['context_json'] is not None:
        from equity_features_intraday import IntradayContext
        context=IntradayContext.from_json(scope['context_json'])
        if context.digest!=scope['context_sha256']:fail()


def validate_input_identity(value):
    from equity_feature_workers.manifests import _decode, InputManifest
    closed(value,'declared_input_manifests body_verification canonical_value_body_sha256 observed_original_pickle_sha256 ownership new_reuse_certificate')
    if value['body_verification'] not in ('generated_current_pickle_observation','canonical_typed_value_fixture_only','unverified_current_body'):fail()
    if value['ownership']!='caller_owned_original_tuple_reference' or value['new_reuse_certificate'] is not False:fail()
    for field in ['canonical_value_body_sha256','observed_original_pickle_sha256']:
        if value[field] is not None:hexhash(value[field])
    if type(value['declared_input_manifests']) is not list or len(value['declared_input_manifests'])>4096:fail()
    for record in value['declared_input_manifests']:
        if type(_decode(record)) is not InputManifest:fail()


def describe_task(task, calculate, batches, *, qualification):
    from equity_feature_workers import TaskManifest
    from equity_feature_workers.manifests import encode_task, _wire
    from equity_feature_contracts import CanonicalBatch
    if type(qualification) is not BoundSource or type(task) is not TaskManifest or type(batches) is not tuple or any(type(b) is not CanonicalBatch for b in batches):
        fail()
    qualification.check()
    encoded = encode_task(task)
    scope_id = digest(b'eq142-scope1\0' + encoded)
    selected = tuple(h.feature_id for h in task.features)
    qualified = calculate is qualification.callback and task.family == 'intraday'
    context, period = None, None
    if qualified:
        from equity_feature_workers.intraday import _context
        context, period = _context(task.config, selected)
        if not set(selected) <= FEATURES:
            fail()
    nodes, edges = [], []
    def node(id, kind, predicate, owner, operation, outputs=(), ordering=None):
        nodes.append({'id': id, 'kind': kind, 'scope_id': scope_id, 'outputs': list(outputs),
            'predicate': predicate, 'owner': owner, 'capabilities': ['batch','full_history_replay'] if kind in ('operand','ordered_state','output') else [],
            'source_anchor': qualification.anchor(operation), 'ordering': ordering})
    def edge(a, b, reason):
        edges.append({'from': a, 'to': b, 'reason': reason, 'contract': CONTRACTS[reason]})
    unresolved = 'unqualified_callback_boundary_only'
    node('input:supplied','supplied_input','supplied_bar_descriptor_only' if qualified else unresolved,'caller_supplied','supplied.admit')
    node('gate:configuration','configuration_gate','valid_intraday_configuration' if qualified else unresolved,'pure_preparation','worker.intraday_context')
    node('gate:structural','structural_admission','common_valid_structure' if qualified else unresolved,'pure_preparation','supplied.admit')
    node('gate:resource','resource_admission','original_logical_resource_admission','coordinator','supervisor.inputs')
    node('join:result','result_join','complete_selected_result_including_unavailable_cells_global_evidence_and_original_whole_call_error_policy','task_result','supplied.result_identity',sorted(selected))
    node('owner:publication','publication_owner','original_serialized_publication_owner','serialized_sink','publisher.submit')
    edge('input:supplied','gate:structural','structural_admission')
    edge('gate:resource','join:result','resource_admission')
    edge('join:result','owner:publication','publication_owner')
    if qualified:
        operands = sorted({'price_moments' if f.startswith('intraday.band_') else 'true_ranges' if f == 'intraday.atr' else 'anchored_closes' for f in selected})
        for operand in operands:
            predicate = {'price_moments':'last_n_scheduled_closes_from_epoch',
                'true_ranges':'predecessor_closes_successor_high_low_anchor_exception',
                'anchored_closes':'anchored_closes_from_anchor_through_target'}[operand]
            nid = 'operand:'+operand
            node(nid,'operand',predicate,'pure_preparation','intraday.prepare_operand')
            edge('gate:configuration',nid,'configuration');edge('gate:structural',nid,'structural_admission')
        for f in selected:
            out = 'output:'+f
            if f.startswith('intraday.band_'):
                node(out,'output','position_requires_nonzero_variance' if f == 'intraday.band_position' else 'selected_band_from_unrounded_moments','pure_calculation','intraday.band_values',(f,))
                edge('operand:price_moments',out,'compatible_witness')
            else:
                state = 'state:'+f; short = f.split('.')[1]
                order = {'policy':'chronological_full_epoch','anchor_index':context.anchor_index,
                    'target_index':context.target_index,'minimum_seed_slots':period+(short != 'ema'), 'incremental_capability':False}
                predicate = {'ema':'ema_seed_then_ordered_full_epoch','rsi':'rsi_n_plus_one_seed_then_ordered_full_epoch',
                    'atr':'atr_anchor_close_n_plus_one_seed_then_ordered_full_epoch'}[short]
                node(state,'ordered_state',predicate,'pure_calculation','intraday.indicator',(f,),order)
                node(out,'output',predicate,'pure_calculation','intraday.indicator',(f,))
                edge('operand:true_ranges' if short == 'atr' else 'operand:anchored_closes',state,'operand_readiness')
                edge(state,out,'ordered_recurrence')
            edge(out,'join:result','result_join')
    else:
        edge('gate:configuration','join:result','configuration');edge('gate:structural','join:result','structural_admission')
    limitations = sorted(LIMITATIONS + ([] if qualified else ['unknown_callback_no_refinement']))
    result = {'schema':'eq142-output-graph:1','source':qualification.source,'task_sha256':task.task_sha256,
        'reuse_sha256':task.reuse_sha256,'selected_headers':[_wire(h) for h in task.features],
        'input_identity_disposition':{'declared_input_manifests':[_wire(i) for i in task.inputs],
            'body_verification':'unverified_current_body','canonical_value_body_sha256':None,
            'observed_original_pickle_sha256':None,'ownership':'caller_owned_original_tuple_reference','new_reuse_certificate':False},
        'scope':{'population':list(task.instruments),'context_json':context.to_json() if context else None,
            'context_sha256':context.digest if context else None,'config_sha256':task.config.digest,
            'full_task_wire_sha256':digest(encoded),'scope_id':scope_id},
        'disposition':'qualified_intraday_description' if qualified else 'conservative_unresolved',
        'disposition_reason':'qualified_builtin' if qualified else 'not_selected_family' if task.family != 'intraday' else 'unqualified_callback',
        'nodes':sorted(nodes,key=lambda n:n['id']),
        'edges':sorted(edges,key=lambda e:(e['from'],e['to'],e['reason'],e['contract'])), 'limitations':limitations}
    validate_graph(result)
    return result


def describe_item(item, *, qualification):
    from equity_feature_workers import WorkItem
    if type(item) is not WorkItem:
        fail()
    return describe_task(item.task,item.calculate,item.batches,qualification=qualification)


BARRIER_KINDS = ['resource_before_dispatch','population_component_order',
    'all_dispatched_worker_outcomes_terminal','pool_shutdown_before_coordinator_validation',
    'complete_task_result_before_publication','serialized_backend_owner']
JOB_LIMITATIONS = ['descriptive_only','source_required_not_observed','global_worker_shutdown_barrier',
    'terminal_not_successful','backend_contract_only','logical_not_physical_resource_enforcement']


def describe_job(items, *, mode, budget, qualification):
    """Describe original partition ownership/barriers without executing admission.

    Resource evidence stays source_required/not_executed. This preserves valid
    batches with multiple original job/generation/destination scopes.
    """
    from equity_feature_workers import WorkItem, ResourceBudget
    from equity_feature_workers.supervisor import partition_tasks
    if type(qualification) is not BoundSource or type(budget) is not ResourceBudget or type(items) is not tuple or not 1<=len(items)<=16 or any(type(i) is not WorkItem for i in items):fail()
    qualification.check()
    if mode not in ('sequential','thread','process') or budget.workers not in (1,2,4,8) or mode=='sequential' and budget.workers!=1:fail()
    partitions=partition_tasks(items,budget.workers)
    tasks=[i.task.task_sha256 for i in items]
    partition_for={i.task.task_sha256:'partition:%03d'%p.ordinal for p in partitions for i in p.items}
    remaining=list(items);components=[]
    while remaining:
        members=[remaining.pop(0)];population=set(members[0].task.instruments)
        while True:
            linked=[i for i in remaining if population.intersection(i.task.instruments)]
            if not linked:break
            for item in linked:
                members.append(item);population.update(item.task.instruments);remaining.remove(item)
        components.append((sorted(population),sorted(members,key=lambda i:(i.task.config.session.open_ns,i.task.task_sha256))))
    components.sort(key=lambda pair:pair[0])
    component_records=[]
    for ordinal,(population,members) in enumerate(components):
        pids={partition_for[i.task.task_sha256] for i in members}
        if len(pids)!=1:fail('source_mismatch')
        component_records.append({'id':'component:%03d'%ordinal,'population':population,
            'ordered_task_hashes':[i.task.task_sha256 for i in members],'partition_id':next(iter(pids))})
    partition_records=[{'id':'partition:%03d'%p.ordinal,'ordinal':p.ordinal,
        'component_ids':[c['id'] for c in component_records if c['partition_id']=='partition:%03d'%p.ordinal],
        'ordered_task_hashes':[i.task.task_sha256 for i in p.items]} for p in partitions]
    scopes={}
    for item in items:
        task=item.task;key=(task.job_id,task.generation_id,task.destination_scope)
        scopes.setdefault(key,[]).append(task.task_sha256)
    job_scopes=[{'id':'job:%03d'%n,'job_id':key[0],'generation_id':key[1],
        'destination_scope':key[2],'task_hashes':members} for n,(key,members) in enumerate(scopes.items())]
    barriers=[]
    kinds=[kind for kind in BARRIER_KINDS if mode!='sequential' or kind!='pool_shutdown_before_coordinator_validation']
    for n,kind in enumerate(kinds):
        barriers.append({'id':'barrier:%03d'%n,'kind':kind,'task_hashes':tasks[:],
            'partition_ids':[p['id'] for p in partition_records],
            'predecessor_ids':[] if n==0 else ['barrier:%03d'%(n-1)],
            'owner_id':'publication:owner' if kind=='serialized_backend_owner' else 'coordinator',
            'evidence':'source_required','source_anchor':qualification.anchor('publisher.inner_drain' if kind=='serialized_backend_owner' else 'supervisor.validate_results' if kind=='complete_task_result_before_publication' else 'supervisor.inputs' if kind=='resource_before_dispatch' else 'supervisor.partition')})
    budget_record=asdict(budget)
    execution_scope_id=digest(b'eq142-job1\0'+wire({'mode':mode,'configured_workers':budget.workers,'tasks':tasks,'budget':budget_record}))
    record={'schema':'eq142-job-boundaries:1','source':qualification.source,'execution_scope_id':execution_scope_id,
        'job_scopes':job_scopes,'mode':mode,'configured_workers':budget.workers,'tasks':tasks,
        'components':component_records,'partitions':partition_records,
        'resource_admission':{'budget':budget_record,'evidence':'not_executed',
            'initial_reserved_resident_bytes':None,'cumulative_committed_output_metadata_bytes':None,
            'trace_reserved_bytes':67108864,'whole_job_sampled_rss_cap_bytes':1073741824,
            'additional_host_memory_reserve_bytes':2147483648,'cpu_host_reserve_slots':2,
            'capacity':None,'original_reason':None,'physical_enforcement':False},
        'barriers':barriers,'publication_owner':{'id':'publication:owner',
            'destination_scopes':sorted({i.task.destination_scope for i in items}),
            'job_scope_ids':[j['id'] for j in job_scopes],'writer_mode':'serialized_writer',
            'backend_owners':1,'durability':'backend_contract_only'},
        'cancelled_or_undispatched':[],'limitations':sorted(v for v in JOB_LIMITATIONS if mode!='sequential' or v!='global_worker_shutdown_barrier')}
    validate_job(record)
    return record


def validate_job(record):
    """Validate descriptive not-executed job records and all membership joins."""
    try:
        closed(record,'schema source execution_scope_id job_scopes mode configured_workers tasks components partitions resource_admission barriers publication_owner cancelled_or_undispatched limitations')
        if record['schema']!='eq142-job-boundaries:1' or len(wire(record))>1048576:fail()
        validate_source(record['source']);hexhash(record['execution_scope_id'])
        tasks=record['tasks']
        if type(tasks) is not list or not 1<=len(tasks)<=16 or len(set(tasks))!=len(tasks):fail()
        for h in tasks:hexhash(h)
        if record['mode'] not in ('sequential','thread','process') or type(record['configured_workers']) is not int or record['configured_workers'] not in (1,2,4,8):fail()
        if record['mode']=='sequential' and record['configured_workers']!=1:fail()
        if type(record['limitations']) is not list or any(type(v) is not str for v in record['limitations']) or record['limitations']!=sorted(set(record['limitations'])) or not set(record['limitations'])<=set(JOB_LIMITATIONS):fail()
        if record['mode']=='sequential' and 'global_worker_shutdown_barrier' in record['limitations']:fail()
        def ids(records):
            if type(records) is not list or not records or len(records)>16:fail()
            names=[r['id'] for r in records]
            if any(type(n) is not str or re.fullmatch('[a-z][a-z0-9_.:-]{0,127}',n) is None for n in names):fail()
            if len(set(names))!=len(names):fail('duplicate')
            return set(names)
        def membership(records,field):
            for record in records:
                values=record[field]
                if type(values) is not list or not values or len(values)>16 or any(type(h) is not str or h not in tasks for h in values) or len(set(values))!=len(values):fail()
            flattened=[h for r in records for h in r[field]]
            if sorted(flattened)!=sorted(tasks):fail()
        scope_ids=ids(record['job_scopes']);component_ids=ids(record['components']);partition_ids=ids(record['partitions'])
        membership(record['job_scopes'],'task_hashes');membership(record['components'],'ordered_task_hashes');membership(record['partitions'],'ordered_task_hashes')
        for s in record['job_scopes']:
            closed(s,'id job_id generation_id destination_scope task_hashes')
            if any(type(s[k]) is not str or not s[k] or len(s[k])>4096 for k in ('job_id','generation_id','destination_scope')):fail()
        if len({(s['job_id'],s['generation_id'],s['destination_scope']) for s in record['job_scopes']})!=len(scope_ids):fail('duplicate')
        populations=[]
        for c in record['components']:
            closed(c,'id population ordered_task_hashes partition_id')
            if c['population']!=sorted(set(c['population'])) or not c['population'] or any(type(s) is not str or not s for s in c['population']):fail()
            if c['partition_id'] not in partition_ids:fail('unknown_endpoint')
            population=set(c['population'])
            if any(population.intersection(other) for other in populations):fail()
            populations.append(population)
        if any(type(p['ordinal']) is not int for p in record['partitions']) or [p['ordinal'] for p in record['partitions']]!=list(range(len(partition_ids))) or len(partition_ids)>min(8,record['configured_workers']):fail()
        members=[]
        for p in record['partitions']:
            closed(p,'id ordinal component_ids ordered_task_hashes')
            cs=[c for c in record['components'] if c['partition_id']==p['id']]
            if p['component_ids']!=[c['id'] for c in cs] or sorted(p['ordered_task_hashes'])!=sorted(h for c in cs for h in c['ordered_task_hashes']):fail()
            members.extend(p['component_ids'])
        if sorted(members)!=sorted(component_ids):fail()
        owner=record['publication_owner'];closed(owner,'id destination_scopes job_scope_ids writer_mode backend_owners durability')
        if owner['writer_mode']!='serialized_writer' or type(owner['backend_owners']) is not int or owner['backend_owners']!=1 or owner['durability']!='backend_contract_only':fail()
        if owner['destination_scopes']!=sorted({s['destination_scope'] for s in record['job_scopes']}) or owner['job_scope_ids']!=[s['id'] for s in record['job_scopes']]:fail()
        barriers=record['barriers'];ids(barriers)
        expected_kinds=[kind for kind in BARRIER_KINDS if record['mode']!='sequential' or kind!='pool_shutdown_before_coordinator_validation']
        if [b['kind'] for b in barriers]!=expected_kinds:fail()
        before=set()
        for b in barriers:
            closed(b,'id kind task_hashes partition_ids predecessor_ids owner_id evidence source_anchor')
            if b['task_hashes']!=tasks or set(b['partition_ids'])!=partition_ids or not set(b['predecessor_ids'])<=before:fail()
            if b['evidence']!='source_required' or b['owner_id'] not in ('coordinator',owner['id']):fail()
            before.add(b['id']);validate_anchor(b['source_anchor'])
        resource=record['resource_admission'];closed(resource,'budget evidence initial_reserved_resident_bytes cumulative_committed_output_metadata_bytes trace_reserved_bytes whole_job_sampled_rss_cap_bytes additional_host_memory_reserve_bytes cpu_host_reserve_slots capacity original_reason physical_enforcement')
        from equity_feature_workers import ResourceBudget
        parsed=ResourceBudget(**resource['budget'])
        if asdict(parsed)!=resource['budget'] or parsed.workers!=record['configured_workers']:fail()
        if resource['evidence']!='not_executed' or any(resource[k] is not None for k in ['initial_reserved_resident_bytes','cumulative_committed_output_metadata_bytes','capacity','original_reason']):fail()
        for name,value in [('trace_reserved_bytes',67108864),('whole_job_sampled_rss_cap_bytes',1073741824),('additional_host_memory_reserve_bytes',2147483648),('cpu_host_reserve_slots',2)]:
            if type(resource[name]) is not int or resource[name]!=value:fail()
        if resource['physical_enforcement'] is not False or record['cancelled_or_undispatched']!=[]:fail()
        expected_scope=digest(b'eq142-job1\0'+wire({'mode':record['mode'],'configured_workers':record['configured_workers'],'tasks':tasks,'budget':resource['budget']}))
        if record['execution_scope_id']!=expected_scope:fail()
        return None
    except GraphSpecificationError:raise
    except Exception:fail()


def validate_graph(record):
    """Closed structural checks; does not authenticate a caller's qualification claim.

    Qualification comes from describe_task's process-local BoundSource, not JSON.
    """
    if type(record) is not dict or set(record) != ROOT_FIELDS or record['schema'] != 'eq142-output-graph:1':
        fail()
    try:
        if len(wire(record)) > 1048576:fail()
        validate_source(record['source']);validate_scope(record['scope']);validate_input_identity(record['input_identity_disposition'])
        from equity_feature_workers.manifests import _decode
        from equity_feature_io_contracts import FeatureHeader
        if type(record['selected_headers']) is not list or not 1<=len(record['selected_headers'])<=4096:fail()
        headers=[_decode(h) for h in record['selected_headers']]
        if any(type(h) is not FeatureHeader for h in headers) or len({h.feature_id for h in headers})!=len(headers):fail()
        for key in ('task_sha256','reuse_sha256'):
            if not re.fullmatch('[0-9a-f]{64}',record[key]):fail()
        if record['disposition'] not in ('qualified_intraday_description','conservative_unresolved'):fail()
        if record['disposition_reason'] not in ('qualified_builtin','not_selected_family','unqualified_callback'):fail()
        if (record['disposition']=='qualified_intraday_description')!=(record['disposition_reason']=='qualified_builtin'):fail()
        if record['limitations']!=sorted(set(record['limitations'])) or not set(record['limitations'])<=set(LIMITATIONS+['unknown_callback_no_refinement']):fail()
        nodes, edges = record['nodes'], record['edges']
        if type(nodes) is not list or not 1 <= len(nodes) <= 512 or type(edges) is not list or len(edges)>4096:fail()
        ids, seen = set(), set()
        for n in nodes:
            if type(n) is not dict or set(n)!=NODE_FIELDS or not re.fullmatch('[a-z][a-z0-9_.:-]{0,127}',n['id']) or n['kind'] not in KINDS or n['predicate'] not in PREDICATES:fail()
            if n['id'] in ids:fail('duplicate')
            ids.add(n['id'])
            if n['scope_id']!=record['scope']['scope_id']:fail()
            validate_anchor(n['source_anchor'])
            if n['owner'] not in ('caller_supplied','pure_preparation','pure_calculation','task_result','coordinator','serialized_sink'):fail()
            if n['outputs']!=sorted(set(n['outputs'])) or not set(n['outputs']) <= FEATURES and record['disposition']=='qualified_intraday_description':fail()
            if n['capabilities'] not in ([],['batch','full_history_replay']):fail()
            if n['ordering'] is not None:
                order=n['ordering'];closed(order,'policy anchor_index target_index minimum_seed_slots incremental_capability')
                if order['policy']!='chronological_full_epoch' or order['incremental_capability'] is not False:fail()
                for field in ['anchor_index','target_index','minimum_seed_slots']:
                    if type(order[field]) is not int or not 0<=order[field]<=4096:fail()
                if order['target_index']<order['anchor_index'] or order['minimum_seed_slots']<1:fail()
        adjacency = {n:[] for n in ids}; degree = {n:0 for n in ids}
        for e in edges:
            if type(e) is not dict or set(e)!={'from','to','reason','contract'} or CONTRACTS.get(e['reason'])!=e['contract']:fail()
            key=(e['from'],e['to'],e['reason'],e['contract'])
            if key in seen:fail('duplicate')
            seen.add(key)
            if e['from'] not in ids or e['to'] not in ids:fail('unknown_endpoint')
            adjacency[e['from']].append(e['to']);degree[e['to']]+=1
        pending=[n for n in ids if degree[n]==0];visited=0
        while pending:
            n=pending.pop();visited+=1
            for target in adjacency[n]:
                degree[target]-=1
                if degree[target]==0:pending.append(target)
        if visited!=len(ids):fail('cycle')
        if nodes!=sorted(nodes,key=lambda n:n['id']) or edges!=sorted(edges,key=lambda e:(e['from'],e['to'],e['reason'],e['contract'])):fail()
        if record['disposition']=='conservative_unresolved' and any(n['kind'] in ('operand','ordered_state','output') for n in nodes):fail('unqualified_callback')
        if record['disposition']=='qualified_intraday_description':
            expected={'output:'+h.feature_id for h in headers}
            if {n['id'] for n in nodes if n['kind']=='output'}!=expected:fail()
        return None
    except GraphSpecificationError:
        raise
    except Exception:
        fail()
