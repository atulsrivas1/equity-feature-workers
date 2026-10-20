"""Development count index component; not a complete-sample certificate.

The controller must independently establish actual writer/source/namespace and
full original proof authority. No study or performance acceptance is implied.
"""
import re
from eq142_graph import wire
from eq142_owned_runner import PROFILES,SERIES
from eq142_proofs import check_body,decode_record,validate_reference

INDEX_FIELDS=set('schema stage source matrix_sha256 capacity_freeze_reference entries all_source_invariants_passed full_sample_accepted'.split())
ENTRY_FIELDS=set('cohort_id selector runner_return_reference runner_reference native_reference profiles comparison source_invariant_disposition timing_authority'.split())
DISPOSITION='exact_fixed_invariants_and_actual_variable_wait_relations_validated'


def closed(value,fields):
    if type(value) is not dict or set(value)!=fields:raise ValueError('count index closed record')


def same(actual,expected):
    if wire(actual)!=wire(expected):raise ValueError('count index independently checked binding')


def digest(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None:raise ValueError('count index digest')


def validate_entry(entry):
    closed(entry,ENTRY_FIELDS)
    cohort=entry['cohort_id']
    if type(cohort) is not str or re.fullmatch('[a-z][a-z0-9_.-]{0,127}',cohort) is None:
        raise ValueError('count index fixed cohort identity')
    closed(entry['selector'],set(('profile','series','mode','degree')))
    selector=entry['selector'];mode=selector['mode'];degree=selector['degree'];profile=selector['profile']
    if (type(profile) is not str or profile not in PROFILES
            or selector['series'] not in SERIES
            or type(mode) is not str or mode not in ('sequential','thread','process')
            or type(degree) is not int or degree not in (1,2,4,8)
            or mode=='sequential' and degree!=1
            or profile.startswith('family_') and (mode,degree) not in (('sequential',1),('thread',2),('process',2))):
        raise ValueError('count index frozen selector')
    for field,codec,name,scope in (
            ('runner_return_reference','eq142_owned_runner_return','runner-reference.json','sample'),
            ('runner_reference','eq142_development_runner','runner.json','runner'),
            ('native_reference','native_receipt','native.json','sample')):
        reference=entry[field];validate_reference(reference,scope=scope,name=name)
        if reference['codec']!=codec:raise ValueError('count index routed reference')
    profiles=entry['profiles']
    if type(profiles) is not list or not 1<=len(profiles)<=17 or mode!='process' and len(profiles)!=1 or mode=='process' and len(profiles)<2:
        raise ValueError('count index profile population')
    for index,reference in enumerate(profiles):
        name='parent.json' if index==0 else f'delegate-{index-1:03d}.json'
        validate_reference(reference,scope='code_counts',name=name)
        if reference['codec']!='eq142_code_counts':raise ValueError('count index profile codec')
    comparison=entry['comparison']
    closed(comparison,set(('operations','call_entries','typed_byte_returns','typed_returned_bytes','scope')))
    if comparison['scope']!='qualification aggregate only; no timing or completion-status authority':
        raise ValueError('count index comparison scope')
    for key in ('operations','call_entries','typed_byte_returns','typed_returned_bytes'):
        if type(comparison[key]) is not int or not 0<=comparison[key]<=9007199254740991:
            raise ValueError('count index strict comparison count')
    if (comparison['operations']>43 or comparison['typed_byte_returns']>comparison['call_entries']
            or comparison['typed_byte_returns']==0 and comparison['typed_returned_bytes']!=0
            or entry['source_invariant_disposition']!=DISPOSITION or entry['timing_authority'] is not False):
        raise ValueError('count index source/count disposition')


def verify_count_index(body,reference,*,source,matrix_sha256,capacity_freeze_reference,checked_entries):
    """Re-read exact fixed index against all independently rebuilt entries.

    This binds reuse only after the controller has supplied every current full
    proof result; it neither runs those checks nor authenticates arbitrary maps.
    Whole fixed namespace, capacity/matrix source origin and final history are
    separate mandatory controller responsibilities. Never enables timing.
    """
    digest(matrix_sha256)
    validate_reference(capacity_freeze_reference,scope='qualification',name='qualification-capacity.json')
    if capacity_freeze_reference['codec']!='capacity_receipt':raise ValueError('count index capacity codec')
    if (type(checked_entries) is not list or not 1<=len(checked_entries)<=72):
        raise ValueError('count index checked population')
    cohorts=[];selectors=[]
    for entry in checked_entries:
        validate_entry(entry);cohorts.append(entry['cohort_id']);selectors.append(wire(entry['selector']))
    if len(set(cohorts))!=len(cohorts) or len(set(selectors))!=len(selectors):
        raise ValueError('count index duplicate checked cohort')
    raw=check_body(body,reference,scope='qualification',name='count-qualification.json')
    if reference['codec']!='eq142_count_qualification':raise ValueError('count index fixed codec')
    record=decode_record(raw,maximum=1048576);closed(record,INDEX_FIELDS)
    if (record['schema']!='eq142-count-qualification:1' or record['stage']!='qualification_only_no_timing_authority'
            or record['all_source_invariants_passed'] is not True or record['full_sample_accepted'] is not False):
        raise ValueError('count index qualification disposition')
    same(record['source'],source);same(record['matrix_sha256'],matrix_sha256)
    same(record['capacity_freeze_reference'],capacity_freeze_reference);same(record['entries'],checked_entries)
    return {'cohorts':len(cohorts),'matrix_sha256':matrix_sha256,
        'scope':'index_joins_against_trusted_independently_checked_entries_only','timing_authority':False,'full_sample_accepted':False}
