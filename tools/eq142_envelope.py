"""Development envelope component; not a complete-sample certificate.

The controller must independently establish actual writer/source/namespace and
full original proof authority. No study or performance acceptance is implied.
"""
from decimal import Decimal
import re
from eq142_graph import wire
from eq142_identity import decimal_integer
from eq142_proofs import validate_reference

INSIDE=('outer_wall_start','outer_cpu_start_captured','exclusive_sample_directory_created',
    'outer_source_before_verified','capacity_before_persisted','native_owned_call_entered_before_Kernel_setup',
    'native_owned_call_returned_after_descendants_exit_and_owned_cleanup','native_receipt_persisted',
    'actual_runner_return_and_all_full_evidence_verified','complete_root_validated_persisted_and_read_back',
    'capacity_after_persisted','outer_source_after_verified','outer_cpu_end_captured','outer_wall_end')
FINALIZATION='measurement_write_validation_final_controller_post_envelope_source_check_and_candidate_write_excluded'
MEASUREMENT_FIELDS=set('schema sample_id root_reference selector control observer checkpoints outer_wall_ns outer_coordinator_cpu_ns selected_wall_ns selected_user_ticks_100ns selected_kernel_ticks_100ns capacity_after_reference outer_source_after_sha256 finalization_policy full_sample_accepted cpu_observation'.split())


def closed(value,fields):
    if type(value) is not dict or set(value)!=set(fields):raise ValueError('envelope closed record')


def same(actual,expected):
    if wire(actual)!=wire(expected):raise ValueError('envelope expected binding differs')


def ns(value,*,positive=False):
    return decimal_integer(value,9223372036854775807,positive=positive)


def validate_cpu_clock(clock):
    closed(clock,('name','implementation','monotonic','adjustable','reported_resolution_seconds','cross_process_raw_ticks_qualified'))
    if (clock['name']!='process_time' or type(clock['implementation']) is not str
            or not 1<=len(clock['implementation'])<=256 or not clock['implementation'].isascii()
            or clock['monotonic'] is not True or clock['adjustable'] is not False
            or clock['cross_process_raw_ticks_qualified'] is not False):
        raise ValueError('envelope actual CPU clock')
    resolution=clock['reported_resolution_seconds']
    if (type(resolution) is not str or len(resolution)>64
            or re.fullmatch('[0-9]+(?:[.][0-9]+)?',resolution) is None
            or not Decimal(resolution).is_finite() or Decimal(resolution)<=0):
        raise ValueError('envelope CPU nominal resolution')


def verify_measurement(record,*,root,root_reference,native,phase,capacity_after_reference,
        expected_source_sha256,expected_cpu_clock):
    """Full pure record joins, not source/native/capacity certificate creation."""
    closed(record,MEASUREMENT_FIELDS)
    if type(expected_source_sha256) is not str or re.fullmatch('[0-9a-f]{64}',expected_source_sha256) is None:
        raise ValueError('envelope expected source hash')
    if len(wire(record))>65536:raise ValueError('envelope bytes')
    if (record['schema']!='eq142-measurement-envelope:1' or record['full_sample_accepted'] is not False
            or record['finalization_policy']!=FINALIZATION):raise ValueError('envelope development scope')
    validate_reference(root_reference,scope='sample',name='complete-root.json')
    if root_reference['codec']!='eq142_complete_root':raise ValueError('envelope root codec')
    validate_reference(capacity_after_reference,scope='sample',name='capacity-after.json')
    if capacity_after_reference['codec']!='capacity_receipt':raise ValueError('envelope capacity codec')
    same(record['root_reference'],root_reference);same(record['capacity_after_reference'],capacity_after_reference)
    for field in ('sample_id','selector','control'):same(record[field],root[field])
    same(record['outer_source_after_sha256'],expected_source_sha256)
    same(root['outer_source_before_sha256'],expected_source_sha256)
    observer=native['native_closure']['observer']
    same(record['observer'],{k:observer[k] for k in ('pid','creation_filetime_100ns_since_1601_utc','clock')})
    checkpoints=record['checkpoints'];closed(checkpoints,INSIDE)
    times=[ns(checkpoints[key]) for key in INSIDE]
    if times!=sorted(times) or times[-1]<=times[0] or ns(record['outer_wall_ns'],positive=True)!=times[-1]-times[0]:
        raise ValueError('envelope outer local wall ordering')
    native_times=[ns(value) for value in native['native_closure']['boundaries'].values()]
    entered=ns(checkpoints['native_owned_call_entered_before_Kernel_setup'])
    returned=ns(checkpoints['native_owned_call_returned_after_descendants_exit_and_owned_cleanup'])
    if not native_times or not entered<=min(native_times)<=max(native_times)<=returned:
        raise ValueError('envelope native call containment on same observer clock')
    cpu=record['cpu_observation'];closed(cpu,('clock','start_ns','end_ns'))
    validate_cpu_clock(cpu['clock']);validate_cpu_clock(expected_cpu_clock);same(cpu['clock'],expected_cpu_clock)
    start,end=ns(cpu['start_ns']),ns(cpu['end_ns'])
    if end<start or ns(record['outer_coordinator_cpu_ns'])!=end-start:
        raise ValueError('envelope separate coordinator CPU delta')
    if type(native['external_observer_cpu_ns']) is not int or not 0<=native['external_observer_cpu_ns']<=end-start:
        raise ValueError('envelope native observer CPU subset')
    if root['selector']['series']=='eq142_owned_runner_e2e':
        wall=record['outer_wall_ns'];user=str(native['final_accounting']['user']);kernel=str(native['final_accounting']['kernel'])
    elif root['selector']['series']=='eq142_admitted_control':
        wall=phase['wall_ns'];user=phase['user_ticks_100ns'];kernel=phase['kernel_ticks_100ns']
    else:raise ValueError('envelope selected series')
    ns(wall,positive=True);ns(user);ns(kernel)
    same(record['selected_wall_ns'],wall);same(record['selected_user_ticks_100ns'],user)
    same(record['selected_kernel_ticks_100ns'],kernel)
    return {'scope':'outer_envelope_record_joins_only','full_sample_accepted':False}


def verify_candidate(record,*,root,root_reference,measurement_reference,final_source_sha256):
    closed(record,('schema','sample_id','root_reference','measurement_reference',
        'final_controller_post_envelope_source_state_sha256','disposition','full_sample_accepted'))
    if type(final_source_sha256) is not str or re.fullmatch('[0-9a-f]{64}',final_source_sha256) is None:
        raise ValueError('candidate final source hash')
    if (record['schema']!='eq142-complete-candidate:1' or record['full_sample_accepted'] is not False
            or record['disposition']!='development_evidence_closed_not_official_study' or len(wire(record))>4096):
        raise ValueError('candidate development scope')
    validate_reference(root_reference,scope='sample',name='complete-root.json')
    validate_reference(measurement_reference,scope='sample',name='measurement.json')
    if root_reference['codec']!='eq142_complete_root' or measurement_reference['codec']!='eq142_measurement_envelope':
        raise ValueError('candidate fixed codec')
    same(record['sample_id'],root['sample_id']);same(record['root_reference'],root_reference)
    same(record['measurement_reference'],measurement_reference)
    same(record['final_controller_post_envelope_source_state_sha256'],final_source_sha256)
    same(final_source_sha256,root['outer_source_before_sha256'])
    return {'scope':'candidate_record_joins_only','full_sample_accepted':False}
