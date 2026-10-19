"""Pure bounded native-local receipt checks, never complete sample acceptance.

No fetching, process probes, filesystem reads or raw cross-process clock joins.
Caller must independently bind original retained bytes/source and trace records.
"""
import json
import math
import re
from decimal import Decimal

from eq142_identity import decimal_integer,clock_valid,join_owned_processes

BOUNDARIES=('observer_start_ns','runner_created_ns','runner_assigned_ns',
    'runner_resume_before_ns','runner_resume_after_ns','runner_exit_observed_ns',
    'job_empty_observed_ns','final_accounting_end_ns','native_measurement_end_ns')
ACCOUNTING=set('user kernel period_user period_kernel page_faults total_processes active_processes terminated_by_limit'.split())
DWORD_ACCOUNTING=set('page_faults total_processes active_processes terminated_by_limit'.split())
TOP=set('job_name samples probe_errors sample_interval_s observer_clock_nominal_resolution_s wall_clock_nominal_resolution_s containment hard_memory_limit rss_limits cpu_limits observed_lifetimes evidence_bounds schema native_closure runner_pid suspended_accounting exit_code cpu_user_100ns cpu_kernel_100ns cpu_seconds final_accounting pids_observed aggregate_sampled_peak_rss_bytes aggregate_sampled_peak_native_threads thread_limits peak_job_private_commit_bytes external_observer_cpu_ns wall_ns'.split())
CREATED='creation_filetime_100ns_since_1601_utc'


def closed(value,fields):
    if type(value) is not dict or set(value)!=set(fields):raise ValueError('native closed record')


def count(value,maximum=9223372036854775807,minimum=0):
    if type(value) is not int or not minimum<=value<=maximum:raise ValueError('native integer bound')
    return value


def lifetime(value):
    decimal_integer(value[CREATED],18446744073709551615,positive=True)
    return (count(value['pid'],4294967295,1),value[CREATED])


def accounting(value):
    closed(value,ACCOUNTING)
    for field in ACCOUNTING:count(value[field],4294967295 if field in DWORD_ACCOUNTING else 9223372036854775807)
    if value['active_processes']>value['total_processes']:raise ValueError('native accounting population')


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)+'\n').encode('ascii')


def validate_native_closure(native,witnesses,*,parent_nonce,traced_processes=None):
    """Validate native-local facts; unsampled witnesses get no resource values."""
    closed(native,TOP)
    if native['schema']!='eq142-development-native-receipt:2' or len(encode(native))>16777216:
        raise ValueError('native receipt schema/bytes')
    if type(native['job_name']) is not str or re.fullmatch(r'Local\\equity-feature-supplied-[0-9a-f]{32}',native['job_name']) is None:
        raise ValueError('native Job identity')
    if native['probe_errors']!=[] or type(native['probe_errors']) is not list:raise ValueError('native probe error')
    if native['hard_memory_limit'] is not False or native['containment']!='owned job; kill on close; no breakaway':
        raise ValueError('native resource scope')
    for field in ('rss_limits','cpu_limits','thread_limits'):
        if type(native[field]) is not str or not 1<=len(native[field])<=1024:raise ValueError('native scope text')
    for field in ('sample_interval_s','observer_clock_nominal_resolution_s','wall_clock_nominal_resolution_s'):
        value=native[field]
        if type(value) not in (float,int) or not math.isfinite(value) or value<=0:raise ValueError('native nominal/interval')
    if native['sample_interval_s']>0.1:raise ValueError('native interval bound')
    if native['evidence_bounds']!={'receipt_max_bytes':16777216,'rows_max_bytes':15728640,
            'max_samples':60001,'max_probe_errors':1024,'max_observed_lifetimes':256,'physical_memory_enforced':False}:
        raise ValueError('native evidence bounds')
    # Equality alone must not admit bools where declared integer limits belong.
    closed(native['evidence_bounds'],('receipt_max_bytes','rows_max_bytes','max_samples','max_probe_errors','max_observed_lifetimes','physical_memory_enforced'))
    for key,value in native['evidence_bounds'].items():
        if key!='physical_memory_enforced':count(value)
    if native['evidence_bounds']['physical_memory_enforced'] is not False:raise ValueError('native physical guarantee')
    closure=native['native_closure']
    closed(closure,('schema','observer','runner','boundaries','job_empty_accounting','scope'))
    if closure['schema']!='eq142-native-local-closure:1' or closure['scope']!='native_local_not_selected_complete_e2e':
        raise ValueError('native local scope')
    observer=closure['observer'];runner=closure['runner']
    closed(observer,('pid',CREATED,'is_member','clock'))
    observer_pair=lifetime(observer);clock_valid(observer['clock'],require_frequency=True)
    if Decimal(str(native['wall_clock_nominal_resolution_s']))!=Decimal(observer['clock']['reported_resolution_seconds']):
        raise ValueError('native same-clock nominal resolution differs')
    if observer['is_member'] is not False:raise ValueError('observer in owned Job')
    closed(runner,('pid',CREATED,'is_member','terminal_wait','terminal_creation_filetime_100ns_since_1601_utc'))
    runner_pair=lifetime(runner)
    if (runner['is_member'] is not True or count(runner['terminal_wait'],0)!=0
            or runner['terminal_creation_filetime_100ns_since_1601_utc']!=runner[CREATED]
            or runner['pid']==observer['pid'] or count(native['runner_pid'],4294967295,1)!=runner['pid']):
        raise ValueError('native runner lifetime/terminal')
    closed(closure['boundaries'],BOUNDARIES)
    ticks=[decimal_integer(closure['boundaries'][field],9223372036854775807) for field in BOUNDARIES]
    if ticks!=sorted(ticks) or count(native['wall_ns'])!=ticks[-1]-ticks[0]:raise ValueError('native local intervals')
    for field in ('suspended_accounting','final_accounting'):accounting(native[field])
    empty=closure['job_empty_accounting'];accounting(empty)
    final=native['final_accounting'];suspended=native['suspended_accounting']
    if (suspended['active_processes']!=1 or suspended['total_processes']!=1
            or empty['active_processes']!=0 or final['active_processes']!=0
            or empty['terminated_by_limit']!=0 or final['terminated_by_limit']!=0
            or final['total_processes']!=empty['total_processes'] or final['total_processes']<1
            or count(native['exit_code'],4294967295)!=0):raise ValueError('native final disposition')
    for field in ACCOUNTING-{'active_processes'}:
        if not suspended[field]<=empty[field]<=final[field]:raise ValueError('native accounting regression')
    if (count(native['cpu_user_100ns'])!=final['user'] or count(native['cpu_kernel_100ns'])!=final['kernel']
            or type(native['cpu_seconds']) is not float or not math.isfinite(native['cpu_seconds'])
            or native['cpu_seconds']!=(final['user']+final['kernel'])/10000000):raise ValueError('native whole Job CPU')
    count(native['external_observer_cpu_ns']);count(native['peak_job_private_commit_bytes'])
    inventory=native['observed_lifetimes']
    if type(inventory) is not list or len(inventory)>256:raise ValueError('native lifetime bound')
    inventory_pairs=[]
    for row in inventory:
        closed(row,('pid',CREATED));inventory_pairs.append(lifetime(row))
    if len(set(inventory_pairs))!=len(inventory_pairs) or inventory_pairs!=sorted(inventory_pairs,key=lambda p:(p[0],int(p[1]))):
        raise ValueError('native lifetime inventory')
    samples=native['samples']
    if type(samples) is not list or not 1<=len(samples)<=60001:raise ValueError('native sample bound')
    used={};rss_peak=threads_peak=0;previous=0;encoded_rows=4;seen_pids=set()
    previous_total=suspended['total_processes']
    sample_min=ticks[4]-ticks[0];sample_max=ticks[6]-ticks[0]
    for sample in samples:
        closed(sample,('native_thread_count','elapsed_ns','active_processes','total_processes','rss_bytes','processes'))
        elapsed=count(sample['elapsed_ns'])
        if not max(previous,sample_min)<=elapsed<=sample_max:raise ValueError('native sample interval')
        previous=elapsed
        if (count(sample['active_processes'],4294967295)>count(sample['total_processes'],4294967295)
                or not previous_total<=sample['total_processes']<=empty['total_processes']):
            raise ValueError('native sample accounting')
        previous_total=sample['total_processes']
        rows=sample['processes']
        if type(rows) is not list or len(rows)>256:raise ValueError('native process bound')
        pids=set();rss=threads=0
        for row in rows:
            closed(row,('pid',CREATED,'rss_bytes','native_threads','thread_disposition'))
            pair=lifetime(row)
            if pair not in inventory_pairs or pair[0]==observer_pair[0] or pair[0] in pids:raise ValueError('native sampled lifetime')
            pids.add(pair[0]);seen_pids.add(pair[0]);used[pair]=used.get(pair,0)+1
            rss+=count(row['rss_bytes']);threads+=count(row['native_threads'],4294967295)
            disposition=row['thread_disposition'];closed(disposition,('state','terminal_wait','snapshot_threads',CREATED))
            if disposition[CREATED]!=row[CREATED]:raise ValueError('native thread/RSS lifetime')
            if disposition['state']=='observed_live':
                if count(disposition['terminal_wait'],258)!=258 or count(disposition['snapshot_threads'],4294967295,1)!=row['native_threads']:
                    raise ValueError('native live threads')
            elif disposition['state']=='observed_terminated':
                if count(disposition['terminal_wait'],0)!=0 or row['native_threads']!=0:raise ValueError('native terminal threads')
                if disposition['snapshot_threads'] is not None:count(disposition['snapshot_threads'],4294967295)
            else:raise ValueError('native thread disposition')
        if count(sample['rss_bytes'])!=rss or count(sample['native_thread_count'])!=threads:raise ValueError('native sampled sums')
        rss_peak=max(rss_peak,rss);threads_peak=max(threads_peak,threads)
        encoded_rows+=len(encode(sample))
    if encoded_rows>15728640 or set(used)!=set(inventory_pairs) or len(inventory_pairs)>final['total_processes']:
        raise ValueError('native row/inventory bound')
    if (type(native['pids_observed']) is not list or native['pids_observed']!=sorted(seen_pids)
            or any(type(pid) is not int for pid in native['pids_observed'])
            or count(native['aggregate_sampled_peak_rss_bytes'])!=rss_peak or rss_peak>1073741824
            or count(native['aggregate_sampled_peak_native_threads'])!=threads_peak):raise ValueError('native sampled peak')
    if traced_processes is None:
        if type(witnesses) is not list or len(witnesses)!=1:raise ValueError('native original control parent only')
        witness=witnesses[0]
        traced_processes={witness['process_startup_nonce']:{'pid':witness['pid'],'clock':witness['clock']}}
    joined=join_owned_processes(witnesses,traced_processes,job_name=native['job_name'])
    parent=[row for row in joined if row['process_startup_nonce']==parent_nonce]
    if len(parent)!=1:raise ValueError('native calculation parent missing')
    parent_pair=lifetime(parent[0])
    if parent_pair[0]==runner_pair[0] and parent_pair!=runner_pair:
        raise ValueError('native parent/launcher lifetime differs for same PID')
    traced_pairs=set();qualified=[]
    for row in joined:
        pair=lifetime(row)
        if pair[0]==observer_pair[0] or row['clock']!=observer['clock']:raise ValueError('native witness local metadata')
        traced_pairs.add(pair)
        qualified.append({**row,'sampled':pair in used,'sample_count':used.get(pair,0)})
    if len({runner_pair,*traced_pairs,*inventory_pairs})>final['total_processes']:
        raise ValueError('native actual lifetime union exceeds total processes')
    return {'schema':'eq142-native-local-validation:1','scope':'native_local_not_selected_complete_e2e',
        'initial_runner':{'pid':runner_pair[0],CREATED:runner_pair[1]},
        'calculation_parent':{'process_startup_nonce':parent_nonce,'pid':parent_pair[0],CREATED:parent_pair[1]},
        'parent_is_initial_runner':parent_pair==runner_pair,'traced_lifetimes':qualified,
        'untraced_sampled_lifetimes':[{'pid':pid,CREATED:created,'sample_count':used[(pid,created)]}
            for pid,created in inventory_pairs if (pid,created) not in traced_pairs],
        'final_total_processes':final['total_processes'],'wall_ns':str(native['wall_ns']),
        'whole_job_user_100ns':str(final['user']),'whole_job_kernel_100ns':str(final['kernel']),
        'sampled_peak_rss_bytes':rss_peak,'sampled_peak_native_threads':threads_peak,'full_sample_accepted':False}
