"""Independent native receipt mutations and actual owned Windows exit probes."""
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
from eq142_native import Kernel,NativeFailure,run_owned,cleanup_owned
from eq142_native_closure import validate_native_closure

CREATED='creation_filetime_100ns_since_1601_utc'
JOB='Local\\equity-feature-supplied-'+'a'*32


def clock():
    return {'name':'perf_counter','implementation':'QueryPerformanceCounter()','monotonic':True,
        'adjustable':False,'reported_resolution_seconds':'0.0000001','qpc_frequency_hz':10000000,
        'cross_process_raw_ticks_qualified':False}


def account(*,total=2,active=0,user=10,kernel=20,faults=2):
    return {'user':user,'kernel':kernel,'period_user':user,'period_kernel':kernel,'page_faults':faults,
        'total_processes':total,'active_processes':active,'terminated_by_limit':0}


def witness(pid=1,created='101',nonce='b'*32):
    return {'schema':'eq142-owned-process-witness:1','job_name':JOB,'pid':pid,
        'process_startup_nonce':nonce,CREATED:created,'is_member':True,
        'capture_start_ns':'1234','capture_end_ns':'1235','clock':clock()}


def receipt():
    # Deliberately unsampled parent and sampled untraced child. Active0 with one
    # row is permitted because accounting and process/thread queries are not atomic.
    row={'pid':3,CREATED:'303','rss_bytes':4096,'native_threads':0,
        'thread_disposition':{'state':'observed_terminated','terminal_wait':0,'snapshot_threads':None,CREATED:'303'}}
    return {'schema':'eq142-development-native-receipt:2','job_name':JOB,
        'samples':[{'native_thread_count':0,'elapsed_ns':44,'active_processes':0,'total_processes':2,
            'rss_bytes':4096,'processes':[row]}],'probe_errors':[],
        'sample_interval_s':0.01,'observer_clock_nominal_resolution_s':0.0000001,
        'wall_clock_nominal_resolution_s':0.0000001,'containment':'owned job; kill on close; no breakaway',
        'hard_memory_limit':False,'rss_limits':'sampled only','cpu_limits':'whole owned Job',
        'observed_lifetimes':[{'pid':3,CREATED:'303'}],
        'evidence_bounds':{'receipt_max_bytes':16777216,'rows_max_bytes':15728640,'max_samples':60001,
            'max_probe_errors':1024,'max_observed_lifetimes':256,'physical_memory_enforced':False},
        'native_closure':{'schema':'eq142-native-local-closure:1',
            'observer':{'pid':9,CREATED:'909','is_member':False,'clock':clock()},
            'runner':{'pid':1,CREATED:'101','is_member':True,'terminal_wait':0,
                'terminal_creation_filetime_100ns_since_1601_utc':'101'},
            'boundaries':dict(zip(('observer_start_ns','runner_created_ns','runner_assigned_ns',
                'runner_resume_before_ns','runner_resume_after_ns','runner_exit_observed_ns',
                'job_empty_observed_ns','final_accounting_end_ns','native_measurement_end_ns'),
                map(str,range(10,100,10)))),
            'job_empty_accounting':account(),'scope':'native_local_not_selected_complete_e2e'},
        'runner_pid':1,'suspended_accounting':account(total=1,active=1,user=0,kernel=0,faults=0),
        'exit_code':0,'cpu_user_100ns':10,'cpu_kernel_100ns':20,'cpu_seconds':0.000003,
        'final_accounting':account(),'pids_observed':[3],'aggregate_sampled_peak_rss_bytes':4096,
        'aggregate_sampled_peak_native_threads':0,'thread_limits':'non-atomic sampled threads',
        'peak_job_private_commit_bytes':8192,'external_observer_cpu_ns':2,'wall_ns':80}


class PortableNativeClosure(unittest.TestCase):
    def validate(self,value,values=None):
        return validate_native_closure(value,[witness()] if values is None else values,parent_nonce='b'*32)

    def test_unsampled_parent_is_not_zero_resource_or_complete_e2e(self):
        result=self.validate(receipt())
        parent=result['traced_lifetimes'][0]
        self.assertFalse(parent['sampled']);self.assertEqual(parent['sample_count'],0)
        self.assertNotIn('rss_bytes',parent);self.assertNotIn('native_threads',parent)
        self.assertEqual(result['untraced_sampled_lifetimes'],[{'pid':3,CREATED:'303','sample_count':1}])
        self.assertEqual(result['sampled_peak_rss_bytes'],4096);self.assertFalse(result['full_sample_accepted'])
        self.assertEqual(result['scope'],'native_local_not_selected_complete_e2e')
        value=receipt();value['cpu_user_100ns']=value['cpu_kernel_100ns']=0;value['cpu_seconds']=0.0
        for key in ('job_empty_accounting',):
            value['native_closure'][key].update(user=0,kernel=0,period_user=0,period_kernel=0)
        value['final_accounting'].update(user=0,kernel=0,period_user=0,period_kernel=0)
        self.assertEqual(self.validate(value)['whole_job_user_100ns'],'0')

    def test_closed_types_lifetimes_bounds_clocks_and_non_atomic_arithmetic(self):
        mutations=[
            (('extra',),0),(('runner_pid',),True),(('exit_code',),259),(('wall_ns',),79),
            (('probe_errors',),[{'code':'unknown'}]),(('aggregate_sampled_peak_rss_bytes',),0),
            (('aggregate_sampled_peak_native_threads',),False),(('pids_observed',),[True]),
            (('native_closure','observer','is_member'),True),
            (('native_closure','observer','clock','qpc_frequency_hz'),0),
            (('native_closure','runner','terminal_wait'),True),
            (('native_closure','runner','terminal_creation_filetime_100ns_since_1601_utc'),'102'),
            (('native_closure','boundaries','job_empty_observed_ns'),'50'),
            (('native_closure','boundaries','runner_resume_before_ns'),None),
            (('native_closure','job_empty_accounting','active_processes'),1),
            (('final_accounting','terminated_by_limit'),1),(('cpu_user_100ns',),11),
            (('evidence_bounds','max_observed_lifetimes'),257),
            (('evidence_bounds','physical_memory_enforced'),0),
            (('samples',0,'native_thread_count'),1),(('samples',0,'elapsed_ns'),81),
            (('samples',0,'elapsed_ns'),39),(('samples',0,'elapsed_ns'),61),
            (('wall_clock_nominal_resolution_s',),0.001),
            (('final_accounting','total_processes'),4294967296),
            (('final_accounting','page_faults'),4294967296),
            (('samples',0,'active_processes'),4294967296),
            (('samples',0,'processes',0,'native_threads'),4294967296),
            (('samples',0,'processes',0,'thread_disposition','snapshot_threads'),4294967296),
            (('samples',0,'processes',0,CREATED),'0303'),
            (('samples',0,'processes',0,'thread_disposition','terminal_wait'),258),
            (('samples',0,'processes',0,'thread_disposition',CREATED),'304'),
            (('samples',0,'processes',0,'native_threads'),None)]
        for path,replacement in mutations:
            with self.subTest(path=path):
                value=receipt();target=value
                for key in path[:-1]:target=target[key]
                target[path[-1]]=replacement
                with self.assertRaises(ValueError):self.validate(value)
        value=receipt();value['samples'][0]['processes']*=2
        with self.assertRaises(ValueError):self.validate(value)
        value=receipt();value['observed_lifetimes']*=2
        with self.assertRaises(ValueError):self.validate(value)
        wrong=witness();wrong[CREATED]='102'
        with self.assertRaises(ValueError):self.validate(receipt(),[wrong])
        value=receipt();value['samples'][0]['processes'][0]['rss_bytes']=1073741825
        value['samples'][0]['rss_bytes']=value['aggregate_sampled_peak_rss_bytes']=1073741825
        with self.assertRaises(ValueError):self.validate(value)

    def test_reused_pid_is_two_lifetimes_and_no_invented_trace_nonce(self):
        value=receipt();second=deepcopy(value['samples'][0]);second['elapsed_ns']=45
        second['processes'][0][CREATED]='304';second['processes'][0]['thread_disposition'][CREATED]='304'
        value['samples'].append(second);value['observed_lifetimes'].append({'pid':3,CREATED:'304'})
        for sample in value['samples']:sample['total_processes']=3
        value['final_accounting']['total_processes']=value['native_closure']['job_empty_accounting']['total_processes']=3
        result=self.validate(value)
        self.assertEqual([r[CREATED] for r in result['untraced_sampled_lifetimes']],['303','304'])
        self.assertTrue(all('process_startup_nonce' not in r for r in result['untraced_sampled_lifetimes']))

    def test_cumulative_job_counts_and_abi_widths_are_independent_of_active_rows(self):
        value=receipt();second=deepcopy(value['samples'][0]);second['elapsed_ns']=45;second['total_processes']=1
        value['samples'].append(second)
        with self.assertRaises(ValueError):self.validate(value)
        for field in ('page_faults','total_processes','active_processes','terminated_by_limit'):
            with self.subTest(field=field):
                value=receipt();value['suspended_accounting'][field]=4294967296
                with self.assertRaises(ValueError):self.validate(value)
        for field in ('user','kernel','period_user','period_kernel'):
            with self.subTest(field=field):
                value=receipt();value['final_accounting'][field]=9223372036854775808
                with self.assertRaises(ValueError):self.validate(value)

    def test_initial_launcher_and_calculation_parent_are_separate_owned_lifetimes(self):
        value=receipt();runner=value['native_closure']['runner'];runner.update(pid=2)
        runner[CREATED]=runner['terminal_creation_filetime_100ns_since_1601_utc']='202'
        value['runner_pid']=2
        for record in (value['final_accounting'],value['native_closure']['job_empty_accounting']):record['total_processes']=3
        value['samples'][0]['total_processes']=3
        result=self.validate(value)
        self.assertFalse(result['parent_is_initial_runner']);self.assertEqual(result['initial_runner']['pid'],2)
        self.assertEqual(result['calculation_parent']['pid'],1)
        for record in (value['final_accounting'],value['native_closure']['job_empty_accounting']):record['total_processes']=2
        value['samples'][0]['total_processes']=2
        with self.assertRaises(ValueError):self.validate(value)

    def test_wait_states_do_not_infer_exit_from_unknown(self):
        kernel=object.__new__(Kernel)
        for state in (0,258):
            kernel.WaitForSingleObject=lambda *args:state
            self.assertEqual(kernel.runner_wait(123),state)
        for state in (128,0xffffffff,1):
            kernel.WaitForSingleObject=lambda *args:state
            with self.assertRaises(NativeFailure) as caught:kernel.runner_wait(123)
            self.assertEqual(caught.exception.code,'RUNNER_WAIT')


class PortableOwnedCleanup(unittest.TestCase):
    def test_null_or_colliding_job_refusal_keeps_primary_without_using_foreign_job(self):
        for mode in ('null','closed','false','exception'):
            with self.subTest(mode=mode):
                calls=[]
                class FakeKernel:
                    def CreateJobObjectW(self,*args):calls.append('create_reference');return None if mode=='null' else 77
                    def CloseHandle(self,handle):
                        calls.append(('close',handle))
                        if mode=='exception':raise OSError('synthetic reference close refusal')
                        return mode=='closed'
                    def SetInformationJobObject(self,*args):raise AssertionError('foreign Job configuration')
                    def CreateProcessW(self,*args):raise AssertionError('runner created after unique refusal')
                    def AssignProcessToJobObject(self,*args):raise AssertionError('foreign Job assignment')
                    def TerminateJobObject(self,*args):raise AssertionError('foreign Job termination')
                with patch('eq142_native.Kernel',FakeKernel),patch('eq142_native.ct.set_last_error',create=True),patch(
                        'eq142_native.ct.get_last_error',return_value=183,create=True):
                    with self.assertRaises(NativeFailure) as caught:run_owned(['fake-owned-runner'],cwd=ROOT,timeout_s=1)
                self.assertEqual(caught.exception.code,'CREATE_UNIQUE_JOB')
                self.assertIsNone(caught.exception.__cause__)
                evidence=caught.exception.evidence
                self.assertEqual(calls,['create_reference'] if mode=='null' else ['create_reference',('close',77)])
                self.assertEqual(evidence['reference_close_attempted'],mode!='null')
                self.assertIs(evidence['reference_close_succeeded'],True if mode=='closed' else False if mode=='false' else None)
                if mode in ('false','exception'):
                    self.assertTrue(evidence['cleanup_failed']);self.assertEqual(len(evidence['cleanup_failures']),1)
                    self.assertEqual(evidence['cleanup_failures'][0]['error_type'],
                        'supplied_native.NativeFailure' if mode=='false' else 'builtins.OSError')
                else:self.assertNotIn('cleanup_failed',evidence)

    def test_persistent_query_and_close_failures_keep_primary_and_attempt_all_owned_handles(self):
        for close_fault in (False,True):
            with self.subTest(close_fault=close_fault):
                calls=[];primary=NativeFailure('PRIMARY_QUERY',{'winerror':5})
                cause=RuntimeError('independent original cause');primary.__cause__=cause
                class FakeKernel:
                    def CreateJobObjectW(self,*args):calls.append(('job',77));return 77
                    def observer_identity(self,*args):return receipt()['native_closure']['observer']
                    def SetInformationJobObject(self,*args):return True
                    def CreateProcessW(self,*args):
                        process=args[-1]._obj;process.process=102;process.thread=101;process.pid=123;return True
                    def AssignProcessToJobObject(self,*args):calls.append(('assign',77));return True
                    def runner_identity(self,*args):return receipt()['native_closure']['runner']
                    def accounting(self,*args):
                        calls.append(('query',77))
                        raise primary if sum(c[0]=='query' for c in calls)==1 else NativeFailure('CLEANUP_QUERY')
                    def TerminateJobObject(self,handle,*args):calls.append(('terminate',handle));return True
                    def WaitForSingleObject(self,handle,*args):calls.append(('wait',handle));return 0
                    def CloseHandle(self,handle):
                        calls.append(('close',handle))
                        if close_fault and handle==101:return False
                        if close_fault and handle==102:raise RuntimeError('independent close failure')
                        return True
                with patch('eq142_native.Kernel',FakeKernel),patch('eq142_native.ct.set_last_error',create=True),patch(
                        'eq142_native.ct.get_last_error',return_value=0,create=True):
                    with self.assertRaises(NativeFailure) as caught:run_owned(['fake-owned-runner'],cwd=ROOT,timeout_s=1)
                self.assertIs(caught.exception,primary);self.assertIs(primary.__cause__,cause)
                self.assertEqual([v for k,v in calls if k=='close'],[101,102,77])
                self.assertIn(('terminate',77),calls);self.assertIn(('wait',102),calls)
                self.assertTrue(primary.evidence['cleanup_failed'])
                self.assertEqual(primary.evidence['primary_failure_identity']['code'],'PRIMARY_QUERY')
                self.assertEqual(primary.evidence['primary_failure_detail'],{'winerror':5})
                stages={row['stage'] for row in primary.evidence['cleanup_failures']}
                self.assertTrue({'query_owned_job','reap_owned_job'}<=stages)
                if close_fault:self.assertTrue({'close_runner_thread','close_runner_process'}<=stages)
                self.assertLessEqual(len(primary.evidence['cleanup_failures']),16)

    def test_probe_close_failure_attempts_all_and_does_not_mask_original(self):
        for primary in (None,NativeFailure('ORIGINAL_PROBE',{'winerror':7})):
            with self.subTest(primary=primary is not None):
                kernel=object.__new__(Kernel);closed=[]
                def close(handle):closed.append(handle);return handle!=101
                kernel.CloseHandle=close
                if primary is None:
                    with self.assertRaises(NativeFailure) as caught:
                        kernel.close_probe_handles([('snapshot',101),('member',102)])
                    error=caught.exception;self.assertEqual(error.code,'PROBE_HANDLE_CLEANUP')
                else:
                    cause=RuntimeError('real existing cause');primary.__cause__=cause
                    kernel.close_probe_handles([('snapshot',101),('member',102)],primary)
                    error=primary;self.assertEqual(primary.code,'ORIGINAL_PROBE');self.assertIs(primary.__cause__,cause)
                    self.assertEqual(error.evidence['winerror'],7)
                self.assertEqual(closed,[101,102]);self.assertEqual(len(error.evidence['probe_handle_close_failures']),1)

    def test_cleanup_termination_and_reap_errors_do_not_skip_closes(self):
        from eq142_native import Process
        process=Process();process.thread=101;process.process=102;closed=[]
        class FakeKernel:
            def accounting(self,*args):raise NativeFailure('QUERY_UNKNOWN')
            def TerminateJobObject(self,*args):return False
            def WaitForSingleObject(self,*args):return 0xffffffff
            def CloseHandle(self,handle):closed.append(handle);return True
        evidence={};failures=cleanup_owned(FakeKernel(),process,77,assigned=True,evidence=evidence)
        self.assertEqual(closed,[101,102,77]);self.assertTrue(evidence['cleanup_failed'])
        self.assertFalse(evidence['cleanup_job_termination_issued']);self.assertFalse(evidence['cleanup_runner_reaped'])
        self.assertIn('terminate_owned_job',{row['stage'] for row in failures})
        self.assertIn('reap_owned_runner',{row['stage'] for row in failures})
        self.assertLessEqual(len(failures),16)


@unittest.skipUnless(os.name=='nt','Windows owned Job native final closure')
class WindowsNativeClosure(unittest.TestCase):
    def test_actual_midrun_close_refusal_retains_handle_for_owned_cleanup(self):
        for selected in ('thread','process'):
            with self.subTest(selected=selected),tempfile.TemporaryDirectory() as directory:
                instances=[]
                class FaultKernel(Kernel):
                    def __init__(self):
                        super().__init__();instances.append(self);self.targets={};self.refused=False;self.close_calls=[]
                        create=self.CreateProcessW;close=self.CloseHandle
                        def created(*args):
                            result=create(*args)
                            if result:
                                process=args[-1]._obj;self.targets={'thread':process.thread,'process':process.process}
                            return result
                        def closed(handle):
                            self.close_calls.append(handle)
                            if handle==self.targets.get(selected) and not self.refused:
                                self.refused=True;return False
                            return close(handle)
                        self.CreateProcessW=created;self.CloseHandle=closed
                with patch('eq142_native.Kernel',FaultKernel):
                    with self.assertRaises(NativeFailure) as caught:
                        run_owned([sys.executable,str(ROOT/'tests/eq142_native_child.py'),
                            '--output',str(Path(directory)/'threads.json')],cwd=ROOT,timeout_s=10)
                self.assertEqual(caught.exception.code,'CLOSE_RUNNER_'+selected.upper())
                self.assertNotIn('cleanup_failed',caught.exception.evidence)
                self.assertGreaterEqual(instances[0].close_calls.count(instances[0].targets[selected]),2)
                if 'cleanup_accounting' in caught.exception.evidence:
                    self.assertEqual(caught.exception.evidence['cleanup_accounting']['active_processes'],0)

    def test_actual_cleanup_only_reported_close_failure_cannot_return_success(self):
        instances=[]
        class FaultKernel(Kernel):
            def __init__(self):
                super().__init__();instances.append(self);self.job=None;self.safely_closed=False
                create=self.CreateJobObjectW;close=self.CloseHandle
                def created(*args):self.job=create(*args);return self.job
                def closed(handle):
                    value=close(handle)
                    if handle==self.job:
                        # Controlled fault reports failure only AFTER real safe
                        # close, so this qualification injection leaves no Job.
                        self.safely_closed=bool(value);return False
                    return value
                self.CreateJobObjectW=created;self.CloseHandle=closed
        with patch('eq142_native.Kernel',FaultKernel):
            with self.assertRaises(NativeFailure) as caught:run_owned([sys.executable,'-c','pass'],cwd=ROOT,timeout_s=10)
        self.assertTrue(instances[0].safely_closed)
        self.assertEqual(caught.exception.code,'OWNED_CLEANUP_FAILED')
        self.assertEqual(caught.exception.evidence['final_accounting']['active_processes'],0)
        self.assertTrue(caught.exception.evidence['cleanup_failed'])
        self.assertEqual(caught.exception.evidence['cleanup_failures'][0]['stage'],'close_owned_job')

    def test_actual_runner_identity_clock_final_exit_and_witness(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'witness.json'
            native=run_owned([sys.executable,str(ROOT/'tests/eq142_identity_child.py'),'--job','{job_name}',
                '--output',str(output)],cwd=ROOT,timeout_s=10)
            record=json.loads(output.read_bytes())
            result=validate_native_closure(native,[record],parent_nonce=record['process_startup_nonce'])
        self.assertEqual(result['initial_runner']['pid'],native['runner_pid'])
        self.assertEqual(result['calculation_parent']['pid'],record['pid'])
        self.assertEqual(result['calculation_parent'][CREATED],record[CREATED]);self.assertFalse(result['full_sample_accepted'])
        self.assertEqual(result['parent_is_initial_runner'],record['pid']==native['runner_pid'])
        self.assertEqual(native['native_closure']['observer']['pid'],os.getpid())
        self.assertEqual(native['probe_errors'],[])

    def test_actual_descendant_outlives_runner_and_fresh_empty_query_follows(self):
        with tempfile.TemporaryDirectory() as directory:
            release=Path(directory)/'release';seen_exit=[];released=[]
            original_wait=Kernel.runner_wait;original_threads=Kernel.threads_for_members
            def waited(kernel,handle):
                state=original_wait(kernel,handle)
                if state==0:seen_exit.append(True)
                return state
            def threads(kernel,*args,**kwargs):
                counts=original_threads(kernel,*args,**kwargs)
                proof=Path(directory)/'child.json'
                if seen_exit and not released and proof.exists():
                    body=proof.read_bytes();self.assertLessEqual(len(body),4096);child=json.loads(body)
                    disposition=kernel.last_thread_dispositions.get(child['pid'])
                    if (disposition is not None and disposition['state']=='observed_live'
                            and counts.get(child['pid'],0)>0):
                        with release.open('xb') as output:output.write(b'release\n')
                        released.append(child['pid'])
                return counts
            # Only this native lifetime fixture uses base Python so the initial
            # runner itself exits; the official baseline venv control is unchanged.
            with patch.object(Kernel,'runner_wait',waited),patch.object(Kernel,'threads_for_members',threads):
                native=run_owned([sys._base_executable,str(ROOT/'tests/eq142_native_closure_child.py'),
                    '--job','{job_name}','--directory',directory,'--release',str(release)],cwd=ROOT,timeout_s=10)
            records=[json.loads((Path(directory)/name).read_bytes()) for name in ('parent.json','child.json')]
            processes={r['process_startup_nonce']:{'pid':r['pid'],'clock':r['clock']} for r in records}
            result=validate_native_closure(native,records,parent_nonce=records[0]['process_startup_nonce'],traced_processes=processes)
        bounds=native['native_closure']['boundaries']
        self.assertGreater(int(bounds['job_empty_observed_ns']),int(bounds['runner_exit_observed_ns']))
        self.assertEqual(native['native_closure']['job_empty_accounting']['active_processes'],0)
        self.assertEqual(native['final_accounting']['active_processes'],0)
        self.assertEqual(len(result['traced_lifetimes']),2)
        self.assertGreaterEqual(native['final_accounting']['total_processes'],2)
        self.assertEqual(released,[records[1]['pid']])
        self.assertTrue(any(int(bounds['observer_start_ns'])+sample['elapsed_ns']>int(bounds['runner_exit_observed_ns'])
            and any(row['pid']==records[1]['pid'] and row['thread_disposition']['state']=='observed_live'
                for row in sample['processes']) for sample in native['samples']))

    def test_unknown_wait_and_terminal_lifetime_drift_reject_and_reap_owned_job(self):
        for fault in ('wait','lifetime'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
                terminal=set();original_wait=Kernel.runner_wait;original_created=Kernel.creation_identity
                def waited(kernel,handle):
                    if fault=='wait':raise NativeFailure('RUNNER_WAIT')
                    state=original_wait(kernel,handle)
                    if state==0:terminal.add(handle)
                    return state
                def created(kernel,handle):
                    value=original_created(kernel,handle)
                    return str(int(value)+1) if handle in terminal else value
                with patch.object(Kernel,'runner_wait',waited),patch.object(Kernel,'creation_identity',created):
                    with self.assertRaises(NativeFailure) as caught:
                        run_owned([sys.executable,str(ROOT/'tests/eq142_native_child.py'),
                            '--output',str(Path(directory)/'threads.json')],cwd=ROOT,timeout_s=10)
                self.assertEqual(caught.exception.code,'RUNNER_WAIT' if fault=='wait' else 'RUNNER_LIFETIME_RACE')
                actual=caught.exception.evidence;self.assertTrue(actual['assigned_to_owned_job'])
                self.assertNotIn('cleanup_failed',actual)
                if 'cleanup_accounting' in actual:self.assertEqual(actual['cleanup_accounting']['active_processes'],0)
                self.assertIsNone(actual['native_closure']['boundaries']['native_measurement_end_ns'])

    def test_observer_api_failure_and_signaled_259_are_not_success(self):
        with patch.object(Kernel,'observer_identity',side_effect=NativeFailure('OBSERVER_QPF_API')):
            with self.assertRaises(NativeFailure) as caught:
                run_owned([sys.executable,'-c','pass'],cwd=ROOT,timeout_s=10)
        self.assertEqual(caught.exception.code,'OBSERVER_QPF_API');self.assertNotIn('runner_pid',caught.exception.evidence)
        native=run_owned([sys.executable,'-c','raise SystemExit(259)'],cwd=ROOT,timeout_s=10)
        self.assertEqual(native['exit_code'],259);self.assertEqual(native['final_accounting']['active_processes'],0)
        self.assertEqual(native['native_closure']['runner']['terminal_wait'],0)
        with self.assertRaises(ValueError):validate_native_closure(native,[],parent_nonce='b'*32)


if __name__=='__main__':unittest.main()
