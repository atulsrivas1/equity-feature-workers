"""V9 native controller with permanent qualification or official-study purpose.

Qualification fixtures have no performance authority. Official runs require
actual completed separate review and current source/platform/installed/CI gates.
Candidate flags remain false; no optimization or default adoption occurs here.
"""
from copy import deepcopy
import os,re,sys
import time
from pathlib import Path
from eq142_graph import wire,digest,bind_source
from eq142_fixture import load,ROOT
from eq142_proofs import read_proof,decode_record
from eq142_evidence import persist_record
from eq142_capacity import CapacityProbe,admit_capacity
from eq142_matrix import build_matrix,verify_matrix
from eq142_preparation import PreparationRecorder
from eq142_namespace import EncodedJournal,FixedMember
from eq142_failure import controller_guard,retain_failure,failure_record,check_available
from eq142_study_records import verify_admission,study_summary
from eq142_study_statistics import control_summary,cohort_summary,operation_totals,summarize_operation_totals,baseline_reasons
from eq142_count_index import verify_count_index,DISPOSITION
from eq142_controller_evidence import load_selected_run
from eq142_reference_validation import validate_preparation

QUAL_CAPS={'trace':4831838208,'native':1207959552,'summary':150994944,
           'profiles':80216064,'witnesses':5013504,'returns':294912,'capacity':2359296,
           'other_runner':14155776,'reference_and_index':33554432,'failure_and_temporary_names':109051904}
TOPOLOGY=('active_processor_groups_before','process_groups_before','primary_thread_group_before',
          'primary_thread_mask_before','primary_system_mask','allowed_affinity_mask',
          'allowed_cpu_ids','allowed_cpu_domain','allowed_cpu_slots')


def same(actual,expected):
    if wire(actual)!=wire(expected):raise ValueError('workspace frozen source/context changed')


def write_ref(directory,name,codec,value,maximum):
    return {'codec':codec,**persist_record(Path(directory)/name,value,max_bytes=maximum)}


class QualificationWorkspace:
    """Own one permanent-purpose workspace; gates grant no automatic acceptance."""
    @controller_guard
    def __init__(self,directory,*,workspace_purpose='controller_qualification_only'):
        from benchmark_execution_baseline import prepare_reference,source_state
        if workspace_purpose not in ('controller_qualification_only','official_instrumentation_study'):
            raise ValueError('workspace permanent purpose')
        self._purpose=workspace_purpose;self._owns_root=False;self._failure_attempted=False
        self._failed=False;self._phase='initial_source';self._available=[]
        self._active_sample_id='qualification';self._active_directory=None
        self._outcome='not_executed';self._capacity_prefixes={}
        self.admission_ref=None;self.admission=None;self._gate_bundle=None
        self.root=Path(directory).absolute()
        for path in (self.root,*self.root.parents):
            if path.is_symlink() or path.is_junction():raise ValueError('workspace indirection')
        if os.path.lexists(self.root):raise ValueError('workspace requires new exclusive root')
        self._source=source_state();self._phase='create_workspace'
        self.root.mkdir(exist_ok=False)
        self._owns_root=True
        (self.root/'qualification').mkdir(exist_ok=False)
        self._active_directory=self.root/'qualification'
        self.journal=EncodedJournal(self.root);self.paths={'study':self.root};self.children={'study':{}}
        self.qual_components={key:0 for key in QUAL_CAPS};self._qual_leaves={}
        self.journal.register('study',self.root,{})
        for name in ('qualification','regular'):
            if name!='qualification':(self.root/name).mkdir()
            self._directory('study',name)
        self._phase='capacity_freeze'
        self.probe=CapacityProbe(self.root,load('CAPACITY_RUNTIME_SOURCE_HASHES.json'))
        self.freeze=self.probe.capture('before',encoded_bytes_already_retained=0)
        admit_capacity(self.freeze,self.probe.probe_sources,degree=1,
                       expected_device=self.probe.directory_device,expected_retained_bytes=0)
        self.freeze_ref=write_ref(self.root/'qualification','qualification-capacity.json','capacity_receipt',self.freeze,16384)
        self._member('qualification','qualification-capacity.json','qualification',self.freeze_ref,'qualification','reference_and_index')
        recorder=PreparationRecorder(self.root/'reference');self._phase='reference_preparation';self._outcome='unknown'
        def capture(profile,series,products_reference,diagnostics_reference):
            recorder(profile,series,products_reference,diagnostics_reference)
            for ref in (products_reference,diagnostics_reference):
                self._remember('prep:'+profile+':'+series,ref)
        self.reference_return=prepare_reference(self.root/'reference',cpu_slots=self.freeze['allowed_cpu_slots'],preparation_recorder=capture)
        self.preparation=recorder.finish()
        self._phase='reference_readback'
        checked=validate_preparation(self.root/'reference/reference.json',self.reference_return,self.preparation)
        self.reference=checked['reference'];self.reference_ref={'codec':'eq142_development_reference',**self.reference_return}
        self._directory('study','reference');self._directory('reference','objects')
        self._member('reference','reference.json','reference',self.reference_ref,'qualification','reference_and_index')
        for entry in self.preparation:
            name=entry['profile']+'-'+entry['series'];key=self._directory('reference',name)
            for field,filename in (('products_reference','products.json'),('diagnostics_reference','diagnostics.json')):
                self._member(key,filename,'reference_preparation',entry[field],'qualification','reference_and_index')
        self.product_refs={}
        self._objects(checked['product_references'])
        self.matrix=build_matrix(self.reference,self.freeze,self.freeze_ref,
            probe_sources=self.probe.probe_sources,expected_device=self.probe.directory_device,expected_retained_bytes=0)
        self.matrix_sha256=digest(wire(self.matrix));self.checked_entries=[];self.index_ref=None
        self._source_check();self.journal.reconcile(self.children)

    @property
    def workspace_purpose(self):return self._purpose

    def _qualification_failure_context(self,phase,*,outcome='not_executed'):
        self._phase=phase;self._active_sample_id='qualification'
        self._active_directory=self.root/'qualification';self._outcome=outcome

    def _remember(self,context,reference,*,derived=False):
        ref=deepcopy(reference)
        if derived:ref['codec']='eq142_unaccepted_diagnostic'
        entry={'origin':'caller_retained_unaccepted_diagnostic' if derived else 'actual_writer_return',
               'context':context,'reference':ref}
        check_available(entry)
        # Bounded deterministic priority prefix, with no completeness claim.
        candidates={wire(e):e for e in [*self._available,entry]}
        self._available=[candidates[key] for key in sorted(candidates)[:64]]

    def _retain_failure(self,error):
        from eq142_failure_audit import audit_failure_namespace
        from supplied_native import NativeFailure
        directory=self._active_directory
        if directory is None or not Path(directory).is_relative_to(self.root):
            raise ValueError('failure has no owned fixed directory')
        audit_failure_namespace(self.root,pending_failure_allowance=2097152)
        if isinstance(error,NativeFailure) and error.evidence is not None:
            try:
                if type(error.evidence) is not dict:raise ValueError('native failure payload type')
                failed_ref={'codec':'native_receipt',**persist_record(Path(directory)/'native.json',error.evidence,max_bytes=16777216)}
                # Actual raw exception payload is not a normal run_owned return
                # and cannot satisfy any successful native/root proof.
                self._remember('sample',failed_ref,derived=True)
                self._outcome='actual_failed_report_only'
            except BaseException:
                try:BaseException.add_note(error,'Native failure payload retention was incomplete; original failure retained.')
                except BaseException:pass
        value=failure_record(error,sample_id=self._active_sample_id,stage=self._phase,
            available_references=deepcopy(self._available),original_outcome_disposition=self._outcome)
        self.failure_reference=retain_failure(directory,value)
        self.failure_storage_audit=audit_failure_namespace(self.root)

    def _directory(self,parent,name):
        if type(name) is not str or re.fullmatch('[a-z][a-z0-9_.-]{0,127}',name) is None:
            raise ValueError('workspace fixed directory name')
        key=name if parent=='study' else parent+'/'+name
        if key in self.paths:raise ValueError('workspace duplicate fixed directory')
        path=self.paths[parent]/name
        self.journal.register(key,path,{})
        self.paths[key]=path;self.children[key]={};self.children[parent][name]=key
        return key

    def _member(self,key,name,scope,reference,category,qual_component=None):
        if category=='qualification':
            if qual_component not in QUAL_CAPS:raise ValueError('qualification component category')
            identity=(key,name)
            if identity in self._qual_leaves:
                same(self._qual_leaves[identity],(qual_component,reference))
            else:
                self.qual_components[qual_component]+=reference['bytes']
                if self.qual_components[qual_component]>QUAL_CAPS[qual_component]:
                    raise ValueError('qualification fixed component encoded cap')
                self._qual_leaves[identity]=(qual_component,deepcopy(reference))
        self.journal.register(key,self.paths[key],{name:FixedMember(scope,category,reference)})
        context=scope
        if scope=='reference_preparation':
            dirname=key.removeprefix('reference/')
            for entry in self.preparation:
                if dirname==entry['profile']+'-'+entry['series']:
                    context='prep:'+entry['profile']+':'+entry['series'];break
        self._remember(context,reference,derived=reference['codec']=='eq142_owned_runner_return')

    def _objects(self,references):
        for sha,ref in references.items():
            if sha in self.product_refs:same(self.product_refs[sha],ref)
            else:
                if len(self.product_refs)>=512:raise ValueError('workspace shared product object count')
                self._member('reference/objects',ref['retained_name'],'products',ref,'shared.products')
                self.product_refs[sha]=deepcopy(ref)

    def _source_check(self):
        from benchmark_execution_baseline import source_state
        same(source_state(),self._source);same(self.reference['state'],self._source)
        bind_source(self.reference['source_inventory']).check()
        if hasattr(self,'matrix'):
            verify_matrix(self.matrix,self.reference,self.freeze,self.freeze_ref,
                probe_sources=self.probe.probe_sources,expected_device=self.probe.directory_device,
                expected_retained_bytes=0)

    def _capacity(self,key,position,degree):
        # Reconcile the actual stage prefix BEFORE writing the new receipt.
        retained=self.journal.reconcile(self.children)['total_bytes']
        record=self.probe.capture(position,encoded_bytes_already_retained=retained)
        for field in TOPOLOGY:same(record[field],self.freeze[field])
        admit_capacity(record,self.probe.probe_sources,degree=degree,
                       expected_device=self.probe.directory_device,expected_retained_bytes=retained)
        name='capacity-'+position+'.json'
        ref=write_ref(self.paths[key],name,'capacity_receipt',record,16384)
        self._capacity_prefixes[(key,position)]=(retained,deepcopy(ref))
        category='qualification' if key.startswith('qualification/') else 'regular.metadata'
        self._member(key,name,'sample',ref,category,'capacity' if category=='qualification' else None)
        return record,ref

    def _register_run(self,key,selected):
        qualification=key.startswith('qualification/')
        self._member(key,'runner-reference.json','sample',selected['wrapper_reference'],
                     'qualification' if qualification else 'regular.metadata','returns' if qualification else None)
        self._member(key,'native.json','sample',selected['native_reference'],
                     'qualification' if qualification else 'regular.native','native' if qualification else None)
        runner=self._directory(key,'runner')
        for name,ref in selected['runner_references'].items():
            component='summary' if name in ('runner.json','diagnostics.json') else 'other_runner'
            regular='regular.summary' if name=='runner.json' else ('regular.diagnostics' if name=='diagnostics.json' else 'regular.metadata')
            self._member(runner,name,'runner',ref,'qualification' if qualification else regular,component if qualification else None)
        for name,scope,refs,component in (
                ('trace','trace',selected['trace_references'],'trace'),
                ('owned-processes','owned_processes',selected['witness_references'],'witnesses'),
                ('code-counts','code_counts',selected['profile_references'],'profiles')):
            if name=='code-counts' and not qualification:continue
            child=self._directory(runner,name)
            regular='regular.trace' if name=='trace' else 'regular.metadata'
            for filename,ref in refs.items():
                self._member(child,filename,scope,ref,'qualification' if qualification else regular,component if qualification else None)
        self._objects(selected['product_references'])

    @controller_guard
    def qualify_counts(self):
        from eq142_native import run_owned
        if self.index_ref is not None or self.checked_entries:raise ValueError('count qualification cannot retry')
        self._source_check()
        for row in self.matrix['cohorts']:
            if not row['supported']:continue
            cohort=row['cohort_id'];selector=row['selector'];local=self.root/'qualification'/cohort
            self._qualification_failure_context('count_qualification')
            local.mkdir(exist_ok=False)
            self._active_directory=local;self._active_sample_id=cohort
            key=self._directory('qualification',cohort)
            self._capacity(key,'before',selector['degree'])
            self._source_check()
            command=[sys.executable,str(ROOT/'tools/eq142_owned_runner.py'),
                '--directory',str(local),'--sample-id',cohort,'--purpose','qualification_counts',
                '--reference',str(self.root/'reference/reference.json'),'--reference-sha256',self.reference_return['sha256'],
                '--profile',selector['profile'],'--mode',selector['mode'],'--degree',str(selector['degree']),
                '--control','instrumented_B','--series',selector['series'],'--job','{job_name}']
            self._phase='native_owned_call';self._outcome='unknown'
            native=run_owned(command,cwd=ROOT,timeout_s=120)
            self._phase='native_receipt'
            native_ref=write_ref(local,'native.json','native_receipt',native,16777216)
            self._remember('sample',native_ref)
            self._phase='sample_evidence_readback'
            selected=load_selected_run(self.root/'reference/reference.json',self.reference_return,
                directory=local,selector=selector,control='instrumented_B',purpose='qualification_counts',
                sample_id=cohort,native=native,native_reference=native_ref)
            self._outcome='actual_returned'
            self._register_run(key,selected)
            self._capacity(key,'after',selector['degree']);self._source_check()
            counts=selected['verified']['profiled_counts']
            self.checked_entries.append({'cohort_id':cohort,'selector':deepcopy(selector),
                'runner_return_reference':selected['wrapper_reference'],'runner_reference':selected['runner_reference'],
                'native_reference':native_ref,'profiles':list(selected['profile_references'].values()),
                'comparison':counts['comparison'],'source_invariant_disposition':DISPOSITION,'timing_authority':False})
        self._qualification_failure_context('count_qualification',outcome='unknown')
        record={'schema':'eq142-count-qualification:1','stage':'qualification_only_no_timing_authority',
            'source':bind_source(self.reference['source_inventory']).source,'matrix_sha256':self.matrix_sha256,
            'capacity_freeze_reference':self.freeze_ref,'entries':self.checked_entries,
            'all_source_invariants_passed':True,'full_sample_accepted':False}
        ref=write_ref(self.root/'qualification','count-qualification.json','eq142_count_qualification',record,1048576)
        self._member('qualification','count-qualification.json','qualification',ref,'qualification','reference_and_index')
        verify_count_index(read_proof(self.root/'qualification',ref,scope='qualification',name='count-qualification.json'),ref,
            source=record['source'],matrix_sha256=self.matrix_sha256,capacity_freeze_reference=self.freeze_ref,
            checked_entries=self.checked_entries)
        verify_matrix(self.matrix,self.reference,self.freeze,self.freeze_ref,probe_sources=self.probe.probe_sources,
                      expected_device=self.probe.directory_device,expected_retained_bytes=0)
        self.journal.reconcile(self.children);self._source_check();self.index_ref=ref
        self._active_sample_id='qualification';self._active_directory=self.root/'qualification'
        return {'count_index_reference':deepcopy(ref),'matrix_sha256':self.matrix_sha256,
                'scope':'qualification_only_no_timing_authority','full_sample_accepted':False}

    @controller_guard
    def replay_count_qualification(self):
        """Rebuild EVERY supported entry from raw current fixed proof files.

        This is for admission/final whole-history checks. A cached prior wait
        count or caller-produced index row cannot substitute for raw same-run
        profiles, trace, products, native witnesses or source invariants.
        """
        self._qualification_failure_context('count_index_replay',outcome='unknown')
        if self.index_ref is None:raise ValueError('count qualification absent')
        self._source_check();self.journal.reconcile(self.children)
        rows=[row for row in self.matrix['cohorts'] if row['supported']]
        if len(self.checked_entries)!=len(rows):raise ValueError('count qualification matrix coverage')
        rebuilt=[]
        for original,row in zip(self.checked_entries,rows):
            local=self.root/'qualification'/row['cohort_id']
            self._active_directory=local;self._active_sample_id=row['cohort_id'];self._outcome='unknown'
            native=decode_record(read_proof(local,original['native_reference'],scope='sample',name='native.json'),maximum=16777216)
            selected=load_selected_run(self.root/'reference/reference.json',self.reference_return,
                directory=local,selector=row['selector'],control='instrumented_B',purpose='qualification_counts',
                sample_id=row['cohort_id'],native=native,native_reference=original['native_reference'])
            self._outcome='actual_returned'
            entry={'cohort_id':row['cohort_id'],'selector':deepcopy(row['selector']),
                'runner_return_reference':selected['wrapper_reference'],'runner_reference':selected['runner_reference'],
                'native_reference':original['native_reference'],'profiles':list(selected['profile_references'].values()),
                'comparison':selected['verified']['profiled_counts']['comparison'],
                'source_invariant_disposition':DISPOSITION,'timing_authority':False}
            same(entry,original);rebuilt.append(entry)
            self._replay_capacity('qualification/'+row['cohort_id'],row['selector']['degree'])
        self._qualification_failure_context('count_index_replay',outcome='unknown')
        checked=verify_count_index(read_proof(self.root/'qualification',self.index_ref,scope='qualification',name='count-qualification.json'),
            self.index_ref,source=bind_source(self.reference['source_inventory']).source,
            matrix_sha256=self.matrix_sha256,capacity_freeze_reference=self.freeze_ref,checked_entries=rebuilt)
        self.journal.reconcile(self.children);self._source_check()
        return checked

    def _replay_capacity(self,key,degree):
        for position in ('before','after'):
            retained,ref=self._capacity_prefixes[(key,position)]
            record=decode_record(read_proof(self.paths[key],ref,scope='sample',name='capacity-'+position+'.json'),maximum=16384)
            for field in TOPOLOGY:same(record[field],self.freeze[field])
            admit_capacity(record,self.probe.probe_sources,degree=degree,
                expected_device=self.probe.directory_device,expected_retained_bytes=retained)

    @controller_guard
    def admit(self,gate_bundle=None):
        from eq142_gate_bundle import ReviewedGateBundle
        from eq142_study_records import RECEIPTS
        import subprocess
        if self.admission_ref is not None:raise ValueError('study admission is exclusive once')
        self._phase='study_admission';self._active_sample_id='qualification'
        self._active_directory=self.root/'qualification'
        self.replay_count_qualification();self._phase='study_admission'
        head=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],stderr=subprocess.PIPE).decode().strip()
        canonical=bind_source(self.reference['source_inventory']).source['canonical_docs_head']
        official=self._purpose=='official_instrumentation_study'
        receipts={k:None for k in RECEIPTS}
        if official:
            if type(gate_bundle) is not ReviewedGateBundle:raise ValueError('official timing requires separately reviewed actual gate bundle')
            receipts=gate_bundle.check(canonical_contract_head=canonical,worker_head=head,reference=self.reference)
            self._gate_bundle=gate_bundle
        elif gate_bundle is not None:raise ValueError('qualification fixtures cannot acquire official gate authority')
        expected={'schema':'eq142-study-admission:1','workspace_purpose':self._purpose,
            'canonical_contract_head':canonical,'worker_head':head,
            'source_state_sha256':digest(wire(self.reference['state'])),
            'source_inventory_sha256':digest(wire(self.reference['source_inventory'])),
            'protocol_sha256':digest((ROOT/'tests/fixtures/eq142_graph/BENCHMARK_PROTOCOL.json').read_bytes()),
            'precode_sha256':digest((ROOT/'tests/fixtures/eq142_graph/COMPLETE_ROOT_PRECODE.json').read_bytes()),
            'matrix_sha256':self.matrix_sha256,'expected_reference':self.reference_ref,
            'capacity_freeze_reference':self.freeze_ref,'count_qualification_reference':self.index_ref,
            'pre_timing_receipt_sha256s':receipts,'all_actual_gates_verified':official,
            'official_timing_authority':official,'full_sample_accepted':False}
        verify_admission(expected,actual_expected=expected)
        ref=write_ref(self.root/'qualification','study-admission.json','eq142_study_admission',expected,16384)
        self._member('qualification','study-admission.json','qualification',ref,'qualification','reference_and_index')
        verify_admission(decode_record(read_proof(self.root/'qualification',ref,scope='qualification',name='study-admission.json'),maximum=16384),actual_expected=expected)
        self.journal.reconcile(self.children);self._source_check()
        self.admission=deepcopy(expected);self.admission_ref=ref;self._index_admitted=True
        return deepcopy(ref)


class DevelopmentWorkspace(QualificationWorkspace):
    """Close native candidates and enforce the fixed gated official schedule.

    Ordinary native qualification controls never become performance evidence.
    """
    def _schedule(self):
        return [(control,row,kind,index)
                for control in ('uninstrumented_A','instrumented_B')
                for row in self.matrix['cohorts'] if row['supported']
                for kind,count in (('w',2),('r',7)) for index in range(count)]

    @staticmethod
    def _cpu_clock():
        clock=time.get_clock_info('process_time')
        return {'name':'process_time','implementation':clock.implementation,
                'monotonic':clock.monotonic,'adjustable':clock.adjustable,
                'reported_resolution_seconds':format(clock.resolution,'.17g'),
                'cross_process_raw_ticks_qualified':False}

    def _summary_limit(self,key,runner_reference,additional_name=None,additional_body=None):
        total=runner_reference['bytes']
        for name in ('complete-root.json','measurement.json','candidate.json'):
            path=self.paths[key]/name
            if path.exists():total+=path.stat().st_size
        if additional_name is not None:
            if (self.paths[key]/additional_name).exists():raise ValueError('duplicate compact summary')
            total+=len(additional_body)
        if total>1048576:raise ValueError('aggregate runner/root/measurement/candidate cap')

    @controller_guard
    def run_next_candidate(self):
        if self.workspace_purpose!='official_instrumentation_study':
            raise ValueError('qualification fixture requires explicit individual control')
        if self.admission_ref is None:raise ValueError('official candidate requires actual study admission')
        schedule=self._schedule();cursor=getattr(self,'_cursor',0)
        if cursor>=len(schedule):raise ValueError('candidate schedule exhausted')
        if schedule[cursor][0]=='instrumented_B' and not getattr(self,'_A_baseline_verified',False):
            raise ValueError('all A raw history and baseline gates must pass before B')
        result=self._run_candidate(schedule[cursor]);self._cursor=cursor+1
        return result

    @controller_guard
    def run_qualification_fixture(self,cohort_id,control):
        if self.workspace_purpose!='controller_qualification_only' or self.admission_ref is None:
            raise ValueError('individual correctness fixture requires permanent qualification admission')
        if control not in ('uninstrumented_A','instrumented_B'):raise ValueError('fixture fixed control')
        rows=[row for row in self.matrix['cohorts'] if row['cohort_id']==cohort_id]
        if len(rows)!=1 or not rows[0]['supported']:raise ValueError('fixture requires supported fixed cohort')
        seen=getattr(self,'_fixture_seen',set())
        if (cohort_id,control) in seen:raise ValueError('individual fixture cannot retry')
        self._fixture_seen=seen|{(cohort_id,control)}
        return self._run_candidate((control,rows[0],'w',0))

    def _run_candidate(self,scheduled):
        from eq142_native import run_owned
        from eq142_root_record import verify_root_record,LIMITATIONS
        from eq142_envelope import verify_measurement,verify_candidate,FINALIZATION
        if self.index_ref is None or self.admission_ref is None:raise ValueError('candidate requires real full count qualification and admission')
        control,row,kind,index=scheduled;selector=deepcopy(row['selector']);cohort=row['cohort_id']
        sample_id=('a' if control=='uninstrumented_A' else 'b')+'-'+cohort+'-'+kind+str(index)
        self._qualification_failure_context('create_sample')
        # Cohort policy directories precede the measured sample interval. Its
        # exclusive sample mkdir, required source/capacity checks and all owned
        # launch/root work occur within the exact published14 checkpoints.
        cohort_key='regular/'+cohort
        if cohort_key not in self.paths:
            (self.root/'regular'/cohort).mkdir(exist_ok=False);self._directory('regular',cohort)
        local=self.root/'regular'/cohort/sample_id;cpu_clock=self._cpu_clock();checkpoints={}
        def checkpoint(name):checkpoints[name]=str(time.perf_counter_ns())
        checkpoint('outer_wall_start');cpu_start=time.process_time_ns();checkpoint('outer_cpu_start_captured')
        local.mkdir(exist_ok=False)
        self._active_directory=local;self._active_sample_id=sample_id
        key=self._directory(cohort_key,sample_id)
        checkpoint('exclusive_sample_directory_created')
        self._phase='sample_source_before'
        self._source_check();source_sha=digest(wire(self._source));checkpoint('outer_source_before_verified')
        self._phase='sample_capacity_before'
        _,before_ref=self._capacity(key,'before',selector['degree']);checkpoint('capacity_before_persisted')
        command=[sys.executable,str(ROOT/'tools/eq142_owned_runner.py'),
            '--directory',str(local),'--sample-id',sample_id,'--purpose','development_candidate',
            '--reference',str(self.root/'reference/reference.json'),'--reference-sha256',self.reference_return['sha256'],
            '--profile',selector['profile'],'--mode',selector['mode'],'--degree',str(selector['degree']),
            '--control',control,'--series',selector['series'],'--job','{job_name}']
        self._phase='native_owned_call';self._outcome='unknown'
        checkpoint('native_owned_call_entered_before_Kernel_setup')
        native=run_owned(command,cwd=ROOT,timeout_s=120)
        checkpoint('native_owned_call_returned_after_descendants_exit_and_owned_cleanup')
        self._phase='native_receipt'
        native_ref=write_ref(local,'native.json','native_receipt',native,16777216)
        self._remember('sample',native_ref)
        checkpoint('native_receipt_persisted')
        self._phase='sample_evidence_readback'
        selected=load_selected_run(self.root/'reference/reference.json',self.reference_return,
            directory=local,selector=selector,control=control,purpose='development_candidate',
            sample_id=sample_id,native=native,native_reference=native_ref)
        self._outcome='actual_returned'
        self._register_run(key,selected);self.journal.reconcile(self.children)
        checkpoint('actual_runner_return_and_all_full_evidence_verified')
        references={'runner_return_reference':selected['wrapper_reference'],'runner_reference':selected['runner_reference'],
            'native_reference':native_ref,'supervisor_reference':selected['runner_references']['supervisor.json'],
            'products_reference':selected['runner_references']['products.json'],
            'publication_reference':selected['runner_references'].get('publication.json'),
            'count_qualification_reference':self.index_ref,'capacity_before_reference':before_ref,
            'expected_reference':self.reference_ref}
        root={'schema':'eq142-execution-trace:2','sample_id':sample_id,'source':selected['context']['source'],
            'selector':selector,'control':control,'acceptance_scope':'development_complete_evidence_candidate_not_official_study',
            'clock_disposition':'local_monotonic_durations_and_parent_causal_bounds',**references,
            'outer_source_before_sha256':source_sha,
            'control_trace_disposition':'untraced_original_control_no_attributed_stage_costs' if control=='uninstrumented_A' else 'complete_instrumented_local_trace',
            'complete':True,'failure_reason':None,'limitations':list(LIMITATIONS),'full_sample_accepted':False}
        self._phase='complete_root'
        self._summary_limit(key,selected['runner_reference'],'complete-root.json',wire(root))
        root_ref=write_ref(local,'complete-root.json','eq142_complete_root',root,65536)
        self._member(key,'complete-root.json','sample',root_ref,'regular.summary')
        verify_root_record(read_proof(local,root_ref,scope='sample',name='complete-root.json'),root_ref,
            sample_id=sample_id,source=selected['context']['source'],selector=selector,control=control,
            actual_references=references,source_before_sha256=source_sha)
        checkpoint('complete_root_validated_persisted_and_read_back')
        self._phase='sample_capacity_after'
        _,after_ref=self._capacity(key,'after',selector['degree']);checkpoint('capacity_after_persisted')
        self._phase='sample_source_after'
        self._source_check();checkpoint('outer_source_after_verified')
        cpu_end=time.process_time_ns();checkpoint('outer_cpu_end_captured');checkpoint('outer_wall_end')
        outer_wall=str(int(checkpoints['outer_wall_end'])-int(checkpoints['outer_wall_start']))
        phase=selected['verified']['runner']['phase'];e2e=selector['series']=='eq142_owned_runner_e2e'
        observer=native['native_closure']['observer']
        if observer['pid']!=os.getpid():raise ValueError('outer observer process binding')
        measurement={'schema':'eq142-measurement-envelope:1','sample_id':sample_id,'root_reference':root_ref,
            'selector':selector,'control':control,
            'observer':{name:observer[name] for name in ('pid','creation_filetime_100ns_since_1601_utc','clock')},
            'checkpoints':checkpoints,'outer_wall_ns':outer_wall,'outer_coordinator_cpu_ns':str(cpu_end-cpu_start),
            'selected_wall_ns':outer_wall if e2e else phase['wall_ns'],
            'selected_user_ticks_100ns':str(native['final_accounting']['user']) if e2e else phase['user_ticks_100ns'],
            'selected_kernel_ticks_100ns':str(native['final_accounting']['kernel']) if e2e else phase['kernel_ticks_100ns'],
            'capacity_after_reference':after_ref,'outer_source_after_sha256':source_sha,
            'finalization_policy':FINALIZATION,'full_sample_accepted':False,
            'cpu_observation':{'clock':cpu_clock,'start_ns':str(cpu_start),'end_ns':str(cpu_end)}}
        self._phase='measurement_envelope'
        verify_measurement(measurement,root=root,root_reference=root_ref,native=native,phase=phase,
            capacity_after_reference=after_ref,expected_source_sha256=source_sha,expected_cpu_clock=cpu_clock)
        self._summary_limit(key,selected['runner_reference'],'measurement.json',wire(measurement))
        measurement_ref=write_ref(local,'measurement.json','eq142_measurement_envelope',measurement,65536)
        self._member(key,'measurement.json','sample',measurement_ref,'regular.summary')
        verify_measurement(decode_record(read_proof(local,measurement_ref,scope='sample',name='measurement.json'),maximum=65536),
            root=root,root_reference=root_ref,native=native,phase=phase,capacity_after_reference=after_ref,
            expected_source_sha256=source_sha,expected_cpu_clock=cpu_clock)
        self._phase='final_source_check';self._source_check()  # Explicitly outside the measured envelope.
        candidate={'schema':'eq142-complete-candidate:1','sample_id':sample_id,'root_reference':root_ref,
            'measurement_reference':measurement_ref,'final_controller_post_envelope_source_state_sha256':source_sha,
            'disposition':'development_evidence_closed_not_official_study','full_sample_accepted':False}
        self._phase='candidate_finalization'
        verify_candidate(candidate,root=root,root_reference=root_ref,measurement_reference=measurement_ref,final_source_sha256=source_sha)
        self._summary_limit(key,selected['runner_reference'],'candidate.json',wire(candidate))
        candidate_ref=write_ref(local,'candidate.json','eq142_complete_candidate',candidate,4096)
        self._member(key,'candidate.json','sample',candidate_ref,'regular.summary')
        self._summary_limit(key,selected['runner_reference']);self.journal.reconcile(self.children)
        # Only compact actual writer references are retained in coordinator
        # memory. Full raw bodies must be freshly revalidated for final history.
        history=getattr(self,'_history',[])
        history.append({'key':key,'sample_id':sample_id,'selector':selector,'control':control,
            'candidate_reference':deepcopy(candidate_ref),'root_reference':deepcopy(root_ref),
            'measurement_reference':deepcopy(measurement_ref),'native_reference':deepcopy(native_ref),
            'before_reference':deepcopy(before_ref),'after_reference':deepcopy(after_ref),
            'cpu_clock':deepcopy(cpu_clock)})
        self._history=history
        return {'candidate_reference':candidate_ref,'sample_id':sample_id,
                'scope':'development_only_not_official_study','full_sample_accepted':False}

    def _check_actual_gates(self):
        import subprocess
        if self.workspace_purpose!='official_instrumentation_study' or self._gate_bundle is None:
            raise ValueError('actual official study gates absent')
        head=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],stderr=subprocess.PIPE).decode().strip()
        receipts=self._gate_bundle.check(canonical_contract_head=self.admission['canonical_contract_head'],
            worker_head=head,reference=self.reference)
        same(receipts,self.admission['pre_timing_receipt_sha256s'])
        same(head,self.admission['worker_head'])
        verify_admission(decode_record(read_proof(self.root/'qualification',self.admission_ref,
            scope='qualification',name='study-admission.json'),maximum=16384),actual_expected=self.admission)

    @controller_guard
    def replay_study_history(self,*,before_B):
        from eq142_root_record import verify_root_record
        from eq142_envelope import verify_measurement,verify_candidate
        if type(before_B) is not bool:raise ValueError('study history exact stage')
        self._phase='study_history_replay';self._active_sample_id='qualification'
        self._active_directory=self.root/'qualification'
        self._check_actual_gates();self.replay_count_qualification();self._phase='study_history_replay'
        schedule=self._schedule();expected=schedule[:len(schedule)//2] if before_B else schedule
        history=getattr(self,'_history',[])
        if len(history)!=len(expected):raise ValueError('study raw history full fixed population')
        observed={};operation_populations={};operations=load('RECORD_CONTRACTS.json')['trace']['operations']
        source_sha=digest(wire(self._source))
        for entry,(control,row,kind,index) in zip(history,expected):
            sample_id=('a' if control=='uninstrumented_A' else 'b')+'-'+row['cohort_id']+'-'+kind+str(index)
            same((entry['sample_id'],entry['selector'],entry['control']),(sample_id,row['selector'],control))
            key=entry['key'];local=self.paths[key]
            self._active_directory=local;self._active_sample_id=sample_id;self._outcome='unknown'
            native=decode_record(read_proof(local,entry['native_reference'],scope='sample',name='native.json'),maximum=16777216)
            selected=load_selected_run(self.root/'reference/reference.json',self.reference_return,
                directory=local,selector=row['selector'],control=control,purpose='development_candidate',
                sample_id=sample_id,native=native,native_reference=entry['native_reference'])
            self._outcome='actual_returned'
            refs={'runner_return_reference':selected['wrapper_reference'],'runner_reference':selected['runner_reference'],
                'native_reference':entry['native_reference'],'supervisor_reference':selected['runner_references']['supervisor.json'],
                'products_reference':selected['runner_references']['products.json'],
                'publication_reference':selected['runner_references'].get('publication.json'),
                'count_qualification_reference':self.index_ref,'capacity_before_reference':entry['before_reference'],
                'expected_reference':self.reference_ref}
            root_body=read_proof(local,entry['root_reference'],scope='sample',name='complete-root.json')
            verify_root_record(root_body,entry['root_reference'],
                sample_id=sample_id,source=selected['context']['source'],selector=row['selector'],control=control,
                actual_references=refs,source_before_sha256=source_sha)
            root=decode_record(root_body,maximum=65536)
            self._replay_capacity(key,row['selector']['degree'])
            cpu_clock=self._cpu_clock();same(cpu_clock,entry['cpu_clock'])
            measurement=decode_record(read_proof(local,entry['measurement_reference'],scope='sample',name='measurement.json'),maximum=65536)
            verify_measurement(measurement,root=root,root_reference=entry['root_reference'],native=native,
                phase=selected['verified']['runner']['phase'],capacity_after_reference=entry['after_reference'],
                expected_source_sha256=source_sha,expected_cpu_clock=cpu_clock)
            candidate=decode_record(read_proof(local,entry['candidate_reference'],scope='sample',name='candidate.json'),maximum=4096)
            verify_candidate(candidate,root=root,root_reference=entry['root_reference'],
                measurement_reference=entry['measurement_reference'],final_source_sha256=source_sha)
            projection={'sample_id':sample_id,'candidate_reference':entry['candidate_reference'],
                'wall_ns':measurement['selected_wall_ns'],'user_ticks_100ns':measurement['selected_user_ticks_100ns'],
                'kernel_ticks_100ns':measurement['selected_kernel_ticks_100ns'],
                'sampled_aggregate_RSS_bytes':native['aggregate_sampled_peak_rss_bytes']}
            observed.setdefault((row['cohort_id'],control),[]).append(projection)
            if control=='instrumented_B' and kind=='r':
                spans=[]
                for name,ref in selected['trace_references'].items():
                    fragment=decode_record(read_proof(local/'runner/trace',ref,scope='trace',name=name),maximum=16777216)
                    spans.extend(fragment['spans'])
                    if len(spans)>131072:raise ValueError('history total source span bound')
                totals=operation_totals(operations,spans)
                operation_populations.setdefault(row['cohort_id'],[]).append(totals)
            self._summary_limit(key,selected['runner_reference'])
        cohorts=[]
        for row in self.matrix['cohorts']:
            A=B=None;op=[]
            if row['supported']:
                A=control_summary(row['cohort_id'],'uninstrumented_A',observed[(row['cohort_id'],'uninstrumented_A')])
                if not before_B:
                    B=control_summary(row['cohort_id'],'instrumented_B',observed[(row['cohort_id'],'instrumented_B')])
                    op=summarize_operation_totals(operations,operation_populations[row['cohort_id']])
            cohorts.append(cohort_summary(row,A,B,op,before_B=before_B))
        self._qualification_failure_context('study_history_replay',outcome='unknown')
        self.journal.reconcile(self.children);self._source_check();self._check_actual_gates()
        return cohorts

    @controller_guard
    def _finalize_study(self,cohorts,*,before_B):
        self._phase='study_summary';self._active_sample_id='qualification'
        self._active_directory=self.root/'qualification'
        self._check_actual_gates();self._source_check();self.journal.reconcile(self.children)
        summary=study_summary(self.admission,self.admission_ref,self.matrix,cohorts,before_B=before_B)
        ref=write_ref(self.root/'qualification','study-summary.json','eq142_study_summary',summary,2097152)
        self._member('qualification','study-summary.json','qualification',ref,'qualification','reference_and_index')
        actual=decode_record(read_proof(self.root/'qualification',ref,scope='qualification',name='study-summary.json'),maximum=2097152)
        same(actual,summary);self._source_check();self._check_actual_gates();self.journal.reconcile(self.children)
        self.summary_reference=deepcopy(ref)
        return {'study_summary_reference':ref,'stage':summary['stage'],
            'attributed_cost_acceptance':summary['attributed_cost_acceptance'],
            'optimization_adoption_attempted':False,'full_sample_accepted':False}

    @controller_guard
    def run_study(self,gate_bundle):
        if self.workspace_purpose!='official_instrumentation_study':
            raise ValueError('qualification-only workspace cannot convert to official study')
        if getattr(self,'_cursor',0)!=0 or getattr(self,'_history',[]) or hasattr(self,'summary_reference'):
            raise ValueError('official study must start fresh and cannot retry')
        if self.index_ref is None:self.qualify_counts()
        if self.admission_ref is None:self.admit(gate_bundle)
        else:
            if gate_bundle is not self._gate_bundle:raise ValueError('official frozen gate bundle changed')
            self._check_actual_gates()
        schedule=self._schedule();A_count=len(schedule)//2
        for unused in range(A_count):self.run_next_candidate()
        self._phase='study_A_baseline_check'
        A_rows=self.replay_study_history(before_B=True)
        self._phase='study_A_baseline_check'
        if any(baseline_reasons(row['A']) for row in A_rows if row['supported']):
            return self._finalize_study(A_rows,before_B=True)
        self._A_baseline_verified=True
        for unused in range(A_count):self.run_next_candidate()
        return self._finalize_study(self.replay_study_history(before_B=False),before_B=False)
