"""Synthetic local event grammar, not original execution or native ownership."""
from copy import deepcopy
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from eq142_execution import verify_execution
from eq142_trace import Collector,OPERATIONS
from eq142_graph import wire
from eq142_retained_trace import combine_retained_fragments

TASK='a'*64
FEATURES=('intraday.atr','intraday.band_lower','intraday.band_middle','intraday.band_position',
    'intraday.band_upper','intraday.band_width_ratio','intraday.ema','intraday.rsi')


def fixture(mode='thread',*,publishing=True,waits=1,extra=None,unavailable=False,foreign=False,unlabelled=False,
        move_validation=False,publication_in_child=False,early_validation=False,early_publication=False,flat_publication=False):
    start=time.perf_counter_ns()
    reservation={'id':'delegate:000','file':'delegate-000.json','task_hashes':[TASK],
        'max_spans':128,'max_bytes':65536}
    plan={'mode':mode,'reservations':[reservation]}
    parent=Collector('parent',max_spans=16384,max_bytes=8388608,operations=OPERATIONS)
    child=Collector('delegate:000',max_spans=128,max_bytes=65536,operations=OPERATIONS)
    parent.default_scope='stage:supervisor';child.default_scope='stage:delegate'
    def call(collector,operation):
        frame=collector.begin(operation,());collector.finish(frame,result=None)
    def publication(collector):
        call(collector,'publisher.inner_submit')
        if flat_publication:
            for operation in ('publisher.inner_drain','sdk.publish','sink.commit'):call(collector,operation)
        else:
            drain=collector.begin('publisher.inner_drain',());publish=collector.begin('sdk.publish',())
            call(collector,'sink.commit');collector.finish(publish,result=None);collector.finish(drain,result=None)
    for operation in ('trace.bind_labels','trace.install','supervisor.partition'):call(parent,operation)
    if early_validation:
        parent.local.current_label=('task:'+TASK,TASK);call(parent,'supervisor.validate_results')
        parent.local.current_label=('stage:supervisor',None)
    rows=[];shutdowns=[]
    def causal(kind,operation):
        a=time.perf_counter_ns();call(parent,operation);b=time.perf_counter_ns()
        rows.append({'id':'causal:%04d'%len(rows),'kind':kind,
            'parent_timeline_id':parent.snapshot()['timeline_bindings'][0]['id'],
            'invocation_id':'delegate:000','task_hashes':[TASK],
            'parent_start_ns':str(a),'parent_end_ns':str(b),'child_timeline_ids':[],
            'cross_process_interval_subtraction':False,'status':'returned'})
    if mode!='sequential':causal('dispatch','pool.'+mode+'_submit')
    call(child,'trace.bind_labels')
    if mode=='process':call(child,'trace.install')
    frame=child.begin('delegate.original_partition',())
    child.local.current_label=('stage:delegate',None) if unlabelled else ('task:'+('b'*64 if foreign else TASK),'b'*64 if foreign else TASK)
    for operation in ('supplied.admit','worker.intraday_context'):call(child,operation)
    prepared=child.begin('intraday.prepare',())
    for _ in range(3):call(child,'intraday.prepare_operand')
    child.finish(prepared,result=None)
    computed=child.begin('intraday.compute',())
    for _ in range(0 if unavailable else 3):call(child,'intraday.indicator')
    call(child,'intraday.band_values');child.finish(computed,result=None)
    for operation in ('supplied.result_identity','supervisor.validate_results'):call(child,operation)
    if extra is not None:call(child,extra)
    if move_validation:call(child,'supervisor.validate_results')
    if publication_in_child and publishing:publication(child)
    child.finish(frame,result=None)
    if mode=='process':call(child,'trace.restore')
    child_record=child.snapshot()
    if mode!='sequential':
        for _ in range(waits):causal('parent_wait','parent.wait')
        causal('result_join','future.result')
        a=time.perf_counter_ns();call(parent,'pool.'+mode+'_shutdown');b=time.perf_counter_ns()
        shutdowns=[{'operation':'pool.'+mode+'_shutdown','executor_id':'owned-executor:0','wait':True,
            'cancel_futures':True,'parent_timeline_id':rows[0]['parent_timeline_id'],
            'parent_start_ns':str(a),'parent_end_ns':str(b),'status':'returned','error_type':None}]
    parent.local.current_label=('task:'+TASK,TASK)
    if publishing and early_publication and not publication_in_child:publication(parent)
    if not move_validation and not early_validation:call(parent,'supervisor.validate_results')
    if publishing and not early_publication and not publication_in_child:publication(parent)
    call(parent,'trace.restore')
    terminal=[]
    if mode!='sequential':
        a=time.perf_counter_ns();b=time.perf_counter_ns()
        terminal=[{'invocation_id':'delegate:000','task_hashes':[TASK],
            'parent_timeline_id':rows[0]['parent_timeline_id'],'capture_start_ns':str(a),
            'capture_end_ns':str(b),'done':True,'cancelled':False,'running':False}]
    for row in rows:row['child_timeline_ids']=[t['id'] for t in child_record['timeline_bindings']]
    bodies={'parent.json':wire(parent.snapshot()),'delegate-000.json':wire(child_record)}
    reservations=[{'id':'parent','file':'parent.json','max_spans':16384,'max_bytes':8388608},reservation]
    joined=combine_retained_fragments(bodies,reservations)
    report={'retained_spans':joined,'parent_relations':rows,'executor_shutdowns':shutdowns,
        'future_terminal_observations':terminal,'executor_observation_attempted':True,
        'executor_terminal_observation_complete':True,'runner_control_phase_start_ns':str(start),
        'runner_control_phase_end_ns':str(time.perf_counter_ns())}
    mathematics={TASK:{'selected_headers':list(FEATURES),'cells':{feature:{'status':
        'incomplete_coverage' if unavailable and not feature.startswith('intraday.band_') else 'available'} for feature in FEATURES}}}
    return {'report':report,'plan':plan,'bodies':bodies,'task_features':{TASK:FEATURES},
        'mathematics':mathematics,'publishing':publishing,'maximum_in_flight':1}


class PortableExecution(unittest.TestCase):
    def check(self,data):
        return verify_execution(data['report'],data['plan'],data['bodies'],
            **{k:v for k,v in data.items() if k not in ('report','plan','bodies')})

    def test_three_source_shapes_header_readiness_and_variable_waits_without_io(self):
        for mode in ('sequential','thread','process'):
            for publishing in (False,True):
                data=fixture(mode,publishing=publishing)
                with patch.object(Path,'open',side_effect=AssertionError('pure proof validation')):result=self.check(data)
                fixed=result['fixed_source_counts']
                self.assertEqual({k:fixed[k] for k in ('supervisor.partition','supervisor.validate_results',
                    'intraday.prepare_operand','intraday.indicator','intraday.band_values')},
                    {'supervisor.partition':1,'supervisor.validate_results':2,
                     'intraday.prepare_operand':3,'intraday.indicator':3,'intraday.band_values':1})
                self.assertFalse(result['full_sample_accepted'])
        self.assertEqual(self.check(fixture(waits=2))['actual_wait_groups'],2)
        self.assertEqual(self.check(fixture(unavailable=True))['fixed_source_counts']['intraday.indicator'],0)

    def test_coherent_extra_source_calls_and_wrong_task_or_missing_labels_reject(self):
        for options in ({'extra':'intraday.indicator'},{'extra':'supervisor.inputs'},
                {'extra':'publisher.submit'},{'foreign':True},{'unlabelled':True}):
            with self.subTest(options=options),self.assertRaises(ValueError):self.check(fixture(**options))

    def test_only_source_qualified_frozen_header_shapes_and_readiness_are_supported(self):
        baseline=fixture()
        for operation in ('single_band','reordered','unknown_status','boolean_status','missing_cell','degree3'):
            data=deepcopy(baseline)
            if operation in ('single_band','reordered'):
                selected=('intraday.band_position',) if operation=='single_band' else tuple(reversed(FEATURES))
                data['task_features'][TASK]=selected
                data['mathematics'][TASK]['selected_headers']=list(selected)
                data['mathematics'][TASK]['cells']={f:{'status':'available'} for f in selected}
            elif operation=='unknown_status':data['mathematics'][TASK]['cells'][FEATURES[0]]['status']='invented'
            elif operation=='boolean_status':data['mathematics'][TASK]['cells'][FEATURES[0]]['status']=True
            elif operation=='missing_cell':data['mathematics'][TASK]['cells'].pop(FEATURES[0])
            else:data['maximum_in_flight']=3
            with self.subTest(operation=operation),self.assertRaises(ValueError):self.check(data)

    def test_raw_counter_replay_and_causal_population_cannot_use_cached_labels(self):
        baseline=fixture()
        for operation in ('cached_bool','missing_relation','duplicate_relation','foreign_invocation',
                'foreign_child_timeline','cross_process','reversed','outside_hook','wait_members','unknown_field'):
            data=deepcopy(baseline);report=data['report'];rows=report['parent_relations']
            if operation=='cached_bool':
                row=next(c for c in report['retained_spans']['counters'] if c['attempts']==1)
                row['attempts']=True
            elif operation=='missing_relation':rows.pop(1)
            elif operation=='duplicate_relation':rows.insert(1,deepcopy(rows[0]))
            elif operation=='foreign_invocation':rows[0]['invocation_id']='delegate:999'
            elif operation=='foreign_child_timeline':rows[0]['child_timeline_ids']=[]
            elif operation=='cross_process':rows[0]['cross_process_interval_subtraction']=True
            elif operation=='reversed':rows.reverse()
            elif operation=='outside_hook':rows[0]['parent_end_ns']=rows[0]['parent_start_ns']
            elif operation=='wait_members':rows[1]['task_hashes']=[]
            else:rows[0]['extra']=1
            with self.subTest(operation=operation),self.assertRaises(ValueError):self.check(data)

    def test_waited_shutdown_and_manual_terminal_state_are_separate_mandatory_proofs(self):
        baseline=fixture()
        for operation in ('shutdown_wait','shutdown_cancel','shutdown_missing','shutdown_count',
                'terminal_running','terminal_cancelled','terminal_missing','terminal_time','terminal_incomplete','in_flight_bool'):
            data=deepcopy(baseline);report=data['report']
            if operation=='shutdown_wait':report['executor_shutdowns'][0]['wait']=False
            elif operation=='shutdown_cancel':report['executor_shutdowns'][0]['cancel_futures']=1
            elif operation=='shutdown_missing':report['executor_shutdowns']=[]
            elif operation=='shutdown_count':report['executor_shutdowns']*=2
            elif operation=='terminal_running':report['future_terminal_observations'][0]['running']=True
            elif operation=='terminal_cancelled':report['future_terminal_observations'][0]['cancelled']=True
            elif operation=='terminal_missing':report['future_terminal_observations']=[]
            elif operation=='terminal_time':report['future_terminal_observations'][0]['capture_start_ns']='0'
            elif operation=='terminal_incomplete':report['executor_terminal_observation_complete']=False
            else:data['maximum_in_flight']=True
            with self.subTest(operation=operation),self.assertRaises(ValueError):self.check(data)

    def test_global_totals_do_not_substitute_for_parent_and_delegate_operation_ownership(self):
        for mode in ('sequential','thread','process'):
            for operation in ('move_validation','publication_in_child'):
                data=fixture(mode,**{operation:True})
                counters=data['report']['retained_spans']['counters']
                # Coherent raw spans/counters keep the expected total of two.
                self.assertEqual(sum(c['attempts'] for c in counters if c['operation']=='supervisor.validate_results'),2)
                self.assertEqual(sum(c['attempts'] for c in counters if c['operation']=='sdk.publish'),1)
                with self.subTest(mode=mode,operation=operation),self.assertRaisesRegex(ValueError,'fragment role'):
                    self.check(data)

    def test_parent_validation_and_publication_require_original_barriers_and_nesting(self):
        for mode in ('sequential','thread','process'):
            for operation in ('early_validation','early_publication','flat_publication'):
                data=fixture(mode,**{operation:True})
                with self.subTest(mode=mode,operation=operation),self.assertRaisesRegex(ValueError,'parent-local interval|publication nesting'):
                    self.check(data)


if __name__=='__main__':unittest.main()
