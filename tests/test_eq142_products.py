"""Literal synthetic numerical expectations and coherent retained-proof attacks."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
import equity_feature_contracts as c
from equity_feature_workers import (WorkItem,BoundedSupervisor,ProgressRecorder,
    ResourceBudget,SerialPublisher,PublicationLimits)
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import descriptor,decode_result,encode_result
from manifest_fixture import fixture
from supplied_memory import SerializedMemorySink
from eq142_admission import original_reservation
from eq142_evidence import ProductStore,record_products,stable_products
from eq142_outcome import snapshot_original
from eq142_graph import digest
from eq142_products import verify_products,decode_diagnostics,RetainedProducts

CALLS=[]
FEATURES=('session.bar.close_weighted_price','session.price.overnight_gap')


def selected():
    task,result,_=fixture()
    result=replace(result,values=tuple(v for v in result.values if v.feature_id in FEATURES),
        quality=tuple(q for q in result.quality if q.feature_id in FEATURES))
    task=replace(task,features=descriptor(result).features)
    # Independent literal input; metadata is the original fixture's binding.
    facts=(('instrument_id',('A','A')),('session_id',('S','S')),
        ('start_ns',(100,150)),('end_ns',(150,200)),('known_at_ns',(150,210)),
        ('open',(100,102)),('high',(103,104)),('low',(99,101)),('close',(102,103)),
        ('volume',(200,300)),('actual_notional',(20300,30900)))
    batch=c.CanonicalBatch(c.DataKind.BAR,tuple(c.Column(n,v) for n,v in facts),task.inputs[0].binding.metadata)
    return task,result,batch


def synthetic_calculate(task,batches):
    CALLS.append(task.task_sha256)
    return (selected()[1],)


def reference(body,codec):
    sha=digest(body)
    return {'codec':codec,'sha256':sha,'bytes':len(body),'retained_name':sha+'.json'}


class PortableProducts(unittest.TestCase):
    def fixture(self,*,publishing=False):
        task,_,batch=selected();items=(WorkItem(task,(batch,),synthetic_calculate),)
        budget=ResourceBudget(workers=1,max_cpu_slots=2,max_resident_bytes=134217728)
        sink=SerializedMemorySink() if publishing else None
        pub=SerialPublisher(sink,task.destination_scope,limits=PublicationLimits(max_pending_tasks=16,
            max_pending_bytes=8388608,max_receipt_wire_bytes=400000),requirements=SinkRequirements(
                max_results=1,max_chunk_bytes=1048576,max_total_bytes=1048576,max_result_cells=100,max_evidence_rows=1)) if publishing else None
        preflight=original_reservation(items,mode='sequential',budget=budget,pending_bytes=8388608 if publishing else 0)
        progress=ProgressRecorder(max_tasks=16)
        outcome=BoundedSupervisor(mode='sequential',budget=budget).run(items,publisher=pub,progress=progress)
        self.assertEqual([t.reason for t in outcome.tasks],[None])
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);store=ProductStore(root/'objects')
            records=record_products(outcome,progress.snapshot(),store,root,sink=sink)
            bodies={sha:(store.root/(sha+'.json')).read_bytes() for sha in store.inventory}
            diagnostic_body=(root/'diagnostics.json').read_bytes()
        # Hand-specified values/coverage, never copied from calculator output.
        mathematics={task.task_sha256:{'selected_headers':list(FEATURES),'evidence':[],
            'cells':{FEATURES[0]:{'value':102.6,'status':'available','expected':2,'observed':2,'reasons':[]},
                FEATURES[1]:{'value':None,'status':'missing_input','expected':2,'observed':2,'reasons':['absent_input']}}}}
        return {'records':records,'bodies':bodies,'diagnostic_body':diagnostic_body,'items':items,
            'mathematics':mathematics,'reference_products':deepcopy(stable_products(records)),
            'summary':snapshot_original(outcome),'preflight':preflight,'mode':'sequential','publishing':publishing}

    def check(self,data):
        return verify_products(data['records'],data['bodies'],data['diagnostic_body'],
            **{k:v for k,v in data.items() if k not in ('records','bodies','diagnostic_body')})

    def change_diagnostics(self,data,change):
        record=json.loads(data['diagnostic_body']);change(record)
        body=json.dumps(record,ensure_ascii=True,sort_keys=True,separators=(',',':'),allow_nan=False).encode('ascii')
        data['diagnostic_body']=body
        for product in data['records']:
            product['raw_diagnostics']={**reference(body,'diagnostic_snapshot'),'retained_name':'diagnostics.json'}

    def test_actual_original_full_codecs_math_readback_and_admission_without_io_or_callbacks(self):
        for publishing in (False,True):
            data=self.fixture(publishing=publishing);calls=list(CALLS)
            with patch.object(Path,'open',side_effect=AssertionError('pure checker must not read')):
                checked=self.check(data)
            self.assertEqual(CALLS,calls)
            self.assertFalse(checked['full_sample_accepted'])
            self.assertFalse(checked['admission']['full_sample_accepted'])
            self.assertEqual(checked['facts']['tasks'][0]['task'],data['items'][0].task)
            self.assertEqual(checked['admission']['accepted_output_metadata_bytes'],
                data['records'][0]['output']['bytes'] if publishing else 0)

    def test_rehashed_numerical_quality_and_entity_tampering_is_not_hash_parity(self):
        baseline=self.fixture()
        result=decode_result(baseline['bodies'][baseline['records'][0]['results'][0]['sha256']])
        changes=(replace(result,values=(replace(result.values[0],values=(102.7,)),result.values[1])),
            replace(result,quality=(replace(result.quality[0],expected=3,observed=3),result.quality[1])),
            replace(result,values=tuple(replace(v,entities=(c.EntityKey('FOREIGN','S'),)) for v in result.values),
                quality=tuple(replace(q,entity=c.EntityKey('FOREIGN','S')) for q in result.quality)))
        for changed in changes:
            data=deepcopy(baseline);body=encode_result(changed);ref=reference(body,'encode_result')
            old=data['records'][0]['results'][0]['sha256'];data['bodies'].pop(old);data['bodies'][ref['sha256']]=body
            data['records'][0]['results']=[ref];data['records'][0]['disposition']['result_wire']=[ref]
            data['summary']['result_bytes']=len(body)
            # Even a coherently relabelled baseline cannot replace the oracle.
            data['reference_products']=stable_products(data['records'])
            with self.subTest(changed=changed),self.assertRaises(ValueError):self.check(data)

    def test_bounded_bodies_codec_confusion_missing_extra_and_noncanonical_sdk_wire(self):
        baseline=self.fixture(publishing=True)
        for operation in ('missing','extra','mutable','codec','length','unknown_field','noncanonical'):
            data=deepcopy(baseline);ref=data['records'][0]['task_wire'];sha=ref['sha256']
            if operation=='missing':data['bodies'].pop(sha)
            elif operation=='extra':data['bodies'][digest(b'{}')]=b'{}'
            elif operation=='mutable':data['bodies'][sha]=bytearray(data['bodies'][sha])
            elif operation=='codec':ref['codec']='encode_result'
            elif operation=='length':ref['bytes']=True
            elif operation=='unknown_field':data['records'][0]['extra']=1
            else:
                body=data['bodies'].pop(sha)+b' ';ref=reference(body,'encode_task')
                data['bodies'][ref['sha256']]=body;data['records'][0]['task_wire']=ref
            with self.subTest(operation=operation),self.assertRaises(ValueError):self.check(data)
        class ForeignDict(dict):pass
        with self.assertRaises(ValueError):RetainedProducts(ForeignDict())
        with self.assertRaises(ValueError):RetainedProducts({'x':b'{}'})
        with self.assertRaises(ValueError):RetainedProducts({digest(b'x'*1048577):b'x'*1048577})

    def test_full_diagnostic_codec_duplicates_types_identity_status_and_timing_joins(self):
        baseline=self.fixture(publishing=True);body=baseline['diagnostic_body']
        with self.assertRaises(ValueError):decode_diagnostics(body.replace(b'{',b'{"clock":"unavailable",',1))
        for field,value in (('attempt',True),('intent_sha256','0'*64),('family_sha256','0'*64),
                ('partition_sha256','0'*64),('status','computed'),('elapsed_ns',True),('extra',0)):
            data=deepcopy(baseline)
            self.change_diagnostics(data,lambda r:r['tasks'][0].__setitem__(field,value))
            with self.subTest(field=field),self.assertRaises(ValueError):self.check(data)
        data=deepcopy(baseline)
        self.change_diagnostics(data,lambda r:r['tasks'][0]['timings'][-1].__setitem__('probes',1))
        with self.assertRaises(ValueError):self.check(data)
        data=deepcopy(baseline);data['records'][0]['diagnostics'][0]['status']='computed'
        with self.assertRaises(ValueError):self.check(data)

    def test_source_impossible_diagnostic_prefix_and_success_omission_reject_after_rehash(self):
        for publishing in (False,True):
            baseline=self.fixture(publishing=publishing)
            self.check(baseline)
            operations=['extra_factory','omit_execution_serialization','execution_probe']
            if publishing:operations+=['missing_prefix','reordered_prefix','queue_probe_count','extra_backend_probe']
            for operation in operations:
                data=deepcopy(baseline)
                def change(record):
                    timings=record['tasks'][0]['timings']
                    if operation=='extra_factory':
                        timings.insert(0,{'stage':'factory','elapsed_ns':0,'probes':0})
                    elif operation=='omit_execution_serialization':
                        timings.pop();data['summary']['tasks'][0]['timings'].pop()
                    elif operation=='execution_probe':
                        timings[-1]['probes']=1;data['summary']['tasks'][0]['timings'][-1]['probes']=1
                    elif operation=='missing_prefix':timings.pop(0)
                    elif operation=='reordered_prefix':timings[0],timings[1]=timings[1],timings[0]
                    elif operation=='queue_probe_count':timings[1]['probes']=0
                    else:timings.insert(4,{'stage':'backend_probe','elapsed_ns':0,'probes':1})
                self.change_diagnostics(data,change)
                # Every raw reference is correctly rehashed; stable projection
                # and pinned reference products still agree. Source shape must
                # reject this independently of hashes or numerical parity.
                self.assertEqual(stable_products(data['records']),data['reference_products'])
                with self.subTest(publishing=publishing,operation=operation),self.assertRaises(ValueError):
                    self.check(data)

    def test_original_partial_outcomes_ledger_and_pinned_reference_are_required(self):
        baseline=self.fixture(publishing=True)
        for operation in ('cancelled','reason','result_count','row_ledger','reserved','result_bytes',
                'lookup','readback','disposition','receipt','reference'):
            data=deepcopy(baseline);row=data['records'][0]
            if operation=='cancelled':data['summary']['cancelled']=True
            elif operation=='reason':data['summary']['tasks'][0]['reason']='CALCULATION_FAILED'
            elif operation=='result_count':data['summary']['tasks'][0]['result_count']=None
            elif operation=='row_ledger':data['summary']['row_ledger'][0][1]+=1
            elif operation=='reserved':data['summary']['reserved_resident_bytes']+=1
            elif operation=='result_bytes':data['summary']['result_bytes']+=1
            elif operation=='lookup':row['committed_lookup']='original_failure'
            elif operation=='readback':row['readback_results']=[]
            elif operation=='disposition':row['disposition']['commit_state']='unknown'
            elif operation=='receipt':row['receipt']=None
            else:data['reference_products']=[]
            with self.subTest(operation=operation),self.assertRaises(ValueError):self.check(data)


if __name__=='__main__':unittest.main()
