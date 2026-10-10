from pathlib import Path
import copy
import hashlib
import json
import struct
import sys
import unittest

ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'packages/client/src'))
from equity_feature_client import DatasetKey, ProducerExpectation, RawExpectation, ScopeKey, FeatureRef, RawSliceView, ResultView, FeatureSliceView
from equity_feature_client import _wire
from equity_feature_client.native import convert_raw, convert_result, _cell
from equity_feature_io_sdk import encode_result
from test_client import producer, envelope, FIXTURES


class Native(unittest.TestCase):
    def test_raw5_complete_metadata_exact_values_and_ns(self):
        for case in FIXTURES['raw_cases']:
            payload=case['expected_raw_payload'];scope=payload['scope']
            expected=RawExpectation(DatasetKey(**payload['dataset']),ScopeKey(scope['instrument_id'],scope['session_id'],int(scope['start_ns']),int(scope['end_ns'])),case['data_kind'],tuple(c['name'] for c in payload['columns']))
            outcome=convert_raw(RawSliceView(_wire.canonical(envelope('slice',payload),limit=262144)),expected)
            self.assertTrue(outcome.ok,outcome.failure)
            for name,values in case['native_values'].items():self.assertEqual(outcome.view.column(name).values,tuple(values))
            self.assertEqual(outcome.view.metadata.source.input_id,payload['dataset']['input_id'])
            self.assertEqual(outcome.view.metadata.scope.start_ns,expected.scope.start_ns)
            if case['adjacent_1ns']:self.assertEqual(outcome.view.column('event_ns').values,(9000000000000000110,9000000000000000111,9000000000000000112))

    def test_native4_full_independent_bytes(self):
        for case in FIXTURES['native_cases']:
            payload,expected=producer(case);view=ResultView(_wire.canonical(envelope('result',payload),limit=262144))
            outcome=convert_result(view,expected);self.assertTrue(outcome.ok,outcome.failure)
            encoded=encode_result(outcome.view)
            self.assertEqual(encoded,_wire.canonical(case['native_result'],limit=1048576))
            self.assertEqual(hashlib.sha256(encoded).hexdigest(),case['native_sha256'])
            self.assertEqual(len(outcome.view.quality),len(payload['quality']));self.assertEqual(len(outcome.view.evidence),len(payload['evidence']))

    def test_missing_required_columns_and_opaque_units_denied(self):
        payload=copy.deepcopy(FIXTURES['raw_cases'][0]['expected_raw_payload']);dataset=DatasetKey(**payload['dataset']);scope=ScopeKey('A','S',100,200)
        payload['columns']=[c for c in payload['columns'] if c['name']!='instrument_id']
        expected=RawExpectation(dataset,scope,'trade',tuple(c['name'] for c in payload['columns']))
        self.assertEqual(convert_raw(RawSliceView(_wire.canonical(envelope('slice',payload),limit=262144)),expected).failure.code,'invalid_conversion')
        original=copy.deepcopy(FIXTURES['raw_cases'][0]['expected_raw_payload']);original['columns'][0]['unit']='caller-unit'
        expected=RawExpectation(dataset,scope,'trade',tuple(c['name'] for c in original['columns']))
        self.assertEqual(convert_raw(RawSliceView(_wire.canonical(envelope('slice',original),limit=262144)),expected).failure.code,'invalid_conversion')

    def test_nonzero_decimal_scale_unknown_record_and_wrong_metadata_denied(self):
        payload,expected=producer(FIXTURES['native_cases'][0])
        for change in ('scale','record','missingfield'):
            changed=copy.deepcopy(payload)
            if change=='scale':next(c for c in changed['columns'] if c['dtype']=='decimal128')['values'][0]['scale']=1
            elif change=='record':changed['metadata']['name']='CallerClass'
            else:del changed['metadata']['fields']['evidence_limit']
            try:view=ResultView(_wire.canonical(envelope('result',changed),limit=262144))
            except ValueError:continue  # Closed transport can reject before conversion.
            self.assertEqual(convert_result(view,expected).failure.code,'invalid_conversion')
        with self.assertRaises(ValueError):_cell({'type':'record','name':'CallerClass','fields':{}})
        with self.assertRaises(ValueError):_cell({'type':'decimal128','coefficient':'1','scale':1})
        self.assertEqual(struct.pack('>d',_cell({'type':'float64','bits':'8000000000000000'})).hex(),'8000000000000000')


if __name__=='__main__':unittest.main()
