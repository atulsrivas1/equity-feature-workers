"""Owned literal decisions through the actual packaged WSGI boundary."""
from dataclasses import replace
import hashlib
import io
import http.client
import json
from pathlib import Path
import secrets
import threading
import unittest

from equity_feature_contracts import (
    CanonicalBatch, Column, DataKind, BatchMetadata, SourceBinding, Coverage,
    PriceUnit, InputScope, ConfigSpec, EntityKey,
    ContractError, IntervalSpec, IntervalCoverage,
)
from equity_features.session.trades import compute_trades
from equity_features.session.bars import compute_structure
from equity_feature_io_contracts.publication import SinkError, SinkErrorCode
from equity_feature_service import Credential, Grant, Ledger, Limits, Service, Scope, RawRead, RawDataset, FeatureDataset, DatasetIdentity
from equity_feature_service import codec
from equity_feature_service.loopback import qualification_server

ROOT = Path(__file__).resolve().parents[2]
START = 9007199254740992
END = START + 2


class Clock:
    def __init__(self): self.n = 100
    def __call__(self): return self.n


def raw_batch(rows=2):
    values = {
        'instrument_id': ('owned:ONE',) * rows, 'session_id': ('session-1',) * rows,
        'event_ns': tuple(START + (n % 2) for n in range(rows)), 'order_key': tuple(range(rows)),
        'event_id': tuple('event-' + str(n) for n in range(rows)), 'eligible': (True,) * rows,
        'price': tuple(10100 if n % 2 == 0 else 10200 for n in range(rows)),
        'size': tuple(2 if n % 2 == 0 else 3 for n in range(rows)),
        'known_at_ns': tuple(None if n % 2 == 0 else 0 for n in range(rows)),
    }
    return CanonicalBatch(DataKind.TRADE, tuple(Column(k, v) for k, v in values.items()),
        BatchMetadata('owned.fixture', SourceBinding('owned', 'snapshot-1', 'mapping-1', 'input-1'),
                      Coverage(rows, rows, True), PriceUnit(2, 'USD'), scope=InputScope(START, END, 'owned-v1')))


class Source:
    def __init__(self):
        self.value = RawRead(raw_batch(), 'a' * 64)
        self.calls = 0
        self.before = None
    def read(self, cancellation=None):
        self.calls += 1
        if self.before: self.before()
        if cancellation is not None and cancellation.is_cancelled():
            raise RuntimeError('cancelled')
        return self.value


def env(value, token, **changes):
    data = value if type(value) is bytes else codec.canonical(value)
    result = {'REQUEST_METHOD': 'POST', 'PATH_INFO': '/v1/request', 'QUERY_STRING': '',
              'CONTENT_TYPE': 'application/json', 'CONTENT_LENGTH': str(len(data)),
              'HTTP_AUTHORIZATION': 'Bearer ' + token, 'REMOTE_ADDR': '127.0.0.1',
              'wsgi.multiprocess': False, 'wsgi.input': io.BytesIO(data)}
    result.update(changes)
    return result


def call(service, value, token, **changes):
    status, headers = [], []
    def start(s, h): status.append(int(s.split()[0])); headers.extend(h)
    data = b''.join(service(env(value, token, **changes), start))
    return status[0], json.loads(data) if data else None, data, dict(headers)


class ServiceVectors(unittest.TestCase):
    def setUp(self):
        self.clock, self.source = Clock(), Source()
        self.token, self.foreign = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        self.scope = Scope('owned:ONE', 'session-1', START, END)
        rights = frozenset(('raw_read', 'discover'))
        self.dataset = RawDataset('synthetic.raw', 'owned-v1', self.scope, self.source.value, self.source.read,
            columns=('event_ns', 'known_at_ns', 'price', 'size'), rights=rights,
            rights_owner='owned-fixture', rights_evidence='synthetic-owned-v1', valid_from_ns=0, expires_at_ns=1000)
        self.grant = Grant('grant-a', 'principal-a', 'synthetic.raw', 'owned-v1', 'policy-v1', self.scope,
            frozenset(self.dataset.columns), rights, 0, 1000)
        self.credentials = (Credential.provision('principal-a', self.token, 0, 1000),
                            Credential.provision('principal-b', self.foreign, 0, 1000))
        self.ledger = self.make_ledger()
        self.service = Service(self.ledger)

    def make_ledger(self, limits=None, grants=None, datasets=None):
        return Ledger(clock=self.clock, limits=limits or Limits(60, 1048576, 2097152, 60000000000),
            credentials=self.credentials, grants=grants if grants is not None else (self.grant,),
            datasets=datasets if datasets is not None else (self.dataset,), policy_revision='policy-v1', registry_snapshot=next((d.registry_snapshot for d in (datasets or ()) if isinstance(d, FeatureDataset)), 'b' * 64))

    def request(self, **changes):
        payload = {'operation': 'slice', 'dataset': self.dataset.identity.wire(), 'scope': self.scope.wire(),
                   'columns': ['event_ns', 'known_at_ns', 'price', 'size'], 'cursor': None}
        payload.update(changes)
        return {'schema': 'equity.remote', 'version': '1.0', 'kind': 'request', 'request_id': 'http-1', 'payload': payload}

    def test_literal_precision_null_and_native_metadata(self):
        status, result, data, headers = call(self.service, self.request(), self.token)
        self.assertEqual(status, 200)
        columns = {c['name']: c for c in result['payload']['columns']}
        self.assertEqual([v['value'] for v in columns['event_ns']['values']], ['9007199254740992', '9007199254740993'])
        self.assertEqual(columns['known_at_ns']['values'], [None, {'type': 'int64', 'value': '0'}])
        self.assertEqual([v['value'] for v in columns['price']['values']], ['10100', '10200'])
        self.assertEqual(result['payload']['metadata'], codec.cell(self.source.value.batch.metadata))
        self.assertEqual(int(headers['Content-Length']), len(data))
        self.assertEqual(self.source.calls, 1)

    def test_anonymous_foreign_and_bad_tokens_do_not_read(self):
        self.assertEqual(call(self.service,self.request(),self.foreign)[0],403)
        self.assertEqual(call(self.service,self.request(),'')[0],401)
        for token in ('', self.foreign, secrets.token_urlsafe(32), self.token + ',Bearer ' + self.token):
            status, result, data, _ = call(self.service, self.request(), token)
            self.assertIn(status, (401, 403))
            self.assertNotIn(self.token.encode(), data)
            self.assertNotIn(self.foreign.encode(), data)
        self.assertEqual(self.source.calls, 0)

    def test_revision_scope_columns_and_injection_denied_before_read(self):
        for change in (
            {'dataset': {**self.dataset.identity.wire(), 'revision': 'changed'}},
            {'scope': {**self.scope.wire(), 'end_ns': str(END - 1)}},
            {'scope': {**self.scope.wire(), 'start_ns': str(START - 1)}},
            {'scope': {**self.scope.wire(), 'instrument_id': "ONE' OR 1=1 --"}},
            {'columns': ['price;DROP TABLE files']}, {'columns': ['__import__']},
        ):
            self.assertEqual(call(self.service, self.request(**change), self.token)[0], 403)
        for field in ('sql', 'path', 'pickle', 'callable'):
            self.assertEqual(call(self.service, self.request(**{field: 'untrusted'}), self.token)[0], 400)
        self.assertEqual(self.source.calls, 0)

    def test_strict_json_versions_direction_and_body_limits(self):
        for data in (b'{"a":1,"a":2}', b'{"a":NaN}', b'\xff', b'\xef\xbb\xbf{}', b'[' * 33 + b'0' + b']' * 33,
                     b' ' * 16385):
            self.assertEqual(call(self.service, data, self.token)[0], 400)
        req = self.request(); req['version'] = '2.0'
        self.assertEqual(call(self.service, req, self.token)[0], 400)
        self.assertEqual(self.source.calls, 0)

    def test_scope_cursor_and_non_slice_operations(self):
        self.assertEqual(call(self.service, self.request(cursor='unimplemented'), self.token)[0], 400)
        req = {'schema': 'equity.remote', 'version': '1.0', 'kind': 'request', 'request_id': 'r',
               'payload': {'operation': 'job_status', 'job_id': 'foreign-job'}}
        self.assertEqual(call(self.service, req, self.token)[0], 400)
        self.assertEqual(self.source.calls, 0)

    def test_expiry_and_revocation(self):
        ledger = self.make_ledger(grants=(replace(self.grant, expires_at_ns=200),))
        self.clock.n = 200
        self.assertEqual(call(Service(ledger), self.request(), self.token)[0], 403)
        self.clock.n = 1000
        self.assertEqual(call(self.service, self.request(), self.token)[0], 401)
        self.assertEqual(self.source.calls, 0)

    def test_shared_request_limits_and_interval_reset(self):
        ledger = self.make_ledger(limits=Limits(1, 1048576, 2097152, 60000000000))
        self.assertEqual(call(Service(ledger), self.request(), self.token)[0], 200)
        self.assertEqual(call(Service(ledger), self.request(), self.token)[0], 429)
        # Expiry is still independently enforced at a new accounting interval.
        self.clock.n = 60000000000
        self.assertEqual(call(Service(ledger), self.request(), self.token)[0], 401)

    def test_policy_window_cannot_be_shortened(self):
        for interval in (1, 999, 59999999999, 60000000001):
            with self.assertRaises(ValueError): Limits(60, 1048576, 2097152, interval)
        self.assertEqual(Limits(60,1048576,2097152,60000000000).interval_ns,60000000000)
        for n in range(60):
            self.clock.n = 100 + n
            self.assertEqual(call(self.service,self.request(),self.token)[0],200)
        self.clock.n = 160
        self.assertEqual(call(self.service,self.request(),self.token)[0],429)
        self.assertEqual(self.source.calls,60)

    def test_absent_native_column_is_not_present_null(self):
        with self.assertRaisesRegex(ValueError, 'absent_native_column'):
            RawDataset('synthetic.raw','owned-v1',self.scope,self.source.value,self.source.read,
                columns=('condition',),rights=self.dataset.rights,rights_owner='owned-fixture',rights_evidence='synthetic-owned-v1',valid_from_ns=0,expires_at_ns=1000)
        # Existing known_at_ns column is present and its actual null cell remains null.
        self.assertIsNotNone(self.source.value.batch.column('known_at_ns'))
        result = call(self.service,self.request(columns=['known_at_ns']),self.token)[1]
        self.assertEqual(result['payload']['columns'][0]['values'],[None,{'type':'int64','value':'0'}])

    def test_reference_effective_start_half_open_scope(self):
        def reference(stamp):
            values = {'instrument_id':('owned:ONE',),'session_id':('session-1',),
                'reference_id':('ref-1',),'fact_kind':('owned-fact',),
                'effective_start_ns':(stamp,),'effective_end_ns':(END+100,), 'price':(10100,)}
            return RawRead(CanonicalBatch(DataKind.REFERENCE,tuple(Column(k,v) for k,v in values.items()),
                BatchMetadata('owned.fixture',SourceBinding('owned','snapshot-1','mapping-1','input-1'),Coverage(1,1,True),PriceUnit(2,'USD'),scope=InputScope(START,END,'owned-v1'))),'a'*64)
        def register(value):
            return RawDataset('synthetic.reference','owned-v1',self.scope,value,lambda cancel:value,
                columns=('effective_start_ns','effective_end_ns','price'),rights=self.dataset.rights,
                rights_owner='owned-fixture',rights_evidence='synthetic-owned-v1',valid_from_ns=0,expires_at_ns=1000)
        for stamp in (0,START-1,END):
            with self.assertRaises(codec.WireError): register(reference(stamp))
        # Canonical reference selection bounds the effective start; end may extend beyond the read window.
        registered = register(reference(START))
        self.assertEqual(registered.produce(('effective_end_ns',),'1.0')[1]['columns'][0]['values'],[{'type':'int64','value':str(END+100)}])

    def test_errors_count_transfer_and_exhaustion_emits_no_free_body(self):
        ledger = self.make_ledger(limits=Limits(60, 1048576, 1, 60000000000))
        status, _, data, headers = call(Service(ledger), self.request(), '')
        self.assertEqual((status, data, headers['Content-Length']), (429, b'', '0'))
        self.assertEqual(self.source.calls, 0)

    def test_revocation_wins_barrier_before_visibility(self):
        entered, release = threading.Event(), threading.Event()
        def pause(): entered.set(); self.assertTrue(release.wait(10))
        self.source.before = pause
        results = []
        worker = threading.Thread(target=lambda: results.append(call(self.service, self.request(), self.token)))
        worker.start(); self.assertTrue(entered.wait(10)); self.ledger.revoke('grant-a'); release.set(); worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(results[0][0], 403)
        self.assertEqual(results[0][1]['kind'], 'error')
        self.assertNotIn(b'10100', results[0][2])

    def test_prepared_response_reauthorized_on_first_iteration(self):
        headers = []
        output = self.service(env(self.request(), self.token), lambda s, h: headers.append(s))
        self.assertEqual(headers, [])
        self.ledger.revoke('grant-a')
        value = json.loads(b''.join(output))
        self.assertTrue(headers[0].startswith('403'))
        self.assertEqual(value['kind'], 'error')

    def test_revocation_during_size_admission_prevents_source_read(self):
        original = self.dataset.size_hint
        def changed(*args):
            amount = original(*args)
            self.ledger.revoke('grant-a')
            return amount
        self.dataset.size_hint = changed
        self.assertEqual(call(self.service, self.request(), self.token)[0], 403)
        self.assertEqual(self.source.calls, 0)
        self.assertEqual(self.ledger._reserved, 0)

    def test_unemitted_response_holds_budget_and_close_releases_it(self):
        amount = self.dataset.size_hint(tuple(self.dataset.columns), '1.0', 'http-1')
        ledger = self.make_ledger(limits=Limits(60, amount, amount, 60000000000))
        service = Service(ledger)
        output = service(env(self.request(), self.token), lambda s, h: None)
        self.assertEqual(ledger._reserved, amount)
        self.assertEqual(call(service, self.request(), self.token)[0], 429)
        self.assertEqual(self.source.calls, 1)
        output.close()
        self.assertEqual(ledger._reserved, 0)
        self.assertEqual(call(service, self.request(), self.token)[0], 200)
        self.assertEqual(ledger._transfer, amount)

    def test_untrusted_clock_and_multiprocess_or_foreign_peer_denied(self):
        for changes in ({'wsgi.multiprocess': True}, {'REMOTE_ADDR': '203.0.113.1'}, {'QUERY_STRING': 'token=untrusted'}):
            self.assertEqual(call(self.service, self.request(), self.token, **changes)[0], 400)
        self.clock.n = True
        self.assertEqual(call(self.service, self.request(), self.token)[0], 500)
        self.assertEqual(self.source.calls, 0)

    def test_producer_receipt_and_mapping_tamper_redacted(self):
        self.source.value = replace(self.source.value, receipt_fingerprint='c' * 64)
        status, result, data, _ = call(self.service, self.request(), self.token)
        self.assertIn(status, (400, 422))
        self.assertNotIn(b'10100', data)
        self.assertEqual(result['kind'], 'error')

    def test_current_source_content_and_full_binding_tamper(self):
        original = self.source.value
        for field, value in (('input_id','input-1:chunk:0:0:2'),('mapping_version','mapping-1:mapped:changed'),('source_id','foreign-source')):
            batch = replace(original.batch,metadata=replace(original.batch.metadata,source=replace(original.batch.metadata.source,**{field:value})))
            self.source.value = replace(original,batch=batch)
            self.assertEqual(call(self.service,self.request(),self.token)[0],422)
        batch = replace(original.batch,columns=tuple(replace(c,values=(99999,10200)) if c.name=='price' else c for c in original.batch.columns))
        self.source.value = replace(original,batch=batch)
        status, _, data, _ = call(self.service,self.request(),self.token)
        self.assertEqual(status,422)
        self.assertNotIn(b'99999',data)

    def test_nonmonotonic_producer_rejects_without_reordering(self):
        original=self.source.value
        batch=replace(original.batch,columns=tuple(replace(c,values=(START+1,START)) if c.name=='event_ns' else c for c in original.batch.columns))
        self.source.value=replace(original,batch=batch)
        status,result,_,_=call(self.service,self.request(),self.token)
        self.assertEqual(status,422)
        self.assertEqual(result['kind'],'error')

    def test_raw_right_and_discovery_are_independently_required(self):
        for actions, op in ((frozenset(('derived_read','discover')),'slice'),(frozenset(('raw_read',)),'discover')):
            service = Service(self.make_ledger(grants=(replace(self.grant,actions=actions),)))
            request = self.request()
            if op=='discover': request['payload']={'operation':'discover'}
            self.assertEqual(call(service,request,self.token)[0],403)
        self.assertEqual(self.source.calls,0)

    def test_discovery_rebuilds_on_revocation_and_versions_are_explicit(self):
        request=self.request();request['payload']={'operation':'discover'};request['version']='1.1'
        headers=[]
        output=self.service(env(request,self.token),lambda s,h:headers.append(s))
        self.ledger.revoke('grant-a')
        self.assertEqual(json.loads(b''.join(output))['kind'],'error')
        self.assertTrue(headers[0].startswith('403'))
        fresh=Service(self.make_ledger())
        self.assertEqual(call(fresh,request,self.token)[1]['payload']['transport_versions'],['1.0','1.1'])
        request['version']='1.0'
        self.assertEqual(call(fresh,request,self.token)[1]['payload']['transport_versions'],['1.0'])

    def test_row_100_and_changed_producer_101_without_partial_output(self):
        baseline=raw_batch(100)
        baseline=replace(baseline,columns=tuple(replace(c,values=(START,)*100) if c.name=='event_ns' else c for c in baseline.columns))
        self.source.value=RawRead(baseline,'a'*64)
        dataset=RawDataset('synthetic.raw','owned-v1',self.scope,self.source.value,self.source.read,
            columns=('event_ns',),rights=self.dataset.rights,rights_owner='owned-fixture',rights_evidence='synthetic-owned-v1',valid_from_ns=0,expires_at_ns=1000)
        service=Service(self.make_ledger(datasets=(dataset,)))
        result=call(service,self.request(columns=['event_ns']),self.token)
        self.assertEqual(result[0],200)
        self.assertEqual(len(result[1]['payload']['columns'][0]['values']),100)
        self.source.value=RawRead(raw_batch(101),'a'*64)
        status, result, _, _=call(service,self.request(columns=['event_ns']),self.token)
        self.assertEqual(status,429)
        self.assertEqual(result['kind'],'error')

    def test_response_actual_262144_and_262145_bytes(self):
        baseline=raw_batch(100)
        baseline=replace(baseline,columns=tuple(replace(c,values=(START,)*100) if c.name=='event_ns' else c for c in baseline.columns)+(Column('condition',('',)*100),))
        def dataset_for(batch):
            self.source.value=RawRead(batch,'a'*64)
            return RawDataset('synthetic.raw','owned-v1',self.scope,self.source.value,self.source.read,
                columns=('condition',),rights=self.dataset.rights,rights_owner='owned-fixture',rights_evidence='synthetic-owned-v1',valid_from_ns=0,expires_at_ns=1000)
        empty=dataset_for(baseline)
        overhead=empty.size_hint(('condition',),'1.0','http-1')
        for total, status in ((262144,200),(262145,429)):
            padding=total-overhead
            values=tuple('x'*(padding//100+(n<padding%100)) for n in range(100))
            self.assertLessEqual(max(map(len,values)),4096)
            batch=replace(baseline,columns=tuple(replace(c,values=values) if c.name=='condition' else c for c in baseline.columns))
            dataset=dataset_for(batch)
            grant=replace(self.grant,columns=frozenset(('condition',)))
            service=Service(self.make_ledger(grants=(grant,),datasets=(dataset,)))
            before=self.source.calls
            result=call(service,self.request(columns=['condition']),self.token)
            self.assertEqual(result[0],status)
            if status==200:
                self.assertEqual(len(result[2]),total)
                self.assertEqual(self.source.calls,before+1)
            else:
                self.assertEqual(result[1]['kind'],'error')
                self.assertEqual(self.source.calls,before)

    def test_transfer_exact_boundary_and_one_byte_excess(self):
        amount=self.dataset.size_hint(tuple(self.dataset.columns),'1.0','http-1')
        for budget, status in ((amount,200),(amount-1,429)):
            service=Service(self.make_ledger(limits=Limits(60,budget,budget,60000000000)))
            before=self.source.calls
            result=call(service,self.request(),self.token)
            self.assertEqual(result[0],status)
            self.assertEqual(self.source.calls,before+(status==200))

    def test_http_correlation_does_not_change_native_source(self):
        first = call(self.service, self.request(), self.token)[1]
        req = self.request(); req['request_id'] = 'http-2'
        second = call(self.service, req, self.token)[1]
        self.assertEqual(first['payload'], second['payload'])
        self.assertEqual(second['request_id'], 'http-2')

    def test_actual_loopback_http_and_duplicate_credentials(self):
        server = qualification_server(self.service)
        worker = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
        worker.start()
        try:
            host, port = server.server_address[:2]
            c = http.client.HTTPConnection(host, port, timeout=10)
            body = codec.canonical(self.request())
            c.request('POST', '/v1/request', body, {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.token})
            response = c.getresponse(); data = response.read()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(data)['payload']['dataset'], self.dataset.identity.wire())
            c.close()
            c = http.client.HTTPConnection(host, port, timeout=10)
            c.putrequest('POST', '/v1/request')
            c.putheader('Authorization', 'Bearer ' + self.token)
            c.putheader('Authorization', 'Bearer ' + self.foreign)
            c.putheader('Content-Type', 'application/json'); c.putheader('Content-Length', str(len(body)))
            c.endheaders(body)
            response = c.getresponse(); data = response.read()
            self.assertEqual(response.status, 401)
            self.assertNotIn(self.token.encode(), data)
            c.close()
        finally:
            server.shutdown(); worker.join(10); server.server_close()

    def feature_setup(self, *, unknown=False, structured=False):
        spec = ConfigSpec.from_json((ROOT / 'tests/service/fixtures/approved-config.json').read_text(encoding='utf-8'))
        batch = raw_batch(1)
        batch = replace(batch, metadata=replace(batch.metadata, scope=InputScope(START, START + 1, 'owned-v1')),
            columns=tuple(replace(c, values=(None if unknown else START,)) if c.name == 'known_at_ns' else c for c in batch.columns))
        if structured:
            interval=IntervalSpec('owned-interval',START,START+1)
            spec=replace(spec,session=replace(spec.session,intervals=(interval,)))
            values={'instrument_id':('owned:ONE',),'session_id':('session-1',),'start_ns':(START,),'end_ns':(START+1,),
                'open':(10100,),'high':(10200,),'low':(10000,),'close':(10150,),'volume':(5,),'known_at_ns':(START,)}
            batch=CanonicalBatch(DataKind.BAR,tuple(Column(k,v) for k,v in values.items()),replace(batch.metadata,interval_coverage=(IntervalCoverage('owned-interval',START,START+1,Coverage(1,1,True)),)))
            result=compute_structure(batch,spec,entity=EntityKey('owned:ONE','session-1'))
        else:
            result = compute_trades(batch, spec, entity=EntityKey('owned:ONE', 'session-1'))
        producer = json.loads((ROOT / 'tests/service/fixtures/calculate.json').read_text(encoding='utf-8'))['payload']
        producer['context']['config']['digest']=spec.digest
        if structured: producer['context']['features']=[{'feature_id':'session.structure.interval_ohlcv','algorithm_version':'v1'}]
        scope = Scope('owned:ONE', 'session-1', START, START + 1)
        original_command={'operation':'calculate','context':producer['context'],'scope':scope.wire()}
        producer['command_digest']=hashlib.sha256(json.dumps(original_command,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')).hexdigest()
        identity = DatasetIdentity.from_source('synthetic.features', 'owned-v1', batch.metadata.source)
        rights = frozenset(('derived_read', 'discover', 'retain'))
        dataset = FeatureDataset(identity, scope, result, producer['context'], producer['command_digest'], rights=rights,
            rights_owner='owned-fixture', rights_evidence='owned-native-calc', valid_from_ns=0, expires_at_ns=1000)
        grant = replace(self.grant, dataset_id='synthetic.features', scope=scope, columns=frozenset(dataset.columns), actions=rights)
        service = Service(self.make_ledger(grants=(grant,), datasets=(dataset,)))
        req = self.request(dataset=identity.wire(), scope=scope.wire(), columns=['session.structure.interval_ohlcv' if structured else 'session.trade.count']); req['version'] = '1.1'
        return service, req, result

    def test_actual_native_feature_quality_metadata_and_version(self):
        service, req, native = self.feature_setup()
        status, result, _, _ = call(service, req, self.token)
        self.assertEqual(status, 200)
        self.assertEqual(result['kind'], 'feature_slice')
        output = result['payload']['feature_result']
        self.assertEqual(output['columns'][0]['values'], [{'type': 'int64', 'value': '1'}])
        self.assertEqual(output['quality'][0]['status'], 'available')
        self.assertEqual(output['metadata'], codec.cell(native.metadata))
        self.assertEqual(output['backend_version'], native.metadata.backend_version)
        req['version'] = '1.0'
        self.assertEqual(call(service, req, self.token)[0], 400)

    def test_derived_slice_requires_derived_right(self):
        service, req, _ = self.feature_setup()
        service.ledger.grants = tuple(replace(g, actions=frozenset(('raw_read',))) for g in service.ledger.grants)
        self.assertEqual(call(service, req, self.token)[0], 403)

    def test_actual_unavailable_native_quality_remains_null(self):
        service,req,native=self.feature_setup(unknown=True)
        status,result,_,_=call(service,req,self.token)
        self.assertEqual(status,200)
        value=result['payload']['feature_result']
        self.assertEqual(value['columns'][0]['values'],[None])
        self.assertEqual(value['quality'][0]['status'],'missing_input')
        self.assertIn('unknown_availability',value['quality'][0]['reasons'])
        self.assertEqual(value['metadata'],codec.cell(native.metadata))

    def test_actual_structured_interval_and_nested_quality(self):
        service,req,native=self.feature_setup(structured=True)
        status,result,_,_=call(service,req,self.token)
        self.assertEqual(status,200)
        value=result['payload']['feature_result']['columns'][0]['values'][0]
        self.assertEqual(value['name'],'IntervalOHLCV')
        row=value['fields']['rows']['items'][0]['fields']
        self.assertEqual(row['volume'],{'type':'int64','value':'5'})
        self.assertEqual(row['open_price'],{'type':'float64','bits':'4059400000000000'})
        self.assertEqual(row['quality']['name'],'QualityRow')
        self.assertEqual(row['quality']['fields']['status'],{'type':'string','value':'available'})
        self.assertEqual(value,codec.cell(native.values[0].values[0]))
        producer=result['payload']['feature_result']
        original_command={'operation':'calculate','context':producer['context'],'scope':req['payload']['scope']}
        expected=hashlib.sha256(json.dumps(original_command,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')).hexdigest()
        self.assertEqual(producer['command_digest'],expected)

    def test_producer_command_digest_is_bound_to_original_context_scope(self):
        service,req,native=self.feature_setup(structured=True)
        identity=DatasetIdentity(**req['payload']['dataset'])
        dataset=service.ledger.datasets[identity.dataset_id]
        original=dataset.produce(tuple(dataset.columns),'1.1')[1]['feature_result']
        stale=json.loads((ROOT/'tests/service/fixtures/calculate.json').read_text(encoding='utf-8'))['payload']['command_digest']
        self.assertNotEqual(stale,original['command_digest'])
        for digest in (stale,'0'*64):
            with self.assertRaises(codec.WireError) as caught:
                FeatureDataset(identity,dataset.scope,native,original['context'],digest,rights=dataset.rights,
                    rights_owner='owned-fixture',rights_evidence='owned-native-calc',valid_from_ns=0,expires_at_ns=1000)
            self.assertEqual(caught.exception.code,'inconsistent_identity')
        # Column projection leaves the original full producer command untouched.
        result=call(service,req,self.token)[1]['payload']['feature_result']
        self.assertEqual(result['context'],original['context'])
        self.assertEqual(result['command_digest'],original['command_digest'])

    def test_forged_native_result_fails_registration(self):
        service,req,native=self.feature_setup()
        identity=DatasetIdentity(**req['payload']['dataset'])
        scope=Scope('owned:ONE','session-1',START,START+1)
        # Bypass frozen dataclass construction to verify SDK native re-admission.
        forged=replace(native)
        object.__setattr__(forged,'quality',())
        original=service.ledger.datasets[identity.dataset_id].produce(('session.trade.count',),'1.1')[1]['feature_result']
        with self.assertRaises(SinkError) as caught:
            FeatureDataset(identity,scope,forged,original['context'],original['command_digest'],rights=frozenset(('derived_read','retain')),
                rights_owner='owned-fixture',rights_evidence='owned-native-calc',valid_from_ns=0,expires_at_ns=1000)
        self.assertEqual(caught.exception.code,SinkErrorCode.INVALID_CONTENT)


if __name__ == '__main__': unittest.main()
