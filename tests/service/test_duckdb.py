"""Owned actual original-Parquet/native-receipt integration, no provider data."""
from pathlib import Path
import secrets
import shutil
import tempfile
import unittest

import duckdb
from equity_feature_contracts import AvailabilitySpec, DataKind, InputScope, PriceUnit, SessionSpec
from equity_feature_contracts.adapters import AcquisitionRequest, SourceError, SourceErrorCode
from equity_feature_duckdb import CatalogConfig, DuckDBHistoricalAdapter, MappingPolicy, ReadConfig, SourceSelection, resolve_source
from equity_feature_service import Credential, Grant, Ledger, Limits, RawDataset, Service
from equity_feature_service.duckdb import DuckDBSource
from test_service import call


class NativeSourceVectors(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        db = root / 'catalog.duckdb'
        original, optimized = root / 'original.parquet', root / 'optimized.parquet'
        with duckdb.connect(str(db)) as c:
            c.execute('CREATE SCHEMA catalog')
            c.execute('CREATE TABLE catalog.catalog.datasets(layer VARCHAR,snapshot VARCHAR,dataset VARCHAR,source_schema VARCHAR,view_schema VARCHAR,view_name VARCHAR)')
            c.execute('CREATE TABLE catalog.catalog.files(layer VARCHAR,snapshot VARCHAR,dataset VARCHAR,source_schema VARCHAR,session_date DATE,path VARCHAR,bytes BIGINT,rows BIGINT,provenance VARCHAR,original_dataset VARCHAR,substituted_dataset VARCHAR,column_signature VARCHAR,optimized_path VARCHAR,optimized_bytes BIGINT,view_schema VARCHAR,view_name VARCHAR)')
            c.execute("INSERT INTO catalog.catalog.datasets VALUES ('prepared','frozen1','FICTION','trades','prepared1','source1')")
            c.execute("COPY (SELECT instrument_id,ts_utc,price::DOUBLE price,size FROM (VALUES (7,'1970-01-01T00:00:00.000000010Z',101.0,2),(7,'1970-01-01T00:00:00.000000015Z',102.0,3),(7,'1970-01-01T00:00:00.000000020Z',999.0,9),(8,'1970-01-01T00:00:00.000000012Z',888.0,8)) t(instrument_id,ts_utc,price,size)) TO ? (FORMAT PARQUET)", [str(original)])
            shutil.copyfile(original, optimized)
            c.execute('INSERT INTO catalog.catalog.files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', ['prepared','frozen1','FICTION','trades','1970-01-01',str(original),original.stat().st_size,4,'unverified_legacy','FICTION',None,'fictional-signature1',str(optimized),optimized.stat().st_size,'prepared1','source1'])
        resolution = resolve_source(CatalogConfig(db), SourceSelection('prepared','frozen1','FICTION','trades',('1970-01-01',)))
        mapping = MappingPolicy('trades',PriceUnit(2,'USD'),'decimal_repr','half_even',((7,'owned:ONE'),(8,'owned:TWO')),'event','owned-v1',True)
        adapter = DuckDBHistoricalAdapter(ReadConfig(catalog_path=db,resolved=resolution,mapping=mapping,namespace='owned.fixture',source_id='synthetic',calendar_version='owned-v1',sessions=(SessionSpec('owned.fixture','session-1',0,30,'UTC'),),partition_sessions=(('1970-01-01','session-1'),),scope=InputScope(0,30,'owned-v1'),max_files=1,max_batch_rows=2))
        request = AcquisitionRequest(request_id='server-owned-query-1',kind=DataKind.TRADE,namespace='owned.fixture',instruments=('owned:ONE',),sessions=('session-1',),start_ns=10,end_ns=20,snapshot_id='frozen1',price_unit=PriceUnit(2,'USD'),availability=AvailabilitySpec(20,20,20),max_batch_rows=2,max_rows=100,max_batches=100,sampling='none',selection='event_half_open')
        self.source = DuckDBSource(adapter, request)
        self.dataset = RawDataset('owned.native','v1',self.source.scope,self.source.baseline,self.source.read,columns=('event_ns','price','size'),rights=frozenset(('raw_read','discover')),rights_owner='owned-fixture',rights_evidence='synthetic-only',valid_from_ns=0,expires_at_ns=1000)
        self.token = secrets.token_urlsafe(32)
        grant = Grant('owned-grant','owned-principal','owned.native','v1','policy-v1',self.source.scope,frozenset(self.dataset.columns),self.dataset.rights,0,1000)
        self.ledger = Ledger(clock=lambda:100,limits=Limits(60,1048576,2097152,60000000000),credentials=(Credential.provision('owned-principal',self.token,0,1000),),grants=(grant,),datasets=(self.dataset,),policy_revision='policy-v1',registry_snapshot='b'*64)
        self.service = Service(self.ledger)
        self.request = {'schema':'equity.remote','version':'1.0','kind':'request','request_id':'http-one','payload':{'operation':'slice','dataset':self.dataset.identity.wire(),'scope':self.source.scope.wire(),'columns':['event_ns','price','size'],'cursor':None}}

    def test_native_full_binding_receipt_and_parameterized_selection(self):
        self.assertEqual(self.source.calls, 1)
        self.assertEqual(call(self.service,self.request,'')[0],401)
        self.assertEqual(self.source.calls, 1)
        status, result, _, _ = call(self.service,self.request,self.token)
        self.assertEqual(status,200)
        columns = {c['name']:[v['value'] for v in c['values']] for c in result['payload']['columns']}
        self.assertEqual(columns,{'event_ns':['10','15'],'price':['10100','10200'],'size':['2','3']})
        self.assertEqual(result['payload']['dataset'],self.dataset.identity.wire())
        self.assertIn('original-read2:', self.dataset.identity.mapping_version)
        self.assertIn('retained-map1:', self.dataset.identity.mapping_version)
        self.assertNotIn(':chunk:',self.dataset.identity.input_id)
        self.request['request_id'] = 'http-two'
        second = call(self.service,self.request,self.token)[1]
        self.assertEqual(result['payload'],second['payload'])
        self.assertEqual(self.source.calls,3)
        self.assertEqual(self.source.read().receipt_fingerprint,self.source.baseline.receipt_fingerprint)

    def test_native_cancellation_and_denied_scope_do_not_change_request(self):
        class Cancel:
            def is_cancelled(self): return True
        with self.assertRaises(SourceError) as caught:
            self.source.read(Cancel())
        self.assertEqual(caught.exception.code,SourceErrorCode.CANCELLED)
        before = self.source.calls
        self.request['payload']['scope']['start_ns'] = '11'
        self.assertEqual(call(self.service,self.request,self.token)[0],403)
        self.assertEqual(self.source.calls,before)
        self.assertEqual(self.source.request.start_ns,10)


if __name__ == '__main__': unittest.main()
