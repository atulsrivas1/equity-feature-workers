"""Run one bounded synthetic HTTP slice; no provider, files or public binding."""
import http.client
import json
import secrets
import threading

from equity_feature_contracts import BatchMetadata, CanonicalBatch, Column, Coverage, DataKind, InputScope, PriceUnit, SourceBinding
from equity_feature_contracts.adapters import Cancellation
from equity_feature_service import Credential, Grant, Ledger, Limits, RawDataset, RawRead, Scope, Service
from equity_feature_service.loopback import qualification_server


def main() -> None:
    scope = Scope('owned:ONE', 'session-1', 10, 20)
    columns = {'instrument_id':('owned:ONE',)*2,'session_id':('session-1',)*2,
        'event_ns':(10,15),'order_key':(0,1),'event_id':('one','two'),'eligible':(True,True),
        'price':(10100,10200),'size':(2,3),'known_at_ns':(None,0)}
    batch = CanonicalBatch(DataKind.TRADE,tuple(Column(k,v) for k,v in columns.items()),
        BatchMetadata('owned.example',SourceBinding('owned','snapshot-1','mapping-1','input-1'),Coverage(2,2,True),
            PriceUnit(2,'USD'),scope=InputScope(10,20,'owned-v1')))
    baseline = RawRead(batch,'a'*64)  # Owned in-memory receipt; real sources provide actual native receipt digest.
    def read(cancellation: Cancellation | None) -> RawRead:
        if cancellation is not None and cancellation.is_cancelled():
            raise RuntimeError('cancelled')
        return baseline
    rights = frozenset(('raw_read','discover'))
    dataset = RawDataset('synthetic.example','owned-v1',scope,baseline,read,
        columns=('event_ns','price','size','known_at_ns'),rights=rights,rights_owner='owned-example',
        rights_evidence='synthetic-only',valid_from_ns=0,expires_at_ns=1000)
    token = secrets.token_urlsafe(32)
    credential = Credential.provision('example-principal',token,0,1000)
    grant = Grant('example-grant','example-principal','synthetic.example','owned-v1','policy-v1',
        scope,frozenset(dataset.columns),rights,0,1000)
    ledger = Ledger(clock=lambda:100,limits=Limits(60,1048576,2097152,60000000000),
        credentials=(credential,),grants=(grant,),datasets=(dataset,),policy_revision='policy-v1',registry_snapshot='b'*64)
    server = qualification_server(Service(ledger))
    thread = threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.05})
    thread.start()
    connection = http.client.HTTPConnection(*server.server_address[:2],timeout=10)
    try:
        request = {'schema':'equity.remote','version':'1.0','kind':'request','request_id':'example',
            'payload':{'operation':'slice','dataset':dataset.identity.wire(),'scope':scope.wire(),
                'columns':list(dataset.columns),'cursor':None}}
        connection.request('POST','/v1/request',json.dumps(request).encode(),
            {'Content-Type':'application/json','Authorization':'Bearer '+token})
        response = connection.getresponse()
        result = json.loads(response.read())
        assert response.status == 200
        actual = {c['name']:c['values'] for c in result['payload']['columns']}
        assert actual['price'] == [{'type':'int64','value':'10100'},{'type':'int64','value':'10200'}]
        assert actual['known_at_ns'] == [None,{'type':'int64','value':'0'}]
        print(json.dumps({'status':response.status,'columns':actual},sort_keys=True))
    finally:
        connection.close()
        server.shutdown()
        thread.join(10)
        server.server_close()


if __name__ == '__main__': main()
