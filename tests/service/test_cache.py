"""Independent pre-code key/byte boundaries and real retained native hits."""
from copy import deepcopy
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import unittest
from equity_feature_service._audit import OwnedAudit

from equity_feature_service._cache import ResultCache, canonical_record, fingerprint, identity_key
import test_delivery as delivery_helpers
import test_jobs as native_helpers
from test_service import call, env

FIXTURE = json.loads((Path(__file__).parent/'fixtures/cache_identity.json').read_text(encoding='utf-8'))


class Cache(unittest.TestCase):
    make = delivery_helpers.Delivery.make
    request = delivery_helpers.Delivery.request

    def test_original_independent_key_and_complete_record_fingerprints(self):
        for name, field in [('grant','grant_sha256'),('job_grant','job_grant_sha256'),('rights','rights_sha256')]:
            self.assertEqual(fingerprint(FIXTURE['fingerprinted_records'][name]),FIXTURE['cache_key'][field])
        self.assertEqual(identity_key(FIXTURE['cache_key']),FIXTURE['cache_key_sha256'])

    def test_all_33_literal_identity_negatives(self):
        native = deepcopy(delivery_helpers.FROZEN['cases'][0]['native_result']['fields'])
        context = deepcopy(delivery_helpers.FROZEN['cases'][0]['complete_producer_payload']['context'])
        records = FIXTURE['fingerprinted_records']
        def key(value):
            k = deepcopy(value['key'])
            for name, field in [('grant','grant_sha256'),('job_grant','job_grant_sha256'),('rights','rights_sha256')]:
                k[field] = fingerprint(value['records'][name])
            # Preserve explicit key mutations while binding raw context/native
            # mutations to their actual full canonical content fingerprints.
            if value['context'] != context:k['context_sha256'] = fingerprint(value['context'])
            if value['native'] != native:k['native_content_sha256'] = fingerprint(value['native'])
            return identity_key(k)
        baseline = dict(key=deepcopy(FIXTURE['cache_key']),records=deepcopy(records),context=context,native=native)
        original = key(baseline)
        self.assertEqual(len(FIXTURE['literal_identity_mutations']),33)
        for mutation in FIXTURE['literal_identity_mutations']:
            with self.subTest(dimension=mutation['dimension']):
                value = deepcopy(baseline)
                path = mutation['path'].split('.')
                target = value
                for part in path[:-1]:target = target[int(part)] if isinstance(target,list) else target[part]
                target[int(path[-1]) if isinstance(target,list) else path[-1]] = mutation['replacement']
                try:changed = key(value)
                except ValueError:continue
                self.assertNotEqual(changed,original)

    def test_complete_record_bounds_integer_and_set_semantics(self):
        self.assertEqual(canonical_record('a'*65534),b'"'+b'a'*65534+b'"')
        with self.assertRaises(ValueError):canonical_record('a'*65535)
        self.assertEqual(canonical_record({'n':9000000000000000001}),b'{"n":9000000000000000001}')
        with self.assertRaises(ValueError):canonical_record({'n':1.0})
        self.assertEqual(fingerprint(frozenset(('b','a'))),fingerprint(['a','b']))
        self.assertNotEqual(fingerprint(('b','a')),fingerprint(('a','b')))

    def test_independent_shared_byte_boundaries_and_cache_full_fallback(self):
        for row in FIXTURE['arithmetic'][:4]:
            with self.subTest(case=row['case']):
                cache = ResultCache()
                principal = row['case'].startswith('principal')
                size = row['cache_bytes']//(1 if principal else 2)
                payload = b'a'*size
                if not principal:
                    cache.representation('first','other','other-result',payload,300,0,0,0)
                result = cache.representation('key','owner','result',payload,300,0,
                    row['job_bytes'] if principal else 0,row['job_bytes'])
                self.assertEqual(result,payload)
                self.assertEqual('key' in cache.entries,row['allow'])
        cache = ResultCache()
        for n in range(9):cache.representation(str(n),str(n//4),'r'+str(n),b'x',300,0,0,0)
        self.assertEqual(len(cache.entries),8)
        self.assertNotIn('8',cache.entries)

    def test_hits_preserve_original_ttl_and_corrupt_partition_is_never_served(self):
        cache = ResultCache()
        cache.representation('key','owner','r',b'original',300,0,0,0)
        self.assertEqual(cache.representation('key','owner','r',b'original',300,299,0,0),b'original')
        cache.purge(300)
        self.assertEqual(cache.total,0)
        self.assertEqual(cache.representation('key','owner','r',b'original',300,300,0,0),b'original')
        self.assertEqual(cache.total,0)
        cache.representation('key','owner','r',b'corrupt',600,301,0,0)
        self.assertEqual(cache.representation('key','owner','r',b'verified',600,302,0,0),b'verified')
        self.assertEqual(cache.total,len(b'verified'))

    def test_real_native_hit_has_no_new_io_and_charges_every_transfer(self):
        scheduler,service,tokens,clock,audit,job = self.make()
        before = service.ledger._transfer
        for _ in range(2):
            status,body,*_ = call(service,self.request(job.result_id),tokens[0])
            self.assertEqual(status,200,body)
        self.assertEqual(len(scheduler.cache.entries),1)
        self.assertEqual(audit['reads'],1)
        self.assertGreater(service.ledger._transfer,before)
        self.assertEqual(service.ledger.audit_totals()['cache_used'],scheduler.cache.total)
        self.assertEqual(scheduler.total_retained,job.held)
        self.assertLessEqual(scheduler.total_retained+scheduler.cache.total,2_097_152)

    def test_warm_cache_cannot_bypass_original_forms_rights_or_expiry(self):
        for field in ('native','wire','receipt','envelope','rights','expiry'):
            with self.subTest(field=field):
                scheduler,service,tokens,clock,audit,job = self.make()
                self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],200)
                self.assertGreater(scheduler.cache.total,0)
                if field == 'rights':
                    service.ledger.grants=tuple(replace(g,actions=g.actions-{'derived_read'}) for g in service.ledger.grants)
                elif field == 'expiry':clock.n=job.expires_ns
                else:setattr(job,field,getattr(job,field)+b' ')
                self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
                self.assertEqual(audit['reads'],1)
                if field == 'expiry':self.assertEqual(scheduler.cache.total,0)

    def test_actual_owner_partitions_four_principal_eight_global_entries(self):
        scheduler,service,tokens,clock,audit,first = self.make()
        helper = native_helpers.Jobs()
        results = [(tokens[0],first)]
        for owner,count in ((0,4),(1,4)):
            for n in range(count):
                job = helper.wait_terminal(scheduler,helper.submit(service,tokens[owner],f'cache-owner-{owner}-{n}'))
                self.assertEqual(job.state,'succeeded')
                results.append((tokens[owner],job))
        payloads=[]
        for token,job in results:
            status,body,*_ = call(service,self.request(job.result_id),token)
            self.assertEqual(status,200,body)
            payloads.append(body['payload'])
        self.assertTrue(all(p == payloads[0] for p in payloads))
        # Equal native math does not collapse original owners/result IDs. The
        # fifth A result remains ordinary delivery; B has its own four entries.
        self.assertEqual(len(scheduler.cache.entries),8)
        self.assertEqual(sum(e.principal=='A' for e in scheduler.cache.entries.values()),4)
        self.assertEqual(sum(e.principal=='B' for e in scheduler.cache.entries.values()),4)
        self.assertEqual(audit['reads'],9)
        before = scheduler.cache.total
        self.assertEqual(call(service,self.request(first.result_id),tokens[1])[0],403)
        self.assertEqual(scheduler.cache.total,before)
        self.assertEqual(audit['reads'],9)

    def test_warm_representation_rebuilds_correlation_and_operation_frames(self):
        scheduler,service,tokens,clock,audit,job = self.make()
        for operation,rid in [('result_read','first'),('result_read','second'),('artifact_read','third')]:
            request=self.request(job.result_id,operation);request['request_id']=rid
            status,body,*_=call(service,request,tokens[0])
            self.assertEqual(status,200,body)
            self.assertEqual(body['request_id'],rid)
        self.assertEqual(len(scheduler.cache.entries),2)
        self.assertEqual(audit['reads'],1)
        for entry in scheduler.cache.entries.values():
            self.assertNotIn(b'"request_id"',entry.payload)
            self.assertEqual(entry.expires_ns,job.expires_ns)

    def test_actual_concurrent_hits_share_one_cache_reservation_and_charge_each_frame(self):
        store=OwnedAudit(owned_synthetic=True,valid_from_ns=0,expires_at_ns=300_000_000_000)
        scheduler,service,tokens,clock,audit,job = self.make(audit_store=store)
        before=service.ledger._transfer
        def read(n):
            request=self.request(job.result_id);request['request_id']='parallel-'+str(n)
            return call(service,request,tokens[0])
        with ThreadPoolExecutor(max_workers=10) as pool:responses=list(pool.map(read,range(10)))
        self.assertTrue(all(r[0]==200 for r in responses))
        self.assertEqual({r[1]['request_id'] for r in responses},{'parallel-'+str(n) for n in range(10)})
        self.assertEqual(service.ledger._transfer-before,sum(len(r[2]) for r in responses))
        self.assertEqual(len(scheduler.cache.entries),1)
        self.assertEqual(scheduler.cache.total,len(next(iter(scheduler.cache.entries.values())).payload))
        self.assertEqual(service.ledger._reserved,0)
        self.assertEqual(audit['reads'],1)
        snapshot=json.loads(store.snapshot(store.permit,clock.n))
        sequences=[r['sequence'] for r in snapshot['records']]
        self.assertEqual(sequences,list(range(len(sequences))))

    def test_concurrent_prepared_hits_revoke_before_emit_clear_cache_and_release_once(self):
        store=OwnedAudit(owned_synthetic=True,valid_from_ns=0,expires_at_ns=300_000_000_000)
        scheduler,service,tokens,clock,audit,job = self.make(audit_store=store)
        prepared=[]
        for n in range(10):
            request=self.request(job.result_id);request['request_id']='prepared-'+str(n)
            statuses=[]
            emission=service(env(request,tokens[0]),lambda status,headers,out=statuses:out.append(status))
            prepared.append((emission,statuses))
        self.assertGreater(scheduler.cache.total,0)
        service.ledger.revoke('grant-A')
        def emit(pair):
            emission,statuses=pair
            raw=b''.join(emission);emission.close();emission.close()
            return statuses,raw
        with ThreadPoolExecutor(max_workers=10) as pool:responses=list(pool.map(emit,prepared))
        self.assertTrue(all(s[0].startswith('403') and json.loads(raw)['kind']=='error' for s,raw in responses))
        self.assertEqual(scheduler.cache.total,0)
        self.assertEqual(service.ledger._reserved,0)
        self.assertEqual(audit['reads'],1)
        self.assertIsNotNone(job.committed_receipt_sha256)
        snapshot=json.loads(store.snapshot(store.permit,clock.n))
        sequences=[r['sequence'] for r in snapshot['records']]
        self.assertEqual(sequences,list(range(len(sequences))))


if __name__ == '__main__':unittest.main()
