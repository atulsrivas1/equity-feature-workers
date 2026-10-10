"""Independent pre-code key/byte boundaries and real retained native hits."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import unittest

from equity_feature_service._cache import ResultCache, canonical_record, fingerprint, identity_key
import test_delivery as delivery_helpers
from test_service import call

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


if __name__ == '__main__':unittest.main()
