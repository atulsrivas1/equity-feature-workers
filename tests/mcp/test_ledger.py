from pathlib import Path
import json
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'packages/client/src'))
sys.path.insert(0, str(ROOT / 'packages/mcp/src'))
from equity_feature_client import FeatureRef, ProducerExpectation, ScopeKey
from equity_feature_mcp import CommandRegistration, MCPProfile
from equity_feature_mcp._ledger import Ledger, MAX_NS, ReferenceError


class References(unittest.TestCase):
    def setUp(self):
        payload = json.loads((ROOT / 'docs/EQ081_CLIENT_FIXTURES.json').read_bytes())['native_cases'][0]['complete_producer_payload']
        self.producer = ProducerExpectation(payload['context'], ScopeKey('A', 'S', 100, 200),
            tuple(FeatureRef(**v) for v in payload['executed_features']), payload['backend_id'], payload['backend_version'])
        self.profile = MCPProfile('owner', (), (CommandRegistration('cmd', self.producer),), 1)
        self.now = 0
        self.ledger = Ledger(self.profile, lambda: self.now)

    def job(self, key='key', job='job'):
        slot = self.ledger.reserve('cmd', key)
        return self.ledger.commit(slot, self.producer.command_digest, job, 'result')

    def test_capacity_before_provider_and_live_key_reuse_without_refresh(self):
        records = [self.job(str(i), 'job.' + str(i)) for i in range(16)]
        with self.assertRaisesRegex(ReferenceError, 'reference_capacity'):
            self.ledger.reserve('cmd', 'new')
        self.now = 100
        self.assertIs(self.job('0', 'job.0'), records[0])
        self.assertEqual(records[0].expires_ns, 1000000000)
        self.assertEqual(len(self.ledger._records), 16)

    def test_original_half_open_expiry_and_alias_job_never_resurrect(self):
        record = self.job()
        self.now = 999999999
        self.assertIs(self.ledger.lookup(record.reference), record)
        self.now += 1
        with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
            self.ledger.lookup(record.reference)
        with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
            self.ledger.reserve('cmd', 'key')
        slot = self.ledger.reserve('cmd', 'alias')
        with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
            self.ledger.commit(slot, self.producer.command_digest, 'job', 'result')
        self.assertEqual(len(self.ledger._records), 1)
        self.assertFalse(self.ledger._reservations)

    def test_late_first_response_retains_tombstone_and_unknown_releases(self):
        slot = self.ledger.reserve('cmd', 'late')
        self.now = 1000000000
        with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
            self.ledger.commit(slot, self.producer.command_digest, 'late-job')
        with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
            self.ledger.reserve('cmd', 'late')
        slot = self.ledger.reserve('cmd', 'unknown')
        self.ledger.release(slot)
        self.assertEqual(len(self.ledger._records), 1)
        self.assertFalse(self.ledger._reservations)

    def test_linked_artifact_denial_and_new_epoch_are_local(self):
        job = self.job()
        self.now = 100
        slot = self.ledger.reserve('cmd')
        artifact = self.ledger.commit(slot, self.producer.command_digest, 'job', 'result', job)
        self.assertEqual(artifact.expires_ns, job.expires_ns)
        self.ledger.invalidate(artifact)
        for value in (job, artifact):
            with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
                self.ledger.lookup(value.reference)
        fresh = Ledger(self.profile, lambda: self.now)
        with self.assertRaisesRegex(ReferenceError, 'unknown_reference'):
            fresh.lookup(job.reference)
        self.assertNotEqual(fresh._nonce, self.ledger._nonce)

    def test_clock_overflow_rollback_boolean_and_exception_fail_closed(self):
        self.now = MAX_NS - 1000000000
        slot = self.ledger.reserve('cmd', 'fits')
        self.assertEqual(slot.expires_ns, MAX_NS)
        self.ledger.release(slot)
        self.now += 1
        with self.assertRaisesRegex(ReferenceError, 'profile_invalid'):
            self.ledger.reserve('cmd', 'overflow')
        self.assertTrue(self.ledger._closed)
        for values in ((10, 9), (True,), (-1,), (MAX_NS + 1,)):
            clock = iter(values)
            ledger = Ledger(self.profile, lambda: next(clock))
            if len(values) == 2:
                ledger.sample()
            with self.assertRaisesRegex(ReferenceError, 'profile_invalid'):
                ledger.sample()
            self.assertTrue(ledger._closed)

    def test_collision_and_wrong_native_identity_do_not_overwrite(self):
        record = self.job()
        with patch('equity_feature_mcp._ledger.secrets.token_urlsafe', return_value=record.reference):
            with self.assertRaisesRegex(ReferenceError, 'reference_capacity'):
                self.ledger.reserve('cmd', 'new')
        slot = self.ledger.reserve('cmd', 'key')
        with self.assertRaisesRegex(ReferenceError, 'profile_invalid'):
            self.ledger.commit(slot, self.producer.command_digest, 'different-job')
        self.assertIs(self.ledger.lookup(record.reference), record)

    def test_tombstoned_native_job_cannot_reenter_through_profile_alias(self):
        profile = MCPProfile('owner', (), (CommandRegistration('A', self.producer), CommandRegistration('B', self.producer)), 1)
        ledger = Ledger(profile, lambda: self.now)
        a = ledger.commit(ledger.reserve('A', 'key'), self.producer.command_digest, 'same-job')
        b = ledger.commit(ledger.reserve('B', 'other'), self.producer.command_digest, 'same-job')
        ledger.invalidate(a)
        with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
            ledger.lookup(b.reference)
        with self.assertRaisesRegex(ReferenceError, 'reference_expired'):
            ledger.commit(ledger.reserve('B', 'new-key'), self.producer.command_digest, 'same-job')
        self.assertEqual(len(ledger._records), 2)

    def test_repeated_calculate_can_observe_result_without_refresh(self):
        record = self.ledger.commit(self.ledger.reserve('cmd', 'key'), self.producer.command_digest, 'job')
        self.assertIsNone(record.result_id)
        self.now = 100
        updated = self.ledger.commit(self.ledger.reserve('cmd', 'key'), self.producer.command_digest, 'job', 'result')
        self.assertEqual(updated.result_id, 'result')
        for field in ('reference', 'created_ns', 'expires_ns', 'stable_key_sha256'):
            self.assertEqual(getattr(updated, field), getattr(record, field))
        self.assertEqual(len(self.ledger._records), 1)


if __name__ == '__main__':
    unittest.main()
