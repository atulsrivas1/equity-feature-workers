from pathlib import Path
import dataclasses
import hashlib
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'packages/client/src'))
sys.path.insert(0, str(ROOT / 'packages/mcp/src'))
from equity_feature_client import DatasetKey, FeatureRef, ProducerExpectation, RawExpectation, ScopeKey
from equity_feature_mcp import CommandRegistration, FeatureSliceRegistration, MCPProfile, RawSliceRegistration


class Profiles(unittest.TestCase):
    def test_all_original_native_contexts_and_order_survive_profile(self):
        raw = (ROOT / 'docs/EQ081_CLIENT_FIXTURES.json').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), '9c9572de93acae9d31a3f09b64d69f9f6f09f226210d68b589eebff080919a29')
        for case in json.loads(raw)['native_cases']:
            original = case['complete_producer_payload']
            features = tuple(FeatureRef(**v) for v in original['executed_features'])
            context = json.loads(json.dumps(original['context']))
            expected = ProducerExpectation(context, ScopeKey('A', 'S', 100, 200), features, original['backend_id'], original['backend_version'])
            profile = MCPProfile('owner', (), (CommandRegistration('cmd', expected),))
            context.clear()
            copied = profile.payload_json()['commands'][0]['expectation']
            self.assertEqual(copied['context'], original['context'])
            self.assertEqual(copied['executed_features'], original['executed_features'])
            self.assertEqual(copied['backend_id'], original['backend_id'])
            self.assertEqual(copied['backend_version'], original['backend_version'])

    def test_projection_unexecuted_inventory_and_cross_namespace_denied(self):
        original = json.loads((ROOT / 'docs/EQ081_CLIENT_FIXTURES.json').read_bytes())['native_cases'][0]['complete_producer_payload']
        features = tuple(FeatureRef(**v) for v in original['executed_features'])
        expected = ProducerExpectation(original['context'], ScopeKey('A', 'S', 100, 200), features, original['backend_id'], original['backend_version'])
        dataset = self.raw().expectation.dataset
        for selected in ((FeatureRef('not-executed', '1'),), (features[0], features[0]), list(features)):
            with self.assertRaises(ValueError): FeatureSliceRegistration('projection', dataset, expected, selected)
        with self.assertRaises(ValueError): MCPProfile('owner', (self.raw('same'),), (CommandRegistration('same', expected),))

    def raw(self, name='owned'):
        dataset = DatasetKey('owned.trades', 'v1', 'snapshot1', 'map1', 'synthetic', 'trades')
        scope = ScopeKey('A', 'S', 9000000000000000110, 9000000000000000113)
        return RawSliceRegistration(name, RawExpectation(dataset, scope, 'trade', ('event_ns', 'price')))

    def test_owned_snapshot_preserves_literal_identity_and_ns(self):
        profile = MCPProfile('owner', (self.raw(),), ())
        value = profile.payload_json()
        self.assertEqual(value['slices'][0]['scope']['start_ns'], '9000000000000000110')
        self.assertEqual(value['slices'][0]['dataset']['source_id'], 'synthetic')
        value['slices'][0]['dataset']['source_id'] = 'foreign'
        self.assertEqual(profile.payload_json()['slices'][0]['dataset']['source_id'], 'synthetic')
        self.assertEqual(hashlib.sha256(profile.snapshot).hexdigest(), profile.snapshot_sha256)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            profile.owner_context_id = 'other'

    def test_invalid_labels_and_equality_impostor_denied(self):
        class Pretend:
            def __eq__(self, other): return True
        for label in ('has space', 'trailing\n', '\u00e9', 'x'*65, True, Pretend()):
            with self.subTest(label=type(label)), self.assertRaises(ValueError): self.raw(label)

    def test_capacity_namespace_and_exact_ttl(self):
        for slices, ttl in (((), 60), ((self.raw(), self.raw()), 60), (tuple(self.raw(str(n)) for n in range(9)), 60), ((self.raw(),), True), ((self.raw(),), 0), ((self.raw(),), 301)):
            with self.subTest(ttl=ttl, count=len(slices)), self.assertRaises(ValueError): MCPProfile('owner', slices, (), ttl)
        for ttl in (1, 300): self.assertEqual(MCPProfile('owner', (self.raw(),), (), ttl).reference_ttl_seconds, ttl)
        with self.assertRaises(ValueError): MCPProfile('owner', [self.raw()], ())

    def test_full_backend_snapshot_changes_with_source_identity(self):
        one = MCPProfile('owner', (self.raw(),), ())
        foreign = RawSliceRegistration('owned', RawExpectation(DatasetKey('owned.trades', 'v1', 'snapshot1', 'map1', 'foreign', 'trades'), self.raw().expectation.scope, 'trade', ('event_ns', 'price')))
        two = MCPProfile('owner', (foreign,), ())
        self.assertNotEqual(one.snapshot, two.snapshot)
        self.assertNotEqual(one.snapshot_sha256, two.snapshot_sha256)


if __name__ == '__main__': unittest.main()
