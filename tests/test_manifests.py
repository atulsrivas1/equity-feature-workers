from dataclasses import replace, FrozenInstanceError
import hashlib
import json
import unittest
from pathlib import Path
from equity_feature_contracts import Coverage, AvailabilitySpec, Parameter
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_contracts.publication import CompletionReceipt
from equity_feature_io_sdk import idempotency_key, verify_content
from equity_feature_workers import ClaimIdentity, ManifestError, OutputManifest, decode_task, encode_task, decode_output, encode_output
from manifest_fixture import fixture


class Manifests(unittest.TestCase):
    def test_literal_independent_wire_oracle(self):
        golden=json.loads(Path(__file__).with_name('manifest_golden.json').read_text(encoding='utf-8'))
        task=decode_task(golden['wire'].encode('ascii'))
        self.assertEqual(task.task_sha256,golden['domain_sha256'])
        self.assertEqual(task.job_id,'j')
        self.assertEqual(task.config.session.open_ns,100)
        self.assertEqual(task.features[0].feature_id,'session.bar.volume')
        self.assertEqual(encode_task(task).decode('ascii'),golden['wire'])

    def setUp(self):
        self.task, self.result, self.envelope = fixture()

    def rejected(self, **changes):
        with self.assertRaises(ManifestError):
            replace(self.task, **changes)

    def test_roundtrip_and_claim_separation(self):
        data = encode_task(self.task)
        self.assertEqual(decode_task(data), self.task)
        self.assertEqual(self.task.task_sha256, hashlib.sha256(b'efworker-task1\0'+data).hexdigest())
        claim = ClaimIdentity(self.task.task_sha256, 'owner1', 'try1', 0, 100)
        retry = replace(claim, owner_id='owner2', attempt_id='try2', issued_at_ns=100, expires_at_ns=200)
        self.assertEqual(claim.task_sha256, retry.task_sha256)
        with self.assertRaises(FrozenInstanceError):
            self.task.family = 'trades'

    def test_isolated_revision_configuration_and_destination(self):
        variants = [replace(self.task, generation_id='generation2'), replace(self.task, partition_id='A-S-2'),
            replace(self.task, destination_scope='other'),
            replace(self.task, inputs=(replace(self.task.inputs[0], revision_id='revision2'),)),
            replace(self.task, config=replace(self.task.config, identity='config2')),
            replace(self.task, config=replace(self.task.config, availability=AvailabilitySpec(199,210,210)))]
        for task in variants:
            self.assertNotEqual(task.task_sha256, self.task.task_sha256)
        self.assertEqual(variants[2].reuse_sha256, self.task.reuse_sha256)
        for task in variants[3:]:
            self.assertNotEqual(task.reuse_sha256, self.task.reuse_sha256)

    def test_missing_empty_partial_and_unknown_availability(self):
        original = self.task.inputs[0]
        missing = replace(original, state='missing', binding=None)
        binding = replace(original.binding, metadata=replace(original.binding.metadata, coverage=Coverage(0,0,True)))
        empty = replace(original, state='empty', binding=binding)
        partial = replace(original, state='partial', binding=replace(binding, metadata=replace(binding.metadata, coverage=Coverage(None,0,False))))
        self.assertEqual(len({replace(self.task, inputs=(v,)).task_sha256 for v in (original,missing,empty,partial)}),4)
        self.assertIsNone(original.known_at_ns)
        for state in ('empty','partial','missing'):
            with self.assertRaises(ManifestError):
                replace(original,state=state)

    def test_history_order_and_initialization(self):
        self.rejected(ordered_history=True)
        history = replace(self.task, ordered_history=True, warmup_sessions=('P',))
        self.assertEqual(decode_task(encode_task(history)),history)
        checkpoint = replace(self.task, ordered_history=True, initialization_sha256='a'*64)
        self.assertNotEqual(history.task_sha256,checkpoint.task_sha256)
        self.rejected(warmup_sessions=('P',))
        self.rejected(ordered_history=True,warmup_sessions=('S',))
        self.rejected(governed_sessions=tuple(reversed(self.task.governed_sessions)))
        self.rejected(governed_sessions=(IntervalSpec('P',0,101),IntervalSpec('S',100,200)))

    def test_exact_integer_version_labels_and_unsupported_merges(self):
        for value in (True,0,-1,2**63,1.5):
            self.rejected(max_input_bytes=value)
        for text in ('','private\ntext','\ud800'):
            self.rejected(job_id=text)
        self.rejected(protocol_version='future')
        self.rejected(merge_policy='continuous')
        self.rejected(ownership='shared_process_writers')
        self.rejected(instruments=('A','A'))
        self.rejected(features=self.task.features+self.task.features)
        self.rejected(inputs=self.task.inputs+self.task.inputs)
        ClaimIdentity('a'*64,'owner','attempt',-(2**63),2**63-1)
        for low,high in ((True,100),(100,100),(0,2**63)):
            with self.assertRaises(ManifestError):
                ClaimIdentity('a'*64,'owner','attempt',low,high)

    def test_nested_config_strings_and_preallocation_limits(self):
        for text in ('bad\nname','\ud800','\x7f'):
            for config in (replace(self.task.config,identity=text),
                replace(self.task.config,parameters=(Parameter(text,1),)),
                replace(self.task.config,parameters=(Parameter('value',text),)),
                replace(self.task.config,session=replace(self.task.config.session,timezone_label=text))):
                self.rejected(config=config)
        self.rejected(job_id='x'*4097)
        self.rejected(config=replace(self.task.config,parameters=(Parameter('large','x'*4097),)))
        self.rejected(config=replace(self.task.config,parameters=(Parameter('large',1<<4097),)))

    def test_closed_canonical_decoder(self):
        data=encode_task(self.task)
        raw=json.loads(data);raw['fields']['extra']=None
        for case in (json.dumps(raw).encode(),data+b'\n',b'{"a":1,"a":1}',b'null',b'x'*1048577,
            data.replace(b'"efworker-task1"',b'"untrusted.module"'),data.replace(b'"int":"4"',b'"int":"04"')):
            with self.assertRaises(ManifestError):
                decode_task(case)

    def test_existing_envelope_receipt_and_result_oracle(self):
        output=OutputManifest(self.task,self.envelope)
        self.assertFalse(output.committed)
        self.assertEqual(decode_output(encode_output(output)),output)
        verify_content(output.envelope,(self.result,))
        e=self.envelope
        receipt=CompletionReceipt(e.identity,idempotency_key(e.identity),e.expected_content_sha256,e.result_count,e.cell_count,e.evidence_count,e.content_bytes,(),None)
        committed=replace(output,receipt=receipt)
        self.assertTrue(committed.committed)
        self.assertEqual(decode_output(encode_output(committed)),committed)
        self.assertIsNone(committed.receipt.caller_committed_at_ns)

    def test_wrong_receipts_and_descriptors_reject(self):
        for field in ('job_id','generation_id','partition_id','destination_scope'):
            with self.assertRaises(ManifestError):
                OutputManifest(self.task,replace(self.envelope,**{field:'other'}))
        d=self.envelope.result_descriptors[0]
        for metadata in (replace(d.metadata,config_digest='b'*64),replace(d.metadata,availability=AvailabilitySpec(199,210,210)),replace(d.metadata,inputs=()),replace(d.metadata,session_id='P')):
            with self.assertRaises(ManifestError):
                OutputManifest(self.task,replace(self.envelope,result_descriptors=(replace(d,metadata=metadata),)))
        with self.assertRaises(ManifestError):
            OutputManifest(self.task,replace(self.envelope,result_descriptors=(replace(d,features=(replace(d.features[0],algorithm_version='wrong'),)+d.features[1:]),)))
        e=self.envelope
        receipt=CompletionReceipt(e.identity,'0'*64,e.expected_content_sha256,e.result_count,e.cell_count,e.evidence_count,e.content_bytes,(),None)
        with self.assertRaises(ManifestError):
            OutputManifest(self.task,e,receipt)


if __name__=='__main__':unittest.main()
