"""Independent fixed-codec/names, raw bytes and bounded read race controls."""
from io import BytesIO
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
from eq142_graph import digest
from eq142_proofs import validate_reference,check_body,decode_record,read_proof,REGISTRY


def reference(body=b'{}\n',codec='eq142_development_runner',name='runner.json'):
    return {'codec':codec,'sha256':digest(body),'bytes':len(body),'retained_name':name}


class PortableProofs(unittest.TestCase):
    def test_exact_body_and_closed_ref_are_pure_not_codec_or_writer_authority(self):
        body=b'{}\n';ref=reference(body)
        with patch.object(Path,'open',side_effect=AssertionError('pure helper must not read')):
            self.assertIs(check_body(body,ref,scope='runner',name='runner.json'),body)
            self.assertEqual(decode_record(body,maximum=1048576),{})
        for value in (b'[]\n',body+b' ',bytearray(body)):
            with self.assertRaises(ValueError):check_body(value,ref,scope='runner',name='runner.json')
        for key,value in [('bytes',True),('bytes',0),('bytes',1048577),('sha256','A'*64),
                ('retained_name','../runner.json'),('codec','unknown'),('extra',False)]:
            bad={**ref,key:value}
            with self.subTest(key=key),self.assertRaises(ValueError):validate_reference(bad,scope='runner',name='runner.json')

    def test_independent_codec_caps_names_and_context_substitutions(self):
        # Explicit V7 cases independent of registry iteration/cap arithmetic.
        cases=(('runner','eq142_development_runner','runner.json',1048576),
            ('runner','eq142_supervisor_outcome','supervisor.json',65536),
            ('runner','diagnostic_snapshot','diagnostics.json',1048576),
            ('reference_preparation','eq142_product_projection','products.json',65536),
            ('reference_preparation','diagnostic_snapshot','diagnostics.json',1048576),
            ('sample','eq142_owned_runner_return','runner-reference.json',4096),
            ('sample','native_receipt','native.json',16777216),
            ('sample','capacity_receipt','capacity-before.json',16384),
            ('sample','eq142_complete_root','complete-root.json',65536),
            ('sample','eq142_measurement_envelope','measurement.json',65536),
            ('sample','eq142_complete_candidate','candidate.json',4096),
            ('reference','eq142_development_reference','reference.json',16777216),
            ('trace','eq142_local_fragment','delegate-015.json',16777216),
            ('owned_processes','eq142_owned_process_witness','parent.json',4096),
            ('code_counts','eq142_code_counts','parent.json',65536),
            ('qualification','eq142_count_qualification','count-qualification.json',1048576))
        for scope,codec,name,maximum in cases:
            ref={'codec':codec,'retained_name':name,'bytes':maximum,'sha256':'a'*64}
            with self.subTest(codec=codec):
                validate_reference(ref,scope=scope,name=name)
                with self.assertRaises(ValueError):validate_reference({**ref,'bytes':maximum+1},scope=scope,name=name)
                with self.assertRaises(ValueError):validate_reference(ref,scope=scope,name='unknown.json')
        with self.assertRaises(TypeError):REGISTRY['runner']['eq142_development_runner']=(16777216,r'.*')
        for scope,ref,name in (
                ('sample',reference(codec='diagnostic_snapshot',name='diagnostics.json'),'diagnostics.json'),
                ('runner',reference(codec='native_receipt',name='native.json'),'native.json'),
                ('reference_preparation',reference(),'runner.json'),
                ('reference_preparation',reference(codec='eq142_product_projection',name='products.json'),'diagnostics.json'),
                ('sample',reference(codec='capacity_receipt',name='capacity-after.json'),'capacity-before.json'),
                ('qualification',reference(codec='capacity_receipt',name='capacity-before.json'),'capacity-before.json'),
                ('trace',reference(codec='eq142_local_fragment',name='delegate-016.json'),'delegate-016.json')):
            with self.assertRaises(ValueError):validate_reference(ref,scope=scope,name=name)
        body=b'original SDK encoding stays opaque';ref=reference(body,codec='encode_result',name=digest(body)+'.json')
        self.assertEqual(check_body(body,ref,scope='products',name=ref['retained_name']),body)
        bad={**ref,'retained_name':'b'*64+'.json'}
        with self.assertRaises(ValueError):validate_reference(bad,scope='products',name=bad['retained_name'])

    def test_development_json_duplicate_noncanonical_and_bound_refusals(self):
        for raw in (b'{"a":1,"a":2}\n',b'{ "a":1}\n',b'{"a":NaN}\n',b'{}',b'\xff',b' '*17):
            with self.assertRaises(ValueError):decode_record(raw,maximum=16)
        for maximum in (True,0,16777217):
            with self.assertRaises(ValueError):decode_record(b'{}\n',maximum=maximum)
        self.assertEqual(decode_record(b'[1,null,false]\n',maximum=16),[1,None,False])

    def test_bounded_io_checks_actual_bytes_and_growth_after_stat(self):
        body=b'{}\n';ref=reference(body)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'runner.json';path.write_bytes(body)
            self.assertEqual(read_proof(directory,ref,scope='runner',name='runner.json'),body)
            requests=[]
            class GrowingStream(BytesIO):
                def read(self,size=-1):
                    requests.append(size);return super().read(size)
            stream=GrowingStream(body+b'x'*100)
            with patch.object(Path,'open',return_value=stream):
                with self.assertRaises(ValueError):read_proof(directory,ref,scope='runner',name='runner.json')
            self.assertTrue(stream.closed);self.assertEqual(requests,[4])
            path.write_bytes(b'[]\n')
            with self.assertRaises(ValueError):read_proof(directory,ref,scope='runner',name='runner.json')
            with patch.object(Path,'is_junction',return_value=True),patch.object(Path,'open',side_effect=AssertionError('refuse before read')):
                with self.assertRaises(ValueError):read_proof(directory,ref,scope='runner',name='runner.json')


if __name__=='__main__':unittest.main()
