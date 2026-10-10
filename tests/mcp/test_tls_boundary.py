"""Public MCP over real verified TLS, owned ephemeral synthetic credentials."""
from pathlib import Path
import json
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'packages/client/src'))
sys.path.insert(0,str(ROOT/'packages/mcp/src'))
sys.path.insert(0,str(ROOT/'tests/client'))
from equity_feature_client import RemoteClient
from equity_feature_mcp import MCPProfile,CommandRegistration
import test_client as fixtures
import test_native_authority as dialogue
import test_transport as transport


class TLSBoundary(unittest.TestCase):
    request=dialogue.NativeAuthority.request
    tool=dialogue.NativeAuthority.tool
    start=dialogue.NativeAuthority.start
    run_dialogue=dialogue.NativeAuthority.run_dialogue
    value=dialogue.NativeAuthority.value

    def test_real_tls_trust_hostname_eof_faults_and_denial_through_mcp(self):
        _,expected=fixtures.producer(fixtures.FIXTURES['native_cases'][0])
        self.profile=MCPProfile('tls-owner',(),(CommandRegistration('cmd',expected),),60)
        openssl=shutil.which('openssl') or ('C:/Program Files/Git/usr/bin/openssl.exe' if Path('C:/Program Files/Git/usr/bin/openssl.exe').is_file() else None)
        self.assertIsNotNone(openssl)
        with tempfile.TemporaryDirectory() as directory:
            cert,key=(Path(directory)/name for name in ('cert.pem','key.pem'))
            subprocess.run([openssl,'req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=owned.invalid','-addext','subjectAltName=IP:127.0.0.1','-keyout',str(key),'-out',str(cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_context.load_cert_chain(cert,key)
            for case in ('valid','untrusted','hostname','extra','truncated','duplicate','authorization'):
                with self.subTest(case=case):
                    host='127.0.0.2' if case=='hostname' else '127.0.0.1'
                    listener=socket.socket();listener.bind((host,0));listener.listen(1);listener.settimeout(3)
                    origin='https://'+host+':'+str(listener.getsockname()[1])
                    context=ssl.create_default_context() if case=='untrusted' else ssl.create_default_context(cafile=str(cert))
                    requests=[];errors=[];calls=[]
                    def serve():
                        try:
                            connection,_=listener.accept()
                            with connection:
                                connection.settimeout(3)
                                try:
                                    with server_context.wrap_socket(connection,server_side=True) as secured:
                                        data=b''
                                        while b'\r\n\r\n' not in data:
                                            chunk=secured.recv(4096)
                                            if not chunk:raise AssertionError('owned request EOF')
                                            data+=chunk
                                            if len(data)>16384:raise AssertionError('owned request bounds')
                                        headers,body=data.split(b'\r\n\r\n',1)
                                        length=int(next(v.split(b':',1)[1] for v in headers.split(b'\r\n') if v.lower().startswith(b'content-length:')))
                                        if not 0<=length<=16384:raise AssertionError('owned content length')
                                        while len(body)<length:
                                            chunk=secured.recv(min(4096,length-len(body)))
                                            if not chunk:raise AssertionError('owned body EOF')
                                            body+=chunk
                                        request=json.loads(body);requests.append(request)
                                        kind='error' if case=='authorization' else 'discovery'
                                        payload=dict(category='authorization',code='not_permitted',retryable=False) if kind=='error' else dict(transport_versions=['1.1'],registry_snapshot='0'*64,features=[])
                                        encoded=json.dumps(fixtures.envelope(kind,payload,request['request_id'],request['version'])).encode()
                                        length=len(encoded)
                                        if case=='extra':encoded+=b'x'
                                        if case=='truncated':encoded=encoded[:-1]
                                        extras=b'Content-Length: '+str(length).encode()+b'\r\n' if case=='duplicate' else b''
                                        secured.sendall(transport.wire(encoded,length=length,extras=extras,prefix=b'HTTP/1.0 403 Owned' if case=='authorization' else b'HTTP/1.0 200 Owned'))
                                except (ssl.SSLError,ConnectionResetError):
                                    if case not in ('untrusted','hostname'):raise
                        except BaseException as error:errors.append(type(error).__name__)
                    thread=threading.Thread(target=serve);thread.start()
                    client=RemoteClient(origin,lambda:calls.append(1) or 'A'*43,ssl_context=context,attempts=3,timeout=2)
                    try:
                        def commands(responses):
                            yield from self.start();yield self.tool('equity_discover')
                            actual=self.value(responses)
                            if case=='valid':self.assertTrue(actual['ok'],actual);self.assertEqual(actual['payload']['representation'],'remote_discovery')
                            else:
                                self.assertFalse(actual['ok']);self.assertEqual(actual['failure']['source'],'client')
                                self.assertEqual(actual['failure']['code'],'tls_verification_failed' if case in ('untrusted','hostname') else 'remote_denial' if case=='authorization' else 'invalid_response')
                                self.assertNotIn('payload',actual)
                            self.assertEqual(len(calls),1);self.assertIsNone(client._socket)
                        self.run_dialogue(client,commands)
                    finally:client.close();listener.close();thread.join(4)
                    self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
                    self.assertEqual(len(requests),0 if case in ('untrusted','hostname') else 1)
        self.assertFalse(cert.exists() or key.exists())
