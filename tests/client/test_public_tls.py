"""Real verified TLS through the public client and inert failure boundary."""
from pathlib import Path
import json
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import unittest

from test_client import RemoteClient, envelope
from test_transport import wire


class PublicTLS(unittest.TestCase):
    def test_public_trust_hostname_complete_eof_and_denials(self):
        openssl=shutil.which('openssl') or ('C:/Program Files/Git/usr/bin/openssl.exe' if Path('C:/Program Files/Git/usr/bin/openssl.exe').is_file() else None)
        self.assertIsNotNone(openssl)
        with tempfile.TemporaryDirectory() as directory:
            cert=Path(directory)/'cert.pem';key=Path(directory)/'key.pem'
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
                                            if not chunk:raise AssertionError('unexpected request EOF')
                                            data+=chunk
                                        headers,body=data.split(b'\r\n\r\n',1)
                                        length=int(next(line.split(b':',1)[1] for line in headers.split(b'\r\n') if line.lower().startswith(b'content-length:')))
                                        while len(body)<length:
                                            chunk=secured.recv(4096)
                                            if not chunk:raise AssertionError('unexpected body EOF')
                                            body+=chunk
                                        request=json.loads(body);requests.append(request)
                                        if case=='authorization':
                                            encoded=json.dumps(envelope('error',dict(category='authorization',code='not_permitted',retryable=False),request['request_id'])).encode()
                                        else:
                                            encoded=json.dumps(envelope('discovery',dict(transport_versions=['1.1'],registry_snapshot='0'*64,features=[]),request['request_id'])).encode()
                                        length=len(encoded)
                                        if case=='extra':encoded+=b'x'
                                        if case=='truncated':encoded=encoded[:-1]
                                        extras=b'Content-Length: '+str(length).encode()+b'\r\n' if case=='duplicate' else b''
                                        secured.sendall(wire(encoded,length=length,extras=extras,prefix=b'HTTP/1.0 403 Owned' if case=='authorization' else b'HTTP/1.0 200 Owned'))
                                except (ssl.SSLError,ConnectionResetError):
                                    if case not in ('untrusted','hostname'):raise
                        except BaseException as error:errors.append(type(error).__name__)
                    thread=threading.Thread(target=serve);thread.start()
                    client=RemoteClient(origin,lambda:calls.append(1) or 'A'*43,ssl_context=context,attempts=3,timeout=2)
                    try:
                        result=client.discover(request_id='owned')
                        if case=='valid':self.assertTrue(result.ok,result.failure)
                        else:
                            self.assertFalse(result.ok);self.assertIsNone(result.view)
                            self.assertEqual(result.failure.code,'tls_verification_failed' if case in ('untrusted','hostname') else 'remote_denial' if case=='authorization' else 'invalid_response')
                            self.assertFalse(hasattr(result.failure,'__traceback__'))
                        self.assertEqual(len(calls),1)
                        self.assertEqual(len(requests),0 if case in ('untrusted','hostname') else 1)
                        self.assertIsNone(client._socket)
                    finally:
                        client.close();listener.close();thread.join(4)
                    self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.assertFalse(cert.exists() or key.exists())


if __name__=='__main__':unittest.main()
