import hashlib
import socket
import ssl
import sys
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'packages/client/src'))
from equity_feature_client._transport import Origin, ExchangeError, decode_frame, exchange, validate_context


def wire(body=b'{}', *, length=None, extras=b'', prefix=b'HTTP/1.0 200 OK'):
    return prefix+b'\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Length: '+str(len(body) if length is None else length).encode()+b'\r\n'+extras+b'\r\n'+body


class Transport(unittest.TestCase):
    def real_exchange(self, response, *, stall=False, timeout=.4):
        listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(2)
        origin=Origin.parse('http://127.0.0.1:'+str(listener.getsockname()[1]))
        captured=[];errors=[];stop=threading.Event();request=[]
        def server():
            try:
                connection,_=listener.accept()
                with connection:
                    connection.settimeout(2);data=b''
                    while b'\r\n\r\n' not in data: data+=connection.recv(4096)
                    headers,body=data.split(b'\r\n\r\n',1)
                    while len(body)<2: body+=connection.recv(4096)
                    request.append((headers,body))
                    try: connection.sendall(response)
                    except (BrokenPipeError,ConnectionResetError): pass
                    if stall: stop.wait(2)
            except BaseException as error: errors.append(type(error).__name__)
        thread=threading.Thread(target=server);thread.start()
        def active(connection):
            if connection is not None: captured.append(connection)
        try:
            return exchange(origin,b'{}','A'*43,timeout=timeout,deadline=time.monotonic()+timeout,context=None,active=active)
        finally:
            stop.set();listener.close();thread.join(3)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            self.assertTrue(all(connection.fileno()==-1 for connection in captured))
            self.assertEqual(len(request),1)
            self.assertIn(b'POST /v1/request HTTP/1.0',request[0][0])
            self.assertEqual(request[0][1],b'{}')

    def test_actual_eof_exact_and_trailing_truncated(self):
        self.assertEqual(self.real_exchange(wire()).body,b'{}')
        for body,length in [(b'{}x',2),(b'{',2)]:
            with self.subTest(body=body),self.assertRaisesRegex(ExchangeError,'invalid_response'):
                self.real_exchange(wire(body,length=length))

    def test_missing_eof_cannot_return_success(self):
        with self.assertRaisesRegex(ExchangeError,'timeout'):
            self.real_exchange(wire(),stall=True,timeout=.15)

    def test_real_inclusive_body_and_overflow(self):
        payload=b'a'*262144
        frame=self.real_exchange(wire(payload),timeout=2)
        self.assertEqual(hashlib.sha256(frame.body).digest(),hashlib.sha256(payload).digest())
        with self.assertRaisesRegex(ExchangeError,'bounds'):
            self.real_exchange(wire(payload+b'a'),timeout=2)

    def test_header_inclusive_and_one_over(self):
        base=wire(extras=b'X-Owned: \r\n')
        size=base.find(b'\r\n\r\n')+4
        exact=wire(extras=b'X-Owned: '+b'a'*(8192-size)+b'\r\n')
        self.assertEqual(exact.find(b'\r\n\r\n')+4,8192)
        self.assertEqual(self.real_exchange(exact,timeout=2).body,b'{}')
        with self.assertRaisesRegex(ExchangeError,'bounds'):
            self.real_exchange(exact.replace(b'X-Owned: ',b'X-Owned: a'),timeout=2)

    def test_duplicate_critical_headers_and_unsupported_codings(self):
        for extra in [b'Content-Length: 2\r\n',b'content-type: application/json\r\n',b'Cache-Control: no-store\r\n',b'X-Content-Type-Options: nosniff\r\n',b'Content-Disposition: one\r\nContent-Disposition: two\r\n',b'Transfer-Encoding: chunked\r\n',b'Content-Encoding: gzip\r\n',b'Connection: keep-alive\r\n']:
            with self.subTest(extra=extra),self.assertRaises(ExchangeError): decode_frame(wire(extras=extra))
        for data in [wire(prefix=b'HTTP/2 200 OK'),wire().replace(b'Content-Type:',b' Content-Type:'),wire().replace(b'application/json',b'text/html'),wire().replace(b'no-store',b'public'),wire().replace(b'nosniff',b'yes'),wire().replace(b'Content-Length: 2',b'Content-Length: +2')]:
            with self.subTest(data=data[:50]),self.assertRaises(ExchangeError):decode_frame(data)

    def test_origin_and_trust_configuration(self):
        for value in ['http://localhost','http://example.com','http://10.0.0.1','https://user:secret@example.com','https://example.com/path','https://example.com/?token=secret','https://example.com/#fragment','https://example.com\r\nSecret','ftp://example.com','https://example.com:0','https://example.com:65536']:
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'^invalid_origin$'):Origin.parse(value)
        self.assertEqual(Origin.parse('http://[::1]:8080').host_header,'[::1]:8080')
        self.assertTrue(Origin.parse('https://example.com').secure)
        validate_context(ssl.create_default_context())
        with self.assertRaisesRegex(ValueError,'^invalid_configuration$'):validate_context(ssl._create_unverified_context())

    def test_token_and_request_denied_before_socket(self):
        origin=Origin.parse('http://127.0.0.1:9')
        for token in ['A'*42,'A'*257,'A'*43+'\r\nsecret',True]:
            with self.assertRaisesRegex(ExchangeError,'token_invalid'):
                exchange(origin,b'{}',token,timeout=.1,deadline=time.monotonic()+.1,context=None,active=lambda _:None)
        with self.assertRaisesRegex(ExchangeError,'bounds'):
            exchange(origin,b'a'*16385,'A'*43,timeout=.1,deadline=time.monotonic()+.1,context=None,active=lambda _:None)


if __name__=='__main__':unittest.main()
