"""Single explicit socket exchange with EOF framing and verified TLS."""
from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import math
import re
import socket
import ssl
import time
from typing import Callable
from urllib.parse import urlsplit

from ._wire import REQUEST_LIMIT, RESPONSE_LIMIT

HEADER_LIMIT = 8192
_TOKEN = re.compile(r'[A-Za-z0-9_-]{43,256}', re.ASCII)


class ExchangeError(Exception):
    """Internal fixed code only; public boundary must return an inert Failure."""
    def __init__(self, code: str, phase: str, sent: bool = False) -> None:
        self.code, self.phase, self.sent = code, phase, sent
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class Origin:
    host: str
    port: int
    secure: bool
    host_header: str

    @classmethod
    def parse(cls, origin: str) -> Origin:
        try:
            if type(origin) is not str or not origin.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in origin):
                raise ValueError
            parsed = urlsplit(origin)
            if parsed.scheme not in ('http', 'https') or parsed.username is not None or parsed.password is not None or parsed.path not in ('', '/') or parsed.query or parsed.fragment or not parsed.hostname:
                raise ValueError
            host = parsed.hostname
            # Authority is a literal IP or a conventional ASCII DNS hostname.
            try:
                address = ipaddress.ip_address(host)
            except ValueError:
                if parsed.scheme == 'http' or len(host) > 253 or any(not re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?', label) for label in host.split('.')):
                    raise ValueError
            else:
                if parsed.scheme == 'http' and not address.is_loopback:
                    raise ValueError
            port = parsed.port if parsed.port is not None else (443 if parsed.scheme == 'https' else 80)
            if not 1 <= port <= 65535:
                raise ValueError
            authority = '[' + host + ']' if ':' in host else host
            return cls(host, port, parsed.scheme == 'https', authority + ':' + str(port))
        except (ValueError, TypeError):
            raise ValueError('invalid_origin') from None


@dataclass(frozen=True, slots=True)
class Frame:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def header(self, name: str) -> str | None:
        values = [value for key, value in self.headers if key == name.lower()]
        return values[0] if len(values) == 1 else None


def validate_context(context: ssl.SSLContext) -> None:
    if not isinstance(context, ssl.SSLContext) or context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
        raise ValueError('invalid_configuration')


def remaining(deadline: float, timeout: float) -> float:
    left = deadline - time.monotonic()
    if not math.isfinite(left) or left <= 0:
        raise ExchangeError('timeout', 'read')
    return min(left, timeout)


def decode_frame(data: bytes) -> Frame:
    split = data.find(b'\r\n\r\n')
    if split < 0 or split + 4 > HEADER_LIMIT:
        raise ExchangeError('invalid_response', 'decode')
    body = data[split + 4:]
    if len(body) > RESPONSE_LIMIT:
        raise ExchangeError('bounds', 'decode')
    try:
        lines = data[:split].decode('ascii').split('\r\n')
        match = re.fullmatch(r'HTTP/1\.[01] ([1-5][0-9]{2}) [\x20-\x7e]*', lines[0])
        if match is None:
            raise ValueError
        headers: list[tuple[str, str]] = []
        for line in lines[1:]:
            if ':' not in line:
                raise ValueError
            name, value = line.split(':', 1)
            if re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is None or any(ord(c) < 32 and c != '\t' or ord(c) == 127 for c in value):
                raise ValueError
            headers.append((name.lower(), value.strip(' \t')))
        critical = ('content-length', 'content-type', 'cache-control', 'x-content-type-options', 'content-disposition', 'transfer-encoding', 'content-encoding', 'connection')
        if any(sum(key == name for key, _ in headers) > 1 for name in critical):
            raise ValueError
        values = dict(headers)
        length = values.get('content-length', '')
        if re.fullmatch(r'0|[1-9][0-9]{0,5}', length) is None or int(length) != len(body):
            raise ValueError
        if 'transfer-encoding' in values or 'content-encoding' in values:
            raise ValueError
        if values.get('content-type', '').lower() != 'application/json' or values.get('cache-control', '').lower() != 'no-store' or values.get('x-content-type-options', '').lower() != 'nosniff':
            raise ValueError
        if 'connection' in values and values['connection'].lower() != 'close':
            raise ValueError
        return Frame(int(match[1]), tuple(headers), body)
    except (UnicodeError, ValueError, IndexError):
        raise ExchangeError('invalid_response', 'decode') from None


def exchange(origin: Origin, encoded: bytes, token: str, *, timeout: float,
             deadline: float, context: ssl.SSLContext | None,
             active: Callable[[socket.socket | None], None]) -> Frame:
    if type(encoded) is not bytes or len(encoded) > REQUEST_LIMIT:
        raise ExchangeError('bounds', 'prepare')
    if type(token) is not str or _TOKEN.fullmatch(token) is None:
        raise ExchangeError('token_invalid', 'provider')
    owned: socket.socket | None = None
    sent = False
    phase = 'connect'
    try:
        if origin.secure:
            if context is None:
                context = ssl.create_default_context()
            validate_context(context)
        # Own/register every allocated socket before any blocking connect.
        # socket.create_connection only cleans OSError, so it cannot provide
        # our explicit cleanup guarantee for SystemInterrupt during setup.
        addresses = socket.getaddrinfo(origin.host, origin.port, type=socket.SOCK_STREAM)
        connected = False
        for family, socktype, protocol, _, address in addresses[:8]:
            owned = socket.socket(family, socktype, protocol)
            active(owned)
            try:
                owned.settimeout(remaining(deadline, timeout))
                owned.connect(address)
            except OSError:
                owned.close()
                owned = None
                active(None)
                continue
            connected = True
            break
        if not connected:
            raise ExchangeError('disconnected', phase)
        assert owned is not None
        if origin.secure:
            assert context is not None
            owned.settimeout(remaining(deadline, timeout))
            wrapped = context.wrap_socket(owned, server_hostname=origin.host, do_handshake_on_connect=False)
            owned = wrapped
            active(owned)
            wrapped.settimeout(remaining(deadline, timeout))
            wrapped.do_handshake()
        phase = 'send'
        headers = ('POST /v1/request HTTP/1.0\r\nHost: ' + origin.host_header + '\r\nAuthorization: Bearer ' + token + '\r\nContent-Type: application/json\r\nContent-Length: ' + str(len(encoded)) + '\r\nConnection: close\r\n\r\n').encode('ascii')
        owned.settimeout(remaining(deadline, timeout))
        sent = True  # sendall can raise after transmitting a partial request.
        owned.sendall(headers + encoded)
        phase = 'read'
        captured = bytearray()
        header_end: int | None = None
        while True:
            owned.settimeout(remaining(deadline, timeout))
            maximum = HEADER_LIMIT if header_end is None else header_end + RESPONSE_LIMIT
            chunk = owned.recv(min(4096, maximum + 1 - len(captured)))
            if not chunk:
                break
            captured.extend(chunk)
            if header_end is None:
                found = captured.find(b'\r\n\r\n')
                if found >= 0:
                    header_end = found + 4
                    if header_end > HEADER_LIMIT:
                        raise ExchangeError('bounds', 'read', sent)
                elif len(captured) > HEADER_LIMIT:
                    raise ExchangeError('bounds', 'read', sent)
            if header_end is not None and len(captured) > header_end + RESPONSE_LIMIT:
                raise ExchangeError('bounds', 'read', sent)
        remaining(deadline, timeout)
        return decode_frame(bytes(captured))
    except ExchangeError as error:
        # Internal callers receive only fixed codes and possible-send state.
        raise ExchangeError(error.code, error.phase, sent) from None
    except ssl.SSLCertVerificationError:
        raise ExchangeError('tls_verification_failed', phase, sent) from None
    except (TimeoutError, socket.timeout):
        raise ExchangeError('timeout', phase, sent) from None
    except (OSError, ValueError):
        raise ExchangeError('disconnected', phase, sent) from None
    finally:
        try:
            if owned is not None:
                owned.close()
        finally:
            active(None)
