"""Local qualification server only; external deployment is a separate gate."""
from __future__ import annotations
import ipaddress
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server
from .service import Service


class _Handler(WSGIRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        # No request lines, arbitrary caller strings or credentials in qualification logs.
        pass

    def get_environ(self) -> dict[str, Any]:
        env = super().get_environ()
        if len(self.headers.get_all("Authorization", [])) != 1:
            env["HTTP_AUTHORIZATION"] = ""
        if len(self.headers.get_all("Content-Length", [])) != 1:
            env["CONTENT_LENGTH"] = ""
        return env


def qualification_server(service: Service, *, host: str = "127.0.0.1", port: int = 0) -> WSGIServer:
    if type(service) is not Service or not ipaddress.ip_address(host).is_loopback:
        raise ValueError("loopback_only")
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("invalid_port")
    # Synchronous single-process server; bounded request/body/transfer admission is in Service.
    return make_server(host, port, service, handler_class=_Handler)
