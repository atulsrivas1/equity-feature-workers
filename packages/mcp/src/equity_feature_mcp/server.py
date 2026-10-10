"""Owning-thread, synchronous bounded stdio tools over the public client."""
from __future__ import annotations

from dataclasses import asdict
from importlib.resources import files
import json
import re
import secrets
from threading import get_ident
import time
from typing import Any, BinaryIO, cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from equity_feature_client import JobExpectation, RemoteClient, Outcome

from .models import MCPProfile, RawSliceRegistration, FeatureSliceRegistration, CommandRegistration
from ._ledger import Ledger, Record, ReferenceError
from . import _wire


def _asset(name: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(files('equity_feature_mcp').joinpath('schemas', name).read_bytes()))


def _identifier(value: object) -> bool:
    return (type(value) is int and 0 <= value <= 2147483647) or (type(value) is str and re.fullmatch(r'[\x20-\x7e]{1,64}', value) is not None)


def _failure(code: str) -> dict[str, Any]:
    return dict(ok=False, failure=dict(source='mcp', code=code))


class StdioServer:
    def __init__(self, client: RemoteClient, profile: MCPProfile) -> None:
        if type(client) is not RemoteClient or type(profile) is not MCPProfile:
            raise ValueError('profile_invalid')
        self._client = client
        self._profile = profile
        self._thread = get_ident()
        self._closed = self._ran = self._initialized = self._ready = False
        self._ledger = Ledger(profile, time.monotonic_ns)
        self._params = _asset('protocol.json')['params_schemas']
        self._tools = _asset('tools.json')['tools']
        self._active: Record | None = None

    def close(self) -> None:
        self._closed = True
        self._ledger.close()
        self._client.close()

    def _error(self, identifier: object, code: int) -> dict[str, Any]:
        messages = {-32700: 'Parse error', -32600: 'Invalid Request', -32601: 'Method not found',
                    -32602: 'Invalid params', -32603: 'Internal error'}
        return dict(jsonrpc='2.0', id=identifier, error=dict(code=code, message=messages[code]))

    def _valid_params(self, name: str, params: object) -> bool:
        if not Draft202012Validator(self._params[name]).is_valid(params):
            return False
        assert isinstance(params, dict)
        if len(_wire.canonical(params.get('_meta', {}))) > 2048:
            return False
        if name == 'initialize' and len(_wire.canonical(params['capabilities'].get('experimental', {}))) > 4096:
            return False
        return True

    def _tool_result(self, tool: dict[str, Any], structured: dict[str, Any]) -> dict[str, Any]:
        if not Draft202012Validator(tool['outputSchema']).is_valid(structured) or len(_wire.canonical(structured)) > 16384:
            structured = _failure('summary_bounds')
        text = _wire.canonical(structured).decode('ascii')
        return dict(content=[dict(type='text', text=text)], structuredContent=structured, isError=not structured['ok'])

    def _call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ledger.sample()
        self._active = None
        correlation = secrets.token_urlsafe(24)
        keyword = dict(request_id=correlation, version='1.1')
        slot = None
        record = None
        registration: RawSliceRegistration | FeatureSliceRegistration | CommandRegistration | None = None
        outcome: Outcome[Any]
        try:
            if name == 'equity_discover':
                outcome = self._client.discover(**keyword)
            elif name == 'equity_slice':
                registration = next((r for r in self._profile.slices if r.profile_id == arguments['profile']), None)
                if registration is None:
                    return _failure('unknown_registration')
                if type(registration) is RawSliceRegistration:
                    outcome = self._client.slice_raw(registration.expectation, **keyword)
                else:
                    assert isinstance(registration, FeatureSliceRegistration)
                    outcome = self._client.slice_features(registration.dataset, registration.expectation, registration.selected, **keyword)
            elif name == 'equity_calculate':
                registration = next((c for c in self._profile.commands if c.profile_id == arguments['profile']), None)
                if registration is None:
                    return _failure('unknown_registration')
                assert isinstance(registration, CommandRegistration)
                slot = self._ledger.reserve(registration.profile_id, arguments['idempotency_key'])
                outcome = self._client.calculate(registration.expectation, arguments['idempotency_key'], **keyword)
            else:
                record = self._ledger.lookup(arguments['reference'])
                registration = next(c for c in self._profile.commands if c.profile_id == record.profile_id)
                assert isinstance(registration, CommandRegistration)
                if name in ('equity_job_status', 'equity_job_cancel'):
                    if record.mode != 'job':
                        return _failure('unknown_reference')
                    expected = JobExpectation(record.job_id, record.command_digest)
                    outcome = self._client.job_status(expected, **keyword) if name == 'equity_job_status' else self._client.job_cancel(expected, **keyword)
                else:
                    if record.result_id is None:
                        return _failure('unknown_reference')
                    if name == 'equity_artifact_reference':
                        if record.mode != 'job':
                            return _failure('unknown_reference')
                        slot = self._ledger.reserve(record.profile_id, origin=record)
                    outcome = self._client.artifact_read(record.result_id, registration.expectation, **keyword) if name == 'equity_artifact_reference' or record.mode == 'artifact' else self._client.result_read(record.result_id, registration.expectation, **keyword)
            self._ledger.sample()
            if outcome.failure is not None:
                denial = outcome.failure
                if record is not None and (denial.status in (401, 403) or denial.category in ('authorization', 'credentials')):
                    self._ledger.invalidate(record)
                return dict(ok=False, failure=dict(source='client', **asdict(denial)))
            assert outcome.view is not None
            data = outcome.view.payload_json()
            if name == 'equity_calculate':
                assert slot is not None and isinstance(registration, CommandRegistration)
                record = self._ledger.commit(slot, registration.expectation.command_digest, data['job_id'], data['result_id'])
            elif record is not None:
                record = self._ledger.lookup(record.reference)
                if name in ('equity_job_status', 'equity_job_cancel') and data['result_id'] is not None:
                    record = self._ledger.set_result(record, data['result_id'])
                elif name == 'equity_artifact_reference':
                    assert slot is not None
                    record = self._ledger.commit(slot, record.command_digest, record.job_id, record.result_id, record)
            self._active = record
            if name == 'equity_discover':
                payload = dict(representation='remote_discovery', data=data)
            elif name == 'equity_slice':
                payload = dict(representation='raw_slice' if type(registration) is RawSliceRegistration else 'projected_feature_slice', data=data)
            elif name in ('equity_calculate', 'equity_job_status', 'equity_job_cancel'):
                assert record is not None
                payload = dict(representation='native_job_state', reference=record.reference, profile=record.profile_id, data=data)
            elif name == 'equity_artifact_reference':
                assert record is not None
                payload = dict(representation='opaque_artifact_reference', reference=record.reference, followup_tool='equity_result_summary', operation='artifact_read')
            else:
                assert record is not None
                payload = dict(representation='complete_transported_producer', reference=record.reference, data=data)
            return dict(ok=True, payload=payload)
        except ReferenceError as error:
            return _failure(str(error))
        finally:
            if slot is not None:
                self._ledger.release(slot)

    def _dispatch(self, value: Any) -> dict[str, Any] | None:
        if type(value) is not dict or set(value) - {'jsonrpc', 'id', 'method', 'params'} or value.get('jsonrpc') != '2.0' or type(value.get('method')) is not str:
            return self._error(None, -32600)
        notification = 'id' not in value
        identifier = value.get('id')
        if not notification and not _identifier(identifier):
            return self._error(None, -32600)
        method = value['method']
        params = value.get('params', {})
        if notification:
            if method == 'notifications/initialized' and self._initialized and self._valid_params('initialized', params):
                self._ready = True
            elif method == 'notifications/cancelled':
                self._valid_params('cancelled', params)  # Synchronous calls are not interruptible here.
            return None
        names = {'initialize': 'initialize', 'ping': 'ping', 'tools/list': 'tools_list', 'tools/call': 'tools_call'}
        if method not in names:
            return self._error(identifier, -32601)
        if not self._valid_params(names[method], params):
            return self._error(identifier, -32602)
        if method == 'initialize':
            if self._initialized:
                return self._error(identifier, -32600)
            self._initialized = True
            result: dict[str, Any] = dict(protocolVersion='2025-11-25', capabilities=dict(tools=dict(listChanged=False)), serverInfo=dict(name='equity-feature-mcp', version='0.1.0a0'))
        elif method == 'ping':
            result = {}
        elif not self._ready:
            return self._error(identifier, -32600)
        elif method == 'tools/list':
            result = dict(tools=self._tools)
        else:
            tool = next((t for t in self._tools if t['name'] == params['name']), None)
            arguments = params.get('arguments', {})
            if tool is None or len(_wire.canonical(arguments)) > 4096 or not Draft202012Validator(tool['inputSchema']).is_valid(arguments):
                return self._error(identifier, -32602)
            result = self._tool_result(tool, self._call(tool['name'], arguments))
        return dict(jsonrpc='2.0', id=identifier, result=result)

    def run_stdio(self, input_binary: BinaryIO, output_binary: BinaryIO) -> None:
        if get_ident() != self._thread or self._ran or self._closed:
            raise ValueError('profile_invalid')
        self._ran = True
        try:
            while not self._closed:
                self._active = None
                try:
                    incoming = _wire.read_frame(input_binary)
                    if incoming is None:
                        break
                    value = _wire.parse(incoming)
                except _wire.FrameError:
                    _wire.write_frame(output_binary, _wire.encode_frame(self._error(None, -32700)))
                    break
                try:
                    response = self._dispatch(value)
                except Exception:
                    response = self._error(value.get('id') if type(value) is dict and _identifier(value.get('id')) else None, -32603)
                if response is None:
                    continue
                frame = _wire.encode_frame(response)
                try:
                    self._ledger.sample()
                    if self._active is not None:
                        self._ledger.lookup(self._active.reference)
                except ReferenceError as error:
                    if str(error) == 'profile_invalid':
                        self.close()
                        break
                    if 'result' in response and 'structuredContent' in response['result']:
                        tool = next(t for t in self._tools if t['name'] == value['params']['name'])
                        response['result'] = self._tool_result(tool, _failure(str(error)))
                        frame = _wire.encode_frame(response)
                        self._ledger.sample()
                    else:
                        raise
                _wire.write_frame(output_binary, frame)
        except (_wire.FrameError, ReferenceError):
            pass
        except BaseException:
            # No original exception graph or text crosses stdout/stderr.
            pass
        finally:
            self.close()
