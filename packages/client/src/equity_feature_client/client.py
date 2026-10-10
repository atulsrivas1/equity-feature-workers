"""Synchronous explicit remote operations; calculations remain local-independent."""
from __future__ import annotations

import math
import re
import socket
import ssl
from threading import RLock
import time
from typing import Callable, TypeVar, cast

from . import _transport
from .models import DatasetKey, Failure, FeatureRef, JobExpectation, Outcome, ProducerExpectation, RawExpectation, Request, _features
from ._response import DiscoveryView, RawSliceView, FeatureSliceView, JobView, ResultView, View, response

V = TypeVar('V', bound=View)
_READS = frozenset(('discover', 'slice', 'job_status', 'result_read', 'artifact_read'))
_MUTATIONS = frozenset(('calculate', 'job_cancel'))
_TOKEN = re.compile(r'[A-Za-z0-9_-]{43,256}', re.ASCII)


class RemoteClient:
    def __init__(self, origin: str, token_provider: Callable[[], str], *, timeout: float = 3.0,
                 attempts: int = 1, retry_delay: float = .1, budget: float = 15.0,
                 ssl_context: ssl.SSLContext | None = None) -> None:
        self._origin = _transport.Origin.parse(origin)
        if not callable(token_provider) or type(attempts) is not int or not 1 <= attempts <= 3:
            raise ValueError('invalid_configuration')
        for value, lower, upper in ((timeout, 0, 5), (budget, 0, 15), (retry_delay, -1, 5)):
            if type(value) not in (int, float) or not math.isfinite(value) or not lower < value <= upper:
                raise ValueError('invalid_configuration')
        if retry_delay < 0:
            raise ValueError('invalid_configuration')
        if ssl_context is not None:
            _transport.validate_context(ssl_context)
        self._provider = token_provider
        self._timeout, self._budget, self._delay = float(timeout), float(budget), float(retry_delay)
        self._attempts, self._context = attempts, ssl_context
        self._lock = RLock()
        self._closed = self._busy = False
        self._socket: socket.socket | None = None

    def __enter__(self) -> RemoteClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            connection = self._socket
            if connection is not None:
                # Shutdown wakes a peer-blocked recv on the caller's thread.
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                finally:
                    connection.close()

    def _active(self, connection: socket.socket | None) -> None:
        with self._lock:
            self._socket = connection
            if connection is not None and self._closed:
                connection.close()
                raise _transport.ExchangeError('client_closed', 'connect')

    def _current(self, deadline: float) -> Failure | None:
        with self._lock:
            if self._closed:
                return Failure('client', 'client_closed', 'read')
        if time.monotonic() >= deadline:
            return Failure('transport', 'timeout', 'read')
        return None

    def _call(self, prepare: Callable[[], Request], view_type: type[V],
              expected: RawExpectation | ProducerExpectation | JobExpectation | None = None,
              selected: tuple[FeatureRef, ...] | None = None) -> Outcome[V]:
        with self._lock:
            if self._closed:
                return Outcome(failure=Failure('client', 'client_closed', 'prepare'))
            if self._busy:
                return Outcome(failure=Failure('client', 'busy', 'prepare'))
            self._busy = True
        try:
            possible = [False]
            return self._perform(prepare, view_type, expected, selected, possible)
        except BaseException:
            # Inert value deliberately discards the original exception graph.
            return Outcome(failure=Failure('unknown', 'outcome_unknown' if possible[0] else 'cancelled', 'read'))
        finally:
            with self._lock:
                self._busy = False
                self._socket = None

    def _perform(self, prepare: Callable[[], Request], view_type: type[V],
                 expected: RawExpectation | ProducerExpectation | JobExpectation | None,
                 selected: tuple[FeatureRef, ...] | None, possible: list[bool]) -> Outcome[V]:
        deadline = time.monotonic() + self._budget
        try:
            request = prepare()
        except BaseException:
            return Outcome(failure=Failure('configuration', 'invalid_request', 'prepare'))
        operation = request.payload_json()['operation']
        mutation = operation in _MUTATIONS
        count = self._attempts if operation in _READS else 1
        for attempt in range(count):
            denial = self._current(deadline)
            if denial is not None:
                return Outcome(failure=denial)
            try:
                token = self._provider()
            except BaseException:
                return Outcome(failure=Failure('credentials', 'provider_failed', 'provider'))
            if type(token) is not str or _TOKEN.fullmatch(token) is None:
                return Outcome(failure=Failure('credentials', 'token_invalid', 'provider'))
            denial = self._current(deadline)
            if denial is not None:
                return Outcome(failure=denial)
            try:
                def sending() -> None:
                    possible[0] = mutation
                frame = _transport.exchange(self._origin, request.encoded, token, timeout=self._timeout,
                                            deadline=deadline, context=self._context, active=self._active, sending=sending)
            except _transport.ExchangeError as error:
                if mutation and error.sent:
                    return Outcome(failure=Failure('unknown', 'outcome_unknown', error.phase))
                if error.code in ('timeout', 'disconnected') and attempt + 1 < count and self._current(deadline) is None:
                    pause = min(self._delay, max(0., deadline - time.monotonic()))
                    time.sleep(pause)
                    continue
                category = 'client' if error.code == 'client_closed' else 'credentials' if error.code == 'token_invalid' else 'protocol' if error.code in ('invalid_response', 'bounds') else 'transport'
                return Outcome(failure=Failure(category, error.code, error.phase))
            except BaseException:
                # A setup fault outside the internal fixed error path cannot
                # safely establish whether a mutation reached the peer.
                return Outcome(failure=Failure('unknown', 'outcome_unknown' if mutation else 'cancelled', 'read'))
            finally:
                token = ''
            denial = self._current(deadline)
            if denial is not None:
                return Outcome(failure=Failure('unknown', 'outcome_unknown', 'read') if mutation else denial)
            try:
                result = response(frame, request, expected, selected)
            except BaseException:
                return Outcome(failure=Failure('protocol', 'invalid_response', 'decode', frame.status))
            denial = self._current(deadline)
            if denial is not None:
                return Outcome(failure=Failure('unknown', 'outcome_unknown', 'decode') if mutation else denial)
            if type(result) is Failure:
                return Outcome(failure=result)
            if type(result) is not view_type:
                return Outcome(failure=Failure('protocol', 'invalid_response', 'decode', frame.status))
            return Outcome(view=cast(V, result))
        return Outcome(failure=Failure('transport', 'timeout', 'read'))

    def discover(self, *, request_id: str, version: str = '1.1') -> Outcome[DiscoveryView]:
        return self._call(lambda: Request(version, request_id, {'operation': 'discover'}), DiscoveryView)

    def slice_raw(self, expectation: RawExpectation, *, request_id: str, version: str = '1.1') -> Outcome[RawSliceView]:
        def prepare() -> Request:
            if type(expectation) is not RawExpectation:
                raise ValueError('invalid_expectation')
            return Request(version, request_id, dict(operation='slice', dataset=expectation.dataset.payload_json(), scope=expectation.scope.payload_json(), columns=list(expectation.columns), cursor=None))
        return self._call(prepare, RawSliceView, expectation)

    def slice_features(self, dataset: DatasetKey, expectation: ProducerExpectation, selected: tuple[FeatureRef, ...], *, request_id: str, version: str = '1.1') -> Outcome[FeatureSliceView]:
        frozen: tuple[FeatureRef, ...] | None = None
        try:
            frozen = _features(selected)
        except BaseException:
            return Outcome(failure=Failure('configuration', 'invalid_expectation', 'prepare'))
        def prepare() -> Request:
            if version != '1.1' or type(dataset) is not DatasetKey or type(expectation) is not ProducerExpectation:
                raise ValueError('invalid_expectation')
            assert frozen is not None
            if any(f not in expectation.executed_features for f in frozen):
                raise ValueError('invalid_expectation')
            return Request(version, request_id, dict(operation='slice', dataset=dataset.payload_json(), scope=expectation.scope.payload_json(), columns=[f.feature_id for f in frozen], cursor=None))
        return self._call(prepare, FeatureSliceView, expectation, frozen)

    def calculate(self, expectation: ProducerExpectation, idempotency_key: str, *, request_id: str, version: str = '1.1') -> Outcome[JobView]:
        def prepare() -> Request:
            if type(expectation) is not ProducerExpectation:
                raise ValueError('invalid_expectation')
            return Request(version, request_id, dict(operation='calculate', context=expectation.context_json(), scope=expectation.scope.payload_json(), idempotency_key=idempotency_key, command_digest=expectation.command_digest))
        return self._call(prepare, JobView, expectation)

    def _job(self, operation: str, expectation: JobExpectation, request_id: str, version: str) -> Outcome[JobView]:
        def prepare() -> Request:
            if type(expectation) is not JobExpectation:
                raise ValueError('invalid_expectation')
            return Request(version, request_id, dict(operation=operation, job_id=expectation.job_id))
        return self._call(prepare, JobView, expectation)

    def job_status(self, expectation: JobExpectation, *, request_id: str, version: str = '1.1') -> Outcome[JobView]:
        return self._job('job_status', expectation, request_id, version)

    def job_cancel(self, expectation: JobExpectation, *, request_id: str, version: str = '1.1') -> Outcome[JobView]:
        return self._job('job_cancel', expectation, request_id, version)

    def _result(self, operation: str, result_id: str, expectation: ProducerExpectation, request_id: str, version: str) -> Outcome[ResultView]:
        def prepare() -> Request:
            if version != '1.1' or type(expectation) is not ProducerExpectation:
                raise ValueError('invalid_expectation')
            return Request(version, request_id, dict(operation=operation, result_id=result_id, cursor=None))
        return self._call(prepare, ResultView, expectation)

    def result_read(self, result_id: str, expectation: ProducerExpectation, *, request_id: str, version: str = '1.1') -> Outcome[ResultView]:
        return self._result('result_read', result_id, expectation, request_id, version)

    def artifact_read(self, result_id: str, expectation: ProducerExpectation, *, request_id: str, version: str = '1.1') -> Outcome[ResultView]:
        return self._result('artifact_read', result_id, expectation, request_id, version)
