"""Coordinator-owned test fixture; no physical or process durability claim."""
from dataclasses import replace
import threading

from equity_feature_io_contracts import SinkError, SinkErrorCode
from supplied_memory_delegate import MemorySink


class SerializedMemorySink:
    """Guard an unchanged accepted MemorySink delegate, including internal calls."""
    def __init__(self, fault=None):
        self._inner = MemorySink(fault)
        self._owner = threading.current_thread()
        self._busy = False
        self.calls = []

    def _call(self, operation, *args):
        if threading.current_thread() is not self._owner:
            raise SinkError(SinkErrorCode.INVALID_SESSION)
        if self._busy:
            raise SinkError(SinkErrorCode.BUSY)
        self._busy = True
        try:
            self.calls.append(getattr(operation, '__name__', type(operation).__name__))
            return operation(*args)
        finally:
            self._busy = False

    def capabilities(self):
        return replace(self._call(self._inner.capabilities), writer_mode='serialized_writer')

    def begin(self, envelope):
        return self._call(self._inner.begin, envelope)

    def write(self, session, ordinal, result):
        return self._call(self._inner.write, session, ordinal, result)

    def commit(self, session):
        return self._call(self._inner.commit, session)

    def abort(self, session):
        return self._call(self._inner.abort, session)

    def lookup(self, key):
        return self._call(self._inner.lookup, key)

    def read(self, receipt):
        return self._call(self._inner.read, receipt)
