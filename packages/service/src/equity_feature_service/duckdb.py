"""Explicit optional bridge to the accepted parameterized DuckDB adapter."""
from __future__ import annotations

from equity_feature_contracts.adapters import AcquisitionRequest, Cancellation
from equity_feature_duckdb import DuckDBHistoricalAdapter, ReadResult

from .codec import WireError
from .datasets import RawRead, Scope


class DuckDBSource:
    """Operator startup admission reads one exact owned query, never a remote factory."""
    def __init__(self, adapter: DuckDBHistoricalAdapter, request: AcquisitionRequest) -> None:
        if type(adapter) is not DuckDBHistoricalAdapter or type(request) is not AcquisitionRequest:
            raise ValueError("typed_source_required")
        if len(request.instruments) != 1 or len(request.sessions) != 1 or request.max_rows > 100 or request.max_batch_rows > 100:
            raise ValueError("bounded_source_required")
        if sum(p.original_bytes for p in adapter.config.resolved.partitions) > 1_048_576:
            raise ValueError("bounded_source_required")
        self.adapter, self.request = adapter, request
        self.scope = Scope(request.instruments[0], request.sessions[0], request.start_ns, request.end_ns)
        self.calls = 0
        self.baseline = self.read()

    def read(self, cancellation: Cancellation | None = None) -> RawRead:
        self.calls += 1
        result = self.adapter.read(self.request, cancellation)
        if type(result) is not ReadResult or result.canonical is None or result.receipt is None:
            raise WireError("inconsistent_identity")
        if result.metrics.selected_file_bytes > 1_048_576 or result.metrics.delivered_rows > 100:
            raise WireError("bounds")
        if result.canonical.metadata.source != result.receipt.source or result.canonical.row_count != result.receipt.rows:
            raise WireError("inconsistent_identity")
        return RawRead(result.canonical, result.receipt.identity_digest)
