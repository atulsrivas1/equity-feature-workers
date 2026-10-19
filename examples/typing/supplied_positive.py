"""Base installed consumer: no optional intraday import."""
from equity_feature_io_contracts import FeatureHeader
from equity_feature_workers import PublicationLimits
from equity_feature_workers.supplied_session import supplied_headers

headers: tuple[FeatureHeader, ...] = supplied_headers('structure')
limits = PublicationLimits(max_receipt_wire_bytes=400000)
assert len(headers) == 2 and limits.max_receipt_wire_bytes == 400000
